"""Adapt asynchronous runtime operations to bounded Docker SDK workers."""

import asyncio
import threading
from collections.abc import Callable, Mapping
from typing import Protocol

import docker

from control_plane.common.async_tasks import (
    settle_task,
    wait_for_completion,
    wait_for_task,
)
from control_plane.domain.errors import (
    CreationFailureInfo,
    CreationResourceState,
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxError,
)
from control_plane.domain.models import (
    SandboxEndpoint,
    SandboxQueryResult,
    SandboxResourceInspection,
    SandboxSpec,
    WorkspaceBinding,
    WorkspaceRecord,
)
from control_plane.runtime.base import SandboxRuntime
from control_plane.runtime.creation import CreationCancelled, DockerCreationTransaction
from control_plane.runtime.images import (
    build_environment_images,
    resolve_environment_image_sync,
)
from control_plane.runtime.inspection import (
    get_endpoint_sync,
    get_sandbox_sync,
    inspect_resources_sync,
)
from control_plane.runtime.lifecycle import change_pause_sync, delete_sandbox_sync
from control_plane.runtime.resources import DockerClient, create_runtime_client
from control_plane.runtime.workspaces import (
    create_workspace_sync,
    delete_workspace_sync,
    inspect_workspace_sync,
)


class ProbeDockerClient(Protocol):
    """Describe the read-only Docker client methods used by the probe."""

    def ping(self) -> object:
        """Ping the Docker daemon."""
        ...

    def close(self) -> None:
        """Close client-owned resources."""
        ...


def create_probe_client() -> ProbeDockerClient:
    """Create a Docker SDK client with a short transport timeout."""
    return docker.from_env(timeout=0.5)


class DockerRuntime(SandboxRuntime):
    """Ping, create and inspect sandboxes with a fixed Docker safety baseline."""

    def __init__(
        self,
        client_factory: Callable[[], DockerClient] | None = None,
        *,
        ping_client_factory: Callable[[], ProbeDockerClient] | None = None,
        additional_environments: Mapping[str, str] | None = None,
    ) -> None:
        """Store separate lazy client factories without connecting during assembly."""
        self._environment_images = build_environment_images(additional_environments)
        # Ping has a shorter transport deadline than lifecycle calls, so factories stay separate.
        self._client_factory = (
            client_factory if client_factory is not None else create_runtime_client
        )
        self._ping_client_factory = (
            ping_client_factory
            if ping_client_factory is not None
            else create_probe_client
        )
        self._creation = DockerCreationTransaction(
            self._client_factory, environment_images=self._environment_images,
        )

    @property
    def environment_images(self) -> Mapping[str, str]:
        """Expose the immutable operator registry used for subsequent creation transactions."""
        return self._environment_images

    async def ping(self) -> None:
        """Run a read-only Docker ping outside the ASGI event loop."""
        await asyncio.to_thread(self._ping_and_close)

    def _ping_and_close(self) -> None:
        """Map backend failures to a domain error and close any created client."""
        try:
            client = self._ping_client_factory()
            try:
                client.ping()
            finally:
                client.close()
        except Exception as error:
            raise DockerUnavailable() from error

    async def create(self, spec: SandboxSpec) -> None:
        """Create resources in a worker; propagate cancellation only after confirmed rollback."""
        # Async cancellation cannot stop an SDK call already running in the thread.
        # The transaction checks this event between resource operations instead.
        cancelled = threading.Event()
        worker = asyncio.create_task(
            asyncio.to_thread(self._creation.create_sync, spec, cancelled)
        )
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            cancelled.set()
            try:
                # Settle first to distinguish rollback, cleanup failure and late success.
                records = await settle_task(worker)
            except CreationCancelled:
                # This internal signal confirms rollback of the recorded resources.
                raise cancellation
            except SandboxCleanupFailed:
                raise
            except DockerUnavailable as error:
                # A lost create response may be classified before cancellation is observed.
                # Preserve that uncertainty rather than claiming rollback completed.
                cleanup_error = SandboxCleanupFailed(spec.sandbox_id)
                cleanup_error.creation_failure = error.creation_failure or CreationFailureInfo(
                    "runtime_create", CreationResourceState.UNKNOWN,
                )
                raise cleanup_error from error
            except SandboxError:
                raise cancellation

            # Creation won the race; reopen a client to compensate the exact returned IDs.
            cleanup = asyncio.create_task(
                asyncio.to_thread(self._creation.rollback_committed, spec, records)
            )
            await settle_task(cleanup)
            raise cancellation

    async def resolve_environment_image(self, environment: str) -> str:
        """Pin the selected Agent image in a bounded worker without checking unrelated images."""
        worker = asyncio.create_task(asyncio.to_thread(
            resolve_environment_image_sync, self._client_factory, environment, self._environment_images,
        ))
        return await settle_task(worker, propagate_cancellation=True)

    async def create_workspace(self, record: WorkspaceRecord) -> None:
        """Settle fresh-volume initialization before cancellation; keep independent data intact."""
        worker = asyncio.create_task(asyncio.to_thread(create_workspace_sync, self._client_factory, record))
        await settle_task(worker, propagate_cancellation=True)

    async def inspect_workspace(self, record: WorkspaceRecord) -> None:
        """Verify independent storage and close its transport before cancellation returns."""
        worker = asyncio.create_task(asyncio.to_thread(inspect_workspace_sync, self._client_factory, record))
        await settle_task(worker, propagate_cancellation=True)

    async def delete_workspace(self, record: WorkspaceRecord) -> None:
        """Settle exact ownership deletion and transport cleanup before cancellation returns."""
        worker = asyncio.create_task(asyncio.to_thread(delete_workspace_sync, self._client_factory, record))
        await settle_task(worker, propagate_cancellation=True)

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read managed container and Agent health facts without changing resources or metadata."""
        return await asyncio.to_thread(get_sandbox_sync, self._client_factory, sandbox_id)

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Recover a verified entry from current Docker facts, independent of creation memory."""
        return await asyncio.to_thread(
            get_endpoint_sync, self._client_factory, sandbox_id
        )

    async def inspect_resources(
        self, sandbox_id: str, *, expected_transaction_id: str,
        expected_workspace: WorkspaceBinding | None = None,
    ) -> SandboxResourceInspection:
        """Inspect every role read-only and settle transport cleanup before returning cancellation."""
        worker = asyncio.create_task(asyncio.to_thread(
            inspect_resources_sync, self._client_factory, sandbox_id,
            expected_transaction_id=expected_transaction_id, expected_workspace=expected_workspace,
        ))
        return await settle_task(worker, propagate_cancellation=True)

    async def pause(self, sandbox_id: str, *, expected_transaction_id: str, expected_workspace: WorkspaceBinding | None = None) -> None:
        """Freeze verified main and optional egress containers without changing the ingress proxy."""
        await self._change_pause(sandbox_id, expected_transaction_id, paused=True, expected_workspace=expected_workspace)

    async def resume(self, sandbox_id: str, *, expected_transaction_id: str, expected_workspace: WorkspaceBinding | None = None) -> None:
        """Thaw verified egress before main without starting any stopped container."""
        await self._change_pause(sandbox_id, expected_transaction_id, paused=False, expected_workspace=expected_workspace)

    async def _change_pause(
        self, sandbox_id: str, transaction_id: str, *, paused: bool,
        expected_workspace: WorkspaceBinding | None = None,
    ) -> None:
        """Signal cancellation between SDK stages and settle in-flight work and transport closure."""
        cancelled = threading.Event()
        worker = asyncio.create_task(asyncio.to_thread(
            change_pause_sync, self._client_factory, sandbox_id,
            expected_transaction_id=transaction_id, paused=paused, cancelled=cancelled, expected_workspace=expected_workspace,
        ))
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            cancelled.set()
            try:
                await settle_task(worker)
            except CreationCancelled:
                raise cancellation
            # A worker failure retains an incomplete/uncertain outcome instead of hiding it.
            raise cancellation

    async def delete(
        self,
        sandbox_id: str,
        *,
        delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
        expected_workspace: WorkspaceBinding | None = None,
    ) -> None:
        """Finish deletion and client closure; cancellation does not restore deleted resources."""
        # Discover partial inventories even if the main container or get() is unavailable.
        worker = asyncio.create_task(
            asyncio.to_thread(
                delete_sandbox_sync,
                self._client_factory,
                sandbox_id,
                delete_workspace=delete_workspace,
                expected_transaction_id=expected_transaction_id, expected_workspace=expected_workspace,
            )
        )
        # Service separately settles metadata; this layer preserves deletion's cancellation priority.
        cancellation = await wait_for_completion(worker)
        if cancellation is not None:
            try:
                worker.result()
            except Exception:
                # Retrieve worker failure before returning cancellation; retries discover leftovers.
                pass
            raise cancellation
        worker.result()
