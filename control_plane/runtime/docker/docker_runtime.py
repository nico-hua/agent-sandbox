"""Async Docker operations, read-only state mapping and cancellation adaptation."""

import asyncio
import threading
from collections.abc import Callable
from typing import Any, Protocol, TypeVar

import docker
from docker.errors import DockerException, NotFound
from requests import exceptions as requests_exceptions

from control_plane.core.errors import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxError,
    SandboxNotFound,
    SandboxQueryFailed,
)
from control_plane.core.models import SandboxQueryResult, SandboxSpec
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.runtime.docker.transaction import (
    CreationCancelled,
    DockerClient,
    DockerCreationTransaction,
    create_runtime_client,
    query_labels_match,
    resource_labels,
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


TaskResult = TypeVar("TaskResult")


async def _await_through_cancellation(
    task: asyncio.Task[TaskResult],
) -> TaskResult:
    """Wait for cleanup work despite repeated cancellation requests."""
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.cancelled():
                raise


class DockerSandboxRuntime(SandboxRuntime):
    """Ping, create and inspect sandboxes with a fixed Docker safety baseline."""

    def __init__(
        self,
        client_factory: Callable[[], DockerClient] | None = None,
        *,
        ping_client_factory: Callable[[], ProbeDockerClient] | None = None,
    ) -> None:
        """Store a lazy Docker client factory without connecting."""
        self._client_factory = (
            client_factory if client_factory is not None else create_runtime_client
        )
        self._ping_client_factory = (
            ping_client_factory
            if ping_client_factory is not None
            else create_probe_client
        )
        self._creation = DockerCreationTransaction(self._client_factory)

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
        """Run the transaction and synchronously clean up on cancellation."""
        cancelled = threading.Event()
        worker = asyncio.create_task(
            asyncio.to_thread(self._creation.create_sync, spec, cancelled)
        )
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancellation:
            cancelled.set()
            try:
                records = await _await_through_cancellation(worker)
            except CreationCancelled:
                raise cancellation
            except SandboxCleanupFailed:
                raise
            except SandboxError:
                raise cancellation

            cleanup = asyncio.create_task(
                asyncio.to_thread(self._creation.rollback_committed, spec, records)
            )
            await _await_through_cancellation(cleanup)
            raise cancellation

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read one owned sandbox state without mutating Docker resources."""
        return await asyncio.to_thread(get_sandbox_sync, self._client_factory, sandbox_id)


def get_sandbox_sync(
    client_factory: Callable[[], DockerClient],
    sandbox_id: str,
) -> SandboxQueryResult:
    """Inspect an exactly labelled managed container and map its state."""
    try:
        client = client_factory()
    except Exception as error:
        raise DockerUnavailable() from error

    try:
        try:
            containers = client.containers.list(
                all=True,
                filters={
                    "label": [
                        "io.agent-sandbox.managed=true",
                        "io.agent-sandbox.project=agent-sandbox",
                        f"io.agent-sandbox.sandbox-id={sandbox_id}",
                        "io.agent-sandbox.resource=container",
                    ]
                },
            )
            if not containers:
                raise SandboxNotFound()
            if len(containers) != 1:
                raise SandboxQueryFailed()

            container_id = containers[0].id
            container = client.containers.get(container_id)
            container.reload()
            if container.id != container_id:
                raise SandboxNotFound()

            labels = resource_labels(container, "container")
            if not query_labels_match(labels, sandbox_id):
                raise SandboxNotFound()

            state = container.attrs.get("State")
            if not isinstance(state, dict):
                raise SandboxQueryFailed()
            return map_container_state(sandbox_id, state)
        except SandboxError:
            raise
        except NotFound as error:
            raise SandboxNotFound() from error
        except (DockerException, requests_exceptions.RequestException) as error:
            raise DockerUnavailable() from error
        except Exception as error:
            raise SandboxQueryFailed() from error
    finally:
        client.close()


def map_container_state(
    sandbox_id: str,
    state: dict[str, Any],
) -> SandboxQueryResult:
    """Map Docker container and health facts to the public state model."""
    status = state.get("Status")
    if status == "created":
        return SandboxQueryResult(
            sandbox_id,
            "starting",
            "container_created",
            "container has been created but is not running",
        )
    if status == "restarting":
        return SandboxQueryResult(
            sandbox_id,
            "starting",
            "container_restarting",
            "container is restarting",
        )
    if status == "running":
        return map_running_state(sandbox_id, state)
    if status == "paused":
        return SandboxQueryResult(
            sandbox_id,
            "stopped",
            "container_paused",
            "container is paused",
        )
    if status == "exited" and state.get("OOMKilled") is True:
        return SandboxQueryResult(
            sandbox_id,
            "failed",
            "container_oom_killed",
            "container was terminated after exceeding its memory limit",
        )
    if status == "exited" and state.get("ExitCode") == 0:
        return SandboxQueryResult(
            sandbox_id,
            "stopped",
            "container_exited",
            "container exited successfully",
        )
    if status == "exited":
        return SandboxQueryResult(
            sandbox_id,
            "failed",
            "container_exited_with_error",
            "container exited with a non-zero status",
        )
    if status == "dead":
        return SandboxQueryResult(
            sandbox_id,
            "failed",
            "container_dead",
            "Docker reports that the container is dead",
        )
    raise SandboxQueryFailed()


def map_running_state(
    sandbox_id: str,
    state: dict[str, Any],
) -> SandboxQueryResult:
    """Map a running container's optional Docker health information."""
    health = state.get("Health")
    if health is None:
        return SandboxQueryResult(
            sandbox_id,
            "running",
            "health_check_not_configured",
            "container is running but no health check is configured",
        )
    if not isinstance(health, dict):
        raise SandboxQueryFailed()

    health_status = health.get("Status")
    if health_status == "starting":
        return SandboxQueryResult(
            sandbox_id,
            "starting",
            "health_check_starting",
            "container is running and its health check is starting",
        )
    if health_status == "healthy":
        return SandboxQueryResult(
            sandbox_id,
            "ready",
            "health_check_passed",
            "Agent health check passed",
        )
    if health_status == "unhealthy":
        return SandboxQueryResult(
            sandbox_id,
            "failed",
            "health_check_failed",
            "Agent health check is failing",
        )
    raise SandboxQueryFailed()
