"""Validate, execute and compensate a single sandbox creation transaction."""

import asyncio
import secrets
from collections.abc import Callable, Collection
from contextlib import AbstractAsyncContextManager
from dataclasses import replace
from datetime import datetime, timedelta

from control_plane.common.async_tasks import settle_task, wait_for_task
from control_plane.domain.endpoint import SandboxAccessCredential
from control_plane.domain.errors import (
    CreationFailureInfo,
    CreationResourceState,
    SandboxCleanupFailed,
    SandboxCreateUncertain,
    SandboxEnvironmentNotRegistered,
    SandboxError,
    SandboxInstanceLimitExceeded,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    WorkspaceConflict,
)
from control_plane.domain.sandbox import (
    SandboxCreateOptions,
    SandboxCreateResult,
    SandboxLifecycleRecord,
    SandboxSpec,
)
from control_plane.domain.workspace import WorkspaceBinding
from control_plane.runtime.base_runtime import SandboxRuntime
from control_plane.services.sandbox_lifecycle import mark_deleting, remove_metadata
from control_plane.services.workspace_service import WorkspaceService
from control_plane.storage.base import (
    LifecycleInstanceLimitExceeded,
    LifecycleRecordNotFound,
    LifecycleStore,
    LifecycleStoreConflict,
)


def validate_creation_options(
    options: SandboxCreateOptions | None, environment_names: Collection[str],
) -> SandboxCreateOptions:
    """Apply creation policy before admission or identity generation."""
    if options is None:
        options = SandboxCreateOptions()
    if type(options) is not SandboxCreateOptions:
        raise ValueError("options must be SandboxCreateOptions")
    options.validate()
    if options.environment not in environment_names:
        raise SandboxEnvironmentNotRegistered()
    return options


def build_spec(
    options: SandboxCreateOptions,
    created_at: datetime,
    id_factory: Callable[[], str],
    transaction_id_factory: Callable[[], str],
) -> SandboxSpec:
    """Allocate the identities, initial deadline and credential for an admitted attempt."""
    expires_at = created_at + timedelta(seconds=options.ttl_seconds)
    sandbox_id = id_factory()
    return SandboxSpec(
        sandbox_id=sandbox_id,
        container_name=f"agent-sandbox-{sandbox_id}",
        network_name=f"agent-sandbox-{sandbox_id}-internal",
        volume_name=f"agent-sandbox-{sandbox_id}-workspace",
        transaction_id=transaction_id_factory(),
        created_at=created_at,
        expires_at=expires_at,
        resources=options.resources,
        environment=options.environment,
        network=options.network,
        access_token=secrets.token_urlsafe(32),
    )


class SandboxCreation:
    """Own one pending-to-active flow and compensate only the original transaction."""

    def __init__(
        self, runtime: SandboxRuntime, store: LifecycleStore, workspaces: WorkspaceService,
        instance_operation: Callable[[str], AbstractAsyncContextManager[None]],
        max_instances: int,
    ) -> None:
        """Use the same adapters and instance coordination as other lifecycle operations."""
        self._runtime = runtime
        self._store = store
        self._workspaces = workspaces
        self._instance_operation = instance_operation
        self._max_instances = max_instances

    async def run(self, spec: SandboxSpec, *, workspace_id: str | None) -> SandboxCreateResult:
        """Persist, create and activate one admitted specification, including any compensation."""
        if workspace_id is not None:
            binding = await self._workspaces.prepare_binding(workspace_id, spec)
            spec = replace(spec, volume_name=binding.volume_name, workspace=binding)
            workspace_id = binding.workspace_id
        sandbox_id, created_at, expires_at = spec.sandbox_id, spec.created_at, spec.expires_at
        record = SandboxLifecycleRecord(
            sandbox_id, spec.transaction_id, created_at, expires_at,
            workspace_id=workspace_id,
        )
        # Persist pending for recovery before creating resources and confirming active.
        # The management state active does not imply the Agent health state ready.
        credential = SandboxAccessCredential(sandbox_id, spec.transaction_id, spec.access_token)
        await self._prewrite(record, spec.workspace, credential=credential)
        await self._create_runtime(spec, record)
        await self._activate(record)
        return SandboxCreateResult(
            sandbox_id=sandbox_id, status="started",
            created_at=created_at, expires_at=expires_at,
            workspace_id=workspace_id,
        )

    async def _prewrite(
        self, record: SandboxLifecycleRecord, workspace: WorkspaceBinding | None = None,
        *, credential: SandboxAccessCredential | None = None,
    ) -> None:
        """Finish the pending write before resource creation, settling and compensating cancellation."""
        if workspace is None:
            operation = self._store.create(
                record, credential=credential, max_instances=self._max_instances,
            )
        else:
            operation = self._store.create_with_workspace(
                record, workspace, expected_workspace_version=workspace.version - 1,
                credential=credential, max_instances=self._max_instances,
            )
        worker = asyncio.create_task(operation)
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            # Cancelling an await cannot stop SQLite in its worker thread. Settle
            # the write, then compensate only our original transaction and version.
            try:
                await settle_task(worker)
            except (LifecycleStoreConflict, LifecycleInstanceLimitExceeded):
                # Both outcomes prove that this pending insert did not commit.
                raise cancellation
            except (Exception, asyncio.CancelledError) as error:
                raise SandboxMetadataUnavailable(record.sandbox_id) from error
            await remove_metadata(self._store, record)
            raise
        except LifecycleInstanceLimitExceeded as error:
            raise SandboxInstanceLimitExceeded() from error
        except LifecycleStoreConflict as error:
            if workspace is not None:
                raise WorkspaceConflict(workspace.workspace_id) from error
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataUnavailable(record.sandbox_id) from error

    async def _activate(self, record: SandboxLifecycleRecord) -> None:
        """Confirm activation, compensating known versions on cancellation and retaining unknown outcomes."""
        worker = asyncio.create_task(self._store.activate(
            record.sandbox_id, record.transaction_id, expected_version=record.version,
        ))
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            try:
                active = await settle_task(worker)
            except LifecycleRecordNotFound:
                await self._rollback_created(record)
                raise cancellation
            except LifecycleStoreConflict as error:
                await self._cleanup_created_runtime(record)
                raise SandboxMetadataConflict(record.sandbox_id) from error
            except (Exception, asyncio.CancelledError) as error:
                # An error may follow a durable commit. Keep metadata and resources
                # when the committed version is unknown rather than guessing a cleanup target.
                raise SandboxMetadataUnavailable(record.sandbox_id) from error
            await self._rollback_created(active)
            raise
        except LifecycleRecordNotFound as error:
            # Manual deletion may finish before Runtime.create. Its missing row
            # must not leave this transaction's late-created resources orphaned.
            await self._rollback_created(record)
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except LifecycleStoreConflict as error:
            # A concurrent operation owns the newer metadata version. Leave it
            # untouched while reclaiming resources from our confirmed creation.
            await self._cleanup_created_runtime(record)
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataUnavailable(record.sandbox_id) from error

    async def _rollback_created(self, record: SandboxLifecycleRecord) -> None:
        """Compensate a confirmed creation and settle cleanup through repeated cancellation."""
        cleanup = asyncio.create_task(self._finish_creation_rollback(record))
        await settle_task(cleanup)

    async def _finish_creation_rollback(self, record: SandboxLifecycleRecord) -> None:
        """Coordinate deletion intent and compensation of committed or late-created resources."""
        async with self._instance_operation(record.sandbox_id):
            try:
                deleting = await mark_deleting(self._store, record, allow_absent=True)
            except SandboxMetadataConflict:
                # Reclaim only our late resources without adopting the newer
                # metadata version; this caller already holds instance ownership.
                await self._delete_created_runtime(record)
                raise
            await self._delete_created_runtime(record)
            await remove_metadata(self._store, deleting)

    async def _cleanup_created_runtime(self, record: SandboxLifecycleRecord) -> None:
        """Settle resource-only compensation, including its wait for instance ownership."""
        cleanup = asyncio.create_task(self._cleanup_created_runtime_guarded(record))
        await settle_task(cleanup)

    async def _cleanup_created_runtime_guarded(self, record: SandboxLifecycleRecord) -> None:
        """Serialize resource-only compensation without changing another metadata version."""
        async with self._instance_operation(record.sandbox_id):
            await self._delete_created_runtime(record)

    async def _delete_created_runtime(self, record: SandboxLifecycleRecord) -> None:
        """Finish exact-transaction resource compensation while the caller holds instance ownership."""
        workspace = await self._workspaces.binding_for(record)
        cleanup = asyncio.create_task(self._runtime.delete(
            record.sandbox_id, delete_workspace=workspace is None,
            expected_transaction_id=record.transaction_id,
            **({"expected_workspace": workspace} if workspace is not None else {}),
        ))
        try:
            await settle_task(cleanup)
        except (Exception, asyncio.CancelledError) as error:
            cleanup_error = SandboxCleanupFailed(record.sandbox_id)
            cleanup_error.creation_failure = CreationFailureInfo(
                "compensate_runtime", CreationResourceState.UNKNOWN,
            )
            raise cleanup_error from error

    async def _create_runtime(
        self, spec: SandboxSpec, record: SandboxLifecycleRecord,
    ) -> None:
        """Create runtime resources, removing recovery metadata only after confirmed cleanup."""
        worker = asyncio.create_task(self._runtime.create(spec))
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            worker.cancel()
            try:
                await settle_task(worker)
            except asyncio.CancelledError:
                # Runtime may return CancelledError only after exact rollback.
                # Incomplete cleanup must use an error that preserves recovery metadata.
                await remove_metadata(self._store, record)
                raise cancellation
            except Exception as error:  # noqa: BLE001
                await self._finish_failed_creation(error, record)
                raise cancellation
            # Creation may win the cancellation race, or Runtime may finish anyway.
            # Explicitly compensate this transaction instead of assuming rollback.
            await self._rollback_created(record)
            raise
        except Exception as error:
            await self._finish_failed_creation(error, record)
            raise

    async def _finish_failed_creation(
        self, error: Exception, record: SandboxLifecycleRecord,
    ) -> None:
        """Use creation_failure.resource_state to retain metadata for unknown or remaining resources."""
        info = (
            error.creation_failure if isinstance(error, SandboxError) else None
        ) or CreationFailureInfo("runtime_create", CreationResourceState.UNKNOWN)
        # Error types alone do not prove cleanup; only clean-resource evidence permits removal.
        if info.resource_state in {
            CreationResourceState.UNCHANGED, CreationResourceState.ROLLED_BACK,
        }:
            await remove_metadata(self._store, record)
            return
        if isinstance(error, SandboxCleanupFailed):
            error.creation_failure = info
            raise error
        uncertain = SandboxCreateUncertain(record.sandbox_id)
        uncertain.creation_failure = info
        raise uncertain from error
