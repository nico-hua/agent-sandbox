"""Sandbox identity generation, validation and lifecycle orchestration."""

import asyncio
import re
import secrets
from collections.abc import AsyncIterator, Callable, Collection
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from control_plane.common.async_tasks import settle_task, wait_for_task
from control_plane.domain.errors import (
    CreationFailureInfo,
    CreationResourceState,
    DockerUnavailable,
    SandboxAgentUnavailable,
    SandboxCleanupFailed,
    SandboxCreateCapacityExceeded,
    SandboxCreateUncertain,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxEndpointAuthRequired,
    SandboxEndpointAuthUnavailable,
    SandboxEndpointUnavailable,
    SandboxEnvironmentNotRegistered,
    SandboxError,
    SandboxInstanceLimitExceeded,
    SandboxListUnavailable,
    SandboxMetadataCleanupFailed,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    SandboxNotFound,
    SandboxNotReady,
    SandboxPauseConflict,
    SandboxPauseFailed,
    SandboxQueryFailed,
    SandboxRenewConflict,
    SandboxResumeConflict,
    SandboxResumeFailed,
    SandboxServiceUnsupported,
    WorkspaceConflict,
    WorkspaceDeleteProtected,
)
from control_plane.domain.models import (
    BUILTIN_ENVIRONMENTS,
    MAX_CUSTOM_ENVIRONMENTS,
    SandboxAccessCredential,
    SandboxCreateOptions,
    SandboxCreateResult,
    SandboxEndpoint,
    SandboxLifecycleRecord,
    SandboxListResult,
    SandboxPendingInspectionResult,
    SandboxPendingRecoveryResult,
    SandboxQueryResult,
    SandboxReadyResult,
    SandboxReapItem,
    SandboxReapResult,
    SandboxRenewResult,
    SandboxServiceEndpoint,
    SandboxSpec,
    WorkspaceBinding,
    WorkspaceRecord,
    _new_transaction_id,
    _validate_environment,
    _validate_service_port,
    validate_sandbox_list_query,
)
from control_plane.runtime.base import AgentHealthProbe, SandboxRuntime
from control_plane.services.endpoint_access import verified_endpoint_credential
from control_plane.services.maintenance import SandboxMaintenance
from control_plane.services.workspace_service import WorkspaceService
from control_plane.storage.base import (
    LifecycleInstanceLimitExceeded,
    LifecycleRecordNotFound,
    LifecycleStore,
    LifecycleStoreConflict,
)


DEFAULT_MAX_CONCURRENT_CREATES = 2
DEFAULT_MAX_INSTANCES = 10


def _utc_now() -> datetime:
    """Read the current timezone-aware UTC time for one lifecycle operation."""
    return datetime.now(UTC)


def _new_sandbox_id() -> str:
    """Generate an unpredictable sandbox ID with 128 random bits."""
    return f"sbx_{secrets.token_hex(16)}"


class SandboxService:
    """Generate sandbox metadata and delegate lifecycle operations."""

    def __init__(
        self,
        runtime: SandboxRuntime,
        store: LifecycleStore,
        id_factory: Callable[[], str] | None = None,
        transaction_id_factory: Callable[[], str] | None = None,
        clock: Callable[[], datetime] | None = None,
        *,
        agent_probe: AgentHealthProbe | None = None,
        max_concurrent_creates: int = DEFAULT_MAX_CONCURRENT_CREATES,
        max_instances: int = DEFAULT_MAX_INSTANCES,
        environment_names: Collection[str] | None = None,
    ) -> None:
        """Validate per-Service admission and store lazy lifecycle dependencies and factories."""
        if type(max_concurrent_creates) is not int or not 1 <= max_concurrent_creates <= 16:
            raise ValueError("max_concurrent_creates must be an integer between 1 and 16")
        if type(max_instances) is not int or not 1 <= max_instances <= 1024:
            raise ValueError("max_instances must be an integer between 1 and 1024")
        names = BUILTIN_ENVIRONMENTS if environment_names is None else environment_names
        if (
            not isinstance(names, Collection) or isinstance(names, (str, bytes))
            or not 1 <= len(names) <= len(BUILTIN_ENVIRONMENTS) + MAX_CUSTOM_ENVIRONMENTS
        ):
            raise ValueError("environment_names must be a bounded collection of registered names")
        copied_names = tuple(names)
        for name in copied_names:
            _validate_environment(name)
        self._environment_names = frozenset(copied_names)
        self._max_instances = max_instances
        self._max_concurrent_creates = max_concurrent_creates
        self._creates_in_progress = 0
        self._instance_operations: dict[str, tuple[asyncio.Lock, int]] = {}
        self._workspace_creation_owners: ContextVar[frozenset[str]] = ContextVar(
            "workspace_creation_owners", default=frozenset(),
        )
        self._runtime = runtime
        self._store = store
        self._id_factory = id_factory if id_factory is not None else _new_sandbox_id
        self._transaction_id_factory = (
            transaction_id_factory
            if transaction_id_factory is not None
            else _new_transaction_id
        )
        self._clock = clock if clock is not None else _utc_now
        self._agent_probe = agent_probe
        self._workspaces = WorkspaceService(runtime, store, self._environment_names, self._now)
        self._maintenance = SandboxMaintenance(runtime, store, self._workspaces, self._now, self._reap_candidate)

    async def ping(self) -> None:
        """Check backend availability and propagate its domain errors."""
        await self._runtime.ping()

    def _now(self) -> datetime:
        """Sample the injected clock once and normalize an aware datetime to UTC."""
        value = self._clock()
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise ValueError("lifecycle clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    async def create_workspace(self, environment: str = "basic") -> WorkspaceRecord:
        """Create a registered independent workspace through the shared business entry."""
        return await self._workspaces.create_workspace(environment)

    async def get_workspace(self, workspace_id: str) -> WorkspaceRecord:
        """Return registered metadata with current volume identity verified."""
        return await self._workspaces.get_workspace(workspace_id)

    async def delete_workspace(self, workspace_id: str) -> None:
        """Delete an unoccupied independent workspace through the shared business entry."""
        await self._workspaces.delete_workspace(workspace_id)

    async def create(self, options: SandboxCreateOptions | None = None) -> SandboxCreateResult:
        """Validate then admit without queueing, holding capacity through all transaction cleanup."""
        if options is None:
            options = SandboxCreateOptions()
        if type(options) is not SandboxCreateOptions:
            raise ValueError("options must be SandboxCreateOptions")
        options.validate()
        if options.environment not in self._environment_names:
            raise SandboxEnvironmentNotRegistered()
        # There is no await between checking and claiming: competing tasks on
        # this Service's event loop cannot interleave admission. This is not a
        # cross-thread, process or worker capacity guarantee.
        if self._creates_in_progress >= self._max_concurrent_creates:
            raise SandboxCreateCapacityExceeded()
        self._creates_in_progress += 1
        try:
            return await self._create_admitted(options)
        finally:
            # Existing helpers settle critical workers and compensation before
            # returning or raising, including repeated caller cancellation.
            self._creates_in_progress -= 1

    async def _create_admitted(self, options: SandboxCreateOptions) -> SandboxCreateResult:
        """Run the admitted creation transaction; started confirms startup, not readiness."""
        created_at = self._now()
        expires_at = created_at + timedelta(seconds=options.ttl_seconds)
        sandbox_id = self._id_factory()
        spec = SandboxSpec(
            sandbox_id=sandbox_id,
            container_name=f"agent-sandbox-{sandbox_id}",
            network_name=f"agent-sandbox-{sandbox_id}-internal",
            volume_name=f"agent-sandbox-{sandbox_id}-workspace",
            transaction_id=self._transaction_id_factory(),
            created_at=created_at,
            expires_at=expires_at,
            resources=options.resources,
            environment=options.environment,
            network=options.network,
            access_token=secrets.token_urlsafe(32),
        )
        if options.workspace_id is not None:
            # Hold instance coordination through pending, creation, confirmation
            # and compensation: deletion must not free a still-in-flight mount.
            async with self._instance_operation(sandbox_id):
                owner = self._workspace_creation_owners.set(frozenset({sandbox_id}))
                try:
                    binding = await self._workspaces.prepare_binding(options.workspace_id, spec)
                    spec = replace(spec, volume_name=binding.volume_name, workspace=binding)
                    return await self._complete_creation(spec)
                finally:
                    self._workspace_creation_owners.reset(owner)
        return await self._complete_creation(spec)

    async def _complete_creation(self, spec: SandboxSpec) -> SandboxCreateResult:
        """Confirm one admitted specification, keeping private and external volume ownership distinct."""
        sandbox_id, created_at, expires_at = spec.sandbox_id, spec.created_at, spec.expires_at
        workspace_id = spec.workspace.workspace_id if spec.workspace is not None else None
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
            await self._remove_metadata(record)
            raise cancellation
        except LifecycleInstanceLimitExceeded as error:
            raise SandboxInstanceLimitExceeded() from error
        except LifecycleStoreConflict as error:
            if workspace is not None:
                raise WorkspaceConflict(workspace.workspace_id) from error
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataUnavailable(record.sandbox_id) from error

    async def _remove_metadata(self, record: SandboxLifecycleRecord) -> None:
        """Delete only the supplied transaction/version without refreshing a changed record."""
        worker = asyncio.create_task(self._store.delete(
            record.sandbox_id, record.transaction_id, expected_version=record.version,
        ))
        try:
            await settle_task(worker, propagate_cancellation=True)
        except asyncio.CancelledError as error:
            if not worker.cancelled():
                raise
            raise SandboxMetadataCleanupFailed(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataCleanupFailed(record.sandbox_id) from error

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
            raise cancellation
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
                deleting = await self._mark_deleting(record, allow_absent=True)
            except SandboxMetadataConflict:
                # Reclaim only our late resources without adopting the newer
                # metadata version; this caller already holds instance ownership.
                await self._delete_created_runtime(record)
                raise
            await self._delete_created_runtime(record)
            await self._remove_metadata(deleting)

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
                await self._remove_metadata(record)
                raise cancellation
            except Exception as error:
                await self._finish_failed_creation(error, record)
                raise cancellation
            # Creation may win the cancellation race, or Runtime may finish anyway.
            # Explicitly compensate this transaction instead of assuming rollback.
            await self._rollback_created(record)
            raise cancellation
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
            await self._remove_metadata(record)
            return
        if isinstance(error, SandboxCleanupFailed):
            error.creation_failure = info
            raise error
        uncertain = SandboxCreateUncertain(record.sandbox_id)
        uncertain.creation_failure = info
        raise uncertain from error

    async def list_sandboxes(self, *, limit: int = 20, cursor: str | None = None) -> SandboxListResult:
        """List persisted public summaries without reading runtime facts or execution credentials."""
        validate_sandbox_list_query(limit, cursor)
        try:
            page = await self._store.list_sandboxes(limit=limit, cursor=cursor)
        except Exception as error:
            raise SandboxListUnavailable() from error
        return SandboxListResult(page.items, page.next_cursor, page.managed_count, self._max_instances)

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read runtime facts and merge persisted expiration only after matching identities."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        observed = await self._runtime.get(sandbox_id)
        try:
            record = await self._store.get(sandbox_id)
        except Exception as error:
            raise SandboxMetadataUnavailable(sandbox_id) from error
        if record is None:
            if observed.workspace_id is not None:
                raise SandboxMetadataConflict(sandbox_id)
            return observed
        # Docker labels retain initial times; the store holds renewed expiration.
        # Match the transaction and created_at before combining resources with metadata.
        if (
            record.sandbox_id != sandbox_id
            or observed.sandbox_id != sandbox_id
            or record.transaction_id != observed.transaction_id
            or record.created_at != observed.created_at
            or record.workspace_id != observed.workspace_id
        ):
            raise SandboxMetadataConflict(sandbox_id)
        await self._workspaces.verify_observed(record, observed)
        return replace(observed, expires_at=record.expires_at)

    async def check_ready(self, sandbox_id: str) -> SandboxReadyResult:
        """Verify runtime and metadata, then check the Agent through its verified local entry."""
        observed = await self.get(sandbox_id)
        if observed.sandbox_id != sandbox_id:
            raise SandboxQueryFailed()
        if observed.status != "ready":
            raise SandboxNotReady()
        try:
            # Local /healthz stays anonymous. Readiness observes the existing
            # HTTP chain without handing execution credentials to the probe.
            endpoint = await self._runtime.get_endpoint(sandbox_id)
        except (DockerUnavailable, SandboxQueryFailed, SandboxEndpointUnavailable):
            # Retain the existing endpoint route's policy for entry lookup failures.
            raise SandboxEndpointUnavailable() from None
        if endpoint.sandbox_id != sandbox_id:
            raise SandboxEndpointUnavailable()
        if self._agent_probe is None:
            raise SandboxAgentUnavailable()
        await self._agent_probe.check(endpoint.url)
        return SandboxReadyResult(sandbox_id)

    async def renew(self, sandbox_id: str, ttl_seconds: int) -> SandboxRenewResult:
        """Renew an identity-matched, active, unexpired instance using one UTC clock snapshot."""
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 86400:
            raise ValueError("ttl_seconds must be an integer between 1 and 86400")
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        now = self._now()
        try:
            observed = await self._runtime.get(sandbox_id)
        except (SandboxNotFound, DockerUnavailable, SandboxQueryFailed):
            raise
        except Exception as error:
            raise SandboxQueryFailed() from error
        try:
            record = await self._store.get(sandbox_id)
        except Exception as error:
            raise SandboxMetadataUnavailable(sandbox_id) from error
        if record is None:
            raise SandboxRenewConflict(sandbox_id)
        if (
            record.sandbox_id != sandbox_id
            or observed.sandbox_id != sandbox_id
            or record.transaction_id != observed.transaction_id
            or record.created_at != observed.created_at
            or record.workspace_id != observed.workspace_id
        ):
            raise SandboxMetadataConflict(sandbox_id)
        if record.management_state != "active" or record.expires_at <= now:
            raise SandboxRenewConflict(sandbox_id)
        await self._workspaces.verify_observed(record, observed)
        # TTL starts at this operation's now; max avoids shortening or adding to the old expiry.
        expires_at = max(record.expires_at, now + timedelta(seconds=ttl_seconds))
        # Even an unchanged expiration needs a guarded commit so a prior deletion claim wins.
        updated = await self._commit_renewal(record, expires_at)
        return SandboxRenewResult(sandbox_id, updated.expires_at)

    async def _commit_renewal(
        self, record: SandboxLifecycleRecord, expires_at: datetime,
    ) -> SandboxLifecycleRecord:
        """Settle a version-guarded renewal without undoing a commit after caller cancellation."""
        worker = asyncio.create_task(self._store.update_expiration(
            record.sandbox_id, record.transaction_id,
            expected_version=record.version, expires_at=expires_at,
        ))
        cancellation: asyncio.CancelledError | None = None
        try:
            try:
                updated = await wait_for_task(worker)
            except asyncio.CancelledError as error:
                cancellation = error
                # SQLite may commit after cancellation. Settle it without reversing
                # later updates, then propagate cancellation to the caller.
                updated = await settle_task(worker)
        except (LifecycleStoreConflict, LifecycleRecordNotFound) as error:
            raise SandboxRenewConflict(record.sandbox_id) from error
        except (Exception, asyncio.CancelledError) as error:
            raise SandboxMetadataUnavailable(record.sandbox_id) from error
        if cancellation is not None:
            raise cancellation
        return updated

    @asynccontextmanager
    async def _instance_operation(self, sandbox_id: str) -> AsyncIterator[None]:
        """Serialize one instance's mutations and discard entries after holders and waiters leave."""
        # Only an explicit-workspace creation holds this ownership marker.
        # Its awaited compensation child inherits the context while the parent
        # keeps the physical lock, so it must not reacquire that same lock.
        if sandbox_id in self._workspace_creation_owners.get():
            yield
            return
        lock, users = self._instance_operations.get(sandbox_id, (asyncio.Lock(), 0))
        # No await separates reference registration from lookup; a waiter must
        # retain this lock even if its current holder finishes before it runs.
        self._instance_operations[sandbox_id] = (lock, users + 1)
        try:
            async with lock:
                yield
        finally:
            _, users = self._instance_operations[sandbox_id]
            if users == 1:
                del self._instance_operations[sandbox_id]
            else:
                self._instance_operations[sandbox_id] = (lock, users - 1)

    async def pause(self, sandbox_id: str) -> None:
        """Freeze an active, unexpired managed instance without changing its TTL or identity."""
        await self._set_paused(sandbox_id, paused=True)

    async def resume(self, sandbox_id: str) -> None:
        """Thaw an active, unexpired managed instance without promising Agent readiness."""
        await self._set_paused(sandbox_id, paused=False)

    async def _set_paused(self, sandbox_id: str, *, paused: bool) -> None:
        """Cancel queued work or settle an already-started mutation before releasing coordination."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        worker = asyncio.create_task(self._change_paused(sandbox_id, paused=paused))
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            worker.cancel()
            try:
                await settle_task(worker)
            except asyncio.CancelledError:
                pass
            raise cancellation

    async def _change_paused(self, sandbox_id: str, *, paused: bool) -> None:
        """Recheck metadata under instance coordination before invoking exact-transaction mutation."""
        conflict = SandboxPauseConflict if paused else SandboxResumeConflict
        failure = SandboxPauseFailed if paused else SandboxResumeFailed
        async with self._instance_operation(sandbox_id):
            try:
                observation = asyncio.create_task(self._runtime.get(sandbox_id))
                observed = await settle_task(observation, propagate_cancellation=True)
            except SandboxError:
                raise
            except Exception as error:
                raise SandboxQueryFailed() from error
            try:
                reading = asyncio.create_task(self._store.get(sandbox_id))
                record = await settle_task(reading, propagate_cancellation=True)
            except Exception as error:
                raise SandboxMetadataUnavailable(sandbox_id) from error
            if record is None:
                raise conflict()
            if (
                record.sandbox_id != sandbox_id or observed.sandbox_id != sandbox_id
                or record.transaction_id != observed.transaction_id
                or record.created_at != observed.created_at
                or record.workspace_id != observed.workspace_id
            ):
                raise SandboxMetadataConflict(sandbox_id)
            if record.management_state != "active" or record.expires_at <= self._now():
                raise conflict()
            workspace = await self._workspaces.verify_observed(record, observed)
            try:
                operation = self._runtime.pause if paused else self._runtime.resume
                # Runtime must settle any started SDK work before propagating
                # cancellation. Never undo a successful or uncertain mutation.
                await operation(
                    sandbox_id, expected_transaction_id=record.transaction_id,
                    **({"expected_workspace": workspace} if workspace is not None else {}),
                )
            except SandboxError:
                raise
            except Exception as error:
                raise failure() from error

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False
    ) -> None:
        """Claim and reclaim resources, preserving workspace by default and settling cancellation."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        worker = asyncio.create_task(self._delete(sandbox_id, delete_workspace))
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            # Finish resource and metadata deletion together, including any late worker commit.
            await settle_task(worker)
            raise cancellation

    async def _delete(self, sandbox_id: str, delete_workspace: bool) -> None:
        """Coordinate deletion with freeze/thaw before reading metadata or claiming deletion."""
        async with self._instance_operation(sandbox_id):
            await self._delete_locked(sandbox_id, delete_workspace)

    async def _delete_locked(self, sandbox_id: str, delete_workspace: bool) -> None:
        """Read metadata and enter guarded deletion without requiring a complete runtime."""
        try:
            record = await self._store.get(sandbox_id)
        except Exception as error:
            raise SandboxMetadataUnavailable(sandbox_id) from error
        if record is not None and record.sandbox_id != sandbox_id:
            raise SandboxMetadataConflict(sandbox_id)
        if record is not None and record.workspace_id is not None and delete_workspace:
            raise WorkspaceDeleteProtected(record.workspace_id)
        if record is None:
            # Legacy instances may lack lifecycle rows; Runtime still verifies resource ownership.
            await self._runtime.delete(sandbox_id, delete_workspace=delete_workspace)
            return
        # Claim this exact version as deleting to block renewal before destructive runtime work.
        deleting = await self._mark_deleting(record)
        await self._delete_record(deleting, delete_workspace=delete_workspace)

    async def _mark_deleting(
        self, record: SandboxLifecycleRecord, *, allow_absent: bool = False,
    ) -> SandboxLifecycleRecord:
        """Confirm an exact transaction/version before any destructive backend work."""
        try:
            return await self._store.mark_deleting(
                record.sandbox_id, record.transaction_id, expected_version=record.version,
            )
        except LifecycleRecordNotFound as error:
            # Only compensation of our confirmed creation permits an absent row.
            # Its original identity/version still prevents deleting replacement metadata.
            if allow_absent:
                return record
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except LifecycleStoreConflict as error:
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataUnavailable(record.sandbox_id) from error

    async def _delete_record(
        self, record: SandboxLifecycleRecord, *, delete_workspace: bool = False,
    ) -> None:
        """Delete the claimed transaction's resources, then remove only its claimed metadata version."""
        workspace = await self._workspaces.binding_for(record)
        if workspace is not None and delete_workspace:
            raise WorkspaceDeleteProtected(workspace.workspace_id)
        await self._runtime.delete(
            record.sandbox_id, delete_workspace=delete_workspace,
            expected_transaction_id=record.transaction_id,
            **({"expected_workspace": workspace} if workspace is not None else {}),
        )
        await self._remove_metadata(record)

    async def reap_expired_once(self, limit: int = 100) -> SandboxReapResult:
        """Process one bounded round of expiration and deletion retries."""
        return await self._maintenance.reap_expired_once(limit)

    async def _reap_candidate(
        self, candidate: SandboxLifecycleRecord, now: datetime,
    ) -> SandboxReapItem:
        """Coordinate a candidate's deletion claim and cleanup with freeze/thaw operations."""
        async with self._instance_operation(candidate.sandbox_id):
            return await self._reap_candidate_locked(candidate, now)

    async def _reap_candidate_locked(
        self, candidate: SandboxLifecycleRecord, now: datetime,
    ) -> SandboxReapItem:
        """Claim a snapshot or validate a retry without adopting a replacement transaction."""
        sandbox_id = candidate.sandbox_id
        try:
            # Candidates are snapshots; recheck versions and skip if renewal or deletion won the race.
            if candidate.management_state == "active":
                deleting = await self._store.claim_expired(
                    sandbox_id, candidate.transaction_id,
                    expected_version=candidate.version, now=now,
                )
                if deleting is None:
                    return SandboxReapItem(sandbox_id, "skipped", "candidate_changed")
            elif candidate.management_state == "deleting":
                deleting = await self._mark_deleting(candidate)
            else:
                return SandboxReapItem(sandbox_id, "skipped", "candidate_changed")
        except (LifecycleStoreConflict, LifecycleRecordNotFound, SandboxMetadataConflict):
            return SandboxReapItem(sandbox_id, "skipped", "candidate_changed")
        except Exception:
            return SandboxReapItem(sandbox_id, "failed", "sandbox_metadata_unavailable")
        try:
            await self._delete_record(deleting)
        except Exception as error:
            return SandboxReapItem(sandbox_id, "failed", _reap_failure_reason(error))
        return SandboxReapItem(sandbox_id, "succeeded", "deleted")

    async def inspect_pending_once(self, limit: int = 100) -> SandboxPendingInspectionResult:
        """Read resource evidence for one bounded round of pending records."""
        return await self._maintenance.inspect_pending_once(limit)

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Deliver only a protected entry whose resource and stored credential identities match."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        endpoint = await self._runtime.get_endpoint(sandbox_id)
        if endpoint.sandbox_id != sandbox_id:
            raise SandboxEndpointUnavailable()
        if endpoint.auth_required is False and endpoint.auth_version is None:
            raise SandboxEndpointAuthRequired()
        observed = await self.get(sandbox_id)
        try:
            record = await self._store.get(sandbox_id)
        except Exception as error:
            raise SandboxMetadataUnavailable(sandbox_id) from error
        if record is None:
            raise SandboxEndpointAuthUnavailable()
        if (
            observed.transaction_id != record.transaction_id
            or observed.created_at != record.created_at
            or observed.workspace_id != record.workspace_id
        ):
            raise SandboxMetadataConflict(sandbox_id)
        credential = await verified_endpoint_credential(self._store, record, endpoint)
        return replace(endpoint, access_token=credential.access_token)

    async def get_service_endpoint(self, sandbox_id: str, port: int) -> SandboxServiceEndpoint:
        """Reuse protected entry identity and credentials without contacting the application."""
        _validate_service_port(port)
        try:
            endpoint = await self.get_endpoint(sandbox_id)
        except SandboxEndpointAuthRequired:
            raise SandboxServiceUnsupported() from None
        if endpoint.services_version != "1":
            raise SandboxServiceUnsupported()
        try:
            return SandboxServiceEndpoint(
                sandbox_id, port, endpoint.url + f"/services/{port}/", True, endpoint.access_token,
            )
        except (ValueError, TypeError) as error:
            raise SandboxEndpointUnavailable() from error

    async def recover_pending_once(self, limit: int = 100) -> SandboxPendingRecoveryResult:
        """Confirm one bounded pending batch while the caller excludes active creation."""
        return await self._maintenance.recover_pending_once(limit)


def _reap_failure_reason(error: Exception) -> str:
    """Map instance failures to stable internal summary codes, never raw exceptions."""
    for error_type, reason in (
        (SandboxMetadataCleanupFailed, "sandbox_metadata_cleanup_failed"),
        (SandboxMetadataConflict, "sandbox_metadata_conflict"),
        (SandboxMetadataUnavailable, "sandbox_metadata_unavailable"),
        (SandboxDeleteConflict, "sandbox_delete_conflict"),
        (DockerUnavailable, "docker_unavailable"),
        (SandboxDeleteFailed, "sandbox_delete_failed"),
    ):
        if isinstance(error, error_type):
            return reason
    return "sandbox_delete_failed"
