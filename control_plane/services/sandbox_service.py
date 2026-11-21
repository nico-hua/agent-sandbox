"""Sandbox lifecycle entry points, admission and per-instance coordination."""

import asyncio
import secrets
from collections.abc import AsyncIterator, Callable, Collection
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta

from control_plane.common.async_tasks import settle_task, wait_for_task
from control_plane.config.defaults import (
    DEFAULT_MAX_CONCURRENT_CREATES,
    DEFAULT_MAX_INSTANCES,
)
from control_plane.domain.endpoint import SandboxEndpoint, SandboxServiceEndpoint
from control_plane.domain.errors import (
    DockerUnavailable,
    SandboxCreateCapacityExceeded,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxError,
    SandboxMetadataCleanupFailed,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    SandboxNotFound,
    SandboxPauseConflict,
    SandboxPauseFailed,
    SandboxQueryFailed,
    SandboxRenewConflict,
    SandboxResumeConflict,
    SandboxResumeFailed,
    WorkspaceDeleteProtected,
)
from control_plane.domain.maintenance import (
    SandboxPendingInspectionResult,
    SandboxPendingRecoveryResult,
    SandboxReapItem,
    SandboxReapResult,
)
from control_plane.domain.sandbox import (
    BUILTIN_ENVIRONMENTS,
    MAX_CUSTOM_ENVIRONMENTS,
    SandboxCreateOptions,
    SandboxCreateResult,
    SandboxLifecycleRecord,
    SandboxListResult,
    SandboxQueryResult,
    SandboxReadyResult,
    SandboxRenewResult,
    _new_transaction_id,
    _validate_environment,
)
from control_plane.domain.workspace import WorkspaceRecord
from control_plane.runtime.base_runtime import AgentHealthProbe, SandboxRuntime
from control_plane.services.endpoint_access import EndpointAccess
from control_plane.services.sandbox_creation import (
    SandboxCreation,
    build_spec,
    validate_creation_options,
)
from control_plane.services.sandbox_lifecycle import mark_deleting, remove_metadata
from control_plane.services.sandbox_maintenance import SandboxMaintenance
from control_plane.services.sandbox_queries import (
    SandboxQueries,
    identities_match,
    read_lifecycle_record,
    require_sandbox_id,
)
from control_plane.services.workspace_service import WorkspaceService
from control_plane.storage.base import (
    LifecycleRecordNotFound,
    LifecycleStore,
    LifecycleStoreConflict,
)


def _utc_now() -> datetime:
    """Read the current timezone-aware UTC time for one lifecycle operation."""
    return datetime.now(UTC)


def _new_sandbox_id() -> str:
    """Generate an unpredictable sandbox ID with 128 random bits."""
    return f"sbx_{secrets.token_hex(16)}"


class SandboxService:
    """Coordinate sandbox lifecycle, admission and shared instance guards."""

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
        self._workspaces = WorkspaceService(runtime, store, self._environment_names, self._now)
        self._creation = SandboxCreation(runtime, store, self._workspaces, self._instance_operation, max_instances)
        self._queries = SandboxQueries(runtime, store, self._workspaces, agent_probe, max_instances)
        self._endpoint_access = EndpointAccess(runtime, store, self._queries)
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
        options = validate_creation_options(options, self._environment_names)
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
        """Keep any independent workspace mount guarded through creation and compensation."""
        spec = build_spec(options, self._now(), self._id_factory, self._transaction_id_factory)
        if options.workspace_id is None:
            return await self._creation.run(spec, workspace_id=None)
        # Deletion must not free a still-in-flight independent workspace mount.
        async with self._instance_operation(spec.sandbox_id):
            owner = self._workspace_creation_owners.set(frozenset({spec.sandbox_id}))
            try:
                return await self._creation.run(spec, workspace_id=options.workspace_id)
            finally:
                self._workspace_creation_owners.reset(owner)

    async def list_sandboxes(self, *, limit: int = 20, cursor: str | None = None) -> SandboxListResult:
        """List persisted public summaries without using the runtime or credentials."""
        return await self._queries.list_sandboxes(limit=limit, cursor=cursor)

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read runtime facts with expiration from matched lifecycle metadata."""
        return await self._queries.get(sandbox_id)

    async def check_ready(self, sandbox_id: str) -> SandboxReadyResult:
        """Check the Agent through the verified local entry."""
        return await self._queries.check_ready(sandbox_id)

    async def renew(self, sandbox_id: str, ttl_seconds: int) -> SandboxRenewResult:
        """Renew an identity-matched, active, unexpired instance using one UTC clock snapshot."""
        if type(ttl_seconds) is not int or not 1 <= ttl_seconds <= 86400:
            raise ValueError("ttl_seconds must be an integer between 1 and 86400")
        require_sandbox_id(sandbox_id)
        now = self._now()
        try:
            observed = await self._runtime.get(sandbox_id)
        except (SandboxNotFound, DockerUnavailable, SandboxQueryFailed):
            raise
        except Exception as error:
            raise SandboxQueryFailed() from error
        record = await read_lifecycle_record(self._store, sandbox_id)
        if record is None:
            raise SandboxRenewConflict(sandbox_id)
        if not identities_match(sandbox_id, record, observed):
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
        require_sandbox_id(sandbox_id)
        worker = asyncio.create_task(self._change_paused(sandbox_id, paused=paused))
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError:
            worker.cancel()
            try:
                await settle_task(worker)
            except asyncio.CancelledError:
                pass
            raise

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
            reading = asyncio.create_task(read_lifecycle_record(self._store, sandbox_id))
            record = await settle_task(reading, propagate_cancellation=True)
            if record is None:
                raise conflict()
            if not identities_match(sandbox_id, record, observed):
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
        require_sandbox_id(sandbox_id)
        worker = asyncio.create_task(self._delete(sandbox_id, delete_workspace))
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError:
            # Finish resource and metadata deletion together, including any late worker commit.
            await settle_task(worker)
            raise

    async def _delete(self, sandbox_id: str, delete_workspace: bool) -> None:
        """Coordinate deletion with freeze/thaw before reading metadata or claiming deletion."""
        async with self._instance_operation(sandbox_id):
            await self._delete_locked(sandbox_id, delete_workspace)

    async def _delete_locked(self, sandbox_id: str, delete_workspace: bool) -> None:
        """Read metadata and enter guarded deletion without requiring a complete runtime."""
        record = await read_lifecycle_record(self._store, sandbox_id)
        if record is not None and record.sandbox_id != sandbox_id:
            raise SandboxMetadataConflict(sandbox_id)
        if record is not None and record.workspace_id is not None and delete_workspace:
            raise WorkspaceDeleteProtected(record.workspace_id)
        if record is None:
            # Legacy instances may lack lifecycle rows; Runtime still verifies resource ownership.
            await self._runtime.delete(sandbox_id, delete_workspace=delete_workspace)
            return
        # Claim this exact version as deleting to block renewal before destructive runtime work.
        deleting = await mark_deleting(self._store, record)
        await self._delete_record(deleting, delete_workspace=delete_workspace)

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
        await remove_metadata(self._store, record)

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
                deleting = await mark_deleting(self._store, candidate)
            else:
                return SandboxReapItem(sandbox_id, "skipped", "candidate_changed")
        except (LifecycleStoreConflict, LifecycleRecordNotFound, SandboxMetadataConflict):
            return SandboxReapItem(sandbox_id, "skipped", "candidate_changed")
        except Exception:  # noqa: BLE001
            return SandboxReapItem(sandbox_id, "failed", "sandbox_metadata_unavailable")
        try:
            await self._delete_record(deleting)
        except Exception as error:  # noqa: BLE001
            return SandboxReapItem(sandbox_id, "failed", _reap_failure_reason(error))
        return SandboxReapItem(sandbox_id, "succeeded", "deleted")

    async def inspect_pending_once(self, limit: int = 100) -> SandboxPendingInspectionResult:
        """Read resource evidence for one bounded round of pending records."""
        return await self._maintenance.inspect_pending_once(limit)

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Deliver the verified authenticated Agent entry."""
        return await self._endpoint_access.get_endpoint(sandbox_id)

    async def get_service_endpoint(self, sandbox_id: str, port: int) -> SandboxServiceEndpoint:
        """Deliver a verified local application route with the original credential."""
        return await self._endpoint_access.get_service_endpoint(sandbox_id, port)

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
