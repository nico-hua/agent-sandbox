"""Sandbox identity generation, validation and lifecycle orchestration."""

import asyncio
import hashlib
import hmac
import re
import secrets
from collections.abc import AsyncIterator, Callable, Collection
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Protocol

from control_plane.core.errors import (
    CreationFailureInfo,
    CreationResourceState,
    DockerUnavailable,
    SandboxAgentUnavailable,
    SandboxCleanupFailed,
    SandboxCreateCapacityExceeded,
    SandboxInstanceLimitExceeded,
    SandboxCreateUncertain,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxError,
    SandboxEnvironmentNotRegistered,
    SandboxEndpointUnavailable,
    SandboxEndpointAuthRequired,
    SandboxEndpointAuthUnavailable,
    SandboxServiceUnsupported,
    SandboxMetadataCleanupFailed,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    SandboxNotFound,
    SandboxNotReady,
    SandboxPauseConflict,
    SandboxPauseFailed,
    SandboxQueryFailed,
    SandboxListUnavailable,
    SandboxRenewConflict,
    SandboxResumeConflict,
    SandboxResumeFailed,
    WorkspaceConflict,
    WorkspaceDeleteProtected,
    WorkspaceError,
    WorkspaceNotFound,
    WorkspaceOperationFailed,
    WorkspaceUnavailable,
)
from control_plane.core.models import (
    BUILTIN_ENVIRONMENTS,
    DEFAULT_TTL_SECONDS,
    MAX_CUSTOM_ENVIRONMENTS,
    SandboxCreateOptions,
    SandboxAccessCredential,
    SandboxCreateResult,
    SandboxEndpoint,
    SandboxServiceEndpoint,
    _validate_service_port,
    SandboxLifecycleRecord,
    SandboxListResult,
    validate_sandbox_list_query,
    SandboxPendingInspectionResult,
    SandboxPendingRecoveryItem,
    SandboxPendingRecoveryResult,
    SandboxQueryResult,
    SandboxReadyResult,
    SandboxReapItem,
    SandboxReapResult,
    SandboxRenewResult,
    SandboxResourceInspection,
    SandboxSpec,
    WorkspaceBinding,
    WorkspaceRecord,
    _validate_environment,
    _validate_workspace_id,
    _new_transaction_id,
)
from control_plane.core.tasks import settle_task, wait_for_task
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.storage.lifecycle_store import (
    LifecycleRecordNotFound,
    LifecycleInstanceLimitExceeded,
    LifecycleStore,
    LifecycleStoreConflict,
)


DEFAULT_MAX_CONCURRENT_CREATES = 2
DEFAULT_MAX_INSTANCES = 10


class AgentHealthProbe(Protocol):
    """Check an already verified endpoint without coupling core to an HTTP client."""

    async def check(self, endpoint: str) -> None:
        """Confirm fixed Agent health or raise a sanitized domain failure."""
        ...


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
            or len(names) > len(BUILTIN_ENVIRONMENTS) + MAX_CUSTOM_ENVIRONMENTS
        ):
            raise ValueError("environment_names must be a bounded collection of registered names")
        copied_names = tuple(names)
        for name in copied_names:
            _validate_environment(name)
        if not set(BUILTIN_ENVIRONMENTS).issubset(copied_names):
            raise ValueError("environment_names must include all built-in environments")
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

    async def ping(self) -> None:
        """Check backend availability and propagate its domain errors."""
        await self._runtime.ping()

    def _now(self) -> datetime:
        """Sample the injected clock once and normalize an aware datetime to UTC."""
        value = self._clock()
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise ValueError("lifecycle clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    async def _read_workspace(self, workspace_id: str) -> WorkspaceRecord:
        """Read only registered workspace metadata, rejecting invalid and absent opaque IDs."""
        try:
            _validate_workspace_id(workspace_id)
        except ValueError:
            raise WorkspaceNotFound() from None
        try:
            record = await self._store.get_workspace(workspace_id)
        except Exception as error:
            raise WorkspaceUnavailable(workspace_id) from error
        if record is None:
            raise WorkspaceNotFound(workspace_id)
        return record

    async def create_workspace(self, environment: str = "basic") -> WorkspaceRecord:
        """Create one independently owned volume using a trusted, immutable image selection."""
        _validate_environment(environment)
        if environment not in self._environment_names:
            raise SandboxEnvironmentNotRegistered()
        resolving = asyncio.create_task(self._runtime.resolve_environment_image(environment))
        image_id = await settle_task(resolving, propagate_cancellation=True)
        workspace_id = f"wsp_{secrets.token_hex(16)}"
        record = WorkspaceRecord(
            workspace_id, f"wct_{secrets.token_hex(16)}", f"agent-sandbox-{workspace_id}",
            image_id, self._now(),
        )
        worker = asyncio.create_task(self._create_workspace_record(record))
        # A request cancellation cannot abandon a submitted volume/initializer
        # operation. Preserve its final metadata and data; never undo by deleting it.
        return await settle_task(worker, propagate_cancellation=True)

    async def _create_workspace_record(self, record: WorkspaceRecord) -> WorkspaceRecord:
        """Prewrite, initialize once and confirm; retain uncertain records for diagnosis."""
        try:
            await self._store.create_workspace(record)
        except LifecycleStoreConflict as error:
            raise WorkspaceConflict(record.workspace_id) from error
        except Exception as error:
            raise WorkspaceUnavailable(record.workspace_id) from error
        try:
            await self._runtime.create_workspace(record)
        except WorkspaceError as error:
            error.workspace_id = record.workspace_id
            raise
        except Exception as error:
            raise WorkspaceOperationFailed(record.workspace_id) from error
        try:
            return await self._store.confirm_workspace(
                record.workspace_id, record.creation_token, expected_version=record.version,
            )
        except LifecycleStoreConflict as error:
            raise WorkspaceConflict(record.workspace_id) from error
        except Exception as error:
            raise WorkspaceUnavailable(record.workspace_id) from error

    async def get_workspace(self, workspace_id: str) -> WorkspaceRecord:
        """Combine durable ownership with verified current volume and container-reference facts."""
        record = await self._read_workspace(workspace_id)
        await self._runtime.inspect_workspace(record)
        return record

    async def delete_workspace(self, workspace_id: str) -> None:
        """Settle guarded independent-volume deletion before allowing cancellation to return."""
        try:
            _validate_workspace_id(workspace_id)
        except ValueError:
            raise WorkspaceNotFound() from None
        worker = asyncio.create_task(self._delete_workspace(workspace_id))
        await settle_task(worker, propagate_cancellation=True)

    async def _delete_workspace(self, workspace_id: str) -> None:
        """Claim before inspecting/removing an idle volume, retaining a retry on uncertain removal."""
        try:
            record = await self._store.get_workspace(workspace_id)
            if record is None:
                return
            deleting = await self._store.claim_workspace_deletion(
                workspace_id, record.creation_token, expected_version=record.version,
            )
        except LifecycleStoreConflict as error:
            raise WorkspaceConflict(workspace_id) from error
        except Exception as error:
            raise WorkspaceUnavailable(workspace_id) from error
        try:
            await self._runtime.delete_workspace(deleting)
        except WorkspaceError:
            raise
        except Exception as error:
            raise WorkspaceOperationFailed(workspace_id) from error
        try:
            await self._store.delete_workspace(
                workspace_id, deleting.creation_token, expected_version=deleting.version,
            )
        except LifecycleStoreConflict as error:
            raise WorkspaceConflict(workspace_id) from error
        except Exception as error:
            raise WorkspaceUnavailable(workspace_id) from error

    async def _workspace_binding(self, record: SandboxLifecycleRecord) -> WorkspaceBinding | None:
        """Require the persisted holder, token, immutable volume identity and occupancy version to match."""
        if record.workspace_id is None:
            return None
        try:
            binding = await self._store.get_workspace_binding(record.sandbox_id, record.transaction_id)
            workspace = await self._store.get_workspace(record.workspace_id)
        except LifecycleStoreConflict as error:
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataUnavailable(record.sandbox_id) from error
        if (
            binding is None or workspace is None
            or binding.workspace_id != record.workspace_id
            or binding.sandbox_id != record.sandbox_id or binding.transaction_id != record.transaction_id
            or workspace.sandbox_id != record.sandbox_id or workspace.transaction_id != record.transaction_id
            or workspace.creation_token != binding.creation_token
            or workspace.image_id != binding.image_id or workspace.volume_name != binding.volume_name
            or workspace.version != binding.version or workspace.management_state != "available"
        ):
            raise SandboxMetadataConflict(record.sandbox_id)
        return binding

    async def _verify_observed_workspace(
        self, record: SandboxLifecycleRecord, observed: SandboxQueryResult,
    ) -> WorkspaceBinding | None:
        """Compare actual runtime ownership to durable occupancy rather than only matching a public ID."""
        expected = await self._workspace_binding(record)
        actual = observed.workspace
        if expected is None:
            if actual is not None or observed.workspace_id is not None:
                raise SandboxMetadataConflict(record.sandbox_id)
        elif actual is None or replace(actual, version=expected.version) != expected:
            # Runtime labels carry immutable identity, not mutable database CAS versions.
            raise SandboxMetadataConflict(record.sandbox_id)
        return expected

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
                    binding = await self._prepare_workspace(options.workspace_id, spec)
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

    async def _prepare_workspace(self, workspace_id: str, spec: SandboxSpec) -> WorkspaceBinding:
        """Check a free independent volume and pin compatibility before atomic occupancy/pending insertion."""
        record = await self._read_workspace(workspace_id)
        if record.management_state != "available" or record.sandbox_id is not None:
            raise WorkspaceConflict(workspace_id)
        image = await self._runtime.resolve_environment_image(spec.environment)
        if image != record.image_id:
            raise WorkspaceConflict(workspace_id)
        await self._runtime.inspect_workspace(record)
        return WorkspaceBinding(
            workspace_id, record.creation_token, record.volume_name, record.image_id,
            record.version + 1, spec.sandbox_id, spec.transaction_id,
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
        workspace = await self._workspace_binding(record)
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
        await self._verify_observed_workspace(record, observed)
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
        await self._verify_observed_workspace(record, observed)
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
            workspace = await self._verify_observed_workspace(record, observed)
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
        workspace = await self._workspace_binding(record)
        if workspace is not None and delete_workspace:
            raise WorkspaceDeleteProtected(workspace.workspace_id)
        await self._runtime.delete(
            record.sandbox_id, delete_workspace=delete_workspace,
            expected_transaction_id=record.transaction_id,
            **({"expected_workspace": workspace} if workspace is not None else {}),
        )
        await self._remove_metadata(record)

    async def reap_expired_once(self, limit: int = 100) -> SandboxReapResult:
        """Reap a bounded batch of expired active or retryable deleting records at one time snapshot."""
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        now = self._now()
        items: list[SandboxReapItem] = []
        try:
            candidates = await self._store.list_reap_candidates(now, limit=limit)
        except Exception:
            return SandboxReapResult(now, error="sandbox_metadata_unavailable")
        for candidate in candidates:
            # Each candidate must finish from claim through cleanup. Cancellation
            # stops later candidates, but must settle the current worker's late claim or commit.
            worker = asyncio.create_task(self._reap_candidate(candidate, now))
            try:
                item = await wait_for_task(worker)
            except asyncio.CancelledError as cancellation:
                await settle_task(worker)
                raise cancellation
            items.append(item)
            if item.reason == "sandbox_metadata_unavailable":
                return SandboxReapResult(now, tuple(items), error=item.reason)
        return SandboxReapResult(now, tuple(items))

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
        """Observe one bounded pending batch without recovery, health checks or age assumptions."""
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        items: list[SandboxResourceInspection] = []
        try:
            candidates = await self._store.list_pending(limit=limit)
        except Exception:
            return SandboxPendingInspectionResult(error="sandbox_metadata_unavailable")
        for candidate in candidates:
            worker = asyncio.create_task(self._inspect_pending_candidate(candidate))
            try:
                item = await wait_for_task(worker)
            except asyncio.CancelledError as cancellation:
                # Read-only SDK/SQLite workers still own connections; settle this
                # observation before stopping, rather than abandoning their results.
                await settle_task(worker)
                raise cancellation
            except SandboxMetadataUnavailable:
                return SandboxPendingInspectionResult(
                    tuple(items), error="sandbox_metadata_unavailable",
                )
            items.append(item)
        return SandboxPendingInspectionResult(tuple(items))

    async def _inspect_pending_candidate(
        self, candidate: SandboxLifecycleRecord,
    ) -> SandboxResourceInspection:
        """Inspect a saved identity and discard its conclusion if metadata changed meanwhile."""
        sandbox_id = candidate.sandbox_id
        try:
            workspace = await self._workspace_binding(candidate)
            observed = await self._runtime.inspect_resources(
                sandbox_id, expected_transaction_id=candidate.transaction_id,
                **({"expected_workspace": workspace} if workspace is not None else {}),
            )
        except SandboxMetadataUnavailable:
            raise
        except SandboxMetadataConflict:
            observed = SandboxResourceInspection(sandbox_id, "conflict", "workspace_binding_conflict")
        except DockerUnavailable:
            observed = SandboxResourceInspection(sandbox_id, "unavailable", "docker_unavailable")
        except Exception:
            observed = SandboxResourceInspection(
                sandbox_id, "unavailable", "runtime_inspection_failed",
            )
        try:
            current = await self._store.get(sandbox_id)
        except Exception as error:
            raise SandboxMetadataUnavailable(sandbox_id) from error
        # Neither this reread nor Docker discovery is a cross-system snapshot.
        # Compare the entire immutable record, including original identity/version.
        if current != candidate or current.management_state != "pending":
            return replace(observed, classification="changed", reason="pending_record_changed")
        return observed

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
        credential = await self._credential_for_endpoint(record, endpoint)
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

    async def _credential_for_endpoint(
        self, record: SandboxLifecycleRecord, endpoint: SandboxEndpoint,
    ) -> SandboxAccessCredential:
        """Verify a persisted secret against the actual proxy without regenerating or adopting it."""
        if endpoint.auth_required is not True or endpoint.auth_version != "1":
            raise SandboxEndpointAuthUnavailable()
        if (
            endpoint.sandbox_id != record.sandbox_id
            or endpoint.transaction_id != record.transaction_id
            or endpoint.created_at != record.created_at
        ):
            raise SandboxMetadataConflict(record.sandbox_id)
        try:
            credential = await self._store.get_credential(record.sandbox_id, record.transaction_id)
        except LifecycleStoreConflict as error:
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except Exception as error:
            raise SandboxEndpointAuthUnavailable() from error
        if credential is None:
            raise SandboxEndpointAuthUnavailable()
        if credential.sandbox_id != record.sandbox_id or credential.transaction_id != record.transaction_id:
            raise SandboxMetadataConflict(record.sandbox_id)
        digest = endpoint.credential_digest
        if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
            raise SandboxEndpointAuthUnavailable()
        expected = hashlib.sha256(credential.access_token.encode("ascii")).hexdigest()
        if not hmac.compare_digest(expected, digest):
            raise SandboxEndpointAuthUnavailable()
        return credential

    async def recover_pending_once(self, limit: int = 100) -> SandboxPendingRecoveryResult:
        """Confirm one bounded pending batch without repairing or compensating runtime resources.

        Startup callers must hold exclusive application ownership before admitting requests.
        Explicit callers must likewise exclude live creation; record age proves no such exclusion.
        """
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        try:
            selection = asyncio.create_task(self._store.list_pending(limit=limit))
            candidates = await settle_task(selection, propagate_cancellation=True)
        except Exception:
            return SandboxPendingRecoveryResult(error="sandbox_metadata_unavailable")
        items: list[SandboxPendingRecoveryItem] = []
        for candidate in candidates:
            worker = asyncio.create_task(self._recover_pending_candidate(candidate))
            try:
                item, error = await wait_for_task(worker)
            except asyncio.CancelledError as cancellation:
                # A late activation may commit. Finish only the current candidate,
                # consume its result and leave its actual state intact before cancelling.
                await settle_task(worker)
                raise cancellation
            items.append(item)
            if error is not None:
                return SandboxPendingRecoveryResult(tuple(items), error)
        return SandboxPendingRecoveryResult(tuple(items))

    async def _recover_pending_candidate(
        self, candidate: SandboxLifecycleRecord,
    ) -> tuple[SandboxPendingRecoveryItem, str | None]:
        """Reread the original snapshot and activate its exact unexpired pending version."""
        sandbox_id = candidate.sandbox_id
        observed = await self._check_pending_recovery(candidate)
        backend_error = observed.reason if observed is not None and observed.reason in {
            "docker_unavailable", "sandbox_metadata_unavailable",
        } else None
        try:
            current = await self._store.get(sandbox_id)
        except Exception:
            return SandboxPendingRecoveryItem(
                sandbox_id, "failed", "sandbox_metadata_unavailable",
            ), "sandbox_metadata_unavailable"
        if current != candidate or current.management_state != "pending":
            return SandboxPendingRecoveryItem(
                sandbox_id, "skipped", "pending_record_changed",
            ), backend_error
        if observed is not None:
            return observed, backend_error
        # Sample once per eligible candidate after all runtime reads and the
        # metadata reread; this is a commit-time check, not a cross-system snapshot.
        if candidate.expires_at <= self._now():
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_expired"), None
        try:
            # Recovery intentionally bypasses _activate(), whose new-creation
            # compensation may delete resources. A failed acknowledgement is uncertain.
            await self._store.activate(
                sandbox_id, candidate.transaction_id, expected_version=candidate.version,
            )
        except (LifecycleStoreConflict, LifecycleRecordNotFound):
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "pending_record_changed"), None
        except Exception:
            return SandboxPendingRecoveryItem(
                sandbox_id, "failed", "sandbox_metadata_unavailable",
            ), "sandbox_metadata_unavailable"
        return SandboxPendingRecoveryItem(sandbox_id, "recovered", "recovered"), None

    async def _check_pending_recovery(
        self, candidate: SandboxLifecycleRecord,
    ) -> SandboxPendingRecoveryItem | None:
        """Require complete owned resources, original runtime identity, readiness and verified entry."""
        sandbox_id = candidate.sandbox_id
        try:
            workspace = await self._workspace_binding(candidate)
            inspection = await self._runtime.inspect_resources(
                sandbox_id, expected_transaction_id=candidate.transaction_id,
                **({"expected_workspace": workspace} if workspace is not None else {}),
            )
            if inspection.sandbox_id != sandbox_id:
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_identity_mismatch")
            if inspection.classification != "all_present":
                outcome, reason = {
                    "partial": ("skipped", "resources_partial"),
                    "not_found": ("skipped", "resources_not_found"),
                    "conflict": ("skipped", "resource_ownership_conflict"),
                    "unavailable": ("failed", "docker_unavailable"),
                    "changed": ("skipped", "pending_record_changed"),
                }.get(inspection.classification, ("failed", "sandbox_recovery_failed"))
                return SandboxPendingRecoveryItem(sandbox_id, outcome, reason)
            observed = await self._runtime.get(sandbox_id)
            if (
                observed.sandbox_id != sandbox_id
                or observed.transaction_id != candidate.transaction_id
                or observed.created_at != candidate.created_at
                or observed.workspace_id != candidate.workspace_id
            ):
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_identity_mismatch")
            if observed.status != "ready":
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_not_ready")
            await self._verify_observed_workspace(candidate, observed)
            endpoint = await self._runtime.get_endpoint(sandbox_id)
            if endpoint.sandbox_id != sandbox_id:
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_identity_mismatch")
            try:
                credential = await self._store.get_credential(sandbox_id, candidate.transaction_id)
            except Exception as error:
                raise SandboxMetadataUnavailable(sandbox_id) from error
            # Old pending resources are not upgraded. A new credential or a
            # protected marker requires both sides to agree before activation.
            if endpoint.auth_required or credential is not None:
                await self._credential_for_endpoint(candidate, endpoint)
        except DockerUnavailable:
            return SandboxPendingRecoveryItem(sandbox_id, "failed", "docker_unavailable")
        except SandboxMetadataUnavailable:
            return SandboxPendingRecoveryItem(sandbox_id, "failed", "sandbox_metadata_unavailable")
        except SandboxMetadataConflict:
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "workspace_binding_conflict")
        except SandboxNotFound:
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_not_found")
        except SandboxQueryFailed:
            return SandboxPendingRecoveryItem(sandbox_id, "failed", "sandbox_query_failed")
        except SandboxEndpointUnavailable:
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_endpoint_unavailable")
        except (SandboxEndpointAuthRequired, SandboxEndpointAuthUnavailable):
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_endpoint_auth_unavailable")
        except Exception:
            return SandboxPendingRecoveryItem(sandbox_id, "failed", "sandbox_recovery_failed")
        return None


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
