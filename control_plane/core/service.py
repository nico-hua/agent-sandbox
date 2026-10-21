"""Sandbox identity generation, validation and lifecycle orchestration."""

import asyncio
import re
import secrets
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Protocol

from control_plane.core.errors import (
    CreationFailureInfo,
    CreationResourceState,
    DockerUnavailable,
    SandboxAgentUnavailable,
    SandboxCleanupFailed,
    SandboxCreateUncertain,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxError,
    SandboxEndpointUnavailable,
    SandboxMetadataCleanupFailed,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    SandboxNotFound,
    SandboxNotReady,
    SandboxQueryFailed,
    SandboxRenewConflict,
)
from control_plane.core.models import (
    SandboxCreateResult,
    SandboxEndpoint,
    SandboxLifecycleRecord,
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
    _new_transaction_id,
)
from control_plane.core.tasks import settle_task, wait_for_task
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.storage.lifecycle_store import (
    LifecycleRecordNotFound,
    LifecycleStore,
    LifecycleStoreConflict,
)

DEFAULT_TTL_SECONDS = 1800


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
    ) -> None:
        """Store backend, metadata, optional Agent probe, identity factories and lifecycle clock."""
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

    async def create(self) -> SandboxCreateResult:
        """Create with a fixed TTL; started confirms startup, while queries determine readiness."""
        created_at = self._now()
        expires_at = created_at + timedelta(seconds=DEFAULT_TTL_SECONDS)
        sandbox_id = self._id_factory()
        spec = SandboxSpec(
            sandbox_id=sandbox_id,
            container_name=f"agent-sandbox-{sandbox_id}",
            network_name=f"agent-sandbox-{sandbox_id}-internal",
            volume_name=f"agent-sandbox-{sandbox_id}-workspace",
            transaction_id=self._transaction_id_factory(),
            created_at=created_at,
            expires_at=expires_at,
        )
        record = SandboxLifecycleRecord(
            sandbox_id, spec.transaction_id, created_at, expires_at,
        )
        # Persist pending for recovery before creating resources and confirming active.
        # The management state active does not imply the Agent health state ready.
        await self._prewrite(record)
        await self._create_runtime(spec, record)
        await self._activate(record)
        return SandboxCreateResult(
            sandbox_id=sandbox_id, status="started",
            created_at=created_at, expires_at=expires_at,
        )

    async def _prewrite(self, record: SandboxLifecycleRecord) -> None:
        """Finish the pending write before resource creation, settling and compensating cancellation."""
        worker = asyncio.create_task(self._store.create(record))
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            # Cancelling an await cannot stop SQLite in its worker thread. Settle
            # the write, then compensate only our original transaction and version.
            try:
                await settle_task(worker)
            except LifecycleStoreConflict:
                raise cancellation
            except (Exception, asyncio.CancelledError) as error:
                raise SandboxMetadataUnavailable(record.sandbox_id) from error
            await self._remove_metadata(record)
            raise cancellation
        except LifecycleStoreConflict as error:
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
        """Persist deletion intent before compensating committed or late-created resources."""
        try:
            deleting = await self._mark_deleting(record, allow_absent=True)
        except SandboxMetadataConflict:
            # Concurrent deletion may still consume its newer row after runtime cleanup.
            # Reclaim only our late resources without adopting that metadata version.
            await self._cleanup_created_runtime(record)
            raise
        await self._cleanup_created_runtime(record)
        await self._remove_metadata(deleting)

    async def _cleanup_created_runtime(self, record: SandboxLifecycleRecord) -> None:
        """Finish exact-transaction resource compensation independently of metadata rights."""
        cleanup = asyncio.create_task(self._runtime.delete(
            record.sandbox_id, delete_workspace=True,
            expected_transaction_id=record.transaction_id,
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
            return observed
        # Docker labels retain initial times; the store holds renewed expiration.
        # Match the transaction and created_at before combining resources with metadata.
        if (
            record.sandbox_id != sandbox_id
            or observed.sandbox_id != sandbox_id
            or record.transaction_id != observed.transaction_id
            or record.created_at != observed.created_at
        ):
            raise SandboxMetadataConflict(sandbox_id)
        return replace(observed, expires_at=record.expires_at)

    async def check_ready(self, sandbox_id: str) -> SandboxReadyResult:
        """Verify runtime and metadata, then check the Agent through its verified local entry."""
        observed = await self.get(sandbox_id)
        if observed.sandbox_id != sandbox_id:
            raise SandboxQueryFailed()
        if observed.status != "ready":
            raise SandboxNotReady()
        try:
            endpoint = await self.get_endpoint(sandbox_id)
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
        ):
            raise SandboxMetadataConflict(sandbox_id)
        if record.management_state != "active" or record.expires_at <= now:
            raise SandboxRenewConflict(sandbox_id)
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
        """Read metadata and enter guarded deletion without requiring a complete runtime."""
        try:
            record = await self._store.get(sandbox_id)
        except Exception as error:
            raise SandboxMetadataUnavailable(sandbox_id) from error
        if record is not None and record.sandbox_id != sandbox_id:
            raise SandboxMetadataConflict(sandbox_id)
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
        await self._runtime.delete(
            record.sandbox_id, delete_workspace=delete_workspace,
            expected_transaction_id=record.transaction_id,
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
            observed = await self._runtime.inspect_resources(
                sandbox_id, expected_transaction_id=candidate.transaction_id,
            )
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
        """Validate the public ID and read a verified local entry without implying Agent readiness."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        return await self._runtime.get_endpoint(sandbox_id)

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
        backend_error = (
            "docker_unavailable" if observed is not None and observed.reason == "docker_unavailable"
            else None
        )
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
            inspection = await self._runtime.inspect_resources(
                sandbox_id, expected_transaction_id=candidate.transaction_id,
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
            ):
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_identity_mismatch")
            if observed.status != "ready":
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_not_ready")
            endpoint = await self._runtime.get_endpoint(sandbox_id)
            if endpoint.sandbox_id != sandbox_id:
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_identity_mismatch")
        except DockerUnavailable:
            return SandboxPendingRecoveryItem(sandbox_id, "failed", "docker_unavailable")
        except SandboxNotFound:
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_not_found")
        except SandboxQueryFailed:
            return SandboxPendingRecoveryItem(sandbox_id, "failed", "sandbox_query_failed")
        except SandboxEndpointUnavailable:
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_endpoint_unavailable")
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
