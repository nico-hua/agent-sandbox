"""Sandbox identity generation, validation and lifecycle orchestration."""

import asyncio
import re
import secrets
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from dataclasses import replace
from typing import TypeVar

from control_plane.core.errors import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxCreateUncertain,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxImageUnavailable,
    SandboxMetadataCleanupFailed,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    SandboxNotFound,
    SandboxQueryFailed,
    SandboxRenewConflict,
    SandboxResourceConflict,
)
from control_plane.core.models import (
    SandboxCreateResult,
    SandboxEndpoint,
    SandboxQueryResult,
    SandboxLifecycleRecord,
    SandboxReapItem,
    SandboxReapResult,
    SandboxRenewResult,
    SandboxSpec,
    _new_transaction_id,
)
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.storage.lifecycle_store import (
    LifecycleStore, LifecycleStoreConflict, LifecycleRecordNotFound,
)


DEFAULT_TTL_SECONDS = 1800
_ROLLED_BACK = (SandboxCreateFailed, SandboxImageUnavailable, SandboxResourceConflict)
_Result = TypeVar("_Result")


async def _settle(
    task: asyncio.Task[_Result], *, propagate_cancellation: bool = False,
) -> _Result:
    """Wait through repeated cancellation and retrieve the final background outcome."""
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.wait({task})
        except asyncio.CancelledError as error:
            cancellation = error
    result = task.result()
    if propagate_cancellation and cancellation is not None:
        raise cancellation
    return result


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
    ) -> None:
        """Store runtime, identity factories and a lazily sampled lifecycle clock."""
        self._runtime = runtime
        self._store = store
        self._id_factory = id_factory if id_factory is not None else _new_sandbox_id
        self._transaction_id_factory = (
            transaction_id_factory
            if transaction_id_factory is not None
            else _new_transaction_id
        )
        self._clock = clock if clock is not None else _utc_now

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
        """Snapshot initial UTC lifecycle times before creating one fixed-TTL sandbox."""
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
        await self._prewrite(record)
        await self._create_runtime(spec, record)
        await self._activate(record)
        return SandboxCreateResult(
            sandbox_id=sandbox_id, status="started",
            created_at=created_at, expires_at=expires_at,
        )

    async def _prewrite(self, record: SandboxLifecycleRecord) -> None:
        """Finish SQLite insertion before creating resources, compensating a cancelled write."""
        worker = asyncio.create_task(self._store.create(record))
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancellation:
            # Cancelling an await does not stop sqlite3 in its worker thread. Never
            # cancel this task: first observe its commit, then compensate our version.
            try:
                await _settle(worker)
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
        """Finish exact transaction/version cleanup without refreshing a changed record."""
        worker = asyncio.create_task(self._store.delete(
            record.sandbox_id, record.transaction_id, expected_version=record.version,
        ))
        try:
            await _settle(worker, propagate_cancellation=True)
        except asyncio.CancelledError as error:
            if not worker.cancelled():
                raise
            raise SandboxMetadataCleanupFailed(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataCleanupFailed(record.sandbox_id) from error

    async def _activate(self, record: SandboxLifecycleRecord) -> None:
        """Confirm creation durably, observing late activation before cancellation cleanup."""
        worker = asyncio.create_task(self._store.activate(
            record.sandbox_id, record.transaction_id, expected_version=record.version,
        ))
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancellation:
            try:
                active = await _settle(worker)
            except LifecycleRecordNotFound:
                await self._rollback_created(record)
                raise cancellation
            except LifecycleStoreConflict as error:
                await self._cleanup_created_runtime(record)
                raise SandboxMetadataConflict(record.sandbox_id) from error
            except (Exception, asyncio.CancelledError) as error:
                # An exception may follow a durable commit. Retain the record and
                # resources rather than compensating an unconfirmed version.
                raise SandboxMetadataUnavailable(record.sandbox_id) from error
            await self._rollback_created(active)
            raise cancellation
        except LifecycleRecordNotFound as error:
            # A manual deletion can finish while Runtime.create is still running.
            # Its missing row must not orphan this transaction's late resources.
            await self._rollback_created(record)
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except LifecycleStoreConflict as error:
            # Another delete may already be removing its metadata. Even when its
            # newer version is untouchable, finish our confirmed runtime creation.
            await self._cleanup_created_runtime(record)
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataUnavailable(record.sandbox_id) from error

    async def _rollback_created(self, record: SandboxLifecycleRecord) -> None:
        """Compensate a cancelled committed creation using its confirmed transaction/version."""
        cleanup = asyncio.create_task(self._finish_creation_rollback(record))
        await _settle(cleanup)

    async def _finish_creation_rollback(self, record: SandboxLifecycleRecord) -> None:
        """Persist deletion intent before compensating committed or late-created resources."""
        try:
            deleting = await self._mark_deleting(record, allow_absent=True)
        except SandboxMetadataConflict:
            # A concurrent deletion can consume a newer row after its runtime step.
            # Clean our late resources without refreshing or removing that row.
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
            await _settle(cleanup)
        except (Exception, asyncio.CancelledError) as error:
            raise SandboxCleanupFailed(record.sandbox_id) from error

    async def _create_runtime(
        self, spec: SandboxSpec, record: SandboxLifecycleRecord,
    ) -> None:
        """Create resources and compensate metadata only after a confirmed runtime rollback."""
        worker = asyncio.create_task(self._runtime.create(spec))
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancellation:
            worker.cancel()
            try:
                await _settle(worker)
            except (asyncio.CancelledError, *_ROLLED_BACK):
                # Runtime's cancellation contract confirms resource rollback before
                # returning CancelledError; incomplete cleanup has its own error.
                await self._remove_metadata(record)
                raise cancellation
            except SandboxCleanupFailed:
                raise
            except Exception as error:
                raise SandboxCreateUncertain(record.sandbox_id) from error
            # Creation may have committed just before cancellation, or a runtime
            # may finish despite cancellation. Delete only this transaction's resources.
            await self._rollback_created(record)
            raise cancellation
        except _ROLLED_BACK:
            await self._remove_metadata(record)
            raise
        except SandboxCleanupFailed:
            raise
        except Exception as error:
            raise SandboxCreateUncertain(record.sandbox_id) from error

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Validate a public ID and resolve its state through the runtime."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        observed = await self._runtime.get(sandbox_id)
        try:
            record = await self._store.get(sandbox_id)
        except Exception as error:
            raise SandboxMetadataUnavailable(sandbox_id) from error
        if record is None:
            return observed
        if (
            record.sandbox_id != sandbox_id
            or observed.sandbox_id != sandbox_id
            or record.transaction_id != observed.transaction_id
            or record.created_at != observed.created_at
        ):
            raise SandboxMetadataConflict(sandbox_id)
        return replace(observed, expires_at=record.expires_at)

    async def renew(self, sandbox_id: str, ttl_seconds: int) -> SandboxRenewResult:
        """Extend an actual active, unexpired instance from one UTC validation snapshot."""
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
        expires_at = max(record.expires_at, now + timedelta(seconds=ttl_seconds))
        # Even a renewal that leaves the expiration unchanged needs a guarded
        # commit: a preceding deletion claim must still make this request fail.
        updated = await self._commit_renewal(record, expires_at)
        return SandboxRenewResult(sandbox_id, updated.expires_at)

    async def _commit_renewal(
        self, record: SandboxLifecycleRecord, expires_at: datetime,
    ) -> SandboxLifecycleRecord:
        """Settle a version-guarded write through repeated cancellation without undoing it."""
        worker = asyncio.create_task(self._store.update_expiration(
            record.sandbox_id, record.transaction_id,
            expected_version=record.version, expires_at=expires_at,
        ))
        cancellation: asyncio.CancelledError | None = None
        try:
            try:
                updated = await asyncio.shield(worker)
            except asyncio.CancelledError as error:
                cancellation = error
                # Cancellation cannot stop sqlite3. A late commit is authoritative
                # and must never be reversed over a subsequent metadata update.
                updated = await _settle(worker)
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
        """Validate a public ID and reclaim resources with an explicit data policy."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        worker = asyncio.create_task(self._delete(sandbox_id, delete_workspace))
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancellation:
            # Finish runtime deletion and metadata removal as one observable unit.
            # A cancellation response must not strand a late metadata write.
            await _settle(worker)
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
            await self._runtime.delete(sandbox_id, delete_workspace=delete_workspace)
            return
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
            # The original identity/version still protects any replacement metadata.
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
        """Delete only a confirmed transaction and then remove exactly its claimed version."""
        await self._runtime.delete(
            record.sandbox_id, delete_workspace=delete_workspace,
            expected_transaction_id=record.transaction_id,
        )
        await self._remove_metadata(record)

    async def reap_expired_once(self, limit: int = 100) -> SandboxReapResult:
        """Reclaim one bounded batch of expired active or retryable deleting records."""
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        now = self._now()
        items: list[SandboxReapItem] = []
        try:
            candidates = await self._store.list_reap_candidates(now, limit=limit)
        except Exception:
            return SandboxReapResult(now, error="sandbox_metadata_unavailable")
        for candidate in candidates:
            # Each candidate is one indivisible finish unit. Cancellation stops
            # later candidates, but must not abandon a thread's late claim/commit.
            worker = asyncio.create_task(self._reap_candidate(candidate, now))
            try:
                item = await asyncio.shield(worker)
            except asyncio.CancelledError as cancellation:
                await _settle(worker)
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

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Validate a public ID and resolve its local entry through the runtime."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        return await self._runtime.get_endpoint(sandbox_id)


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
