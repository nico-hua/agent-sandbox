"""Explicit bounded reclamation against temporary SQLite and isolated runtime resources."""

import asyncio
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from control_plane.core.errors import SandboxDeleteFailed, SandboxMetadataUnavailable
from control_plane.core.models import SandboxLifecycleRecord
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore
from test_sandbox_service import FakeRuntime


NOW = datetime(2026, 10, 19, 8, 30, 0, 123456, tzinfo=UTC)


class ReapStore(SQLiteLifecycleStore):
    """Keep real SQLite guards while injecting candidate races and transient failures."""

    def __init__(self, database: Path) -> None:
        """Initialize controlled access failures without opening a database connection."""
        super().__init__(database)
        self.fail_candidates = False
        self.fail_claim = False
        self.fail_mark = False
        self.fail_deletion: set[str] = set()
        self.after_candidates = None
        self.claims: list[str] = []
        self.block_transition: str | None = None
        self.fail_after_transition = False
        self.transition_entered = threading.Event()
        self.transition_release = threading.Event()

    def _wait_for_transition(self, phase: str) -> None:
        """Pause a selected real SQLite worker until cancellation has been requested."""
        if self.block_transition == phase:
            self.transition_entered.set()
            assert self.transition_release.wait(timeout=3), "state transition gate was not released"

    def _claim_expired_sync(
        self, sandbox_id: str, transaction_id: str, expected_version: int, now_us: int,
    ):
        """Gate real claiming and optionally lose its acknowledgement after SQLite commits."""
        self._wait_for_transition("claim")
        result = super()._claim_expired_sync(
            sandbox_id, transaction_id, expected_version, now_us,
        )
        if self.block_transition == "claim" and self.fail_after_transition:
            raise LifecycleStoreError()
        return result

    def _mark_deleting_sync(
        self, sandbox_id: str, transaction_id: str, expected_version: int,
    ):
        """Gate a guarded deletion retry while leaving its committed identity recoverable."""
        self._wait_for_transition("mark")
        result = super()._mark_deleting_sync(sandbox_id, transaction_id, expected_version)
        if self.block_transition == "mark" and self.fail_after_transition:
            raise LifecycleStoreError()
        return result

    async def list_reap_candidates(self, now: datetime, *, limit: int = 100):
        """Return real bounded candidates and optionally race a competing operation."""
        if self.fail_candidates:
            raise LifecycleStoreError()
        candidates = await super().list_reap_candidates(now, limit=limit)
        if self.after_candidates is not None:
            callback, self.after_candidates = self.after_candidates, None
            await callback(candidates)
        return candidates

    async def claim_expired(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
        now: datetime,
    ):
        """Record atomic claims and fail before any runtime call when storage is down."""
        self.claims.append(sandbox_id)
        if self.fail_claim:
            raise LifecycleStoreError()
        return await super().claim_expired(
            sandbox_id, transaction_id, expected_version=expected_version, now=now,
        )

    async def mark_deleting(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ):
        """Preserve retry identity checks even when an already-deleting record is selected."""
        if self.fail_mark:
            raise LifecycleStoreError()
        return await super().mark_deleting(
            sandbox_id, transaction_id, expected_version=expected_version,
        )

    async def delete(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> None:
        """Fail one selected metadata cleanup while retaining the actual deleting row."""
        if sandbox_id in self.fail_deletion:
            self.fail_deletion.remove(sandbox_id)
            raise LifecycleStoreError()
        await super().delete(sandbox_id, transaction_id, expected_version=expected_version)


class ReapRuntime(FakeRuntime):
    """Expose only precise deletion effects and gates without connecting to Docker."""

    def __init__(self) -> None:
        """Initialize per-instance failures and a reusable deletion synchronization gate."""
        super().__init__()
        self.expected_deletions: list[tuple[str, bool, str | None]] = []
        self.failures: set[str] = set()
        self.deleted: set[str] = set()
        self.block_delete = False
        self.delete_entered = asyncio.Event()
        self.delete_release = asyncio.Event()

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Complete idempotent deletion only after the test allows its critical section."""
        self.expected_deletions.append((sandbox_id, delete_workspace, expected_transaction_id))
        self.delete_entered.set()
        if self.block_delete:
            await self.delete_release.wait()
        if sandbox_id in self.failures:
            raise SandboxDeleteFailed(sandbox_id)
        self.deleted.add(sandbox_id)


@pytest.fixture
async def store(tmp_path: Path) -> ReapStore:
    """Initialize a fresh per-test file database and leave development storage untouched."""
    result = ReapStore(tmp_path / "lifecycle.sqlite3")
    await result.initialize()
    return result


async def add_record(
    store: SQLiteLifecycleStore, number: int, *, state: str = "active",
    expires_at: datetime = NOW,
) -> SandboxLifecycleRecord:
    """Persist one deterministic identity and use real guarded state transitions."""
    record = SandboxLifecycleRecord(
        sandbox_id=f"sbx_{number:032x}", transaction_id=f"txn_{number:032x}",
        created_at=NOW - timedelta(hours=1), expires_at=expires_at,
    )
    await store.create(record)
    if state in ("active", "deleting"):
        record = await store.activate(
            record.sandbox_id, record.transaction_id, expected_version=record.version,
        )
    if state == "deleting":
        record = await store.mark_deleting(
            record.sandbox_id, record.transaction_id, expected_version=record.version,
        )
    return record


def service_for(runtime: FakeRuntime, store: SQLiteLifecycleStore) -> SandboxService:
    """Inject a fixed UTC instant into a real Service with no background work."""
    return SandboxService(runtime, store, clock=lambda: NOW)


@pytest.mark.anyio
async def test_pending_and_future_active_are_not_reclaimed(store: ReapStore) -> None:
    """Only a completed and expired creation may be selected for its first reclamation."""
    pending = await add_record(store, 1, state="pending", expires_at=NOW - timedelta(seconds=1))
    active = await add_record(store, 2)
    future = await add_record(store, 3, expires_at=NOW + timedelta(microseconds=1))
    runtime = ReapRuntime()

    result = await service_for(runtime, store).reap_expired_once()

    assert result.now == NOW
    assert result.succeeded == 1 and result.skipped == result.failed == 0
    assert result.error is None
    assert [(item.sandbox_id, item.outcome, item.reason) for item in result.items] == [
        (active.sandbox_id, "succeeded", "deleted"),
    ]
    assert runtime.expected_deletions == [(active.sandbox_id, False, active.transaction_id)]
    assert await store.get(active.sandbox_id) is None
    assert await store.get(pending.sandbox_id) == pending
    assert await store.get(future.sandbox_id) == future


@pytest.mark.anyio
async def test_reclamation_reads_clock_once_and_preserves_microsecond_boundary(
    store: ReapStore,
) -> None:
    """One aware clock snapshot determines all exact expiry comparisons in the batch."""
    earlier = await add_record(store, 1, expires_at=NOW - timedelta(microseconds=1))
    equal = await add_record(store, 2, expires_at=NOW)
    later = await add_record(store, 3, expires_at=NOW + timedelta(microseconds=1))
    runtime = ReapRuntime()
    calls = 0

    def clock() -> datetime:
        """Return Shanghai's representation of one instant and reject a second sampling."""
        nonlocal calls
        calls += 1
        assert calls == 1
        return NOW.astimezone(timezone(timedelta(hours=8)))

    result = await SandboxService(runtime, store, clock=clock).reap_expired_once()

    assert calls == 1 and result.now.tzinfo is UTC
    assert [item.sandbox_id for item in result.items] == [earlier.sandbox_id, equal.sandbox_id]
    assert await store.get(later.sandbox_id) == later


@pytest.mark.anyio
async def test_candidates_have_stable_order_and_bounded_batch(store: ReapStore) -> None:
    """A bounded batch is ordered by exact expiration and then sandbox identity."""
    third = await add_record(store, 3)
    second = await add_record(store, 2)
    first = await add_record(store, 1, expires_at=NOW - timedelta(microseconds=1))
    runtime = ReapRuntime()

    result = await service_for(runtime, store).reap_expired_once(limit=2)

    assert [item.sandbox_id for item in result.items] == [first.sandbox_id, second.sandbox_id]
    assert result.succeeded == 2
    assert len(runtime.expected_deletions) == 2
    assert await store.get(third.sandbox_id) == third


@pytest.mark.anyio
@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
async def test_invalid_batch_limit_does_not_delete(store: ReapStore, limit) -> None:
    """Invalid batch bounds must fail before any destructive runtime operation."""
    record = await add_record(store, 1)
    runtime = ReapRuntime()

    with pytest.raises(ValueError):
        await service_for(runtime, store).reap_expired_once(limit=limit)

    assert runtime.expected_deletions == []
    assert await store.get(record.sandbox_id) == record


@pytest.mark.anyio
@pytest.mark.parametrize("race", ["renewed", "deleted", "replaced"])
async def test_stale_active_candidate_is_skipped_without_runtime_deletion(
    store: ReapStore, race: str,
) -> None:
    """Renewed, absent or replaced metadata cannot be adopted using an old candidate."""
    original = await add_record(store, 1)
    replacement = None

    async def change_candidate(candidates) -> None:
        """Commit a competing change after selection but before atomic deletion claiming."""
        nonlocal replacement
        assert candidates == [original]
        if race == "renewed":
            replacement = await store.update_expiration(
                original.sandbox_id, original.transaction_id, expected_version=original.version,
                expires_at=NOW + timedelta(seconds=1),
            )
        else:
            await store.delete(
                original.sandbox_id, original.transaction_id, expected_version=original.version,
            )
            if race == "replaced":
                replacement = replace(
                    original, transaction_id="txn_ffffffffffffffffffffffffffffffff",
                    management_state="pending", version=0,
                )
                await store.create(replacement)

    store.after_candidates = change_candidate
    runtime = ReapRuntime()

    result = await service_for(runtime, store).reap_expired_once()

    assert result.skipped == 1 and result.succeeded == result.failed == 0
    assert result.items[0].reason == "candidate_changed"
    assert runtime.expected_deletions == []
    assert await store.get(original.sandbox_id) == replacement


@pytest.mark.anyio
async def test_deleting_retry_is_selected_even_if_expiration_is_in_future(store: ReapStore) -> None:
    """A previously confirmed deletion remains retryable independently of the current clock."""
    deleting = await add_record(store, 1, state="deleting", expires_at=NOW + timedelta(hours=1))
    runtime = ReapRuntime()

    result = await service_for(runtime, store).reap_expired_once()
    repeated = await service_for(runtime, store).reap_expired_once()

    assert result.succeeded == 1 and repeated.items == ()
    assert await store.get(deleting.sandbox_id) is None
    assert runtime.expected_deletions == [(deleting.sandbox_id, False, deleting.transaction_id)]


@pytest.mark.anyio
async def test_replaced_deleting_candidate_cannot_be_adopted(store: ReapStore) -> None:
    """Retrying deletion must retain the selected transaction rather than reread by ID."""
    original = await add_record(store, 1, state="deleting")
    replacement = replace(
        original, transaction_id="txn_ffffffffffffffffffffffffffffffff",
        management_state="pending", version=0,
    )

    async def replace_candidate(candidates) -> None:
        """Replace one selected deleting identity before its guarded retry begins."""
        assert candidates == [original]
        await store.delete(
            original.sandbox_id, original.transaction_id, expected_version=original.version,
        )
        await store.create(replacement)

    store.after_candidates = replace_candidate
    runtime = ReapRuntime()

    result = await service_for(runtime, store).reap_expired_once()

    assert result.skipped == 1 and result.items[0].reason == "candidate_changed"
    assert runtime.expected_deletions == []
    assert await store.get(original.sandbox_id) == replacement


@pytest.mark.anyio
async def test_runtime_failure_retains_deleting_and_continues_remaining_candidates(
    store: ReapStore,
) -> None:
    """A partial resource failure is isolated and can finish in a later explicit round."""
    first = await add_record(store, 1)
    second = await add_record(store, 2)
    runtime = ReapRuntime()
    runtime.failures.add(first.sandbox_id)
    service = service_for(runtime, store)

    result = await service.reap_expired_once()

    assert result.failed == result.succeeded == 1 and result.skipped == 0
    assert result.items[0].reason == "sandbox_delete_failed"
    assert result.items[1].sandbox_id == second.sandbox_id
    retained = await store.get(first.sandbox_id)
    assert retained.management_state == "deleting" and retained.version == first.version + 1
    assert await store.get(second.sandbox_id) is None
    runtime.failures.clear()
    retry = await service.reap_expired_once()
    assert retry.succeeded == 1
    assert await store.get(first.sandbox_id) is None
    assert len(runtime.expected_deletions) == 3


@pytest.mark.anyio
async def test_cleanup_failure_keeps_deleting_after_runtime_success_and_retries(
    store: ReapStore,
) -> None:
    """A late metadata failure preserves a retry identity after runtime resources are gone."""
    first = await add_record(store, 1)
    second = await add_record(store, 2)
    store.fail_deletion.add(first.sandbox_id)
    runtime = ReapRuntime()
    service = service_for(runtime, store)

    result = await service.reap_expired_once()

    assert result.failed == result.succeeded == 1 and result.error is None
    assert result.items[0].reason == "sandbox_metadata_cleanup_failed"
    assert runtime.deleted == {first.sandbox_id, second.sandbox_id}
    retained = await store.get(first.sandbox_id)
    assert retained.management_state == "deleting" and retained.version == first.version + 1
    retry = await service.reap_expired_once()
    assert retry.succeeded == 1
    assert await store.get(first.sandbox_id) is None


@pytest.mark.anyio
async def test_database_outage_after_cleanup_failure_stops_before_next_runtime_call(
    store: ReapStore, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A batch must stop once the next atomic claim detects an ongoing database outage."""
    first = await add_record(store, 1)
    second = await add_record(store, 2)
    runtime = ReapRuntime()

    async def fail_cleanup(
        sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> None:
        """Model a database outage beginning after the first runtime deletion succeeds."""
        store.fail_claim = True
        raise LifecycleStoreError()

    monkeypatch.setattr(store, "delete", fail_cleanup)

    result = await service_for(runtime, store).reap_expired_once()

    assert result.error == "sandbox_metadata_unavailable"
    assert result.failed == 2 and result.succeeded == 0
    assert [item.reason for item in result.items] == [
        "sandbox_metadata_cleanup_failed", "sandbox_metadata_unavailable",
    ]
    assert runtime.expected_deletions == [(first.sandbox_id, False, first.transaction_id)]
    assert (await store.get(first.sandbox_id)).management_state == "deleting"
    assert await store.get(second.sandbox_id) == second


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["candidates", "claim", "mark"])
async def test_storage_unavailable_stops_batch_before_runtime_deletion(
    store: ReapStore, stage: str,
) -> None:
    """An unavailable database cannot authorize deletion or cause a blind runtime fallback."""
    first = await add_record(store, 1, state="deleting" if stage == "mark" else "active")
    second = await add_record(store, 2)
    setattr(store, f"fail_{stage}", True)
    runtime = ReapRuntime()

    result = await service_for(runtime, store).reap_expired_once()

    assert result.error == "sandbox_metadata_unavailable"
    assert result.succeeded == 0
    assert runtime.expected_deletions == []
    assert await store.get(first.sandbox_id) == first
    assert await store.get(second.sandbox_id) == second


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["pending", "active", "deleting"])
async def test_manual_delete_accepts_all_management_states(store: ReapStore, state: str) -> None:
    """Manual cleanup can recover pending, completed and already-claimed transactions."""
    record = await add_record(store, 1, state=state)
    runtime = ReapRuntime()
    service = service_for(runtime, store)

    await service.delete(record.sandbox_id)

    assert runtime.expected_deletions == [(record.sandbox_id, False, record.transaction_id)]
    assert await store.get(record.sandbox_id) is None
    await service.delete(record.sandbox_id)
    assert runtime.expected_deletions[-1] == (record.sandbox_id, False, None)
    assert runtime.queries == []


@pytest.mark.anyio
async def test_repeated_cancellation_finishes_current_delete_and_stops_next_candidate(
    store: ReapStore,
) -> None:
    """Cancellation waits for in-flight cleanup and never starts a later candidate."""
    first = await add_record(store, 1)
    second = await add_record(store, 2)
    runtime = ReapRuntime()
    runtime.block_delete = True
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    unhandled: list[dict] = []

    def capture_exception(event_loop, context) -> None:
        """Collect background task failures that would otherwise be reported as unhandled."""
        unhandled.append(context)

    loop.set_exception_handler(capture_exception)
    task = asyncio.create_task(service_for(runtime, store).reap_expired_once())
    try:
        await runtime.delete_entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        retained = await store.get(first.sandbox_id)
        assert retained.management_state == "deleting"
        runtime.delete_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await asyncio.sleep(0)
        assert runtime.expected_deletions == [(first.sandbox_id, False, first.transaction_id)]
        assert await store.get(first.sandbox_id) is None
        assert await store.get(second.sandbox_id) == second
        assert unhandled == []
    finally:
        runtime.delete_release.set()
        loop.set_exception_handler(previous_handler)


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["claim", "mark"])
@pytest.mark.parametrize("lost_acknowledgement", [False, True])
async def test_cancellation_during_sqlite_claim_waits_for_late_commit_and_stops_batch(
    store: ReapStore, phase: str, lost_acknowledgement: bool,
) -> None:
    """Repeated cancellation cannot abandon a state write or process a later candidate."""
    first = await add_record(store, 1, state="deleting" if phase == "mark" else "active")
    second = await add_record(store, 2)
    store.block_transition = phase
    store.fail_after_transition = lost_acknowledgement
    runtime = ReapRuntime()
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    unhandled: list[dict] = []

    def capture_exception(event_loop, context) -> None:
        """Observe any detached task exception produced by a late committed worker."""
        unhandled.append(context)

    loop.set_exception_handler(capture_exception)
    task = asyncio.create_task(service_for(runtime, store).reap_expired_once())
    try:
        assert await asyncio.to_thread(store.transition_entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert runtime.expected_deletions == []
        store.transition_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        retained = await store.get(first.sandbox_id)
        if lost_acknowledgement:
            assert retained is not None and retained.management_state == "deleting"
            assert retained.version == first.version + (phase == "claim")
            assert runtime.expected_deletions == []
            store.block_transition = None
            retried = await service_for(runtime, store).reap_expired_once(limit=1)
            assert retried.succeeded == 1
        else:
            assert retained is None
            assert runtime.expected_deletions == [(first.sandbox_id, False, first.transaction_id)]
        assert await store.get(second.sandbox_id) == second
        await asyncio.sleep(0)
        assert unhandled == []
    finally:
        store.transition_release.set()
        loop.set_exception_handler(previous_handler)


@pytest.mark.anyio
async def test_manual_delete_claim_failure_never_calls_runtime(store: ReapStore) -> None:
    """Manual deletion also requires a confirmed management-state claim before Docker work."""
    record = await add_record(store, 1)
    store.fail_mark = True
    runtime = ReapRuntime()

    with pytest.raises(SandboxMetadataUnavailable):
        await service_for(runtime, store).delete(record.sandbox_id)

    assert runtime.expected_deletions == []
    assert await store.get(record.sandbox_id) == record
