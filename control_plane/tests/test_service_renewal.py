"""Renewal semantics and races against independent temporary SQLite connections."""

import asyncio
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from control_plane.core.models import SandboxLifecycleRecord
from control_plane.core.errors import (
    DockerUnavailable, SandboxMetadataConflict, SandboxMetadataUnavailable,
    SandboxNotFound, SandboxQueryFailed, SandboxRenewConflict,
)
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import (
    LifecycleStoreError, SQLiteLifecycleStore,
)
from test_sandbox_service import FakeRuntime, FIXED_SANDBOX_ID, FIXED_TRANSACTION_ID


NOW = datetime(2026, 10, 19, 8, 30, 0, 123456, tzinfo=UTC)


class RenewalStore(SQLiteLifecycleStore):
    """Coordinate real worker commits and concurrent connections without replacing SQLite."""

    def __init__(self, database: Path) -> None:
        """Initialize optional update gates and stable failure injection without opening SQLite."""
        super().__init__(database)
        self.get_calls: list[str] = []
        self.updates: list[tuple[str, str, int, datetime]] = []
        self.fail_get = False
        self.fail_before_update = False
        self.fail_after_update = False
        self.block_update = False
        self.block_after_update = False
        self.update_entered = threading.Event()
        self.update_release = threading.Event()
        self.commit_entered = threading.Event()
        self.commit_release = threading.Event()
        self.concurrent_updates = 0
        self.all_updates_entered = asyncio.Event()

    async def get(self, sandbox_id: str):
        """Track reads and optionally fail before any update can be submitted."""
        self.get_calls.append(sandbox_id)
        if self.fail_get:
            raise LifecycleStoreError()
        return await super().get(sandbox_id)

    async def update_expiration(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
        expires_at: datetime,
    ):
        """Submit real independent SQLite writes once competing renewals share a snapshot."""
        self.updates.append((sandbox_id, transaction_id, expected_version, expires_at))
        if self.concurrent_updates:
            if len(self.updates) >= self.concurrent_updates:
                self.all_updates_entered.set()
            await self.all_updates_entered.wait()
        return await super().update_expiration(
            sandbox_id, transaction_id,
            expected_version=expected_version, expires_at=expires_at,
        )

    def _update_sync(
        self, sandbox_id: str, transaction_id: str, expected_version: int,
        expires_at: datetime,
    ):
        """Pause before a true database transaction and optionally lose its acknowledgement."""
        if self.block_update:
            self.update_entered.set()
            assert self.update_release.wait(timeout=3), "renewal update gate was not released"
        if self.fail_before_update:
            raise LifecycleStoreError()
        record = super()._update_sync(sandbox_id, transaction_id, expected_version, expires_at)
        if self.block_after_update:
            self.commit_entered.set()
            assert self.commit_release.wait(timeout=3), "renewal commit gate was not released"
        if self.fail_after_update:
            raise LifecycleStoreError()
        return record


@pytest.fixture
async def store(tmp_path: Path) -> RenewalStore:
    """Initialize an isolated file database shared only by the test's store connections."""
    result = RenewalStore(tmp_path / "lifecycle.sqlite3")
    await result.initialize()
    return result


async def active_record(
    store: SQLiteLifecycleStore, *, state: str = "active",
    expires_at: datetime = NOW + timedelta(seconds=30),
) -> SandboxLifecycleRecord:
    """Persist one deterministic creation through the actual guarded state transitions."""
    record = SandboxLifecycleRecord(
        FIXED_SANDBOX_ID, FIXED_TRANSACTION_ID,
        NOW - timedelta(minutes=30), expires_at,
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


def runtime_for(record: SandboxLifecycleRecord) -> FakeRuntime:
    """Expose a matching backend identity and its immutable initial lifecycle labels."""
    runtime = FakeRuntime()
    runtime.query_result = replace(
        runtime.query_result, sandbox_id=record.sandbox_id,
        transaction_id=record.transaction_id, created_at=record.created_at,
        expires_at=record.expires_at,
    )
    return runtime


def service_for(runtime: FakeRuntime, store: SQLiteLifecycleStore) -> SandboxService:
    """Use a fixed expiry-validation instant without starting background work."""
    return SandboxService(runtime, store, clock=lambda: NOW)


@pytest.mark.anyio
async def test_renewal_persists_current_expiration_without_changing_runtime_labels(
    tmp_path: Path,
) -> None:
    """A committed renewal survives a fresh Store and Service without rewriting backend facts."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    await store.initialize()
    original = await active_record(store)
    runtime = FakeRuntime()
    runtime.query_result = replace(
        runtime.query_result, sandbox_id=original.sandbox_id,
        transaction_id=original.transaction_id, created_at=original.created_at,
        expires_at=original.expires_at,
    )
    service = SandboxService(runtime, store, clock=lambda: NOW)

    renewed = await service.renew(original.sandbox_id, 1800)

    assert renewed.sandbox_id == original.sandbox_id
    assert renewed.expires_at == NOW + timedelta(seconds=1800)
    recovered_store = SQLiteLifecycleStore(database)
    recovered = await recovered_store.get(original.sandbox_id)
    assert recovered == replace(original, expires_at=renewed.expires_at, version=2)
    queried = await SandboxService(runtime, recovered_store).get(original.sandbox_id)
    assert queried.expires_at == renewed.expires_at
    assert queried.status == runtime.query_result.status
    assert runtime.query_result.expires_at == original.expires_at
    assert runtime.specs == [] and runtime.deletions == []


@pytest.mark.anyio
@pytest.mark.parametrize("ttl_seconds", [1, 86400])
async def test_ttl_boundaries_and_precise_utc_clock_snapshot(
    store: RenewalStore, ttl_seconds: int,
) -> None:
    """One normalized UTC sample preserves microseconds at both permitted TTL boundaries."""
    record = await active_record(store, expires_at=NOW + timedelta(microseconds=1))
    runtime = runtime_for(record)
    calls = 0

    def clock() -> datetime:
        """Return the same instant from Shanghai and reject a second operation snapshot."""
        nonlocal calls
        calls += 1
        assert calls == 1
        return NOW.astimezone(timezone(timedelta(hours=8)))

    result = await SandboxService(runtime, store, clock=clock).renew(record.sandbox_id, ttl_seconds)

    assert calls == 1
    assert result.expires_at == NOW + timedelta(seconds=ttl_seconds)
    assert result.expires_at.tzinfo is UTC


@pytest.mark.anyio
async def test_repeated_renewals_neither_accumulate_ttl_nor_shorten_existing_expiry(
    store: RenewalStore,
) -> None:
    """Every request targets now plus TTL and still participates in optimistic versioning."""
    record = await active_record(store, expires_at=NOW + timedelta(hours=1))
    runtime = runtime_for(record)
    service = service_for(runtime, store)

    results = [await service.renew(record.sandbox_id, 1800) for _ in range(2)]

    assert [item.expires_at for item in results] == [record.expires_at, record.expires_at]
    assert await store.get(record.sandbox_id) == replace(record, version=3)
    assert [item[2] for item in store.updates] == [1, 2]


@pytest.mark.anyio
@pytest.mark.parametrize("ttl_seconds", [0, -1, 86401, True, False, 1.0, "1", None])
async def test_invalid_ttl_never_reaches_runtime_or_storage(
    store: RenewalStore, ttl_seconds,
) -> None:
    """Domain callers also reject values that could be coerced into a valid TTL."""
    runtime = FakeRuntime()

    with pytest.raises(ValueError):
        await service_for(runtime, store).renew(FIXED_SANDBOX_ID, ttl_seconds)

    assert runtime.queries == [] and store.get_calls == [] and store.updates == []


@pytest.mark.anyio
@pytest.mark.parametrize("sandbox_id", ["invalid", "sbx_ABC", "sbx_" + "f" * 33])
async def test_invalid_public_id_never_reaches_runtime_or_storage(
    store: RenewalStore, sandbox_id: str,
) -> None:
    """Invalid public identifiers retain the existing not-found semantics."""
    runtime = FakeRuntime()

    with pytest.raises(SandboxNotFound):
        await service_for(runtime, store).renew(sandbox_id, 1800)

    assert runtime.queries == [] and store.get_calls == [] and store.updates == []


@pytest.mark.anyio
async def test_runtime_not_found_wins_over_existing_or_unavailable_metadata(
    store: RenewalStore,
) -> None:
    """A persisted record cannot make renewal succeed for an absent runtime instance."""
    record = await active_record(store)
    store.get_calls.clear()
    store.fail_get = True
    runtime = FakeRuntime(SandboxNotFound())

    with pytest.raises(SandboxNotFound):
        await service_for(runtime, store).renew(record.sandbox_id, 1800)

    assert store.get_calls == [] and store.updates == []


@pytest.mark.anyio
@pytest.mark.parametrize("initial_times", [False, True])
async def test_unmanaged_instance_is_rejected_without_adopting_metadata(
    store: RenewalStore, initial_times: bool,
) -> None:
    """Neither legacy null times nor initial Docker labels authorize implicit management."""
    runtime = FakeRuntime()
    if initial_times:
        runtime.query_result = replace(
            runtime.query_result, transaction_id=FIXED_TRANSACTION_ID,
            created_at=NOW - timedelta(minutes=1), expires_at=NOW + timedelta(hours=1),
        )

    with pytest.raises(SandboxRenewConflict) as failure:
        await service_for(runtime, store).renew(FIXED_SANDBOX_ID, 1800)

    assert failure.value.sandbox_id == FIXED_SANDBOX_ID
    assert await store.get(FIXED_SANDBOX_ID) is None
    assert store.updates == []


@pytest.mark.anyio
@pytest.mark.parametrize("field", ["sandbox_id", "transaction_id", "created_at", "legacy"])
async def test_mismatched_backend_identity_is_metadata_conflict(
    store: RenewalStore, field: str,
) -> None:
    """Identity failures are distinct from valid-identity renewal eligibility conflicts."""
    record = await active_record(store)
    runtime = runtime_for(record)
    changes = {
        "sandbox_id": {"sandbox_id": "sbx_" + "f" * 32},
        "transaction_id": {"transaction_id": "txn_" + "f" * 32},
        "created_at": {"created_at": record.created_at - timedelta(microseconds=1)},
        "legacy": {"transaction_id": None, "created_at": None, "expires_at": None},
    }
    runtime.query_result = replace(runtime.query_result, **changes[field])

    with pytest.raises(SandboxMetadataConflict):
        await service_for(runtime, store).renew(record.sandbox_id, 1800)

    assert store.updates == [] and await store.get(record.sandbox_id) == record


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["pending", "deleting"])
async def test_nonactive_metadata_cannot_be_renewed(store: RenewalStore, state: str) -> None:
    """Unconfirmed creations and claimed deletions cannot be made active by renewal."""
    record = await active_record(store, state=state)

    with pytest.raises(SandboxRenewConflict):
        await service_for(runtime_for(record), store).renew(record.sandbox_id, 1800)

    assert store.updates == [] and await store.get(record.sandbox_id) == record


@pytest.mark.anyio
@pytest.mark.parametrize("delta", [timedelta(microseconds=-1), timedelta(0)])
async def test_expired_and_exactly_due_metadata_cannot_be_renewed(
    store: RenewalStore, delta: timedelta,
) -> None:
    """Expiry is exclusive: even an exactly equal operation snapshot rejects revival."""
    record = await active_record(store, expires_at=NOW + delta)

    with pytest.raises(SandboxRenewConflict):
        await service_for(runtime_for(record), store).renew(record.sandbox_id, 1800)

    assert store.updates == [] and await store.get(record.sandbox_id) == record


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["read", "update", "after_commit"])
async def test_storage_failures_are_stable_and_never_roll_back_committed_renewal(
    store: RenewalStore, phase: str,
) -> None:
    """An unavailable or ambiguous write has a stable error and preserves its final database state."""
    record = await active_record(store)
    store.fail_get = phase == "read"
    store.fail_before_update = phase == "update"
    store.fail_after_update = phase == "after_commit"

    with pytest.raises(SandboxMetadataUnavailable) as failure:
        await service_for(runtime_for(record), store).renew(record.sandbox_id, 1800)

    assert str(failure.value) == "sandbox lifecycle metadata is unavailable"
    assert failure.value.sandbox_id == record.sandbox_id
    persisted = await SQLiteLifecycleStore(store._path).get(record.sandbox_id)
    expected = replace(record, expires_at=NOW + timedelta(seconds=1800), version=2)
    assert persisted == (expected if phase == "after_commit" else record)


@pytest.mark.anyio
@pytest.mark.parametrize("error", [DockerUnavailable(), SandboxQueryFailed(), RuntimeError("private Docker response")])
async def test_runtime_failures_prevent_storage_access_and_hide_unknown_errors(
    store: RenewalStore, error: Exception,
) -> None:
    """Read-only verification keeps known domain failures and masks unexpected runtime details."""
    runtime = FakeRuntime(error)
    expected = SandboxQueryFailed if type(error) is RuntimeError else type(error)

    with pytest.raises(expected) as failure:
        await service_for(runtime, store).renew(FIXED_SANDBOX_ID, 1800)

    assert "private Docker response" not in str(failure.value)
    assert store.get_calls == [] and store.updates == []


@pytest.mark.anyio
async def test_renewal_commit_invalidates_old_reaper_snapshot(store: RenewalStore) -> None:
    """A future due snapshot cannot claim a version superseded by committed renewal."""
    record = await active_record(store)
    reaper = SQLiteLifecycleStore(store._path)
    old_candidates = await reaper.list_reap_candidates(record.expires_at)
    assert old_candidates == [record]

    await service_for(runtime_for(record), store).renew(record.sandbox_id, 1800)
    claimed = await reaper.claim_expired(
        record.sandbox_id, record.transaction_id,
        expected_version=record.version, now=record.expires_at,
    )

    assert claimed is None
    persisted = await reaper.get(record.sandbox_id)
    assert persisted.management_state == "active" and persisted.version == 2


@pytest.mark.anyio
@pytest.mark.parametrize("competitor", ["reaper", "manual_delete", "removed", "replaced"])
async def test_renewal_loses_to_guarded_claim_without_refreshing_identity(
    store: RenewalStore, competitor: str,
) -> None:
    """A paused write cannot overwrite deletion intent, disappearance or a replacement transaction."""
    record = await active_record(store)
    runtime = runtime_for(record)
    store.block_update = True
    renewal = asyncio.create_task(service_for(runtime, store).renew(record.sandbox_id, 1800))
    other_store = SQLiteLifecycleStore(store._path)
    try:
        assert await asyncio.to_thread(store.update_entered.wait, 3)
        if competitor == "reaper":
            changed = await other_store.claim_expired(
                record.sandbox_id, record.transaction_id,
                expected_version=record.version, now=record.expires_at,
            )
        elif competitor == "manual_delete":
            changed = await other_store.mark_deleting(
                record.sandbox_id, record.transaction_id, expected_version=record.version,
            )
        else:
            await other_store.delete(
                record.sandbox_id, record.transaction_id, expected_version=record.version,
            )
            changed = None
            if competitor == "replaced":
                changed = replace(record, transaction_id="txn_" + "f" * 32,
                                  version=0, management_state="pending")
                await other_store.create(changed)
        store.update_release.set()
        with pytest.raises(SandboxRenewConflict):
            await renewal
        assert len(store.updates) == 1
        assert await other_store.get(record.sandbox_id) == changed
        assert runtime.deletions == []
    finally:
        store.update_release.set()
        await asyncio.gather(renewal, return_exceptions=True)


@pytest.mark.anyio
async def test_two_renewals_with_same_version_allow_only_one_commit(store: RenewalStore) -> None:
    """Independent SQLite connections settle a same-snapshot race without lost updates."""
    record = await active_record(store)
    store.concurrent_updates = 2
    service = service_for(runtime_for(record), store)

    outcomes = await asyncio.gather(
        service.renew(record.sandbox_id, 60), service.renew(record.sandbox_id, 120),
        return_exceptions=True,
    )

    successes = [item for item in outcomes if not isinstance(item, BaseException)]
    failures = [item for item in outcomes if isinstance(item, BaseException)]
    assert len(successes) == len(failures) == 1
    assert isinstance(failures[0], SandboxRenewConflict)
    assert [item[2] for item in store.updates] == [record.version, record.version]
    persisted = await SQLiteLifecycleStore(store._path).get(record.sandbox_id)
    assert persisted == replace(record, expires_at=successes[0].expires_at, version=2)


@pytest.mark.anyio
@pytest.mark.parametrize("cancellations", [1, 2])
@pytest.mark.parametrize("worker_failure", ["none", "before_commit", "after_commit"])
async def test_cancelled_sqlite_write_settles_final_commit_without_reverse_compensation(
    store: RenewalStore, cancellations: int, worker_failure: str,
) -> None:
    """Cancellation waits for a worker's final state and consumes errors without shortening expiry."""
    record = await active_record(store)
    store.block_update = True
    store.fail_before_update = worker_failure == "before_commit"
    store.fail_after_update = worker_failure == "after_commit"
    loop = asyncio.get_running_loop()
    old_handler = loop.get_exception_handler()
    unhandled: list[dict] = []

    def capture_unhandled(_loop, context) -> None:
        """Record orphaned worker exceptions as test failures instead of hiding them in logs."""
        unhandled.append(context)

    loop.set_exception_handler(capture_unhandled)
    renewal = asyncio.create_task(service_for(runtime_for(record), store).renew(record.sandbox_id, 1800))
    try:
        assert await asyncio.to_thread(store.update_entered.wait, 3)
        for _ in range(cancellations):
            renewal.cancel()
            await asyncio.sleep(0)
            assert not renewal.done()
        store.update_release.set()
        expected = asyncio.CancelledError if worker_failure == "none" else SandboxMetadataUnavailable
        with pytest.raises(expected):
            await renewal
        persisted = await SQLiteLifecycleStore(store._path).get(record.sandbox_id)
        committed = replace(record, expires_at=NOW + timedelta(seconds=1800), version=2)
        assert persisted == (record if worker_failure == "before_commit" else committed)
        assert len(store.updates) == 1
        await asyncio.sleep(0)
        assert unhandled == []
    finally:
        store.update_release.set()
        await asyncio.gather(renewal, return_exceptions=True)
        loop.set_exception_handler(old_handler)


@pytest.mark.anyio
@pytest.mark.parametrize("no_expiry_extension", [False, True])
async def test_manual_service_delete_claim_blocks_even_unchanged_renewal(
    store: RenewalStore, no_expiry_extension: bool,
) -> None:
    """A real manual delete enters deleting before Runtime work and defeats every stale renewal."""
    expiration = NOW + timedelta(hours=1 if no_expiry_extension else 0, seconds=30)
    record = await active_record(store, expires_at=expiration)
    deletion_entered = asyncio.Event()
    deletion_release = asyncio.Event()

    class DeleteRuntime(FakeRuntime):
        """Keep a manual deletion open after SQLite has claimed its exact version."""

        async def delete(
            self, sandbox_id: str, *, delete_workspace: bool = False,
            expected_transaction_id: str | None = None,
        ) -> None:
            """Observe the protected identity and let the test release runtime completion."""
            assert sandbox_id == record.sandbox_id
            assert expected_transaction_id == record.transaction_id
            assert delete_workspace is False
            deletion_entered.set()
            await deletion_release.wait()

    runtime = DeleteRuntime()
    runtime.query_result = runtime_for(record).query_result
    store.block_update = True
    renewal = asyncio.create_task(service_for(runtime, store).renew(record.sandbox_id, 1800))
    deletion = None
    try:
        assert await asyncio.to_thread(store.update_entered.wait, 3)
        other_store = SQLiteLifecycleStore(store._path)
        deletion = asyncio.create_task(service_for(runtime, other_store).delete(record.sandbox_id))
        await deletion_entered.wait()
        deleting = await other_store.get(record.sandbox_id)
        assert deleting.management_state == "deleting" and deleting.version == record.version + 1
        store.update_release.set()
        with pytest.raises(SandboxRenewConflict):
            await renewal
        assert await other_store.get(record.sandbox_id) == deleting
        assert len(store.updates) == 1
        deletion_release.set()
        await deletion
        assert await other_store.get(record.sandbox_id) is None
    finally:
        store.update_release.set()
        deletion_release.set()
        tasks = [renewal] if deletion is None else [renewal, deletion]
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.anyio
async def test_cancelled_late_acknowledgement_preserves_a_subsequent_longer_renewal(
    store: RenewalStore,
) -> None:
    """A cancelled commit cannot compensate over a later independent connection's renewal."""
    record = await active_record(store)
    runtime = runtime_for(record)
    store.block_after_update = True
    first = asyncio.create_task(service_for(runtime, store).renew(record.sandbox_id, 1800))
    try:
        assert await asyncio.to_thread(store.commit_entered.wait, 3)
        first.cancel()
        await asyncio.sleep(0)
        first.cancel()
        await asyncio.sleep(0)
        assert not first.done()
        fresh_store = SQLiteLifecycleStore(store._path)
        subsequent = await service_for(runtime, fresh_store).renew(record.sandbox_id, 3600)
        store.commit_release.set()
        with pytest.raises(asyncio.CancelledError):
            await first
        assert await fresh_store.get(record.sandbox_id) == replace(
            record, expires_at=subsequent.expires_at, version=3,
        )
        assert subsequent.expires_at == NOW + timedelta(seconds=3600)
        assert runtime.query_result.expires_at == record.expires_at
    finally:
        store.commit_release.set()
        await asyncio.gather(first, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("clock_value", [datetime(2026, 10, 19, 8, 30), None])
async def test_invalid_clock_prevents_runtime_and_storage_access(
    store: RenewalStore, clock_value,
) -> None:
    """An invalid clock snapshot cannot be interpreted as local time or persisted."""
    runtime = FakeRuntime()
    service = SandboxService(runtime, store, clock=lambda: clock_value)

    with pytest.raises(ValueError):
        await service.renew(FIXED_SANDBOX_ID, 1800)

    assert runtime.queries == [] and store.get_calls == [] and store.updates == []


@pytest.mark.anyio
async def test_store_returning_another_sandbox_identity_is_rejected(
    store: RenewalStore, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even a store adapter must return the requested identity before it can authorize writes."""
    record = await active_record(store)

    async def wrong_identity(sandbox_id: str):
        """Simulate a broken adapter response without modifying the actual stored row."""
        assert sandbox_id == record.sandbox_id
        return replace(record, sandbox_id="sbx_" + "f" * 32)

    monkeypatch.setattr(store, "get", wrong_identity)

    with pytest.raises(SandboxMetadataConflict):
        await service_for(runtime_for(record), store).renew(record.sandbox_id, 1800)

    assert store.updates == []
    assert await SQLiteLifecycleStore(store._path).get(record.sandbox_id) == record
