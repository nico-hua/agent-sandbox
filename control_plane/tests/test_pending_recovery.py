"""Controlled confirmation of original pending records using file SQLite and read-only facts."""

import asyncio
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
import sqlite3
import threading

import pytest

from control_plane.core.errors import (
    DockerUnavailable, SandboxEndpointUnavailable, SandboxNotFound, SandboxQueryFailed,
)
from control_plane.core.models import (
    SandboxEndpoint, SandboxLifecycleRecord, SandboxQueryResult, SandboxResourceInspection,
)
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore
from test_sandbox_service import FakeRuntime


NOW = datetime(2026, 10, 21, 8, 0, 0, 123456, tzinfo=UTC)
ROLES = ("container", "proxy", "network", "ingress", "workspace")


class RecoveryRuntime(FakeRuntime):
    """Keep backend facts independent of SQLite and expose controllable read boundaries."""

    def __init__(self) -> None:
        """Initialize fixed observations without creating any runtime resources."""
        super().__init__()
        self.records = {}
        self.classification = "all_present"
        self.status = "ready"
        self.failure_stage = None
        self.failure = DockerUnavailable("private runtime details")
        self.identity_change = None
        self.calls = []
        self.block_endpoint = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def inspect_resources(self, sandbox_id, *, expected_transaction_id):
        """Confirm resource ownership against the candidate's original transaction."""
        self.calls.append(("inspect", sandbox_id))
        assert self.records[sandbox_id].transaction_id == expected_transaction_id
        if self.failure_stage == "inspect":
            raise self.failure
        return SandboxResourceInspection(
            sandbox_id, self.classification, "private untrusted reason", ROLES,
        )

    async def get(self, sandbox_id):
        """Return independently saved identity and health without using merged Service queries."""
        self.calls.append(("get", sandbox_id))
        if self.failure_stage == "get":
            raise self.failure
        record = self.records[sandbox_id]
        result = SandboxQueryResult(
            sandbox_id, self.status, "observed", "observed",
            record.created_at, record.expires_at, record.transaction_id,
        )
        if self.identity_change is not None:
            field, value = self.identity_change
            result = replace(result, **{field: value})
        return result

    async def get_endpoint(self, sandbox_id):
        """Gate the final runtime read while a competing SQLite connection changes metadata."""
        self.calls.append(("endpoint", sandbox_id))
        self.entered.set()
        if self.block_endpoint:
            await self.release.wait()
        if self.failure_stage == "endpoint":
            raise self.failure
        return SandboxEndpoint(sandbox_id, "http://127.0.0.1:49153")


class RecoveryStore(SQLiteLifecycleStore):
    """Exercise real guarded activation with deterministic late commits and lost responses."""

    def __init__(self, path):
        """Create thread gates without touching the configured database."""
        super().__init__(path)
        self.block_activation = False
        self.fail_before_commit = False
        self.fail_after_commit = False
        self.entered = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        self.before_activate = None

    async def activate(self, sandbox_id, transaction_id, *, expected_version):
        """Allow a competing independent connection to win just before the guarded SQL."""
        if self.before_activate is not None:
            await self.before_activate()
        return await super().activate(
            sandbox_id, transaction_id, expected_version=expected_version,
        )

    def _activate_sync(self, sandbox_id, transaction_id, expected_version):
        """Pause or lose acknowledgement without undoing the actual SQLite commit."""
        self.entered.set()
        try:
            if self.block_activation:
                assert self.release.wait(5), "activation test gate timed out"
            if self.fail_before_commit:
                raise LifecycleStoreError()
            result = super()._activate_sync(sandbox_id, transaction_id, expected_version)
            if self.fail_after_commit:
                raise LifecycleStoreError()
            return result
        finally:
            self.finished.set()


@pytest.fixture
async def store(tmp_path):
    """Initialize a real temporary SQLite file without changing development storage."""
    result = RecoveryStore(tmp_path / "recovery.sqlite3")
    await result.initialize()
    return result


async def add_pending(store, runtime, number=1, *, expires_at=None, offset_us=0):
    """Save matching independent backend and pending facts with exact original UTC times."""
    record = SandboxLifecycleRecord(
        f"sbx_{number:032x}", f"txn_{number:032x}",
        NOW - timedelta(minutes=1) + timedelta(microseconds=offset_us),
        expires_at if expires_at is not None else NOW + timedelta(minutes=29),
    )
    await store.create(record)
    runtime.records[record.sandbox_id] = record
    return record


@pytest.mark.anyio
async def test_recovery_activates_exact_record_without_changing_times_or_resources(store):
    """Eligible recovery changes only management state and version and is not repeated."""
    runtime = RecoveryRuntime()
    original = await add_pending(store, runtime)
    service = SandboxService(runtime, store, clock=lambda: NOW)

    result = await service.recover_pending_once()

    assert result.recovered == 1 and result.skipped == result.failed == 0
    assert result.error is None and result.items[0].reason == "recovered"
    assert await store.get(original.sandbox_id) == replace(original, management_state="active", version=1)
    assert runtime.calls == [(stage, original.sandbox_id) for stage in ("inspect", "get", "endpoint")]
    assert (await service.recover_pending_once()).items == ()
    assert runtime.specs == runtime.deletions == []


@pytest.mark.anyio
async def test_recovery_uses_the_original_nonzero_pending_version(store):
    """Confirmation uses the saved version rather than assuming every pending row is version zero."""
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime)
    with closing(sqlite3.connect(store.database_path)) as connection, connection:
        connection.execute(
            "UPDATE lifecycle_records SET version = 3 WHERE sandbox_id = ?", (record.sandbox_id,),
        )
    result = await SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once()
    assert result.recovered == 1
    assert await store.get(record.sandbox_id) == replace(record, management_state="active", version=4)
    assert runtime.deletions == []


@pytest.mark.anyio
async def test_resources_without_a_lifecycle_record_are_not_adopted(store):
    """Recovery selects only persisted pending candidates and never enumerates unmanaged instances."""
    runtime = RecoveryRuntime()
    legacy = SandboxLifecycleRecord(
        "sbx_" + "f" * 32, "txn_" + "f" * 32, NOW - timedelta(minutes=1),
        NOW + timedelta(minutes=29),
    )
    runtime.records[legacy.sandbox_id] = legacy
    result = await SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once()
    assert result.items == () and result.error is None
    assert await store.get(legacy.sandbox_id) is None
    assert runtime.calls == runtime.specs == runtime.deletions == []


@pytest.mark.anyio
@pytest.mark.parametrize("classification,outcome,reason", [
    ("partial", "skipped", "resources_partial"),
    ("not_found", "skipped", "resources_not_found"),
    ("conflict", "skipped", "resource_ownership_conflict"),
    ("unavailable", "failed", "docker_unavailable"),
])
async def test_incomplete_or_uncertain_inventory_is_never_activated(store, classification, outcome, reason):
    """Resource discovery alone cannot justify activation of incomplete or uncertain candidates."""
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime)
    runtime.classification = classification

    result = await SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once()

    assert (result.items[0].outcome, result.items[0].reason) == (outcome, reason)
    assert "private" not in repr(result)
    assert runtime.calls == [("inspect", record.sandbox_id)]
    assert await store.get(record.sandbox_id) == record
    assert runtime.specs == runtime.deletions == []


@pytest.mark.anyio
@pytest.mark.parametrize("status", ["starting", "running", "stopped", "failed"])
async def test_only_ready_is_eligible_for_recovery(store, status):
    """Ready is a conservative recovery gate and does not redefine general active semantics."""
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime)
    runtime.status = status
    result = await SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once()
    assert result.skipped == 1 and result.items[0].reason == "sandbox_not_ready"
    assert await store.get(record.sandbox_id) == record
    assert runtime.specs == runtime.deletions == []


@pytest.mark.anyio
@pytest.mark.parametrize("offset", [-1, 0, 1])
async def test_recovery_checks_expiration_at_candidate_commit_time(store, offset):
    """The exact microsecond boundary is judged after runtime checks using one clock sample."""
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime, expires_at=NOW + timedelta(microseconds=offset))
    calls = []

    def clock():
        """Require all runtime checks to finish before sampling this candidate's current time."""
        assert runtime.calls[-1] == ("endpoint", record.sandbox_id)
        calls.append(NOW)
        return NOW

    result = await SandboxService(runtime, store, clock=clock).recover_pending_once()
    assert calls == [NOW]
    assert result.recovered == (1 if offset > 0 else 0)
    assert result.skipped == (0 if offset > 0 else 1)
    if offset <= 0:
        assert result.items[0].reason == "sandbox_expired"
        assert await store.get(record.sandbox_id) == record


@pytest.mark.anyio
@pytest.mark.parametrize("field,value", [
    ("sandbox_id", "sbx_" + "f" * 32),
    ("transaction_id", "txn_" + "f" * 32),
    ("created_at", NOW - timedelta(minutes=2)),
])
async def test_runtime_identity_must_match_original_pending(store, field, value):
    """A full, ready instance from another identity cannot be adopted into the record."""
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime)
    runtime.identity_change = (field, value)
    result = await SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once()
    assert result.skipped == 1 and result.items[0].reason == "sandbox_identity_mismatch"
    assert await store.get(record.sandbox_id) == record


@pytest.mark.anyio
@pytest.mark.parametrize("stage,failure,reason", [
    ("inspect", DockerUnavailable("private"), "docker_unavailable"),
    ("get", DockerUnavailable("private"), "docker_unavailable"),
    ("endpoint", DockerUnavailable("private"), "docker_unavailable"),
    ("get", SandboxNotFound(), "sandbox_not_found"),
    ("get", SandboxQueryFailed("private"), "sandbox_query_failed"),
    ("endpoint", SandboxEndpointUnavailable("private"), "sandbox_endpoint_unavailable"),
    ("endpoint", RuntimeError("private runtime error"), "sandbox_recovery_failed"),
])
async def test_runtime_failures_preserve_pending_without_any_creation_compensation(store, stage, failure, reason):
    """Read failures have stable reasons and never call the creation rollback or deletion paths."""
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime)
    runtime.failure_stage, runtime.failure = stage, failure
    result = await SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once()
    assert result.items[0].reason == reason and "private" not in repr(result)
    assert await store.get(record.sandbox_id) == record
    assert runtime.specs == runtime.deletions == []


@pytest.mark.anyio
@pytest.mark.parametrize("race", ["activated", "deleted", "replaced", "version"])
async def test_changed_record_is_skipped_without_adopting_a_new_snapshot(store, race):
    """Independent SQLite changes during a runtime read invalidate the saved pending snapshot."""
    runtime = RecoveryRuntime()
    original = await add_pending(store, runtime)
    runtime.block_endpoint = True
    task = asyncio.create_task(SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once())
    try:
        await asyncio.wait_for(runtime.entered.wait(), 3)
        other = SQLiteLifecycleStore(store.database_path)
        if race == "activated":
            current = await other.activate(original.sandbox_id, original.transaction_id, expected_version=0)
        elif race == "version":
            def change_version():
                """Model an independently committed version change with a separate connection."""
                with closing(sqlite3.connect(store.database_path)) as connection, connection:
                    connection.execute("UPDATE lifecycle_records SET version = 1 WHERE sandbox_id = ?", (original.sandbox_id,))

            await asyncio.to_thread(change_version)
            current = replace(original, version=1)
        else:
            await other.delete(original.sandbox_id, original.transaction_id, expected_version=0)
            current = None
            if race == "replaced":
                current = replace(original, transaction_id="new-transaction")
                await other.create(current)
        runtime.release.set()
        result = await task
        assert result.skipped == 1 and result.items[0].reason == "pending_record_changed"
        assert await store.get(original.sandbox_id) == current
        assert runtime.specs == runtime.deletions == []
    finally:
        runtime.release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_activation_conflict_after_reread_is_skipped_without_refresh(store):
    """The Store CAS defeats a competing activation even after the final metadata read."""
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime)
    other = SQLiteLifecycleStore(store.database_path)

    async def activate_elsewhere():
        """Commit another owner's activation immediately before the recovery CAS."""
        await other.activate(record.sandbox_id, record.transaction_id, expected_version=0)

    store.before_activate = activate_elsewhere
    result = await SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once()
    assert result.skipped == 1 and result.items[0].reason == "pending_record_changed"
    assert (await store.get(record.sandbox_id)).version == 1
    assert runtime.deletions == []


@pytest.mark.anyio
@pytest.mark.parametrize("late_error", [False, True])
async def test_repeated_cancel_during_activation_settles_commit_and_stops_next_candidate(store, late_error):
    """Cancelled recovery retains a late active commit and consumes its result or failure."""
    runtime = RecoveryRuntime()
    first = await add_pending(store, runtime, 1)
    second = await add_pending(store, runtime, 2)
    store.block_activation = True
    store.fail_after_commit = late_error
    task = asyncio.create_task(SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once())
    try:
        assert await asyncio.to_thread(store.entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not store.finished.is_set()
        store.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert store.finished.is_set()
        assert await store.get(first.sandbox_id) == replace(first, management_state="active", version=1)
        assert await store.get(second.sandbox_id) == second
        assert all(sandbox_id == first.sandbox_id for _, sandbox_id in runtime.calls)
        assert runtime.specs == runtime.deletions == []
    finally:
        store.release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_commit_acknowledgement_failure_keeps_actual_active_record(store):
    """A write exception is uncertainty, not evidence that activation did not commit."""
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime)
    store.fail_after_commit = True
    result = await SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once()
    assert result.failed == 1 and result.error == "sandbox_metadata_unavailable"
    assert await store.get(record.sandbox_id) == replace(record, management_state="active", version=1)
    assert runtime.specs == runtime.deletions == []


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["list_pending", "get", "activate"])
async def test_storage_failure_stops_round_and_is_not_an_empty_success(store, monkeypatch, stage):
    """An overall storage outage stops processing and keeps all runtime resources untouched."""
    runtime = RecoveryRuntime()
    await add_pending(store, runtime, 1)
    await add_pending(store, runtime, 2)

    async def unavailable(*args, **kwargs):
        """Model a store failure without allowing raw diagnostic text into the result."""
        raise LifecycleStoreError()

    monkeypatch.setattr(store, stage, unavailable)
    result = await SandboxService(runtime, store, clock=lambda: NOW).recover_pending_once()
    assert result.error == "sandbox_metadata_unavailable" and result.recovered == 0
    assert len(runtime.calls) == (0 if stage == "list_pending" else 3)
    assert runtime.specs == runtime.deletions == []


@pytest.mark.anyio
async def test_backend_unavailable_stops_round_but_item_failure_allows_later_candidate(store):
    """A backend-wide outage is bounded while instance-specific non-readiness does not stop a batch."""
    runtime = RecoveryRuntime()
    first = await add_pending(store, runtime, 1)
    second = await add_pending(store, runtime, 2)
    runtime.classification = "unavailable"
    service = SandboxService(runtime, store, clock=lambda: NOW)
    result = await service.recover_pending_once()
    assert result.error == "docker_unavailable" and len(result.items) == 1
    runtime.classification = "all_present"
    runtime.status = "running"
    result = await service.recover_pending_once()
    assert result.skipped == 2 and result.error is None
    assert await store.get(first.sandbox_id) == first and await store.get(second.sandbox_id) == second


@pytest.mark.anyio
async def test_batch_is_bounded_stable_and_samples_clock_per_candidate(store):
    """One stable limited batch is processed without scanning all pending or selecting active rows."""
    runtime = RecoveryRuntime()
    third = await add_pending(store, runtime, 3, offset_us=1)
    second = await add_pending(store, runtime, 2)
    first = await add_pending(store, runtime, 1)
    calls = []

    def clock():
        """Give each eligible candidate its own commit-time clock snapshot."""
        calls.append(NOW)
        return NOW

    result = await SandboxService(runtime, store, clock=clock).recover_pending_once(limit=2)
    assert [item.sandbox_id for item in result.items] == [first.sandbox_id, second.sandbox_id]
    assert result.recovered == 2 and calls == [NOW, NOW]
    assert await store.get(third.sandbox_id) == third


@pytest.mark.anyio
@pytest.mark.parametrize("limit", [0, -1, True, 1.0, "1", None, 2**63])
async def test_invalid_recovery_limit_fails_before_runtime_or_store_access(store, limit):
    """Strict real-integer batch bounds cannot be silently coerced or unbounded."""
    runtime = RecoveryRuntime()
    with pytest.raises(ValueError):
        await SandboxService(runtime, store).recover_pending_once(limit=limit)
    assert runtime.calls == []
