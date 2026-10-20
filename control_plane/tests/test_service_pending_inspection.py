"""Bounded read-only pending inspection against real file storage and controlled runtimes."""

import asyncio
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from control_plane.core.errors import DockerUnavailable
from control_plane.core.models import SandboxLifecycleRecord, SandboxResourceInspection
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore
from test_sandbox_service import FakeRuntime


CREATED_AT = datetime(2026, 10, 20, 8, 0, 0, 123456, tzinfo=UTC)
ROLES = ("container", "proxy", "network", "ingress", "workspace")


class InspectionRuntime(FakeRuntime):
    """Expose immutable observations and gates without performing backend mutations."""

    def __init__(self) -> None:
        """Initialize inspection results, identity tracking and deterministic race gates."""
        super().__init__()
        self.inspections: list[tuple[str, str]] = []
        self.results: dict[str, SandboxResourceInspection | Exception] = {}
        self.block = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.completed = asyncio.Event()

    async def inspect_resources(
        self, sandbox_id: str, *, expected_transaction_id: str,
    ) -> SandboxResourceInspection:
        """Observe only the candidate's supplied identity and optionally pause for a race."""
        self.inspections.append((sandbox_id, expected_transaction_id))
        self.entered.set()
        if self.block:
            await self.release.wait()
        self.completed.set()
        result = self.results.get(sandbox_id, SandboxResourceInspection(
            sandbox_id, "all_present", "resources_present", found_resources=ROLES,
        ))
        if isinstance(result, Exception):
            raise result
        return result


@pytest.fixture
async def store(tmp_path: Path) -> SQLiteLifecycleStore:
    """Create an isolated initialized SQLite file without accessing development storage."""
    result = SQLiteLifecycleStore(tmp_path / "pending.sqlite3")
    await result.initialize()
    return result


async def add_pending(
    store: SQLiteLifecycleStore, number: int, *, offset_us: int = 0,
) -> SandboxLifecycleRecord:
    """Persist one pending record with deterministic identity and exact UTC times."""
    record = SandboxLifecycleRecord(
        sandbox_id=f"sbx_{number:032x}", transaction_id=f"txn_{number:032x}",
        created_at=CREATED_AT + timedelta(microseconds=offset_us),
        expires_at=CREATED_AT + timedelta(seconds=1800),
    )
    await store.create(record)
    return record


def inspection_service(runtime: InspectionRuntime, store: SQLiteLifecycleStore) -> SandboxService:
    """Reject clock access because pending inspection must not make age-based decisions."""
    def forbidden_clock() -> datetime:
        """Fail if an observational operation tries to sample or regenerate lifecycle time."""
        raise AssertionError("inspection must not read the clock")

    return SandboxService(runtime, store, clock=forbidden_clock)


@pytest.mark.anyio
async def test_empty_pending_batch_does_not_call_runtime(store: SQLiteLifecycleStore) -> None:
    """An empty batch is successful without claiming that every pending record was examined."""
    runtime = InspectionRuntime()

    result = await inspection_service(runtime, store).inspect_pending_once()

    assert result.items == () and result.error is None
    assert runtime.inspections == []


@pytest.mark.anyio
@pytest.mark.parametrize("classification,found,missing", [
    ("all_present", ROLES, ()),
    ("partial", ("proxy", "network", "ingress", "workspace"), ("container",)),
    ("not_found", (), ROLES),
    ("conflict", (), ()),
    ("unavailable", (), ()),
])
async def test_inspection_returns_observations_without_mutating_records(
    store: SQLiteLifecycleStore, classification: str, found: tuple, missing: tuple,
) -> None:
    """Every observation preserves pending contents, identity, version and runtime operations."""
    record = await add_pending(store, 1)
    runtime = InspectionRuntime()
    expected = SandboxResourceInspection(
        record.sandbox_id, classification, "stable_reason", found, missing,
    )
    runtime.results[record.sandbox_id] = expected

    result = await inspection_service(runtime, store).inspect_pending_once()

    assert result.items == (expected,) and result.error is None
    assert runtime.inspections == [(record.sandbox_id, record.transaction_id)]
    assert await store.get(record.sandbox_id) == record
    assert runtime.specs == runtime.queries == runtime.deletions == []


@pytest.mark.anyio
async def test_inspection_obeys_bounded_pending_order(store: SQLiteLifecycleStore) -> None:
    """Only this ordered pending batch is inspected; active and deleting rows stay excluded."""
    third = await add_pending(store, 3, offset_us=1)
    second = await add_pending(store, 2)
    first = await add_pending(store, 1)
    active = await add_pending(store, 4)
    await store.activate(active.sandbox_id, active.transaction_id, expected_version=0)
    deleting = await add_pending(store, 5)
    await store.mark_deleting(deleting.sandbox_id, deleting.transaction_id, expected_version=0)
    runtime = InspectionRuntime()

    result = await inspection_service(runtime, store).inspect_pending_once(limit=2)

    assert [item.sandbox_id for item in result.items] == [first.sandbox_id, second.sandbox_id]
    assert len(runtime.inspections) == 2
    assert await store.get(third.sandbox_id) == third


@pytest.mark.anyio
@pytest.mark.parametrize("limit", [0, -1, True, False, 1.0, "1", None, 2**63, float("inf")])
async def test_inspection_rejects_invalid_limit_before_reading(
    store: SQLiteLifecycleStore, monkeypatch: pytest.MonkeyPatch, limit,
) -> None:
    """A strict batch bound is required before either the store or runtime is consulted."""
    async def forbidden_list(**kwargs):
        """Fail if validation reaches storage for an invalid caller-provided bound."""
        raise AssertionError("store must not be called")

    monkeypatch.setattr(store, "list_pending", forbidden_list)
    runtime = InspectionRuntime()
    with pytest.raises(ValueError):
        await inspection_service(runtime, store).inspect_pending_once(limit=limit)
    assert runtime.inspections == []


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["active", "deleting", "deleted", "replaced", "version", "created_at"])
async def test_record_changes_during_runtime_inspection_override_observation(
    store: SQLiteLifecycleStore, tmp_path: Path, change: str,
) -> None:
    """A final read detects competing state, identity, version or removal without adopting it."""
    original = await add_pending(store, 1)
    runtime = InspectionRuntime()
    runtime.block = True
    task = asyncio.create_task(inspection_service(runtime, store).inspect_pending_once())
    await runtime.entered.wait()
    other = SQLiteLifecycleStore(tmp_path / "pending.sqlite3")
    if change == "active":
        current = await other.activate(original.sandbox_id, original.transaction_id, expected_version=0)
    elif change == "deleting":
        current = await other.mark_deleting(
            original.sandbox_id, original.transaction_id, expected_version=0,
        )
    elif change == "version":
        def advance_version() -> None:
            """Model an independently committed version-only change on the same SQLite file."""
            with closing(sqlite3.connect(tmp_path / "pending.sqlite3")) as connection, connection:
                connection.execute(
                    "UPDATE lifecycle_records SET version = version + 1 WHERE sandbox_id = ?",
                    (original.sandbox_id,),
                )

        await asyncio.to_thread(advance_version)
        current = replace(original, version=1)
    else:
        await other.delete(original.sandbox_id, original.transaction_id, expected_version=0)
        current = None
        if change == "replaced":
            current = replace(original, transaction_id="replacement-transaction")
        elif change == "created_at":
            current = replace(original, created_at=original.created_at + timedelta(microseconds=1))
        if current is not None:
            await other.create(current)
    runtime.release.set()
    result = await task

    assert result.error is None
    assert result.items[0].classification == "changed"
    assert result.items[0].reason == "pending_record_changed"
    assert runtime.inspections == [(original.sandbox_id, original.transaction_id)]
    assert await store.get(original.sandbox_id) == current
    assert runtime.specs == runtime.deletions == []


@pytest.mark.anyio
async def test_initial_storage_failure_is_not_an_empty_success(tmp_path: Path) -> None:
    """An uninitialized database produces a stable batch error without implicit initialization."""
    database = tmp_path / "missing" / "lifecycle.sqlite3"
    runtime = InspectionRuntime()

    result = await inspection_service(runtime, SQLiteLifecycleStore(database)).inspect_pending_once()

    assert result.items == () and result.error == "sandbox_metadata_unavailable"
    assert runtime.inspections == [] and not database.parent.exists()


@pytest.mark.anyio
async def test_reread_storage_failure_stops_batch_with_explicit_error(
    store: SQLiteLifecycleStore, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An unverified runtime observation is omitted if its final metadata read fails."""
    first = await add_pending(store, 1)
    await add_pending(store, 2)

    async def unavailable(sandbox_id: str):
        """Model an overall database outage without exposing its internal diagnostic text."""
        raise LifecycleStoreError()

    monkeypatch.setattr(store, "get", unavailable)
    runtime = InspectionRuntime()
    result = await inspection_service(runtime, store).inspect_pending_once()

    assert result.items == () and result.error == "sandbox_metadata_unavailable"
    assert runtime.inspections == [(first.sandbox_id, first.transaction_id)]


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [DockerUnavailable(), RuntimeError("sensitive backend details")])
async def test_runtime_unavailable_is_isolated_and_later_candidates_are_inspected(
    store: SQLiteLifecycleStore, failure: Exception,
) -> None:
    """An unavailable backend is distinct from absence and does not silently terminate the batch."""
    first = await add_pending(store, 1)
    second = await add_pending(store, 2)
    runtime = InspectionRuntime()
    runtime.results[first.sandbox_id] = failure

    result = await inspection_service(runtime, store).inspect_pending_once()

    assert [item.classification for item in result.items] == ["unavailable", "all_present"]
    assert "sensitive" not in repr(result)
    assert result.items[0].found_resources == result.items[0].missing_resources == ()
    assert result.error is None
    assert await store.get(first.sandbox_id) == first
    assert await store.get(second.sandbox_id) == second


@pytest.mark.anyio
async def test_conflicting_candidate_does_not_block_remaining_pending(store: SQLiteLifecycleStore) -> None:
    """Ownership conflicts remain item-level findings, preserving all candidate records."""
    first = await add_pending(store, 1)
    second = await add_pending(store, 2)
    runtime = InspectionRuntime()
    runtime.results[first.sandbox_id] = SandboxResourceInspection(
        first.sandbox_id, "conflict", "resource_ownership_conflict",
    )

    result = await inspection_service(runtime, store).inspect_pending_once()

    assert [item.classification for item in result.items] == ["conflict", "all_present"]
    assert await store.get(first.sandbox_id) == first
    assert await store.get(second.sandbox_id) == second


@pytest.mark.anyio
async def test_inspection_does_not_interrupt_an_in_progress_creation(store: SQLiteLifecycleStore) -> None:
    """A live pending creation can be inspected without treating it as abandoned or completing it."""
    runtime = InspectionRuntime()
    entered, release = asyncio.Event(), asyncio.Event()
    original_create = runtime.create

    async def gated_create(spec) -> None:
        """Keep an ordinary Service creation pending while its runtime work remains in progress."""
        await original_create(spec)
        entered.set()
        await release.wait()

    runtime.create = gated_create
    service = SandboxService(runtime, store, clock=lambda: CREATED_AT)
    task = asyncio.create_task(service.create())
    await entered.wait()
    spec = runtime.specs[0]
    pending = await store.get(spec.sandbox_id)

    result = await service.inspect_pending_once()

    assert result.items[0].classification == "all_present"
    assert await store.get(spec.sandbox_id) == pending
    assert pending.management_state == "pending" and pending.version == 0
    assert not task.done() and runtime.deletions == []
    release.set()
    created = await task
    assert created.sandbox_id == spec.sandbox_id
    assert (await store.get(spec.sandbox_id)).management_state == "active"


@pytest.mark.anyio
@pytest.mark.parametrize("late_failure", [False, True])
async def test_repeated_cancellation_settles_inspection_and_stops_later_candidates(
    store: SQLiteLifecycleStore, late_failure: bool,
) -> None:
    """Cancellation reads the current worker result and leaves later candidates untouched."""
    first = await add_pending(store, 1)
    second = await add_pending(store, 2)
    runtime = InspectionRuntime()
    runtime.block = True
    if late_failure:
        runtime.results[first.sandbox_id] = RuntimeError("late backend error")
    task = asyncio.create_task(inspection_service(runtime, store).inspect_pending_once())
    await runtime.entered.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.sleep(0)
    assert not task.done()
    runtime.release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert runtime.completed.is_set()
    assert runtime.inspections == [(first.sandbox_id, first.transaction_id)]
    assert await store.get(first.sandbox_id) == first
    assert await store.get(second.sandbox_id) == second
