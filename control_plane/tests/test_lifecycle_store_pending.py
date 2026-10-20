"""Bounded, read-only pending selection against independent temporary SQLite files."""

import sqlite3
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from control_plane.core.models import SandboxLifecycleRecord
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore


CREATED_AT = datetime(2026, 10, 20, 8, 0, 0, 123456, tzinfo=UTC)


def pending_record(
    sandbox_id: str, *, created_at: datetime = CREATED_AT,
) -> SandboxLifecycleRecord:
    """Build exact pending metadata whose creation order need not match its ID."""
    return SandboxLifecycleRecord(
        sandbox_id, f"transaction-{sandbox_id}", created_at,
        created_at + timedelta(seconds=1800),
    )


@pytest.mark.anyio
async def test_empty_pending_query_preserves_initialized_database(tmp_path: Path) -> None:
    """An initialized empty store returns an empty batch without changing its file."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    await store.initialize()
    before = database.read_bytes()

    assert await store.list_pending() == []
    assert database.read_bytes() == before


@pytest.mark.anyio
async def test_pending_selection_is_precise_stable_and_excludes_other_states(tmp_path: Path) -> None:
    """Read only pending rows ordered by creation microseconds and sandbox identity."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    await store.initialize()
    records = [
        pending_record("later", created_at=CREATED_AT + timedelta(microseconds=1)),
        pending_record("same-b"),
        pending_record("active", created_at=CREATED_AT - timedelta(microseconds=2)),
        pending_record("same-a"),
        pending_record("earlier", created_at=CREATED_AT - timedelta(microseconds=1)),
        pending_record("deleting", created_at=CREATED_AT - timedelta(microseconds=3)),
    ]
    for record in records:
        await store.create(record)
    await store.activate("active", "transaction-active", expected_version=0)
    await store.mark_deleting("deleting", "transaction-deleting", expected_version=0)
    original = {record.sandbox_id: await store.get(record.sandbox_id) for record in records}
    before = database.read_bytes()

    restored = SQLiteLifecycleStore(database)
    selected = await restored.list_pending()

    assert selected == [original[key] for key in ("earlier", "same-a", "same-b", "later")]
    assert await restored.list_pending(limit=2) == selected[:2]
    assert await restored.list_pending(limit=2**63 - 1) == selected
    assert {key: await store.get(key) for key in original} == original
    assert database.read_bytes() == before


@pytest.mark.anyio
async def test_default_pending_batch_is_limited_to_one_hundred(tmp_path: Path) -> None:
    """The default query bounds a larger pending backlog without changing any record."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    records = [pending_record(f"sandbox-{index:03d}") for index in range(101)]
    for record in reversed(records):
        await store.create(record)

    assert await store.list_pending() == records[:100]
    assert await store.get("sandbox-100") == records[100]


@pytest.mark.anyio
async def test_pending_limit_is_applied_before_record_deserialization(tmp_path: Path) -> None:
    """A corrupt out-of-batch row must not be read then discarded by Python slicing."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    await store.initialize()
    first = pending_record("first")
    await store.create(first)
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(
            "INSERT INTO lifecycle_records VALUES (?, ?, ?, ?, ?, ?)",
            ("outside-batch", "transaction-outside", 2**63 - 1, 2**63 - 1, 0, "pending"),
        )
    before = database.read_bytes()

    assert await store.list_pending(limit=1) == [first]
    with pytest.raises(LifecycleStoreError):
        await store.list_pending(limit=2)
    assert database.read_bytes() == before


@pytest.mark.anyio
@pytest.mark.parametrize("limit", [0, -1, True, False, 1.0, 1.5, "1", None, 2**63])
async def test_pending_rejects_invalid_limits_without_opening_database(
    tmp_path: Path, limit: object,
) -> None:
    """Only positive real SQLite integers are accepted before any database access."""
    database = tmp_path / "absent-parent" / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)

    with pytest.raises(ValueError, match="limit must be a positive SQLite integer"):
        await store.list_pending(limit=limit)

    assert not database.parent.exists()


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["missing", "uninitialized", "corrupt", "directory"])
async def test_pending_storage_failures_are_explicit_and_do_not_repair(
    tmp_path: Path, kind: str,
) -> None:
    """Unavailable or incompatible storage cannot masquerade as an empty pending batch."""
    database = tmp_path / "lifecycle.sqlite3"
    if kind == "uninitialized":
        database.touch()
    elif kind == "corrupt":
        database.write_bytes(b"not a SQLite database")
    elif kind == "directory":
        database.mkdir()
    before = database.read_bytes() if database.is_file() else None

    with pytest.raises(LifecycleStoreError) as failure:
        await SQLiteLifecycleStore(database).list_pending()

    assert str(database) not in str(failure.value)
    if before is not None:
        assert database.read_bytes() == before
    elif kind == "missing":
        assert not database.exists()
    else:
        assert database.is_dir()


@pytest.mark.anyio
async def test_locked_pending_query_reports_error_and_preserves_record(tmp_path: Path) -> None:
    """A locked database yields a stable failure while retaining pending metadata."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database, lock_timeout=0)
    await store.initialize()
    original = pending_record("locked")
    await store.create(original)
    with closing(sqlite3.connect(database)) as lock:
        lock.execute("BEGIN EXCLUSIVE")
        try:
            with pytest.raises(LifecycleStoreError, match="lifecycle storage operation failed"):
                await store.list_pending()
        finally:
            lock.rollback()

    assert await store.get(original.sandbox_id) == original
    assert await store.list_pending() == [original]
