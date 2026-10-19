"""File-backed lifecycle storage tests without Docker or wall-clock waiting."""

import asyncio
import sqlite3
import subprocess
import sys
import threading
from collections import Counter
from contextlib import closing
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from control_plane.core.models import SandboxLifecycleRecord
from control_plane.storage.lifecycle_store import (
    LifecycleRecordNotFound,
    LifecycleSchemaError,
    LifecycleStoreConflict,
    LifecycleStoreError,
    SQLiteLifecycleStore,
)


CREATED_AT = datetime(2026, 10, 19, 8, 0, 0, 123456, tzinfo=UTC)
EXPIRES_AT = CREATED_AT + timedelta(seconds=1800)


def make_record(
    sandbox_id: str = "sandbox-a",
    *,
    created_at: datetime = CREATED_AT,
    expires_at: datetime = EXPIRES_AT,
    version: int = 0,
) -> SandboxLifecycleRecord:
    """Build fixed lifecycle metadata with meaningful microsecond precision."""
    return SandboxLifecycleRecord(
        sandbox_id=sandbox_id,
        transaction_id="transaction-a",
        created_at=created_at,
        expires_at=expires_at,
        version=version,
    )


@pytest.mark.anyio
async def test_initialize_repeats_without_losing_records(tmp_path: Path) -> None:
    """Explicit initialization may create parent directories and preserve records."""
    database = tmp_path / "nested" / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    assert not database.parent.exists()

    await store.initialize()
    await store.create(make_record())
    await store.initialize()
    await SQLiteLifecycleStore(database).initialize()

    assert await store.get("sandbox-a") == make_record()
    connection = sqlite3.connect(database)
    try:
        assert connection.execute("PRAGMA user_version").fetchone() == (2,)
    finally:
        connection.close()


def test_constructor_and_application_import_have_no_database_io(tmp_path: Path) -> None:
    """Fresh imports and store construction must never open SQLite connections."""
    database = tmp_path / "missing" / "lifecycle.sqlite3"
    script = """
import sqlite3
import sys

def forbidden_connect(*args, **kwargs):
    \"\"\"Reject any database operation during imports or construction.\"\"\"
    raise AssertionError("unexpected SQLite connection")

sqlite3.connect = forbidden_connect
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
import control_plane.app
SQLiteLifecycleStore(sys.argv[1])
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(database)],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    assert completed.returncode == 0, completed.stderr
    assert not database.parent.exists()


@pytest.mark.anyio
async def test_records_survive_new_store_and_new_process(tmp_path: Path) -> None:
    """Read the original values through independent objects and a fresh interpreter."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    await store.initialize()
    await store.create(make_record())
    restored = SQLiteLifecycleStore(str(database))
    assert await restored.get("sandbox-a") == make_record()
    assert await restored.get("missing") is None

    script = """
import asyncio
import sys
from datetime import UTC, datetime
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore

async def verify():
    \"\"\"Recover metadata without any process-local initialization state.\"\"\"
    record = await SQLiteLifecycleStore(sys.argv[1]).get("sandbox-a")
    assert record.sandbox_id == "sandbox-a"
    assert record.transaction_id == "transaction-a"
    assert record.created_at == datetime(2026, 10, 19, 8, 0, 0, 123456, tzinfo=UTC)
    assert record.expires_at == datetime(2026, 10, 19, 8, 30, 0, 123456, tzinfo=UTC)
    assert record.version == 0

asyncio.run(verify())
"""
    completed = subprocess.run(
        [sys.executable, "-c", script, str(database)],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        check=False,
        timeout=15,
    )
    assert completed.returncode == 0, completed.stderr


@pytest.mark.anyio
async def test_duplicate_id_does_not_replace_original_record(tmp_path: Path) -> None:
    """A duplicate identity must conflict even when other metadata differs."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    original = make_record()
    await store.create(original)
    replacement = replace(
        original,
        transaction_id="another-transaction",
        created_at=CREATED_AT + timedelta(seconds=1),
        expires_at=EXPIRES_AT + timedelta(seconds=1),
    )

    with pytest.raises(LifecycleStoreConflict) as failure:
        await store.create(replacement)

    assert failure.value.reason == "already_exists"
    assert await store.get(original.sandbox_id) == original


@pytest.mark.anyio
async def test_expiration_update_increments_version_and_preserves_identity(
    tmp_path: Path,
) -> None:
    """Changing expiration must preserve original IDs and creation time exactly."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    original = make_record()
    await store.create(original)
    expiration = EXPIRES_AT + timedelta(microseconds=1)

    changed = await store.update_expiration(
        original.sandbox_id,
        original.transaction_id,
        expected_version=0,
        expires_at=expiration,
    )

    assert changed == replace(original, expires_at=expiration, version=1)
    assert await store.get(original.sandbox_id) == changed
    changed_again = await store.update_expiration(
        original.sandbox_id,
        original.transaction_id,
        expected_version=1,
        expires_at=expiration,
    )
    assert changed_again == replace(changed, version=2)


@pytest.mark.anyio
async def test_concurrent_updates_with_same_version_have_one_winner(
    tmp_path: Path,
) -> None:
    """Two independent connections may never overwrite the same expected version."""
    database = tmp_path / "lifecycle.sqlite3"
    first = SQLiteLifecycleStore(database)
    second = SQLiteLifecycleStore(database)
    await first.initialize()
    original = make_record()
    await first.create(original)
    expirations = [EXPIRES_AT + timedelta(seconds=1), EXPIRES_AT + timedelta(seconds=2)]

    results = await asyncio.gather(
        first.update_expiration(
            original.sandbox_id,
            original.transaction_id,
            expected_version=0,
            expires_at=expirations[0],
        ),
        second.update_expiration(
            original.sandbox_id,
            original.transaction_id,
            expected_version=0,
            expires_at=expirations[1],
        ),
        return_exceptions=True,
    )

    winners = [result for result in results if isinstance(result, SandboxLifecycleRecord)]
    conflicts = [result for result in results if isinstance(result, LifecycleStoreConflict)]
    assert len(winners) == 1
    assert len(conflicts) == 1
    assert conflicts[0].reason == "version_conflict"
    assert winners[0].version == 1
    assert winners[0].expires_at in expirations
    assert await first.get(original.sandbox_id) == winners[0]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("transaction_id", "version", "reason"),
    [
        ("wrong-transaction", 0, "transaction_mismatch"),
        ("transaction-a", 1, "version_conflict"),
    ],
)
async def test_conflicting_update_preserves_original_record(
    tmp_path: Path, transaction_id: str, version: int, reason: str,
) -> None:
    """Transaction identity and version each guard an expiration change."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    await store.create(make_record())

    with pytest.raises(LifecycleStoreConflict) as failure:
        await store.update_expiration(
            "sandbox-a", transaction_id, expected_version=version,
            expires_at=EXPIRES_AT + timedelta(hours=1),
        )

    assert failure.value.reason == reason
    assert await store.get("sandbox-a") == make_record()


@pytest.mark.anyio
async def test_update_missing_record_reports_not_found(tmp_path: Path) -> None:
    """An update must distinguish a missing record from conflicting metadata."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    with pytest.raises(LifecycleRecordNotFound):
        await store.update_expiration(
            "missing", "transaction-a", expected_version=0, expires_at=EXPIRES_AT,
        )
    assert await store.get("missing") is None


@pytest.mark.anyio
async def test_expired_filter_keeps_microseconds_boundaries_order_and_limit(
    tmp_path: Path,
) -> None:
    """Expiry ordering uses precise instants and ID ties with an inclusive cutoff."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    before = make_record("before", expires_at=EXPIRES_AT - timedelta(microseconds=1))
    exact_a = make_record("exact-a")
    exact_b = make_record("exact-b")
    after = make_record("after", expires_at=EXPIRES_AT + timedelta(microseconds=1))
    for record in [after, exact_b, before, exact_a]:
        await store.create(record)

    assert await store.list_expired(CREATED_AT) == []
    assert await store.list_expired(EXPIRES_AT - timedelta(microseconds=1)) == [before]
    assert await store.list_expired(EXPIRES_AT) == [before, exact_a, exact_b]
    assert await store.list_expired(EXPIRES_AT, limit=2) == [before, exact_a]
    assert await store.list_expired(after.expires_at) == [before, exact_a, exact_b, after]


@pytest.mark.anyio
async def test_default_expired_limit_is_bounded(tmp_path: Path) -> None:
    """Omitting a limit must still bound a potentially large expiration query."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    for index in range(101):
        await store.create(make_record(f"sandbox-{index:03d}"))

    expired = await store.list_expired(EXPIRES_AT)

    assert len(expired) == 100
    assert [record.sandbox_id for record in expired] == [
        f"sandbox-{index:03d}" for index in range(100)
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("created_at", "expires_at"),
    [
        (
            datetime(1969, 12, 31, 23, 59, 59, 1, tzinfo=UTC),
            datetime(1969, 12, 31, 23, 59, 59, 999999, tzinfo=UTC),
        ),
        (
            datetime(2500, 1, 1, 0, 0, 0, 123457, tzinfo=UTC),
            datetime(2500, 1, 1, 0, 30, 0, 123458, tzinfo=UTC),
        ),
    ],
)
async def test_integer_time_roundtrip_avoids_float_precision_loss(
    tmp_path: Path, created_at: datetime, expires_at: datetime,
) -> None:
    """Pre-epoch and distant dates retain every microsecond after persistence."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    record = make_record(created_at=created_at, expires_at=expires_at)
    await store.create(record)

    assert await store.get(record.sandbox_id) == record
    assert await store.list_expired(expires_at - timedelta(microseconds=1)) == []
    assert await store.list_expired(expires_at) == [record]


@pytest.mark.anyio
async def test_delete_is_idempotent_and_identity_version_protected(tmp_path: Path) -> None:
    """Only the matching transaction and latest version may remove metadata."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    original = make_record()
    await store.create(original)
    updated = await store.update_expiration(
        original.sandbox_id, original.transaction_id,
        expected_version=0, expires_at=EXPIRES_AT + timedelta(seconds=1),
    )

    for transaction_id, version, reason in [
        ("wrong-transaction", 1, "transaction_mismatch"),
        (original.transaction_id, 0, "version_conflict"),
    ]:
        with pytest.raises(LifecycleStoreConflict) as failure:
            await store.delete(original.sandbox_id, transaction_id, expected_version=version)
        assert failure.value.reason == reason
        assert await store.get(original.sandbox_id) == updated

    await store.delete(original.sandbox_id, original.transaction_id, expected_version=1)
    assert await store.get(original.sandbox_id) is None
    await store.delete(original.sandbox_id, original.transaction_id, expected_version=1)
    assert await store.list_expired(EXPIRES_AT + timedelta(days=1)) == []


@pytest.mark.anyio
async def test_explicit_initialization_required_without_creating_database(
    tmp_path: Path,
) -> None:
    """A read or write before initialization must not silently create storage."""
    database = tmp_path / "missing" / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    operations = [
        store.get("sandbox-a"),
        store.create(make_record()),
        store.update_expiration(
            "sandbox-a", "transaction-a", expected_version=0, expires_at=EXPIRES_AT,
        ),
        store.list_expired(EXPIRES_AT),
        store.list_reap_candidates(EXPIRES_AT),
        store.activate("sandbox-a", "transaction-a", expected_version=0),
        store.mark_deleting("sandbox-a", "transaction-a", expected_version=0),
        store.claim_expired("sandbox-a", "transaction-a", expected_version=0, now=EXPIRES_AT),
        store.delete("sandbox-a", "transaction-a", expected_version=0),
    ]
    for operation in operations:
        with pytest.raises(LifecycleStoreError):
            await operation
        assert not database.parent.exists()


@pytest.mark.anyio
async def test_existing_empty_database_still_requires_initialization(tmp_path: Path) -> None:
    """An existing file with no supported schema must not initialize on reads."""
    database = tmp_path / "lifecycle.sqlite3"
    database.touch()
    store = SQLiteLifecycleStore(database)

    with pytest.raises(LifecycleStoreError):
        await store.get("sandbox-a")

    assert database.stat().st_size == 0
    await store.initialize()
    assert await store.get("sandbox-a") is None


@pytest.mark.anyio
@pytest.mark.parametrize("lock_kind", ["exclusive", "commit"])
async def test_database_lock_returns_stable_error_then_recovers(
    tmp_path: Path, lock_kind: str,
) -> None:
    """Lock and commit failures must roll back, release connections and preserve data."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database, lock_timeout=0.01)
    await store.initialize()
    await store.create(make_record())
    blocker = sqlite3.connect(database)
    try:
        blocker.execute("BEGIN EXCLUSIVE" if lock_kind == "exclusive" else "BEGIN")
        if lock_kind == "commit":
            # A shared reader permits the write but blocks its commit in rollback mode.
            blocker.execute("SELECT * FROM lifecycle_records").fetchall()
        with pytest.raises(LifecycleStoreError) as failure:
            await store.update_expiration(
                "sandbox-a", "transaction-a", expected_version=0,
                expires_at=EXPIRES_AT + timedelta(seconds=1),
            )
        assert str(failure.value) == "lifecycle storage operation failed"
    finally:
        blocker.rollback()
        blocker.close()

    assert await store.get("sandbox-a") == make_record()
    updated = await store.update_expiration(
        "sandbox-a", "transaction-a", expected_version=0,
        expires_at=EXPIRES_AT + timedelta(seconds=1),
    )
    assert updated.version == 1


@pytest.mark.anyio
@pytest.mark.parametrize("failure_kind", ["directory", "blocked_parent", "corruption"])
async def test_database_failures_have_stable_public_errors(
    tmp_path: Path, failure_kind: str,
) -> None:
    """Unusable paths and damaged files must hide raw filesystem and SQLite errors."""
    database = tmp_path / "lifecycle.sqlite3"
    if failure_kind == "directory":
        database.mkdir()
    elif failure_kind == "blocked_parent":
        database.write_text("parent is a file", encoding="utf-8")
        database /= "nested.sqlite3"
    else:
        database.write_bytes(b"this is not a SQLite database\x00")
    store = SQLiteLifecycleStore(database)

    for operation in [store.initialize(), store.get("sandbox-a")]:
        with pytest.raises(LifecycleStoreError) as failure:
            await operation
        assert str(failure.value) == "lifecycle storage operation failed"


@pytest.mark.anyio
async def test_unsupported_schema_version_is_explicit_and_preserves_data(
    tmp_path: Path,
) -> None:
    """Initialization must refuse incompatible schemas instead of resetting them."""
    database = tmp_path / "lifecycle.sqlite3"
    connection = sqlite3.connect(database)
    try:
        connection.execute("CREATE TABLE future_data (value TEXT NOT NULL)")
        connection.execute("INSERT INTO future_data VALUES (?)", ("preserve me",))
        connection.execute("PRAGMA user_version = 99")
        connection.commit()
    finally:
        connection.close()
    store = SQLiteLifecycleStore(database)
    failures = []
    for operation in [store.initialize(), store.get("sandbox-a")]:
        with pytest.raises(LifecycleSchemaError) as failure:
            await operation
        failures.append(str(failure.value))
    assert failures[0] == failures[1]
    assert failures[0] == "lifecycle database schema is incompatible"
    connection = sqlite3.connect(database)
    try:
        assert connection.execute("PRAGMA user_version").fetchone() == (99,)
        assert connection.execute("SELECT value FROM future_data").fetchall() == [
            ("preserve me",),
        ]
    finally:
        connection.close()


@pytest.mark.anyio
@pytest.mark.parametrize("existing_table", [False, True])
async def test_current_version_with_missing_or_wrong_schema_is_rejected(
    tmp_path: Path, existing_table: bool,
) -> None:
    """A version marker cannot make a missing or incompatible table acceptable."""
    database = tmp_path / "lifecycle.sqlite3"
    connection = sqlite3.connect(database)
    try:
        if existing_table:
            connection.execute("CREATE TABLE lifecycle_records (value TEXT)")
            connection.execute("INSERT INTO lifecycle_records VALUES (?)", ("preserve me",))
        connection.execute("PRAGMA user_version = 2")
        connection.commit()
    finally:
        connection.close()
    store = SQLiteLifecycleStore(database)

    for operation in [store.initialize(), store.get("sandbox-a")]:
        with pytest.raises(LifecycleSchemaError) as failure:
            await operation
        assert str(failure.value) == "lifecycle database schema is incompatible"

    connection = sqlite3.connect(database)
    try:
        if existing_table:
            assert connection.execute("SELECT value FROM lifecycle_records").fetchall() == [
                ("preserve me",),
            ]
        else:
            assert connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'",
            ).fetchall() == []
    finally:
        connection.close()


@pytest.mark.anyio
async def test_values_are_bound_as_sql_parameters(tmp_path: Path) -> None:
    """Opaque IDs containing SQL syntax must remain literal data in every operation."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    record = replace(
        make_record("sandbox'; DROP TABLE lifecycle_records; --"),
        transaction_id="transaction' OR 1=1 --",
    )
    await store.create(record)
    assert await store.get(record.sandbox_id) == record
    updated = await store.update_expiration(
        record.sandbox_id, record.transaction_id,
        expected_version=0, expires_at=EXPIRES_AT + timedelta(microseconds=1),
    )
    assert updated.version == 1
    await store.delete(record.sandbox_id, record.transaction_id, expected_version=1)
    await store.create(make_record())
    assert await store.get("sandbox-a") == make_record()


@pytest.mark.parametrize(
    "changes",
    [
        {"created_at": None},
        {"expires_at": None},
        {"created_at": CREATED_AT.replace(tzinfo=None)},
        {"expires_at": EXPIRES_AT.replace(tzinfo=None)},
        {"created_at": CREATED_AT.astimezone(timezone(timedelta(hours=8)))},
        {"expires_at": EXPIRES_AT.astimezone(timezone(timedelta(hours=8)))},
        {"expires_at": CREATED_AT - timedelta(microseconds=1)},
        {"version": -1},
        {"version": True},
        {"version": 1.0},
        {"version": "0"},
        {"sandbox_id": ""},
        {"sandbox_id": None},
        {"transaction_id": ""},
        {"transaction_id": 1},
    ],
)
def test_lifecycle_record_rejects_invalid_metadata(changes: dict) -> None:
    """Records require complete UTC times, opaque nonempty IDs and integer versions."""
    with pytest.raises((TypeError, ValueError)):
        replace(make_record(), **changes)


def test_lifecycle_record_is_immutable_and_allows_zero_duration() -> None:
    """The model preserves identity and permits expiration exactly at creation."""
    record = make_record(expires_at=CREATED_AT)
    assert record.version == 0
    with pytest.raises(FrozenInstanceError):
        record.version = 1


@pytest.mark.anyio
async def test_create_rejects_nonzero_initial_version(tmp_path: Path) -> None:
    """New records must start at version zero without persisting rejected inputs."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    with pytest.raises((TypeError, ValueError)):
        await store.create(make_record(version=1))
    assert await store.get("sandbox-a") is None


@pytest.mark.anyio
@pytest.mark.parametrize("invalid_limit", [0, -1, True, 1.5, "1", None])
async def test_expired_limit_requires_positive_integer(
    tmp_path: Path, invalid_limit: object,
) -> None:
    """Invalid query bounds must never degrade into an unbounded SQL query."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    with pytest.raises((TypeError, ValueError)):
        await store.list_expired(EXPIRES_AT, limit=invalid_limit)


@pytest.mark.anyio
@pytest.mark.parametrize("invalid_version", [-1, True, 1.5, "0", None])
async def test_modification_version_requires_nonnegative_integer(
    tmp_path: Path, invalid_version: object,
) -> None:
    """Update and delete must reject ambiguous or negative expected versions."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    await store.create(make_record())
    with pytest.raises((TypeError, ValueError)):
        await store.update_expiration(
            "sandbox-a", "transaction-a", expected_version=invalid_version,
            expires_at=EXPIRES_AT,
        )
    with pytest.raises((TypeError, ValueError)):
        await store.delete("sandbox-a", "transaction-a", expected_version=invalid_version)
    for operation in (store.activate, store.mark_deleting):
        with pytest.raises((TypeError, ValueError)):
            await operation("sandbox-a", "transaction-a", expected_version=invalid_version)
    with pytest.raises((TypeError, ValueError)):
        await store.claim_expired(
            "sandbox-a", "transaction-a", expected_version=invalid_version, now=EXPIRES_AT,
        )
    assert await store.get("sandbox-a") == make_record()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "invalid_time",
    [
        None,
        EXPIRES_AT.replace(tzinfo=None),
        EXPIRES_AT.astimezone(timezone(timedelta(hours=8))),
        "2026-10-19T08:30:00Z",
    ],
)
async def test_store_rejects_non_utc_query_and_update_times(
    tmp_path: Path, invalid_time: object,
) -> None:
    """Read cutoffs and expiration edits follow the same strict UTC model rule."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    await store.create(make_record())
    with pytest.raises((TypeError, ValueError)):
        await store.list_expired(invalid_time)
    with pytest.raises((TypeError, ValueError)):
        await store.list_reap_candidates(invalid_time)
    with pytest.raises((TypeError, ValueError)):
        await store.claim_expired(
            "sandbox-a", "transaction-a", expected_version=0, now=invalid_time,
        )
    with pytest.raises((TypeError, ValueError)):
        await store.update_expiration(
            "sandbox-a", "transaction-a", expected_version=0, expires_at=invalid_time,
        )
    assert await store.get("sandbox-a") == make_record()


@pytest.mark.anyio
async def test_update_rejects_expiration_before_creation_without_version_change(
    tmp_path: Path,
) -> None:
    """A rejected expiration must leave both stored time and version unchanged."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    await store.create(make_record())
    with pytest.raises((TypeError, ValueError)):
        await store.update_expiration(
            "sandbox-a", "transaction-a", expected_version=0,
            expires_at=CREATED_AT - timedelta(microseconds=1),
        )
    assert await store.get("sandbox-a") == make_record()


@pytest.mark.parametrize("invalid_timeout", [-1, True, float("inf"), float("nan"), "1"])
def test_lock_timeout_requires_finite_nonnegative_number(
    tmp_path: Path, invalid_timeout: object,
) -> None:
    """Lock waits must have a finite explicit bound rather than an invalid timeout."""
    with pytest.raises((TypeError, ValueError)):
        SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3", lock_timeout=invalid_timeout)
    assert list(tmp_path.iterdir()) == []


def test_memory_database_is_rejected() -> None:
    """Per-operation connections require a persistent file instead of isolated memory."""
    with pytest.raises((TypeError, ValueError)):
        SQLiteLifecycleStore(":memory:")


def test_nul_in_database_path_is_rejected_without_creating_alias(tmp_path: Path) -> None:
    """A URI-decoded NUL must never silently redirect SQLite to a truncated filename."""
    with pytest.raises(ValueError):
        SQLiteLifecycleStore(tmp_path / "unexpected\x00requested.sqlite3")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.anyio
async def test_connections_are_owned_and_closed_in_worker_threads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Success, conflicts and rollback paths close each connection in its worker."""
    loop_thread = threading.get_ident()
    events = []

    class TrackingConnection(sqlite3.Connection):
        """Record connection close calls without changing SQLite thread checking."""

        def close(self) -> None:
            """Record the connection owner before applying SQLite's normal close."""
            events.append(("close", self.ticket, threading.get_ident()))
            super().close()

    original_connect = sqlite3.connect

    def tracked_connect(*args, **kwargs) -> sqlite3.Connection:
        """Give each real connection a unique identifier and retain thread checks."""
        assert kwargs.get("check_same_thread", True) is True
        kwargs["factory"] = TrackingConnection
        connection = original_connect(*args, **kwargs)
        connection.ticket = len(events)
        events.append(("open", connection.ticket, threading.get_ident()))
        return connection

    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    await store.create(make_record())
    with pytest.raises(LifecycleStoreConflict):
        await store.create(make_record())
    with pytest.raises((TypeError, ValueError)):
        await store.update_expiration(
            "sandbox-a", "transaction-a", expected_version=0,
            expires_at=CREATED_AT - timedelta(microseconds=1),
        )
    updated = await store.update_expiration(
        "sandbox-a", "transaction-a", expected_version=0,
        expires_at=EXPIRES_AT + timedelta(microseconds=1),
    )
    assert await store.get("sandbox-a") == updated
    assert await store.list_expired(updated.expires_at) == [updated]
    active = await store.activate("sandbox-a", "transaction-a", expected_version=1)
    with pytest.raises(LifecycleStoreConflict):
        await store.activate("sandbox-a", "transaction-a", expected_version=active.version)
    assert await store.list_reap_candidates(active.expires_at) == [active]
    deleting = await store.claim_expired(
        "sandbox-a", "transaction-a", expected_version=active.version, now=active.expires_at,
    )
    assert await store.mark_deleting(
        "sandbox-a", "transaction-a", expected_version=deleting.version,
    ) == deleting
    await store.delete("sandbox-a", "transaction-a", expected_version=deleting.version)

    opened = [(ticket, worker) for event, ticket, worker in events if event == "open"]
    closed = [(ticket, worker) for event, ticket, worker in events if event == "close"]
    assert len(opened) >= 8
    assert Counter(opened) == Counter(closed)
    assert all(worker != loop_thread for _, worker in opened)


@pytest.mark.anyio
@pytest.mark.parametrize("table_name", ["sentinel", "sqliteXforeign"])
async def test_unversioned_database_is_not_adopted_or_repaired(
    tmp_path: Path, table_name: str,
) -> None:
    """An unrelated unversioned database must retain its schema and original data."""
    database = tmp_path / "lifecycle.sqlite3"
    with closing(sqlite3.connect(database)) as connection, connection:
        # These identifiers are fixed test cases, never caller-supplied SQL.
        connection.execute(f"CREATE TABLE {table_name} (value TEXT NOT NULL)")
        connection.execute(f"INSERT INTO {table_name} VALUES (?)", ("preserve me",))
    store = SQLiteLifecycleStore(database)

    with pytest.raises(LifecycleSchemaError):
        await store.initialize()
    with pytest.raises(LifecycleSchemaError):
        await store.get("sandbox-a")
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute(f"SELECT value FROM {table_name}").fetchall() == [("preserve me",)]
        assert connection.execute("PRAGMA user_version").fetchone() == (0,)


@pytest.mark.anyio
@pytest.mark.parametrize("alteration", ["trigger", "same_name_trigger", "missing_index", "weak_table"])
async def test_same_version_schema_changes_are_rejected_before_modification(
    tmp_path: Path, alteration: str,
) -> None:
    """Version alone must not accept triggers or layouts that change record semantics."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    await store.initialize()
    await store.create(make_record())
    with closing(sqlite3.connect(database)) as connection, connection:
        original_rows = connection.execute("SELECT * FROM lifecycle_records").fetchall()
        if alteration in {"trigger", "same_name_trigger"}:
            trigger_name = "change_creation"
            if alteration == "same_name_trigger":
                connection.execute("DROP INDEX lifecycle_expiration_idx")
                trigger_name = "lifecycle_expiration_idx"
            connection.execute(f"""
                CREATE TRIGGER {trigger_name} AFTER UPDATE ON lifecycle_records
                BEGIN
                    UPDATE lifecycle_records SET created_at_us = created_at_us + 1
                    WHERE sandbox_id = NEW.sandbox_id;
                END
            """)
            if alteration == "same_name_trigger":
                connection.execute(
                    "CREATE INDEX lifecycle_expiration_idx ON lifecycle_records (expires_at_us, sandbox_id)"
                )
        elif alteration == "missing_index":
            connection.execute("DROP INDEX lifecycle_expiration_idx")
        else:
            connection.execute("DROP TABLE lifecycle_records")
            connection.execute("""
                CREATE TABLE lifecycle_records (
                    sandbox_id TEXT PRIMARY KEY NOT NULL,
                    transaction_id TEXT NOT NULL,
                    created_at_us INTEGER NOT NULL,
                    expires_at_us INTEGER NOT NULL,
                    version INTEGER NOT NULL,
                    management_state TEXT NOT NULL
                )
            """)
            connection.executemany(
                "INSERT INTO lifecycle_records VALUES (?, ?, ?, ?, ?, ?)", original_rows,
            )
            connection.execute(
                "CREATE INDEX lifecycle_expiration_idx ON lifecycle_records (expires_at_us, sandbox_id)"
            )
    with pytest.raises(LifecycleSchemaError):
        await store.initialize()
    with pytest.raises(LifecycleSchemaError):
        await store.update_expiration(
            "sandbox-a", "transaction-a", expected_version=0,
            expires_at=EXPIRES_AT + timedelta(seconds=1),
        )
    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("SELECT * FROM lifecycle_records").fetchall() == original_rows
