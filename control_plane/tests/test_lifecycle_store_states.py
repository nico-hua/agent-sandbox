"""Lifecycle management transitions and atomic reaper claims using temporary SQLite files."""

import asyncio
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
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


def record(sandbox_id: str = "sandbox-a", *, expires_at: datetime = EXPIRES_AT) -> SandboxLifecycleRecord:
    """Build a pending transaction with fixed times and a unique sandbox identity."""
    return SandboxLifecycleRecord(sandbox_id, "transaction-a", CREATED_AT, expires_at)


@pytest.mark.anyio
async def test_pending_activation_and_deleting_persist_across_stores(tmp_path: Path) -> None:
    """Successful transitions increase versions while preserving identity and precise times."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    await store.initialize()
    original = record()
    await store.create(original)
    assert (await store.get("sandbox-a")).management_state == "pending"

    active = await store.activate("sandbox-a", "transaction-a", expected_version=0)
    assert active == replace(original, management_state="active", version=1)
    assert await SQLiteLifecycleStore(database).get("sandbox-a") == active

    deleting = await store.mark_deleting("sandbox-a", "transaction-a", expected_version=1)
    assert deleting == replace(original, management_state="deleting", version=2)
    assert await SQLiteLifecycleStore(database).get("sandbox-a") == deleting
    assert await store.mark_deleting("sandbox-a", "transaction-a", expected_version=2) == deleting

    await store.delete("sandbox-a", "transaction-a", expected_version=2)
    assert await store.get("sandbox-a") is None


@pytest.mark.anyio
async def test_pending_can_enter_manual_deletion_without_activation(tmp_path: Path) -> None:
    """Unconfirmed creation records remain available for explicit guarded cleanup."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    await store.create(record())

    deleting = await store.mark_deleting("sandbox-a", "transaction-a", expected_version=0)

    assert deleting.management_state == "deleting"
    assert deleting.version == 1


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["activate", "mark_deleting"])
@pytest.mark.parametrize(
    ("transaction_id", "expected_version", "reason"),
    [("different", 0, "transaction_mismatch"), ("transaction-a", 1, "version_conflict")],
)
async def test_state_changes_reject_wrong_identity_or_version(
    tmp_path: Path, operation: str, transaction_id: str, expected_version: int, reason: str,
) -> None:
    """A transition cannot refresh its own version or adopt another transaction."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    original = record()
    await store.create(original)

    with pytest.raises(LifecycleStoreConflict) as failure:
        await getattr(store, operation)("sandbox-a", transaction_id, expected_version=expected_version)

    assert failure.value.reason == reason
    assert await store.get("sandbox-a") == original


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["activate", "mark_deleting"])
async def test_state_change_requires_existing_record(tmp_path: Path, operation: str) -> None:
    """Missing metadata cannot be implicitly inserted by a management transition."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()

    with pytest.raises(LifecycleRecordNotFound):
        await getattr(store, operation)("missing", "transaction-a", expected_version=0)


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["active", "deleting"])
async def test_activation_cannot_reactivate_other_states(tmp_path: Path, state: str) -> None:
    """Activation confirms only pending creation and never reverses deletion."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    await store.create(record())
    current = await store.activate("sandbox-a", "transaction-a", expected_version=0)
    if state == "deleting":
        current = await store.mark_deleting("sandbox-a", "transaction-a", expected_version=current.version)

    with pytest.raises(LifecycleStoreConflict) as failure:
        await store.activate("sandbox-a", "transaction-a", expected_version=current.version)

    assert failure.value.reason == "state_conflict"
    assert await store.get("sandbox-a") == current


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["active", "deleting"])
async def test_create_cannot_insert_an_already_managed_state(tmp_path: Path, state: str) -> None:
    """All new persisted lifecycles start pending rather than bypassing confirmation."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()

    with pytest.raises(ValueError):
        await store.create(replace(record(), management_state=state))

    assert await store.get("sandbox-a") is None


@pytest.mark.parametrize("state", ["", "unknown", None, 1, ["pending"]])
def test_model_rejects_invalid_management_state(state: object) -> None:
    """Malformed states cannot become records eligible for later management actions."""
    with pytest.raises(ValueError):
        replace(record(), management_state=state)


@pytest.mark.anyio
async def test_candidates_exclude_pending_and_use_inclusive_precise_stable_order(tmp_path: Path) -> None:
    """Only due active and deleting rows are selected, with one-microsecond boundaries."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    specifications = [
        ("pending", EXPIRES_AT - timedelta(seconds=1), "pending"),
        ("before", EXPIRES_AT - timedelta(microseconds=1), "active"),
        ("exact-b", EXPIRES_AT, "active"),
        ("exact-a", EXPIRES_AT, "active"),
        ("after", EXPIRES_AT + timedelta(microseconds=1), "active"),
        ("retry", EXPIRES_AT + timedelta(seconds=1), "deleting"),
    ]
    for sandbox_id, expires_at, state in specifications:
        await store.create(record(sandbox_id, expires_at=expires_at))
        if state == "active":
            await store.activate(sandbox_id, "transaction-a", expected_version=0)
        elif state == "deleting":
            await store.mark_deleting(sandbox_id, "transaction-a", expected_version=0)

    assert [item.sandbox_id for item in await store.list_reap_candidates(EXPIRES_AT)] == [
        "before", "exact-a", "exact-b", "retry",
    ]
    assert [item.sandbox_id for item in await store.list_reap_candidates(EXPIRES_AT, limit=2)] == [
        "before", "exact-a",
    ]
    assert [item.sandbox_id for item in await store.list_reap_candidates(CREATED_AT)] == ["retry"]


@pytest.mark.anyio
async def test_default_reap_candidate_limit_is_bounded(tmp_path: Path) -> None:
    """A large retry backlog is still capped by the default batch limit."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    for index in range(101):
        sandbox_id = f"sandbox-{index:03d}"
        await store.create(record(sandbox_id))
        await store.mark_deleting(sandbox_id, "transaction-a", expected_version=0)

    candidates = await store.list_reap_candidates(CREATED_AT)

    assert [item.sandbox_id for item in candidates] == [f"sandbox-{index:03d}" for index in range(100)]


@pytest.mark.anyio
async def test_claim_has_inclusive_boundary_and_preserves_original_metadata(tmp_path: Path) -> None:
    """A claim moves exactly-due active metadata to deleting with one version increment."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    await store.create(record())
    active = await store.activate("sandbox-a", "transaction-a", expected_version=0)
    assert await store.claim_expired(
        "sandbox-a", "transaction-a", expected_version=1, now=EXPIRES_AT - timedelta(microseconds=1),
    ) is None
    assert await store.get("sandbox-a") == active

    claimed = await store.claim_expired("sandbox-a", "transaction-a", expected_version=1, now=EXPIRES_AT)

    assert claimed == replace(active, management_state="deleting", version=2)
    assert await store.get("sandbox-a") == claimed
    assert await store.claim_expired(
        "sandbox-a", "transaction-a", expected_version=2, now=EXPIRES_AT,
    ) is None


@pytest.mark.anyio
@pytest.mark.parametrize("case", ["missing", "transaction", "version", "pending", "deleting", "future"])
async def test_claim_skips_any_changed_or_ineligible_candidate(tmp_path: Path, case: str) -> None:
    """A stale candidate must not acquire permission to delete a different lifecycle."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    await store.create(record())
    current = record()
    if case != "pending":
        current = await store.activate("sandbox-a", "transaction-a", expected_version=0)
    if case == "deleting":
        current = await store.mark_deleting("sandbox-a", "transaction-a", expected_version=current.version)
    if case == "missing":
        await store.delete("sandbox-a", "transaction-a", expected_version=current.version)
        current = None
    expected_version = 99 if case == "version" else (current.version if current is not None else 1)

    claimed = await store.claim_expired(
        "sandbox-a", "different" if case == "transaction" else "transaction-a",
        expected_version=expected_version,
        now=EXPIRES_AT - timedelta(microseconds=1) if case == "future" else EXPIRES_AT,
    )

    assert claimed is None
    assert await store.get("sandbox-a") == current


@pytest.mark.anyio
async def test_concurrent_claims_on_same_file_have_only_one_winner(tmp_path: Path) -> None:
    """Separate connections cannot both move the same active version into deletion."""
    database = tmp_path / "lifecycle.sqlite3"
    first, second = SQLiteLifecycleStore(database), SQLiteLifecycleStore(database)
    await first.initialize()
    await first.create(record())
    await first.activate("sandbox-a", "transaction-a", expected_version=0)

    results = await asyncio.gather(*(
        store.claim_expired("sandbox-a", "transaction-a", expected_version=1, now=EXPIRES_AT)
        for store in (first, second)
    ))

    assert sum(item is not None for item in results) == 1
    assert results.count(None) == 1
    current = await second.get("sandbox-a")
    assert current.management_state == "deleting"
    assert current.version == 2


@pytest.mark.anyio
@pytest.mark.parametrize("first_operation", ["update", "claim"])
async def test_update_and_claim_cannot_both_change_same_active_version(
    tmp_path: Path, first_operation: str,
) -> None:
    """Either an extension invalidates the claim or a claim forbids later expiration edits."""
    database = tmp_path / "lifecycle.sqlite3"
    first, second = SQLiteLifecycleStore(database), SQLiteLifecycleStore(database)
    await first.initialize()
    await first.create(record())
    await first.activate("sandbox-a", "transaction-a", expected_version=0)

    if first_operation == "update":
        updated = await first.update_expiration(
            "sandbox-a", "transaction-a", expected_version=1, expires_at=EXPIRES_AT + timedelta(seconds=1),
        )
        assert await second.claim_expired(
            "sandbox-a", "transaction-a", expected_version=1, now=EXPIRES_AT,
        ) is None
        assert await second.claim_expired(
            "sandbox-a", "transaction-a", expected_version=2, now=EXPIRES_AT,
        ) is None
        assert await second.get("sandbox-a") == updated
    else:
        claimed = await first.claim_expired(
            "sandbox-a", "transaction-a", expected_version=1, now=EXPIRES_AT,
        )
        with pytest.raises(LifecycleStoreConflict) as failure:
            await second.update_expiration(
                "sandbox-a", "transaction-a", expected_version=claimed.version,
                expires_at=EXPIRES_AT + timedelta(seconds=1),
            )
        assert failure.value.reason == "state_conflict"
        assert await second.get("sandbox-a") == claimed


@pytest.mark.anyio
async def test_concurrent_update_and_claim_preserve_one_winner(tmp_path: Path) -> None:
    """Real competing file connections serialize extension and deletion ownership."""
    database = tmp_path / "lifecycle.sqlite3"
    first, second = SQLiteLifecycleStore(database), SQLiteLifecycleStore(database)
    await first.initialize()
    await first.create(record())
    await first.activate("sandbox-a", "transaction-a", expected_version=0)

    update, claim = await asyncio.gather(
        first.update_expiration(
            "sandbox-a", "transaction-a", expected_version=1, expires_at=EXPIRES_AT + timedelta(seconds=1),
        ),
        second.claim_expired("sandbox-a", "transaction-a", expected_version=1, now=EXPIRES_AT),
        return_exceptions=True,
    )

    current = await second.get("sandbox-a")
    assert current.version == 2
    if isinstance(update, SandboxLifecycleRecord):
        assert claim is None
        assert current == update
        assert current.management_state == "active"
    else:
        assert isinstance(update, LifecycleStoreConflict)
        assert current == claim
        assert current.management_state == "deleting"


@pytest.mark.anyio
async def test_legacy_database_is_refused_without_migrating_or_changing_data(tmp_path: Path) -> None:
    """Version-one databases stay untouched; this feature requires an explicit new database."""
    database = tmp_path / "legacy.sqlite3"
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute("""
            CREATE TABLE lifecycle_records (
                sandbox_id TEXT PRIMARY KEY NOT NULL,
                transaction_id TEXT NOT NULL,
                created_at_us INTEGER NOT NULL,
                expires_at_us INTEGER NOT NULL CHECK (expires_at_us >= created_at_us),
                version INTEGER NOT NULL CHECK (version >= 0)
            ) STRICT
        """)
        connection.execute("CREATE INDEX lifecycle_expiration_idx ON lifecycle_records (expires_at_us, sandbox_id)")
        connection.execute("INSERT INTO lifecycle_records VALUES (?, ?, ?, ?, ?)", ("old", "txn", 1, 1800000001, 7))
        connection.execute("PRAGMA user_version = 1")
    store = SQLiteLifecycleStore(database)

    with pytest.raises(LifecycleSchemaError):
        await store.initialize()
    with pytest.raises(LifecycleSchemaError):
        await store.get("old")

    with closing(sqlite3.connect(database)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (1,)
        assert connection.execute("SELECT * FROM lifecycle_records").fetchall() == [("old", "txn", 1, 1800000001, 7)]


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["activate", "mark_deleting", "claim_expired"])
@pytest.mark.parametrize("failure_kind", ["lock", "commit"])
async def test_failed_state_write_rolls_back_and_recovers(
    tmp_path: Path, operation: str, failure_kind: str,
) -> None:
    """Failed write locks and commits preserve the original version for explicit retry."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database, lock_timeout=0)
    await store.initialize()
    await store.create(record())
    current = record()
    if operation != "activate":
        current = await store.activate("sandbox-a", "transaction-a", expected_version=0)
    arguments = {"expected_version": current.version}
    if operation == "claim_expired":
        arguments["now"] = EXPIRES_AT
    with closing(sqlite3.connect(database)) as blocker:
        blocker.execute("BEGIN IMMEDIATE" if failure_kind == "lock" else "BEGIN")
        if failure_kind == "commit":
            blocker.execute("SELECT * FROM lifecycle_records").fetchall()
        try:
            with pytest.raises(LifecycleStoreError):
                await getattr(store, operation)("sandbox-a", "transaction-a", **arguments)
        finally:
            blocker.rollback()

    assert await store.get("sandbox-a") == current
    changed = await getattr(store, operation)("sandbox-a", "transaction-a", **arguments)
    assert changed.version == current.version + 1


@pytest.mark.anyio
@pytest.mark.parametrize("invalid_limit", [0, -1, True, 1.5, "1", None])
async def test_reap_candidates_reject_unbounded_or_invalid_limit(tmp_path: Path, invalid_limit: object) -> None:
    """Candidate reads cannot silently become unbounded through invalid SQL limits."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()

    with pytest.raises(ValueError):
        await store.list_reap_candidates(EXPIRES_AT, limit=invalid_limit)
