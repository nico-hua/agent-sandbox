"""Temporary SQLite original PVC receipts, atomic confirmation and explicit schema migration."""

import asyncio
import sqlite3
import threading
from contextlib import closing, contextmanager
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta

import pytest

from control_plane.core import models
from control_plane.storage import lifecycle_store as module
from control_plane.storage.lifecycle_store import (
    SQLiteLifecycleStore, LifecycleStoreConflict, LifecycleStoreError, LifecycleRecordNotFound, LifecycleSchemaError,
)
from control_plane.storage.run_lock import SQLiteRunLock
from test_workspace_store import workspace, lifecycle, binding, legacy_database, WORKSPACE_ID, TOKEN
from test_endpoint_credentials_store import make_v3

UID = "45667ab6-3f02-40ba-9b29-b1967503a3f0"
OTHER_UID = "55667ab6-3f02-40ba-9b29-b1967503a3f0"


def receipt(**changes):
    """Supply explicit trusted original-create evidence without an SDK object, generated ID or clock."""
    item = workspace()
    values = dict(workspace_id=item.workspace_id, creation_token=item.creation_token, volume_name=item.volume_name,
        image_id=item.image_id, created_at=item.created_at, backend="kubernetes", namespace="agent-sandbox-test",
        pvc_name="agent-sandbox-wsp-" + "a" * 32, pvc_uid=UID, storage_class="agent-sandbox-workspace",
        original_uid_verified=True)
    values.update(changes)
    return models.WorkspaceResourceReceipt(**values)


async def prepared(path):
    """Initialize only a test-owned database and insert an original pending workspace."""
    store = SQLiteLifecycleStore(path)
    await store.initialize()
    await store.create_workspace(workspace())
    return store


@pytest.mark.anyio
async def test_original_receipt_survives_store_reconstruction_without_changing_workspace(tmp_path):
    """Saving an original immutable receipt must survive reopen without advancing state, version or occupancy."""
    path = tmp_path / "store.sqlite3"
    store = await prepared(path)
    value = receipt()
    assert await store.save_workspace_receipt(value, expected_version=0) == value
    assert await store.get_workspace(WORKSPACE_ID) == workspace()
    reopened = SQLiteLifecycleStore(path)
    assert await reopened.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=0) == value
    with pytest.raises(FrozenInstanceError):
        value.pvc_uid = OTHER_UID


@pytest.mark.anyio
async def test_confirm_with_receipt_is_atomic_and_repeated_confirmation_keeps_cas(tmp_path):
    """Confirmation publishes both original receipt and available v1, and cannot use identical receipt as CAS bypass."""
    store = await prepared(tmp_path / "store.sqlite3")
    confirmed = await store.confirm_workspace_with_receipt(receipt(), expected_version=0)
    assert confirmed == replace(workspace(), version=1, management_state="available")
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=1) == receipt()
    with pytest.raises(LifecycleStoreConflict):
        await store.confirm_workspace_with_receipt(receipt(), expected_version=0)
    with pytest.raises(LifecycleStoreConflict, match="state_conflict"):
        await store.confirm_workspace_with_receipt(receipt(), expected_version=1)
    assert await store.get_workspace(WORKSPACE_ID) == confirmed


@pytest.mark.anyio
async def test_same_pending_receipt_save_is_idempotent_but_state_and_version_still_guard(tmp_path):
    """Identical saves are harmless only while the same original pending version remains valid."""
    store = await prepared(tmp_path / "store.sqlite3")
    assert await store.save_workspace_receipt(receipt(), expected_version=0) == receipt()
    assert await store.save_workspace_receipt(receipt(), expected_version=0) == receipt()
    with pytest.raises(LifecycleStoreConflict, match="version_conflict"):
        await store.save_workspace_receipt(receipt(), expected_version=1)
    active = await store.confirm_workspace_with_receipt(receipt(), expected_version=0)
    with pytest.raises(LifecycleStoreConflict, match="state_conflict"):
        await store.save_workspace_receipt(receipt(), expected_version=active.version)


@pytest.mark.anyio
@pytest.mark.parametrize("field,value", [("pvc_uid", OTHER_UID), ("namespace", "other"),
    ("pvc_name", "other-pvc"), ("storage_class", "other-storage")])
async def test_original_target_cannot_be_overwritten(tmp_path, field, value):
    """A competing original UID or locator cannot replace an already saved receipt or confirm with it."""
    store = await prepared(tmp_path / "store.sqlite3")
    await store.save_workspace_receipt(receipt(), expected_version=0)
    changed = receipt(**{field: value})
    for action in (store.save_workspace_receipt, store.confirm_workspace_with_receipt):
        with pytest.raises(LifecycleStoreConflict):
            await action(changed, expected_version=0)
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=0) == receipt()
    assert await store.get_workspace(WORKSPACE_ID) == workspace()


@pytest.mark.anyio
@pytest.mark.parametrize("field,value", [("creation_token", "wct_" + "c" * 32),
    ("image_id", "sha256:" + "d" * 64), ("created_at", workspace().created_at + timedelta(microseconds=1))])
async def test_receipt_cannot_change_workspace_immutable_identity(tmp_path, field, value):
    """A validly formed receipt for another original identity cannot mutate metadata or be stored beside it."""
    store = await prepared(tmp_path / "store.sqlite3")
    with pytest.raises(LifecycleStoreConflict):
        await store.confirm_workspace_with_receipt(receipt(**{field: value}), expected_version=0)
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=0) is None
    assert await store.get_workspace(WORKSPACE_ID) == workspace()


@pytest.mark.anyio
async def test_receipt_missing_workspace_and_stale_reads_fail_closed(tmp_path):
    """Absent workspace differs from a missing optional receipt and cannot be bypassed with an old token/version."""
    store = SQLiteLifecycleStore(tmp_path / "store.sqlite3")
    await store.initialize()
    with pytest.raises(LifecycleRecordNotFound):
        await store.save_workspace_receipt(receipt(), expected_version=0)
    with pytest.raises(LifecycleRecordNotFound):
        await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=0)
    await store.create_workspace(workspace())
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=0) is None
    with pytest.raises(LifecycleStoreConflict):
        await store.get_workspace_receipt(WORKSPACE_ID, "wct_" + "f" * 32, expected_version=0)
    with pytest.raises(LifecycleStoreConflict):
        await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=1)


@pytest.mark.anyio
async def test_occupancy_release_and_final_delete_preserve_then_cascade_receipt(tmp_path):
    """Business management/occupancy versions cannot rewrite the original backend identity."""
    path = tmp_path / "store.sqlite3"
    store = await prepared(path)
    await store.confirm_workspace_with_receipt(receipt(), expected_version=0)
    item = lifecycle()
    await store.create_with_workspace(item, binding(item), expected_workspace_version=1)
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=2) == receipt()
    await store.delete(item.sandbox_id, item.transaction_id, expected_version=0)
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=3) == receipt()
    deleting = await store.claim_workspace_deletion(WORKSPACE_ID, TOKEN, expected_version=3)
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=deleting.version) == receipt()
    await store.delete_workspace(WORKSPACE_ID, TOKEN, expected_version=deleting.version)
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM workspace_resource_receipts").fetchall() == []
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    newer = replace(workspace(), creation_token="wct_" + "f" * 32)
    await store.create_workspace(newer)
    with pytest.raises(LifecycleStoreConflict):
        await store.save_workspace_receipt(receipt(), expected_version=0)
    assert await store.get_workspace_receipt(WORKSPACE_ID, newer.creation_token, expected_version=0) is None


@pytest.mark.anyio
async def test_confirmation_insert_failure_rolls_back_receipt_and_business_transition(tmp_path, monkeypatch):
    """A failure after insertion cannot leave either half of a confirmation committed."""
    store = await prepared(tmp_path / "store.sqlite3")
    insert = module._insert_workspace_receipt

    def fail_after_insert(connection, value):
        """Exercise real SQL insertion before failing within the same transaction."""
        insert(connection, value)
        raise sqlite3.OperationalError("synthetic write failure")

    monkeypatch.setattr(module, "_insert_workspace_receipt", fail_after_insert)
    with pytest.raises(LifecycleStoreError):
        await store.confirm_workspace_with_receipt(receipt(), expected_version=0)
    assert await store.get_workspace(WORKSPACE_ID) == workspace()
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=0) is None


@pytest.mark.anyio
async def test_state_update_constraint_failure_rolls_back_inserted_receipt(tmp_path, monkeypatch):
    """A real SQL constraint failure during state update rolls back the preceding receipt insert."""
    store = await prepared(tmp_path / "store.sqlite3")
    original = store._connection

    @contextmanager
    def rejecting_connection(**kwargs):
        """Install a connection-local test trigger after schema validation, without changing durable DDL."""
        with original(**kwargs) as connection:
            connection.execute("CREATE TEMP TRIGGER reject_confirmation BEFORE UPDATE OF management_state ON workspaces "
                               "BEGIN SELECT RAISE(ABORT, 'synthetic constraint'); END")
            yield connection

    with monkeypatch.context() as patch:
        patch.setattr(store, "_connection", rejecting_connection)
        with pytest.raises(LifecycleStoreError):
            await store.confirm_workspace_with_receipt(receipt(), expected_version=0)
    assert await store.get_workspace(WORKSPACE_ID) == workspace()
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=0) is None


@pytest.mark.anyio
async def test_commit_failure_cannot_partially_publish_confirmation(tmp_path):
    """A held reader causes a real commit failure after writes; both table changes must roll back."""
    path = tmp_path / "store.sqlite3"
    store = await prepared(path)
    blocked = SQLiteLifecycleStore(path, lock_timeout=0)
    with closing(sqlite3.connect(path)) as reader:
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM workspaces").fetchall()
        with pytest.raises(LifecycleStoreError):
            await blocked.confirm_workspace_with_receipt(receipt(), expected_version=0)
        reader.rollback()
    assert await store.get_workspace(WORKSPACE_ID) == workspace()
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=0) is None


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["save", "confirm"])
async def test_independent_connections_racing_different_original_uids_have_one_winner(tmp_path, operation):
    """A write-lock transaction serializes competing receipt claims without replacing the winner."""
    path = tmp_path / "store.sqlite3"
    first = await prepared(path)
    second = SQLiteLifecycleStore(path)
    barrier = threading.Barrier(2)
    values = [receipt(), receipt(pvc_uid=OTHER_UID)]

    async def submit(store, value):
        """Start independent workers together, without timing assumptions or sharing a connection."""
        await asyncio.to_thread(barrier.wait, 3)
        method = store.save_workspace_receipt if operation == "save" else store.confirm_workspace_with_receipt
        return await method(value, expected_version=0)

    results = await asyncio.gather(submit(first, values[0]), submit(second, values[1]), return_exceptions=True)
    assert sum(isinstance(result, LifecycleStoreConflict) for result in results) == 1
    version = 0 if operation == "save" else 1
    saved = await first.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=version)
    assert saved in values


@pytest.mark.parametrize("field,value", [("backend", "docker"), ("namespace", ""), ("namespace", "other/ns"),
    ("pvc_uid", ""), ("pvc_uid", "bad"), ("storage_class", ""), ("storage_class", "BAD"),
    ("pvc_name", "a/b"), ("original_uid_verified", False), ("original_uid_verified", 1)])
def test_receipt_requires_explicit_trusted_original_uid_and_safe_targets(field, value):
    """Candidate proof flags, missing originals and malformed trusted locations cannot become persisted receipts."""
    with pytest.raises(ValueError):
        receipt(**{field: value})


def old_database(path, version):
    """Build exact historical test-only DDL and populated rows, never touching a development database."""
    if version == 2:
        legacy_database(path)
    elif version == 3:
        make_v3(path, occupied=True)
    else:
        with closing(sqlite3.connect(path)) as connection, connection:
            for _kind, _name, sql in module._V4_SCHEMA_DEFINITIONS:
                connection.execute(sql)
            item, sandbox = workspace(), lifecycle()
            created = module._microseconds(item.created_at)
            connection.execute("INSERT INTO workspaces VALUES (?, ?, ?, ?, ?, 2, 'available')",
                               (item.workspace_id, item.creation_token, item.volume_name, item.image_id, created))
            connection.execute("INSERT INTO lifecycle_records VALUES (?, ?, ?, ?, 0, 'pending', ?)",
                               (sandbox.sandbox_id, sandbox.transaction_id, created, created + 1000000, item.workspace_id))
            connection.execute("INSERT INTO workspace_bindings VALUES (?, ?, ?, ?, 2, ?, ?)",
                               (item.workspace_id, item.creation_token, item.volume_name, item.image_id,
                                sandbox.sandbox_id, sandbox.transaction_id))
            connection.execute("INSERT INTO endpoint_credentials VALUES (?, ?, ?)",
                               (sandbox.sandbox_id, sandbox.transaction_id, "A" * 43))
            connection.execute("PRAGMA user_version = 4")


def snapshot(path):
    """Capture owned schema and row data for exact temporary-database migration comparisons."""
    with closing(sqlite3.connect(path)) as connection:
        tables = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name")]
        return (connection.execute("PRAGMA user_version").fetchone()[0],
                connection.execute("SELECT type, name, sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY name").fetchall(),
                {name: connection.execute('SELECT * FROM "' + name + '"').fetchall() for name in tables})


@pytest.mark.anyio
@pytest.mark.parametrize("version", [2, 3, 4])
async def test_explicit_v5_upgrade_preserves_all_original_data_without_fabricating_receipts(tmp_path, version):
    """All supported historical paths preserve lifecycle, credentials and occupancy; old records gain no invented UID."""
    path = tmp_path / "legacy.sqlite3"
    old_database(path, version)
    before = snapshot(path)
    store = SQLiteLifecycleStore(path)
    lock = SQLiteRunLock(path)
    lock.acquire()
    try:
        with pytest.raises(LifecycleSchemaError):
            await store.initialize()
        assert snapshot(path) == before
        await store.upgrade()
        await store.upgrade()
        await store.initialize()
    finally:
        lock.release()
    after = snapshot(path)
    assert after[0] == 5
    unchanged = {name: after[2][name] for name in before[2]}
    if version == 2:
        assert after[2]["lifecycle_records"] == [tuple(row) + (None,) for row in before[2]["lifecycle_records"]]
    else:
        assert unchanged == before[2], "upgrade changed pre-existing data"
    assert after[2]["workspace_resource_receipts"] == []
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["unknown-version", "extra-table", "modified-ddl"])
async def test_migration_rejects_unverified_v4_structure_without_writes(tmp_path, fault):
    """Neither a version marker nor similar table names authorize adopting an unknown old database."""
    path = tmp_path / "legacy.sqlite3"
    old_database(path, 4)
    with closing(sqlite3.connect(path)) as connection, connection:
        if fault == "unknown-version":
            connection.execute("PRAGMA user_version = 99")
        elif fault == "extra-table":
            connection.execute("CREATE TABLE external_data (value TEXT)")
        else:
            connection.execute("DROP INDEX lifecycle_expiration_idx")
            connection.execute("CREATE INDEX lifecycle_expiration_idx ON lifecycle_records(sandbox_id)")
    before = snapshot(path)
    lock = SQLiteRunLock(path)
    lock.acquire()
    try:
        with pytest.raises(LifecycleSchemaError):
            await SQLiteLifecycleStore(path).upgrade()
    finally:
        lock.release()
    assert snapshot(path) == before


@pytest.mark.anyio
@pytest.mark.parametrize("version", [2, 3, 4])
async def test_upgrade_ddl_failure_rolls_back_new_table_index_trigger_and_version(tmp_path, monkeypatch, version):
    """Mid-upgrade SQL failure restores exact original DDL, version and rows, without deleting/rebuilding the test file."""
    path = tmp_path / "legacy.sqlite3"
    old_database(path, version)
    before = snapshot(path)
    monkeypatch.setattr(module, "_SCHEMA_DEFINITIONS", module._SCHEMA_DEFINITIONS + (("table", "bad", "INVALID SQL"),))
    lock = SQLiteRunLock(path)
    lock.acquire()
    try:
        with pytest.raises(LifecycleStoreError):
            await SQLiteLifecycleStore(path).upgrade()
    finally:
        lock.release()
    assert snapshot(path) == before


@pytest.mark.anyio
async def test_confirm_and_delete_race_preserves_original_cas(tmp_path):
    """A final deletion cannot erase pending confirmation, or a receipt resurrect an already claimed deletion."""
    path = tmp_path / "store.sqlite3"
    store = await prepared(path)
    other = SQLiteLifecycleStore(path)
    barrier = threading.Barrier(2)

    async def confirm():
        """Compete with final deletion from an independent worker using the pending version."""
        await asyncio.to_thread(barrier.wait, 3)
        return await store.confirm_workspace_with_receipt(receipt(), expected_version=0)

    async def remove():
        """Attempt final deletion with the same version; pending cannot satisfy its deleting-state predicate."""
        await asyncio.to_thread(barrier.wait, 3)
        return await other.delete_workspace(WORKSPACE_ID, TOKEN, expected_version=0)

    results = await asyncio.gather(confirm(), remove(), return_exceptions=True)
    assert sum(isinstance(result, LifecycleStoreConflict) for result in results) == 1
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=1) == receipt()


@pytest.mark.anyio
async def test_receipt_cascade_and_metadata_delete_roll_back_together(tmp_path, monkeypatch):
    """Failure during the actual FK cascade leaves both parent deletion claim and original receipt intact."""
    store = await prepared(tmp_path / "store.sqlite3")
    await store.confirm_workspace_with_receipt(receipt(), expected_version=0)
    deleting = await store.claim_workspace_deletion(WORKSPACE_ID, TOKEN, expected_version=1)
    original = store._connection

    @contextmanager
    def rejecting_connection(**kwargs):
        """Reject the child cascade in a connection-local trigger without modifying the owned main schema."""
        with original(**kwargs) as connection:
            connection.execute("CREATE TEMP TRIGGER reject_cascade BEFORE DELETE ON workspace_resource_receipts "
                               "BEGIN SELECT RAISE(ABORT, 'synthetic cascade failure'); END")
            yield connection

    with monkeypatch.context() as patch:
        patch.setattr(store, "_connection", rejecting_connection)
        with pytest.raises(LifecycleStoreError):
            await store.delete_workspace(WORKSPACE_ID, TOKEN, expected_version=deleting.version)
    assert await store.get_workspace(WORKSPACE_ID) == deleting
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=deleting.version) == receipt()


@pytest.mark.anyio
async def test_sql_foreign_keys_and_immutability_protect_original_receipt(tmp_path):
    """Schema constraints independently reject a receipt rewrite or a parent image/time identity change."""
    path = tmp_path / "store.sqlite3"
    store = await prepared(path)
    await store.save_workspace_receipt(receipt(), expected_version=0)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        for sql, values in (("UPDATE workspace_resource_receipts SET pvc_uid = ?", (OTHER_UID,)),
                            ("UPDATE workspaces SET created_at_us = created_at_us + 1", ()),
                            ("UPDATE workspaces SET image_id = ?", ("sha256:" + "f" * 64,))):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(sql, values)
            connection.rollback()
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=0) == receipt()


@pytest.mark.anyio
async def test_cancelled_wait_can_still_commit_original_confirmation(tmp_path, monkeypatch):
    """Cancelling or repeatedly cancelling a waiter never proves its SQLite worker did not commit."""
    path = tmp_path / "store.sqlite3"
    store = await prepared(path)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = store._workspace_transition_sync

    def delayed(*args, **kwargs):
        """Hold the worker after dispatch, then allow its real atomic transaction to finish."""
        entered.set()
        try:
            assert release.wait(4)
            return original(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(store, "_workspace_transition_sync", delayed)
    task = asyncio.create_task(store.confirm_workspace_with_receipt(receipt(), expected_version=0))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        assert await asyncio.to_thread(finished.wait, 3)
        reopened = SQLiteLifecycleStore(path)
        assert (await reopened.get_workspace(WORKSPACE_ID)).management_state == "available"
        assert await reopened.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=1) == receipt()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(finished.wait, 3)


@pytest.mark.anyio
async def test_lost_commit_acknowledgement_keeps_both_durable_facts(tmp_path, monkeypatch):
    """A post-commit error cannot justify deleting or re-confirming the successfully saved original workspace."""
    path = tmp_path / "store.sqlite3"
    store = await prepared(path)
    original = store._workspace_transition_sync

    def loses_ack(*args, **kwargs):
        """Commit through production SQL, then fail only the caller's acknowledgement."""
        original(*args, **kwargs)
        raise LifecycleStoreError()

    monkeypatch.setattr(store, "_workspace_transition_sync", loses_ack)
    with pytest.raises(LifecycleStoreError):
        await store.confirm_workspace_with_receipt(receipt(), expected_version=0)
    reopened = SQLiteLifecycleStore(path)
    assert (await reopened.get_workspace(WORKSPACE_ID)).version == 1
    assert await reopened.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=1) == receipt()
    with pytest.raises(LifecycleStoreConflict):
        await reopened.confirm_workspace_with_receipt(receipt(), expected_version=0)


@pytest.mark.anyio
async def test_final_delete_racing_stale_save_never_resurrects_receipt(tmp_path):
    """A deleting workspace can be removed while a late save is rejected by state or absence, never recreated."""
    path = tmp_path / "store.sqlite3"
    store = await prepared(path)
    await store.confirm_workspace_with_receipt(receipt(), expected_version=0)
    await store.claim_workspace_deletion(WORKSPACE_ID, TOKEN, expected_version=1)
    other = SQLiteLifecycleStore(path)
    barrier = threading.Barrier(2)

    async def save():
        """Attempt a late receipt save using the current deleting version but invalid lifecycle state."""
        await asyncio.to_thread(barrier.wait, 3)
        return await other.save_workspace_receipt(receipt(), expected_version=2)

    async def remove():
        """Remove parent and receipt together through the original final deletion CAS."""
        await asyncio.to_thread(barrier.wait, 3)
        return await store.delete_workspace(WORKSPACE_ID, TOKEN, expected_version=2)

    results = await asyncio.gather(save(), remove(), return_exceptions=True)
    assert isinstance(results[0], (LifecycleStoreConflict, LifecycleRecordNotFound)) and results[1] is None
    assert await store.get_workspace(WORKSPACE_ID) is None
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM workspace_resource_receipts").fetchall() == []


@pytest.mark.anyio
async def test_identical_concurrent_confirmation_is_not_idempotent_activation(tmp_path):
    """Even identical receipts cannot let two independent connections confirm the same pending version."""
    path = tmp_path / "store.sqlite3"
    store = await prepared(path)
    results = await asyncio.gather(
        store.confirm_workspace_with_receipt(receipt(), expected_version=0),
        SQLiteLifecycleStore(path).confirm_workspace_with_receipt(receipt(), expected_version=0),
        return_exceptions=True,
    )
    assert sum(isinstance(value, models.WorkspaceRecord) for value in results) == 1
    assert sum(isinstance(value, LifecycleStoreConflict) for value in results) == 1
    assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=1) == receipt()


@pytest.mark.anyio
@pytest.mark.parametrize("expected_version", [True, -1, 0.0, "0", 2**63])
async def test_invalid_receipt_versions_fail_before_io(tmp_path, expected_version):
    """Receipt operations retain strict integer CAS validation and cannot create a database through invalid input."""
    path = tmp_path / "absent.sqlite3"
    store = SQLiteLifecycleStore(path)
    with pytest.raises(ValueError):
        await store.save_workspace_receipt(receipt(), expected_version=expected_version)
    with pytest.raises(ValueError):
        await store.confirm_workspace_with_receipt(receipt(), expected_version=expected_version)
    with pytest.raises(ValueError):
        await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=expected_version)
    assert not path.exists()


@pytest.mark.anyio
async def test_forged_unverified_candidate_cannot_bypass_model_guard_at_store(tmp_path):
    """A caller mutating a frozen receipt cannot bypass Store revalidation or reach SQLite."""
    store = SQLiteLifecycleStore(tmp_path / "absent.sqlite3")
    candidate = receipt()
    object.__setattr__(candidate, "original_uid_verified", False)
    for action in (store.save_workspace_receipt, store.confirm_workspace_with_receipt):
        with pytest.raises(ValueError):
            await action(candidate, expected_version=0)
    assert not store.database_path.exists()


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["save", "confirm"])
async def test_worker_uses_validated_private_snapshot_after_dispatch(tmp_path, monkeypatch, operation):
    """Caller-side mutation after async validation cannot turn an original receipt into an unverified candidate write."""
    path = tmp_path / "store.sqlite3"
    store = await prepared(path)
    value, expected = receipt(), receipt()
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    attribute = "_save_workspace_receipt_sync" if operation == "save" else "_workspace_transition_sync"
    original = getattr(store, attribute)

    def gated(*args, **kwargs):
        """Pause the real transaction after dispatch so the caller can mutate only its retained source object."""
        entered.set()
        try:
            assert release.wait(4)
            return original(*args, **kwargs)
        finally:
            finished.set()

    monkeypatch.setattr(store, attribute, gated)
    method = store.save_workspace_receipt if operation == "save" else store.confirm_workspace_with_receipt
    task = asyncio.create_task(method(value, expected_version=0))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        object.__setattr__(value, "pvc_uid", OTHER_UID)
        object.__setattr__(value, "original_uid_verified", False)
        release.set()
        await task
        version = 0 if operation == "save" else 1
        assert await store.get_workspace_receipt(WORKSPACE_ID, TOKEN, expected_version=version) == expected
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        assert await asyncio.to_thread(finished.wait, 3)
