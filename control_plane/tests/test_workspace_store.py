"""Verify explicit migration and durable exclusive workspace occupancy."""

import asyncio
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from control_plane.storage import lifecycle_store as module
from control_plane.storage.lifecycle_store import LifecycleSchemaError, LifecycleStoreConflict, LifecycleStoreError, SQLiteLifecycleStore

NOW = datetime(2026, 11, 3, tzinfo=UTC)
WORKSPACE_ID = 'wsp_' + 'a' * 32
TOKEN = 'wct_' + 'b' * 32


def workspace():
    """Build the canonical pending workspace using the shared model."""
    from control_plane.core.models import WorkspaceRecord
    return WorkspaceRecord(WORKSPACE_ID, TOKEN, 'agent-sandbox-' + WORKSPACE_ID, 'sha256:' + 'c' * 64, NOW)


def lifecycle(sandbox_id='sbx_' + 'a' * 32, transaction_id='txn_' + 'a' * 32):
    """Build pending metadata pointing at the explicitly selected workspace."""
    from control_plane.core.models import SandboxLifecycleRecord
    return SandboxLifecycleRecord(sandbox_id, transaction_id, NOW, NOW + timedelta(hours=1), workspace_id=WORKSPACE_ID)


def binding(record, version=2):
    """Describe the workspace version resulting from acquisition."""
    from control_plane.core.models import WorkspaceBinding
    item = workspace()
    return WorkspaceBinding(item.workspace_id, item.creation_token, item.volume_name, item.image_id, version, record.sandbox_id, record.transaction_id)


def legacy_database(path):
    """Create exactly the deployed v2 schema with retained lifecycle data."""
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute('''CREATE TABLE lifecycle_records (
            sandbox_id TEXT PRIMARY KEY NOT NULL,
            transaction_id TEXT NOT NULL,
            created_at_us INTEGER NOT NULL,
            expires_at_us INTEGER NOT NULL CHECK (expires_at_us >= created_at_us),
            version INTEGER NOT NULL CHECK (version >= 0),
            management_state TEXT NOT NULL
                CHECK (management_state IN ('pending', 'active', 'deleting'))
        ) STRICT''')
        connection.execute('CREATE INDEX lifecycle_expiration_idx ON lifecycle_records (expires_at_us, sandbox_id)')
        connection.execute("INSERT INTO lifecycle_records VALUES ('legacy', 'old', 1, 3, 7, 'active')")
        connection.execute('PRAGMA user_version = 2')


@pytest.mark.anyio
async def test_initialize_refuses_v2_without_modification(tmp_path):
    """Only explicit upgrade may modify deployed lifecycle schemas."""
    path = tmp_path / 'store.sqlite3'
    legacy_database(path)
    with pytest.raises(LifecycleSchemaError):
        await SQLiteLifecycleStore(path).initialize()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute('PRAGMA user_version').fetchone() == (2,)
        assert connection.execute('SELECT * FROM lifecycle_records').fetchall() == [('legacy', 'old', 1, 3, 7, 'active')]


@pytest.mark.anyio
async def test_explicit_upgrade_preserves_legacy_metadata(tmp_path):
    """Verified v2 upgrades preserve original identities, versions and exact times."""
    path = tmp_path / 'store.sqlite3'
    legacy_database(path)
    store = SQLiteLifecycleStore(path)
    await store.upgrade()
    await store.upgrade()
    await store.initialize()
    old = await store.get('legacy')
    assert old.transaction_id == 'old' and old.version == 7 and old.workspace_id is None
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute('PRAGMA user_version').fetchone() == (3,)
        assert connection.execute('SELECT * FROM lifecycle_records').fetchall() == [('legacy', 'old', 1, 3, 7, 'active', None)]


@pytest.mark.anyio
async def test_upgrade_rejects_modified_v2(tmp_path):
    """A version marker cannot authorize migration of altered schemas."""
    path = tmp_path / 'store.sqlite3'
    legacy_database(path)
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute('CREATE TABLE unrelated(value TEXT)')
    with pytest.raises(LifecycleSchemaError):
        await SQLiteLifecycleStore(path).upgrade()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute('PRAGMA user_version').fetchone() == (2,)
        assert len(connection.execute('PRAGMA table_info(lifecycle_records)').fetchall()) == 6


@pytest.mark.anyio
async def test_upgrade_failure_rolls_back_schema(tmp_path, monkeypatch):
    """DDL and version marker roll back together when an upgrade step fails."""
    path = tmp_path / 'store.sqlite3'
    legacy_database(path)
    monkeypatch.setattr(module, '_SCHEMA_DEFINITIONS', module._SCHEMA_DEFINITIONS + (('table', 'bad', 'INVALID SQL'),))
    with pytest.raises(LifecycleStoreError):
        await SQLiteLifecycleStore(path).upgrade()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute('PRAGMA user_version').fetchone() == (2,)
        assert connection.execute('SELECT * FROM lifecycle_records').fetchall() == [('legacy', 'old', 1, 3, 7, 'active')]
        assert connection.execute("SELECT name FROM sqlite_schema WHERE name='workspaces'").fetchall() == []


@pytest.mark.anyio
async def test_workspace_acquire_release_and_reuse(tmp_path):
    """Occupancy persists until guarded cleanup and permits later sequential reuse."""
    store = SQLiteLifecycleStore(tmp_path / 'store.sqlite3')
    await store.initialize()
    await store.create_workspace(workspace())
    available = await store.confirm_workspace(WORKSPACE_ID, TOKEN, expected_version=0)
    assert available.version == 1 and available.management_state == 'available'
    item = lifecycle()
    await store.create_with_workspace(item, binding(item), expected_workspace_version=1)
    occupied = await store.get_workspace(WORKSPACE_ID)
    assert occupied.sandbox_id == item.sandbox_id and occupied.version == 2
    assert await store.get_workspace_binding(item.sandbox_id, item.transaction_id) == binding(item)
    assert await store.get(item.sandbox_id) == item
    with pytest.raises(LifecycleStoreConflict):
        await store.delete(item.sandbox_id, 'stale', expected_version=0)
    assert await store.get_workspace(WORKSPACE_ID) == occupied
    await store.delete(item.sandbox_id, item.transaction_id, expected_version=0)
    await store.delete(item.sandbox_id, item.transaction_id, expected_version=0)
    free = await store.get_workspace(WORKSPACE_ID)
    assert free.version == 3 and free.sandbox_id is None
    newer = lifecycle('sbx_' + 'b' * 32, 'txn_' + 'b' * 32)
    await store.create_with_workspace(newer, binding(newer, 4), expected_workspace_version=3)
    await store.delete(item.sandbox_id, item.transaction_id, expected_version=0)
    assert (await store.get_workspace(WORKSPACE_ID)).sandbox_id == newer.sandbox_id


@pytest.mark.anyio
async def test_competing_acquisitions_have_one_winner(tmp_path):
    """Independent connections cannot both reserve one available workspace."""
    path = tmp_path / 'store.sqlite3'
    first, second = SQLiteLifecycleStore(path), SQLiteLifecycleStore(path)
    await first.initialize()
    await first.create_workspace(workspace())
    await first.confirm_workspace(WORKSPACE_ID, TOKEN, expected_version=0)
    records = [lifecycle(), lifecycle('sbx_' + 'b' * 32, 'txn_' + 'b' * 32)]
    results = await asyncio.gather(*(store.create_with_workspace(record, binding(record), expected_workspace_version=1) for store, record in zip((first, second), records)), return_exceptions=True)
    assert sum(result is None for result in results) == 1
    assert sum(isinstance(result, LifecycleStoreConflict) for result in results) == 1
    persisted = await asyncio.gather(*(first.get(record.sandbox_id) for record in records))
    assert sum(record is not None for record in persisted) == 1


@pytest.mark.anyio
@pytest.mark.parametrize('changed', ['token', 'image', 'volume', 'version', 'lifecycle', 'pending'])
async def test_failed_acquisition_has_no_partial_metadata(tmp_path, changed):
    """Stale or forged bindings cannot prewrite an unoccupied lifecycle."""
    store = SQLiteLifecycleStore(tmp_path / 'store.sqlite3')
    await store.initialize()
    await store.create_workspace(workspace())
    if changed != 'pending':
        await store.confirm_workspace(WORKSPACE_ID, TOKEN, expected_version=0)
    record = lifecycle()
    candidate = binding(record)
    overrides = {'token': {'creation_token': 'wct_' + 'c' * 32}, 'image': {'image_id': 'sha256:' + 'd' * 64}, 'volume': {'volume_name': 'other'}, 'version': {'version': 8}, 'lifecycle': {'transaction_id': 'other'}, 'pending': {}}
    with pytest.raises((LifecycleStoreConflict, ValueError)):
        candidate = replace(candidate, **overrides[changed])
        await store.create_with_workspace(record, candidate, expected_workspace_version=1)
    assert await store.get(record.sandbox_id) is None
    assert (await store.get_workspace(WORKSPACE_ID)).sandbox_id is None


@pytest.mark.anyio
async def test_workspace_delete_claim_blocks_acquisition(tmp_path):
    """Deletion persists before runtime work and exact retries preserve the claim version."""
    store = SQLiteLifecycleStore(tmp_path / 'store.sqlite3')
    await store.initialize()
    await store.create_workspace(workspace())
    with pytest.raises(LifecycleStoreConflict):
        await store.claim_workspace_deletion(WORKSPACE_ID, TOKEN, expected_version=0)
    await store.confirm_workspace(WORKSPACE_ID, TOKEN, expected_version=0)
    deleting = await store.claim_workspace_deletion(WORKSPACE_ID, TOKEN, expected_version=1)
    assert deleting.management_state == 'deleting' and deleting.version == 2
    assert await store.claim_workspace_deletion(WORKSPACE_ID, TOKEN, expected_version=2) == deleting
    with pytest.raises(LifecycleStoreConflict):
        item = lifecycle()
        await store.create_with_workspace(item, binding(item, 3), expected_workspace_version=2)
    await store.delete_workspace(WORKSPACE_ID, TOKEN, expected_version=2)
    await store.delete_workspace(WORKSPACE_ID, TOKEN, expected_version=2)
    assert await store.get_workspace(WORKSPACE_ID) is None


@pytest.mark.anyio
async def test_occupied_workspace_cannot_be_deleted(tmp_path):
    """Neither deletion operation can detach an occupied workspace."""
    store = SQLiteLifecycleStore(tmp_path / 'store.sqlite3')
    await store.initialize()
    await store.create_workspace(workspace())
    await store.confirm_workspace(WORKSPACE_ID, TOKEN, expected_version=0)
    item = lifecycle()
    await store.create_with_workspace(item, binding(item), expected_workspace_version=1)
    with pytest.raises(LifecycleStoreConflict):
        await store.claim_workspace_deletion(WORKSPACE_ID, TOKEN, expected_version=2)
    with pytest.raises(LifecycleStoreConflict):
        await store.delete_workspace(WORKSPACE_ID, TOKEN, expected_version=2)
    assert await store.get_workspace_binding(item.sandbox_id, item.transaction_id) == binding(item)


@pytest.mark.anyio
@pytest.mark.parametrize('operation', ['acquire', 'release'])
@pytest.mark.parametrize('failure_kind', ['lock', 'commit'])
async def test_workspace_transaction_failure_rolls_back_and_retries(tmp_path, operation, failure_kind):
    """Lock and commit failures preserve occupancy and lifecycle for an explicit retry."""
    path = tmp_path / 'store.sqlite3'
    store = SQLiteLifecycleStore(path, lock_timeout=0)
    await store.initialize()
    await store.create_workspace(workspace())
    await store.confirm_workspace(WORKSPACE_ID, TOKEN, expected_version=0)
    item = lifecycle()
    if operation == 'release':
        await store.create_with_workspace(item, binding(item), expected_workspace_version=1)
    before = await store.get_workspace(WORKSPACE_ID)
    with closing(sqlite3.connect(path)) as blocker:
        blocker.execute('BEGIN IMMEDIATE' if failure_kind == 'lock' else 'BEGIN')
        if failure_kind == 'commit':
            blocker.execute('SELECT * FROM workspaces').fetchall()
        try:
            with pytest.raises(LifecycleStoreError):
                if operation == 'acquire':
                    await store.create_with_workspace(item, binding(item), expected_workspace_version=1)
                else:
                    await store.delete(item.sandbox_id, item.transaction_id, expected_version=0)
        finally:
            blocker.rollback()
    assert await store.get_workspace(WORKSPACE_ID) == before
    assert await store.get(item.sandbox_id) == (item if operation == 'release' else None)
    if operation == 'acquire':
        await store.create_with_workspace(item, binding(item), expected_workspace_version=1)
    else:
        await store.delete(item.sandbox_id, item.transaction_id, expected_version=0)
    assert (await store.get_workspace(WORKSPACE_ID)).version == before.version + 1


@pytest.mark.anyio
async def test_workspace_claim_and_acquisition_cannot_both_win(tmp_path):
    """Deletion and acquisition serialize across connections on the same workspace version."""
    path = tmp_path / 'store.sqlite3'
    first, second = SQLiteLifecycleStore(path), SQLiteLifecycleStore(path)
    await first.initialize()
    await first.create_workspace(workspace())
    await first.confirm_workspace(WORKSPACE_ID, TOKEN, expected_version=0)
    item = lifecycle()
    results = await asyncio.gather(
        first.claim_workspace_deletion(WORKSPACE_ID, TOKEN, expected_version=1),
        second.create_with_workspace(item, binding(item), expected_workspace_version=1),
        return_exceptions=True,
    )
    assert sum(isinstance(result, LifecycleStoreConflict) for result in results) == 1
    current = await first.get_workspace(WORKSPACE_ID)
    assert current.version == 2
    assert (current.management_state == 'deleting') != (current.sandbox_id == item.sandbox_id)


@pytest.mark.anyio
@pytest.mark.parametrize('corruption', ['missing_binding', 'binding_version', 'wrong_transaction', 'orphan'])
async def test_inconsistent_binding_is_rejected_without_release(tmp_path, corruption):
    """Contradictions fail closed during binding reads and guarded lifecycle deletion."""
    path = tmp_path / 'store.sqlite3'
    store = SQLiteLifecycleStore(path)
    await store.initialize()
    await store.create_workspace(workspace())
    await store.confirm_workspace(WORKSPACE_ID, TOKEN, expected_version=0)
    item = lifecycle()
    await store.create_with_workspace(item, binding(item), expected_workspace_version=1)
    statements = {
        'missing_binding': 'DELETE FROM workspace_bindings',
        'binding_version': 'UPDATE workspace_bindings SET version = version + 1',
        'wrong_transaction': "UPDATE workspace_bindings SET transaction_id = 'wrong'",
        'orphan': 'DELETE FROM lifecycle_records',
    }
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(statements[corruption])
    before = path.read_bytes()
    with pytest.raises(LifecycleStoreError):
        await store.get_workspace_binding(item.sandbox_id, item.transaction_id)
    with pytest.raises(LifecycleStoreError):
        await store.delete(item.sandbox_id, item.transaction_id, expected_version=0)
    assert path.read_bytes() == before


@pytest.mark.anyio
async def test_sql_constraints_protect_occupancy_identities(tmp_path):
    """SQL constraints independently reject duplicate occupancy and forged identity copies."""
    path = tmp_path / 'store.sqlite3'
    store = SQLiteLifecycleStore(path)
    await store.initialize()
    await store.create_workspace(workspace())
    await store.confirm_workspace(WORKSPACE_ID, TOKEN, expected_version=0)
    item = lifecycle()
    await store.create_with_workspace(item, binding(item), expected_workspace_version=1)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute('PRAGMA foreign_keys = ON')
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute('INSERT INTO workspace_bindings SELECT * FROM workspace_bindings')
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("UPDATE workspace_bindings SET image_id = 'forged'")
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute("UPDATE lifecycle_records SET transaction_id = 'forged'")


@pytest.mark.anyio
async def test_private_create_cannot_bypass_workspace_acquisition(tmp_path):
    """The legacy insert path cannot persist an explicit workspace without occupancy."""
    store = SQLiteLifecycleStore(tmp_path / 'store.sqlite3')
    await store.initialize()
    with pytest.raises(ValueError):
        await store.create(lifecycle())
    assert await store.get(lifecycle().sandbox_id) is None


@pytest.mark.anyio
async def test_sql_rejects_second_lifecycle_for_workspace(tmp_path):
    """A workspace cannot have a second lifecycle reference even outside Store methods."""
    path = tmp_path / 'store.sqlite3'
    store = SQLiteLifecycleStore(path)
    await store.initialize()
    await store.create_workspace(workspace())
    await store.confirm_workspace(WORKSPACE_ID, TOKEN, expected_version=0)
    item = lifecycle()
    await store.create_with_workspace(item, binding(item), expected_workspace_version=1)
    with closing(sqlite3.connect(path)) as connection:
        connection.execute('PRAGMA foreign_keys = ON')
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO lifecycle_records SELECT 'second', 'other', created_at_us, expires_at_us, 0, 'pending', workspace_id FROM lifecycle_records",
            )
