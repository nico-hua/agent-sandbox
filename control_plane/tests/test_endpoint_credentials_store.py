"""Exercise durable endpoint credentials, transaction guards and explicit schema upgrades."""

import os
import sqlite3
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from control_plane.core import models
from control_plane.storage import lifecycle_store as module
from control_plane.storage.lifecycle_store import (
    LifecycleSchemaError, LifecycleStoreConflict, LifecycleStoreError, SQLiteLifecycleStore,
)

NOW = datetime(2026, 11, 4, 0, 0, 0, 123456, tzinfo=UTC)
TOKEN = "a" * 42 + "_"
WORKSPACE_ID = "wsp_" + "a" * 32


def record():
    """Build stable pending metadata without any secret-bearing fields."""
    return models.SandboxLifecycleRecord("sandbox", "transaction", NOW, NOW + timedelta(hours=1))


def credential(item=None, token=TOKEN):
    """Bind one synthetic bearer credential to its original lifecycle identity."""
    item = item or record()
    return models.SandboxAccessCredential(item.sandbox_id, item.transaction_id, token)


def make_v3(path, *, occupied=False):
    """Build the exact deployed v3 DDL with optional original workspace occupancy."""
    with closing(sqlite3.connect(path)) as connection, connection:
        for _kind, _name, sql in module._V3_SCHEMA_DEFINITIONS:
            connection.execute(sql)
        connection.execute("PRAGMA user_version = 3")
        connection.execute("INSERT INTO workspaces VALUES (?, 'creation', 'volume', 'image', 7, 9, 'available')", (WORKSPACE_ID,))
        connection.execute("INSERT INTO lifecycle_records VALUES ('sandbox', 'transaction', 11, 33, 6, 'active', ?)", (WORKSPACE_ID if occupied else None,))
        if occupied:
            connection.execute("INSERT INTO workspace_bindings VALUES (?, 'creation', 'volume', 'image', 9, 'sandbox', 'transaction')", (WORKSPACE_ID,))


@pytest.mark.anyio
async def test_new_database_and_directory_have_private_permissions(tmp_path):
    """Fresh storage is private even when the process umask allows other-user access."""
    path = tmp_path / "private" / "store.sqlite3"
    store = SQLiteLifecycleStore(path)
    assert not path.parent.exists()
    await store.initialize()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (4,)
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600
        assert path.parent.stat().st_mode & 0o777 == 0o700


@pytest.mark.anyio
async def test_credential_survives_restart_and_is_not_ordinary_metadata(tmp_path):
    """Explicit credential reads survive reopening while lifecycle reads contain no token."""
    path = tmp_path / "store.sqlite3"
    store = SQLiteLifecycleStore(path)
    await store.initialize()
    await store.create(record(), credential=credential())
    reopened = SQLiteLifecycleStore(path)
    assert await reopened.get_credential("sandbox", "transaction") == credential()
    assert await reopened.get("sandbox") == record()
    assert TOKEN not in repr(await reopened.get("sandbox"))
    assert TOKEN not in repr(await reopened.list_pending())
    assert TOKEN not in repr(credential())


@pytest.mark.anyio
async def test_legacy_record_has_no_credential(tmp_path):
    """Legacy metadata remains readable without inventing a protected endpoint credential."""
    store = SQLiteLifecycleStore(tmp_path / "store.sqlite3")
    await store.initialize()
    await store.create(record())
    assert await store.get_credential("sandbox", "transaction") is None
    assert await store.get_credential("absent", "transaction") is None


@pytest.mark.anyio
async def test_credential_identity_rejection_precedes_any_insert(tmp_path):
    """A token for another transaction cannot be inserted alongside pending metadata."""
    store = SQLiteLifecycleStore(tmp_path / "store.sqlite3")
    await store.initialize()
    with pytest.raises(LifecycleStoreConflict, match="transaction_mismatch"):
        await store.create(record(), credential=credential(replace(record(), transaction_id="other")))
    assert await store.get("sandbox") is None


@pytest.mark.anyio
async def test_credential_and_pending_insert_roll_back_together(tmp_path, monkeypatch):
    """A credential write error cannot leave an unprotected pending lifecycle row."""
    store = SQLiteLifecycleStore(tmp_path / "store.sqlite3")
    await store.initialize()

    def fail_insert(connection, value):
        """Force the secret write to fail after pending insertion."""
        raise sqlite3.OperationalError("synthetic insertion failure")

    monkeypatch.setattr(module, "_insert_credential", fail_insert)
    with pytest.raises(LifecycleStoreError):
        await store.create(record(), credential=credential())
    assert await store.get("sandbox") is None
    assert await store.get_credential("sandbox", "transaction") is None


@pytest.mark.anyio
async def test_cas_delete_cascades_without_exposing_replacement_credential(tmp_path):
    """Stale deletion and secret reads cannot affect a replacement transaction."""
    path = tmp_path / "store.sqlite3"
    store = SQLiteLifecycleStore(path)
    await store.initialize()
    await store.create(record(), credential=credential())
    active = await store.activate("sandbox", "transaction", expected_version=0)
    with pytest.raises(LifecycleStoreConflict):
        await store.delete("sandbox", "transaction", expected_version=0)
    assert await store.get_credential("sandbox", "transaction") == credential()
    await store.delete("sandbox", "transaction", expected_version=active.version)
    assert await store.get_credential("sandbox", "transaction") is None
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("SELECT * FROM endpoint_credentials").fetchall() == []
    newer = replace(record(), transaction_id="new-transaction")
    await store.create(newer, credential=credential(newer, "b" * 43))
    with pytest.raises(LifecycleStoreConflict, match="transaction_mismatch"):
        await store.get_credential("sandbox", "transaction")
    with pytest.raises(LifecycleStoreConflict):
        await store.delete("sandbox", "transaction", expected_version=active.version)
    assert await store.get_credential("sandbox", newer.transaction_id) == credential(newer, "b" * 43)


@pytest.mark.anyio
async def test_stored_credential_transaction_is_checked(tmp_path):
    """Persisted credential identity drift fails closed without returning the token."""
    path = tmp_path / "store.sqlite3"
    store = SQLiteLifecycleStore(path)
    await store.initialize()
    await store.create(record(), credential=credential())
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("UPDATE endpoint_credentials SET transaction_id = 'other'")
    with pytest.raises(LifecycleStoreConflict, match="transaction_mismatch"):
        await store.get_credential("sandbox", "transaction")


@pytest.mark.anyio
@pytest.mark.parametrize("occupied", [False, True])
async def test_v3_upgrade_is_explicit_and_preserves_every_row(tmp_path, occupied):
    """Verified v3 upgrades add only credentials while preserving occupancy and timestamps."""
    path = tmp_path / "store.sqlite3"
    make_v3(path, occupied=occupied)
    with closing(sqlite3.connect(path)) as connection:
        before = {name: connection.execute(f"SELECT * FROM {name}").fetchall() for name in ("lifecycle_records", "workspaces", "workspace_bindings")}
    store = SQLiteLifecycleStore(path)
    with pytest.raises(LifecycleSchemaError):
        await store.initialize()
    await store.upgrade()
    await store.upgrade()
    await store.initialize()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (4,)
        assert {name: connection.execute(f"SELECT * FROM {name}").fetchall() for name in before} == before
        assert connection.execute("SELECT * FROM endpoint_credentials").fetchall() == []
    assert await store.get_credential("sandbox", "transaction") is None
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600


@pytest.mark.anyio
async def test_modified_v3_upgrade_rejected_without_permission_changes(tmp_path):
    """Version alone never authorizes migration or permission changes to an unrelated file."""
    path = tmp_path / "store.sqlite3"
    make_v3(path)
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("CREATE TABLE unexpected (value TEXT)")
    path.chmod(0o644)
    with pytest.raises(LifecycleSchemaError):
        await SQLiteLifecycleStore(path).upgrade()
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o644


@pytest.mark.anyio
async def test_failed_v3_upgrade_rolls_back_new_table(tmp_path, monkeypatch):
    """A schema verification failure rolls back added secret tables and the version marker."""
    path = tmp_path / "store.sqlite3"
    make_v3(path, occupied=True)
    original = SQLiteLifecycleStore._check_schema

    def fail_final(self, connection, **kwargs):
        """Allow validation of the original schema but fail after migration writes."""
        original(self, connection, **kwargs)
        if connection.execute("PRAGMA user_version").fetchone()[0] == 4:
            raise LifecycleStoreError()

    monkeypatch.setattr(SQLiteLifecycleStore, "_check_schema", fail_final)
    with pytest.raises(LifecycleStoreError):
        await SQLiteLifecycleStore(path).upgrade()
    with closing(sqlite3.connect(path)) as connection:
        assert connection.execute("PRAGMA user_version").fetchone() == (3,)
        assert connection.execute("SELECT name FROM sqlite_schema WHERE name = 'endpoint_credentials'").fetchall() == []
        assert connection.execute("SELECT * FROM workspace_bindings").fetchone()[-2:] == ("sandbox", "transaction")


@pytest.mark.anyio
async def test_workspace_credential_insert_is_one_transaction(tmp_path, monkeypatch):
    """Workspace acquisition and pending credentials commit or roll back together."""
    from test_workspace_store import workspace, binding, lifecycle

    store = SQLiteLifecycleStore(tmp_path / "store.sqlite3")
    await store.initialize()
    item = workspace()
    await store.create_workspace(item)
    available = await store.confirm_workspace(item.workspace_id, item.creation_token, expected_version=0)
    pending = lifecycle()

    def fail_insert(connection, value):
        """Reject credential insertion inside the workspace transaction."""
        raise sqlite3.OperationalError("synthetic insertion failure")

    with monkeypatch.context() as patch:
        patch.setattr(module, "_insert_credential", fail_insert)
        with pytest.raises(LifecycleStoreError):
            await store.create_with_workspace(pending, binding(pending), expected_workspace_version=available.version, credential=credential(pending))
    assert await store.get(pending.sandbox_id) is None
    assert await store.get_workspace(item.workspace_id) == available
    await store.create_with_workspace(pending, binding(pending), expected_workspace_version=available.version, credential=credential(pending))
    assert await store.get_credential(pending.sandbox_id, pending.transaction_id) == credential(pending)
    await store.delete(pending.sandbox_id, pending.transaction_id, expected_version=0)
    assert await store.get_credential(pending.sandbox_id, pending.transaction_id) is None


@pytest.mark.anyio
@pytest.mark.parametrize("sandbox_id,transaction_id", [("", "transaction"), (None, "transaction"), ("sandbox", ""), ("sandbox", None), ("sandbox", 7)])
async def test_credential_read_rejects_invalid_identity_without_io(tmp_path, sandbox_id, transaction_id):
    """Explicit credential lookup rejects invalid caller identities before opening storage."""
    store = SQLiteLifecycleStore(tmp_path / "missing.sqlite3")
    with pytest.raises(ValueError):
        await store.get_credential(sandbox_id, transaction_id)
    assert not store.database_path.exists()


@pytest.mark.anyio
async def test_malformed_persisted_credential_uses_sanitized_error(tmp_path):
    """A malformed secret accepted by the SQL length constraint is never returned or logged."""
    path = tmp_path / "store.sqlite3"
    store = SQLiteLifecycleStore(path)
    await store.initialize()
    await store.create(record(), credential=credential())
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("UPDATE endpoint_credentials SET access_token = ?", ("!" * 43,))
    with pytest.raises(LifecycleStoreError) as failure:
        await store.get_credential("sandbox", "transaction")
    assert str(failure.value) == "lifecycle storage operation failed"
    assert TOKEN not in str(failure.value) and "!" * 43 not in str(failure.value)


@pytest.mark.anyio
async def test_private_creation_does_not_change_process_umask(tmp_path, monkeypatch):
    """New nested storage secures each created directory without a global process mutation."""
    def forbidden_umask(value):
        """Reject process-global permission changes during database initialization."""
        raise AssertionError("global umask must not change")

    monkeypatch.setattr(os, "umask", forbidden_umask)
    path = tmp_path / "outer" / "inner" / "store.sqlite3"
    await SQLiteLifecycleStore(path).initialize()
    if os.name != "nt":
        assert (path.stat().st_mode & 0o777, path.parent.stat().st_mode & 0o777, path.parent.parent.stat().st_mode & 0o777) == (0o600, 0o700, 0o700)
