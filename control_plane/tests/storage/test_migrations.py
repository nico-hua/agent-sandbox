"""Exercise the explicit offline upgrade using temporary deployed schemas."""

import os
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from control_plane.storage import migrations
from control_plane.storage.base import (
    LifecycleSchemaError,
    LifecycleStoreError,
)
from control_plane.storage.sqlite_store import SQLiteLifecycleStore
from control_plane.storage.run_lock import LifecycleRunLockBusy, SQLiteRunLock

TABLES = ("lifecycle_records", "workspaces", "workspace_bindings", "endpoint_credentials")
TOKEN = "x" * 43


def make_legacy(path: Path, version: int, *, receipt: bool = False) -> None:
    """Write a legacy snapshot with an active holder and its original credentials."""
    with closing(sqlite3.connect(path)) as connection, connection:
        for _, _, ddl in migrations.LEGACY_SCHEMAS[version]:
            connection.execute(ddl)
        connection.execute(f"PRAGMA user_version = {version}")
        connection.execute("INSERT INTO workspaces VALUES ('workspace', 'created', 'volume', 'image', 7, 4, 'available')")
        connection.execute("INSERT INTO lifecycle_records VALUES ('sandbox', 'transaction', 11, 33, 6, 'active', 'workspace')")
        connection.execute("INSERT INTO workspace_bindings VALUES ('workspace', 'created', 'volume', 'image', 4, 'sandbox', 'transaction')")
        connection.execute("INSERT INTO endpoint_credentials VALUES ('sandbox', 'transaction', ?)", (TOKEN,))
        if receipt:
            connection.execute(
                "INSERT INTO workspace_resource_receipts VALUES "
                "('workspace', 'created', 'volume', 'image', 7, 'kubernetes', 'namespace', 'pvc', ?, 'class', 1)",
                ("a" * 36,),
            )


def snapshot(path: Path) -> tuple:
    """Capture the exact version, schema and persisted rows."""
    with closing(sqlite3.connect(path)) as connection:
        version, = connection.execute("PRAGMA user_version").fetchone()
        schema = connection.execute("SELECT type, name, sql FROM sqlite_schema ORDER BY type, name").fetchall()
        tables = [name for kind, name, _ in schema if kind == "table"]
        rows = {name: connection.execute(f'SELECT * FROM "{name}"').fetchall() for name in tables}
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
        return version, schema, rows


@pytest.mark.anyio
@pytest.mark.parametrize("version", [4, 5])
async def test_upgrade_preserves_docker_metadata_and_backup(tmp_path, version):
    """Preserve exact rows and keep a restorable copy of the pre-upgrade database."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = tmp_path / "before.sqlite3"
    make_legacy(original, version)
    before = snapshot(original)
    with pytest.raises(LifecycleSchemaError):
        await SQLiteLifecycleStore(original).initialize()
    assert snapshot(original) == before

    migrations.upgrade_database(original, backup, source="docker", confirm_docker_source=True)

    assert snapshot(backup) == before
    assert set(snapshot(original)[2]) == set(TABLES)
    assert snapshot(original)[2] == {name: before[2][name] for name in TABLES}
    assert snapshot(original)[0] == 6
    assert not any("receipt" in name for _, name, _ in snapshot(original)[1])
    await SQLiteLifecycleStore(original).initialize()
    if os.name != "nt":
        assert backup.stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("source,confirmed", [(None, False), ("docker", False), ("kubernetes", True)])
def test_upgrade_requires_explicit_docker_provenance(tmp_path, source, confirmed):
    """An empty legacy table and the default filename are insufficient evidence."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = tmp_path / "before.sqlite3"
    make_legacy(original, 5)
    before = original.read_bytes()
    with pytest.raises(ValueError, match="Docker"):
        migrations.upgrade_database(original, backup, source=source, confirm_docker_source=confirmed)
    assert original.read_bytes() == before and not backup.exists()
    assert not original.with_suffix(".sqlite3.lock").exists()


def test_upgrade_refuses_nonempty_historical_evidence(tmp_path):
    """The old receipt must survive even if a caller asserts Docker provenance."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = tmp_path / "before.sqlite3"
    make_legacy(original, 5, receipt=True)
    before = snapshot(original)
    with pytest.raises(LifecycleSchemaError):
        migrations.upgrade_database(original, backup, source="docker", confirm_docker_source=True)
    assert snapshot(original) == before and not backup.exists()


def test_upgrade_cannot_bypass_run_lock(tmp_path):
    """A live control plane retains ownership across equivalent database paths."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = tmp_path / "before.sqlite3"
    make_legacy(original, 5)
    lock = SQLiteRunLock(original)
    lock.acquire()
    try:
        with pytest.raises(LifecycleRunLockBusy):
            migrations.upgrade_database(original, backup, source="docker", confirm_docker_source=True)
        assert snapshot(original)[0] == 5 and not backup.exists()
    finally:
        lock.release()


@pytest.mark.parametrize("version", [4, 5])
def test_upgrade_rollback_preserves_source_and_backup(tmp_path, monkeypatch, version):
    """Simulate SQLite reporting a write failure after destructive DDL was executed."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = tmp_path / "before.sqlite3"
    make_legacy(original, version)
    before = snapshot(original)
    apply = migrations._apply_upgrade

    def fail_after_writes(connection, version):
        """Raise before transaction commit even though DDL completed."""
        apply(connection, version)
        raise sqlite3.OperationalError("synthetic failure")

    monkeypatch.setattr(migrations, "_apply_upgrade", fail_after_writes)
    with pytest.raises(LifecycleStoreError):
        migrations.upgrade_database(original, backup, source="docker", confirm_docker_source=True)
    assert snapshot(original) == snapshot(backup) == before


def test_upgrade_rejects_existing_backup_without_overwriting(tmp_path):
    """An existing backup path aborts the source transaction."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = tmp_path / "before.sqlite3"
    make_legacy(original, 5)
    backup.write_bytes(b"sentinel")
    before = snapshot(original)
    with pytest.raises(LifecycleStoreError):
        migrations.upgrade_database(original, backup, source="docker", confirm_docker_source=True)
    assert snapshot(original) == before and backup.read_bytes() == b"sentinel"


@pytest.mark.parametrize("suffix", ["-wal", "-journal", "-shm"])
def test_backup_path_cannot_collide_with_source_sidecars(tmp_path, suffix):
    """Reserve the source's SQLite sidecar names even when they do not exist."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = original.with_name(original.name + suffix)
    make_legacy(original, 5)
    before = snapshot(original)
    with pytest.raises(ValueError, match="backup path"):
        migrations.upgrade_database(original, backup, source="docker", confirm_docker_source=True)
    assert not backup.exists()
    assert snapshot(original) == before


def test_backup_contains_committed_wal_data_in_a_standalone_file(tmp_path):
    """Keep committed WAL frames in the pre-upgrade copy without shipping SQLite sidecars."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = tmp_path / "before.sqlite3"
    make_legacy(original, 5)
    with closing(sqlite3.connect(original)) as writer:
        assert writer.execute("PRAGMA journal_mode = WAL").fetchone() == ("wal",)
        with writer:
            writer.execute("UPDATE lifecycle_records SET expires_at_us = 12345")
        assert original.with_name(original.name + "-wal").exists()
        before = snapshot(original)
        assert migrations.upgrade_database(original, backup, source="docker", confirm_docker_source=True)
    assert snapshot(backup) == before
    assert snapshot(original)[2]["lifecycle_records"][0][3] == 12345
    assert not any(tmp_path.glob("before.sqlite3-*"))


@pytest.mark.parametrize("version", [4, 5])
def test_upgrade_rejects_altered_ddl_literal(tmp_path, version):
    """DDL comparison must distinguish case-sensitive constraints."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = tmp_path / "before.sqlite3"
    with closing(sqlite3.connect(original)) as connection, connection:
        for _, _, ddl in migrations.LEGACY_SCHEMAS[version]:
            connection.execute(ddl.replace("'pending'", "'Pending'"))
        connection.execute(f"PRAGMA user_version = {version}")
    before = snapshot(original)
    with pytest.raises(LifecycleSchemaError):
        migrations.upgrade_database(original, backup, source="docker", confirm_docker_source=True)
    assert snapshot(original) == before and not backup.exists()


@pytest.mark.parametrize("change", ["PRAGMA user_version = 99", "CREATE TABLE extra (x TEXT)"])
def test_upgrade_rejects_unknown_version_or_extra_objects(tmp_path, change):
    """Only the deployed version and its exact workspace structure authorize writes."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = tmp_path / "before.sqlite3"
    make_legacy(original, 4)
    with closing(sqlite3.connect(original)) as connection, connection:
        connection.execute(change)
    before = snapshot(original)
    with pytest.raises(LifecycleSchemaError):
        migrations.upgrade_database(original, backup, source="docker", confirm_docker_source=True)
    assert snapshot(original) == before and not backup.exists()


def test_upgrade_rejects_unicode_that_sqlite_treats_as_identifier_content(tmp_path):
    """SQLite treats a no-break space after an unquoted column name as part of that name."""
    original = tmp_path / "lifecycle.sqlite3"
    backup = tmp_path / "before.sqlite3"
    with closing(sqlite3.connect(original)) as connection, connection:
        for _, _, ddl in migrations.LEGACY_SCHEMAS[4]:
            connection.execute(ddl.replace("access_token", "access_token\u00a0"))
        connection.execute("PRAGMA user_version = 4")
    before = snapshot(original)
    with pytest.raises(LifecycleSchemaError):
        migrations.upgrade_database(original, backup, source="docker", confirm_docker_source=True)
    assert snapshot(original) == before and not backup.exists()
