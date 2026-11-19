"""Explicit migration of verified legacy Docker databases with a private offline backup."""

import argparse
import os
import sqlite3
import sys
import tempfile
from contextlib import closing
from pathlib import Path

from control_plane.config.settings import validate_database_path
from control_plane.storage.base import LifecycleSchemaError, LifecycleStoreError
from control_plane.storage.run_lock import LifecycleRunLockError, SQLiteRunLock
from control_plane.storage.schema import (
    EXPIRATION_INDEX,
    LIFECYCLE_COLUMNS,
    SCHEMA_DEFINITIONS,
    SCHEMA_VERSION,
    WORKSPACE_SCHEMA,
    check_schema,
)

_V2_COLUMNS = "sandbox_id, transaction_id, created_at_us, expires_at_us, version, management_state"

_V2_SCHEMA_DEFINITIONS = (
    ("table", "lifecycle_records", """
        CREATE TABLE lifecycle_records (
            sandbox_id TEXT PRIMARY KEY NOT NULL,
            transaction_id TEXT NOT NULL,
            created_at_us INTEGER NOT NULL,
            expires_at_us INTEGER NOT NULL CHECK (expires_at_us >= created_at_us),
            version INTEGER NOT NULL CHECK (version >= 0),
            management_state TEXT NOT NULL
                CHECK (management_state IN ('pending', 'active', 'deleting'))
        ) STRICT
    """),
    EXPIRATION_INDEX,
)

_V5_SCHEMA_DEFINITIONS = SCHEMA_DEFINITIONS + (
    ("index", "workspace_receipt_identity_idx", """
        CREATE UNIQUE INDEX workspace_receipt_identity_idx
        ON workspaces (workspace_id, creation_token, volume_name, image_id, created_at_us)
    """),
    ("table", "workspace_resource_receipts", """
        CREATE TABLE workspace_resource_receipts (
            workspace_id TEXT PRIMARY KEY NOT NULL,
            creation_token TEXT NOT NULL,
            volume_name TEXT NOT NULL,
            image_id TEXT NOT NULL,
            created_at_us INTEGER NOT NULL,
            backend TEXT NOT NULL CHECK (backend = 'kubernetes'),
            namespace TEXT NOT NULL CHECK (length(namespace) BETWEEN 1 AND 63),
            pvc_name TEXT NOT NULL CHECK (length(pvc_name) BETWEEN 1 AND 253),
            pvc_uid TEXT NOT NULL CHECK (length(pvc_uid) = 36),
            storage_class TEXT NOT NULL CHECK (length(storage_class) BETWEEN 1 AND 253),
            original_uid_verified INTEGER NOT NULL CHECK (original_uid_verified = 1),
            UNIQUE (backend, namespace, pvc_name),
            UNIQUE (backend, pvc_uid),
            FOREIGN KEY (workspace_id, creation_token, volume_name, image_id, created_at_us)
                REFERENCES workspaces(workspace_id, creation_token, volume_name, image_id, created_at_us)
                ON DELETE CASCADE
        ) STRICT
    """),
    ("trigger", "workspace_receipt_immutable", """
        CREATE TRIGGER workspace_receipt_immutable
        BEFORE UPDATE ON workspace_resource_receipts
        BEGIN
            SELECT RAISE(ABORT, 'workspace resource receipt is immutable');
        END
    """),
)

LEGACY_SCHEMAS = {2: _V2_SCHEMA_DEFINITIONS, 3: WORKSPACE_SCHEMA, 4: SCHEMA_DEFINITIONS, 5: _V5_SCHEMA_DEFINITIONS}


class LegacyReceiptError(LifecycleSchemaError):
    """Report a legacy database containing evidence from the removed runtime."""

    def __init__(self) -> None:
        """Describe why the validated legacy schema cannot be converted."""
        RuntimeError.__init__(self, "legacy database contains workspace receipts and cannot be converted to Docker")


def _backup_database(source: Path, destination: Path) -> None:
    """Publish a checked private SQLite snapshot atomically without replacing an existing file."""
    # The caller has a reserved writer lock and has not executed any DDL. A
    # second, read-only connection obtains the same committed snapshot, WAL included.
    descriptor, name = tempfile.mkstemp(dir=destination.parent, prefix=".lifecycle-backup-")
    temporary = Path(name)
    try:
        os.close(descriptor)
        with (
            closing(sqlite3.connect(f"{source.as_uri()}?mode=ro", uri=True, timeout=0.5)) as reader,
            closing(sqlite3.connect(temporary, timeout=0.5)) as backup,
        ):
            reader.backup(backup)
            # Publish a standalone database, even when the source used WAL.
            backup.execute("PRAGMA journal_mode = DELETE")
            if backup.execute("PRAGMA integrity_check").fetchall() != [("ok",)]:
                raise LifecycleStoreError()
        with temporary.open("rb+") as backup_file:
            if os.name != "nt":
                os.fchmod(backup_file.fileno(), 0o600)
            os.fsync(backup_file.fileno())
        # SQLite associates these names with the eventual backup database.
        if any(os.path.lexists(str(destination) + suffix) for suffix in ("-wal", "-shm", "-journal")):
            raise LifecycleStoreError()
        os.link(temporary, destination)
        if sys.platform == "linux":
            descriptor = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        temporary.unlink()


def _apply_upgrade(connection: sqlite3.Connection, version: int) -> None:
    """Change only the schema objects that differ and preserve all Docker rows."""
    if version == 2:
        connection.execute("ALTER TABLE lifecycle_records RENAME TO lifecycle_records_v2")
        connection.execute("DROP INDEX lifecycle_expiration_idx")
        for _, _, ddl in SCHEMA_DEFINITIONS:
            connection.execute(ddl)
        connection.execute(
            f"INSERT INTO lifecycle_records ({LIFECYCLE_COLUMNS}) "
            f"SELECT {_V2_COLUMNS}, NULL FROM lifecycle_records_v2"
        )
        connection.execute("DROP TABLE lifecycle_records_v2")
    elif version == 3:
        for _, _, ddl in SCHEMA_DEFINITIONS[len(WORKSPACE_SCHEMA):]:
            connection.execute(ddl)
    elif version == 5:
        connection.execute("DROP TRIGGER workspace_receipt_immutable")
        connection.execute("DROP TABLE workspace_resource_receipts")
        connection.execute("DROP INDEX workspace_receipt_identity_idx")
    connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")


def upgrade_database(
    database: str | Path, backup: str | Path, *, source: str | None = None,
    confirm_docker_source: bool = False,
) -> bool:
    """Upgrade verified and explicitly attributed Docker data while holding the application lock."""
    if source != "docker" or confirm_docker_source is not True:
        raise ValueError("upgrade requires explicit confirmation of Docker database provenance")
    try:
        database = Path(database)
        validate_database_path(database)
        database = database.resolve(strict=True)
        validate_database_path(database)
        # Resolve the parent, keeping the final name as supplied so a dangling
        # symlink cannot be interpreted as a valid new backup destination.
        backup = Path(backup)
        backup = backup.parent.resolve(strict=True) / backup.name
    except (OSError, RuntimeError) as error:
        raise LifecycleStoreError() from error
    if backup in {database.with_name(database.name + suffix) for suffix in ("", "-wal", "-shm", "-journal", ".lock")}:
        raise ValueError("backup path overlaps the source database or its companion files")
    lock = SQLiteRunLock(database)
    lock.acquire()
    try:
        with closing(sqlite3.connect(
            f"{database.as_uri()}?mode=rw", uri=True, timeout=0.5,
        )) as connection, connection:
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA synchronous = FULL")
            connection.execute("BEGIN IMMEDIATE")
            version, = connection.execute("PRAGMA user_version").fetchone()
            if version == SCHEMA_VERSION:
                check_schema(connection)
                return False
            if version not in LEGACY_SCHEMAS:
                raise LifecycleSchemaError()
            check_schema(connection, version_expected=version, definitions=LEGACY_SCHEMAS[version])
            if version == 5 and connection.execute("SELECT 1 FROM workspace_resource_receipts LIMIT 1").fetchone():
                raise LegacyReceiptError()
            if os.path.lexists(backup):
                raise LifecycleStoreError()
            _backup_database(database, backup)
            if os.name != "nt":
                database.chmod(0o600)
            _apply_upgrade(connection, version)
            check_schema(connection)
            return True
    except (sqlite3.Error, OSError) as error:
        raise LifecycleStoreError() from error
    finally:
        lock.release()


def main() -> int:
    """Run the offline upgrade and report only fixed storage or provenance errors."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--source", choices=("docker",), required=True)
    parser.add_argument("--confirm-docker-source", action="store_true", required=True)
    args = parser.parse_args()
    try:
        upgraded = upgrade_database(
            args.database, args.backup, source=args.source, confirm_docker_source=args.confirm_docker_source,
        )
    except (LifecycleStoreError, LifecycleRunLockError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    print(f"Docker lifecycle database {'upgraded to' if upgraded else 'already at'} schema {SCHEMA_VERSION}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
