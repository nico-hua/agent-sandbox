"""File-backed SQLite lifecycle metadata with explicit initialization and version guards."""

import asyncio
import math
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from control_plane.core.models import SandboxLifecycleRecord, _validate_lifecycle_times


class LifecycleStoreError(RuntimeError):
    """Report a stable persistence failure without exposing database paths or SQL."""

    def __init__(self) -> None:
        """Set the fixed error message shared by SQLite access and corruption failures."""
        super().__init__("lifecycle storage operation failed")


class LifecycleStoreConflict(LifecycleStoreError):
    """Reject an existing identity, mismatched transaction or stale record version."""

    def __init__(
        self, reason: Literal["already_exists", "transaction_mismatch", "version_conflict"],
    ) -> None:
        """Expose a machine-readable conflict reason without backend details."""
        RuntimeError.__init__(self, f"lifecycle record conflict: {reason}")
        self.reason = reason


class LifecycleRecordNotFound(LifecycleStoreError):
    """Report an update whose lifecycle record no longer exists."""

    def __init__(self) -> None:
        """Set a stable missing-record error for expiration updates."""
        RuntimeError.__init__(self, "lifecycle record not found")


class LifecycleSchemaError(LifecycleStoreError):
    """Reject a database that is uninitialized or uses an incompatible schema."""

    def __init__(self) -> None:
        """Set the fixed schema error without attempting migration or repair."""
        RuntimeError.__init__(self, "lifecycle database schema is incompatible")


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_SCHEMA_VERSION = 1
_COLUMNS = "sandbox_id, transaction_id, created_at_us, expires_at_us, version"
_SCHEMA_DEFINITIONS = (
    ("table", "lifecycle_records", """
        CREATE TABLE lifecycle_records (
            sandbox_id TEXT PRIMARY KEY NOT NULL,
            transaction_id TEXT NOT NULL,
            created_at_us INTEGER NOT NULL,
            expires_at_us INTEGER NOT NULL CHECK (expires_at_us >= created_at_us),
            version INTEGER NOT NULL CHECK (version >= 0)
        ) STRICT
    """),
    ("index", "lifecycle_expiration_idx", """
        CREATE INDEX lifecycle_expiration_idx
        ON lifecycle_records (expires_at_us, sandbox_id)
    """),
)


def _microseconds(value: datetime) -> int:
    """Convert an aware UTC datetime to exact signed epoch microseconds without floats."""
    if not isinstance(value, datetime):
        raise ValueError("time must be a timezone-aware UTC datetime")
    _validate_lifecycle_times(value, value)
    delta = value - _EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _identity(sandbox_id: str, transaction_id: str | None = None) -> None:
    """Validate nonempty identity arguments without prescribing a new public ID format."""
    if not isinstance(sandbox_id, str) or not sandbox_id:
        raise ValueError("sandbox_id must be a nonempty string")
    if transaction_id is not None and (
        not isinstance(transaction_id, str) or not transaction_id
    ):
        raise ValueError("transaction_id must be a nonempty string")


def _version(value: int) -> None:
    """Require a nonnegative SQLite integer version rather than a boolean or float."""
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ValueError("expected_version must be a nonnegative SQLite integer")


def _record(row: sqlite3.Row) -> SandboxLifecycleRecord:
    """Restore exact UTC times and reject malformed persisted records with a stable error."""
    try:
        if any(type(row[key]) is not int for key in ("created_at_us", "expires_at_us", "version")):
            raise ValueError("invalid persisted integer")
        return SandboxLifecycleRecord(
            sandbox_id=row["sandbox_id"],
            transaction_id=row["transaction_id"],
            created_at=_EPOCH + timedelta(microseconds=row["created_at_us"]),
            expires_at=_EPOCH + timedelta(microseconds=row["expires_at_us"]),
            version=row["version"],
        )
    except (ValueError, TypeError, OverflowError) as error:
        raise LifecycleStoreError() from error


def _load(connection: sqlite3.Connection, sandbox_id: str) -> SandboxLifecycleRecord | None:
    """Read one identity on the caller's connection, preserving its transaction snapshot."""
    row = connection.execute(
        f"SELECT {_COLUMNS} FROM lifecycle_records WHERE sandbox_id = ?", (sandbox_id,),
    ).fetchone()
    return None if row is None else _record(row)


def _match(record: SandboxLifecycleRecord, transaction_id: str, expected_version: int) -> None:
    """Reject transaction mismatches before checking the caller's optimistic version."""
    if record.transaction_id != transaction_id:
        raise LifecycleStoreConflict("transaction_mismatch")
    if record.version != expected_version:
        raise LifecycleStoreConflict("version_conflict")


class SQLiteLifecycleStore:
    """Persist lifecycle records using independent connections confined to worker threads."""

    def __init__(self, database_path: str | Path, *, lock_timeout: float = 0.5) -> None:
        """Save a file path and finite lock wait without creating any filesystem resources."""
        if "\x00" in str(database_path):
            raise ValueError("database_path must not contain NUL")
        if str(database_path) == ":memory:":
            raise ValueError("lifecycle storage requires a file database")
        if (
            type(lock_timeout) not in (int, float)
            or not math.isfinite(lock_timeout)
            or lock_timeout < 0
        ):
            raise ValueError("lock_timeout must be finite and nonnegative")
        self._path = Path(database_path).absolute()
        self._lock_timeout = lock_timeout

    async def initialize(self) -> None:
        """Explicitly initialize a compatible schema while preserving existing records."""
        await asyncio.to_thread(self._initialize_sync)

    async def create(self, record: SandboxLifecycleRecord) -> None:
        """Insert initial version-zero metadata without replacing an existing identity."""
        if record.version != 0:
            raise ValueError("initial lifecycle version must be zero")
        await asyncio.to_thread(self._create_sync, record)

    async def get(self, sandbox_id: str) -> SandboxLifecycleRecord | None:
        """Return the persisted record or None without initializing a missing database."""
        _identity(sandbox_id)
        return await asyncio.to_thread(self._get_sync, sandbox_id)

    async def update_expiration(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
        expires_at: datetime,
    ) -> SandboxLifecycleRecord:
        """Replace only expiration after verifying transaction and version atomically."""
        _identity(sandbox_id, transaction_id)
        _version(expected_version)
        _microseconds(expires_at)
        return await asyncio.to_thread(
            self._update_sync, sandbox_id, transaction_id, expected_version, expires_at,
        )

    async def list_expired(
        self, now: datetime, *, limit: int = 100,
    ) -> list[SandboxLifecycleRecord]:
        """List expires_at <= now, ordered by expiration then sandbox ID, within limit."""
        now_us = _microseconds(now)
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        return await asyncio.to_thread(self._list_sync, now_us, limit)

    async def delete(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> None:
        """Delete guarded metadata, accepting an already absent record idempotently."""
        _identity(sandbox_id, transaction_id)
        _version(expected_version)
        await asyncio.to_thread(self._delete_sync, sandbox_id, transaction_id, expected_version)

    @contextmanager
    def _connection(self, *, initializing: bool = False) -> Iterator[sqlite3.Connection]:
        """Commit or roll back and close each thread-local connection on every path."""
        try:
            if initializing:
                self._path.parent.mkdir(parents=True, exist_ok=True)
            mode = "rwc" if initializing else "rw"
            with closing(sqlite3.connect(
                f"{self._path.as_uri()}?mode={mode}", uri=True, timeout=self._lock_timeout,
            )) as connection:
                connection.row_factory = sqlite3.Row
                with connection:
                    if not initializing:
                        self._check_schema(connection)
                    yield connection
        except (sqlite3.Error, OSError, OverflowError) as error:
            raise LifecycleStoreError() from error

    def _check_schema(self, connection: sqlite3.Connection) -> None:
        """Verify the owned v1 schema, including constraints, index and absent triggers."""
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        expected = {
            (kind, name): " ".join(sql.split()).casefold()
            for kind, name, sql in _SCHEMA_DEFINITIONS
        }
        actual = {
            (row["type"], row["name"]): " ".join((row["sql"] or "").split()).casefold()
            for row in connection.execute(
                "SELECT name, type, sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'"
            )
        }
        # Column names alone cannot detect different CHECK rules or a trigger that
        # rewrites immutable identity. This database version owns exactly two objects.
        if version != _SCHEMA_VERSION or actual != expected:
            raise LifecycleSchemaError()

    def _initialize_sync(self) -> None:
        """Create the first schema transactionally or validate an existing version."""
        with self._connection(initializing=True) as connection:
            connection.execute("BEGIN IMMEDIATE")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version == 0:
                # Do not adopt or overwrite an unrelated database with unversioned tables.
                if connection.execute(
                    "SELECT 1 FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*' LIMIT 1"
                ).fetchone() is not None:
                    raise LifecycleSchemaError()
                for _kind, _name, sql in _SCHEMA_DEFINITIONS:
                    connection.execute(sql)
                connection.execute("PRAGMA user_version = 1")
            self._check_schema(connection)

    def _create_sync(self, record: SandboxLifecycleRecord) -> None:
        """Insert metadata in a write transaction so duplicate IDs cannot replace data."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if _load(connection, record.sandbox_id) is not None:
                raise LifecycleStoreConflict("already_exists")
            connection.execute(
                f"INSERT INTO lifecycle_records ({_COLUMNS}) VALUES (?, ?, ?, ?, ?)",
                (record.sandbox_id, record.transaction_id, _microseconds(record.created_at),
                 _microseconds(record.expires_at), record.version),
            )

    def _get_sync(self, sandbox_id: str) -> SandboxLifecycleRecord | None:
        """Read one record and release the connection before returning to the event loop."""
        with self._connection() as connection:
            return _load(connection, sandbox_id)

    def _update_sync(
        self, sandbox_id: str, transaction_id: str, expected_version: int,
        expires_at: datetime,
    ) -> SandboxLifecycleRecord:
        """Validate and conditionally update under one reserved SQLite write lock."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = _load(connection, sandbox_id)
            if current is None:
                raise LifecycleRecordNotFound()
            _match(current, transaction_id, expected_version)
            updated = replace(current, expires_at=expires_at, version=current.version + 1)
            cursor = connection.execute(
                "UPDATE lifecycle_records SET expires_at_us = ?, version = version + 1 "
                "WHERE sandbox_id = ? AND transaction_id = ? AND version = ?",
                (_microseconds(expires_at), sandbox_id, transaction_id, expected_version),
            )
            if cursor.rowcount != 1:
                raise LifecycleStoreConflict("version_conflict")
            return updated

    def _list_sync(self, now_us: int, limit: int) -> list[SandboxLifecycleRecord]:
        """Use integer comparison, stable index order and SQL LIMIT for expired records."""
        with self._connection() as connection:
            rows = connection.execute(
                f"SELECT {_COLUMNS} FROM lifecycle_records WHERE expires_at_us <= ? "
                "ORDER BY expires_at_us, sandbox_id LIMIT ?", (now_us, limit),
            ).fetchall()
            return [_record(row) for row in rows]

    def _delete_sync(self, sandbox_id: str, transaction_id: str, expected_version: int) -> None:
        """Guard deletion in the same write transaction, including absent-record retries."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = _load(connection, sandbox_id)
            if current is None:
                return
            _match(current, transaction_id, expected_version)
            cursor = connection.execute(
                "DELETE FROM lifecycle_records "
                "WHERE sandbox_id = ? AND transaction_id = ? AND version = ?",
                (sandbox_id, transaction_id, expected_version),
            )
            if cursor.rowcount != 1:
                raise LifecycleStoreConflict("version_conflict")
