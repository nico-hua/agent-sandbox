"""File-backed SQLite lifecycle metadata with explicit initialization and version guards."""

import asyncio
import math
import os
import sqlite3
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from control_plane.domain.models import (
    SandboxAccessCredential,
    SandboxLifecycleRecord,
    SandboxListPage,
    SandboxManagementState,
    SandboxManagementSummary,
    WorkspaceBinding,
    WorkspaceRecord,
    _validate_lifecycle_times,
    encode_sandbox_cursor,
    validate_sandbox_list_query,
)
from control_plane.storage.base import (
    LifecycleInstanceLimitExceeded,
    LifecycleRecordNotFound,
    LifecycleSchemaError,
    LifecycleStoreConflict,
    LifecycleStoreError,
)
from control_plane.storage.run_lock import ensure_private_directory
from control_plane.storage.schema import (
    LIFECYCLE_COLUMNS,
    SCHEMA_DEFINITIONS,
    SCHEMA_VERSION,
    check_schema,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _validate_credential(record: SandboxLifecycleRecord, credential: SandboxAccessCredential | None) -> None:
    """Validate secret syntax and exact lifecycle ownership before starting any write."""
    if credential is None:
        return
    if type(credential) is not SandboxAccessCredential:
        raise ValueError("credential must be SandboxAccessCredential")
    SandboxAccessCredential(credential.sandbox_id, credential.transaction_id, credential.access_token)
    if (credential.sandbox_id, credential.transaction_id) != (record.sandbox_id, record.transaction_id):
        raise LifecycleStoreConflict("transaction_mismatch")


def _insert_credential(connection: sqlite3.Connection, credential: SandboxAccessCredential | None) -> None:
    """Write the optional credential using the caller's pending metadata transaction."""
    if credential is not None:
        connection.execute(
            "INSERT INTO endpoint_credentials (sandbox_id, transaction_id, access_token) VALUES (?, ?, ?)",
            (credential.sandbox_id, credential.transaction_id, credential.access_token),
        )


def _microseconds(value: datetime) -> int:
    """Convert an aware UTC datetime to exact signed epoch microseconds without floats."""
    if not isinstance(value, datetime):
        raise TypeError("time must be a timezone-aware UTC datetime")
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
            management_state=row["management_state"],
            workspace_id=row["workspace_id"],
        )
    except (ValueError, TypeError, OverflowError, KeyError) as error:
        raise LifecycleStoreError() from error


def _load(connection: sqlite3.Connection, sandbox_id: str) -> SandboxLifecycleRecord | None:
    """Read one identity on the caller's connection, preserving its transaction snapshot."""
    row = connection.execute(
        f"SELECT {LIFECYCLE_COLUMNS} FROM lifecycle_records WHERE sandbox_id = ?", (sandbox_id,),
    ).fetchone()
    return None if row is None else _record(row)


def _match(record: SandboxLifecycleRecord, transaction_id: str, expected_version: int) -> None:
    """Reject transaction mismatches before checking the caller's optimistic version."""
    if record.transaction_id != transaction_id:
        raise LifecycleStoreConflict("transaction_mismatch")
    if record.version != expected_version:
        raise LifecycleStoreConflict("version_conflict")


def _validate_instance_limit(max_instances: int) -> None:
    """Require a finite trusted instance count before starting a SQLite worker."""
    if type(max_instances) is not int or not 1 <= max_instances <= 1024:
        raise ValueError("max_instances must be an integer between 1 and 1024")


def _check_instance_capacity(connection: sqlite3.Connection, max_instances: int) -> None:
    """Check every lifecycle row while the caller holds its BEGIN IMMEDIATE transaction."""
    if connection.execute("SELECT COUNT(*) FROM lifecycle_records").fetchone()[0] >= max_instances:
        raise LifecycleInstanceLimitExceeded()


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

    @property
    def database_path(self) -> Path:
        """Expose the actual absolute database path without resolving or opening files."""
        return self._path

    async def initialize(self) -> None:
        """Explicitly initialize a compatible schema while preserving existing records."""
        await asyncio.to_thread(self._initialize_sync)

    async def create(self, record: SandboxLifecycleRecord, *, credential: SandboxAccessCredential | None = None, max_instances: int = 10) -> None:
        """Prewrite a pending identity within the strict total-instance bound."""
        if record.workspace_id is not None:
            raise ValueError("explicit workspaces require atomic acquisition")
        if record.version != 0:
            raise ValueError("initial lifecycle version must be zero")
        if record.management_state != "pending":
            raise ValueError("initial management_state must be pending")
        _validate_instance_limit(max_instances)
        _validate_credential(record, credential)
        await asyncio.to_thread(self._create_sync, record, credential, max_instances)

    async def list_sandboxes(self, *, limit: int = 20, cursor: str | None = None) -> SandboxListPage:
        """Validate the position before opening a thread-local read transaction."""
        position = validate_sandbox_list_query(limit, cursor)
        return await asyncio.to_thread(self._list_sandboxes_sync, limit, position)

    def _list_sandboxes_sync(self, limit: int, position: tuple[int, str] | None) -> SandboxListPage:
        """Select only public columns and limit+1 rows, sharing a snapshot with the total count."""
        with self._connection() as connection:
            connection.execute("BEGIN")
            count = connection.execute("SELECT COUNT(*) FROM lifecycle_records").fetchone()[0]
            condition = "" if position is None else " WHERE (created_at_us, sandbox_id) > (?, ?)"
            parameters = (limit + 1,) if position is None else (*position, limit + 1)
            rows = connection.execute(
                "SELECT sandbox_id, management_state, created_at_us, expires_at_us, workspace_id "
                "FROM lifecycle_records" + condition + " ORDER BY created_at_us, sandbox_id LIMIT ?",
                parameters,
            ).fetchall()
            try:
                items = tuple(SandboxManagementSummary(
                    row["sandbox_id"], row["management_state"],
                    _EPOCH + timedelta(microseconds=row["created_at_us"]),
                    _EPOCH + timedelta(microseconds=row["expires_at_us"]), row["workspace_id"],
                ) for row in rows[:limit])
            except (ValueError, TypeError, OverflowError) as error:
                raise LifecycleStoreError() from error
            next_cursor = encode_sandbox_cursor(items[-1].created_at, items[-1].sandbox_id) if len(rows) > limit else None
            return SandboxListPage(items, next_cursor, count)

    async def get_credential(self, sandbox_id: str, transaction_id: str) -> SandboxAccessCredential | None:
        """Read the secret only after checking the current lifecycle's exact transaction."""
        _identity(sandbox_id, transaction_id)
        if transaction_id is None:
            raise ValueError("transaction_id must be a nonempty string")
        return await asyncio.to_thread(self._get_credential_sync, sandbox_id, transaction_id)

    async def get(self, sandbox_id: str) -> SandboxLifecycleRecord | None:
        """Return the persisted record or None without initializing a missing database."""
        _identity(sandbox_id)
        return await asyncio.to_thread(self._get_sync, sandbox_id)

    async def activate(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> SandboxLifecycleRecord:
        """Move the matching pending creation to active and advance its version."""
        _identity(sandbox_id, transaction_id)
        _version(expected_version)
        return await asyncio.to_thread(
            self._activate_sync, sandbox_id, transaction_id, expected_version,
        )

    async def mark_deleting(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> SandboxLifecycleRecord:
        """Guard deletion entry, accepting an already-deleting exact version unchanged."""
        _identity(sandbox_id, transaction_id)
        _version(expected_version)
        return await asyncio.to_thread(
            self._mark_deleting_sync, sandbox_id, transaction_id, expected_version,
        )

    async def claim_expired(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
        now: datetime,
    ) -> SandboxLifecycleRecord | None:
        """Claim only a still-active, due record with the exact candidate identity/version."""
        _identity(sandbox_id, transaction_id)
        _version(expected_version)
        now_us = _microseconds(now)
        return await asyncio.to_thread(
            self._claim_expired_sync, sandbox_id, transaction_id, expected_version, now_us,
        )

    async def list_reap_candidates(
        self, now: datetime, *, limit: int = 100,
    ) -> list[SandboxLifecycleRecord]:
        """Read due active records plus deletion retries using precise bounded ordering."""
        now_us = _microseconds(now)
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        return await asyncio.to_thread(self._list_reap_sync, now_us, limit)

    async def list_pending(self, *, limit: int = 100) -> list[SandboxLifecycleRecord]:
        """Read pending records in creation order, bounded in SQL and without initialization."""
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        return await asyncio.to_thread(self._list_pending_sync, limit)

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
                self._prepare_private_database()
            with closing(sqlite3.connect(
                f"{self._path.as_uri()}?mode=rw", uri=True, timeout=self._lock_timeout,
            )) as connection:
                connection.row_factory = sqlite3.Row
                connection.execute("PRAGMA foreign_keys = ON")
                with connection:
                    if not initializing:
                        check_schema(connection)
                    yield connection
        except (sqlite3.Error, OSError, OverflowError) as error:
            raise LifecycleStoreError() from error

    def _prepare_private_database(self) -> None:
        """Create only missing directories and the database with private permissions."""
        ensure_private_directory(self._path.parent)
        try:
            descriptor = os.open(self._path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            return
        try:
            if os.name != "nt":
                os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)

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
                for _kind, _name, sql in SCHEMA_DEFINITIONS:
                    connection.execute(sql)
                connection.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            check_schema(connection)

    def _create_sync(self, record: SandboxLifecycleRecord, credential: SandboxAccessCredential | None = None, max_instances: int = 10) -> None:
        """Serialize capacity check and pending/credential insertion without replacing identities."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if _load(connection, record.sandbox_id) is not None:
                raise LifecycleStoreConflict("already_exists")
            # Keep capacity, pending, credential and optional occupancy in the
            # same serialized transaction; no reservation exists outside this row.
            _check_instance_capacity(connection, max_instances)
            connection.execute(
                f"INSERT INTO lifecycle_records ({LIFECYCLE_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (record.sandbox_id, record.transaction_id, _microseconds(record.created_at),
                 _microseconds(record.expires_at), record.version, record.management_state, record.workspace_id),
            )
            _insert_credential(connection, credential)

    def _get_credential_sync(self, sandbox_id: str, transaction_id: str) -> SandboxAccessCredential | None:
        """Check lifecycle and credential identity together in one read snapshot."""
        with self._connection() as connection:
            connection.execute("BEGIN")
            current = _load(connection, sandbox_id)
            if current is None:
                return None
            if current.transaction_id != transaction_id:
                raise LifecycleStoreConflict("transaction_mismatch")
            row = connection.execute(
                "SELECT sandbox_id, transaction_id, access_token FROM endpoint_credentials WHERE sandbox_id = ?",
                (sandbox_id,),
            ).fetchone()
            if row is None:
                return None
            if row["transaction_id"] != transaction_id:
                raise LifecycleStoreConflict("transaction_mismatch")
            try:
                return SandboxAccessCredential(**dict(row))
            except (ValueError, TypeError) as error:
                raise LifecycleStoreError() from error

    def _get_sync(self, sandbox_id: str) -> SandboxLifecycleRecord | None:
        """Read one record and release the connection before returning to the event loop."""
        with self._connection() as connection:
            return _load(connection, sandbox_id)

    def _activate_sync(
        self, sandbox_id: str, transaction_id: str, expected_version: int,
    ) -> SandboxLifecycleRecord:
        """Confirm pending creation within a single reserved write transaction."""
        return self._transition_sync(sandbox_id, transaction_id, expected_version, "active")

    def _mark_deleting_sync(
        self, sandbox_id: str, transaction_id: str, expected_version: int,
    ) -> SandboxLifecycleRecord:
        """Enter deletion or preserve the exact already-deleting version for retries."""
        return self._transition_sync(sandbox_id, transaction_id, expected_version, "deleting")

    def _transition_sync(
        self, sandbox_id: str, transaction_id: str, expected_version: int,
        target: SandboxManagementState,
    ) -> SandboxLifecycleRecord:
        """Activate or claim deletion under a write lock after checking identity and version."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = _load(connection, sandbox_id)
            if current is None:
                raise LifecycleRecordNotFound()
            _match(current, transaction_id, expected_version)
            # Exact-version deleting retries stay unchanged; always check identity first.
            if target == "deleting" and current.management_state == "deleting":
                return current
            if target == "active" and current.management_state != "pending":
                raise LifecycleStoreConflict("state_conflict")
            changed = replace(current, management_state=target, version=current.version + 1)
            # Commit state and version together; retain the prior state in the conditional update.
            cursor = connection.execute(
                "UPDATE lifecycle_records SET management_state = ?, version = version + 1 "
                "WHERE sandbox_id = ? AND transaction_id = ? AND version = ? "
                "AND management_state = ?",
                (target, sandbox_id, transaction_id, expected_version, current.management_state),
            )
            if cursor.rowcount != 1:
                raise LifecycleStoreConflict("version_conflict")
            return changed

    def _claim_expired_sync(
        self, sandbox_id: str, transaction_id: str, expected_version: int, now_us: int,
    ) -> SandboxLifecycleRecord | None:
        """Atomically check identity/version, active state and expiry, returning the new version."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            # Recheck expiry/version: a committed renewal must defeat a stale candidate claim.
            row = connection.execute(
                "UPDATE lifecycle_records SET management_state = 'deleting', version = version + 1 "
                "WHERE sandbox_id = ? AND transaction_id = ? AND version = ? "
                "AND management_state = 'active' AND expires_at_us <= ? "
                f"RETURNING {LIFECYCLE_COLUMNS}",
                (sandbox_id, transaction_id, expected_version, now_us),
            ).fetchone()
            return None if row is None else _record(row)

    def _update_sync(
        self, sandbox_id: str, transaction_id: str, expected_version: int,
        expires_at: datetime,
    ) -> SandboxLifecycleRecord:
        """Update expiry in one write transaction, rejecting stale or deletion-claimed records."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = _load(connection, sandbox_id)
            if current is None:
                raise LifecycleRecordNotFound()
            _match(current, transaction_id, expected_version)
            if current.management_state == "deleting":
                raise LifecycleStoreConflict("state_conflict")
            updated = replace(current, expires_at=expires_at, version=current.version + 1)
            # Change only expiry/version; a committed deletion claim must prevent later renewal.
            cursor = connection.execute(
                "UPDATE lifecycle_records SET expires_at_us = ?, version = version + 1 "
                "WHERE sandbox_id = ? AND transaction_id = ? AND version = ? "
                "AND management_state != 'deleting'",
                (_microseconds(expires_at), sandbox_id, transaction_id, expected_version),
            )
            if cursor.rowcount != 1:
                raise LifecycleStoreConflict("version_conflict")
            return updated

    def _list_sync(self, now_us: int, limit: int) -> list[SandboxLifecycleRecord]:
        """Use integer comparison, stable index order and SQL LIMIT for expired records."""
        with self._connection() as connection:
            rows = connection.execute(
                f"SELECT {LIFECYCLE_COLUMNS} FROM lifecycle_records WHERE expires_at_us <= ? "
                "ORDER BY expires_at_us, sandbox_id LIMIT ?", (now_us, limit),
            ).fetchall()
            return [_record(row) for row in rows]

    def _list_reap_sync(self, now_us: int, limit: int) -> list[SandboxLifecycleRecord]:
        """Bound candidate selection in SQL without treating pending rows as created sandboxes."""
        with self._connection() as connection:
            rows = connection.execute(
                f"SELECT {LIFECYCLE_COLUMNS} FROM lifecycle_records "
                "WHERE (management_state = 'active' AND expires_at_us <= ?) "
                "OR management_state = 'deleting' "
                "ORDER BY expires_at_us, sandbox_id LIMIT ?", (now_us, limit),
            ).fetchall()
            return [_record(row) for row in rows]

    def _list_pending_sync(self, limit: int) -> list[SandboxLifecycleRecord]:
        """Select only this pending batch with stable integer-microsecond creation ordering."""
        with self._connection() as connection:
            rows = connection.execute(
                f"SELECT {LIFECYCLE_COLUMNS} FROM lifecycle_records WHERE management_state = 'pending' "
                "ORDER BY created_at_us, sandbox_id LIMIT ?", (limit,),
            ).fetchall()
            return [_record(row) for row in rows]

    def _delete_sync(self, sandbox_id: str, transaction_id: str, expected_version: int) -> None:
        """Delete by transaction/version, accepting absence without erasing a changed record."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = _load(connection, sandbox_id)
            if current is None:
                return
            _match(current, transaction_id, expected_version)
            if current.workspace_id is not None and self._load_workspace(connection, current.workspace_id) is None:
                raise LifecycleStoreConflict("workspace_conflict")
            cursor = connection.execute(
                "DELETE FROM lifecycle_records "
                "WHERE sandbox_id = ? AND transaction_id = ? AND version = ?",
                (sandbox_id, transaction_id, expected_version),
            )
            if cursor.rowcount != 1:
                raise LifecycleStoreConflict("version_conflict")
            if current.workspace_id is not None:
                # Removing the holder unfreezes the workspace within the same transaction.
                connection.execute(
                    "UPDATE workspaces SET version = version + 1 WHERE workspace_id = ?",
                    (current.workspace_id,),
                )

    async def create_workspace(self, record: WorkspaceRecord) -> None:
        """Insert unoccupied version-zero pending workspace metadata without replacement."""
        if type(record) is not WorkspaceRecord:
            raise ValueError("record must be WorkspaceRecord")
        WorkspaceRecord(**record.__dict__)
        if record.version != 0 or record.management_state != "pending" or record.sandbox_id is not None:
            raise ValueError("workspace must start pending, unoccupied and at version zero")
        await asyncio.to_thread(self._create_workspace_sync, record)

    def _create_workspace_sync(self, record: WorkspaceRecord) -> None:
        """Check uniqueness and insert workspace creation intent under a write lock."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute(
                "SELECT 1 FROM workspaces WHERE workspace_id = ? OR creation_token = ? OR volume_name = ?",
                (record.workspace_id, record.creation_token, record.volume_name),
            ).fetchone() is not None:
                raise LifecycleStoreConflict("already_exists")
            connection.execute(
                "INSERT INTO workspaces VALUES (?, ?, ?, ?, ?, ?, ?)",
                (record.workspace_id, record.creation_token, record.volume_name, record.image_id,
                 _microseconds(record.created_at), record.version, record.management_state),
            )

    async def get_workspace(self, workspace_id: str) -> WorkspaceRecord | None:
        """Read workspace metadata and its holder from lifecycle records."""
        _identity(workspace_id)
        return await asyncio.to_thread(self._get_workspace_sync, workspace_id)

    def _get_workspace_sync(self, workspace_id: str) -> WorkspaceRecord | None:
        """Restore workspace metadata within one consistent read transaction."""
        with self._connection() as connection:
            connection.execute("BEGIN")
            return self._load_workspace(connection, workspace_id)

    def _load_workspace(self, connection: sqlite3.Connection, workspace_id: str) -> WorkspaceRecord | None:
        """Join the authoritative holder and validate the occupied workspace state."""
        row = connection.execute(
            "SELECT w.*, l.sandbox_id, l.transaction_id "
            "FROM workspaces w LEFT JOIN lifecycle_records l USING (workspace_id) WHERE w.workspace_id = ?",
            (workspace_id,),
        ).fetchone()
        if row is None:
            return None
        try:
            if type(row["created_at_us"]) is not int or type(row["version"]) is not int:
                raise ValueError("invalid persisted integer")
            record = WorkspaceRecord(
                row["workspace_id"], row["creation_token"], row["volume_name"], row["image_id"],
                _EPOCH + timedelta(microseconds=row["created_at_us"]), row["version"],
                row["management_state"], row["sandbox_id"], row["transaction_id"],
            )
        except (ValueError, TypeError, OverflowError) as error:
            raise LifecycleStoreError() from error
        if record.sandbox_id is not None and (record.management_state != "available" or record.version == 0):
            raise LifecycleStoreConflict("workspace_conflict")
        return record

    async def confirm_workspace(
        self, workspace_id: str, creation_token: str, *, expected_version: int,
    ) -> WorkspaceRecord:
        """Confirm the original pending creation and advance its workspace version."""
        _identity(workspace_id, creation_token)
        _version(expected_version)
        return await asyncio.to_thread(self._workspace_transition_sync, workspace_id, creation_token, expected_version, "available")

    async def claim_workspace_deletion(
        self, workspace_id: str, creation_token: str, *, expected_version: int,
    ) -> WorkspaceRecord:
        """Persist deletion intent only while unoccupied, accepting an exact deleting retry."""
        _identity(workspace_id, creation_token)
        _version(expected_version)
        return await asyncio.to_thread(self._workspace_transition_sync, workspace_id, creation_token, expected_version, "deleting")

    def _workspace_match(self, record: WorkspaceRecord | None, creation_token: str, expected_version: int) -> WorkspaceRecord:
        """Require original identity and optimistic version before any workspace update."""
        if record is None:
            raise LifecycleRecordNotFound()
        if record.creation_token != creation_token:
            raise LifecycleStoreConflict("transaction_mismatch")
        if record.version != expected_version:
            raise LifecycleStoreConflict("version_conflict")
        return record

    def _workspace_transition_sync(
        self, workspace_id: str, creation_token: str, expected_version: int, target: str,
    ) -> WorkspaceRecord:
        """Check occupancy and state before atomically confirming or claiming a workspace."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._workspace_match(self._load_workspace(connection, workspace_id), creation_token, expected_version)
            if current.sandbox_id is not None:
                raise LifecycleStoreConflict("workspace_conflict")
            if target == "deleting" and current.management_state == "deleting":
                return current
            required = "pending" if target == "available" else "available"
            if current.management_state != required:
                raise LifecycleStoreConflict("state_conflict")
            changed = replace(current, management_state=target, version=current.version + 1)
            cursor = connection.execute(
                "UPDATE workspaces SET management_state = ?, version = version + 1 "
                "WHERE workspace_id = ? AND creation_token = ? AND version = ? AND management_state = ? "
                "AND NOT EXISTS (SELECT 1 FROM lifecycle_records WHERE workspace_id = ?)",
                (target, workspace_id, creation_token, expected_version, required, workspace_id),
            )
            if cursor.rowcount != 1:
                raise LifecycleStoreConflict("workspace_conflict")
            return changed

    async def create_with_workspace(
        self, record: SandboxLifecycleRecord, binding: WorkspaceBinding, *, expected_workspace_version: int,
        credential: SandboxAccessCredential | None = None, max_instances: int = 10,
    ) -> None:
        """Atomically prewrite a pending sandbox and acquire an available workspace."""
        _version(expected_workspace_version)
        if type(binding) is not WorkspaceBinding:
            raise ValueError("binding must be WorkspaceBinding")
        WorkspaceBinding(**binding.__dict__)
        if (record.version != 0 or record.management_state != "pending"
            or record.sandbox_id != binding.sandbox_id or record.transaction_id != binding.transaction_id
            or record.workspace_id != binding.workspace_id or binding.version != expected_workspace_version + 1):
            raise LifecycleStoreConflict("workspace_conflict")
        _validate_instance_limit(max_instances)
        _validate_credential(record, credential)
        await asyncio.to_thread(self._create_with_workspace_sync, record, binding, expected_workspace_version, credential, max_instances)

    def _create_with_workspace_sync(
        self, record: SandboxLifecycleRecord, binding: WorkspaceBinding, expected_version: int,
        credential: SandboxAccessCredential | None = None, max_instances: int = 10,
    ) -> None:
        """Reserve workspace and lifecycle identities together, rolling back partial inserts."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._workspace_match(self._load_workspace(connection, binding.workspace_id), binding.creation_token, expected_version)
            if (current.management_state != "available" or current.sandbox_id is not None
                or current.volume_name != binding.volume_name or current.image_id != binding.image_id):
                raise LifecycleStoreConflict("workspace_conflict")
            if _load(connection, record.sandbox_id) is not None:
                raise LifecycleStoreConflict("already_exists")
            # Keep capacity, pending, credential and optional occupancy in the
            # same serialized transaction; no reservation exists outside this row.
            _check_instance_capacity(connection, max_instances)
            # Advance the version before inserting the holder. Occupied rows are frozen.
            cursor = connection.execute(
                "UPDATE workspaces SET version = version + 1 WHERE workspace_id = ? "
                "AND creation_token = ? AND version = ? AND management_state = 'available'",
                (binding.workspace_id, binding.creation_token, expected_version),
            )
            if cursor.rowcount != 1:
                raise LifecycleStoreConflict("workspace_conflict")
            connection.execute(
                f"INSERT INTO lifecycle_records ({LIFECYCLE_COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (record.sandbox_id, record.transaction_id, _microseconds(record.created_at),
                 _microseconds(record.expires_at), 0, "pending", binding.workspace_id),
            )
            _insert_credential(connection, credential)

    async def get_workspace_binding(self, sandbox_id: str, transaction_id: str) -> WorkspaceBinding | None:
        """Reconstruct a binding for the original transaction from one read snapshot."""
        _identity(sandbox_id, transaction_id)
        return await asyncio.to_thread(self._get_workspace_binding_sync, sandbox_id, transaction_id)

    def _get_workspace_binding_sync(self, sandbox_id: str, transaction_id: str) -> WorkspaceBinding | None:
        """Validate lifecycle and workspace identity in one snapshot before restoring binding."""
        with self._connection() as connection:
            connection.execute("BEGIN")
            current = _load(connection, sandbox_id)
            if current is None:
                return None
            if current.transaction_id != transaction_id:
                raise LifecycleStoreConflict("transaction_mismatch")
            if current.workspace_id is None:
                return None
            workspace = self._load_workspace(connection, current.workspace_id)
            if workspace is None:
                raise LifecycleStoreConflict("workspace_conflict")
            return WorkspaceBinding(
                workspace.workspace_id, workspace.creation_token, workspace.volume_name, workspace.image_id,
                workspace.version, current.sandbox_id, current.transaction_id,
            )

    async def delete_workspace(self, workspace_id: str, creation_token: str, *, expected_version: int) -> None:
        """Remove only an unoccupied deletion claim, accepting absent retries."""
        _identity(workspace_id, creation_token)
        _version(expected_version)
        await asyncio.to_thread(self._delete_workspace_sync, workspace_id, creation_token, expected_version)

    def _delete_workspace_sync(self, workspace_id: str, creation_token: str, expected_version: int) -> None:
        """Guard final deletion by token, version, persisted claim and absent occupancy."""
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            current = self._load_workspace(connection, workspace_id)
            if current is None:
                return
            self._workspace_match(current, creation_token, expected_version)
            if current.sandbox_id is not None or current.management_state != "deleting":
                raise LifecycleStoreConflict("workspace_conflict")
            cursor = connection.execute(
                "DELETE FROM workspaces WHERE workspace_id = ? AND creation_token = ? AND version = ? "
                "AND management_state = 'deleting' AND NOT EXISTS "
                "(SELECT 1 FROM lifecycle_records WHERE workspace_id = ?)",
                (workspace_id, creation_token, expected_version, workspace_id),
            )
            if cursor.rowcount != 1:
                raise LifecycleStoreConflict("workspace_conflict")
