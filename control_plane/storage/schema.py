"""Current SQLite lifecycle schema and structural checks."""

import re
import sqlite3

from control_plane.storage.base import LifecycleSchemaError, LifecycleStoreError

SCHEMA_VERSION = 6
LIFECYCLE_COLUMNS = "sandbox_id, transaction_id, created_at_us, expires_at_us, version, management_state, workspace_id"

SCHEMA_DEFINITIONS = (
    ("table", "lifecycle_records", """
        CREATE TABLE lifecycle_records (
            sandbox_id TEXT PRIMARY KEY NOT NULL,
            transaction_id TEXT NOT NULL,
            created_at_us INTEGER NOT NULL,
            expires_at_us INTEGER NOT NULL CHECK (expires_at_us >= created_at_us),
            version INTEGER NOT NULL CHECK (version >= 0),
            management_state TEXT NOT NULL
                CHECK (management_state IN ('pending', 'active', 'deleting')),
            workspace_id TEXT REFERENCES workspaces(workspace_id),
            UNIQUE (sandbox_id, transaction_id, workspace_id),
            UNIQUE (workspace_id)
        ) STRICT
    """),
    ("index", "lifecycle_expiration_idx", """
        CREATE INDEX lifecycle_expiration_idx
        ON lifecycle_records (expires_at_us, sandbox_id)
    """),
    ("table", "workspaces", """
        CREATE TABLE workspaces (
            workspace_id TEXT PRIMARY KEY NOT NULL,
            creation_token TEXT NOT NULL UNIQUE,
            volume_name TEXT NOT NULL UNIQUE,
            image_id TEXT NOT NULL,
            created_at_us INTEGER NOT NULL,
            version INTEGER NOT NULL CHECK (version >= 0),
            management_state TEXT NOT NULL
                CHECK (management_state IN ('pending', 'available', 'deleting')),
            UNIQUE (workspace_id, creation_token, volume_name, image_id)
        ) STRICT
    """),
    ("table", "workspace_bindings", """
        CREATE TABLE workspace_bindings (
            workspace_id TEXT PRIMARY KEY NOT NULL,
            creation_token TEXT NOT NULL,
            volume_name TEXT NOT NULL,
            image_id TEXT NOT NULL,
            version INTEGER NOT NULL CHECK (version > 0),
            sandbox_id TEXT NOT NULL UNIQUE,
            transaction_id TEXT NOT NULL,
            FOREIGN KEY (workspace_id, creation_token, volume_name, image_id)
                REFERENCES workspaces(workspace_id, creation_token, volume_name, image_id),
            FOREIGN KEY (sandbox_id, transaction_id, workspace_id)
                REFERENCES lifecycle_records(sandbox_id, transaction_id, workspace_id)
        ) STRICT
    """),
    ("table", "endpoint_credentials", """
        CREATE TABLE endpoint_credentials (
            sandbox_id TEXT PRIMARY KEY NOT NULL
                REFERENCES lifecycle_records(sandbox_id) ON DELETE CASCADE,
            transaction_id TEXT NOT NULL,
            access_token TEXT NOT NULL CHECK (length(access_token) = 43)
        ) STRICT
    """),
)

_SQL_TOKENS = re.compile(
    r"""'(?:''|[^'])*'|"(?:""|[^"])*"|[A-Za-z_][A-Za-z_0-9]*|[0-9]+|>=|<=|<>|!=|==|[^\s]""",
    flags=re.ASCII,
)


def _ddl_tokens(sql: str) -> tuple[str, ...]:
    """Allow SQL formatter whitespace and keyword case while preserving literal values."""
    return tuple(
        token.lower() if token.isascii() and token[0] not in ("'", '"') else token
        for token in _SQL_TOKENS.findall(sql)
    )


def check_schema(connection: sqlite3.Connection) -> None:
    """Verify the current version, owned DDL and foreign keys."""
    version, = connection.execute("PRAGMA user_version").fetchone()
    if version != SCHEMA_VERSION:
        raise LifecycleSchemaError()
    expected = {(kind, name): _ddl_tokens(sql) for kind, name, sql in SCHEMA_DEFINITIONS}
    actual = {
        (kind, name): _ddl_tokens(sql or "")
        for kind, name, sql in connection.execute(
            "SELECT type, name, sql FROM sqlite_schema WHERE name NOT GLOB 'sqlite_*'"
        )
    }
    if actual != expected:
        raise LifecycleSchemaError()
    if connection.execute("PRAGMA foreign_key_check").fetchone() is not None:
        raise LifecycleStoreError()
