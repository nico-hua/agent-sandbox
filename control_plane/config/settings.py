"""Trusted settings for Docker-only application assembly."""

import os
from pathlib import Path

DEFAULT_DATABASE_PATH = Path(__file__).resolve().parents[1] / ".local" / "lifecycle.sqlite3"


def configured_database_path() -> str | None:
    """Read a trusted database path without touching the filesystem."""
    value = os.environ.get("AGENT_SANDBOX_DATABASE_PATH")
    if value is not None and (
        not 1 <= len(value) <= 4096 or value != value.strip()
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError("invalid database path configuration")
    return value
