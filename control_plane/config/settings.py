"""Trusted settings for Docker-only application assembly."""

import os
from pathlib import Path

DEFAULT_DATABASE_PATH = Path(__file__).resolve().parents[1] / ".local" / "lifecycle.sqlite3"


def validate_runtime_settings() -> None:
    """Reject removed target settings and accept the former explicit Docker selection."""
    runtime = os.environ.get("AGENT_SANDBOX_RUNTIME", "docker")
    if runtime == "kubernetes" or any(key.startswith("AGENT_SANDBOX_K8S_") for key in os.environ):
        raise ValueError("Kubernetes runtime was removed; clear its settings and select a Docker database")
    if runtime != "docker":
        raise ValueError("AGENT_SANDBOX_RUNTIME only accepts docker")


def configured_database_path() -> str | None:
    """Read a trusted database path without touching the filesystem."""
    value = os.environ.get("AGENT_SANDBOX_DATABASE_PATH")
    if value is not None and (
        not 1 <= len(value) <= 4096 or value != value.strip()
        or any(ord(char) < 32 or ord(char) == 127 for char in value)
    ):
        raise ValueError("invalid database path configuration")
    return value


def validate_database_path(path: Path) -> None:
    """Reject the former Kubernetes database path, even if it has not been created."""
    if path.name.casefold() == "kubernetes-lifecycle.sqlite3":
        raise ValueError("the former Kubernetes database cannot be used with Docker")
