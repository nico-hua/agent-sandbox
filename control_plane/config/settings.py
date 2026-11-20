"""Load a snapshot of trusted control-plane configuration at assembly time."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from pathlib import Path
from types import MappingProxyType

from dotenv import dotenv_values

from control_plane.services.maintenance import (
    DEFAULT_REAPER_BATCH_LIMIT,
    DEFAULT_REAPER_INTERVAL_SECONDS,
    validate_reaper_config,
)
from control_plane.services.sandbox_service import (
    DEFAULT_MAX_CONCURRENT_CREATES,
    DEFAULT_MAX_INSTANCES,
)

CONTROL_PLANE_DIR = Path(__file__).resolve().parents[1]
ENV_FILE = CONTROL_PLANE_DIR / ".env"
DEFAULT_DATABASE_PATH = CONTROL_PLANE_DIR / ".local" / "lifecycle.sqlite3"
ENV_PREFIX = "AGENT_SANDBOX_"
UNSET_API_KEY = object()


def _parse_bool(value: str) -> bool:
    """Read an explicit true or false value, independent of case."""
    return {"true": True, "false": False}[value.strip().lower()]


def _parse_environments(value: str) -> Mapping[str, str]:
    """Read a JSON object and leave image policy checks to the runtime."""
    images = json.loads(value)
    if not isinstance(images, dict):
        raise TypeError("expected a JSON object")
    return MappingProxyType(images)


@dataclass(frozen=True)
class ControlPlaneSettings:
    """Hold settings that the application consumes when assembling its dependencies."""

    runtime: str = "docker"
    readiness_timeout: float = field(default=1.0, metadata={"parse": float})
    sandbox_query_timeout: float = field(default=1.0, metadata={"parse": float})
    database_path: str | Path = DEFAULT_DATABASE_PATH
    reaper_enabled: bool = field(default=True, metadata={"parse": _parse_bool})
    reaper_interval_seconds: float = field(default=DEFAULT_REAPER_INTERVAL_SECONDS, metadata={"parse": float})
    reaper_batch_limit: int = field(default=DEFAULT_REAPER_BATCH_LIMIT, metadata={"parse": int})
    pending_recovery_enabled: bool = field(default=False, metadata={"parse": _parse_bool})
    pending_recovery_batch_limit: int = field(default=100, metadata={"parse": int})
    max_concurrent_creates: int = field(default=DEFAULT_MAX_CONCURRENT_CREATES, metadata={"parse": int})
    max_instances: int = field(default=DEFAULT_MAX_INSTANCES, metadata={"parse": int})
    additional_environments: Mapping[str, str] | None = field(default=None, metadata={"parse": _parse_environments})
    api_key: object = field(default=UNSET_API_KEY, repr=False)
    auth_enabled: bool = field(default=False, metadata={"parse": _parse_bool})

    def __post_init__(self) -> None:
        """Check runtime selection, deadlines and maintenance options before startup."""
        if self.runtime != "docker":
            raise ValueError("AGENT_SANDBOX_RUNTIME only supports docker")
        if type(self.auth_enabled) is not bool:
            raise ValueError("auth_enabled must be a boolean")
        for name in ("readiness_timeout", "sandbox_query_timeout"):
            seconds = getattr(self, name)
            if type(seconds) not in (float, int) or not 0 < seconds < math.inf:
                raise ValueError(f"{name} must be a positive finite number")
        validate_reaper_config(self.reaper_enabled, self.reaper_interval_seconds, self.reaper_batch_limit)
        if type(self.pending_recovery_enabled) is not bool:
            raise ValueError("pending recovery enabled must be a boolean")
        if type(self.pending_recovery_batch_limit) is not int or not 1 <= self.pending_recovery_batch_limit <= 2**63 - 1:
            raise ValueError("pending recovery batch limit must be a positive SQLite integer")

    @classmethod
    def from_env(cls) -> ControlPlaneSettings:
        """Read the local .env file and fill missing or blank values with defaults."""
        try:
            with ENV_FILE.open(encoding="utf-8-sig") as stream:
                file_values = dotenv_values(stream=stream, interpolate=False)
        except FileNotFoundError:
            file_values = {}

        values = {}
        for option in fields(cls):
            variable = ENV_PREFIX + option.name.upper()
            raw = file_values.get(variable)
            if raw is None or not raw.strip():
                continue
            try:
                if option.name == "database_path":
                    values[option.name] = CONTROL_PLANE_DIR / raw.strip()
                else:
                    values[option.name] = option.metadata.get("parse", str)(raw)
            except (KeyError, TypeError, ValueError):
                raise ValueError(f"invalid {variable}") from None
        return cls(**values)
