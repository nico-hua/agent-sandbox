"""Sandbox specifications, ownership metadata and public result types."""

import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal


ResourceKind = Literal["container", "network", "workspace", "proxy", "ingress"]
SandboxStatus = Literal["started"]
SandboxObservedStatus = Literal["starting", "running", "ready", "stopped", "failed"]


def _validate_lifecycle_times(
    created_at: datetime | None, expires_at: datetime | None,
) -> None:
    """Allow legacy absence or a complete ordered pair of timezone-aware UTC times."""
    if created_at is None and expires_at is None:
        return
    for value in (created_at, expires_at):
        if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
            raise ValueError("lifecycle times must be a complete timezone-aware UTC pair")
    if expires_at < created_at:
        raise ValueError("expires_at must not precede created_at")


def format_utc_datetime(value: datetime | None) -> str | None:
    """Serialize UTC lifecycle metadata as RFC 3339 with Z, retaining microseconds."""
    return None if value is None else value.isoformat().replace("+00:00", "Z")


def _new_transaction_id() -> str:
    """Generate an unpredictable per-attempt ownership token."""
    return f"txn_{secrets.token_hex(16)}"


@dataclass(frozen=True)
class SandboxSpec:
    """Describe fixed names and labels for one sandbox transaction."""

    sandbox_id: str
    container_name: str
    network_name: str
    volume_name: str
    transaction_id: str = field(default_factory=_new_transaction_id)
    created_at: datetime | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        """Validate supplied lifecycle metadata without generating times for old reads."""
        _validate_lifecycle_times(self.created_at, self.expires_at)

    @property
    def proxy_name(self) -> str:
        """Derive the dedicated proxy name from the sandbox identity."""
        return f"agent-sandbox-{self.sandbox_id}-proxy"

    @property
    def ingress_network_name(self) -> str:
        """Derive the dedicated ingress bridge name from the sandbox identity."""
        return f"agent-sandbox-{self.sandbox_id}-ingress"

    def labels_for(self, resource: ResourceKind) -> dict[str, str]:
        """Build complete ownership labels for one managed resource."""
        return {
            "io.agent-sandbox.managed": "true",
            "io.agent-sandbox.project": "agent-sandbox",
            "io.agent-sandbox.sandbox-id": self.sandbox_id,
            "io.agent-sandbox.transaction-id": self.transaction_id,
            "io.agent-sandbox.resource": resource,
        }


@dataclass(frozen=True)
class SandboxCreateResult:
    """Describe a Docker container that completed its start operation."""

    sandbox_id: str
    status: SandboxStatus
    created_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        """Require the initial UTC lifecycle times on successful creation."""
        if self.created_at is None or self.expires_at is None:
            raise ValueError("creation requires lifecycle times")
        _validate_lifecycle_times(self.created_at, self.expires_at)


@dataclass(frozen=True)
class SandboxQueryResult:
    """Describe the observed Docker and Agent health state of a sandbox."""

    sandbox_id: str
    status: SandboxObservedStatus
    reason: str
    message: str
    created_at: datetime | None = None
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        """Keep recovered lifecycle metadata complete, ordered and in UTC."""
        _validate_lifecycle_times(self.created_at, self.expires_at)


@dataclass(frozen=True)
class SandboxLifecycleRecord:
    """Describe mutable lifecycle metadata independently of initial Docker labels."""

    sandbox_id: str
    transaction_id: str
    created_at: datetime
    expires_at: datetime
    version: int = 0

    def __post_init__(self) -> None:
        """Require immutable identity, complete UTC times and a nonnegative version."""
        if not isinstance(self.sandbox_id, str) or not self.sandbox_id:
            raise ValueError("sandbox_id must be a nonempty string")
        if not isinstance(self.transaction_id, str) or not self.transaction_id:
            raise ValueError("transaction_id must be a nonempty string")
        if self.created_at is None or self.expires_at is None:
            raise ValueError("lifecycle records require both times")
        _validate_lifecycle_times(self.created_at, self.expires_at)
        if type(self.version) is not int or self.version < 0:
            raise ValueError("version must be a nonnegative integer")


@dataclass(frozen=True)
class SandboxEndpoint:
    """Describe the local WSL development URL of one sandbox entry proxy."""

    sandbox_id: str
    url: str
