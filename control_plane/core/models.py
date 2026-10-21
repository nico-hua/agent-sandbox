"""Sandbox specifications, ownership metadata and public result types."""

import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal

ResourceKind = Literal["container", "network", "workspace", "proxy", "ingress"]
SandboxStatus = Literal["started"]
SandboxObservedStatus = Literal["starting", "running", "ready", "stopped", "failed"]
SandboxManagementState = Literal["pending", "active", "deleting"]
DEFAULT_TTL_SECONDS = 1800
DEFAULT_CPU_MILLIS = 1000
DEFAULT_MEMORY_MIB = 256


def _validate_integer(value: int, minimum: int, maximum: int, name: str) -> None:
    """Require an actual integer in range without accepting booleans or coercing inputs."""
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")


@dataclass(frozen=True)
class SandboxResources:
    """Describe per-instance CPU and memory ceilings, without reserving host capacity."""

    cpu_millis: int = DEFAULT_CPU_MILLIS
    memory_mib: int = DEFAULT_MEMORY_MIB

    def __post_init__(self) -> None:
        """Reject resource values outside the constrained creation policy."""
        self.validate()

    def validate(self) -> None:
        """Recheck strict resource bounds for direct Service and specification callers."""
        _validate_integer(self.cpu_millis, 100, 2000, "cpu_millis")
        _validate_integer(self.memory_mib, 128, 2048, "memory_mib")


@dataclass(frozen=True)
class SandboxCreateOptions:
    """Describe the optional initial TTL and constrained resources of a new sandbox."""

    ttl_seconds: int = DEFAULT_TTL_SECONDS
    resources: SandboxResources = field(default_factory=SandboxResources)

    def __post_init__(self) -> None:
        """Validate supplied options without silently replacing explicit invalid values."""
        self.validate()

    def validate(self) -> None:
        """Keep non-HTTP creation subject to the same TTL and resource limits."""
        _validate_integer(self.ttl_seconds, 1, 86400, "ttl_seconds")
        if type(self.resources) is not SandboxResources:
            raise ValueError("resources must be SandboxResources")
        self.resources.validate()


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
    """Describe names, ownership, initial times and resource ceilings for one transaction."""

    sandbox_id: str
    container_name: str
    network_name: str
    volume_name: str
    transaction_id: str = field(default_factory=_new_transaction_id)
    created_at: datetime | None = None
    expires_at: datetime | None = None
    resources: SandboxResources = field(default_factory=SandboxResources)

    def __post_init__(self) -> None:
        """Validate resource ceilings and supplied times without inventing lifecycle metadata."""
        _validate_lifecycle_times(self.created_at, self.expires_at)
        if type(self.resources) is not SandboxResources:
            raise ValueError("resources must be SandboxResources")
        self.resources.validate()

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
    transaction_id: str | None = None

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
    management_state: SandboxManagementState = "pending"

    def __post_init__(self) -> None:
        """Require immutable identity, UTC times, a version and a known management state."""
        if not isinstance(self.sandbox_id, str) or not self.sandbox_id:
            raise ValueError("sandbox_id must be a nonempty string")
        if not isinstance(self.transaction_id, str) or not self.transaction_id:
            raise ValueError("transaction_id must be a nonempty string")
        if self.created_at is None or self.expires_at is None:
            raise ValueError("lifecycle records require both times")
        _validate_lifecycle_times(self.created_at, self.expires_at)
        if type(self.version) is not int or self.version < 0:
            raise ValueError("version must be a nonnegative integer")
        if self.management_state not in ("pending", "active", "deleting"):
            raise ValueError("management_state must be pending, active or deleting")


@dataclass(frozen=True)
class SandboxEndpoint:
    """Describe the local WSL development URL of one sandbox entry proxy."""

    sandbox_id: str
    url: str


@dataclass(frozen=True)
class SandboxReadyResult:
    """Describe one successful Agent HTTP link observation without a future readiness guarantee."""

    sandbox_id: str
    status: Literal["ready"] = "ready"


@dataclass(frozen=True)
class SandboxRenewResult:
    """Describe the committed expiration without implying runtime health or readiness."""

    sandbox_id: str
    expires_at: datetime


@dataclass(frozen=True)
class SandboxReapItem:
    """Report one candidate's stable outcome without exposing backend error details."""

    sandbox_id: str
    outcome: Literal["succeeded", "skipped", "failed"]
    reason: str


@dataclass(frozen=True)
class SandboxReapResult:
    """Summarize a bounded explicit round and any store failure that stopped it."""

    now: datetime
    items: tuple[SandboxReapItem, ...] = ()
    error: str | None = None

    @property
    def succeeded(self) -> int:
        """Count candidates whose runtime resources and metadata reached deletion targets."""
        return sum(item.outcome == "succeeded" for item in self.items)

    @property
    def skipped(self) -> int:
        """Count candidates invalidated before runtime deletion could begin."""
        return sum(item.outcome == "skipped" for item in self.items)

    @property
    def failed(self) -> int:
        """Count attempted candidates that retain a failure or retry requirement."""
        return sum(item.outcome == "failed" for item in self.items)


@dataclass(frozen=True)
class SandboxResourceInspection:
    """Report read-only ownership observations, without asserting creation or health."""

    sandbox_id: str
    classification: Literal[
        "all_present", "partial", "not_found", "conflict", "unavailable", "changed",
    ]
    reason: str
    found_resources: tuple[ResourceKind, ...] = ()
    missing_resources: tuple[ResourceKind, ...] = ()


@dataclass(frozen=True)
class SandboxPendingInspectionResult:
    """Describe only this bounded batch and any storage failure that stopped inspection."""

    items: tuple[SandboxResourceInspection, ...] = ()
    error: str | None = None


@dataclass(frozen=True)
class SandboxPendingRecoveryItem:
    """Report confirmation of one original pending candidate using stable internal reasons."""

    sandbox_id: str
    outcome: Literal["recovered", "skipped", "failed"]
    reason: str


@dataclass(frozen=True)
class SandboxPendingRecoveryResult:
    """Summarize only this bounded recovery round and any store or backend outage."""

    items: tuple[SandboxPendingRecoveryItem, ...] = ()
    error: str | None = None

    @property
    def recovered(self) -> int:
        """Count original pending versions whose activation was acknowledged."""
        return sum(item.outcome == "recovered" for item in self.items)

    @property
    def skipped(self) -> int:
        """Count changed or ineligible candidates without asserting anything was repaired."""
        return sum(item.outcome == "skipped" for item in self.items)

    @property
    def failed(self) -> int:
        """Count failed observations or uncertain writes, which may already have committed."""
        return sum(item.outcome == "failed" for item in self.items)
