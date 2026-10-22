"""Sandbox specifications, ownership metadata and public result types."""

import re
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal

ResourceKind = Literal["container", "network", "workspace", "proxy", "ingress", "egress"]
SandboxStatus = Literal["started"]
SandboxObservedStatus = Literal["starting", "running", "ready", "stopped", "failed"]
SandboxManagementState = Literal["pending", "active", "deleting"]
SandboxEnvironment = Literal["basic", "python", "node"]
DEFAULT_TTL_SECONDS = 1800
DEFAULT_CPU_MILLIS = 1000
DEFAULT_MEMORY_MIB = 256


def _validate_integer(value: int, minimum: int, maximum: int, name: str) -> None:
    """Require an actual integer in range without accepting booleans or coercing inputs."""
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")


def _validate_environment(value: SandboxEnvironment) -> None:
    """Accept only exact preset names without coercion or backend-specific image addresses."""
    if type(value) is not str or value not in ("basic", "python", "node"):
        raise ValueError("environment must be basic, python or node")


def _normalize_domain(domain: str) -> str:
    """Match the Go egress policy's ASCII DNS and conservative numeric-address rules."""
    if type(domain) is not str or not domain.isascii():
        raise ValueError("allow_domains entries must be ASCII DNS names")
    host = domain.lower().removesuffix(".")
    if not host or len(host) > 253:
        raise ValueError("allow_domains entries must be DNS names of at most 253 bytes")
    all_numeric = True
    for label in host.split("."):
        if not re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label):
            raise ValueError("allow_domains entries must use valid DNS labels")
        # The proxy treats even empty 0x suffixes and overflowing numeric forms
        # conservatively as address syntax, without parsing integers or doing DNS.
        digits = label[2:] if label.startswith("0x") else label
        alphabet = "0123456789abcdef" if label.startswith("0x") else "0123456789"
        all_numeric = all_numeric and all(character in alphabet for character in digits)
    if all_numeric:
        raise ValueError("allow_domains entries must not be numeric addresses")
    return host


def _normalize_allow_domains(domains: tuple[str, ...] | list[str]) -> tuple[str, ...]:
    """Bound supplied entries before copying, normalizing and coalescing duplicates."""
    if type(domains) not in (list, tuple) or len(domains) > 32:
        raise ValueError("allow_domains must be a list or tuple of at most 32 domains")
    return tuple(sorted({_normalize_domain(domain) for domain in domains}))


@dataclass(frozen=True)
class SandboxNetwork:
    """Hold immutable exact DNS domains; an empty tuple disables the egress sidecar."""

    allow_domains: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Copy mutable input into a deterministic immutable domain policy."""
        object.__setattr__(self, "allow_domains", _normalize_allow_domains(self.allow_domains))

    def validate(self) -> None:
        """Reject forged mutable or noncanonical settings before lifecycle side effects."""
        if (
            type(self.allow_domains) is not tuple
            or _normalize_allow_domains(self.allow_domains) != self.allow_domains
        ):
            raise ValueError("allow_domains must be a canonical immutable domain tuple")


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
    """Describe a preset environment, initial TTL and constrained resources for creation."""

    ttl_seconds: int = DEFAULT_TTL_SECONDS
    resources: SandboxResources = field(default_factory=SandboxResources)
    environment: SandboxEnvironment = "basic"
    network: SandboxNetwork = field(default_factory=SandboxNetwork)

    def __post_init__(self) -> None:
        """Validate supplied options without silently replacing explicit invalid values."""
        self.validate()

    def validate(self) -> None:
        """Keep non-HTTP creation subject to the same environment, TTL and resource policy."""
        _validate_environment(self.environment)
        _validate_integer(self.ttl_seconds, 1, 86400, "ttl_seconds")
        if type(self.resources) is not SandboxResources:
            raise ValueError("resources must be SandboxResources")
        self.resources.validate()
        if type(self.network) is not SandboxNetwork:
            raise ValueError("network must be SandboxNetwork")
        self.network.validate()


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
    """Describe a preset's identity, initial times and resource ceilings for one transaction."""

    sandbox_id: str
    container_name: str
    network_name: str
    volume_name: str
    transaction_id: str = field(default_factory=_new_transaction_id)
    created_at: datetime | None = None
    expires_at: datetime | None = None
    resources: SandboxResources = field(default_factory=SandboxResources)
    environment: SandboxEnvironment = "basic"
    network: SandboxNetwork = field(default_factory=SandboxNetwork)

    def __post_init__(self) -> None:
        """Validate the preset, resource ceilings and supplied times without inventing metadata."""
        _validate_environment(self.environment)
        _validate_lifecycle_times(self.created_at, self.expires_at)
        if type(self.resources) is not SandboxResources:
            raise ValueError("resources must be SandboxResources")
        self.resources.validate()
        if type(self.network) is not SandboxNetwork:
            raise ValueError("network must be SandboxNetwork")
        self.network.validate()

    @property
    def egress_name(self) -> str:
        """Derive the optional egress sidecar name from the sandbox identity."""
        return f"agent-sandbox-{self.sandbox_id}-egress"

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
