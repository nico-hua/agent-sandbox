"""Sandbox specifications, lifecycle metadata and query results."""

import base64
import binascii
import json
import re
import secrets
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

from control_plane.domain.endpoint import _validate_access_token
from control_plane.domain.validation import _validate_integer, _validate_lifecycle_times
from control_plane.domain.workspace import WorkspaceBinding, _validate_workspace_id

ResourceKind = Literal["container", "network", "workspace", "proxy", "ingress", "egress"]
SandboxStatus = Literal["started"]
SandboxObservedStatus = Literal["starting", "running", "ready", "paused", "stopped", "failed"]
SandboxManagementState = Literal["pending", "active", "deleting"]
SandboxEnvironment = str
BUILTIN_ENVIRONMENTS = ("basic", "python", "node")
MAX_CUSTOM_ENVIRONMENTS = 32
DEFAULT_TTL_SECONDS = 1800
DEFAULT_CPU_MILLIS = 1000
DEFAULT_MEMORY_MIB = 256


def _validate_environment(value: SandboxEnvironment) -> None:
    """Validate bounded ASCII names; registration belongs to the Service configuration."""
    if type(value) is not str or re.fullmatch(r"[a-z][a-z0-9-]{0,31}", value, flags=re.ASCII) is None:
        raise ValueError("environment must match [a-z][a-z0-9-]{0,31}")


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
        digits = label.removeprefix("0x") if label.startswith("0x") else label
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
    """Describe a named environment, initial TTL and constrained resources for creation."""

    ttl_seconds: int = DEFAULT_TTL_SECONDS
    resources: SandboxResources = field(default_factory=SandboxResources)
    environment: SandboxEnvironment = "basic"
    network: SandboxNetwork = field(default_factory=SandboxNetwork)
    workspace_id: str | None = None

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
        if self.workspace_id is not None:
            _validate_workspace_id(self.workspace_id)


def format_utc_datetime(value: datetime | None) -> str | None:
    """Serialize UTC lifecycle metadata as RFC 3339 with Z, retaining microseconds."""
    return None if value is None else value.isoformat().replace("+00:00", "Z")


def _new_transaction_id() -> str:
    """Generate an unpredictable per-attempt ownership token."""
    return f"txn_{secrets.token_hex(16)}"


@dataclass(frozen=True)
class SandboxSpec:
    """Describe an environment's identity, initial times and resource ceilings for one transaction."""

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
    workspace: WorkspaceBinding | None = None
    access_token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        """Validate the environment name, resource ceilings and supplied initial times."""
        _validate_environment(self.environment)
        _validate_lifecycle_times(self.created_at, self.expires_at)
        if type(self.resources) is not SandboxResources:
            raise ValueError("resources must be SandboxResources")
        self.resources.validate()
        if type(self.network) is not SandboxNetwork:
            raise ValueError("network must be SandboxNetwork")
        self.network.validate()
        if self.access_token is not None:
            _validate_access_token(self.access_token)
        if self.workspace is not None and (
            type(self.workspace) is not WorkspaceBinding
            or self.workspace.volume_name != self.volume_name
            or self.workspace.sandbox_id != self.sandbox_id
            or self.workspace.transaction_id != self.transaction_id
        ):
            raise ValueError("workspace binding must match the sandbox transaction")
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
    workspace_id: str | None = None

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
    workspace_id: str | None = None
    workspace: WorkspaceBinding | None = None

    def __post_init__(self) -> None:
        """Keep recovered lifecycle metadata complete, ordered and in UTC."""
        _validate_lifecycle_times(self.created_at, self.expires_at)
        if self.workspace is not None and (
            type(self.workspace) is not WorkspaceBinding or self.workspace.workspace_id != self.workspace_id
        ):
            raise ValueError("observed workspace identity is inconsistent")


@dataclass(frozen=True)
class SandboxLifecycleRecord:
    """Describe mutable lifecycle metadata independently of initial Docker labels."""

    sandbox_id: str
    transaction_id: str
    created_at: datetime
    expires_at: datetime
    version: int = 0
    management_state: SandboxManagementState = "pending"
    workspace_id: str | None = None

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
        if self.workspace_id is not None:
            _validate_workspace_id(self.workspace_id)


def encode_sandbox_cursor(created_at: datetime, sandbox_id: str) -> str:
    """Encode an exact ordering position, without identity secrets or a paging session."""
    delta = created_at - datetime(1970, 1, 1, tzinfo=UTC)
    micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
    payload = json.dumps([micros, sandbox_id], separators=(",", ":")).encode("ascii")
    return "v1." + base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def validate_sandbox_list_query(limit: int, cursor: str | None) -> tuple[int, str] | None:
    """Require bounded page sizes and canonical version-one integer-microsecond positions."""
    _validate_integer(limit, 1, 100, "limit")
    if cursor is None:
        return None
    if type(cursor) is not str or len(cursor) > 256 or re.fullmatch(r"v1\.[A-Za-z0-9_-]+", cursor) is None:
        raise ValueError("invalid sandbox list cursor")
    try:
        encoded = cursor[3:]
        raw = base64.b64decode(encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
        position = json.loads(raw.decode("ascii"))
        if (type(position) is not list or len(position) != 2
                or type(position[0]) is not int or not -62135596800000000 <= position[0] <= 253402300799999999
                or type(position[1]) is not str or re.fullmatch(r"sbx_[0-9a-f]{32}", position[1]) is None):
            raise ValueError()
        canonical = base64.urlsafe_b64encode(json.dumps(position, separators=(",", ":")).encode("ascii")).decode("ascii").rstrip("=")
        if encoded != canonical:
            raise ValueError()
    except (ValueError, UnicodeError, binascii.Error, RecursionError):
        raise ValueError("invalid sandbox list cursor") from None
    return position[0], position[1]


@dataclass(frozen=True)
class SandboxManagementSummary:
    """Expose persisted management facts without implying runtime existence or health."""

    sandbox_id: str
    management_state: SandboxManagementState
    created_at: datetime
    expires_at: datetime
    workspace_id: str | None

    def __post_init__(self) -> None:
        """Reject malformed persisted public metadata without reading private identity fields."""
        if type(self.sandbox_id) is not str or re.fullmatch(r"sbx_[0-9a-f]{32}", self.sandbox_id) is None:
            raise ValueError("invalid sandbox_id")
        if self.management_state not in ("pending", "active", "deleting"):
            raise ValueError("invalid management_state")
        if self.created_at is None or self.expires_at is None:
            raise ValueError("management summaries require both times")
        _validate_lifecycle_times(self.created_at, self.expires_at)
        if self.workspace_id is not None:
            _validate_workspace_id(self.workspace_id)


@dataclass(frozen=True)
class SandboxListPage:
    """Hold a SQL-bounded public page and count from the same read snapshot."""

    items: tuple[SandboxManagementSummary, ...]
    next_cursor: str | None
    managed_count: int


@dataclass(frozen=True)
class SandboxListResult(SandboxListPage):
    """Add the current trusted instance limit without rejecting an existing excess count."""

    max_instances: int


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
