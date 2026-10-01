"""Sandbox lifecycle domain models and minimal orchestration."""

import re
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal, Protocol


ResourceKind = Literal["container", "network", "workspace"]
SandboxStatus = Literal["started"]
SandboxObservedStatus = Literal["starting", "running", "ready", "stopped", "failed"]


def _new_transaction_id() -> str:
    """Generate an unpredictable per-attempt ownership token."""
    return f"txn_{secrets.token_hex(16)}"


class SandboxError(RuntimeError):
    """Base class for stable sandbox operation failures."""


class SandboxImageUnavailable(SandboxError):
    """Report that the fixed sandbox image is unavailable."""


class DockerUnavailable(SandboxError):
    """Report that the Docker runtime cannot serve the request."""


class SandboxResourceConflict(SandboxError):
    """Report a conflict with a generated sandbox resource name."""


class SandboxCreateFailed(SandboxError):
    """Report a sandbox creation failure after successful rollback."""


class SandboxCleanupFailed(SandboxError):
    """Report a sandbox creation failure with incomplete cleanup."""

    def __init__(self, sandbox_id: str) -> None:
        """Store the sandbox ID needed to locate managed leftovers."""
        super().__init__("sandbox cleanup failed")
        self.sandbox_id = sandbox_id


class SandboxNotFound(SandboxError):
    """Report that no owned sandbox exists for the requested ID."""


class SandboxQueryFailed(SandboxError):
    """Report an ambiguous or malformed managed Docker state."""


@dataclass(frozen=True)
class SandboxSpec:
    """Describe fixed names and labels for one sandbox transaction."""

    sandbox_id: str
    container_name: str
    network_name: str
    volume_name: str
    transaction_id: str = field(default_factory=_new_transaction_id)

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


@dataclass(frozen=True)
class SandboxQueryResult:
    """Describe the observed Docker and Agent health state of a sandbox."""

    sandbox_id: str
    status: SandboxObservedStatus
    reason: str
    message: str


class SandboxRuntime(Protocol):
    """Describe runtime operations needed by the sandbox service."""

    async def create(self, spec: SandboxSpec) -> None:
        """Create and start all resources described by one specification."""
        ...

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read the current state of one owned sandbox from the runtime."""
        ...


class SandboxCreator(Protocol):
    """Describe the HTTP layer's sandbox creation dependency."""

    async def create(self) -> SandboxCreateResult:
        """Create one sandbox and return its public result."""
        ...


class SandboxReader(Protocol):
    """Describe the HTTP layer's sandbox status dependency."""

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Return the current state of one owned sandbox."""
        ...


def _new_sandbox_id() -> str:
    """Generate an unpredictable sandbox ID with 128 random bits."""
    return f"sbx_{secrets.token_hex(16)}"


class SandboxService:
    """Generate sandbox metadata and delegate lifecycle operations."""

    def __init__(
        self,
        runtime: SandboxRuntime,
        id_factory: Callable[[], str] | None = None,
        transaction_id_factory: Callable[[], str] | None = None,
    ) -> None:
        """Store runtime and optional deterministic identity factories."""
        self._runtime = runtime
        self._id_factory = id_factory if id_factory is not None else _new_sandbox_id
        self._transaction_id_factory = (
            transaction_id_factory
            if transaction_id_factory is not None
            else _new_transaction_id
        )

    async def create(self) -> SandboxCreateResult:
        """Create one sandbox and report only after the runtime succeeds."""
        sandbox_id = self._id_factory()
        spec = SandboxSpec(
            sandbox_id=sandbox_id,
            container_name=f"agent-sandbox-{sandbox_id}",
            network_name=f"agent-sandbox-{sandbox_id}-internal",
            volume_name=f"agent-sandbox-{sandbox_id}-workspace",
            transaction_id=self._transaction_id_factory(),
        )
        await self._runtime.create(spec)
        return SandboxCreateResult(sandbox_id=sandbox_id, status="started")

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Validate a public ID and resolve its state through the runtime."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        return await self._runtime.get(sandbox_id)
