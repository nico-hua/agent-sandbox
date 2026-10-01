"""Sandbox creation domain models and orchestration."""

import secrets
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol


ResourceKind = Literal["container", "network", "workspace"]
SandboxStatus = Literal["started"]


class SandboxError(RuntimeError):
    """Base class for stable sandbox creation failures."""


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


@dataclass(frozen=True)
class SandboxSpec:
    """Describe fixed names and labels for one sandbox transaction."""

    sandbox_id: str
    container_name: str
    network_name: str
    volume_name: str

    def labels_for(self, resource: ResourceKind) -> dict[str, str]:
        """Build complete ownership labels for one managed resource."""
        return {
            "io.agent-sandbox.managed": "true",
            "io.agent-sandbox.project": "agent-sandbox",
            "io.agent-sandbox.sandbox-id": self.sandbox_id,
            "io.agent-sandbox.resource": resource,
        }


@dataclass(frozen=True)
class SandboxCreateResult:
    """Describe a Docker container that completed its start operation."""

    sandbox_id: str
    status: SandboxStatus


class SandboxRuntime(Protocol):
    """Describe the runtime operation needed by the sandbox service."""

    async def create(self, spec: SandboxSpec) -> None:
        """Create and start all resources described by one specification."""
        ...


class SandboxCreator(Protocol):
    """Describe the HTTP layer's sandbox creation dependency."""

    async def create(self) -> SandboxCreateResult:
        """Create one sandbox and return its public result."""
        ...


def _new_sandbox_id() -> str:
    """Generate an unpredictable sandbox ID with 128 random bits."""
    return f"sbx_{secrets.token_hex(16)}"


class SandboxService:
    """Generate sandbox metadata and delegate resource creation."""

    def __init__(
        self,
        runtime: SandboxRuntime,
        id_factory: Callable[[], str] | None = None,
    ) -> None:
        """Store the runtime and an optional deterministic ID factory."""
        self._runtime = runtime
        self._id_factory = id_factory if id_factory is not None else _new_sandbox_id

    async def create(self) -> SandboxCreateResult:
        """Create one sandbox and report only after the runtime succeeds."""
        sandbox_id = self._id_factory()
        spec = SandboxSpec(
            sandbox_id=sandbox_id,
            container_name=f"agent-sandbox-{sandbox_id}",
            network_name=f"agent-sandbox-{sandbox_id}-internal",
            volume_name=f"agent-sandbox-{sandbox_id}-workspace",
        )
        await self._runtime.create(spec)
        return SandboxCreateResult(sandbox_id=sandbox_id, status="started")
