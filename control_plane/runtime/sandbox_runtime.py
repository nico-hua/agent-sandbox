"""Backend-independent asynchronous contract for sandbox operations."""

from abc import ABC, abstractmethod

from control_plane.core.models import SandboxEndpoint, SandboxQueryResult, SandboxSpec


class SandboxRuntime(ABC):
    """Define readiness, lifecycle and read-only state/entry lookup for a backend."""

    @abstractmethod
    async def ping(self) -> None:
        """Check backend availability or raise a domain error."""

    @abstractmethod
    async def create(self, spec: SandboxSpec) -> None:
        """Create and start the resources described by one specification."""

    @abstractmethod
    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False
    ) -> None:
        """Reclaim owned resources, preserving workspace unless explicitly opted in."""

    @abstractmethod
    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read the current state of one owned sandbox."""

    @abstractmethod
    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Read the verified local entry of one owned sandbox."""
