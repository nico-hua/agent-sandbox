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
        """Create resources; cancellation means exact rollback completed.

        Raise SandboxCleanupFailed with the sandbox ID if rollback is incomplete
        or uncertain so the Service can retain its persisted recovery record.
        Domain errors carry optional immutable creation_failure evidence: callers
        may compensate metadata only for UNCHANGED or ROLLED_BACK. For legacy
        runtimes, SandboxCreateFailed and SandboxResourceConflict default to
        ROLLED_BACK, while SandboxImageUnavailable defaults to UNCHANGED; raise
        another error or attach actual evidence when these guarantees do not hold.
        """

    @abstractmethod
    async def delete(
        self,
        sandbox_id: str,
        *,
        delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Verify optional expected identity on all survivors before reclaiming resources."""

    @abstractmethod
    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read the current state of one owned sandbox."""

    @abstractmethod
    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Read the verified local entry of one owned sandbox."""
