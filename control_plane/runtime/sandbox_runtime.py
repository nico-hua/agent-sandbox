"""Backend-independent asynchronous contract for sandbox operations."""

from abc import ABC, abstractmethod

from control_plane.core.models import (
    SandboxEndpoint,
    SandboxQueryResult,
    SandboxResourceInspection,
    SandboxSpec,
)


class SandboxRuntime(ABC):
    """Define readiness, lifecycle and read-only state/entry lookup for a backend."""

    @abstractmethod
    async def ping(self) -> None:
        """Check backend availability or raise a domain error."""

    @abstractmethod
    async def create(self, spec: SandboxSpec) -> None:
        """Create the selected preset with fixed security policy; cancellation means exact rollback.

        Incomplete or uncertain rollback raises SandboxCleanupFailed with the sandbox ID
        so Service retains recovery metadata. Only UNCHANGED or ROLLED_BACK evidence
        in creation_failure permits metadata removal.

        Legacy SandboxCreateFailed and SandboxResourceConflict default to ROLLED_BACK;
        SandboxImageUnavailable defaults to UNCHANGED. Attach actual evidence or use
        another error when these guarantees do not hold.
        """

    @abstractmethod
    async def pause(self, sandbox_id: str, *, expected_transaction_id: str) -> None:
        """Freeze the verified main and optional egress containers; settle changes on cancellation."""

    @abstractmethod
    async def resume(self, sandbox_id: str, *, expected_transaction_id: str) -> None:
        """Thaw verified egress before main without restarting stopped containers; settle cancellation."""

    @abstractmethod
    async def delete(
        self,
        sandbox_id: str,
        *,
        delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Verify every survivor's ownership and optional transaction; preserve workspace by default."""

    @abstractmethod
    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read verified ownership and runtime health facts, with initial lifecycle times from labels."""

    @abstractmethod
    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Read a local entry after ownership and binding checks, without implying Agent readiness."""

    @abstractmethod
    async def inspect_resources(
        self, sandbox_id: str, *, expected_transaction_id: str,
    ) -> SandboxResourceInspection:
        """Read all resource roles and verify ownership without implying creation or readiness."""
