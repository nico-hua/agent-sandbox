"""Backend-independent asynchronous contract for sandbox operations."""

from abc import ABC, abstractmethod

from control_plane.core.models import (
    SandboxEndpoint,
    SandboxQueryResult,
    SandboxResourceInspection,
    SandboxSpec,
    WorkspaceBinding,
    WorkspaceRecord,
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

    async def resolve_environment_image(self, environment: str) -> str:
        """Resolve a selected preset to an immutable image before reserving a workspace."""
        raise NotImplementedError

    async def create_workspace(self, record: WorkspaceRecord) -> None:
        """Create and initialize a new independent workspace without adopting existing storage."""
        raise NotImplementedError

    async def inspect_workspace(self, record: WorkspaceRecord) -> None:
        """Verify independent volume ownership and its actual persisted holder."""
        raise NotImplementedError

    async def delete_workspace(self, record: WorkspaceRecord) -> None:
        """Remove only unused independently owned storage and confirm absence."""
        raise NotImplementedError

    @abstractmethod
    async def pause(self, sandbox_id: str, *, expected_transaction_id: str, expected_workspace: WorkspaceBinding | None = None) -> None:
        """Freeze the verified main and optional egress containers; settle changes on cancellation."""

    @abstractmethod
    async def resume(self, sandbox_id: str, *, expected_transaction_id: str, expected_workspace: WorkspaceBinding | None = None) -> None:
        """Thaw verified egress before main without restarting stopped containers; settle cancellation."""

    @abstractmethod
    async def delete(
        self,
        sandbox_id: str,
        *,
        delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
        expected_workspace: WorkspaceBinding | None = None,
    ) -> None:
        """Verify every survivor's ownership and optional transaction; preserve workspace by default."""

    @abstractmethod
    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read verified ownership and runtime health facts, with initial lifecycle times from labels."""

    @abstractmethod
    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Verify local entry ownership and auth identity without handing out its raw secret.

        Authenticated entries include the proxy protocol version, transaction,
        creation time and configured-token digest for trusted Service comparison.
        A legacy anonymous entry is explicitly distinguished; neither implies readiness.
        Optional services_version reports verified HTTP application forwarding capability,
        independently of Agent access and without probing any application port.
        """

    @abstractmethod
    async def inspect_resources(
        self, sandbox_id: str, *, expected_transaction_id: str,
        expected_workspace: WorkspaceBinding | None = None,
    ) -> SandboxResourceInspection:
        """Read all resource roles and verify ownership without implying creation or readiness."""
