"""Lifecycle metadata contract and stable storage errors."""

from datetime import datetime
from typing import Literal, Protocol

from control_plane.domain.models import (
    SandboxAccessCredential,
    SandboxLifecycleRecord,
    SandboxListPage,
    WorkspaceBinding,
    WorkspaceRecord,
)


class LifecycleStore(Protocol):
    """Expose only initialization and identity-guarded metadata used by the service."""

    async def initialize(self) -> None:
        """Explicitly initialize durable storage or fail application startup."""
        ...

    async def create(self, record: SandboxLifecycleRecord, *, credential: SandboxAccessCredential | None = None, max_instances: int = 10) -> None:
        """Atomically check total capacity and insert pending identity and its credential."""
        ...

    async def list_sandboxes(self, *, limit: int = 20, cursor: str | None = None) -> SandboxListPage:
        """Read a bounded public page and total lifecycle count within one snapshot."""
        ...

    async def get_credential(self, sandbox_id: str, transaction_id: str) -> SandboxAccessCredential | None:
        """Read a credential only for the matching current lifecycle transaction."""
        ...

    async def get(self, sandbox_id: str) -> SandboxLifecycleRecord | None:
        """Read the current metadata or return None without adopting resources."""
        ...

    async def update_expiration(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
        expires_at: datetime,
    ) -> SandboxLifecycleRecord:
        """Commit expiration only for the same non-deleting transaction and version."""
        ...

    async def activate(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> SandboxLifecycleRecord:
        """Confirm a pending creation using its original identity and current version."""
        ...

    async def mark_deleting(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> SandboxLifecycleRecord:
        """Enter or resume deletion without adopting changed lifecycle metadata."""
        ...

    async def claim_expired(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
        now: datetime,
    ) -> SandboxLifecycleRecord | None:
        """Atomically claim a still-due active version or return None when it changed."""
        ...

    async def list_reap_candidates(
        self, now: datetime, *, limit: int = 100,
    ) -> list[SandboxLifecycleRecord]:
        """List a bounded stable batch of due active records and deletion retries."""
        ...

    async def list_pending(self, *, limit: int = 100) -> list[SandboxLifecycleRecord]:
        """Read one bounded pending batch without confirming or changing creation state."""
        ...

    async def delete(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> None:
        """Remove only the matching transaction and version, allowing absent retries."""
        ...

    async def create_workspace(self, record: WorkspaceRecord) -> None:
        """Insert unoccupied version-zero pending workspace metadata without replacement."""
        ...

    async def get_workspace(self, workspace_id: str) -> WorkspaceRecord | None:
        """Read workspace ownership with its separately persisted current holder."""
        ...

    async def confirm_workspace(
        self, workspace_id: str, creation_token: str, *, expected_version: int,
    ) -> WorkspaceRecord:
        """Confirm the original pending creation and advance its workspace version."""
        ...

    async def claim_workspace_deletion(
        self, workspace_id: str, creation_token: str, *, expected_version: int,
    ) -> WorkspaceRecord:
        """Persist deletion intent only while unoccupied, accepting an exact deleting retry."""
        ...

    async def create_with_workspace(
        self, record: SandboxLifecycleRecord, binding: WorkspaceBinding, *, expected_workspace_version: int,
        credential: SandboxAccessCredential | None = None, max_instances: int = 10,
    ) -> None:
        """Atomically prewrite a pending sandbox and acquire an available workspace."""
        ...

    async def get_workspace_binding(self, sandbox_id: str, transaction_id: str) -> WorkspaceBinding | None:
        """Read the original holder and available workspace identities in one snapshot."""
        ...

    async def delete_workspace(self, workspace_id: str, creation_token: str, *, expected_version: int) -> None:
        """Remove only an unoccupied deletion claim, accepting absent retries."""
        ...


class LifecycleStoreError(RuntimeError):
    """Report a stable persistence failure without exposing database paths or SQL."""

    def __init__(self) -> None:
        """Set the fixed error message shared by SQLite access and corruption failures."""
        super().__init__("lifecycle storage operation failed")


class LifecycleInstanceLimitExceeded(LifecycleStoreError):
    """Report a full managed-instance registry without disguising storage failures."""

    def __init__(self) -> None:
        """Expose a fixed expected admission result without backend diagnostics."""
        RuntimeError.__init__(self, "sandbox instance limit exceeded")


class LifecycleStoreConflict(LifecycleStoreError):
    """Reject an existing identity, mismatched transaction or stale record version."""

    def __init__(
        self, reason: Literal[
            "already_exists", "transaction_mismatch", "version_conflict", "state_conflict",
            "workspace_conflict",
        ],
    ) -> None:
        """Expose a machine-readable conflict reason without backend details."""
        RuntimeError.__init__(self, f"lifecycle record conflict: {reason}")
        self.reason = reason


class LifecycleRecordNotFound(LifecycleStoreError):
    """Report an update whose lifecycle record no longer exists."""

    def __init__(self) -> None:
        """Set a stable missing-record error for expiration updates."""
        RuntimeError.__init__(self, "lifecycle record not found")


class LifecycleSchemaError(LifecycleStoreError):
    """Reject a database that is uninitialized or uses an incompatible schema."""

    def __init__(self) -> None:
        """Set the fixed schema error without attempting migration or repair."""
        RuntimeError.__init__(self, "lifecycle database schema is incompatible")
