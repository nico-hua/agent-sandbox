"""Sandbox identity generation, validation and lifecycle orchestration."""

import re
import secrets
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from control_plane.core.errors import SandboxNotFound
from control_plane.core.models import (
    SandboxCreateResult,
    SandboxEndpoint,
    SandboxQueryResult,
    SandboxSpec,
    _new_transaction_id,
)
from control_plane.runtime.sandbox_runtime import SandboxRuntime


DEFAULT_TTL_SECONDS = 1800


def _utc_now() -> datetime:
    """Read the current timezone-aware UTC time for one creation snapshot."""
    return datetime.now(UTC)


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
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Store runtime, identity factories and a lazily sampled creation clock."""
        self._runtime = runtime
        self._id_factory = id_factory if id_factory is not None else _new_sandbox_id
        self._transaction_id_factory = (
            transaction_id_factory
            if transaction_id_factory is not None
            else _new_transaction_id
        )
        self._clock = clock if clock is not None else _utc_now

    async def ping(self) -> None:
        """Check backend availability and propagate its domain errors."""
        await self._runtime.ping()

    async def create(self) -> SandboxCreateResult:
        """Snapshot initial UTC lifecycle times before creating one fixed-TTL sandbox."""
        created_at = self._clock()
        if created_at.utcoffset() is None:
            raise ValueError("creation clock must return a timezone-aware datetime")
        created_at = created_at.astimezone(UTC)
        expires_at = created_at + timedelta(seconds=DEFAULT_TTL_SECONDS)
        sandbox_id = self._id_factory()
        spec = SandboxSpec(
            sandbox_id=sandbox_id,
            container_name=f"agent-sandbox-{sandbox_id}",
            network_name=f"agent-sandbox-{sandbox_id}-internal",
            volume_name=f"agent-sandbox-{sandbox_id}-workspace",
            transaction_id=self._transaction_id_factory(),
            created_at=created_at,
            expires_at=expires_at,
        )
        await self._runtime.create(spec)
        return SandboxCreateResult(
            sandbox_id=sandbox_id, status="started",
            created_at=created_at, expires_at=expires_at,
        )

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Validate a public ID and resolve its state through the runtime."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        return await self._runtime.get(sandbox_id)

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False
    ) -> None:
        """Validate a public ID and reclaim resources with an explicit data policy."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        await self._runtime.delete(sandbox_id, delete_workspace=delete_workspace)

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Validate a public ID and resolve its local entry through the runtime."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        return await self._runtime.get_endpoint(sandbox_id)
