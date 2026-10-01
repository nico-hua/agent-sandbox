"""Sandbox identity generation, validation and lifecycle orchestration."""

import re
import secrets
from collections.abc import Callable

from control_plane.core.errors import SandboxNotFound
from control_plane.core.models import (
    SandboxCreateResult,
    SandboxQueryResult,
    SandboxSpec,
    _new_transaction_id,
)
from control_plane.runtime.sandbox_runtime import SandboxRuntime


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

    async def ping(self) -> None:
        """Check backend availability and propagate its domain errors."""
        await self._runtime.ping()

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
