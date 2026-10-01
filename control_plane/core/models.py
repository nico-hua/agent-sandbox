"""Sandbox specifications, ownership metadata and public result types."""

import secrets
from dataclasses import dataclass, field
from typing import Literal


ResourceKind = Literal["container", "network", "workspace"]
SandboxStatus = Literal["started"]
SandboxObservedStatus = Literal["starting", "running", "ready", "stopped", "failed"]


def _new_transaction_id() -> str:
    """Generate an unpredictable per-attempt ownership token."""
    return f"txn_{secrets.token_hex(16)}"


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
