"""Independent workspace identity, occupancy and sandbox bindings."""

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

from control_plane.domain.validation import _validate_integer, _validate_lifecycle_times


def _validate_workspace_id(value: str) -> None:
    """Require a server-generated opaque workspace ID, never a volume name or path."""
    if type(value) is not str or re.fullmatch(r"wsp_[0-9a-f]{32}", value) is None:
        raise ValueError("invalid workspace_id")


@dataclass(frozen=True)
class WorkspaceRecord:
    """Describe an independently owned volume and its current persistent exclusive holder."""

    workspace_id: str
    creation_token: str
    volume_name: str
    image_id: str
    created_at: datetime
    version: int = 0
    management_state: Literal["pending", "available", "deleting"] = "pending"
    sandbox_id: str | None = None
    transaction_id: str | None = None

    def __post_init__(self) -> None:
        """Keep independent ownership, exact image identity and optional holder well formed."""
        _validate_workspace_id(self.workspace_id)
        if type(self.creation_token) is not str or re.fullmatch(r"wct_[0-9a-f]{32}", self.creation_token) is None:
            raise ValueError("invalid workspace creation token")
        if self.volume_name != f"agent-sandbox-{self.workspace_id}":
            raise ValueError("invalid managed workspace volume name")
        if type(self.image_id) is not str or re.fullmatch(r"sha256:[0-9a-f]{64}", self.image_id) is None:
            raise ValueError("invalid workspace image identity")
        _validate_lifecycle_times(self.created_at, self.created_at)
        if self.created_at is None:
            raise ValueError("workspace creation time is required")
        _validate_integer(self.version, 0, 2**63 - 1, "workspace version")
        if self.management_state not in ("pending", "available", "deleting"):
            raise ValueError("invalid workspace management state")
        if (self.sandbox_id is None) != (self.transaction_id is None):
            raise ValueError("workspace holder requires both identities")
        if self.sandbox_id is not None and (
            type(self.sandbox_id) is not str or re.fullmatch(r"sbx_[0-9a-f]{32}", self.sandbox_id) is None
            or type(self.transaction_id) is not str or re.fullmatch(r"txn_[0-9a-f]{32}", self.transaction_id) is None
        ):
            raise ValueError("invalid workspace holder identity")


@dataclass(frozen=True)
class WorkspaceBinding:
    """Pin one sandbox transaction to an independent workspace and its occupancy version."""

    workspace_id: str
    creation_token: str
    volume_name: str
    image_id: str
    version: int
    sandbox_id: str
    transaction_id: str

    def __post_init__(self) -> None:
        """Validate all immutable binding fields without inventing a clock or ownership."""
        if self.sandbox_id is None or self.transaction_id is None:
            raise ValueError("workspace binding requires a sandbox transaction")
        WorkspaceRecord(
            self.workspace_id, self.creation_token, self.volume_name, self.image_id,
            datetime(1970, 1, 1, tzinfo=UTC), self.version, "available",
            self.sandbox_id, self.transaction_id,
        )
