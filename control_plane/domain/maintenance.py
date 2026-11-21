"""Resource observations and bounded lifecycle maintenance results."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from control_plane.domain.sandbox import ResourceKind


@dataclass(frozen=True)
class SandboxReapItem:
    """Report one candidate's stable outcome without exposing backend error details."""

    sandbox_id: str
    outcome: Literal["succeeded", "skipped", "failed"]
    reason: str


@dataclass(frozen=True)
class SandboxReapResult:
    """Summarize a bounded explicit round and any store failure that stopped it."""

    now: datetime
    items: tuple[SandboxReapItem, ...] = ()
    error: str | None = None

    @property
    def succeeded(self) -> int:
        """Count candidates whose runtime resources and metadata reached deletion targets."""
        return sum(item.outcome == "succeeded" for item in self.items)

    @property
    def skipped(self) -> int:
        """Count candidates invalidated before runtime deletion could begin."""
        return sum(item.outcome == "skipped" for item in self.items)

    @property
    def failed(self) -> int:
        """Count attempted candidates that retain a failure or retry requirement."""
        return sum(item.outcome == "failed" for item in self.items)


@dataclass(frozen=True)
class SandboxResourceInspection:
    """Report read-only ownership observations, without asserting creation or health."""

    sandbox_id: str
    classification: Literal[
        "all_present", "partial", "not_found", "conflict", "unavailable", "changed",
    ]
    reason: str
    found_resources: tuple[ResourceKind, ...] = ()
    missing_resources: tuple[ResourceKind, ...] = ()


@dataclass(frozen=True)
class SandboxPendingInspectionResult:
    """Describe only this bounded batch and any storage failure that stopped inspection."""

    items: tuple[SandboxResourceInspection, ...] = ()
    error: str | None = None


@dataclass(frozen=True)
class SandboxPendingRecoveryItem:
    """Report confirmation of one original pending candidate using stable internal reasons."""

    sandbox_id: str
    outcome: Literal["recovered", "skipped", "failed"]
    reason: str


@dataclass(frozen=True)
class SandboxPendingRecoveryResult:
    """Summarize only this bounded recovery round and any store or backend outage."""

    items: tuple[SandboxPendingRecoveryItem, ...] = ()
    error: str | None = None

    @property
    def recovered(self) -> int:
        """Count original pending versions whose activation was acknowledged."""
        return sum(item.outcome == "recovered" for item in self.items)

    @property
    def skipped(self) -> int:
        """Count changed or ineligible candidates without asserting anything was repaired."""
        return sum(item.outcome == "skipped" for item in self.items)

    @property
    def failed(self) -> int:
        """Count failed observations or uncertain writes, which may already have committed."""
        return sum(item.outcome == "failed" for item in self.items)
