"""One bounded startup round; Service validates metadata while the existing manager owns processes."""

import asyncio
from dataclasses import dataclass
from time import monotonic

from control_plane.core.errors import (
    KubernetesConfigurationError, KubernetesConnectionFailed, KubernetesPermissionDenied, KubernetesTimeout,
    SandboxMetadataUnavailable, SandboxMetadataConflict, SandboxNotFound, SandboxQueryFailed,
)
from control_plane.core.tasks import settle_task, wait_for_task
from control_plane.runtime.kubernetes.local_entry import LocalEntryError


@dataclass(frozen=True)
class EntryRestoreItem:
    """Keep only a public ID and fixed classification, never credentials or backend exception text."""

    sandbox_id: str
    outcome: str
    reason: str


@dataclass(frozen=True)
class EntryRestoreResult:
    """Bound diagnostics to the selected batch and report stopping conditions separately."""

    items: tuple[EntryRestoreItem, ...] = ()
    selected: int = 0
    error: str | None = None
    stopped: str | None = None

    @property
    def restored(self) -> int:
        """Count entries opened and revalidated successfully in this round."""
        return sum(item.outcome == "restored" for item in self.items)

    @property
    def skipped(self) -> int:
        """Count rejected or explicitly prepared candidates without exposing their details."""
        return sum(item.outcome == "skipped" for item in self.items)

    @property
    def failed(self) -> int:
        """Count failed attempts while keeping summary storage bounded."""
        return sum(item.outcome == "failed" for item in self.items)


async def _validate(service, candidate, target):
    """Pass only nonplaintext authentication facts to the backend-independent Service validator."""
    return await service.validate_active_entry(
        candidate, transaction_id=target.transaction_id, created_at=target.created_at,
        credential_digest=target.credential_digest, auth_version=target.auth_version,
        workspace_receipt=target.workspace_receipt,
    )


def _failure(error):
    """Translate only known classes and fixed codes; never copy exception messages into summaries."""
    if isinstance(error, (KubernetesConfigurationError, KubernetesConnectionFailed, KubernetesPermissionDenied, KubernetesTimeout)):
        return "failed", "kubernetes_unavailable"
    if isinstance(error, SandboxNotFound):
        return "skipped", "sandbox_not_found"
    if isinstance(error, SandboxMetadataConflict):
        return "skipped", "sandbox_identity_mismatch"
    if isinstance(error, SandboxQueryFailed):
        return "skipped", "sandbox_not_ready_or_conflict"
    if isinstance(error, LocalEntryError):
        if error.code == "local_entry_cleanup_failed":
            raise error
        return "skipped", {
            "local_entry_capacity_exceeded": "session_capacity_exceeded",
            "local_entry_target_changed": "target_changed",
            "local_entry_identity_conflict": "target_changed",
        }.get(error.code, "local_entry_unavailable")
    return "failed", "entry_restore_failed"


async def _restore_one(service, manager, candidate):
    """Never open before credential checks; close only this attempt's exact original reservation on failure."""
    owner, target = object(), None
    try:
        reason = await service.check_active_entry_record(candidate)
        if reason is not None:
            return EntryRestoreItem(candidate.sandbox_id, "skipped", reason)
        target = await manager._resolve(candidate.sandbox_id)
        reason = await _validate(service, candidate, target)
        if reason is not None:
            return EntryRestoreItem(candidate.sandbox_id, "skipped", reason)
        await manager.open(candidate.sandbox_id, expected_target=target, reservation_owner=owner)
        reason = await _validate(service, candidate, target)
        if reason is None:
            # Recheck actual original UIDs after database and credential observations.
            connection = await manager.lookup(candidate.sandbox_id)
            if connection.target != target:
                reason = "target_changed"
            else:
                reason = await service.check_active_entry_record(candidate)
        if reason is not None:
            await manager.close(candidate.sandbox_id, expected_transaction_id=candidate.transaction_id,
                                expected_target=target, reservation_owner=owner)
            return EntryRestoreItem(candidate.sandbox_id, "skipped", reason)
        return EntryRestoreItem(candidate.sandbox_id, "restored", "restored")
    except BaseException as error:
        if target is not None:
            await manager.close(candidate.sandbox_id, expected_transaction_id=candidate.transaction_id,
                                expected_target=target, reservation_owner=owner)
        if isinstance(error, (asyncio.CancelledError, SandboxMetadataUnavailable)):
            raise
        outcome, reason = _failure(error)
        return EntryRestoreItem(candidate.sandbox_id, outcome, reason)


async def restore_active_entries(service, manager, *, explicit_ids=(), limit=100, budget_seconds=30.0):
    """Stop admitting at the monotonic budget, but settle begun work before relinquishing application ownership."""
    deadline = monotonic() + budget_seconds
    try:
        candidates = await service.active_entry_candidates(limit=limit)
    except SandboxMetadataUnavailable:
        return EntryRestoreResult(error="sandbox_metadata_unavailable")
    items = []
    for candidate in candidates:
        if monotonic() >= deadline:
            return EntryRestoreResult(tuple(items), len(candidates), stopped="budget_exhausted")
        if candidate.sandbox_id in explicit_ids:
            items.append(EntryRestoreItem(candidate.sandbox_id, "skipped", "explicit_entry"))
            continue
        worker = asyncio.create_task(_restore_one(service, manager, candidate))
        try:
            item = await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            # The manager keeps late processes owned; lifespan finally closes them
            # before RunLock release. Settle the current attempt, never start another.
            await settle_task(worker)
            raise cancellation
        except SandboxMetadataUnavailable:
            items.append(EntryRestoreItem(candidate.sandbox_id, "failed", "sandbox_metadata_unavailable"))
            return EntryRestoreResult(tuple(items), len(candidates), error="sandbox_metadata_unavailable")
        items.append(item)
        if item.reason == "kubernetes_unavailable":
            return EntryRestoreResult(tuple(items), len(candidates), error="kubernetes_unavailable")
        if item.reason == "session_capacity_exceeded":
            return EntryRestoreResult(tuple(items), len(candidates), stopped="session_capacity_exceeded")
    return EntryRestoreResult(tuple(items), len(candidates))
