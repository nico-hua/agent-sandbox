"""Bounded expiration reaping, pending inspection and original-transaction recovery."""

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import datetime
from typing import Protocol

from control_plane.common.async_tasks import (
    settle_task,
    wait_for_completion,
    wait_for_task,
)
from control_plane.config.defaults import (
    DEFAULT_REAPER_BATCH_LIMIT,
    DEFAULT_REAPER_INTERVAL_SECONDS,
)
from control_plane.config.settings import validate_reaper_config
from control_plane.domain.errors import (
    DockerUnavailable,
    SandboxEndpointAuthRequired,
    SandboxEndpointAuthUnavailable,
    SandboxEndpointUnavailable,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    SandboxNotFound,
    SandboxQueryFailed,
)
from control_plane.domain.maintenance import (
    SandboxPendingInspectionResult,
    SandboxPendingRecoveryItem,
    SandboxPendingRecoveryResult,
    SandboxReapItem,
    SandboxReapResult,
    SandboxResourceInspection,
)
from control_plane.domain.sandbox import SandboxLifecycleRecord
from control_plane.runtime.base_runtime import SandboxRuntime
from control_plane.services.endpoint_access import verified_endpoint_credential
from control_plane.services.sandbox_queries import identities_match
from control_plane.services.workspace_service import WorkspaceService
from control_plane.storage.base import (
    LifecycleRecordNotFound,
    LifecycleStore,
    LifecycleStoreConflict,
)

# Keep the existing logger name for operator filters after the module move.
_LOGGER = logging.getLogger("control_plane.services.maintenance")
_REAP_REASONS = frozenset({
    "candidate_changed",
    "deleted",
    "sandbox_metadata_cleanup_failed",
    "sandbox_metadata_conflict",
    "sandbox_metadata_unavailable",
    "sandbox_delete_conflict",
    "docker_unavailable",
    "sandbox_delete_failed",
})


def _stable_reason(reason: str | None) -> str:
    """Keep logs within the Service's stable codes even if an adapter misbehaves."""
    if reason is None:
        return "none"
    return reason if isinstance(reason, str) and reason in _REAP_REASONS else "unknown_reap_error"


def _log_result(result: SandboxReapResult) -> None:
    """Record round counts and actionable instance failures without backend details."""
    level = logging.WARNING if result.error is not None or result.failed else logging.INFO
    _LOGGER.log(
        level,
        "sandbox_reaper_round processed=%s succeeded=%s skipped=%s failed=%s error=%s",
        len(result.items), result.succeeded, result.skipped, result.failed,
        _stable_reason(result.error),
    )
    for item in result.items:
        if item.outcome == "failed":
            sandbox_id = (
                item.sandbox_id
                if isinstance(item.sandbox_id, str)
                and re.fullmatch(r"sbx_[0-9a-f]{32}", item.sandbox_id) is not None
                else "invalid_sandbox_id"
            )
            _LOGGER.warning(
                "sandbox_reaper_item sandbox_id=%s reason=%s",
                sandbox_id, _stable_reason(item.reason),
            )


class ExpirationReaper(Protocol):
    """Describe the single bounded operation required by the background loop."""

    async def reap_expired_once(self, limit: int = 100) -> SandboxReapResult:
        """Reap due instances or retry already claimed deletions."""
        ...


async def run_reaper(
    service: ExpirationReaper,
    *,
    interval_seconds: float = DEFAULT_REAPER_INTERVAL_SECONDS,
    batch_limit: int = DEFAULT_REAPER_BATCH_LIMIT,
    wait: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Run immediately, then delay after every serial round until cancellation."""
    validate_reaper_config(True, interval_seconds, batch_limit)
    while True:
        try:
            result = await service.reap_expired_once(limit=batch_limit)
            _log_result(result)
        except Exception:  # noqa: BLE001
            # Service results already carry domain codes. Unexpected exceptions
            # may contain SQL, paths or backend responses and must stay private.
            _LOGGER.warning(
                "sandbox_reaper_round processed=0 succeeded=0 skipped=0 failed=0 "
                "error=sandbox_reaper_round_failed",
            )
        # A full batch is still followed by a delay: retry pressure must remain
        # bounded even when all candidates repeatedly fail or remain eligible.
        await wait(interval_seconds)


async def stop_reaper(task: asyncio.Task[None]) -> None:
    """Cancel once, settle the current Service candidate, and retrieve task failure."""
    if not task.done() and not task.cancelling():
        task.cancel()
    cancellation = await wait_for_completion(task)
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:  # noqa: BLE001
        _LOGGER.error("sandbox_reaper_stopped error=sandbox_reaper_task_failed")
    if cancellation is not None:
        raise cancellation


class SandboxMaintenance:
    """Select and process bounded snapshots with the service's existing mutation guards."""

    def __init__(
        self, runtime: SandboxRuntime, store: LifecycleStore, workspaces: WorkspaceService,
        clock: Callable[[], datetime],
        reap_candidate: Callable[[SandboxLifecycleRecord, datetime], Awaitable[SandboxReapItem]],
    ) -> None:
        """Reuse the service's clock and guarded per-instance deletion for all rounds."""
        self._runtime = runtime
        self._store = store
        self._workspaces = workspaces
        self._now = clock
        self._reap_candidate = reap_candidate

    async def reap_expired_once(self, limit: int = 100) -> SandboxReapResult:
        """Reap a bounded batch of expired active or retryable deleting records at one time snapshot."""
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        now = self._now()
        items: list[SandboxReapItem] = []
        try:
            candidates = await self._store.list_reap_candidates(now, limit=limit)
        except Exception:  # noqa: BLE001
            return SandboxReapResult(now, error="sandbox_metadata_unavailable")
        for candidate in candidates:
            # Each candidate must finish from claim through cleanup. Cancellation
            # stops later candidates, but must settle the current worker's late claim or commit.
            worker = asyncio.create_task(self._reap_candidate(candidate, now))
            try:
                item = await wait_for_task(worker)
            except asyncio.CancelledError:
                await settle_task(worker)
                raise
            items.append(item)
            if item.reason == "sandbox_metadata_unavailable":
                return SandboxReapResult(now, tuple(items), error=item.reason)
        return SandboxReapResult(now, tuple(items))

    async def inspect_pending_once(self, limit: int = 100) -> SandboxPendingInspectionResult:
        """Observe one bounded pending batch without recovery, health checks or age assumptions."""
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        items: list[SandboxResourceInspection] = []
        try:
            candidates = await self._store.list_pending(limit=limit)
        except Exception:  # noqa: BLE001
            return SandboxPendingInspectionResult(error="sandbox_metadata_unavailable")
        for candidate in candidates:
            worker = asyncio.create_task(self._inspect_pending_candidate(candidate))
            try:
                item = await wait_for_task(worker)
            except asyncio.CancelledError:
                # Read-only SDK/SQLite workers still own connections; settle this
                # observation before stopping, rather than abandoning their results.
                await settle_task(worker)
                raise
            except SandboxMetadataUnavailable:
                return SandboxPendingInspectionResult(
                    tuple(items), error="sandbox_metadata_unavailable",
                )
            items.append(item)
        return SandboxPendingInspectionResult(tuple(items))

    async def _inspect_pending_candidate(
        self, candidate: SandboxLifecycleRecord,
    ) -> SandboxResourceInspection:
        """Inspect a saved identity and discard its conclusion if metadata changed meanwhile."""
        sandbox_id = candidate.sandbox_id
        try:
            workspace = await self._workspaces.binding_for(candidate)
            observed = await self._runtime.inspect_resources(
                sandbox_id, expected_transaction_id=candidate.transaction_id,
                **({"expected_workspace": workspace} if workspace is not None else {}),
            )
        except SandboxMetadataUnavailable:
            raise
        except SandboxMetadataConflict:
            observed = SandboxResourceInspection(sandbox_id, "conflict", "workspace_binding_conflict")
        except DockerUnavailable:
            observed = SandboxResourceInspection(sandbox_id, "unavailable", "docker_unavailable")
        except Exception:  # noqa: BLE001
            observed = SandboxResourceInspection(
                sandbox_id, "unavailable", "runtime_inspection_failed",
            )
        try:
            current = await self._store.get(sandbox_id)
        except Exception as error:
            raise SandboxMetadataUnavailable(sandbox_id) from error
        # Neither this reread nor Docker discovery is a cross-system snapshot.
        # Compare the entire immutable record, including original identity/version.
        if current != candidate or current.management_state != "pending":
            return replace(observed, classification="changed", reason="pending_record_changed")
        return observed

    async def recover_pending_once(self, limit: int = 100) -> SandboxPendingRecoveryResult:
        """Confirm one bounded pending batch without repairing or compensating runtime resources.

        Startup callers must hold exclusive application ownership before admitting requests.
        Explicit callers must likewise exclude live creation; record age proves no such exclusion.
        """
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        try:
            selection = asyncio.create_task(self._store.list_pending(limit=limit))
            candidates = await settle_task(selection, propagate_cancellation=True)
        except Exception:  # noqa: BLE001
            return SandboxPendingRecoveryResult(error="sandbox_metadata_unavailable")
        items: list[SandboxPendingRecoveryItem] = []
        for candidate in candidates:
            worker = asyncio.create_task(self._recover_pending_candidate(candidate))
            try:
                item, error = await wait_for_task(worker)
            except asyncio.CancelledError:
                # A late activation may commit. Finish only the current candidate,
                # consume its result and leave its actual state intact before cancelling.
                await settle_task(worker)
                raise
            items.append(item)
            if error is not None:
                return SandboxPendingRecoveryResult(tuple(items), error)
        return SandboxPendingRecoveryResult(tuple(items))

    async def _recover_pending_candidate(
        self, candidate: SandboxLifecycleRecord,
    ) -> tuple[SandboxPendingRecoveryItem, str | None]:
        """Reread the original snapshot and activate its exact unexpired pending version."""
        sandbox_id = candidate.sandbox_id
        observed = await self._check_pending_recovery(candidate)
        backend_error = observed.reason if observed is not None and observed.reason in {
            "docker_unavailable", "sandbox_metadata_unavailable",
        } else None
        try:
            current = await self._store.get(sandbox_id)
        except Exception:  # noqa: BLE001
            return SandboxPendingRecoveryItem(
                sandbox_id, "failed", "sandbox_metadata_unavailable",
            ), "sandbox_metadata_unavailable"
        if current != candidate or current.management_state != "pending":
            return SandboxPendingRecoveryItem(
                sandbox_id, "skipped", "pending_record_changed",
            ), backend_error
        if observed is not None:
            return observed, backend_error
        # Sample once per eligible candidate after all runtime reads and the
        # metadata reread; this is a commit-time check, not a cross-system snapshot.
        if candidate.expires_at <= self._now():
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_expired"), None
        try:
            # Recovery uses its own guarded activation. New-creation
            # compensation may delete resources, and a failed acknowledgement is uncertain.
            await self._store.activate(
                sandbox_id, candidate.transaction_id, expected_version=candidate.version,
            )
        except (LifecycleStoreConflict, LifecycleRecordNotFound):
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "pending_record_changed"), None
        except Exception:  # noqa: BLE001
            return SandboxPendingRecoveryItem(
                sandbox_id, "failed", "sandbox_metadata_unavailable",
            ), "sandbox_metadata_unavailable"
        return SandboxPendingRecoveryItem(sandbox_id, "recovered", "recovered"), None

    async def _check_pending_recovery(
        self, candidate: SandboxLifecycleRecord,
    ) -> SandboxPendingRecoveryItem | None:
        """Require complete owned resources, original runtime identity, readiness and verified entry."""
        sandbox_id = candidate.sandbox_id
        try:
            workspace = await self._workspaces.binding_for(candidate)
            inspection = await self._runtime.inspect_resources(
                sandbox_id, expected_transaction_id=candidate.transaction_id,
                **({"expected_workspace": workspace} if workspace is not None else {}),
            )
            if inspection.sandbox_id != sandbox_id:
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_identity_mismatch")
            if inspection.classification != "all_present":
                outcome, reason = {
                    "partial": ("skipped", "resources_partial"),
                    "not_found": ("skipped", "resources_not_found"),
                    "conflict": ("skipped", "resource_ownership_conflict"),
                    "unavailable": ("failed", "docker_unavailable"),
                    "changed": ("skipped", "pending_record_changed"),
                }.get(inspection.classification, ("failed", "sandbox_recovery_failed"))
                return SandboxPendingRecoveryItem(sandbox_id, outcome, reason)
            observed = await self._runtime.get(sandbox_id)
            if not identities_match(sandbox_id, candidate, observed):
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_identity_mismatch")
            if observed.status != "ready":
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_not_ready")
            await self._workspaces.verify_observed(candidate, observed)
            endpoint = await self._runtime.get_endpoint(sandbox_id)
            if endpoint.sandbox_id != sandbox_id:
                return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_identity_mismatch")
            try:
                credential = await self._store.get_credential(sandbox_id, candidate.transaction_id)
            except Exception as error:
                raise SandboxMetadataUnavailable(sandbox_id) from error
            # Old pending resources are not upgraded. A new credential or a
            # protected marker requires both sides to agree before activation.
            if endpoint.auth_required or credential is not None:
                await verified_endpoint_credential(self._store, candidate, endpoint)
        except DockerUnavailable:
            return SandboxPendingRecoveryItem(sandbox_id, "failed", "docker_unavailable")
        except SandboxMetadataUnavailable:
            return SandboxPendingRecoveryItem(sandbox_id, "failed", "sandbox_metadata_unavailable")
        except SandboxMetadataConflict:
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "workspace_binding_conflict")
        except SandboxNotFound:
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_not_found")
        except SandboxQueryFailed:
            return SandboxPendingRecoveryItem(sandbox_id, "failed", "sandbox_query_failed")
        except SandboxEndpointUnavailable:
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_endpoint_unavailable")
        except (SandboxEndpointAuthRequired, SandboxEndpointAuthUnavailable):
            return SandboxPendingRecoveryItem(sandbox_id, "skipped", "sandbox_endpoint_auth_unavailable")
        except Exception:  # noqa: BLE001
            return SandboxPendingRecoveryItem(sandbox_id, "failed", "sandbox_recovery_failed")
        return None
