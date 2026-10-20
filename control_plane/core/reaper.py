"""Serial background scheduling for the Service's bounded expiration reaper."""

import asyncio
import logging
import math
import re
from collections.abc import Awaitable, Callable

from control_plane.core.models import SandboxReapResult
from control_plane.core.service import SandboxService
from control_plane.core.tasks import wait_for_completion

DEFAULT_REAPER_INTERVAL_SECONDS = 60.0
DEFAULT_REAPER_BATCH_LIMIT = 100
_LOGGER = logging.getLogger(__name__)
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


def validate_reaper_config(
    enabled: bool, interval_seconds: float, batch_limit: int,
) -> None:
    """Reject coercions and unsupported delay or SQLite batch-size values eagerly."""
    if type(enabled) is not bool:
        raise ValueError("reaper enabled must be a boolean")
    if type(interval_seconds) not in (int, float):
        raise ValueError("reaper interval must be a positive finite number")
    try:
        finite_interval = math.isfinite(interval_seconds)
    except OverflowError:
        finite_interval = False
    if not finite_interval or interval_seconds <= 0:
        raise ValueError("reaper interval must be a positive finite number")
    if type(batch_limit) is not int or not 1 <= batch_limit <= 2**63 - 1:
        raise ValueError("reaper batch limit must be a positive SQLite integer")


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


async def run_reaper(
    service: SandboxService,
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
