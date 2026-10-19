"""Deterministic scheduling, logging and cancellation coverage for the TTL loop."""

import asyncio
from datetime import UTC, datetime
import logging

import pytest

from control_plane.core.models import SandboxReapItem, SandboxReapResult
from control_plane.core.reaper import run_reaper, stop_reaper, validate_reaper_config


NOW = datetime(2026, 10, 19, 8, tzinfo=UTC)
SANDBOX_ID = "sbx_" + "a" * 32


class ControlledWait:
    """Expose each requested interval without passing real wall-clock time."""

    def __init__(self) -> None:
        """Record interval requests and release them through an event queue."""
        self.entered: asyncio.Queue[float] = asyncio.Queue()
        self.release: asyncio.Queue[None] = asyncio.Queue()

    async def __call__(self, seconds: float) -> None:
        """Wait until the test explicitly advances this interval."""
        self.entered.put_nowait(seconds)
        await self.release.get()


class SequenceService:
    """Return complete Service results or injected failures in a known order."""

    def __init__(self, *results: SandboxReapResult | BaseException) -> None:
        """Retain round outcomes and record the scheduler's requested limits."""
        self.results = list(results)
        self.limits: list[int] = []

    async def reap_expired_once(self, limit: int = 100) -> SandboxReapResult:
        """Record a bounded round and return or raise its next controlled outcome."""
        self.limits.append(limit)
        result = self.results.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


@pytest.mark.anyio
async def test_first_round_runs_immediately_and_next_round_waits_for_interval() -> None:
    """The scheduler must start immediately and apply its configured delay and limit."""
    service = SequenceService(SandboxReapResult(NOW), SandboxReapResult(NOW))
    wait = ControlledWait()
    task = asyncio.create_task(run_reaper(service, interval_seconds=7.5, batch_limit=3, wait=wait))
    try:
        assert await wait.entered.get() == 7.5
        assert service.limits == [3]
        wait.release.put_nowait(None)
        assert await wait.entered.get() == 7.5
        assert service.limits == [3, 3]
    finally:
        await stop_reaper(task)
    assert task.done()


@pytest.mark.anyio
async def test_slow_round_does_not_overlap_or_start_its_interval_early() -> None:
    """An unfinished round must prevent both another round and its inter-round wait."""
    entered = asyncio.Event()
    release = asyncio.Event()
    limits: list[int] = []

    class SlowService:
        """Hold one round open while the scheduler is observed."""

        async def reap_expired_once(self, limit: int = 100) -> SandboxReapResult:
            """Pause before returning a complete empty round."""
            limits.append(limit)
            entered.set()
            await release.wait()
            return SandboxReapResult(NOW)

    wait = ControlledWait()
    task = asyncio.create_task(run_reaper(SlowService(), wait=wait))
    try:
        await entered.wait()
        assert limits == [100]
        assert wait.entered.empty()
        release.set()
        assert await wait.entered.get() == 60.0
        assert limits == [100]
    finally:
        await stop_reaper(task)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "first",
    [
        SandboxReapResult(NOW),
        SandboxReapResult(NOW, (SandboxReapItem(SANDBOX_ID, "succeeded", "deleted"),)),
        SandboxReapResult(NOW, error="sandbox_metadata_unavailable"),
        SandboxReapResult(NOW, (SandboxReapItem(SANDBOX_ID, "failed", "sandbox_delete_failed"),)),
        RuntimeError("private database path and SQL must stay out of logs"),
    ],
    ids=["empty", "full", "store-error", "item-failure", "exception"],
)
async def test_every_round_waits_and_errors_allow_later_rounds(
    first: SandboxReapResult | BaseException,
) -> None:
    """Empty, full and failed rounds must all wait instead of spinning or exiting."""
    service = SequenceService(first, SandboxReapResult(NOW))
    wait = ControlledWait()
    task = asyncio.create_task(run_reaper(service, batch_limit=1, wait=wait))
    try:
        assert await wait.entered.get() == 60.0
        assert service.limits == [1]
        wait.release.put_nowait(None)
        assert await wait.entered.get() == 60.0
        assert service.limits == [1, 1]
    finally:
        await stop_reaper(task)


@pytest.mark.anyio
async def test_logs_include_counts_and_stable_result_failures_without_raw_errors(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Returned errors and item failures need visible diagnostics without backend details."""
    caplog.set_level(logging.INFO, logger="control_plane.core.reaper")
    service = SequenceService(
        SandboxReapResult(NOW, (
            SandboxReapItem(SANDBOX_ID, "succeeded", "deleted"),
            SandboxReapItem(SANDBOX_ID, "skipped", "candidate_changed"),
            SandboxReapItem(SANDBOX_ID, "failed", "sandbox_delete_failed"),
        ), error="sandbox_metadata_unavailable"),
        RuntimeError("secret SQL /private/database.sqlite3"),
        SandboxReapResult(NOW, error="secret SQL /private/database.sqlite3"),
    )
    wait = ControlledWait()
    task = asyncio.create_task(run_reaper(service, wait=wait))
    try:
        await wait.entered.get()
        assert "processed=3" in caplog.text
        assert "succeeded=1 skipped=1 failed=1" in caplog.text
        assert "sandbox_metadata_unavailable" in caplog.text
        assert "sandbox_delete_failed" in caplog.text
        assert SANDBOX_ID in caplog.text
        wait.release.put_nowait(None)
        await wait.entered.get()
        assert "sandbox_reaper_round_failed" in caplog.text
        wait.release.put_nowait(None)
        await wait.entered.get()
        assert "unknown_reap_error" in caplog.text
        assert "secret" not in caplog.text
        assert "/private" not in caplog.text
        assert all(record.exc_info is None for record in caplog.records)
    finally:
        await stop_reaper(task)


@pytest.mark.anyio
async def test_cancelled_round_exits_without_retrying_or_waiting() -> None:
    """Cancellation must escape the loop rather than be logged and retried."""
    service = SequenceService(asyncio.CancelledError())
    wait = ControlledWait()
    task = asyncio.create_task(run_reaper(service, wait=wait))
    with pytest.raises(asyncio.CancelledError):
        await task
    assert service.limits == [100]
    assert wait.entered.empty()


@pytest.mark.anyio
async def test_shutdown_cancels_interval_and_leaves_no_running_task() -> None:
    """Waiting for the next interval must be interruptible by normal shutdown."""
    service = SequenceService(SandboxReapResult(NOW))
    wait = ControlledWait()
    task = asyncio.create_task(run_reaper(service, wait=wait))
    await wait.entered.get()
    await stop_reaper(task)
    assert task.done()
    assert task.cancelled()
    assert service.limits == [100]


@pytest.mark.anyio
async def test_repeated_shutdown_cancellation_waits_for_current_round_cleanup() -> None:
    """Repeated cancellation of shutdown must not interrupt the Service's first cleanup."""
    started = asyncio.Event()
    finishing = asyncio.Event()
    finish = asyncio.Event()
    interruptions = 0

    class FinishingService:
        """Model Service cancellation that finishes its already-started candidate."""

        async def reap_expired_once(self, limit: int = 100) -> SandboxReapResult:
            """Wait for cancellation, then perform an observable protected finish."""
            nonlocal interruptions
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                interruptions += 1
                finishing.set()
                try:
                    await finish.wait()
                except asyncio.CancelledError:
                    interruptions += 1
                    raise
                raise

    task = asyncio.create_task(run_reaper(FinishingService()))
    await started.wait()
    stopping = asyncio.create_task(stop_reaper(task))
    await finishing.wait()
    stopping.cancel()
    await asyncio.sleep(0)
    stopping.cancel()
    await asyncio.sleep(0)
    assert not stopping.done()
    assert interruptions == 1
    finish.set()
    with pytest.raises(asyncio.CancelledError):
        await stopping
    assert task.done()
    assert interruptions == 1


@pytest.mark.anyio
async def test_shutdown_retrieves_unexpected_background_exception(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """An already-failed task must have its failure read and logged without raw details."""
    caplog.set_level(logging.ERROR, logger="control_plane.core.reaper")

    async def fail() -> None:
        """Fail outside the normal Service call boundary to exercise final retrieval."""
        raise RuntimeError("secret SQL /private/database.sqlite3")

    task = asyncio.create_task(fail())
    await asyncio.wait({task})
    await stop_reaper(task)
    assert "sandbox_reaper_task_failed" in caplog.text
    assert "secret" not in caplog.text
    assert task._log_traceback is False


@pytest.mark.parametrize(
    ("enabled", "interval", "limit"),
    [
        (0, 60, 100), ("true", 60, 100), (None, 60, 100),
        (True, True, 100), (True, "60", 100), (True, None, 100),
        (True, 0, 100), (True, -1, 100), (True, float("inf"), 100),
        (True, float("-inf"), 100), (True, float("nan"), 100),
        (True, 60, True), (True, 60, 1.5), (True, 60, "100"),
        (True, 60, 0), (True, 60, -1), (True, 60, 2**63),
        (False, 0, 100), (False, 60, 0),
    ],
)
def test_invalid_configuration_is_rejected_even_when_disabled(
    enabled: object, interval: object, limit: object,
) -> None:
    """Strict validation rejects booleans, coercions and invalid finite bounds."""
    with pytest.raises(ValueError):
        validate_reaper_config(enabled, interval, limit)


@pytest.mark.parametrize(("enabled", "interval", "limit"), [(True, 60, 100), (False, 0.001, 1), (True, 1.5, 2**63 - 1)])
def test_valid_configuration_preserves_supported_bounds(
    enabled: bool, interval: float, limit: int,
) -> None:
    """The scheduler accepts either switch value and positive finite supported values."""
    validate_reaper_config(enabled, interval, limit)


@pytest.mark.anyio
async def test_run_reaper_validates_direct_configuration_before_calling_service() -> None:
    """A direct caller must not bypass scheduler validation and enter a busy loop."""
    service = SequenceService()
    with pytest.raises(ValueError):
        await run_reaper(service, interval_seconds=0)
    assert service.limits == []
