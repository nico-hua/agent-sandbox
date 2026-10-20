"""Cancellation and final-result contracts for critical background work."""

import asyncio

import pytest

from control_plane.core.tasks import settle_task, wait_for_task


@pytest.mark.anyio
async def test_wait_cancellation_leaves_worker_running() -> None:
    """Cancelling a waiter must leave its worker available for explicit settlement."""
    release = asyncio.Event()

    async def work() -> str:
        """Finish only after the caller has observed waiter cancellation."""
        await release.wait()
        return "committed"

    worker = asyncio.create_task(work())
    waiter = asyncio.create_task(wait_for_task(worker))
    try:
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert not worker.done()
        release.set()
        assert await settle_task(worker) == "committed"
    finally:
        release.set()
        await settle_task(worker)


@pytest.mark.anyio
@pytest.mark.parametrize("propagate_cancellation", [False, True])
async def test_settlement_waits_through_repeated_cancellation(
    propagate_cancellation: bool,
) -> None:
    """Repeated cancellation cannot skip a worker result or its caller's cancellation policy."""
    release = asyncio.Event()

    async def work() -> str:
        """Represent a write that cannot finish until the test releases it."""
        await release.wait()
        return "committed"

    worker = asyncio.create_task(work())
    waiter = asyncio.create_task(settle_task(
        worker, propagate_cancellation=propagate_cancellation,
    ))
    try:
        await asyncio.sleep(0)
        waiter.cancel()
        await asyncio.sleep(0)
        waiter.cancel()
        await asyncio.sleep(0)
        assert not waiter.done()
        assert not worker.done()
        release.set()
        if propagate_cancellation:
            with pytest.raises(asyncio.CancelledError):
                await waiter
        else:
            assert await waiter == "committed"
        assert worker.result() == "committed"
    finally:
        release.set()
        await settle_task(worker)


@pytest.mark.anyio
async def test_settlement_preserves_failure_without_extra_loop_report() -> None:
    """A failed worker is retrieved once even when its first waiter was cancelled."""
    release = asyncio.Event()
    failure = RuntimeError("synthetic worker failure")
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    reports: list[dict] = []
    loop.set_exception_handler(lambda _loop, context: reports.append(context))

    async def work() -> None:
        """Fail after cancellation while keeping the original exception identity."""
        await release.wait()
        raise failure

    worker = asyncio.create_task(work())
    waiter = asyncio.create_task(wait_for_task(worker))
    try:
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        release.set()
        with pytest.raises(RuntimeError) as captured:
            await settle_task(worker)
        assert captured.value is failure
        await asyncio.sleep(0)
        assert reports == []
    finally:
        release.set()
        if not worker.done():
            await asyncio.wait({worker})
        worker.exception()
        loop.set_exception_handler(previous_handler)
