"""Wait for critical workers; callers retain control of compensation and cancellation priority."""

import asyncio
from typing import TypeVar


_Result = TypeVar("_Result")


async def wait_for_task(task: asyncio.Task[_Result]) -> _Result:
    """Allow caller cancellation while keeping the worker and its final result available."""
    # wait() avoids input cancellation and Python 3.14 shield's late-exception report callback.
    # A cancelled waiter must settle its worker; cancellation does not stop the operation.
    if not task.done():
        await asyncio.wait({task})
    return task.result()


async def wait_for_completion(task: asyncio.Task[_Result]) -> asyncio.CancelledError | None:
    """Wait through cancellation; return the first signal, leaving task.result() to the caller."""
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.wait({task})
        except asyncio.CancelledError as error:
            # Remember the first signal; the caller decides whether to propagate it after cleanup.
            if cancellation is None:
                cancellation = error
    return cancellation


async def settle_task(
    task: asyncio.Task[_Result], *, propagate_cancellation: bool = False,
) -> _Result:
    """Read the worker's final result before optionally propagating caller cancellation."""
    cancellation = await wait_for_completion(task)
    # Retrieve the result first: worker failure must take precedence over caller cancellation.
    result = task.result()
    if propagate_cancellation and cancellation is not None:
        raise cancellation
    return result
