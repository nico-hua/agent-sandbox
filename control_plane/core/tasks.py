"""Wait for critical tasks without forwarding cancellation or duplicating failures."""

import asyncio
from typing import TypeVar


_Result = TypeVar("_Result")


async def wait_for_task(task: asyncio.Task[_Result]) -> _Result:
    """Read a task result while allowing caller cancellation to leave the task running."""
    # Unlike shield(), wait() does not install a callback reporting a late worker
    # failure after caller cancellation. The owner explicitly retrieves that result.
    if not task.done():
        await asyncio.wait({task})
    return task.result()


async def wait_for_completion(task: asyncio.Task[_Result]) -> asyncio.CancelledError | None:
    """Wait through repeated caller cancellation; the owner must retrieve task.result()."""
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.wait({task})
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
    return cancellation


async def settle_task(
    task: asyncio.Task[_Result], *, propagate_cancellation: bool = False,
) -> _Result:
    """Retrieve the final outcome before optionally propagating caller cancellation."""
    cancellation = await wait_for_completion(task)
    result = task.result()
    if propagate_cancellation and cancellation is not None:
        raise cancellation
    return result
