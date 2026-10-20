"""Service recovery decisions use explicit resource evidence instead of error names."""

import asyncio
import threading

import pytest

from control_plane.core.errors import (
    CreationFailureInfo,
    CreationResourceState,
    SandboxCreateFailed,
    SandboxCreateUncertain,
    SandboxCleanupFailed,
    SandboxError,
)
from control_plane.core.service import SandboxService
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from docker.errors import DockerException
from lifecycle_fakes import FakeLifecycleStore
from test_sandbox_service import FakeRuntime, FIXED_SANDBOX_ID
from test_sandbox_runtime import RecordingClient


@pytest.mark.anyio
@pytest.mark.parametrize("state", list(CreationResourceState))
async def test_create_uses_resource_state_even_for_legacy_error_type(
    state: CreationResourceState,
) -> None:
    """An explicit unknown or remaining outcome overrides the legacy clean default."""
    failure = SandboxCreateFailed()
    failure.creation_failure = CreationFailureInfo("container", state)
    store = FakeLifecycleStore()
    service = SandboxService(FakeRuntime(failure), store, id_factory=lambda: FIXED_SANDBOX_ID)
    clean = state in {CreationResourceState.UNCHANGED, CreationResourceState.ROLLED_BACK}
    expected = SandboxCreateFailed if clean else SandboxCreateUncertain

    with pytest.raises(expected) as captured:
        await service.create()

    assert (FIXED_SANDBOX_ID in store.records) is not clean
    if clean:
        assert captured.value is failure
    else:
        assert captured.value.creation_failure is failure.creation_failure
        assert captured.value.__cause__ is failure


@pytest.mark.anyio
async def test_create_accepts_confirmed_rollback_from_another_failure_type() -> None:
    """A runtime can report clean recovery without joining a Service exception whitelist."""
    failure = SandboxError("synthetic runtime failure")
    failure.creation_failure = CreationFailureInfo("network", CreationResourceState.ROLLED_BACK)
    store = FakeLifecycleStore()
    service = SandboxService(FakeRuntime(failure), store, id_factory=lambda: FIXED_SANDBOX_ID)

    with pytest.raises(SandboxError) as captured:
        await service.create()

    assert captured.value is failure
    assert store.records == {}


@pytest.mark.anyio
async def test_cancelled_create_preserves_explicitly_unknown_worker_outcome() -> None:
    """Cancellation cannot turn an uncertain legacy error into confirmed rollback."""
    entered = asyncio.Event()
    failure = SandboxCreateFailed()
    failure.creation_failure = CreationFailureInfo("network", CreationResourceState.UNKNOWN)

    class Runtime(FakeRuntime):
        """Return an uncertain creation failure after receiving cancellation."""

        async def create(self, spec) -> None:
            """Expose cancellation before reporting the worker's actual outcome."""
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise failure

    store = FakeLifecycleStore()
    service = SandboxService(Runtime(), store, id_factory=lambda: FIXED_SANDBOX_ID)
    task = asyncio.create_task(service.create())
    await entered.wait()
    task.cancel()
    with pytest.raises(SandboxCreateUncertain) as captured:
        await task
    assert captured.value.creation_failure is failure.creation_failure
    assert FIXED_SANDBOX_ID in store.records


@pytest.mark.anyio
@pytest.mark.timeout(3)
async def test_cancelled_transport_failure_preserves_recovery_diagnostics() -> None:
    """Runtime cancellation wrapping must keep the transaction's unknown-outcome evidence."""
    client = RecordingClient()
    client.networks.error = DockerException("private daemon address")
    entered, release = threading.Event(), threading.Event()
    client.close_entered, client.close_release = entered, release
    store = FakeLifecycleStore()
    service = SandboxService(
        DockerSandboxRuntime(lambda: client), store, id_factory=lambda: FIXED_SANDBOX_ID,
    )
    task = asyncio.create_task(service.create())
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(SandboxCleanupFailed) as captured:
            await task
        info = captured.value.creation_failure
        assert info is not None
        assert info.phase == "create_network"
        assert info.resource_state == CreationResourceState.UNKNOWN
        assert FIXED_SANDBOX_ID in store.records
        assert "private" not in repr(info)
    finally:
        release.set()
