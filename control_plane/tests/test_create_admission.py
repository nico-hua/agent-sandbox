"""Nonblocking creation admission across persistence, runtime and cancellation cleanup."""

import asyncio
from itertools import count
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core import errors
from control_plane.core.models import SandboxCreateOptions
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import LifecycleStoreConflict, LifecycleStoreError
from lifecycle_fakes import FakeLifecycleStore
from test_sandbox_service import CREATED_AT, FakeRuntime
from test_service_lifecycle import ControlledRuntime, FaultStore


class GatedRuntime(FakeRuntime):
    """Hold admitted creates at a deterministic boundary and measure their overlap."""

    def __init__(self) -> None:
        """Initialize per-instance entry notifications and a shared release gate."""
        super().__init__()
        self.entered = asyncio.Queue()
        self.release = asyncio.Event()
        self.running = 0
        self.peak = 0

    async def create(self, spec) -> None:
        """Keep each create pending until released, recording actual runtime overlap."""
        self.running += 1
        self.peak = max(self.peak, self.running)
        self.entered.put_nowait(spec)
        try:
            await self.release.wait()
            await super().create(spec)
        finally:
            self.running -= 1


class AdmissionHarness:
    """Observe factories and metadata side effects without Docker or shared test state."""

    def __init__(self, **configuration) -> None:
        """Assemble a real Service with isolated, counted dependencies and optional limits."""
        self.runtime = GatedRuntime()
        self.store = FakeLifecycleStore()
        self.store.create = AsyncMock(wraps=self.store.create)
        sequence = count(1)
        self.identity = Mock(side_effect=lambda: f"sbx_{next(sequence):032x}")
        self.transaction = Mock(side_effect=lambda: f"txn_{next(sequence):032x}")
        self.clock = Mock(return_value=CREATED_AT)
        self.service = SandboxService(
            self.runtime, self.store, self.identity, self.transaction, self.clock,
            **configuration,
        )

    async def start(self, total: int) -> list[asyncio.Task]:
        """Wait for every requested create to enter Runtime, bounding test failures."""
        tasks = [asyncio.create_task(self.service.create()) for _ in range(total)]
        try:
            for _ in tasks:
                await asyncio.wait_for(self.runtime.entered.get(), 2)
        except BaseException:
            self.runtime.release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        return tasks


@pytest.mark.parametrize("value", [1, 2, 16])
def test_valid_limits_are_accepted_by_service_and_application(value: int) -> None:
    """Server configuration accepts the default and inclusive integer boundaries lazily."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    SandboxService(runtime, store, max_concurrent_creates=value)
    create_app(runtime, store=store, reaper_enabled=False, max_concurrent_creates=value)
    assert store.initialize_calls == 0
    assert runtime.specs == []


@pytest.mark.parametrize("value", [
    0, -1, 17, True, False, 1.0, 2.5, "2", None, [], 10**100,
    float("nan"), float("inf"), float("-inf"),
])
@pytest.mark.parametrize("entrypoint", ["service", "app"])
def test_invalid_server_configuration_is_rejected(value, entrypoint: str) -> None:
    """Reject coercion and out-of-range limits at assembly, before any backend activity."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    with pytest.raises(ValueError, match="max_concurrent_creates"):
        if entrypoint == "service":
            SandboxService(runtime, store, max_concurrent_creates=value)
        else:
            create_app(runtime, store=store, max_concurrent_creates=value)
    assert store.initialize_calls == 0 and not store.records
    assert not runtime.specs


@pytest.mark.anyio
@pytest.mark.parametrize("limit", [None, 1, 16])
async def test_capacity_rejects_immediately_before_identity_clock_or_persistence(limit) -> None:
    """The default admits two; the next create has no effects and cannot enter a queue."""
    harness = AdmissionHarness(**({} if limit is None else {"max_concurrent_creates": limit}))
    expected = 2 if limit is None else limit
    tasks = await harness.start(expected)
    try:
        before = (harness.identity.call_count, harness.transaction.call_count,
                  harness.clock.call_count, harness.store.create.call_count)
        with pytest.raises(errors.SandboxCreateCapacityExceeded) as failure:
            await asyncio.wait_for(harness.service.create(), 1)
        assert not hasattr(failure.value, "sandbox_id")
        assert before == (expected,) * 4
        assert before == (harness.identity.call_count, harness.transaction.call_count,
                          harness.clock.call_count, harness.store.create.call_count)
        assert harness.runtime.peak == expected
        assert harness.runtime.entered.empty()
        assert len(harness.store.records) == expected
    finally:
        harness.runtime.release.set()
        await asyncio.gather(*tasks)
    # Completed instances retain metadata, but consume no creation slots.
    await harness.service.create()
    assert len(harness.store.records) == expected + 1


@pytest.mark.anyio
async def test_invalid_creation_options_take_precedence_even_when_full() -> None:
    """Direct callers still receive specification errors without admission or factory calls."""
    harness = AdmissionHarness(max_concurrent_creates=1)
    tasks = await harness.start(1)
    forged = SandboxCreateOptions()
    object.__setattr__(forged, "ttl_seconds", True)
    try:
        for invalid in ({}, forged):
            with pytest.raises(ValueError):
                await harness.service.create(invalid)
        assert harness.identity.call_count == harness.clock.call_count == 1
        assert harness.store.create.call_count == 1
    finally:
        harness.runtime.release.set()
        await asyncio.gather(*tasks)


@pytest.mark.anyio
@pytest.mark.parametrize("limit", [None, 1, 16])
async def test_application_uses_one_service_limit_and_returns_fixed_429(limit) -> None:
    """HTTP and direct callers share admission while client-supplied limits stay invalid."""
    runtime, store = GatedRuntime(), FakeLifecycleStore()
    app = create_app(runtime, store=store, reaper_enabled=False,
                     **({} if limit is None else {"max_concurrent_creates": limit}))
    expected = 2 if limit is None else limit
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        first = [asyncio.create_task(client.post("/v1/sandboxes")) for _ in range(expected)]
        try:
            for _ in first:
                await asyncio.wait_for(runtime.entered.get(), 2)
            rejected = await asyncio.wait_for(client.post("/v1/sandboxes"), 1)
            assert rejected.status_code == 429
            assert rejected.json() == {"error": {
                "code": "sandbox_create_capacity_exceeded",
                "message": "sandbox creation capacity exceeded",
            }}
            with pytest.raises(errors.SandboxCreateCapacityExceeded):
                await app.state.service.create()
            invalid = await client.post("/v1/sandboxes", json={"max_concurrent_creates": 16})
            assert invalid.status_code == 400
            assert invalid.json()["error"]["code"] == "invalid_request"
            assert len(store.records) == expected
        finally:
            runtime.release.set()
            completed = await asyncio.gather(*first)
        assert all(response.status_code == 201 for response in completed)


@pytest.mark.anyio
@pytest.mark.parametrize("outcome", [
    "success", "prewrite", "prewrite_conflict", "prewrite_late_commit",
    "runtime_rolled_back", "runtime_unchanged", "runtime_incomplete", "runtime_unknown",
    "activation", "activation_late_commit", "activation_conflict", "metadata_cleanup",
    "clock", "identity", "transaction",
])
async def test_finished_creation_releases_one_slot_for_every_outcome(outcome: str) -> None:
    """Failures and unknown results leave their diagnostic rows without retaining admission."""
    harness = AdmissionHarness(max_concurrent_creates=1)
    harness.runtime.release.set()
    expected_error = None
    original_write = harness.store.create
    original_activate = harness.store.activate
    original_delete = harness.store.delete
    if outcome == "prewrite":
        harness.store.create = AsyncMock(side_effect=LifecycleStoreError())
        expected_error = errors.SandboxMetadataUnavailable
    elif outcome == "prewrite_conflict":
        harness.store.create = AsyncMock(side_effect=LifecycleStoreConflict("already_exists"))
        expected_error = errors.SandboxMetadataConflict
    elif outcome == "prewrite_late_commit":
        async def write_then_fail(record):
            """Lose the acknowledgement after the pending row has been committed."""
            await original_write(record)
            raise LifecycleStoreError()
        harness.store.create = write_then_fail
        expected_error = errors.SandboxMetadataUnavailable
    elif outcome.startswith("runtime_") or outcome == "metadata_cleanup":
        failures = {
            "runtime_rolled_back": errors.SandboxCreateFailed(),
            "runtime_unchanged": errors.SandboxImageUnavailable(),
            "runtime_incomplete": errors.SandboxCleanupFailed("sbx_" + "1" * 32),
            "runtime_unknown": RuntimeError("unconfirmed resources"),
            "metadata_cleanup": errors.SandboxCreateFailed(),
        }
        harness.runtime.error = failures[outcome]
        expected_error = type(harness.runtime.error)
        if outcome == "runtime_unknown":
            expected_error = errors.SandboxCreateUncertain
        elif outcome == "metadata_cleanup":
            harness.store.delete = AsyncMock(side_effect=LifecycleStoreError())
            expected_error = errors.SandboxMetadataCleanupFailed
    elif outcome == "activation":
        harness.store.activate = AsyncMock(side_effect=LifecycleStoreError())
        expected_error = errors.SandboxMetadataUnavailable
    elif outcome == "activation_late_commit":
        async def activate_then_fail(*args, **kwargs):
            """Commit active and fail without inventing a rollback of the committed row."""
            await original_activate(*args, **kwargs)
            raise LifecycleStoreError()
        harness.store.activate = activate_then_fail
        expected_error = errors.SandboxMetadataUnavailable
    elif outcome == "activation_conflict":
        harness.store.activate = AsyncMock(side_effect=LifecycleStoreConflict("version_conflict"))
        expected_error = errors.SandboxMetadataConflict
    elif outcome in ("clock", "identity", "transaction"):
        getattr(harness, outcome).side_effect = ValueError("factory unavailable")
        expected_error = ValueError

    if expected_error is None:
        await harness.service.create()
    else:
        with pytest.raises(expected_error):
            await harness.service.create()

    if outcome in ("prewrite", "prewrite_conflict", "clock", "identity", "transaction"):
        assert not harness.store.records and not harness.runtime.specs
    elif outcome in ("runtime_rolled_back", "runtime_unchanged"):
        assert not harness.store.records
    else:
        record, = harness.store.records.values()
        assert record.management_state == (
            "active" if outcome in ("success", "activation_late_commit") else "pending"
        )
    # Keep uncertain rows, but reset the injected fault and prove capacity was
    # released exactly once: one new blocked operation fits and the next does not.
    harness.store.create = original_write
    harness.store.activate = original_activate
    harness.store.delete = original_delete
    harness.runtime.error = None
    harness.identity.side_effect = lambda: f"sbx_{harness.identity.call_count + 100:032x}"
    harness.transaction.side_effect = lambda: f"txn_{harness.transaction.call_count + 100:032x}"
    harness.clock.side_effect = None
    harness.runtime.release.clear()
    while not harness.runtime.entered.empty():
        harness.runtime.entered.get_nowait()
    tasks = await harness.start(1)
    try:
        with pytest.raises(errors.SandboxCreateCapacityExceeded):
            await harness.service.create()
    finally:
        harness.runtime.release.set()
        await asyncio.gather(*tasks)


@pytest.fixture
async def durable_store(tmp_path: Path) -> FaultStore:
    """Use real temporary SQLite workers for cancellation and late-commit boundaries."""
    store = FaultStore(tmp_path / "admission.sqlite3")
    await store.initialize()
    return store


def _durable_service(runtime: ControlledRuntime, store: FaultStore) -> SandboxService:
    """Allocate fresh identities for repeated admitted operations on one temporary database."""
    identities = count(1)
    return SandboxService(
        runtime, store, id_factory=lambda: f"sbx_{next(identities):032x}",
        clock=lambda: CREATED_AT, max_concurrent_creates=1,
    )


def _release_gates(store: FaultStore, runtime: ControlledRuntime) -> None:
    """Unblock all owned workers even when a cancellation test assertion fails."""
    store.write_release.set()
    store.activation_release.set()
    store.delete_release.set()
    runtime.create_release.set()
    runtime.rollback_release.set()
    runtime.delete_release.set()


async def _prove_one_slot_is_reusable(service: SandboxService, runtime: ControlledRuntime) -> None:
    """Admit exactly one new operation after failure and detect both leaks and over-release."""
    runtime.block_create = True
    runtime.create_entered.clear()
    runtime.create_release.clear()
    replacement = asyncio.create_task(service.create())
    try:
        await asyncio.wait_for(runtime.create_entered.wait(), 2)
        with pytest.raises(errors.SandboxCreateCapacityExceeded):
            await service.create()
    finally:
        runtime.create_release.set()
        await replacement


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["prewrite", "runtime", "activation"])
@pytest.mark.parametrize("cancellations", [1, 2])
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_cancellation_keeps_capacity_until_critical_cleanup_finishes(
    durable_store: FaultStore, stage: str, cancellations: int, cleanup_fails: bool,
) -> None:
    """Cancel real worker waits without freeing capacity during commits, rollback or deletion."""
    store = durable_store
    runtime = ControlledRuntime(store)
    service = _durable_service(runtime, store)
    if stage == "prewrite":
        store.block_write = True
    elif stage == "runtime":
        runtime.block_create = runtime.block_rollback = True
    else:
        store.block_activation = True
    # Independently block metadata removal after the initial worker finishes.
    store.block_delete = True
    store.fail_delete = cleanup_fails
    creation = asyncio.create_task(service.create())
    try:
        if stage == "prewrite":
            assert await asyncio.to_thread(store.write_entered.wait, 2)
        elif stage == "runtime":
            await asyncio.wait_for(runtime.create_entered.wait(), 2)
        else:
            assert await asyncio.to_thread(store.activation_entered.wait, 2)
        for _ in range(cancellations):
            creation.cancel()
            await asyncio.sleep(0)
            with pytest.raises(errors.SandboxCreateCapacityExceeded):
                await service.create()
        assert not creation.done()
        if stage == "runtime":
            await asyncio.wait_for(runtime.rollback_entered.wait(), 2)
        store.write_release.set()
        store.activation_release.set()
        runtime.rollback_release.set()
        assert await asyncio.to_thread(store.delete_entered.wait, 2)
        assert not creation.done()
        with pytest.raises(errors.SandboxCreateCapacityExceeded):
            await service.create()
        # A further cancellation during metadata compensation must not release.
        creation.cancel()
        await asyncio.sleep(0)
        assert not creation.done()
        with pytest.raises(errors.SandboxCreateCapacityExceeded):
            await service.create()
        store.delete_release.set()
        expected = errors.SandboxMetadataCleanupFailed if cleanup_fails else asyncio.CancelledError
        with pytest.raises(expected):
            await creation
        record = await store.get("sbx_" + f"{1:032x}")
        assert (record is not None) == cleanup_fails
        if record is not None:
            assert record.management_state == ("deleting" if stage == "activation" else "pending")
        store.block_write = store.block_activation = store.block_delete = store.fail_delete = False
        runtime.block_create = runtime.block_rollback = False
        await _prove_one_slot_is_reusable(service, runtime)
    finally:
        _release_gates(store, runtime)
        await asyncio.gather(creation, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("outcome", ["clean", "runtime_cleanup_error", "metadata_cleanup_error"])
async def test_late_runtime_success_holds_slot_through_compensation_and_repeated_cancel(
    durable_store: FaultStore, outcome: str,
) -> None:
    """A late success retains admission until exact-transaction compensation has settled."""
    store = durable_store
    runtime = ControlledRuntime(store)
    runtime.block_create = runtime.ignore_create_cancel = runtime.block_delete = True
    service = _durable_service(runtime, store)
    creation = asyncio.create_task(service.create())
    try:
        await asyncio.wait_for(runtime.create_entered.wait(), 2)
        creation.cancel()
        await asyncio.wait_for(runtime.rollback_entered.wait(), 2)
        with pytest.raises(errors.SandboxCreateCapacityExceeded):
            await service.create()
        runtime.create_release.set()
        await asyncio.wait_for(runtime.delete_entered.wait(), 2)
        record = await store.get("sbx_" + f"{1:032x}")
        assert record.management_state == "deleting" and record.version == 1
        creation.cancel()
        await asyncio.sleep(0)
        with pytest.raises(errors.SandboxCreateCapacityExceeded):
            await service.create()
        if outcome == "runtime_cleanup_error":
            runtime.delete_error = errors.SandboxDeleteFailed(record.sandbox_id)
        if outcome == "metadata_cleanup_error":
            store.fail_delete = True
        runtime.delete_release.set()
        expected = {
            "clean": asyncio.CancelledError,
            "runtime_cleanup_error": errors.SandboxCleanupFailed,
            "metadata_cleanup_error": errors.SandboxMetadataCleanupFailed,
        }[outcome]
        with pytest.raises(expected):
            await creation
        remaining = await store.get(record.sandbox_id)
        assert (remaining is not None) == (outcome != "clean")
        if remaining is not None:
            assert remaining.management_state == "deleting" and remaining.version == 1
        assert runtime.expected_deletions == [(record.sandbox_id, True, record.transaction_id)]
        runtime.block_create = runtime.ignore_create_cancel = runtime.block_delete = False
        runtime.delete_error = None
        store.fail_delete = False
        await _prove_one_slot_is_reusable(service, runtime)
    finally:
        _release_gates(store, runtime)
        await asyncio.gather(creation, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("outcome", ["incomplete", "unknown", "activation_late_ack"])
async def test_cancelled_uncertain_result_releases_capacity_but_preserves_diagnostics(
    durable_store: FaultStore, outcome: str,
) -> None:
    """Repeated cancellation and uncertain writes retain rows without permanently reserving slots."""
    store = durable_store
    runtime = ControlledRuntime(store)
    service = _durable_service(runtime, store)
    if outcome == "activation_late_ack":
        store.block_activation = store.fail_after_activation = True
    else:
        runtime.block_create = runtime.block_rollback = True
        runtime.rollback_error = (
            errors.SandboxCleanupFailed("sbx_" + f"{1:032x}")
            if outcome == "incomplete" else RuntimeError("unknown rollback outcome")
        )
    creation = asyncio.create_task(service.create())
    try:
        if outcome == "activation_late_ack":
            assert await asyncio.to_thread(store.activation_entered.wait, 2)
        else:
            await asyncio.wait_for(runtime.create_entered.wait(), 2)
        creation.cancel()
        if outcome != "activation_late_ack":
            await asyncio.wait_for(runtime.rollback_entered.wait(), 2)
        await asyncio.sleep(0)
        creation.cancel()
        await asyncio.sleep(0)
        with pytest.raises(errors.SandboxCreateCapacityExceeded):
            await service.create()
        assert not creation.done()
        _release_gates(store, runtime)
        expected = {
            "incomplete": errors.SandboxCleanupFailed,
            "unknown": errors.SandboxCreateUncertain,
            "activation_late_ack": errors.SandboxMetadataUnavailable,
        }[outcome]
        with pytest.raises(expected):
            await creation
        record = await store.get("sbx_" + f"{1:032x}")
        assert record is not None
        assert record.management_state == ("active" if outcome == "activation_late_ack" else "pending")
        assert record.version == int(outcome == "activation_late_ack")
        assert not runtime.expected_deletions
        runtime.block_create = runtime.block_rollback = False
        runtime.rollback_error = None
        store.block_activation = store.fail_after_activation = False
        await _prove_one_slot_is_reusable(service, runtime)
    finally:
        _release_gates(store, runtime)
        await asyncio.gather(creation, return_exceptions=True)


@pytest.mark.anyio
async def test_simultaneous_waves_never_exceed_configured_admission() -> None:
    """Competing callers never queue, and new waves cannot exploit duplicate slot releases."""
    harness = AdmissionHarness(max_concurrent_creates=3)
    for _ in range(3):
        begin = asyncio.Event()
        rejected = asyncio.Queue()

        async def compete():
            """Enter on one barrier and report only completed admission rejections."""
            await begin.wait()
            try:
                return await harness.service.create()
            except errors.SandboxCreateCapacityExceeded:
                rejected.put_nowait(True)
                return None

        harness.runtime.release.clear()
        tasks = [asyncio.create_task(compete()) for _ in range(24)]
        try:
            begin.set()
            for _ in range(3):
                await asyncio.wait_for(harness.runtime.entered.get(), 2)
            for _ in range(21):
                await asyncio.wait_for(rejected.get(), 2)
            assert harness.runtime.running == harness.runtime.peak == 3
        finally:
            harness.runtime.release.set()
            results = await asyncio.gather(*tasks)
        assert sum(result is not None for result in results) == 3
    assert harness.identity.call_count == harness.clock.call_count == 9


@pytest.mark.anyio
async def test_full_service_does_not_block_noncreation_operations_or_other_services() -> None:
    """Creation admission is local to each Service and leaves unrelated operations available."""
    first, second = AdmissionHarness(max_concurrent_creates=1), AdmissionHarness(max_concurrent_creates=1)
    tasks = await first.start(1)
    other_tasks = []
    try:
        other_tasks = await second.start(1)
        for harness in (first, second):
            with pytest.raises(errors.SandboxCreateCapacityExceeded):
                await harness.service.create()
        # Use an unrelated legacy instance so querying and deleting cannot
        # accidentally interfere with either deliberately paused creation.
        sandbox_id = first.runtime.query_result.sandbox_id
        assert (await first.service.get(sandbox_id)).sandbox_id == sandbox_id
        assert (await first.service.get_endpoint(sandbox_id)).sandbox_id == sandbox_id
        await first.service.ping()
        await first.service.delete(sandbox_id)
        assert first.runtime.deletions == [(sandbox_id, False)]
        result = await first.service.reap_expired_once()
        assert result.succeeded == result.failed == 0
        assert first.runtime.running == second.runtime.running == 1
    finally:
        first.runtime.release.set()
        second.runtime.release.set()
        await asyncio.gather(*tasks, *other_tasks)
