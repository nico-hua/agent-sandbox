"""Deterministic application ownership, capacity and deletion coordination of local entry sessions."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest

from control_plane.runtime.kubernetes.local_entry import LocalEntryError
from test_kubernetes_local_entry import target_snapshot
from test_kubernetes_proxy_lifecycle import runtime
from test_kubernetes_query import SID, TXN


class FakeSession:
    """Control only process ownership; manager admission and coordination run as real production code."""

    def __init__(self, harness, sandbox_id):
        """Keep explicit gates and independently mutable live/closed state without starting a process."""
        self.h = harness
        self.target = harness.targets[sandbox_id]
        self.is_active = False
        self.url = "http://127.0.0.1:43123"
        self.close_calls = 0
        self.started, self.closed = asyncio.Event(), asyncio.Event()

    async def open(self):
        """Hold startup until the test releases its gate, then report an actually active owned session."""
        self.started.set()
        await self.h.open_gate.wait()
        if self.h.open_error:
            raise RuntimeError("private startup diagnostic")
        self.is_active = True
        return self

    async def close(self):
        """Retain ownership through a held or failed cleanup before publishing the closed event."""
        self.close_calls += 1
        self.h.closing.set()
        await self.h.close_gate.wait()
        if self.h.close_error:
            raise RuntimeError("private cleanup diagnostic")
        self.is_active = False
        self.closed.set()


@pytest.fixture
def manager_harness():
    """Provide isolated factories, immutable targets and explicit gates with no network or subprocesses."""
    from control_plane.runtime.kubernetes.local_entry_manager import KubernetesLocalEntryManager
    h = SimpleNamespace(backend=runtime(None), sessions=[], targets={SID: target_snapshot()},
        resolves=[], open_gate=asyncio.Event(), close_gate=asyncio.Event(), closing=asyncio.Event(),
        open_error=False, close_error=False)
    h.open_gate.set()
    h.close_gate.set()

    async def resolver(backend, sandbox_id):
        """Return this target unless a test models replacement or loss before fresh lookup."""
        h.resolves.append(sandbox_id)
        return h.targets[sandbox_id]

    def factory(backend, sandbox_id):
        """Track exactly which process-owning sessions the manager constructs."""
        session = FakeSession(h, sandbox_id)
        h.sessions.append(session)
        return session

    h.make = lambda **kwargs: KubernetesLocalEntryManager(h.backend, session_factory=factory,
                                                         target_resolver=resolver, **kwargs)
    return h


@pytest.mark.anyio
async def test_concurrent_open_owns_one_startup_and_lookup_never_spawns(manager_harness):
    """Competing opens share one owned startup and every lookup checks current identity without a new child."""
    h = manager_harness
    h.open_gate.clear()
    manager = h.make()
    tasks = [asyncio.create_task(manager.open(SID)) for _ in range(5)]
    while not h.sessions:
        await asyncio.sleep(0)
    await h.sessions[0].started.wait()
    assert manager.session_count == 1 and len(h.sessions) == 1
    h.open_gate.set()
    results = await asyncio.gather(*tasks)
    assert {item.url for item in results} == {"http://127.0.0.1:43123"}
    for _ in range(3):
        assert (await manager.lookup(SID)).target == h.targets[SID]
    assert len(h.sessions) == 1
    await manager.close_all()
    assert manager.is_closed and manager.session_count == 0 and h.sessions[0].closed.is_set()


@pytest.mark.anyio
async def test_cancelled_waiter_leaves_late_session_owned_until_shutdown(manager_harness):
    """The request can stop waiting, while the manager retains and eventually closes its late result."""
    h = manager_harness
    h.open_gate.clear()
    manager = h.make()
    waiting = asyncio.create_task(manager.open(SID))
    while not h.sessions:
        await asyncio.sleep(0)
    await h.sessions[0].started.wait()
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert manager.session_count == 1
    closing = asyncio.create_task(manager.close_all())
    await asyncio.sleep(0)
    assert not closing.done()
    h.open_gate.set()
    await closing
    assert manager.is_closed and h.sessions[0].closed.is_set()


@pytest.mark.anyio
async def test_capacity_counts_startup_and_failed_cleanup_without_eviction(manager_harness):
    """Unconfirmed cleanup retains its slot and cannot be displaced by another sandbox open."""
    h = manager_harness
    manager = h.make(max_sessions=1)
    await manager.open(SID)
    h.close_error = True
    with pytest.raises(LocalEntryError) as captured:
        await manager.close(SID, expected_transaction_id=TXN)
    assert captured.value.code == "local_entry_cleanup_failed" and manager.session_count == 1
    other = "sbx_" + "f" * 32
    with pytest.raises(LocalEntryError) as full:
        await manager.open(other)
    assert full.value.code == "local_entry_capacity_exceeded" and len(h.sessions) == 1
    h.close_error = False
    await manager.close_all()
    assert manager.is_closed


@pytest.mark.anyio
async def test_failed_open_is_cleaned_and_can_be_explicitly_retried(manager_harness):
    """A fully cleaned startup failure frees capacity but never retries the request automatically."""
    h = manager_harness
    manager = h.make(max_sessions=1)
    h.open_error = True
    with pytest.raises(LocalEntryError) as failure:
        await manager.open(SID)
    assert "private" not in str(failure.value)
    assert manager.session_count == 0 and len(h.sessions) == 1 and h.sessions[0].closed.is_set()
    h.open_error = False
    assert (await manager.open(SID)).target.transaction_id == TXN
    assert len(h.sessions) == 2
    await manager.close_all()


@pytest.mark.anyio
async def test_close_wrong_transaction_never_signals_original_session(manager_harness):
    """An old or mistaken deletion identity cannot close another transaction's local entry."""
    h = manager_harness
    manager = h.make()
    await manager.open(SID)
    with pytest.raises(LocalEntryError):
        await manager.close(SID, expected_transaction_id="txn_" + "f" * 32)
    assert h.sessions[0].close_calls == 0 and h.sessions[0].is_active
    assert (await manager.lookup(SID)).target.transaction_id == TXN
    await manager.close_all()


@pytest.mark.anyio
async def test_fresh_lookup_rejects_replacement_or_dead_session_without_reconnect(manager_harness):
    """Session cache validity cannot replace per-request identity checks or restart a stopped forward."""
    h = manager_harness
    manager = h.make()
    await manager.open(SID)
    h.targets[SID] = replace(h.targets[SID], transaction_id="txn_" + "f" * 32)
    with pytest.raises(LocalEntryError):
        await manager.lookup(SID)
    assert len(h.sessions) == 1
    h.targets[SID] = h.sessions[0].target
    h.sessions[0].is_active = False
    with pytest.raises(LocalEntryError):
        await manager.lookup(SID)
    assert len(h.sessions) == 1
    await manager.close_all()


@pytest.mark.anyio
async def test_delete_guard_blocks_open_and_lookup_until_deletion_finished(manager_harness):
    """Closing before deletion is insufficient unless new opens stay blocked through the resource operation."""
    h = manager_harness
    manager = h.make()
    await manager.open(SID)
    async with manager.delete_guard(SID, expected_transaction_id=TXN):
        assert h.sessions[0].closed.is_set()
        with pytest.raises(LocalEntryError):
            await manager.open(SID)
        with pytest.raises(LocalEntryError):
            await manager.lookup(SID)
    assert len(h.sessions) == 1
    await manager.open(SID)
    assert len(h.sessions) == 2
    await manager.close_all()


@pytest.mark.anyio
async def test_repeated_shutdown_cancellation_waits_for_cleanup(manager_harness):
    """Repeated cancellation cannot drop application ownership of closing sessions."""
    h = manager_harness
    manager = h.make()
    await manager.open(SID)
    h.close_gate.clear()
    closing = asyncio.create_task(manager.close_all())
    await h.closing.wait()
    closing.cancel()
    await asyncio.sleep(0)
    closing.cancel()
    await asyncio.sleep(0)
    assert not closing.done() and manager.session_count == 1
    h.close_gate.set()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert manager.is_closed
    await manager.close_all()
    with pytest.raises(LocalEntryError):
        await manager.open(SID)


@pytest.mark.parametrize("value", [True, 0, -1, 33, 1.5, "2", None])
def test_manager_capacity_is_strict_and_construction_is_inert(value, manager_harness):
    """Configuration mistakes are rejected before target reads, factories or background tasks."""
    h = manager_harness
    with pytest.raises(ValueError):
        h.make(max_sessions=value)
    assert not h.sessions and not h.resolves


@pytest.mark.anyio
async def test_late_open_waiter_cannot_receive_reopened_transaction(manager_harness, monkeypatch):
    """A waiter belongs to its reserved entry, even if that slot is closed and explicitly reopened meanwhile."""
    from control_plane.runtime.kubernetes import local_entry_manager as module
    h = manager_harness
    manager = h.make()
    returned, release = asyncio.Event(), asyncio.Event()
    original = module.wait_for_task
    calls = 0

    async def paused_return(task):
        """Pause only the original requester after startup completes to expose its identity handoff window."""
        nonlocal calls
        calls += 1
        first = calls == 1
        result = await original(task)
        if first:
            returned.set()
            await release.wait()
        return result

    monkeypatch.setattr(module, "wait_for_task", paused_return)
    waiting = asyncio.create_task(manager.open(SID))
    await returned.wait()
    await manager.close(SID, expected_transaction_id=TXN)
    h.targets[SID] = replace(h.targets[SID], transaction_id="txn_" + "f" * 32)
    current = await manager.open(SID)
    release.set()
    try:
        with pytest.raises(LocalEntryError):
            await waiting
        assert current.target.transaction_id != TXN and h.sessions[-1].is_active
    finally:
        await manager.close_all()


@pytest.mark.anyio
@pytest.mark.parametrize("origin", ["http://localhost:43123", "https://127.0.0.1:43123", "http://127.0.0.1:0",
    "http://user@127.0.0.1:43123", "http://127.0.0.1:43123/", "http://127.0.0.1:43123?x=1"])
async def test_lookup_rejects_noncanonical_session_origin(origin, manager_harness):
    """An injected session cannot widen the localhost origin or smuggle credentials and routing in its URL."""
    h = manager_harness
    manager = h.make()
    await manager.open(SID)
    h.sessions[0].url = origin
    with pytest.raises(LocalEntryError):
        await manager.lookup(SID)
    assert len(h.sessions) == 1
    await manager.close_all()


@pytest.mark.anyio
async def test_capacity_rejects_while_original_startup_is_still_pending(manager_harness):
    """A second sandbox cannot bypass capacity while the first process is still starting."""
    h = manager_harness
    manager = h.make(max_sessions=1)
    h.open_gate.clear()
    opening = asyncio.create_task(manager.open(SID))
    while not h.sessions:
        await asyncio.sleep(0)
    await h.sessions[0].started.wait()
    before = len(h.resolves)
    with pytest.raises(LocalEntryError) as captured:
        await manager.open("sbx_" + "f" * 32)
    assert captured.value.code == "local_entry_capacity_exceeded" and len(h.resolves) == before
    assert manager.session_count == 1
    h.open_gate.set()
    await opening
    await manager.close_all()
