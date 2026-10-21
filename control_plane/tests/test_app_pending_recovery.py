"""Opt-in, once-per-startup pending confirmation under SQLite application ownership."""

import asyncio
import threading
from dataclasses import replace

import pytest

import control_plane.app as app_module
import control_plane.core.service as service_module
from control_plane.app import create_app
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore
from control_plane.storage.run_lock import LifecycleRunLockBusy, SQLiteRunLock
from lifecycle_fakes import FakeLifecycleStore
from test_pending_recovery import NOW, RecoveryRuntime, RecoveryStore, add_pending


@pytest.mark.anyio
async def test_cancelled_candidate_selection_keeps_lock_until_read_finishes(tmp_path):
    """Even the initial SQLite selection must close its connection before startup unlocks."""
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()

    class SelectionStore(SQLiteLifecycleStore):
        """Gate the real initial pending query without changing its bounded SQL behavior."""

        def _list_pending_sync(self, limit):
            """Keep the worker observable until the test permits its final read to finish."""
            entered.set()
            try:
                assert release.wait(5)
                return super()._list_pending_sync(limit)
            finally:
                finished.set()

    store = SelectionStore(tmp_path / "lifecycle.sqlite3")
    runtime = RecoveryRuntime()
    application = create_app(runtime, store=store, pending_recovery_enabled=True, reaper_enabled=False)
    context = application.router.lifespan_context(application)
    startup = asyncio.create_task(context.__aenter__())
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        startup.cancel()
        await asyncio.sleep(0)
        startup.cancel()
        await asyncio.sleep(0)
        assert not startup.done() and not finished.is_set()
        lock = SQLiteRunLock(store.database_path)
        with pytest.raises(LifecycleRunLockBusy):
            lock.acquire()
        lock.release()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await startup
        assert finished.is_set() and runtime.calls == []
        lock.acquire()
        lock.release()
    finally:
        release.set()
        await asyncio.gather(startup, return_exceptions=True)


@pytest.mark.parametrize("options", [
    {"pending_recovery_enabled": 1}, {"pending_recovery_enabled": None},
    {"pending_recovery_enabled": "true"}, {"pending_recovery_batch_limit": 0},
    {"pending_recovery_batch_limit": -1}, {"pending_recovery_batch_limit": True},
    {"pending_recovery_batch_limit": 1.0}, {"pending_recovery_batch_limit": "100"},
    {"pending_recovery_batch_limit": 2**63},
])
def test_startup_recovery_configuration_is_strict_and_lazy(tmp_path, options):
    """Invalid options fail during assembly before a lock or database can be created."""
    database = tmp_path / "absent" / "lifecycle.sqlite3"
    with pytest.raises(ValueError):
        create_app(RecoveryRuntime(), database_path=database, **options)
    assert not database.parent.exists()


def test_startup_recovery_rejects_store_without_application_ownership(tmp_path):
    """A database_path argument cannot invent ownership for an unrelated injected fake Store."""
    with pytest.raises(ValueError, match="ownership"):
        create_app(
            RecoveryRuntime(), store=FakeLifecycleStore(),
            database_path=tmp_path / "unrelated.sqlite3", pending_recovery_enabled=True,
        )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.anyio
async def test_default_startup_does_not_recover_pending(tmp_path):
    """The existing startup behavior leaves pending records unchanged without an opt-in."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime)
    application = create_app(runtime, store=store, reaper_enabled=False)
    async with application.router.lifespan_context(application):
        assert await store.get(record.sandbox_id) == record
        assert runtime.calls == []
        assert application.state.pending_recovery_result is None


@pytest.mark.anyio
async def test_recovery_is_locked_initialized_and_complete_before_reaper_and_serving(tmp_path, monkeypatch):
    """Startup yields only after the one bounded round and starts reaping afterwards."""
    monkeypatch.setattr(service_module, "_utc_now", lambda: NOW)
    store = RecoveryStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    runtime = RecoveryRuntime()
    first = await add_pending(store, runtime, 1)
    second = await add_pending(store, runtime, 2)
    runtime.block_endpoint = True
    reaper_started = asyncio.Event()
    events = []
    initialize = store.initialize
    list_pending = store.list_pending

    async def initialized():
        """Record completion of this startup's actual SQLite initialization."""
        await initialize()
        events.append("initialized")

    async def candidates(**kwargs):
        """Require initialization first and detect attempts to drain more than one batch."""
        assert events == ["initialized"]
        events.append("recovery")
        return await list_pending(**kwargs)

    monkeypatch.setattr(store, "initialize", initialized)
    monkeypatch.setattr(store, "list_pending", candidates)

    async def reaper(service, **kwargs):
        """Require the acknowledged activation to precede scheduling background work."""
        assert (await store.get(first.sandbox_id)).management_state == "active"
        reaper_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(app_module, "run_reaper", reaper)
    application = create_app(
        runtime, store=store, pending_recovery_enabled=True, pending_recovery_batch_limit=1,
    )
    context = application.router.lifespan_context(application)
    startup = asyncio.create_task(context.__aenter__())
    try:
        await asyncio.wait_for(runtime.entered.wait(), 3)
        assert not startup.done() and not reaper_started.is_set()
        assert await store.get(first.sandbox_id) == first
        contender = SQLiteRunLock(store.database_path)
        with pytest.raises(LifecycleRunLockBusy):
            contender.acquire()
        contender.release()
        runtime.release.set()
        await startup
        await asyncio.wait_for(reaper_started.wait(), 3)
        result = application.state.pending_recovery_result
        assert result.recovered == 1 and len(result.items) == 1
        assert await store.get(first.sandbox_id) == replace(first, management_state="active", version=1)
        assert await store.get(second.sandbox_id) == second
        assert runtime.specs == runtime.deletions == []
        assert events == ["initialized", "recovery"]
    finally:
        runtime.release.set()
        await asyncio.gather(startup, return_exceptions=True)
        if not startup.cancelled() and startup.exception() is None:
            await context.__aexit__(None, None, None)


@pytest.mark.anyio
async def test_initialization_failure_prevents_enabled_recovery(tmp_path, monkeypatch):
    """Recovery cannot run on storage whose startup initialization has not succeeded."""
    runtime = RecoveryRuntime()
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")

    async def unavailable():
        """Fail initialization without preparing any metadata for recovery."""
        raise LifecycleStoreError()

    monkeypatch.setattr(store, "initialize", unavailable)
    application = create_app(runtime, store=store, pending_recovery_enabled=True)
    with pytest.raises(LifecycleStoreError):
        async with application.router.lifespan_context(application):
            pytest.fail("failed initialization served requests")
    assert runtime.calls == [] and application.state.reaper_task is None
    lock = SQLiteRunLock(store.database_path)
    lock.acquire()
    lock.release()


@pytest.mark.anyio
async def test_lock_competitor_never_initializes_or_recovers(tmp_path, monkeypatch):
    """A competing app fails before it can perform even read-only recovery work."""
    database = tmp_path / "lifecycle.sqlite3"
    first = create_app(RecoveryRuntime(), database_path=database, reaper_enabled=False)
    runtime = RecoveryRuntime()
    second = create_app(runtime, database_path=database, pending_recovery_enabled=True)

    async def forbidden():
        """Detect any initialization attempted after losing application ownership."""
        pytest.fail("competing application initialized storage")

    monkeypatch.setattr(second.state.lifecycle_store, "initialize", forbidden)
    async with first.router.lifespan_context(first):
        with pytest.raises(LifecycleRunLockBusy):
            async with second.router.lifespan_context(second):
                pytest.fail("competing app served requests")
    assert runtime.calls == [] and second.state.reaper_task is None


@pytest.mark.anyio
async def test_backend_outage_warns_but_does_not_prevent_startup(tmp_path, monkeypatch, caplog):
    """An unavailable runtime stops this recovery round while preserving service availability."""
    monkeypatch.setattr(service_module, "_utc_now", lambda: NOW)
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime)
    await add_pending(store, runtime, 2)
    runtime.classification = "unavailable"
    application = create_app(runtime, store=store, pending_recovery_enabled=True, reaper_enabled=False)
    async with application.router.lifespan_context(application):
        result = application.state.pending_recovery_result
        assert result.error == "docker_unavailable" and len(result.items) == 1
        assert await store.get(record.sandbox_id) == record
    assert "docker_unavailable" in caplog.text
    assert "private" not in caplog.text and str(store.database_path) not in caplog.text


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["list", "activate"])
async def test_storage_failure_blocks_startup_and_releases_ownership(tmp_path, monkeypatch, phase):
    """Storage uncertainty prevents startup, preserves actual commits and does not trigger deletion."""
    monkeypatch.setattr(service_module, "_utc_now", lambda: NOW)
    store = RecoveryStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    runtime = RecoveryRuntime()
    record = await add_pending(store, runtime)
    if phase == "list":
        async def unavailable(**kwargs):
            """Fail pending selection without letting the result look like an empty batch."""
            raise LifecycleStoreError()

        monkeypatch.setattr(store, "list_pending", unavailable)
    else:
        store.fail_after_commit = True
    application = create_app(runtime, store=store, pending_recovery_enabled=True)
    with pytest.raises(LifecycleStoreError):
        async with application.router.lifespan_context(application):
            pytest.fail("storage failure must abort startup")
    assert application.state.reaper_task is None and runtime.specs == runtime.deletions == []
    expected = record if phase == "list" else replace(record, management_state="active", version=1)
    assert await store.get(record.sandbox_id) == expected
    retry = create_app(runtime, database_path=store.database_path, reaper_enabled=False)
    async with retry.router.lifespan_context(retry):
        assert await retry.state.lifecycle_store.get(record.sandbox_id) == expected


@pytest.mark.anyio
@pytest.mark.parametrize("late_error", [False, True])
async def test_cancelled_startup_holds_lock_until_current_recovery_commit_settles(tmp_path, monkeypatch, late_error):
    """Repeated startup cancellation waits for activation without continuing the pending batch."""
    monkeypatch.setattr(service_module, "_utc_now", lambda: NOW)
    store = RecoveryStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    runtime = RecoveryRuntime()
    first = await add_pending(store, runtime, 1)
    second = await add_pending(store, runtime, 2)
    store.block_activation = True
    store.fail_after_commit = late_error
    application = create_app(runtime, store=store, pending_recovery_enabled=True)
    context = application.router.lifespan_context(application)
    startup = asyncio.create_task(context.__aenter__())
    try:
        assert await asyncio.to_thread(store.entered.wait, 3)
        startup.cancel()
        await asyncio.sleep(0)
        startup.cancel()
        await asyncio.sleep(0)
        assert not startup.done()
        competitor = SQLiteRunLock(store.database_path)
        with pytest.raises(LifecycleRunLockBusy):
            competitor.acquire()
        competitor.release()
        store.release.set()
        with pytest.raises(asyncio.CancelledError):
            await startup
        assert store.finished.is_set() and application.state.reaper_task is None
        assert await store.get(first.sandbox_id) == replace(first, management_state="active", version=1)
        assert await store.get(second.sandbox_id) == second
        assert runtime.specs == runtime.deletions == []
        competitor.acquire()
        competitor.release()
    finally:
        store.release.set()
        await asyncio.gather(startup, return_exceptions=True)
