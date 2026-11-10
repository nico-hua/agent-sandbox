"""Application-owned local entry startup, ownership and cancellation boundaries."""

import asyncio

import pytest

import control_plane.app as app_module
from control_plane.app import create_app
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from control_plane.runtime.kubernetes.local_entry import LocalEntryError
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore
from control_plane.storage.run_lock import LifecycleRunLockBusy, SQLiteRunLock
from lifecycle_fakes import FakeLifecycleStore
from test_app import FakeRuntime

SBX = "sbx_0123456789abcdef0123456789abcdef"
OTHER = "sbx_abcdef0123456789abcdef0123456789"


@pytest.fixture
def anyio_backend():
    """Use asyncio for lifecycle cancellation tests."""
    return "asyncio"


def runtime():
    """Build an inert Kubernetes runtime whose client must never be acquired."""
    def forbidden(**kwargs):
        """Detect unintended client acquisition during assembly or fake lifespans."""
        pytest.fail("unexpected Kubernetes client acquisition")

    return KubernetesSandboxRuntime(kubeconfig_path="/unread/kubeconfig", context="test", namespace="sandboxes", client_factory=forbidden)


class EntryManager:
    """Expose the manager contract with controlled asynchronous cleanup completion."""

    def __init__(self, backend, *, events=None, fail_open=None, fail_close=False):
        """Keep inert ownership and controls without starting resource work."""
        self.runtime = backend
        self._application_owner = None
        self.events = [] if events is None else events
        self.fail_open = fail_open
        self.fail_close = fail_close
        self.sessions = set()
        self.is_closed = False
        self.close_entered = asyncio.Event()
        self.close_release = asyncio.Event()
        self.close_release.set()

    def claim_application(self, owner):
        """Model the exclusive ownership claim made by the real application before any startup work."""
        if self._application_owner is not None and self._application_owner is not owner:
            raise ValueError("manager already owned")
        self._application_owner = owner

    @property
    def session_count(self):
        """Expose retained sessions so cleanup failures remain observable."""
        return len(self.sessions)

    async def open(self, sandbox_id):
        """Record startup order and retain each successfully opened session."""
        self.events.append(("open", sandbox_id))
        if sandbox_id == self.fail_open:
            raise LocalEntryError("local_entry_unavailable")
        self.sessions.add(sandbox_id)
        return object()

    async def close_all(self):
        """Hold shutdown until cleanup completion is explicitly allowed."""
        self.events.append("closing")
        self.close_entered.set()
        await self.close_release.wait()
        if self.fail_close:
            raise LocalEntryError("local_entry_cleanup_failed")
        self.sessions.clear()
        self.is_closed = True
        self.events.append("closed")


def application(backend, manager, **kwargs):
    """Assemble a quiet application with explicit manager ownership."""
    return create_app(backend, kubernetes_entry_manager=manager, allow_unauthenticated=True, reaper_enabled=False, **kwargs)


@pytest.mark.parametrize("ids", [None, SBX, {SBX}, [f"sbx_{value:032x}" for value in range(9)], [SBX, SBX], ["sbx_bad"], [True], [SBX.upper()]])
def test_invalid_initial_ids_fail_during_assembly(ids):
    """Reject invalid types, raw oversize, duplicates and noncanonical names."""
    with pytest.raises(ValueError):
        create_app(runtime(), kubernetes_initial_sandbox_ids=ids, store=FakeLifecycleStore())


def test_non_kubernetes_configuration_and_wrong_runtime_are_rejected():
    """Never bind entry ownership to Docker or to another Kubernetes runtime."""
    first, second = runtime(), runtime()
    with pytest.raises(ValueError):
        application(FakeRuntime(), EntryManager(first), store=FakeLifecycleStore())
    with pytest.raises(ValueError):
        create_app(FakeRuntime(), kubernetes_initial_sandbox_ids=[SBX], store=FakeLifecycleStore())
    with pytest.raises(ValueError):
        application(first, EntryManager(second), store=FakeLifecycleStore())


def test_implicit_existing_manager_is_ambiguous():
    """Runtime attachment cannot silently transfer manager shutdown ownership."""
    backend = runtime()
    manager = EntryManager(backend)
    backend.bind_local_entry_manager(manager)
    with pytest.raises(ValueError):
        create_app(backend, store=FakeLifecycleStore())
    assert application(backend, manager, store=FakeLifecycleStore()).state.kubernetes_entry_manager is manager


@pytest.mark.parametrize("count", [1, 8])
def test_initial_ids_automatically_create_inert_manager(tmp_path, monkeypatch, count):
    """Explicit IDs attach a pure manager without filesystem, task or client work."""
    backend = runtime()
    database = tmp_path / "uncreated" / "lifecycle.sqlite3"

    def forbidden(*args, **kwargs):
        """Reject external resource work before entering the lifespan."""
        pytest.fail("assembly started external work")

    monkeypatch.setattr(app_module.asyncio, "create_task", forbidden)
    monkeypatch.setattr(app_module.SQLiteRunLock, "acquire", forbidden)
    app = create_app(backend, kubernetes_initial_sandbox_ids=[f"sbx_{value:032x}" for value in range(count)], database_path=database)
    assert app.state.kubernetes_entry_manager.runtime is backend
    assert backend.local_entry_manager is app.state.kubernetes_entry_manager
    assert app.state.kubernetes_entry_manager.session_count == 0
    assert not database.parent.exists()


@pytest.mark.anyio
async def test_authentication_failure_prevents_initial_open(tmp_path, monkeypatch):
    """Missing authentication fails before storage and forwarding."""
    monkeypatch.delenv("AGENT_SANDBOX_API_KEY", raising=False)
    backend = runtime()
    manager = EntryManager(backend)
    database = tmp_path / "uncreated" / "lifecycle.sqlite3"
    app = create_app(backend, kubernetes_entry_manager=manager, kubernetes_initial_sandbox_ids=[SBX], database_path=database)
    with pytest.raises(RuntimeError, match="API key"):
        async with app.router.lifespan_context(app):
            pytest.fail("unconfigured authentication started application")
    assert manager.events == []
    assert not database.parent.exists()


@pytest.mark.anyio
async def test_initial_ids_are_frozen_and_open_after_storage(tmp_path, monkeypatch):
    """Startup opens copied explicit IDs only after real SQLite initialization."""
    backend = runtime()
    events = []
    manager = EntryManager(backend, events=events)
    initial = [SBX, OTHER]
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    initialize = store.initialize

    async def initialized():
        """Record this startup's real database initialization completion."""
        await initialize()
        events.append("initialized")

    monkeypatch.setattr(store, "initialize", initialized)
    app = application(backend, manager, store=store, kubernetes_initial_sandbox_ids=initial)
    initial.clear()
    async with app.router.lifespan_context(app):
        assert events == ["initialized", ("open", SBX), ("open", OTHER)]
        assert manager.session_count == 2
    assert events[-2:] == ["closing", "closed"]
    assert manager.session_count == 0


@pytest.mark.anyio
@pytest.mark.parametrize("fail_initialization", [True, False])
async def test_startup_failure_closes_owned_manager(tmp_path, monkeypatch, fail_initialization):
    """Storage and partial entry failure both finish cleanup before ownership release."""
    backend = runtime()
    manager = EntryManager(backend, fail_open=OTHER)
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    if fail_initialization:
        async def unavailable():
            """Fail initialization before forwarding is requested."""
            raise LifecycleStoreError()

        monkeypatch.setattr(store, "initialize", unavailable)
    app = application(backend, manager, store=store, kubernetes_initial_sandbox_ids=[SBX, OTHER])
    with pytest.raises(LifecycleStoreError if fail_initialization else LocalEntryError):
        async with app.router.lifespan_context(app):
            pytest.fail("failed startup yielded application")
    assert manager.events == (["closing", "closed"] if fail_initialization else [("open", SBX), ("open", OTHER), "closing", "closed"])
    assert manager.is_closed
    contender = SQLiteRunLock(store.database_path)
    contender.acquire()
    contender.release()


@pytest.mark.anyio
async def test_shutdown_holds_lock_through_repeated_cancellation(tmp_path):
    """Cancellation cannot release database ownership while cleanup owns processes."""
    backend = runtime()
    manager = EntryManager(backend)
    manager.close_release.clear()
    database = tmp_path / "lifecycle.sqlite3"
    app = application(backend, manager, database_path=database, kubernetes_initial_sandbox_ids=[SBX])
    started, stop = asyncio.Event(), asyncio.Event()

    async def serve():
        """Run through requested shutdown and application-owned cleanup."""
        async with app.router.lifespan_context(app):
            started.set()
            await stop.wait()

    worker = asyncio.create_task(serve())
    await started.wait()
    stop.set()
    await manager.close_entered.wait()
    contender = SQLiteRunLock(database)
    try:
        for _ in range(3):
            worker.cancel()
            await asyncio.sleep(0)
            assert not worker.done()
            with pytest.raises(LifecycleRunLockBusy):
                contender.acquire()
        manager.close_release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
        assert manager.is_closed
        contender.acquire()
    finally:
        manager.close_release.set()
        await asyncio.gather(worker, return_exceptions=True)
        contender.release()


@pytest.mark.anyio
async def test_cleanup_failure_retains_manager_lock_and_running_guard(tmp_path):
    """Unconfirmed cleanup cannot advertise reusable lifespan or database ownership."""
    backend = runtime()
    manager = EntryManager(backend, fail_close=True)
    database = tmp_path / "lifecycle.sqlite3"
    app = application(backend, manager, database_path=database, kubernetes_initial_sandbox_ids=[SBX])
    try:
        with pytest.raises(LocalEntryError, match="cleanup"):
            async with app.router.lifespan_context(app):
                pass
        assert app.state.kubernetes_entry_manager is manager
        assert manager.session_count == 1
        assert app.state.kubernetes_entry_run_lock is not None
        contender = SQLiteRunLock(database)
        with pytest.raises(LifecycleRunLockBusy):
            contender.acquire()
        with pytest.raises(RuntimeError, match="already running"):
            async with app.router.lifespan_context(app):
                pytest.fail("application restarted after unconfirmed cleanup")
    finally:
        manager.fail_close = False
        await manager.close_all()
        held = getattr(app.state, "kubernetes_entry_run_lock", None)
        if held is not None:
            held.release()


@pytest.mark.anyio
async def test_cancelled_initial_open_still_settles_cleanup(tmp_path, monkeypatch):
    """Startup cancellation stops the initial batch and retains ownership until cleanup settles."""
    backend = runtime()
    manager = EntryManager(backend)
    manager.close_release.clear()
    opened = asyncio.Event()

    async def opening(sandbox_id):
        """Hold the first initial open until the lifespan owner is cancelled."""
        manager.events.append(("open", sandbox_id))
        opened.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(manager, "open", opening)
    database = tmp_path / "lifecycle.sqlite3"
    app = application(backend, manager, database_path=database, kubernetes_initial_sandbox_ids=[SBX, OTHER])

    async def startup():
        """Attempt startup while the first initial entry remains in flight."""
        async with app.router.lifespan_context(app):
            pytest.fail("cancelled startup served requests")

    worker = asyncio.create_task(startup())
    await opened.wait()
    worker.cancel()
    await manager.close_entered.wait()
    contender = SQLiteRunLock(database)
    try:
        worker.cancel()
        await asyncio.sleep(0)
        assert not worker.done()
        with pytest.raises(LifecycleRunLockBusy):
            contender.acquire()
        assert manager.events == [("open", SBX), "closing"]
        manager.close_release.set()
        with pytest.raises(asyncio.CancelledError):
            await worker
        contender.acquire()
    finally:
        manager.close_release.set()
        await asyncio.gather(worker, return_exceptions=True)
        contender.release()


@pytest.mark.anyio
async def test_reaper_settles_before_owned_entries_close(tmp_path, monkeypatch):
    """The running reaper cannot operate concurrently with manager teardown."""
    backend = runtime()
    events = []
    manager = EntryManager(backend, events=events)
    reaper_started = asyncio.Event()

    async def reaper(service, **kwargs):
        """Keep the actual application task active and record its completed shutdown."""
        reaper_started.set()
        try:
            await asyncio.Event().wait()
        finally:
            events.append("reaper stopped")

    monkeypatch.setattr(app_module, "run_reaper", reaper)
    app = create_app(backend, kubernetes_entry_manager=manager, kubernetes_initial_sandbox_ids=[SBX], database_path=tmp_path / "lifecycle.sqlite3", allow_unauthenticated=True)
    async with app.router.lifespan_context(app):
        await reaper_started.wait()
        assert events == [("open", SBX)]
    assert events == [("open", SBX), "reaper stopped", "closing", "closed"]


@pytest.mark.anyio
async def test_two_applications_cannot_share_shutdown_authority_over_one_manager():
    """A second application must not acquire authority to close the first application's live entries."""
    backend = runtime()
    manager = EntryManager(backend)
    first = application(backend, manager, store=FakeLifecycleStore(), kubernetes_initial_sandbox_ids=[SBX])
    async with first.router.lifespan_context(first):
        assert manager.sessions == {SBX}
        with pytest.raises(ValueError):
            application(backend, manager, store=FakeLifecycleStore())
        assert manager.sessions == {SBX} and not manager.is_closed
    assert manager.is_closed
