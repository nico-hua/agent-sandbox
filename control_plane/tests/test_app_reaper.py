"""Application-owned periodic expiration tasks and safe lifespan shutdown."""

import asyncio
from datetime import timedelta
from pathlib import Path

import httpx
import pytest

import control_plane.app as app_module
import control_plane.core.service as service_module
from control_plane.app import create_app
from control_plane.core.models import SandboxReapResult
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore
from test_sandbox_service import FakeRuntime
from test_service_reaping import NOW, ReapRuntime, add_record


@pytest.mark.parametrize(
    "options",
    [
        {"reaper_enabled": 1},
        {"reaper_enabled": None},
        {"reaper_interval_seconds": 0},
        {"reaper_interval_seconds": -1},
        {"reaper_interval_seconds": True},
        {"reaper_interval_seconds": float("nan")},
        {"reaper_interval_seconds": float("inf")},
        {"reaper_interval_seconds": "60"},
        {"reaper_batch_limit": 0},
        {"reaper_batch_limit": -1},
        {"reaper_batch_limit": True},
        {"reaper_batch_limit": 1.5},
        {"reaper_batch_limit": 2**63},
        {"reaper_enabled": False, "reaper_interval_seconds": 0},
    ],
)
def test_create_app_rejects_invalid_reaper_configuration(tmp_path: Path, options: dict) -> None:
    """Invalid scheduling options must fail before any startup or persistence effects."""
    database = tmp_path / "new" / "lifecycle.sqlite3"
    with pytest.raises(ValueError):
        create_app(FakeRuntime(), database_path=database, **options)
    assert not database.parent.exists()


@pytest.mark.anyio
async def test_lifespan_starts_single_background_task_without_waiting_for_first_round(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Startup returns while a slow first round runs and HTTP remains available."""
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = []

    async def controlled_reaper(service, *, interval_seconds, batch_limit) -> None:
        """Hold the first background round until lifespan cancels the task."""
        calls.append((service, interval_seconds, batch_limit))
        entered.set()
        await release.wait()

    monkeypatch.setattr(app_module, "run_reaper", controlled_reaper)
    application = create_app(
        FakeRuntime(), database_path=tmp_path / "lifecycle.sqlite3",
        reaper_enabled=True, reaper_interval_seconds=0.25, reaper_batch_limit=7,
    )
    assert calls == []
    assert application.state.reaper_task is None
    async with asyncio.timeout(3):
        async with application.router.lifespan_context(application):
            await entered.wait()
            task = application.state.reaper_task
            assert task is not None and not task.done()
            assert calls == [(application.state.service, 0.25, 7)]
            assert await application.state.lifecycle_store.get("missing") is None
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=application), base_url="http://test",
            ) as client:
                response = await client.get("/healthz")
            assert response.status_code == 200
        assert task.done()
        assert application.state.reaper_task is None
        assert task not in asyncio.all_tasks()


@pytest.mark.anyio
async def test_default_reaper_starts_after_database_initialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default startup enables one loop with the documented interval and batch size."""
    called = asyncio.Event()
    observations = []

    async def observe(service, *, interval_seconds, batch_limit) -> None:
        """Read initialized storage before waiting for normal lifespan cancellation."""
        observations.append((await service._store.get("missing"), interval_seconds, batch_limit))
        called.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(app_module, "run_reaper", observe)
    application = create_app(FakeRuntime(), database_path=tmp_path / "lifecycle.sqlite3")
    async with asyncio.timeout(3):
        async with application.router.lifespan_context(application):
            await called.wait()
            assert observations == [(None, 60.0, 100)]


@pytest.mark.anyio
@pytest.mark.parametrize("initialization_fails", [False, True])
async def test_disabled_or_failed_initialization_never_starts_reaper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, initialization_fails: bool,
) -> None:
    """Disabled scheduling still initializes metadata; failed initialization never schedules."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")

    async def fail_initialize() -> None:
        """Reject startup before opening a database or starting any background work."""
        raise LifecycleStoreError()

    async def forbidden_reaper(*args, **kwargs) -> None:
        """Detect an unwanted background task when initialization or configuration forbids it."""
        pytest.fail("background reaper must not start")

    monkeypatch.setattr(app_module, "run_reaper", forbidden_reaper)
    if initialization_fails:
        monkeypatch.setattr(store, "initialize", fail_initialize)
    application = create_app(
        FakeRuntime(), store=store, reaper_enabled=initialization_fails,
    )
    if initialization_fails:
        with pytest.raises(LifecycleStoreError):
            async with application.router.lifespan_context(application):
                pytest.fail("startup must fail")
    else:
        async with application.router.lifespan_context(application):
            assert await store.get("missing") is None
    assert application.state.reaper_task is None


@pytest.mark.anyio
async def test_one_application_cannot_enter_two_overlapping_lifespans(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A duplicate lifespan cannot replace the owned task or create an overlapping loop."""
    entered = asyncio.Event()
    calls = 0

    async def controlled_reaper(*args, **kwargs) -> None:
        """Count a single owned loop and wait until application shutdown."""
        nonlocal calls
        calls += 1
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(app_module, "run_reaper", controlled_reaper)
    application = create_app(
        FakeRuntime(), database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=True,
    )
    async with asyncio.timeout(3):
        async with application.router.lifespan_context(application):
            await entered.wait()
            original = application.state.reaper_task
            with pytest.raises(RuntimeError, match="already running"):
                async with application.router.lifespan_context(application):
                    pytest.fail("second startup must be refused")
            assert application.state.reaper_task is original
            assert not original.done() and calls == 1


@pytest.mark.anyio
async def test_shutdown_during_interval_does_not_wait_for_next_scan(tmp_path: Path) -> None:
    """Closing an idle loop cancels its long interval and awaits its completed task."""
    application = create_app(
        FakeRuntime(), database_path=tmp_path / "lifecycle.sqlite3",
        reaper_enabled=True, reaper_interval_seconds=3600,
    )
    round_finished = asyncio.Event()

    async def one_round(*, limit) -> SandboxReapResult:
        """Signal a completed empty round before the scheduler enters its interval."""
        round_finished.set()
        return SandboxReapResult(NOW)

    application.state.service.reap_expired_once = one_round
    async with asyncio.timeout(3):
        async with application.router.lifespan_context(application):
            await round_finished.wait()
            task = application.state.reaper_task
        assert task.done()
        assert application.state.reaper_task is None


@pytest.mark.anyio
async def test_shutdown_finishes_current_candidate_and_skips_remaining_batch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Repeated shutdown cancellation waits for real Service cleanup without deleting the next row."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    first = await add_record(store, 1)
    second = await add_record(store, 2)
    runtime = ReapRuntime()
    runtime.block_delete = True
    monkeypatch.setattr(service_module, "_utc_now", lambda: NOW)
    application = create_app(runtime, store=store, reaper_enabled=True)
    context = application.router.lifespan_context(application)
    shutdown = None
    try:
        async with asyncio.timeout(3):
            await context.__aenter__()
            await runtime.delete_entered.wait()
            task = application.state.reaper_task
            shutdown = asyncio.create_task(context.__aexit__(None, None, None))
            while task.cancelling() == 0:
                await asyncio.sleep(0)
            shutdown.cancel()
            await asyncio.sleep(0)
            shutdown.cancel()
            await asyncio.sleep(0)
            assert not shutdown.done()
            assert (await store.get(first.sandbox_id)).management_state == "deleting"
            assert await store.get(second.sandbox_id) == second
            runtime.delete_release.set()
            with pytest.raises(asyncio.CancelledError):
                await shutdown
            assert task.done() and application.state.reaper_task is None
            assert runtime.deleted == {first.sandbox_id}
            assert await store.get(first.sandbox_id) is None
            assert await store.get(second.sandbox_id) == second
    finally:
        runtime.delete_release.set()
        if shutdown is not None and not shutdown.done():
            await shutdown


@pytest.mark.anyio
async def test_application_restart_recovers_expired_active_and_deleting_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh application resumes persisted deletion work without adopting pending or future rows."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    await store.initialize()
    active = await add_record(store, 1)
    deleting = await add_record(store, 2, state="deleting")
    pending = await add_record(store, 3, state="pending")
    future = await add_record(store, 4, expires_at=NOW + timedelta(hours=1))
    old_application = create_app(
        ReapRuntime(), store=store, reaper_enabled=False,
    )
    async with old_application.router.lifespan_context(old_application):
        assert await store.get(active.sandbox_id) == active
    fresh_store = SQLiteLifecycleStore(database)
    runtime = ReapRuntime()
    monkeypatch.setattr(service_module, "_utc_now", lambda: NOW)
    application = create_app(runtime, store=fresh_store, reaper_enabled=True)
    async with asyncio.timeout(3):
        async with application.router.lifespan_context(application):
            task = application.state.reaper_task
            while (
                await fresh_store.get(active.sandbox_id) is not None
                or await fresh_store.get(deleting.sandbox_id) is not None
            ):
                await asyncio.sleep(0)
            assert runtime.deleted == {active.sandbox_id, deleting.sandbox_id}
            assert all(not policy for _, policy, _ in runtime.expected_deletions)
            assert await fresh_store.get(pending.sandbox_id) == pending
            assert await fresh_store.get(future.sandbox_id) == future
        assert task.done()


@pytest.mark.anyio
async def test_same_application_can_restart_with_a_new_owned_task(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Normal shutdown releases lifecycle ownership so a later startup creates one fresh task."""
    started = asyncio.Queue()

    async def controlled_reaper(*args, **kwargs) -> None:
        """Publish the active task then wait for cancellation on each startup."""
        started.put_nowait(asyncio.current_task())
        await asyncio.Event().wait()

    monkeypatch.setattr(app_module, "run_reaper", controlled_reaper)
    application = create_app(
        FakeRuntime(), database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=True,
    )
    previous = None
    async with asyncio.timeout(3):
        for _ in range(2):
            async with application.router.lifespan_context(application):
                task = await started.get()
                assert task is not previous and application.state.reaper_task is task
            assert task.done() and application.state.reaper_task is None
            previous = task
