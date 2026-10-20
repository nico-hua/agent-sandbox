"""SQLite application exclusivity across startup, cancellation and safe shutdown."""

import asyncio
import subprocess
import sys
import threading
from pathlib import Path

import pytest

import control_plane.app as app_module
from control_plane.app import create_app
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore
from control_plane.storage.run_lock import LifecycleRunLockBusy, LifecycleRunLockError
from lifecycle_fakes import FakeLifecycleStore
from test_app import FakeRuntime


class ObservedStore(SQLiteLifecycleStore):
    """Retain real SQLite behavior while recording startup attempts."""

    def __init__(self, path: Path, *, fail: bool = False) -> None:
        """Keep initialization counters and an optional stable failure without I/O."""
        super().__init__(path)
        self.initializations = 0
        self.fail = fail

    async def initialize(self) -> None:
        """Count startup and optionally fail before creating the database."""
        self.initializations += 1
        if self.fail:
            raise LifecycleStoreError()
        await super().initialize()


class GatedInitializationStore(ObservedStore):
    """Hold an actual SQLite worker so startup cancellation cannot imply completion."""

    def __init__(self, path: Path, *, fail_after_release: bool = False) -> None:
        """Create thread coordination without starting a thread or opening files."""
        super().__init__(path)
        self.fail_after_release = fail_after_release
        self.entered = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()

    def _initialize_sync(self) -> None:
        """Finish real initialization only after the test permits the worker to proceed."""
        self.entered.set()
        try:
            if not self.release.wait(timeout=5):
                raise AssertionError("test did not release initialization")
            super()._initialize_sync()
            if self.fail_after_release:
                raise LifecycleStoreError()
        finally:
            self.finished.set()


@pytest.mark.anyio
async def test_distinct_apps_using_same_database_cannot_start_together(tmp_path: Path) -> None:
    """An injected SQLite Store cannot evade another application's database lock."""
    path = tmp_path / "metadata" / "lifecycle.sqlite3"
    first = create_app(FakeRuntime(), database_path=path, reaper_enabled=False)
    second_store = ObservedStore(path)
    second_runtime = FakeRuntime()
    second = create_app(second_runtime, store=second_store, reaper_enabled=True)

    async with first.router.lifespan_context(first):
        with pytest.raises(LifecycleRunLockBusy) as error:
            async with second.router.lifespan_context(second):
                pytest.fail("a competing application must never start serving")
        assert str(path) not in str(error.value)
        assert second_store.initializations == 0
        assert second_runtime.calls == 0
        assert second.state.reaper_task is None
        assert await first.state.lifecycle_store.get("missing") is None

    async with second.router.lifespan_context(second):
        assert second_store.initializations == 1


@pytest.mark.anyio
async def test_independent_databases_can_start_together(tmp_path: Path) -> None:
    """Exclusivity belongs to one database rather than all applications in a process."""
    first = create_app(
        FakeRuntime(), database_path=tmp_path / "first.sqlite3", reaper_enabled=False,
    )
    second = create_app(
        FakeRuntime(), database_path=tmp_path / "second.sqlite3", reaper_enabled=False,
    )

    async with first.router.lifespan_context(first):
        async with second.router.lifespan_context(second):
            assert await first.state.lifecycle_store.get("missing") is None
            assert await second.state.lifecycle_store.get("missing") is None


@pytest.mark.anyio
async def test_lock_identity_comes_from_injected_store_not_unused_app_path(tmp_path: Path) -> None:
    """An unused database_path argument cannot choose an unrelated lock for a real Store."""
    database = tmp_path / "actual.sqlite3"
    unused = tmp_path / "unused" / "ignored.sqlite3"
    first = create_app(FakeRuntime(), database_path=database, reaper_enabled=False)
    second = create_app(
        FakeRuntime(), store=ObservedStore(database), database_path=unused,
        reaper_enabled=False,
    )

    async with first.router.lifespan_context(first):
        with pytest.raises(LifecycleRunLockBusy):
            async with second.router.lifespan_context(second):
                pytest.fail("the Store's actual database is already in use")
    assert not unused.parent.exists()


@pytest.mark.anyio
async def test_relative_path_alias_uses_the_same_lock(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Relative and normalized absolute paths cannot open overlapping application lifespans."""
    monkeypatch.chdir(tmp_path)
    database = tmp_path / "metadata.sqlite3"
    first = create_app(FakeRuntime(), database_path=database, reaper_enabled=False)
    second = create_app(FakeRuntime(), database_path=Path("metadata.sqlite3"), reaper_enabled=False)

    async with first.router.lifespan_context(first):
        with pytest.raises(LifecycleRunLockBusy):
            async with second.router.lifespan_context(second):
                pytest.fail("path aliases must share their database lock")


@pytest.mark.anyio
async def test_symlinked_database_parent_uses_the_same_lock(tmp_path: Path) -> None:
    """Canonical database parents prevent symlink paths from obtaining separate locks."""
    original = tmp_path / "original"
    original.mkdir()
    alias = tmp_path / "alias"
    try:
        alias.symlink_to(original, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("directory symlinks are unavailable in this test environment")
    first = create_app(
        FakeRuntime(), database_path=original / "metadata.sqlite3", reaper_enabled=False,
    )
    second = create_app(
        FakeRuntime(), database_path=alias / "metadata.sqlite3", reaper_enabled=False,
    )

    async with first.router.lifespan_context(first):
        with pytest.raises(LifecycleRunLockBusy):
            async with second.router.lifespan_context(second):
                pytest.fail("symlink aliases must share their database lock")


@pytest.mark.anyio
async def test_initialization_failure_releases_lock_for_later_startup(tmp_path: Path) -> None:
    """Startup failure releases ownership without deleting its companion lock file."""
    database = tmp_path / "new" / "lifecycle.sqlite3"
    store = ObservedStore(database, fail=True)
    application = create_app(FakeRuntime(), store=store, reaper_enabled=False)

    with pytest.raises(LifecycleStoreError):
        async with application.router.lifespan_context(application):
            pytest.fail("failed initialization must abort startup")

    assert not database.exists()
    assert database.with_name(database.name + ".lock").is_file()
    store.fail = False
    async with application.router.lifespan_context(application):
        assert database.is_file()
    assert store.initializations == 2


@pytest.mark.anyio
async def test_lock_file_failure_aborts_before_database_or_background_work(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operational lock failures escape startup without masquerading as contention."""
    database = tmp_path / "metadata.sqlite3"
    store = ObservedStore(database)
    runtime = FakeRuntime()

    class FailingLock:
        """Simulate stable file access failure before any database initialization."""

        def __init__(self, path: Path) -> None:
            """Keep construction lazy while observing the actual Store path."""
            assert path == store.database_path

        def acquire(self) -> None:
            """Raise a stable operation failure rather than a lock-busy response."""
            raise LifecycleRunLockError()

        def release(self) -> None:
            """Permit safe cleanup even though lock ownership was never acquired."""

    monkeypatch.setattr(app_module, "SQLiteRunLock", FailingLock)
    application = create_app(runtime, store=store, reaper_enabled=True)
    with pytest.raises(LifecycleRunLockError) as error:
        async with application.router.lifespan_context(application):
            pytest.fail("lock access failure must abort startup")

    assert not isinstance(error.value, LifecycleRunLockBusy)
    assert store.initializations == 0
    assert runtime.calls == 0
    assert application.state.reaper_task is None
    assert not database.exists()


@pytest.mark.anyio
@pytest.mark.parametrize("fail_after_release", [False, True])
async def test_cancelled_initialization_keeps_lock_until_sqlite_worker_finishes(
    tmp_path: Path, fail_after_release: bool,
) -> None:
    """First and repeated cancellation cannot unlock while a started worker can still write."""
    database = tmp_path / "metadata.sqlite3"
    store = GatedInitializationStore(database, fail_after_release=fail_after_release)
    application = create_app(FakeRuntime(), store=store, reaper_enabled=False)
    contender_store = ObservedStore(database)
    contender = create_app(FakeRuntime(), store=contender_store, reaper_enabled=False)
    context = application.router.lifespan_context(application)
    startup = asyncio.create_task(context.__aenter__())
    try:
        async with asyncio.timeout(5):
            assert await asyncio.to_thread(store.entered.wait, 3)
            startup.cancel()
            await asyncio.sleep(0)
            startup.cancel()
            await asyncio.sleep(0)
            assert not startup.done()
            assert not store.finished.is_set()
            with pytest.raises(LifecycleRunLockBusy):
                async with contender.router.lifespan_context(contender):
                    pytest.fail("initialization is still running under the first lock")
            assert contender_store.initializations == 0
            store.release.set()
            expected_error = LifecycleStoreError if fail_after_release else asyncio.CancelledError
            with pytest.raises(expected_error):
                await startup
            assert store.finished.is_set()
            assert database.is_file()
            assert application.state.reaper_task is None
            async with contender.router.lifespan_context(contender):
                assert contender_store.initializations == 1
    finally:
        store.release.set()
        if not startup.done():
            await asyncio.gather(startup, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("cancel_shutdown", [False, True])
async def test_lock_remains_held_while_reaper_finishes_shutdown(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, cancel_shutdown: bool,
) -> None:
    """Reaper cancellation must finish before another application can own the database."""
    entered = asyncio.Event()
    finishing = asyncio.Event()
    release = asyncio.Event()

    async def gated_reaper(*args, **kwargs) -> None:
        """Model a Service candidate that requires cancellation-safe backend cleanup."""
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            finishing.set()
            await release.wait()
            raise

    monkeypatch.setattr(app_module, "run_reaper", gated_reaper)
    database = tmp_path / "metadata.sqlite3"
    application = create_app(FakeRuntime(), database_path=database, reaper_enabled=True)
    contender_store = ObservedStore(database)
    contender = create_app(FakeRuntime(), store=contender_store, reaper_enabled=False)
    context = application.router.lifespan_context(application)
    shutdown = None
    try:
        async with asyncio.timeout(5):
            await context.__aenter__()
            await entered.wait()
            reaper_task = application.state.reaper_task
            shutdown = asyncio.create_task(context.__aexit__(None, None, None))
            await finishing.wait()
            if cancel_shutdown:
                shutdown.cancel()
                await asyncio.sleep(0)
                shutdown.cancel()
                await asyncio.sleep(0)
            assert not shutdown.done()
            with pytest.raises(LifecycleRunLockBusy):
                async with contender.router.lifespan_context(contender):
                    pytest.fail("reaper cleanup still requires exclusive ownership")
            assert contender_store.initializations == 0
            release.set()
            if cancel_shutdown:
                with pytest.raises(asyncio.CancelledError):
                    await shutdown
            else:
                await shutdown
            assert reaper_task.done()
            assert application.state.reaper_task is None
            async with contender.router.lifespan_context(contender):
                assert contender_store.initializations == 1
    finally:
        release.set()
        if shutdown is not None and not shutdown.done():
            await asyncio.gather(shutdown, return_exceptions=True)


@pytest.mark.anyio
async def test_exceptional_shutdown_and_repeated_close_do_not_retain_lock(tmp_path: Path) -> None:
    """An application-body exception and an already-complete close leave startup reusable."""
    database = tmp_path / "metadata.sqlite3"
    application = create_app(FakeRuntime(), database_path=database, reaper_enabled=False)
    context = application.router.lifespan_context(application)

    with pytest.raises(RuntimeError, match="request cleanup failed"):
        async with context:
            raise RuntimeError("request cleanup failed")
    await context.__aexit__(None, None, None)

    async with application.router.lifespan_context(application):
        assert await application.state.lifecycle_store.get("missing") is None


@pytest.mark.anyio
async def test_non_sqlite_injected_store_keeps_its_existing_lifespan() -> None:
    """An explicit fake Store remains usable without inventing a filesystem lock identity."""
    store = FakeLifecycleStore()
    application = create_app(FakeRuntime(), store=store, reaper_enabled=False)

    async with application.router.lifespan_context(application):
        assert store.initialize_calls == 1


@pytest.mark.anyio
async def test_independent_process_app_cannot_start_until_first_app_closes(tmp_path: Path) -> None:
    """A separate Python application obeys the lock before initialization and may retry later."""
    database = tmp_path / "metadata.sqlite3"
    application = create_app(FakeRuntime(), database_path=database, reaper_enabled=False)
    script = '''
import asyncio
import sys
from control_plane.app import create_app
from control_plane.storage.run_lock import LifecycleRunLockBusy

async def main():
    """Try one real application lifespan without running Docker or a background reaper."""
    application = create_app(database_path=sys.argv[1], reaper_enabled=False)
    try:
        async with application.router.lifespan_context(application):
            print("STARTED")
    except LifecycleRunLockBusy:
        print("BUSY")
        return 7
    return 0

raise SystemExit(asyncio.run(main()))
'''

    async def attempt() -> tuple[int, str, str]:
        """Bound process execution and forcibly clean only this test's child on failure."""
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", script, str(database),
            cwd=Path(__file__).resolve().parents[2],
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
            return process.returncode, stdout.decode(), stderr.decode()
        finally:
            if process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=5)

    async with application.router.lifespan_context(application):
        returncode, stdout, stderr = await attempt()
        assert returncode == 7, stderr
        assert stdout.strip() == "BUSY"
    returncode, stdout, stderr = await attempt()
    assert returncode == 0, stderr
    assert stdout.strip() == "STARTED"


def test_import_and_app_assembly_do_not_access_lock_files() -> None:
    """Fresh process imports and SQLite app assembly may not open or stat lock paths."""
    script = '''
import builtins
import io
import os
from pathlib import Path

original_open = builtins.open
original_io_open = io.open
original_os_open = os.open
original_stat = os.stat

def reject_lock_path(path):
    """Reject companion-lock filesystem work before lifespan begins."""
    if isinstance(path, (str, bytes, os.PathLike)) and str(path).endswith(".lock"):
        raise AssertionError("lock filesystem I/O during application assembly")

def checked_open(path, *args, **kwargs):
    """Allow normal imports while forbidding lockfile builtins access."""
    reject_lock_path(path)
    return original_open(path, *args, **kwargs)

def checked_io_open(path, *args, **kwargs):
    """Apply the lockfile guard to pathlib's open implementation."""
    reject_lock_path(path)
    return original_io_open(path, *args, **kwargs)

def checked_os_open(path, *args, **kwargs):
    """Apply the lockfile guard to descriptor-level open operations."""
    reject_lock_path(path)
    return original_os_open(path, *args, **kwargs)

def checked_stat(path, *args, **kwargs):
    """Detect lockfile existence checks as well as reads or writes."""
    reject_lock_path(path)
    return original_stat(path, *args, **kwargs)

builtins.open = checked_open
io.open = checked_io_open
os.open = checked_os_open
os.stat = checked_stat
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from control_plane.app import app, create_app
store = SQLiteLifecycleStore("lock-assembly-must-remain-lazy.sqlite3")
create_app(store=store, reaper_enabled=False)
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True, text=True, timeout=10,
    )

    assert result.returncode == 0, result.stderr
