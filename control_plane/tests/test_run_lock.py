"""Process-level SQLite runtime locking without database or Docker operations."""

import errno
import gc
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
from types import SimpleNamespace
from weakref import ref

import pytest

from control_plane.storage import run_lock
from control_plane.storage.run_lock import (
    LifecycleRunLockBusy,
    LifecycleRunLockError,
    LifecycleRunLockUnsupported,
    SQLiteRunLock,
)


_ROOT = Path(__file__).resolve().parents[2]
_HOLDER = """
import pathlib
import sys
from control_plane.storage.run_lock import SQLiteRunLock
lock = SQLiteRunLock(sys.argv[1])
lock.acquire()
pathlib.Path(sys.argv[2]).write_text('ready', encoding='utf-8')
try:
    sys.stdin.buffer.read(1)
finally:
    lock.release()
"""
_ATTEMPT = """
import sys
from control_plane.storage.run_lock import SQLiteRunLock, LifecycleRunLockBusy
lock = SQLiteRunLock(sys.argv[1])
try:
    lock.acquire()
except LifecycleRunLockBusy:
    raise SystemExit(23)
else:
    lock.release()
"""
_UNRELATED = """
import os
import pathlib
import sys
try:
    os.fstat(int(sys.argv[2]))
except OSError:
    state = 'ready'
else:
    state = 'inherited'
pathlib.Path(sys.argv[1]).write_text(state, encoding='utf-8')
sys.stdin.buffer.read(1)
"""


def _attempt(database: Path) -> subprocess.CompletedProcess[str]:
    """Try ownership from an independent interpreter with a bounded lifetime."""
    return subprocess.run(
        [sys.executable, "-c", _ATTEMPT, str(database)],
        cwd=_ROOT, capture_output=True, text=True, timeout=10,
    )


def _ready(process: subprocess.Popen[str], marker: Path) -> None:
    """Bound child startup so a failed interpreter cannot hang its parent test."""
    deadline = time.monotonic() + 10
    while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert marker.exists(), "child did not reach its readiness marker"
    assert process.poll() is None


def _finish(process: subprocess.Popen[str], *, terminate: bool = False) -> None:
    """Release or terminate only this test's child and always reap it promptly."""
    if process.poll() is not None:
        process.communicate(timeout=10)
        return
    if terminate:
        process.terminate()
    try:
        process.communicate(input=None if terminate else "x", timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate(timeout=10)
        raise


def _finish_fork(child: int, writer: int) -> bool:
    """Reap this test's fork child even when it has already closed its input pipe."""
    import signal

    try:
        os.write(writer, b"x")
    except OSError:
        pass
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        finished, _status = os.waitpid(child, os.WNOHANG)
        if finished:
            return False
        time.sleep(0.01)
    os.kill(child, signal.SIGKILL)
    os.waitpid(child, 0)
    return True


def test_construction_performs_no_filesystem_operations(tmp_path: Path, monkeypatch) -> None:
    """Lock identity is resolved only when startup explicitly acquires ownership."""
    def forbidden(*_args, **_kwargs):
        """Fail any eager lock-file or canonical-path operation."""
        raise AssertionError("unexpected filesystem access")

    with monkeypatch.context() as patch:
        patch.setattr(Path, "resolve", forbidden)
        patch.setattr(Path, "mkdir", forbidden)
        patch.setattr(run_lock.os, "open", forbidden)
        lock = SQLiteRunLock(tmp_path / "missing" / "lifecycle.sqlite3")
        lock.release()
    assert not (tmp_path / "missing").exists()


def test_independent_objects_compete_and_release_is_repeatable(tmp_path: Path) -> None:
    """One process still cannot acquire the same database through another object."""
    database = tmp_path / "nested" / "lifecycle.sqlite3"
    first, second = SQLiteRunLock(database), SQLiteRunLock(database)
    try:
        first.acquire()
        first.acquire()
        with pytest.raises(LifecycleRunLockBusy, match="already in use"):
            second.acquire()
        first.release()
        first.release()
        second.acquire()
    finally:
        first.release()
        second.release()
    assert database.with_name(database.name + ".lock").is_file()
    assert not database.exists()


def test_different_databases_do_not_contend(tmp_path: Path) -> None:
    """Ownership is per database rather than one global process lock."""
    first, second = SQLiteRunLock(tmp_path / "one.sqlite3"), SQLiteRunLock(tmp_path / "two.sqlite3")
    try:
        first.acquire()
        second.acquire()
    finally:
        first.release()
        second.release()


def test_existing_unheld_lock_file_can_be_acquired(tmp_path: Path) -> None:
    """Stale file contents and existence never substitute for an OS lock."""
    database = tmp_path / "lifecycle.sqlite3"
    path = database.with_name(database.name + ".lock")
    path.write_text("a stale pid", encoding="utf-8")
    lock = SQLiteRunLock(database)
    try:
        lock.acquire()
    finally:
        lock.release()
    assert path.exists()


def test_relative_and_canonical_paths_contend(tmp_path: Path, monkeypatch) -> None:
    """Equivalent relative paths identify the same adjacent lock file."""
    monkeypatch.chdir(tmp_path)
    first = SQLiteRunLock(tmp_path / "lifecycle.sqlite3")
    second = SQLiteRunLock(Path("directory") / ".." / "lifecycle.sqlite3")
    try:
        first.acquire()
        with pytest.raises(LifecycleRunLockBusy):
            second.acquire()
    finally:
        first.release()
        second.release()


def test_symlinked_database_paths_contend(tmp_path: Path) -> None:
    """A resolvable local symlink cannot create a second owner for one database."""
    target = tmp_path / "actual.sqlite3"
    target.touch()
    alias = tmp_path / "alias.sqlite3"
    try:
        alias.symlink_to(target)
    except OSError:
        pytest.skip("this account cannot create filesystem symlinks")
    first, second = SQLiteRunLock(target), SQLiteRunLock(alias)
    try:
        first.acquire()
        with pytest.raises(LifecycleRunLockBusy):
            second.acquire()
    finally:
        first.release()
        second.release()


def test_independent_process_competes_with_current_owner(tmp_path: Path) -> None:
    """A separate interpreter observes contention and can acquire after release."""
    database = tmp_path / "lifecycle.sqlite3"
    lock = SQLiteRunLock(database)
    try:
        lock.acquire()
        assert _attempt(database).returncode == 23
    finally:
        lock.release()
    assert _attempt(database).returncode == 0


@pytest.mark.parametrize("terminate", [False, True])
def test_child_exit_releases_operating_system_ownership(tmp_path: Path, terminate: bool) -> None:
    """Normal release and abrupt termination both permit another process to start."""
    database, marker = tmp_path / "lifecycle.sqlite3", tmp_path / "ready"
    process = subprocess.Popen(
        [sys.executable, "-c", _HOLDER, str(database), str(marker)],
        cwd=_ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _ready(process, marker)
        assert _attempt(database).returncode == 23
        _finish(process, terminate=terminate)
        assert _attempt(database).returncode == 0
    finally:
        _finish(process, terminate=True)


def test_unrelated_exec_does_not_inherit_lock_handle(tmp_path: Path) -> None:
    """Even close_fds=False cannot keep a released parent's ownership alive."""
    database, marker = tmp_path / "lifecycle.sqlite3", tmp_path / "ready"
    lock = SQLiteRunLock(database)
    process = None
    try:
        lock.acquire()
        assert not os.get_inheritable(lock._descriptor)
        process = subprocess.Popen(
            [sys.executable, "-c", _UNRELATED, str(marker), str(lock._descriptor)],
            cwd=_ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            close_fds=False, text=True,
        )
        _ready(process, marker)
        assert marker.read_text(encoding="utf-8") == "ready"
        lock.release()
        assert process.poll() is None
        assert _attempt(database).returncode == 0
    finally:
        lock.release()
        if process is not None:
            _finish(process, terminate=True)


@pytest.mark.skipif(not hasattr(os, "fork"), reason="raw fork is unavailable on this platform")
@pytest.mark.parametrize("drop_reference", [False, True])
@pytest.mark.parametrize("child_exits_early", [False, True])
def test_raw_fork_child_closes_inherited_handle_without_unlocking_parent(
    tmp_path: Path, drop_reference: bool, child_exits_early: bool,
) -> None:
    """Every open lock handle remains tracked even if its caller drops its reference."""
    import select

    database = tmp_path / "lifecycle.sqlite3"
    lock = SQLiteRunLock(database)
    lock_reference = ref(lock)
    result_reader, result_writer = os.pipe()
    finish_reader, finish_writer = os.pipe()
    child = None
    try:
        lock.acquire()
        descriptor = lock._descriptor
        if drop_reference:
            del lock
            gc.collect()
            assert lock_reference() is not None
        child = os.fork()
        if child == 0:
            try:
                os.close(result_reader)
                os.close(finish_writer)
                try:
                    os.fstat(descriptor)
                except OSError as error:
                    status = b"closed" if error.errno == errno.EBADF else b"error"
                else:
                    status = b"inherited"
                os.write(result_writer, status)
                if not child_exits_early:
                    os.read(finish_reader, 1)
            finally:
                os._exit(0)
        os.close(result_writer)
        result_writer = None
        os.close(finish_reader)
        finish_reader = None
        ready, _, _ = select.select([result_reader], [], [], 10)
        assert ready, "fork child did not report descriptor ownership"
        assert os.read(result_reader, 16) == b"closed"
        assert _attempt(database).returncode == 23
        lock_reference().release()
        assert _attempt(database).returncode == 0
    finally:
        timed_out = False
        try:
            if lock_reference() is not None:
                lock_reference().release()
        finally:
            try:
                if child:
                    timed_out = _finish_fork(child, finish_writer)
            finally:
                for descriptor in (result_reader, result_writer, finish_reader, finish_writer):
                    if descriptor is not None:
                        try:
                            os.close(descriptor)
                        except OSError:
                            pass
        assert not timed_out, "fork child did not finish"


def test_release_removes_the_active_ownership_reference(tmp_path: Path) -> None:
    """An explicitly released lock is not retained by descriptor bookkeeping."""
    database = tmp_path / "lifecycle.sqlite3"
    lock = SQLiteRunLock(database)
    reference = ref(lock)
    try:
        lock.acquire()
    finally:
        lock.release()
    del lock
    gc.collect()
    assert reference() is None
    assert _attempt(database).returncode == 0


@pytest.mark.parametrize("operation", ["resolve", "mkdir", "open"])
def test_filesystem_errors_are_sanitized_and_not_contention(
    tmp_path: Path, monkeypatch, operation: str,
) -> None:
    """Permission and path errors do not falsely report an already running instance."""
    lock = SQLiteRunLock(tmp_path / "private.sqlite3")

    def denied(*_args, **_kwargs):
        """Represent a backend error whose message must stay private."""
        raise PermissionError(errno.EACCES, "private backend detail", "/private/credential")

    with monkeypatch.context() as patch:
        if operation == "open":
            patch.setattr(run_lock.os, "open", denied)
        else:
            patch.setattr(Path, operation, denied)
        with pytest.raises(LifecycleRunLockError) as captured:
            lock.acquire()
    assert type(captured.value) is LifecycleRunLockError
    rendered = "".join(traceback.format_exception(captured.value))
    assert "private backend detail" not in rendered
    assert "/private/credential" not in rendered
    lock.release()


def test_locking_failure_closes_descriptor_and_allows_retry(tmp_path: Path, monkeypatch) -> None:
    """A non-contention kernel failure is stable and does not leak a failed handle."""
    database = tmp_path / "lifecycle.sqlite3"
    lock = SQLiteRunLock(database)
    closed = []
    original_close = os.close

    def fail(_descriptor: int) -> None:
        """Represent a lock operation unavailable for a particular descriptor."""
        raise OSError(errno.EBADF, "private descriptor error")

    def close(descriptor: int) -> None:
        """Observe cleanup without changing OS descriptor semantics."""
        closed.append(descriptor)
        original_close(descriptor)

    with monkeypatch.context() as patch:
        patch.setattr(run_lock, "_operations", lambda: (fail, fail))
        patch.setattr(run_lock.os, "close", close)
        with pytest.raises(LifecycleRunLockError) as captured:
            lock.acquire()
    assert type(captured.value) is LifecycleRunLockError
    assert len(closed) == 1
    try:
        lock.acquire()
    finally:
        lock.release()


@pytest.mark.parametrize("stage", ["inheritance", "lock"])
def test_failed_acquisition_cleanup_never_exposes_close_errors(
    tmp_path: Path, monkeypatch, stage: str,
) -> None:
    """Closing an unsuccessful handle preserves stable errors even if close also fails."""
    lock = SQLiteRunLock(tmp_path / "lifecycle.sqlite3")
    original_close = os.close
    closed = []

    def fail(*_args) -> None:
        """Represent either an inheritance setup failure or ordinary contention."""
        raise OSError(errno.EACCES, "private acquisition detail")

    def close_then_fail(descriptor: int) -> None:
        """Actually close the descriptor before simulating a private cleanup failure."""
        original_close(descriptor)
        closed.append(descriptor)
        raise OSError(errno.EIO, "private cleanup detail")

    with monkeypatch.context() as patch:
        patch.setattr(run_lock.os, "close", close_then_fail)
        if stage == "inheritance":
            patch.setattr(run_lock.os, "set_inheritable", fail)
            expected = LifecycleRunLockError
        else:
            patch.setattr(run_lock, "_operations", lambda: (fail, fail))
            expected = LifecycleRunLockBusy
        with pytest.raises(expected) as captured:
            lock.acquire()
    assert type(captured.value) is expected
    assert len(closed) == 1
    with pytest.raises(OSError) as invalid:
        os.fstat(closed[0])
    assert invalid.value.errno == errno.EBADF
    rendered = "".join(traceback.format_exception(captured.value))
    assert "private acquisition detail" not in rendered
    assert "private cleanup detail" not in rendered
    lock.release()


@pytest.mark.parametrize("windows_error, expected", [
    (5, LifecycleRunLockError), (33, LifecycleRunLockBusy),
])
def test_windows_access_denied_is_distinct_from_lock_violation(
    tmp_path: Path, monkeypatch, windows_error: int, expected: type[LifecycleRunLockError],
) -> None:
    """A Windows access error is not mislabeled as another application's lock."""
    def fail(_descriptor: int) -> None:
        """Raise a synthetic Windows result without requiring a native Windows host."""
        error = OSError(errno.EACCES, "private Windows detail")
        error.winerror = windows_error
        raise error

    monkeypatch.setattr(run_lock, "_operations", lambda: (fail, fail))
    lock = SQLiteRunLock(tmp_path / "lifecycle.sqlite3")
    with pytest.raises(expected) as captured:
        lock.acquire()
    assert type(captured.value) is expected
    assert "private Windows detail" not in "".join(traceback.format_exception(captured.value))
    lock.release()


def test_unlock_failure_still_closes_handle_and_can_be_retried(tmp_path: Path, monkeypatch) -> None:
    """A failed explicit unlock cannot skip descriptor close or leak raw backend text."""
    database = tmp_path / "lifecycle.sqlite3"
    acquire, _unlock = run_lock._operations()

    def fail(_descriptor: int) -> None:
        """Represent an unlock failure whose fallback close must release ownership."""
        raise OSError(errno.EIO, "private unlock detail")

    lock = SQLiteRunLock(database)
    with monkeypatch.context() as patch:
        patch.setattr(run_lock, "_operations", lambda: (acquire, fail))
        lock.acquire()
    with pytest.raises(LifecycleRunLockError) as captured:
        lock.release()
    assert "private unlock detail" not in "".join(traceback.format_exception(captured.value))
    lock.release()
    assert _attempt(database).returncode == 0


def test_unsupported_platform_fails_before_filesystem_io(tmp_path: Path, monkeypatch) -> None:
    """Unsupported operating systems never silently continue without ownership."""
    lock = SQLiteRunLock(tmp_path / "missing" / "lifecycle.sqlite3")
    monkeypatch.setattr(run_lock.sys, "platform", "unsupported-platform")
    with pytest.raises(LifecycleRunLockUnsupported):
        lock.acquire()
    assert not (tmp_path / "missing").exists()


def test_windows_backend_locks_one_byte_at_fixed_offset(tmp_path: Path, monkeypatch) -> None:
    """Exercise the Windows adapter contract without claiming native Windows validation."""
    calls = []

    def locking(descriptor: int, operation: int, length: int) -> None:
        """Observe the fixed byte range used by the Windows CRT adapter."""
        calls.append((operation, length, os.lseek(descriptor, 0, os.SEEK_CUR)))

    fake = SimpleNamespace(LK_NBLCK=2, LK_UNLCK=0, locking=locking)
    monkeypatch.setitem(sys.modules, "msvcrt", fake)
    monkeypatch.setattr(run_lock.sys, "platform", "win32")
    lock = SQLiteRunLock(tmp_path / "lifecycle.sqlite3")
    try:
        lock.acquire()
    finally:
        lock.release()
    assert calls == [(2, 1, 0), (0, 1, 0)]
