"""Non-blocking process ownership for one local SQLite control-plane database."""

from collections.abc import Callable
import errno
import os
from pathlib import Path
import sys
import threading


class LifecycleRunLockError(RuntimeError):
    """Report a stable runtime-lock failure without filesystem or backend details."""

    def __init__(self) -> None:
        """Set the sanitized failure shared by path, permission and handle errors."""
        super().__init__("lifecycle runtime lock operation failed")


class LifecycleRunLockBusy(LifecycleRunLockError):
    """Reject a database whose runtime lock already has an owner."""

    def __init__(self) -> None:
        """Describe contention separately from inability to access the lock file."""
        RuntimeError.__init__(self, "lifecycle database is already in use")


class LifecycleRunLockUnsupported(LifecycleRunLockError):
    """Reject environments that cannot enforce the supported operating-system lock."""

    def __init__(self) -> None:
        """Fail explicitly instead of falling back to an unprotected process."""
        RuntimeError.__init__(self, "lifecycle runtime locking is unsupported")


def _operations() -> tuple[Callable[[int], None], Callable[[int], None]]:
    """Load only the Linux/WSL or Windows primitives available on this platform."""
    try:
        if sys.platform == "linux":
            import fcntl

            def acquire(descriptor: int) -> None:
                """Try an exclusive advisory lock without waiting for its current owner."""
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)

            def release(descriptor: int) -> None:
                """Explicitly unlock the same open file description before closing it."""
                fcntl.flock(descriptor, fcntl.LOCK_UN)

            return acquire, release
        if sys.platform == "win32":
            import msvcrt

            def acquire(descriptor: int) -> None:
                """Lock a fixed byte range, including when the companion file is empty."""
                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_NBLCK, 1)

            def release(descriptor: int) -> None:
                """Unlock exactly the byte range used by the Windows acquisition."""
                os.lseek(descriptor, 0, os.SEEK_SET)
                msvcrt.locking(descriptor, msvcrt.LK_UNLCK, 1)

            return acquire, release
    except ImportError:
        raise LifecycleRunLockUnsupported() from None
    raise LifecycleRunLockUnsupported()


def _is_contention(error: OSError) -> bool:
    """Recognize lock-operation contention without treating file-open denial as busy."""
    windows_error = getattr(error, "winerror", None)
    if windows_error is not None:
        return windows_error == 33  # ERROR_LOCK_VIOLATION, rather than access denied.
    return error.errno in (errno.EACCES, errno.EAGAIN)


def _close_after_failure(descriptor: int | None) -> None:
    """Close an unsuccessful handle without exposing a second low-level exception."""
    if descriptor is not None:
        try:
            os.close(descriptor)
        except OSError:
            pass


_FORK_GUARD = threading.Lock()
# Keep every open handle registered until release, including when its original
# caller drops the object; otherwise a later fork could inherit an untracked fd.
_ACTIVE_LOCKS: set["SQLiteRunLock"] = set()


class SQLiteRunLock:
    """Own a stable companion file lock for a canonical local SQLite database path."""

    def __init__(self, database_path: str | Path) -> None:
        """Remember the database path without resolving it or accessing the filesystem."""
        self._database_path = Path(database_path)
        self._descriptor: int | None = None
        self._unlock: Callable[[int], None] | None = None

    def acquire(self) -> None:
        """Acquire once without blocking on another owner or deleting a stale lock file."""
        if self._descriptor is not None:
            return
        acquire, unlock = _operations()
        try:
            database = self._database_path.resolve(strict=False)
            database.parent.mkdir(parents=True, exist_ok=True)
            lock_path = database.with_name(database.name + ".lock")
        except (OSError, RuntimeError, ValueError):
            raise LifecycleRunLockError() from None

        # Serializing descriptor registration with fork prevents a child from
        # retaining a handle opened by another thread just before registration.
        with _FORK_GUARD:
            if self._descriptor is not None:
                return
            descriptor = None
            try:
                descriptor = os.open(
                    lock_path, os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0), 0o600,
                )
                os.set_inheritable(descriptor, False)
            except (OSError, ValueError):
                _close_after_failure(descriptor)
                raise LifecycleRunLockError() from None
            try:
                acquire(descriptor)
            except OSError as error:
                _close_after_failure(descriptor)
                if _is_contention(error):
                    raise LifecycleRunLockBusy() from None
                if error.errno in (errno.ENOSYS, errno.ENOTSUP):
                    raise LifecycleRunLockUnsupported() from None
                raise LifecycleRunLockError() from None
            self._descriptor = descriptor
            self._unlock = unlock
            _ACTIVE_LOCKS.add(self)

    def release(self) -> None:
        """Explicitly unlock and close a held handle; repeated release is harmless."""
        with _FORK_GUARD:
            descriptor, unlock = self._descriptor, self._unlock
            self._descriptor = self._unlock = None
            _ACTIVE_LOCKS.discard(self)
            if descriptor is None:
                return
            failed = False
            try:
                if unlock is not None:
                    unlock(descriptor)
            except OSError:
                failed = True
            finally:
                try:
                    os.close(descriptor)
                except OSError:
                    failed = True
            if failed:
                raise LifecycleRunLockError() from None


def _close_inherited_locks() -> None:
    """Close fork-inherited handles without unlocking their parent's file description."""
    try:
        for lock in tuple(_ACTIVE_LOCKS):
            descriptor = lock._descriptor
            lock._descriptor = lock._unlock = None
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
        _ACTIVE_LOCKS.clear()
    finally:
        _FORK_GUARD.release()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(
        before=_FORK_GUARD.acquire,
        after_in_parent=_FORK_GUARD.release,
        after_in_child=_close_inherited_locks,
    )
