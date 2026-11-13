"""Application-owned, bounded local forwarding sessions with per-instance deletion coordination."""

import asyncio
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from urllib.parse import urlsplit

from control_plane.core.tasks import settle_task, wait_for_task
from control_plane.runtime.kubernetes.local_entry import KubernetesLocalEntry, LocalEntryError
from control_plane.runtime.kubernetes.local_entry_target import LocalEntryTarget, resolve_local_entry_target
from control_plane.runtime.kubernetes.pod_query import sandbox_pod_name


@dataclass(frozen=True)
class LocalEntryConnection:
    """Expose only a verified origin and immutable nonplaintext authentication/identity facts."""

    url: str
    target: LocalEntryTarget


@dataclass
class _Entry:
    """Retain ownership from reservation through confirmed process and startup-task cleanup."""

    sandbox_id: str
    task: asyncio.Task | None = None
    session: object | None = None
    target: LocalEntryTarget | None = None
    expected_target: LocalEntryTarget | None = None
    closing: bool = False
    ready: bool = False
    cleanup: asyncio.Task | None = None


def _consume(task):
    """Retrieve background failures even when all request waiters have been cancelled."""
    if not task.cancelled():
        task.exception()


def _transaction(value):
    """Reject invalid optional expected identities without interpreting them as resource names."""
    if value is not None and (type(value) is not str or re.fullmatch(r"txn_[0-9a-f]{32}", value) is None):
        raise LocalEntryError("local_entry_identity_conflict")


class KubernetesLocalEntryManager:
    """Own sessions in one application/event loop; never reconnect or admit beyond the bounded registry."""

    def __init__(self, runtime, *, max_sessions: int = 8, session_factory=None, target_resolver=None):
        """Freeze inert dependencies without clients, processes, files or background tasks."""
        if type(max_sessions) is not int or not 1 <= max_sessions <= 32:
            raise ValueError("max_sessions must be an integer between 1 and 32")
        self.runtime, self.max_sessions = runtime, max_sessions
        self._factory = KubernetesLocalEntry if session_factory is None else session_factory
        self._resolver = resolve_local_entry_target if target_resolver is None else target_resolver
        self._entries, self._blocked = {}, set()
        self._stopping, self._shutdown = False, None
        self._preparing = set()
        self._application_owner = None

    def claim_application(self, owner) -> None:
        """Give one application exclusive lifetime ownership without filesystem or resource operations."""
        if self._application_owner is not None and self._application_owner is not owner:
            raise ValueError("local entry manager already belongs to another application")
        self._application_owner = owner

    @property
    def session_count(self) -> int:
        """Count all reservations, including opening and unconfirmed cleanup."""
        return len(self._entries)

    @property
    def is_closed(self) -> bool:
        """Report shutdown complete only once every owned reservation has been safely released."""
        return self._stopping and not self._entries and not self._preparing

    @property
    def is_stopping(self) -> bool:
        """Let in-flight creation stop read-only waiting once its application starts shutdown."""
        return self._stopping

    @asynccontextmanager
    async def creation_scope(self):
        """Retain application ownership of creation preparation and compensation until they settle."""
        if self._stopping:
            raise LocalEntryError("local_entry_stopping")
        owner = asyncio.current_task()
        self._preparing.add(owner)
        try:
            yield
        finally:
            self._preparing.discard(owner)

    async def _resolve(self, sandbox_id):
        """Recheck injected snapshots and bind them to this manager's explicit namespace and public ID."""
        target = await self._resolver(self.runtime, sandbox_id)
        if not isinstance(target, LocalEntryTarget):
            raise LocalEntryError("local_entry_unavailable")
        target.__post_init__()
        if target.sandbox_id != sandbox_id or target.namespace != self.runtime._namespace:
            raise LocalEntryError("local_entry_identity_conflict")
        return target

    def _connection(self, entry):
        """Check live ownership and strict loopback origin after any asynchronous identity verification."""
        if (self._stopping or entry.closing or entry.sandbox_id in self._blocked
            or self._entries.get(entry.sandbox_id) is not entry or not entry.ready
            or entry.session is None or not entry.session.is_active or entry.target is None):
            raise LocalEntryError("local_entry_unavailable")
        try:
            url = entry.session.url
            address = urlsplit(url)
            if (type(url) is not str or address.scheme != "http" or address.hostname != "127.0.0.1"
                or address.username is not None or address.password is not None or address.path
                or address.query or address.fragment or address.port is None or not 1 <= address.port <= 65535
                or url != f"http://127.0.0.1:{address.port}"):
                raise ValueError()
        except (AttributeError, TypeError, ValueError):
            raise LocalEntryError("local_entry_unavailable") from None
        return LocalEntryConnection(url, entry.target)

    async def _start(self, entry):
        """Resolve before constructing one owned session and retain cleanup failures instead of freeing its slot."""
        try:
            entry.target = await self._resolve(entry.sandbox_id)
            if entry.expected_target is not None and entry.target != entry.expected_target:
                raise LocalEntryError("local_entry_target_changed")
            if self._stopping or entry.closing:
                raise LocalEntryError("local_entry_stopping")
            entry.session = self._factory(self.runtime, entry.sandbox_id)
            await entry.session.open()
            if entry.session.target != entry.target:
                raise LocalEntryError("local_entry_target_changed")
            if self._stopping or entry.closing or not entry.session.is_active:
                raise LocalEntryError("local_entry_unavailable")
            entry.ready = True
        except BaseException as error:
            entry.ready = False
            if entry.session is not None:
                try:
                    await settle_task(asyncio.create_task(entry.session.close()))
                except BaseException:
                    entry.closing = True
                    raise LocalEntryError("local_entry_cleanup_failed") from None
            if self._entries.get(entry.sandbox_id) is entry:
                del self._entries[entry.sandbox_id]
            if isinstance(error, asyncio.CancelledError):
                raise
            if isinstance(error, LocalEntryError):
                raise error from None
            raise LocalEntryError("local_entry_unavailable") from None

    async def open(self, sandbox_id: str, *, expected_target: LocalEntryTarget | None = None) -> LocalEntryConnection:
        """Atomically reserve without awaiting, share in-flight startup and never lose a cancelled waiter's result."""
        sandbox_pod_name(sandbox_id)
        if expected_target is not None:
            if (not isinstance(expected_target, LocalEntryTarget) or expected_target.sandbox_id != sandbox_id
                or expected_target.namespace != self.runtime._namespace):
                raise LocalEntryError("local_entry_identity_conflict")
            expected_target.__post_init__()
        if self._stopping:
            raise LocalEntryError("local_entry_stopping")
        if sandbox_id in self._blocked:
            raise LocalEntryError("local_entry_unavailable")
        entry = self._entries.get(sandbox_id)
        if entry is None:
            if len(self._entries) >= self.max_sessions:
                raise LocalEntryError("local_entry_capacity_exceeded")
            entry = _Entry(sandbox_id, expected_target=expected_target)
            self._entries[sandbox_id] = entry
            entry.task = asyncio.create_task(self._start(entry))
            entry.task.add_done_callback(_consume)
        elif expected_target is not None:
            if ((entry.target is not None and entry.target != expected_target)
                or (entry.expected_target is not None and entry.expected_target != expected_target)):
                raise LocalEntryError("local_entry_identity_conflict")
            entry.expected_target = expected_target
        if entry.closing:
            raise LocalEntryError("local_entry_unavailable")
        await wait_for_task(entry.task)
        self._connection(entry)
        return await self.lookup(sandbox_id)

    async def lookup(self, sandbox_id: str) -> LocalEntryConnection:
        """Read only an existing ready session and perform a fresh target check without opening or repairing it."""
        sandbox_pod_name(sandbox_id)
        entry = self._entries.get(sandbox_id)
        if entry is None:
            raise LocalEntryError("local_entry_unavailable")
        self._connection(entry)
        try:
            target = await self._resolve(sandbox_id)
            if target != entry.target:
                raise LocalEntryError("local_entry_target_changed")
        except Exception:
            raise LocalEntryError("local_entry_unavailable") from None
        return self._connection(entry)

    async def _cleanup_entry(self, entry):
        """Wait through startup and require successful close before relinquishing this exact reservation."""
        if entry.task is not None:
            try:
                await settle_task(entry.task)
            except Exception:
                pass
        if entry.session is not None:
            try:
                await entry.session.close()
            except Exception:
                raise LocalEntryError("local_entry_cleanup_failed") from None
        if self._entries.get(entry.sandbox_id) is entry:
            del self._entries[entry.sandbox_id]

    async def _retire(self, entry):
        """Share critical cleanup across deletion, explicit close and shutdown, including failed-close retries."""
        entry.closing, entry.ready = True, False
        if entry.cleanup is None or (entry.cleanup.done() and entry.cleanup.exception() is not None):
            entry.cleanup = asyncio.create_task(self._cleanup_entry(entry))
            entry.cleanup.add_done_callback(_consume)
        await settle_task(entry.cleanup, propagate_cancellation=True)

    async def close(self, sandbox_id: str, *, expected_transaction_id: str | None = None) -> None:
        """Close only the original matching transaction, including startup whose identity is not known yet."""
        sandbox_pod_name(sandbox_id)
        _transaction(expected_transaction_id)
        entry = self._entries.get(sandbox_id)
        if entry is None:
            return
        if entry.target is None:
            try:
                await settle_task(entry.task, propagate_cancellation=True)
            except Exception:
                if entry.target is None:
                    return
        if expected_transaction_id is not None and entry.target.transaction_id != expected_transaction_id:
            raise LocalEntryError("local_entry_identity_conflict")
        await self._retire(entry)

    @asynccontextmanager
    async def delete_guard(self, sandbox_id: str, *, expected_transaction_id: str | None = None):
        """Block only this instance through original-session close and the caller's complete resource deletion."""
        sandbox_pod_name(sandbox_id)
        _transaction(expected_transaction_id)
        if sandbox_id in self._blocked:
            raise LocalEntryError("local_entry_identity_conflict")
        self._blocked.add(sandbox_id)
        try:
            await self.close(sandbox_id, expected_transaction_id=expected_transaction_id)
            yield
        finally:
            self._blocked.discard(sandbox_id)

    async def _close_all(self):
        """Settle every owned entry even if one cleanup fails, retaining all unconfirmed handles."""
        # Creation notices _stopping before opening/returning an entry and runs
        # its own exact compensation. Do not retire that entry concurrently or
        # release the application's SQLite lock while its resource work is live.
        await asyncio.gather(*(settle_task(task) for task in tuple(self._preparing)), return_exceptions=True)
        entries = tuple(self._entries.values())
        for entry in entries:
            entry.closing, entry.ready = True, False
        results = await asyncio.gather(*(self._retire(entry) for entry in entries), return_exceptions=True)
        if any(isinstance(result, BaseException) for result in results) or self._entries:
            raise LocalEntryError("local_entry_cleanup_failed")

    async def close_all(self) -> None:
        """Permanently stop admission and await shutdown through repeated cancellation before reporting completion."""
        self._stopping = True
        if self._shutdown is None or (self._shutdown.done() and self._shutdown.exception() is not None):
            self._shutdown = asyncio.create_task(self._close_all())
            self._shutdown.add_done_callback(_consume)
        await settle_task(self._shutdown, propagate_cancellation=True)
