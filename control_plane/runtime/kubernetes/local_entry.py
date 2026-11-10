"""Own one WSL loopback kubectl process without publishing a public Kubernetes endpoint."""

import asyncio
import re

from control_plane.core.errors import SandboxQueryFailed
from control_plane.core.tasks import settle_task
from control_plane.runtime.kubernetes.pod_query import sandbox_pod_name
from control_plane.runtime.kubernetes.local_entry_target import LocalEntryTarget, resolve_local_entry_target

STARTUP_TIMEOUT_SECONDS = 10.0
STOP_TIMEOUT_SECONDS = 2.0
TARGET_CHECK_INTERVAL_SECONDS = 2.0
_READ_SIZE = 4096
_MAX_LINE = 512
_READY = re.compile(rb"Forwarding from 127\.0\.0\.1:([1-9][0-9]{0,4}) -> 8080")
_MESSAGES = {
    "local_entry_unavailable": "local entry is unavailable",
    "local_entry_timeout": "local entry startup timed out",
    "local_entry_protocol_error": "local entry startup response is invalid",
    "local_entry_target_changed": "local entry target identity changed",
    "local_entry_closed": "local entry session is closed",
    "local_entry_cleanup_failed": "local entry process cleanup could not be confirmed",
    "local_entry_permission_denied": "local entry access denied",
    "kubectl_unavailable": "kubectl executable is unavailable",
}


class LocalEntryError(RuntimeError):
    """Expose fixed local-only categories without subprocess output, paths or credentials."""

    def __init__(self, code: str):
        """Retain a known category and fixed public message rather than an underlying exception."""
        self.code = code
        super().__init__(_MESSAGES[code])


class KubernetesLocalEntry:
    """Own one explicitly verified, nonreconnecting loopback kubectl forwarding session."""

    def __init__(self, runtime, sandbox_id: str, *, process_factory=None, target_resolver=None):
        """Store inert dependencies; acquiring clients and creating processes only happens in open()."""
        sandbox_pod_name(sandbox_id)
        self._runtime, self._sandbox_id = runtime, sandbox_id
        self._process_factory = asyncio.create_subprocess_exec if process_factory is None else process_factory
        self._target_resolver = resolve_local_entry_target if target_resolver is None else target_resolver
        self._process = self._target = self._port = self._cache = None
        self._opening = self._closing = self._cleanup = self._exit = self._watcher = None
        self._drains = []
        self._stop = asyncio.Event()
        self._usable = False

    @property
    def target(self) -> LocalEntryTarget | None:
        """Expose only original nonsecret identities, including after the session has closed."""
        return self._target

    @property
    def is_active(self) -> bool:
        """Never advertise a dead or closing process as a currently available local entry."""
        return (self._usable and not self._stop.is_set() and self._process is not None
                and self._process.returncode is None and not self._exit.done()
                and not self._output_failure.done())

    @property
    def url(self) -> str:
        """Return a loopback origin only for this session's currently live original forward."""
        if not self.is_active:
            raise LocalEntryError("local_entry_closed" if self._stop.is_set() else "local_entry_unavailable")
        return f"http://127.0.0.1:{self._port}"

    async def __aenter__(self):
        """Start the explicit owned session when entering its asynchronous context."""
        return await self.open()

    async def __aexit__(self, exc_type, exc, traceback):
        """Close only the local forward and preserve body errors with any cleanup failure noted."""
        try:
            await self.close()
        except LocalEntryError:
            if exc is None:
                raise
            exc.add_note(_MESSAGES["local_entry_cleanup_failed"])

    async def open(self):
        """Start once; cancellation waits through late process creation and mandatory cleanup."""
        from control_plane.core.tasks import wait_for_task
        if self._stop.is_set():
            raise LocalEntryError("local_entry_closed")
        if self._opening is None:
            self._opening = asyncio.create_task(self._start())
        try:
            await wait_for_task(self._opening)
            if not self.is_active:
                raise LocalEntryError("local_entry_unavailable")
            return self
        except asyncio.CancelledError:
            self._stop.set()
            await self.close()
            raise

    async def _step(self, operation, deadline, *, owns_work=True):
        """Respect startup and close signals without abandoning an in-flight worker or process factory."""
        task = asyncio.ensure_future(operation)
        stopped = asyncio.create_task(self._stop.wait())
        try:
            done, _ = await asyncio.wait({task, stopped}, timeout=max(0, deadline - asyncio.get_running_loop().time()),
                                         return_when=asyncio.FIRST_COMPLETED)
            if stopped in done:
                raise LocalEntryError("local_entry_closed")
            if task not in done:
                raise LocalEntryError("local_entry_timeout")
            return task.result()
        finally:
            stopped.cancel()
            await asyncio.gather(stopped, return_exceptions=True)
            # A cancelled waiter cannot cancel an OS spawn or SDK thread. Read
            # its late outcome before the caller's finally disposes the handle.
            if owns_work:
                await settle_task(task)
            else:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def _resolve(self):
        """Revalidate even injected snapshots and keep namespace/ID bound to this explicit Runtime."""
        value = await self._target_resolver(self._runtime, self._sandbox_id)
        if not isinstance(value, LocalEntryTarget):
            raise SandboxQueryFailed()
        value.__post_init__()
        if value.sandbox_id != self._sandbox_id or value.namespace != self._runtime._namespace:
            raise SandboxQueryFailed()
        return value

    async def _spawn(self):
        """Use fixed argv and a minimal environment without shell, proxies, tokens or arbitrary targets."""
        import os
        from tempfile import TemporaryDirectory
        try:
            self._cache = TemporaryDirectory(prefix="agent-sandbox-kubectl-")
            self._process = await self._process_factory(
                "kubectl", "--cache-dir=" + self._cache.name, "--kubeconfig=" + str(self._runtime._kubeconfig_path),
                "--context=" + self._runtime._context, "--namespace=" + self._target.namespace,
                "port-forward", "--address=127.0.0.1", "--pod-running-timeout=5s",
                "pod/" + self._target.proxy_pod_name, ":8080",
                stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                limit=8192, env={"PATH": os.environ.get("PATH", os.defpath), "LANG": "C", "LC_ALL": "C"},
            )
        except FileNotFoundError:
            raise LocalEntryError("kubectl_unavailable") from None
        except Exception:
            raise LocalEntryError("local_entry_unavailable") from None
        self._exit = asyncio.create_task(self._process.wait())
        self._drains = [asyncio.create_task(self._drain(self._process.stdout, True)),
                        asyncio.create_task(self._drain(self._process.stderr, False))]

    async def _start(self):
        """Resolve, spawn, parse listening evidence and recheck the full original target before delivery."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + STARTUP_TIMEOUT_SECONDS
        self._port_ready, self._output_failure = loop.create_future(), loop.create_future()
        try:
            self._target = await self._step(self._resolve(), deadline)
            await self._step(self._spawn(), deadline)
            if self._stop.is_set():
                raise LocalEntryError("local_entry_closed")
            await self._step(self._listening(), deadline, owns_work=False)
            current = await self._step(self._resolve(), deadline)
            if current != self._target:
                raise LocalEntryError("local_entry_target_changed")
            if (self._process.returncode is not None or self._exit.done()
                or self._output_failure.done() or self._stop.is_set() or loop.time() >= deadline):
                raise LocalEntryError("local_entry_unavailable")
            self._usable = True
            self._watcher = asyncio.create_task(self._watch())
        except BaseException as error:
            if self._process is not None:
                await self._finish_process()
            else:
                self._clear_cache()
            if isinstance(error, FileNotFoundError):
                raise LocalEntryError("kubectl_unavailable") from None
            if isinstance(error, OSError):
                raise LocalEntryError("local_entry_unavailable") from None
            raise

    async def _listening(self):
        """Accept kubectl's listener acknowledgement only while the same owned process remains alive."""
        done, _ = await asyncio.wait({self._port_ready, self._output_failure, self._exit},
                                     return_when=asyncio.FIRST_COMPLETED)
        if self._output_failure in done:
            raise LocalEntryError(self._output_failure.result())
        if self._exit in done:
            raise LocalEntryError("local_entry_unavailable")
        self._port = self._port_ready.result()

    def _line(self, line: bytes, stdout: bool):
        """Parse at most one bounded fragment, retaining only port or a fixed diagnostic category."""
        if stdout and line.startswith(b"Forwarding from "):
            match = _READY.fullmatch(line)
            if match is None or int(match[1]) > 65535:
                if not self._output_failure.done():
                    self._output_failure.set_result("local_entry_protocol_error")
            elif self._port_ready.done() and self._port_ready.result() != int(match[1]):
                if not self._output_failure.done():
                    self._output_failure.set_result("local_entry_protocol_error")
            elif not self._port_ready.done():
                self._port_ready.set_result(int(match[1]))
        elif not stdout:
            lower = line.lower()
            if any(marker in lower for marker in (b"forbidden", b"unauthorized")):
                code = "local_entry_permission_denied"
            elif any(marker in lower for marker in (b"unable to connect", b"connection refused", b"connection timed out")):
                code = "local_entry_unavailable"
            else:
                return
            if not self._output_failure.done():
                self._output_failure.set_result(code)

    async def _drain(self, stream, stdout: bool):
        """Continuously drain fixed-size chunks, discarding overlong lines without accumulating logs."""
        pending, discarding = b"", False
        try:
            while True:
                chunk = await stream.read(_READ_SIZE)
                if not chunk:
                    if pending and not discarding:
                        self._line(pending, stdout)
                    return
                fragments = chunk.split(b"\n")
                for index, fragment in enumerate(fragments):
                    end = index < len(fragments) - 1
                    if not discarding:
                        if len(pending) + len(fragment) <= _MAX_LINE:
                            pending += fragment
                        else:
                            pending, discarding = b"", True
                    if end:
                        if not discarding:
                            self._line(pending, stdout)
                        pending, discarding = b"", False
        except asyncio.CancelledError:
            raise
        except Exception:
            if not self._output_failure.done():
                self._output_failure.set_result("local_entry_protocol_error")

    async def _watch(self):
        """Detect child exit and idle target loss without reconnecting to any replacement object."""
        stopped = asyncio.create_task(self._stop.wait())
        try:
            while not self._stop.is_set():
                done, _ = await asyncio.wait({self._exit, self._output_failure, stopped},
                    timeout=TARGET_CHECK_INTERVAL_SECONDS, return_when=asyncio.FIRST_COMPLETED)
                if done:
                    break
                try:
                    current = await self._step(self._resolve(), asyncio.get_running_loop().time() + STARTUP_TIMEOUT_SECONDS)
                    if current != self._target:
                        raise LocalEntryError("local_entry_target_changed")
                except Exception:
                    if not self._output_failure.done():
                        self._output_failure.set_result("local_entry_target_changed")
                    break
            self._usable = False
            try:
                await self._finish_process()
            except LocalEntryError:
                # Explicit close retries or reports this owned cleanup outcome.
                pass
        finally:
            stopped.cancel()
            await asyncio.gather(stopped, return_exceptions=True)

    def _clear_cache(self):
        """Remove only this session's private discovery directory after its writer has stopped."""
        if self._cache is not None:
            try:
                self._cache.cleanup()
            except Exception:
                raise LocalEntryError("local_entry_cleanup_failed") from None
            self._cache = None

    async def _finish_process(self):
        """Share one cleanup task across startup failure, process monitoring and explicit close."""
        if self._cleanup is None or (self._cleanup.done() and self._cleanup.exception() is not None):
            self._cleanup = asyncio.create_task(self._stop_process())
        return await settle_task(self._cleanup)

    async def _stop_process(self):
        """TERM then KILL only the owned handle, require reap, and stop every bounded output consumer."""
        self._usable = False
        try:
            if self._process.returncode is None:
                try:
                    self._process.terminate()
                except ProcessLookupError:
                    pass
            done, _ = await asyncio.wait({self._exit}, timeout=STOP_TIMEOUT_SECONDS)
            if not done:
                try:
                    self._process.kill()
                except ProcessLookupError:
                    pass
                done, _ = await asyncio.wait({self._exit}, timeout=STOP_TIMEOUT_SECONDS)
            if not done:
                raise LocalEntryError("local_entry_cleanup_failed")
            self._exit.result()
            self._clear_cache()
        except Exception:
            raise LocalEntryError("local_entry_cleanup_failed") from None
        finally:
            for task in self._drains:
                task.cancel()
            await asyncio.gather(*self._drains, return_exceptions=True)

    async def _close(self):
        """Wait for any in-flight spawn and then reap the session's single process before returning."""
        if self._opening is not None:
            try:
                await settle_task(self._opening)
            except Exception:
                # The opening caller already received this failure. Retry any
                # owned process cleanup instead of replaying an old outcome.
                pass
        if self._process is not None:
            await self._finish_process()
        if self._watcher is not None:
            await settle_task(self._watcher)

    async def close(self):
        """Idempotently close the owned session; repeated caller cancellation never interrupts reaping."""
        self._stop.set()
        self._usable = False
        if self._closing is None or (self._closing.done() and self._closing.exception() is not None):
            self._closing = asyncio.create_task(self._close())
        await settle_task(self._closing, propagate_cancellation=True)
