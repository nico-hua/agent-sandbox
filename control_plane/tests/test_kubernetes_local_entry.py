"""Offline target checks and owned subprocess lifecycle for local Kubernetes entry sessions."""

import asyncio
import copy
from dataclasses import replace

import pytest

from control_plane.core.errors import KubernetesOperationUnsupported, SandboxNotFound, SandboxQueryFailed
from kubernetes_proxy_fakes import FakeProxyClient
from test_kubernetes_proxy_lifecycle import authenticated_spec, proxy_clock, runtime
from test_kubernetes_proxy_service import historical_seven
from test_kubernetes_query import NAMESPACE, SID, TXN


@pytest.mark.anyio
async def test_resolver_returns_original_ready_eight_resource_identity_without_token(proxy_clock):
    """The real resolver must use complete resource validation, not accept a free-form Pod name."""
    from control_plane.runtime.kubernetes.local_entry import resolve_local_entry_target
    fake = FakeProxyClient()
    backend = runtime(fake)
    await backend.create(authenticated_spec())
    before = [(item.metadata.uid, item.metadata.resource_version) for item in fake.objects()]
    target = await resolve_local_entry_target(backend, SID)
    assert target.sandbox_id == SID and target.transaction_id == TXN
    assert target.namespace == NAMESPACE and target.layout_version == "2"
    assert target.proxy_pod_name == fake.proxy_pod.metadata.name
    assert target.proxy_pod_uid == fake.proxy_pod.metadata.uid
    assert target.proxy_service_uid == fake.proxy_service.metadata.uid
    assert len(target.resource_uids) == 8
    assert "Q" * 43 not in repr(target)
    assert before == [(item.metadata.uid, item.metadata.resource_version) for item in fake.objects()]
    assert fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["v1", "unready", "missing-service", "selector"])
async def test_resolver_rejects_old_unready_or_damaged_target(fault, proxy_clock):
    """A ready Agent alone must not authorize a local forward when the v2 proxy chain is invalid."""
    from control_plane.runtime.kubernetes.local_entry import resolve_local_entry_target
    fake = FakeProxyClient()
    backend = runtime(fake)
    await backend.create(authenticated_spec())
    if fault == "v1":
        historical_seven(fake)
    elif fault == "unready":
        fake.startup["proxy"] = "not-ready"
    elif fault == "missing-service":
        fake.proxy_service = None
    else:
        fake.proxy_service.spec.selector["io.agent-sandbox.resource"] = "container"
    with pytest.raises((KubernetesOperationUnsupported, SandboxNotFound, SandboxQueryFailed)):
        await resolve_local_entry_target(backend, SID)


@pytest.mark.anyio
async def test_invalid_public_id_is_rejected_before_client_acquisition():
    """Malformed public identifiers cannot cause kubeconfig loading or process creation."""
    from control_plane.runtime.kubernetes.local_entry import resolve_local_entry_target

    def forbidden(**kwargs):
        """Fail if input validation opens a Kubernetes client."""
        pytest.fail("invalid public identity opened a client")

    with pytest.raises(SandboxNotFound):
        await resolve_local_entry_target(runtime(None, client_factory=forbidden), "../pod")


def target_snapshot(**changes):
    """Build an independent canonical eight-resource target for subprocess-only tests."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryTarget
    from kubernetes_proxy_fakes import ROLE_UIDS
    from test_kubernetes_query import CREATED, EXPIRES
    roles = {"agent": "container", "policy": "network-policy", "service": "agent-service",
             "secret": "proxy-access", "proxy": "proxy", "ingress": "agent-proxy-ingress",
             "isolation": "proxy-isolation", "proxy_service": "proxy-service"}
    root = "agent-sandbox-sbx-" + SID.removeprefix("sbx_")
    args = dict(sandbox_id=SID, transaction_id=TXN, namespace=NAMESPACE,
                proxy_pod_name=root + "-proxy", proxy_pod_uid=ROLE_UIDS["proxy"],
                proxy_service_name=root + "-proxy", proxy_service_uid=ROLE_UIDS["proxy-service"],
                resource_uids=tuple(sorted((key, ROLE_UIDS[role]) for key, role in roles.items())),
                created_at=CREATED, expires_at=EXPIRES)
    args.update(changes)
    return LocalEntryTarget(**args)


class FakeForwardProcess:
    """Model only an owned asynchronous process and bounded output streams, never spawn a real program."""

    def __init__(self, *, ignore_term=False, ignore_kill=False):
        """Expose controlled stream, termination and reap events for deterministic lifecycle assertions."""
        self.stdout, self.stderr = asyncio.StreamReader(limit=8192), asyncio.StreamReader(limit=8192)
        self.returncode = None
        self.finished, self.term_received, self.kill_received = asyncio.Event(), asyncio.Event(), asyncio.Event()
        self.ignore_term, self.ignore_kill = ignore_term, ignore_kill
        self.terminations = self.kills = self.reaps = 0
        self.stdout.feed_data(b"Forwarding from 127.0.0.1:43123 -> 8080\n")

    async def wait(self):
        """Return only once the controlled child has actually exited."""
        await self.finished.wait()
        self.reaps += 1
        return self.returncode

    def finish(self, code=0):
        """End both pipes and publish process exit without addressing any other PID."""
        if self.returncode is None:
            self.returncode = code
            self.stdout.feed_eof()
            self.stderr.feed_eof()
            self.finished.set()

    def terminate(self):
        """Deliver TERM only to this fake handle and optionally emulate a stuck graceful shutdown."""
        self.terminations += 1
        self.term_received.set()
        if not self.ignore_term:
            self.finish(-15)

    def kill(self):
        """Deliver KILL only to this fake handle and retain explicit failure control."""
        self.kills += 1
        self.kill_received.set()
        if not self.ignore_kill:
            self.finish(-9)


@pytest.fixture
def entry_harness(monkeypatch):
    """Inject controlled target and process dependencies while keeping production session orchestration real."""
    from types import SimpleNamespace
    from control_plane.runtime.kubernetes import local_entry
    monkeypatch.setattr(local_entry, "STARTUP_TIMEOUT_SECONDS", 0.2)
    monkeypatch.setattr(local_entry, "STOP_TIMEOUT_SECONDS", 0.03)
    harness = SimpleNamespace(process=None, calls=[], resolves=[], target=None, failure=None,
                              spawn_entered=asyncio.Event(), spawn_release=asyncio.Event())
    harness.target = target_snapshot()
    harness.spawn_release.set()

    async def resolve(backend, sandbox_id):
        """Return a stable tested identity unless a test explicitly changes the second observation."""
        harness.resolves.append(sandbox_id)
        return harness.target

    async def spawn(*args, **kwargs):
        """Record actual argv and resource ownership flags before returning or delaying the handle."""
        harness.calls.append((args, kwargs))
        harness.spawn_entered.set()
        await harness.spawn_release.wait()
        if harness.failure is not None:
            raise harness.failure
        return harness.process

    harness.make = lambda: local_entry.KubernetesLocalEntry(
        runtime(None), SID, process_factory=spawn, target_resolver=resolve)
    return harness


@pytest.mark.anyio
async def test_session_opens_dynamic_loopback_only_after_two_identity_checks(entry_harness, monkeypatch):
    """A ready, live child and stable target produce the URL without sending either platform credential."""
    monkeypatch.setenv("AGENT_SANDBOX_API_KEY", "synthetic-admin-secret")
    monkeypatch.setenv("HTTPS_PROXY", "http://untrusted.invalid")
    h = entry_harness
    h.process = FakeForwardProcess()
    entry = h.make()
    async with entry:
        assert entry.url == "http://127.0.0.1:43123" and entry.is_active
        assert h.resolves == [SID, SID]
        args, kwargs = h.calls[0]
        assert args[0] == "kubectl" and args[1].startswith("--cache-dir=")
        assert args[2:] == ("--kubeconfig=/never-read", "--context=explicit-test",
                        "--namespace=" + NAMESPACE, "port-forward", "--address=127.0.0.1",
                        "--pod-running-timeout=5s", "pod/" + h.target.proxy_pod_name, ":8080")
        assert kwargs["stdin"] == asyncio.subprocess.DEVNULL
        assert kwargs["stdout"] == kwargs["stderr"] == asyncio.subprocess.PIPE
        assert "synthetic-admin-secret" not in repr(h.calls)
        assert "HTTPS_PROXY" not in kwargs["env"] and "Q" * 43 not in repr(entry)
        assert entry.target == h.target
    assert h.process.returncode is not None and h.process.reaps == 1
    assert not entry.is_active
    await entry.close()
    assert h.process.terminations == 1


@pytest.mark.anyio
@pytest.mark.parametrize("line", [b"Forwarding from 0.0.0.0:43123 -> 8080\n",
    b"Forwarding from [::1]:43123 -> 8080\n", b"Forwarding from 127.0.0.1:0 -> 8080\n",
    b"Forwarding from 127.0.0.1:65536 -> 8080\n", b"Forwarding from 127.0.0.1:43123 -> 9090\n",
    b"Forwarding from 127.0.0.1:043123 -> 8080\n"])
async def test_unexpected_forwarding_address_or_port_never_delivers_url(line, entry_harness):
    """Untrusted output cannot widen the bind address or route to an unapproved remote port."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = entry_harness
    h.process = FakeForwardProcess()
    await h.process.stdout.read(4096)
    h.process.stdout.feed_data(line)
    entry = h.make()
    with pytest.raises(LocalEntryError) as captured:
        await entry.open()
    assert captured.value.code == "local_entry_protocol_error"
    assert not entry.is_active and h.process.returncode is not None


@pytest.mark.anyio
@pytest.mark.parametrize("failure,code", [(FileNotFoundError("private executable path"), "kubectl_unavailable"),
    (OSError("private transport detail"), "local_entry_unavailable")])
async def test_spawn_failure_is_fixed_and_does_not_leak_details(failure, code, entry_harness):
    """Missing kubectl and other spawn failures are bounded fixed categories without raw paths."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = entry_harness
    h.failure = failure
    with pytest.raises(LocalEntryError) as captured:
        await h.make().open()
    assert captured.value.code == code and "private" not in str(captured.value)
    assert len(h.calls) == 1


@pytest.mark.anyio
async def test_early_exit_and_later_exit_invalidate_address(entry_harness):
    """A stale readiness line or an already-closed forwarding process never remains usable."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = entry_harness
    h.process = FakeForwardProcess()
    h.process.finish(1)
    with pytest.raises(LocalEntryError):
        await h.make().open()
    h.process = FakeForwardProcess()
    entry = await h.make().open()
    h.process.finish(1)
    assert not entry.is_active
    with pytest.raises(LocalEntryError):
        _ = entry.url
    await entry.close()
    assert h.process.reaps == 1


@pytest.mark.anyio
async def test_startup_timeout_reaps_owned_process(entry_harness, monkeypatch):
    """A live child without a listening announcement is terminated instead of leaking on timeout."""
    from control_plane.runtime.kubernetes import local_entry
    monkeypatch.setattr(local_entry, "STARTUP_TIMEOUT_SECONDS", 0.02)
    h = entry_harness
    h.process = FakeForwardProcess()
    await h.process.stdout.read(4096)
    with pytest.raises(local_entry.LocalEntryError) as captured:
        await h.make().open()
    assert captured.value.code == "local_entry_timeout"
    assert h.process.returncode is not None and h.process.reaps == 1


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["spawn", "second-resolve"])
async def test_repeated_cancel_settles_late_handle_before_return(stage, entry_harness):
    """Cancellation during process creation or final identity verification still owns and reaps the child."""
    h = entry_harness
    h.process = FakeForwardProcess()
    h.spawn_release.clear()
    if stage == "second-resolve":
        h.spawn_release.set()
    entered, release = asyncio.Event(), asyncio.Event()
    entry = h.make()
    original = entry._target_resolver

    async def controlled(backend, sandbox_id):
        """Hold final verification at an explicit event rather than relying on scheduler timing."""
        value = await original(backend, sandbox_id)
        if len(h.resolves) == 2:
            entered.set()
            await release.wait()
        return value

    entry._target_resolver = controlled
    operation = asyncio.create_task(entry.open())
    await (h.spawn_entered.wait() if stage == "spawn" else entered.wait())
    operation.cancel()
    await asyncio.sleep(0)
    operation.cancel()
    await asyncio.sleep(0)
    assert not operation.done() and h.process.returncode is None
    h.spawn_release.set()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert h.process.returncode is not None and h.process.reaps == 1
    await entry.close()


@pytest.mark.anyio
async def test_changed_target_before_delivery_closes_forward(entry_harness):
    """A new transaction or replacement object cannot inherit an already-started forwarding handle."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = entry_harness
    h.process = FakeForwardProcess()
    entry = h.make()
    calls = 0

    async def replace_target(backend, sandbox_id):
        """Change the full independently valid transaction at the second observation."""
        nonlocal calls
        calls += 1
        return h.target if calls == 1 else replace(h.target, transaction_id="txn_" + "f" * 32)

    entry._target_resolver = replace_target
    with pytest.raises(LocalEntryError) as captured:
        await entry.open()
    assert captured.value.code == "local_entry_target_changed"
    assert h.process.returncode is not None and h.process.reaps == 1


@pytest.mark.anyio
async def test_unresponsive_term_escalates_only_owned_handle_and_reaps(entry_harness):
    """The stop budget escalates to KILL without signaling any externally supplied PID or group."""
    h = entry_harness
    h.process = FakeForwardProcess(ignore_term=True)
    entry = await h.make().open()
    await entry.close()
    assert (h.process.terminations, h.process.kills, h.process.reaps) == (1, 1, 1)


@pytest.mark.anyio
@pytest.mark.parametrize("changes", [{"proxy_pod_name": "arbitrary"}, {"proxy_service_name": "arbitrary"},
    {"layout_version": "1"}, {"namespace": "other-namespace"}, {"sandbox_id": "sbx_" + "f" * 32}])
async def test_invalid_or_mismatched_target_never_spawns(changes, entry_harness):
    """Injected dependencies cannot supply an arbitrary target or silently change the explicit namespace."""
    h = entry_harness
    entry = h.make()

    async def malformed(backend, sandbox_id):
        """Return a forged snapshot to prove the consumer revalidates it before process creation."""
        value = copy.copy(h.target)
        for key, item in changes.items():
            object.__setattr__(value, key, item)
        return value

    entry._target_resolver = malformed
    with pytest.raises(SandboxQueryFailed):
        await entry.open()
    assert not h.calls


@pytest.mark.anyio
async def test_continuous_large_output_is_drained_without_retained_logs(entry_harness):
    """Both pipes remain consumable after startup even when output grows far beyond parser limits."""
    h = entry_harness
    h.process = FakeForwardProcess()
    entry = await h.make().open()
    for _ in range(128):
        h.process.stdout.feed_data(b"x" * 4096)
        h.process.stderr.feed_data(b"y" * 4096)
        await asyncio.sleep(0)
    h.process.stdout.feed_data(b"\nHandling connection for 43123\n")
    h.process.stderr.feed_data(b"\n")
    await asyncio.sleep(0)
    assert entry.is_active
    assert len(h.process.stdout._buffer) <= 8192 and len(h.process.stderr._buffer) <= 8192
    await entry.close()
    assert h.process.reaps == 1


@pytest.mark.anyio
@pytest.mark.parametrize("message,code", [(b"error: Forbidden private diagnostic\n", "local_entry_permission_denied"),
    (b"Unable to connect to the server: private endpoint\n", "local_entry_unavailable")])
async def test_stderr_is_classified_without_exposing_original_output(message, code, entry_harness):
    """Permission and API reachability diagnostics remain fixed categories rather than raw stderr."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = entry_harness
    h.process = FakeForwardProcess()
    h.process.stderr.feed_data(message)
    with pytest.raises(LocalEntryError) as captured:
        await h.make().open()
    assert captured.value.code == code and "private" not in str(captured.value)
    assert h.process.returncode is not None


@pytest.mark.anyio
async def test_close_cancellation_waits_for_forced_reap_and_is_idempotent(entry_harness):
    """Even repeatedly cancelled close calls wait for the same child before reporting cancellation."""
    h = entry_harness
    h.process = FakeForwardProcess(ignore_term=True)
    entry = await h.make().open()
    closing = asyncio.create_task(entry.close())
    await h.process.term_received.wait()
    closing.cancel()
    await asyncio.sleep(0)
    closing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await closing
    assert h.process.returncode is not None and h.process.reaps == 1
    await entry.close()
    assert h.process.terminations == h.process.kills == 1


@pytest.mark.anyio
async def test_unconfirmed_stop_reports_failure_and_later_close_can_retry(entry_harness):
    """A failed TERM/KILL budget cannot claim cleanup and must retain ownership for an explicit retry."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = entry_harness
    h.process = FakeForwardProcess(ignore_term=True, ignore_kill=True)
    entry = await h.make().open()
    try:
        with pytest.raises(LocalEntryError) as captured:
            await entry.close()
        assert captured.value.code == "local_entry_cleanup_failed" and not entry.is_active
    finally:
        h.process.finish()
        await entry.close()
    assert h.process.reaps == 1


@pytest.mark.anyio
async def test_timeout_during_spawn_waits_for_late_handle_then_closes(entry_harness, monkeypatch):
    """An expired startup timer never abandons a factory that has already started creating a child."""
    from control_plane.runtime.kubernetes import local_entry
    h = entry_harness
    h.process = FakeForwardProcess()
    h.spawn_release.clear()
    monkeypatch.setattr(local_entry, "STARTUP_TIMEOUT_SECONDS", 0.01)
    entry = h.make()
    operation = asyncio.create_task(entry.open())
    await h.spawn_entered.wait()
    # A timer event proves expiration without guessing when the factory ran.
    expired = asyncio.Event()
    asyncio.get_running_loop().call_later(0.02, expired.set)
    await expired.wait()
    assert not operation.done()
    h.spawn_release.set()
    with pytest.raises(local_entry.LocalEntryError) as captured:
        await operation
    assert captured.value.code == "local_entry_timeout" and h.process.reaps == 1


@pytest.mark.anyio
async def test_startup_cleanup_failure_retains_owned_handle_for_later_retry(entry_harness):
    """A failed startup cleanup must not permanently prevent a later explicit close from reaping."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = entry_harness
    h.process = FakeForwardProcess(ignore_term=True, ignore_kill=True)
    await h.process.stdout.read(4096)
    h.process.stdout.feed_data(b"Forwarding from 0.0.0.0:43123 -> 8080\n")
    entry = h.make()
    try:
        with pytest.raises(LocalEntryError) as captured:
            await entry.open()
        assert captured.value.code == "local_entry_cleanup_failed"
    finally:
        h.process.finish()
        await entry.close()
    assert h.process.reaps == 1


@pytest.mark.anyio
async def test_arbitrary_spawn_failure_does_not_escape_as_raw_diagnostic(entry_harness):
    """Unexpected process-factory failures get a fixed error instead of disclosing environment details."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = entry_harness
    h.failure = RuntimeError("private diagnostic, never display")
    with pytest.raises(LocalEntryError) as captured:
        await h.make().open()
    assert captured.value.code == "local_entry_unavailable" and "private" not in str(captured.value)


@pytest.mark.anyio
async def test_kubectl_cache_is_session_private_and_removed_after_reap(entry_harness):
    """The real kubectl must not default discovery cache into the checkout or caller's home directory."""
    from pathlib import Path
    h = entry_harness
    h.process = FakeForwardProcess()
    entry = await h.make().open()
    try:
        args = h.calls[0][0]
        cache_arguments = [arg for arg in args if arg.startswith("--cache-dir=")]
        assert len(cache_arguments) == 1
        directory = Path(cache_arguments[0].partition("=")[2])
        assert directory.name.startswith("agent-sandbox-kubectl-") and directory.is_dir()
        (directory / "simulated-discovery.json").write_text("{}")
    finally:
        await entry.close()
    assert not directory.exists() and h.process.reaps == 1


@pytest.mark.anyio
async def test_idle_forward_discovers_target_loss_even_when_kubectl_stays_alive(entry_harness, monkeypatch):
    """An idle kubectl need not exit immediately on Pod loss, so identity observation must close it."""
    from control_plane.runtime.kubernetes import local_entry
    monkeypatch.setattr(local_entry, "TARGET_CHECK_INTERVAL_SECONDS", 0.01)
    h = entry_harness
    h.process = FakeForwardProcess()
    entry = await h.make().open()
    h.target = replace(h.target, transaction_id="txn_" + "f" * 32)
    try:
        await asyncio.wait_for(h.process.finished.wait(), 1)
        assert not entry.is_active and h.process.terminations == 1
    finally:
        await entry.close()
    assert h.process.reaps == 1 and len(h.calls) == 1


@pytest.mark.anyio
async def test_close_before_open_stays_inert_and_cannot_be_reopened(entry_harness):
    """An unopened or permanently closed session cannot acquire a target or spawn resources."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = entry_harness
    entry = h.make()
    assert not h.resolves and not h.calls
    with pytest.raises(LocalEntryError):
        _ = entry.url
    await entry.close()
    await entry.close()
    with pytest.raises(LocalEntryError):
        await entry.open()
    assert not h.calls and not h.resolves


@pytest.mark.anyio
@pytest.mark.parametrize("field", ["proxy", "proxy_service"])
async def test_original_uid_replacement_before_delivery_is_rejected(field, entry_harness):
    """An otherwise valid same-name target with a new original UID cannot be handed to the caller."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = entry_harness
    h.process = FakeForwardProcess()
    entry = h.make()
    calls = 0

    async def resolver(backend, sandbox_id):
        """Supply a separately valid replacement only after the forward has reported listening."""
        nonlocal calls
        calls += 1
        if calls == 1:
            return h.target
        records = dict(h.target.resource_uids)
        records[field] = "b5667ab6-3f02-40ba-9b29-b1967503a3f0"
        changes = {"resource_uids": tuple(sorted(records.items())),
                   "proxy_pod_uid" if field == "proxy" else "proxy_service_uid": records[field]}
        return replace(h.target, **changes)

    entry._target_resolver = resolver
    with pytest.raises(LocalEntryError) as captured:
        await entry.open()
    assert captured.value.code == "local_entry_target_changed"
    assert h.process.reaps == 1 and len(h.calls) == 1


@pytest.mark.anyio
async def test_cancellation_during_real_target_worker_waits_for_client_close(proxy_clock, entry_harness):
    """A cancelled resolver cannot leave its synchronous SDK thread or client running behind the caller."""
    import threading
    from control_plane.runtime.kubernetes.local_entry import KubernetesLocalEntry
    fake = FakeProxyClient()
    backend = runtime(fake)
    await backend.create(authenticated_spec())
    fake.closed.clear()
    entered, release = threading.Event(), threading.Event()
    fake.barriers[("list", "pod")] = (entered, release)

    async def forbidden(*args, **kwargs):
        """Detect an unsafe spawn after cancellation in the target validation stage."""
        pytest.fail("cancelled target lookup launched kubectl")

    entry = KubernetesLocalEntry(backend, SID, process_factory=forbidden)
    task = asyncio.create_task(entry.open())
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not fake.closed.is_set()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert fake.closed.is_set()
    await entry.close()


def test_construction_does_not_open_files_clients_or_processes(monkeypatch):
    """Constructing the default session only retains configuration and never acquires external resources."""
    from pathlib import Path
    from kubernetes import client
    from control_plane.runtime.kubernetes.local_entry import KubernetesLocalEntry

    def forbidden(*args, **kwargs):
        """Detect any forbidden external acquisition before explicit open()."""
        pytest.fail("inert local entry construction performed I/O")

    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(client, "ApiClient", forbidden)
    monkeypatch.setattr(asyncio, "create_subprocess_exec", forbidden)
    entry = KubernetesLocalEntry(runtime(None), SID)
    assert not entry.is_active and entry.target is None
