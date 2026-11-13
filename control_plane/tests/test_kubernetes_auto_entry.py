"""Creation-time entry preparation before activation using real Service, SQLite and session ownership."""

import asyncio
import base64
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest

import control_plane.app as app_module
import control_plane.core.service as service_module
from control_plane.app import create_app
from control_plane.runtime.kubernetes import kubernetes_runtime, local_entry_manager
from control_plane.runtime.kubernetes.local_entry_target import resolve_local_entry_target
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore, LifecycleStoreConflict
from control_plane.core.models import SandboxAccessCredential
from control_plane.storage.run_lock import SQLiteRunLock, LifecycleRunLockBusy
from control_plane.core.errors import SandboxCreateFailed, CreationResourceState
from control_plane.core.errors import SandboxCleanupFailed, SandboxMetadataConflict, SandboxMetadataUnavailable
from control_plane.core.errors import KubernetesConfigurationError
from kubernetes_proxy_fakes import FakeProxyClient
from test_backend_configuration import configure_kubernetes
from test_kubernetes_local_entry_manager import FakeSession, manager_harness
from test_kubernetes_persistent_workspace import persistence_clock
from test_kubernetes_proxy_lifecycle import proxy_clock, runtime, authenticated_spec
from test_kubernetes_query import CREATED, SID, TXN
from test_kubernetes_service_create import ActivationStore


@pytest.fixture
def automatic(manager_harness, persistence_clock, monkeypatch, tmp_path):
    """Assemble via actual trusted configuration, replacing only SDK transport and kubectl process work."""
    h = manager_harness
    h.api = FakeProxyClient()
    h.database = tmp_path / "automatic.sqlite3"
    h.events = []
    configure_kubernetes(monkeypatch)
    monkeypatch.setattr(kubernetes_runtime, "create_probe_client", lambda **_: h.api)
    monkeypatch.setattr(service_module, "_utc_now", lambda: CREATED)

    async def resolver(backend, sandbox_id):
        """Resolve through real inventory and retain only verified original target facts for the fake process."""
        target = (h.external_target if hasattr(h, "external_target") and sandbox_id == h.external_target.sandbox_id
                  else await resolve_local_entry_target(backend, sandbox_id))
        if hasattr(h, "target_transform"):
            target = h.target_transform(target)
        h.targets[sandbox_id] = target
        return target

    def session_factory(backend, sandbox_id):
        """Record process ownership without starting a real kubectl child."""
        h.events.append("entry")
        session = FakeSession(h, sandbox_id)
        h.sessions.append(session)
        return session

    def manager_factory(backend):
        """Keep the production manager and ownership, substituting only its external session boundary."""
        h.backend = backend
        return local_entry_manager.KubernetesLocalEntryManager(
            backend, session_factory=session_factory, target_resolver=resolver,
            max_sessions=getattr(h, "capacity", 8),
        )

    monkeypatch.setattr(app_module, "KubernetesLocalEntryManager", manager_factory)
    h.make = lambda **kwargs: create_app(api_key="synthetic-auto-key", database_path=h.database,
                                        reaper_enabled=False, **kwargs)
    return h


def identified_app(h, **kwargs):
    """Use deterministic Service factories while preserving real prewrite, Runtime and activation behavior."""
    app = h.make(**kwargs)
    app.state.service._id_factory = lambda: SID
    app.state.service._transaction_id_factory = lambda: TXN
    return app


def test_formal_application_owns_empty_manager_without_external_work(automatic):
    """Formal Kubernetes selection must prepare an inert application owner even without initial IDs."""
    h = automatic
    app = h.make()
    assert app.state.kubernetes_entry_manager is not None
    assert app.state.runtime.local_entry_manager is app.state.kubernetes_entry_manager
    assert not h.api.events and not h.sessions and not h.database.exists()


@pytest.mark.parametrize("value", [None, 1, "true"])
def test_automatic_mode_requires_explicit_boolean_before_io(value):
    """Ambiguous trusted mode values cannot silently enable or disable entry preparation."""
    api = FakeProxyClient()
    with pytest.raises(KubernetesConfigurationError):
        runtime(api, prepare_entry_on_create=value)
    assert not api.events


@pytest.mark.anyio
async def test_missing_owner_fails_unchanged_without_resource_creation():
    """Automatic creation cannot secretly create a manager outside application ownership."""
    api = FakeProxyClient()
    backend = runtime(api, prepare_entry_on_create=True)
    with pytest.raises(SandboxCreateFailed) as caught:
        await backend.create(authenticated_spec())
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED
    assert not api.events and not api.objects()


@pytest.mark.anyio
@pytest.mark.parametrize("persistent", [False, True])
async def test_service_activation_follows_automatic_entry(automatic, monkeypatch, persistent):
    """Eight/nine-resource creation can deliver the original authenticated endpoint before any manual open."""
    h = automatic
    if persistent:
        monkeypatch.setenv("AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", "agent-sandbox-workspace")
    store = ActivationStore(h.database)
    app = h.make(store=store)
    service = app.state.service
    service._id_factory = lambda: SID
    service._transaction_id_factory = lambda: TXN
    original_activate = store.activate

    async def activate(*args, **kwargs):
        """Observe true pending/credential state and a usable entry before the real SQLite CAS activation."""
        h.events.append("activate")
        record = await store.get(SID)
        credential = await store.get_credential(SID, TXN)
        assert record.management_state == "pending" and credential is not None
        assert len(h.api.objects()) == (9 if persistent else 8)
        assert app.state.kubernetes_entry_manager.session_count == 1
        assert (await h.backend.get_endpoint(SID)).auth_required is True
        return await original_activate(*args, **kwargs)

    monkeypatch.setattr(store, "activate", activate)
    async with app.router.lifespan_context(app):
        created = await service.create()
        assert created.status == "started" and created.sandbox_id == SID
        assert h.events == ["entry", "activate"]
        endpoint = await service.get_endpoint(SID)
        matches = endpoint.access_token == (await store.get_credential(SID, TXN)).access_token
        assert matches, "automatic entry did not use original persisted credential"
        assert endpoint.url == "http://127.0.0.1:43123" and len(h.sessions) == 1
        assert (await store.get(SID)).management_state == "active"
        await service.delete(SID, delete_workspace=True)
        assert not h.api.objects() and h.sessions[0].closed.is_set()
        assert await store.get(SID) is None and await store.get_credential(SID, TXN) is None


@pytest.mark.anyio
async def test_entry_rejects_changed_target_before_starting_process(automatic):
    """A target changed after ready observation cannot start a forward before original identity validation."""
    h = automatic
    h.target_transform = lambda target: replace(target, credential_digest="0" * 64)
    app = h.make()
    app.state.service._id_factory = lambda: SID
    app.state.service._transaction_id_factory = lambda: TXN
    async with app.router.lifespan_context(app):
        with pytest.raises(SandboxCreateFailed) as caught:
            await app.state.service.create()
        assert caught.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
        assert not h.sessions and not h.api.objects()
        assert await app.state.lifecycle_store.get(SID) is None


@pytest.mark.anyio
async def test_unready_resources_are_only_reread_until_ready(automatic, monkeypatch):
    """Readiness polling does not repeat creation POSTs or start an entry before the original Pods become ready."""
    h = automatic
    app = identified_app(h)
    original = h.backend._created_entry_state
    observations = []

    def state(api, operation, deadline):
        """Expose a running unready original once, followed by its independently ready state."""
        h.api.startup["proxy"] = "not-ready" if not observations else "running"
        status = original(api, operation, deadline)
        observations.append(status)
        return status

    monkeypatch.setattr(h.backend, "_created_entry_state", state)
    async with app.router.lifespan_context(app):
        await app.state.service.create()
        assert observations == ["running", "ready"] and len(h.sessions) == 1
        assert len([item for item in h.api.trace if item[0] == "create"]) == 8
        await app.state.service.delete(SID, delete_workspace=True)


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["timeout", "process"])
async def test_entry_failure_rolls_back_compute_and_private_pvc(automatic, monkeypatch, failure):
    """No activation or retained PVC is allowed after an entry failed and all cleanup is confirmed."""
    h = automatic
    monkeypatch.setenv("AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", "agent-sandbox-workspace")
    if failure == "timeout":
        monkeypatch.setattr(kubernetes_runtime, "ENTRY_READY_TIMEOUT_SECONDS", 0)
    else:
        h.open_error = True
    app = identified_app(h)
    async with app.router.lifespan_context(app):
        with pytest.raises(SandboxCreateFailed) as caught:
            await app.state.service.create()
        assert caught.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
        assert not h.api.objects() and app.state.kubernetes_entry_manager.session_count == 0
        assert await app.state.lifecycle_store.get(SID) is None
        assert await app.state.lifecycle_store.get_credential(SID, TXN) is None
        assert app.state.service._creates_in_progress == 0
        assert len(h.sessions) == (0 if failure == "timeout" else 1)


@pytest.mark.anyio
async def test_capacity_rejection_after_creation_preserves_other_entry(automatic):
    """Atomic session admission may reject completed resources; only this new transaction is compensated."""
    from test_kubernetes_local_entry import target_snapshot
    from control_plane.runtime.kubernetes.proxy_spec import proxy_pod_name
    from control_plane.runtime.kubernetes.proxy_service import proxy_service_name
    h = automatic
    h.capacity = 1
    other = "sbx_" + "f" * 32
    h.external_target = target_snapshot(sandbox_id=other, transaction_id="txn_" + "f" * 32,
        proxy_pod_name=proxy_pod_name(other), proxy_service_name=proxy_service_name(other))
    app = identified_app(h)
    async with app.router.lifespan_context(app):
        await app.state.kubernetes_entry_manager.open(other)
        with pytest.raises(SandboxCreateFailed) as caught:
            await app.state.service.create()
        assert caught.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
        assert not h.api.objects() and len(h.sessions) == 1 and h.sessions[0].is_active
        assert app.state.kubernetes_entry_manager.session_count == 1
        assert await app.state.lifecycle_store.get(SID) is None


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["close", "resources", "uid"])
async def test_uncertain_cleanup_retains_pending_and_credential(automatic, failure):
    """Unconfirmed session closure or precise UID rollback cannot be reported as a clean create failure."""
    h = automatic
    app = identified_app(h)
    if failure == "close":
        h.open_error = h.close_error = True
    elif failure == "resources":
        h.open_error = True
        h.api.stuck.add("container")
        h.api.faults[("delete", "container")] = ConnectionError("private SDK output")
    else:
        original = h.backend._created_entry_state

        def replacement(api, operation, deadline):
            """Replace the proxy UID after creation; neither open nor compensation may adopt the new object."""
            h.api.proxy_pod.metadata.uid = "a5667ab6-3f02-40ba-9b29-b1967503a3f0"
            return original(api, operation, deadline)

        h.backend._created_entry_state = replacement
    async with app.router.lifespan_context(app):
        with pytest.raises(SandboxCleanupFailed) as caught:
            await app.state.service.create()
        assert caught.value.creation_failure.resource_state in {CreationResourceState.REMAINING, CreationResourceState.UNKNOWN}
        assert (await app.state.lifecycle_store.get(SID)).management_state == "pending"
        assert await app.state.lifecycle_store.get_credential(SID, TXN) is not None
        assert h.api.objects() and app.state.service._creates_in_progress == 0
        if failure == "close":
            assert not h.api.delete_calls and app.state.kubernetes_entry_manager.session_count == 1
            h.close_error = False
        elif failure == "uid":
            assert h.api.proxy_pod.metadata.uid == "a5667ab6-3f02-40ba-9b29-b1967503a3f0" and not h.sessions
        h.api.faults.clear()
        h.api.stuck.clear()
        if failure != "uid":
            await app.state.service.delete(SID, delete_workspace=True)


@pytest.mark.anyio
async def test_cancellation_during_open_waits_for_late_session_close(automatic):
    """Repeated request cancellation holds creation capacity until startup and original resource cleanup finish."""
    h = automatic
    h.open_gate.clear()
    h.close_gate.clear()
    app = identified_app(h)
    async with app.router.lifespan_context(app):
        task = asyncio.create_task(app.state.service.create())
        try:
            while not h.sessions:
                await asyncio.sleep(0)
            await h.sessions[0].started.wait()
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            h.open_gate.set()
            await asyncio.wait_for(h.closing.wait(), 2)
            assert not task.done() and app.state.service._creates_in_progress == 1
            assert h.api.objects() and not h.api.delete_calls
            h.close_gate.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not h.api.objects() and h.sessions[0].closed.is_set()
            assert await app.state.lifecycle_store.get(SID) is None
            assert app.state.service._creates_in_progress == 0
        finally:
            h.open_gate.set()
            h.close_gate.set()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["missing", "unknown-before", "unknown-after"])
async def test_activation_failure_keeps_existing_service_compensation_semantics(automatic, mode):
    """An acknowledged entry is removed only on proven activation failure, never on an uncertain DB commit."""
    h = automatic
    store = ActivationStore(h.database)
    store.mode = mode
    app = identified_app(h, store=store)
    async with app.router.lifespan_context(app):
        expected = SandboxMetadataConflict if mode == "missing" else SandboxMetadataUnavailable
        with pytest.raises(expected):
            await app.state.service.create()
        if mode == "missing":
            assert not h.api.objects() and h.sessions[0].closed.is_set()
            assert await store.get(SID) is None
        else:
            assert h.api.objects() and h.sessions[0].is_active
            assert (await store.get(SID)).management_state == ("active" if mode == "unknown-after" else "pending")
            assert await store.get_credential(SID, TXN) is not None
            await app.state.service.delete(SID, delete_workspace=True)


@pytest.mark.anyio
async def test_cancel_after_entry_success_still_compensates_original(automatic, monkeypatch):
    """Cancellation at the return boundary cannot leave the successfully opened original session or resources."""
    h = automatic
    app = identified_app(h)
    original_resources = h.backend._create_resources
    original_entry = h.backend._prepare_created_entry
    owner = []

    async def resources(spec):
        """Capture the actual Runtime task before it awaits a separately owned preparation task."""
        owner.append(asyncio.current_task())
        return await original_resources(spec)

    async def entry(operation):
        """Complete entry startup, then make the waiting Runtime observe cancellation before success."""
        await original_entry(operation)
        owner[0].cancel()

    monkeypatch.setattr(h.backend, "_create_resources", resources)
    monkeypatch.setattr(h.backend, "_prepare_created_entry", entry)
    async with app.router.lifespan_context(app):
        with pytest.raises(asyncio.CancelledError):
            await app.state.service.create()
        assert not h.api.objects() and h.sessions[0].closed.is_set()
        assert await app.state.lifecycle_store.get(SID) is None


@pytest.mark.anyio
@pytest.mark.parametrize("status", ["running", "ready"])
async def test_readiness_budget_expires_without_opening_new_session(automatic, monkeypatch, status):
    """A completed read cannot extend the finite budget or start a process after its deadline."""
    h = automatic
    app = identified_app(h)
    clock = SimpleNamespace(now=0.0)
    monkeypatch.setattr(kubernetes_runtime, "monotonic", lambda: clock.now)
    monkeypatch.setattr(kubernetes_runtime, "ENTRY_READY_TIMEOUT_SECONDS", 1)

    def state(api, operation, deadline):
        """Consume the remaining read budget using an event-independent deterministic clock."""
        clock.now = 1.0
        return status

    monkeypatch.setattr(h.backend, "_created_entry_state", state)
    async with app.router.lifespan_context(app):
        with pytest.raises(SandboxCreateFailed) as caught:
            await app.state.service.create()
        assert caught.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
        assert not h.sessions and not h.api.objects()
        assert await app.state.lifecycle_store.get(SID) is None


@pytest.mark.anyio
async def test_original_secret_credential_mismatch_is_not_accepted(automatic, monkeypatch):
    """A validly shaped but different Secret cannot be accepted using only healthy Pods and matching UIDs."""
    h = automatic
    app = identified_app(h)
    original = h.backend._created_entry_state

    def changed(api, operation, deadline):
        """Alter only credential bytes after resources exist, retaining the original declared digest."""
        h.api.secret.data["access-token"] = base64.b64encode(b"R" * 43).decode()
        return original(api, operation, deadline)

    monkeypatch.setattr(h.backend, "_created_entry_state", changed)
    async with app.router.lifespan_context(app):
        with pytest.raises(SandboxCreateFailed) as caught:
            await app.state.service.create()
        assert caught.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
        assert not h.sessions and not h.api.objects()
        assert await app.state.lifecycle_store.get(SID) is None


@pytest.mark.anyio
async def test_shutdown_waits_for_preparation_worker_and_resource_compensation(automatic, monkeypatch):
    """Application ownership and RunLock outlive a blocked readiness worker and its precise rollback."""
    h = automatic
    app = identified_app(h)
    entered, release = threading.Event(), threading.Event()

    def state(api, operation, deadline):
        """Retain an in-flight synchronous observation until shutdown ownership has been checked."""
        entered.set()
        assert release.wait(3)
        return "running"

    monkeypatch.setattr(h.backend, "_created_entry_state", state)
    context = app.router.lifespan_context(app)
    await context.__aenter__()
    creating = asyncio.create_task(app.state.service.create())
    closing = None
    contender = SQLiteRunLock(h.database)
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        closing = asyncio.create_task(context.__aexit__(None, None, None))
        await asyncio.sleep(0)
        closing.cancel()
        await asyncio.sleep(0)
        closing.cancel()
        await asyncio.sleep(0)
        assert not closing.done() and h.api.objects()
        with pytest.raises(LifecycleRunLockBusy):
            contender.acquire()
        release.set()
        with pytest.raises(SandboxCreateFailed):
            await creating
        with pytest.raises(asyncio.CancelledError):
            await closing
        assert app.state.kubernetes_entry_manager.is_closed and not h.api.objects() and not h.sessions
        assert await app.state.lifecycle_store.get(SID) is None
        contender.acquire()
    finally:
        release.set()
        tasks = (creating,) if closing is None else (creating, closing)
        await asyncio.gather(*tasks, return_exceptions=True)
        contender.release()


@pytest.mark.anyio
async def test_activation_identity_conflict_preserves_replacement_credential(automatic, monkeypatch):
    """Original entry/resource compensation cannot erase a newer transaction's SQLite row or credential."""
    h = automatic
    store = SQLiteLifecycleStore(h.database)
    app = identified_app(h, store=store)
    replacement = []

    async def activate(sandbox_id, transaction_id, *, expected_version):
        """Commit a legitimate competing record before the original activation can acquire its CAS."""
        original = await store.get(sandbox_id)
        await store.delete(sandbox_id, transaction_id, expected_version=expected_version)
        changed = replace(original, transaction_id="txn_" + "f" * 32)
        credential = SandboxAccessCredential(sandbox_id, changed.transaction_id, "Z" * 43)
        await store.create(changed, credential=credential)
        replacement.append((changed, credential))
        raise LifecycleStoreConflict("transaction_mismatch")

    monkeypatch.setattr(store, "activate", activate)
    async with app.router.lifespan_context(app):
        with pytest.raises(SandboxMetadataConflict):
            await app.state.service.create()
        assert not h.api.objects() and h.sessions[0].closed.is_set()
        assert await store.get(SID) == replacement[0][0]
        assert await store.get_credential(SID, replacement[0][0].transaction_id) == replacement[0][1]
        with pytest.raises(LifecycleStoreConflict):
            await store.get_credential(SID, TXN)
        assert (await store.list_sandboxes()).managed_count == 1
        assert app.state.service._creates_in_progress == 0
