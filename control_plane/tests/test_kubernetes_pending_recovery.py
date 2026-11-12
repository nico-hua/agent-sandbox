"""Opt-in recovery of original Kubernetes pending transactions with real temporary SQLite."""

import asyncio
import copy
import logging
import threading
from datetime import timedelta
from dataclasses import replace

import pytest
from kubernetes import client

import control_plane.app as app_module
import control_plane.core.service as service_module
from control_plane.app import create_app
from control_plane.core.models import SandboxAccessCredential, SandboxLifecycleRecord
from control_plane.core.errors import KubernetesConfigurationError, KubernetesConnectionFailed, KubernetesPermissionDenied, KubernetesTimeout
from control_plane.core.service import SandboxService
from control_plane.runtime.kubernetes.local_entry_manager import KubernetesLocalEntryManager
from control_plane.runtime.kubernetes.local_entry import LocalEntryError
from control_plane.runtime.kubernetes.local_entry_target import resolve_local_entry_target
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore
from control_plane.storage.run_lock import LifecycleRunLockBusy, SQLiteRunLock
from kubernetes_proxy_fakes import FakeProxyClient
from test_kubernetes_local_entry_manager import FakeSession, manager_harness
from test_kubernetes_persistent_workspace import persistence_clock
from test_kubernetes_proxy_lifecycle import authenticated_spec, runtime, proxy_clock
from test_kubernetes_query import CREATED, EXPIRES, SID, TXN
from test_pending_recovery import RecoveryStore


@pytest.fixture
def recovery_harness(manager_harness, persistence_clock, monkeypatch):
    """Use real Runtime/manager/Service boundaries, replacing only SDK transport and process handles."""
    h = manager_harness
    h.api = FakeProxyClient()
    h.backend = runtime(h.api)
    h.events = []
    monkeypatch.setattr(service_module, "_utc_now", lambda: CREATED)

    async def resolve(backend, sandbox_id):
        """Verify every target through the actual Kubernetes inventory rather than a saved fake snapshot."""
        h.targets[sandbox_id] = await resolve_local_entry_target(backend, sandbox_id)
        return h.targets[sandbox_id]

    def session_factory(backend, sandbox_id):
        """Replace kubectl process creation while preserving real manager identity and ownership checks."""
        session = FakeSession(h, sandbox_id)
        h.sessions.append(session)
        h.events.append("session")
        return session

    h.make_manager = lambda **kwargs: KubernetesLocalEntryManager(
        h.backend, session_factory=session_factory, target_resolver=resolve, **kwargs,
    )
    return h


async def interrupted(h, tmp_path, *, persistent=False, store_type=SQLiteLifecycleStore):
    """Model process exit after Runtime completion but before acknowledging the original pending record."""
    if persistent:
        h.backend = runtime(h.api, workspace_storage_class="agent-sandbox-workspace")
    spec = authenticated_spec()
    store = store_type(tmp_path / "recovery.sqlite3")
    await store.initialize()
    record = SandboxLifecycleRecord(SID, TXN, CREATED, EXPIRES)
    credential = SandboxAccessCredential(SID, TXN, spec.access_token)
    await store.create(record, credential=credential)
    await h.backend.create(spec)
    h.api.trace.clear()
    h.originals = copy.deepcopy(h.api.objects())
    h.manager = h.make_manager()
    return store, record, credential


def application(h, store, *, initial_ids=(SID,), enabled=True, **options):
    """Assemble explicitly owned SQLite startup recovery without accessing a real cluster or process."""
    return create_app(
        h.backend, store=store, api_key="synthetic-recovery-admin", reaper_enabled=False,
        pending_recovery_enabled=enabled, kubernetes_entry_manager=h.manager,
        kubernetes_initial_sandbox_ids=initial_ids, **options,
    )


async def unchanged(h, store, record, credential):
    """Require resource identity/content and persisted credentials to survive recovery unchanged."""
    same_resources = h.api.objects() == h.originals
    assert same_resources, "recovery modified Kubernetes resources"
    assert all(verb in {"read", "list"} for verb, _ in h.api.trace)
    assert await store.get_credential(SID, TXN) == credential
    assert "Q" * 43 not in repr(record)


@pytest.mark.anyio
@pytest.mark.parametrize("persistent", [False, True])
async def test_startup_prepares_entry_before_recovering_original_record(recovery_harness, tmp_path, persistent):
    """Eight/nine-resource recovery changes only state/version after the explicitly owned entry is usable."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path, persistent=persistent)
    app = application(h, store)
    async with app.router.lifespan_context(app):
        result = app.state.pending_recovery_result
        assert result.recovered == 1 and result.error is None
        assert await store.get(SID) == replace(record, management_state="active", version=1)
        assert h.manager.session_count == 1 and len(h.sessions) == 1
        endpoint = await app.state.service.get_endpoint(SID)
        matches = endpoint.access_token == credential.access_token
        assert matches, "recovery replaced the original credential"
        assert endpoint.url == "http://127.0.0.1:43123"
        await unchanged(h, store, record, credential)
    assert h.manager.is_closed and h.sessions[0].closed.is_set()


@pytest.mark.anyio
async def test_kubernetes_unavailable_is_fixed_and_does_not_block_startup(
    recovery_harness, tmp_path, caplog,
):
    """An inspection outage stops one recovery batch without misreporting Docker or exposing SDK text."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)
    h.api.faults[("list", "pod")] = client.ApiException(status=403, reason="private-cluster-diagnostic")
    app = application(h, store, initial_ids=())
    with caplog.at_level(logging.INFO):
        async with app.router.lifespan_context(app):
            result = app.state.pending_recovery_result
            assert result.error == "kubernetes_unavailable"
            assert (result.items[0].outcome, result.items[0].reason) == ("failed", "kubernetes_unavailable")
            assert await store.get(SID) == record
    assert "kubernetes_unavailable" in caplog.text and "docker_unavailable" not in caplog.text
    assert "private-cluster-diagnostic" not in caplog.text and "Q" * 43 not in caplog.text
    await unchanged(h, store, record, credential)


@pytest.mark.anyio
@pytest.mark.parametrize("fault,reason", [
    ("partial", "resources_partial"), ("not_found", "resources_not_found"),
    ("conflict", "resource_ownership_conflict"), ("not-ready", "sandbox_not_ready"),
    ("no-entry", "sandbox_endpoint_unavailable"),
    ("missing-credential", "sandbox_endpoint_auth_unavailable"),
    ("wrong-credential", "sandbox_endpoint_auth_unavailable"),
    ("expired", "sandbox_expired"), ("created-at", "sandbox_identity_mismatch"),
])
async def test_unqualified_original_is_skipped_without_resource_or_metadata_changes(
    recovery_harness, tmp_path, fault, reason,
):
    """Integrity, readiness, original identity, a live entry and a matching credential are separate recovery gates."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path, persistent=True)
    h.backend.bind_local_entry_manager(h.manager)
    if fault not in {"not-ready", "no-entry", "not_found", "conflict", "partial"}:
        await h.manager.open(SID)
    if fault == "partial":
        h.api.proxy_service = None
    elif fault == "not_found":
        for kind in ("pod", "service", "secret", "network_policy", "persistent_volume_claim"):
            for resource in h.api.objects(kind):
                h.api._store(kind, None, role=resource.metadata.labels["io.agent-sandbox.resource"])
    elif fault == "conflict":
        h.api.pod.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    elif fault == "not-ready":
        h.api.startup["proxy"] = "not-ready"
    elif fault in {"missing-credential", "wrong-credential", "created-at"}:
        await store.delete(SID, TXN, expected_version=0)
        if fault == "missing-credential":
            credential = None
        elif fault == "wrong-credential":
            credential = SandboxAccessCredential(SID, TXN, "R" * 43)
        else:
            record = replace(record, created_at=CREATED + timedelta(microseconds=1))
        await store.create(record, **({"credential": credential} if credential is not None else {}))
    h.originals = copy.deepcopy(h.api.objects())
    h.api.trace.clear()
    service = SandboxService(h.backend, store, clock=lambda: EXPIRES if fault == "expired" else CREATED)
    try:
        result = await service.recover_pending_once()
        assert result.error is None
        assert (result.items[0].outcome, result.items[0].reason) == ("skipped", reason)
        assert await store.get(SID) == record
        await unchanged(h, store, record, credential)
    finally:
        await h.manager.close_all()


@pytest.mark.anyio
@pytest.mark.parametrize("error_type", [
    KubernetesConfigurationError, KubernetesConnectionFailed, KubernetesPermissionDenied, KubernetesTimeout,
])
async def test_later_runtime_failure_keeps_kubernetes_classification(recovery_harness, tmp_path, monkeypatch, error_type):
    """Backend errors after complete inspection do not become generic or Docker recovery failures."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)

    async def unavailable(sandbox_id):
        """Model a sanitized Kubernetes failure after a successful integrity observation."""
        raise error_type()

    monkeypatch.setattr(h.backend, "get", unavailable)
    result = await SandboxService(h.backend, store, clock=lambda: CREATED).recover_pending_once()
    assert result.error == "kubernetes_unavailable"
    assert (result.items[0].outcome, result.items[0].reason) == ("failed", "kubernetes_unavailable")
    assert await store.get(SID) == record
    await unchanged(h, store, record, credential)


@pytest.mark.anyio
async def test_default_is_disabled_and_one_explicit_round_is_bounded(recovery_harness, tmp_path):
    """Explicit entries may open without recovery; enabling one batch neither scans nor opens other records."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)
    other = SandboxLifecycleRecord("sbx_" + "f" * 32, "txn_" + "f" * 32, CREATED, EXPIRES)
    await store.create(other)
    app = application(h, store, enabled=False)
    async with app.router.lifespan_context(app):
        assert app.state.pending_recovery_result is None
        assert await store.get(SID) == record
        assert h.manager.session_count == 1
    restarted = runtime(h.api, proxy_image=None)
    h.backend = restarted
    h.manager = h.make_manager()
    app = application(h, store, pending_recovery_batch_limit=1)
    async with app.router.lifespan_context(app):
        assert app.state.pending_recovery_result.recovered == 1
        assert len(app.state.pending_recovery_result.items) == 1
        assert await store.get(other.sandbox_id) == other
        assert h.manager.session_count == 1 and len(h.sessions) == 2
        await unchanged(h, store, record, credential)


@pytest.mark.anyio
async def test_entry_failure_aborts_before_any_recovery(recovery_harness, tmp_path, monkeypatch):
    """A failed explicit entry is cleaned and never bypassed to activate a pending record."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)
    h.open_error = True

    async def forbidden(**kwargs):
        """Detect any recovery query attempted before every explicit entry is prepared."""
        pytest.fail("entry failure must precede recovery selection")

    monkeypatch.setattr(store, "list_pending", forbidden)
    app = application(h, store)
    with pytest.raises(LocalEntryError):
        async with app.router.lifespan_context(app):
            pytest.fail("entry failure cannot accept requests")
    assert app.state.pending_recovery_result is None and h.manager.is_closed
    assert await store.get(SID) == record
    lock = SQLiteRunLock(store.database_path)
    lock.acquire()
    lock.release()
    await unchanged(h, store, record, credential)


@pytest.mark.anyio
async def test_session_capacity_failure_cleans_prepared_entries_before_recovery(recovery_harness, tmp_path):
    """An over-capacity explicit list fails before recovery and closes the already prepared first session."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)
    h.manager = h.make_manager(max_sessions=1)
    app = application(h, store, initial_ids=(SID, "sbx_" + "f" * 32))
    with pytest.raises(LocalEntryError) as captured:
        async with app.router.lifespan_context(app):
            pytest.fail("session capacity failure cannot serve requests")
    assert captured.value.code == "local_entry_capacity_exceeded"
    assert app.state.pending_recovery_result is None
    assert h.manager.is_closed and len(h.sessions) == 1 and h.sessions[0].closed.is_set()
    assert await store.get(SID) == record
    await unchanged(h, store, record, credential)


@pytest.mark.anyio
async def test_startup_order_includes_entry_recovery_reaper_and_request_admission(recovery_harness, tmp_path, monkeypatch):
    """The lifespan never yields or starts reaping before entry validation and exact activation complete."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)
    selection = store.list_pending
    reaper_started = asyncio.Event()

    async def list_pending(**kwargs):
        """Require the entry to exist under the exclusive database lock before recovery selection."""
        assert h.manager.session_count == 1 and h.sessions[0].is_active
        contender = SQLiteRunLock(store.database_path)
        with pytest.raises(LifecycleRunLockBusy):
            contender.acquire()
        contender.release()
        h.events.append("recovery")
        return await selection(**kwargs)

    async def reaper(service, **kwargs):
        """Observe only an acknowledged active record after the startup recovery round."""
        assert (await store.get(SID)).management_state == "active"
        h.events.append("reaper")
        reaper_started.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(store, "list_pending", list_pending)
    monkeypatch.setattr(app_module, "run_reaper", reaper)
    app = create_app(h.backend, store=store, api_key="synthetic-admin", pending_recovery_enabled=True,
                     kubernetes_entry_manager=h.manager, kubernetes_initial_sandbox_ids=(SID,))
    async with app.router.lifespan_context(app):
        assert h.events == ["session", "recovery"]
        assert app.state.pending_recovery_result.recovered == 1
        await asyncio.wait_for(reaper_started.wait(), 2)
        assert h.events == ["session", "recovery", "reaper"]
    await unchanged(h, store, record, credential)


@pytest.mark.anyio
async def test_lost_activation_acknowledgement_retains_actual_commit_and_resources(recovery_harness, tmp_path):
    """A committed activation with a lost response aborts startup without resource compensation or retry."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path, store_type=RecoveryStore)
    store.fail_after_commit = True
    app = application(h, store)
    with pytest.raises(LifecycleStoreError):
        async with app.router.lifespan_context(app):
            pytest.fail("activation uncertainty cannot be acknowledged as successful startup")
    assert app.state.pending_recovery_result.error == "sandbox_metadata_unavailable"
    assert await store.get(SID) == replace(record, management_state="active", version=1)
    assert h.manager.is_closed and h.sessions[0].closed.is_set()
    await unchanged(h, store, record, credential)


@pytest.mark.anyio
@pytest.mark.parametrize("late_error", [False, True])
async def test_repeated_cancel_keeps_lock_through_activation_and_entry_close(recovery_harness, tmp_path, late_error):
    """Repeated cancellation cannot abandon a late commit, owned entry or SQLite application lock."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path, store_type=RecoveryStore)
    store.block_activation = True
    store.fail_after_commit = late_error
    h.close_gate.clear()
    app = application(h, store)
    context = app.router.lifespan_context(app)
    startup = asyncio.create_task(context.__aenter__())
    contender = SQLiteRunLock(store.database_path)
    try:
        assert await asyncio.to_thread(store.entered.wait, 2)
        startup.cancel()
        await asyncio.sleep(0)
        startup.cancel()
        await asyncio.sleep(0)
        assert not startup.done()
        with pytest.raises(LifecycleRunLockBusy):
            contender.acquire()
        store.release.set()
        await asyncio.wait_for(h.closing.wait(), 2)
        with pytest.raises(LifecycleRunLockBusy):
            contender.acquire()
        startup.cancel()
        await asyncio.sleep(0)
        assert not startup.done() and not h.manager.is_closed
        h.close_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await startup
        assert store.finished.is_set() and h.manager.is_closed
        assert await store.get(SID) == replace(record, management_state="active", version=1)
        contender.acquire()
        await unchanged(h, store, record, credential)
    finally:
        store.release.set()
        h.close_gate.set()
        await asyncio.gather(startup, return_exceptions=True)
        contender.release()


@pytest.mark.anyio
async def test_observed_resource_change_never_reaches_activation(recovery_harness, tmp_path, monkeypatch):
    """A positive list/read UID change remains skipped even if a previous entry was already usable."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)
    original_read = h.api._read

    def changed(kind, name, namespace, kwargs):
        """Return evidence of one replacement between the list and exact read without mutating the fixture."""
        observed = original_read(kind, name, namespace, kwargs)
        if kind == "pod" and observed.metadata.labels["io.agent-sandbox.resource"] == "container":
            observed.metadata.uid = "a5667ab6-3f02-40ba-9b29-b1967503a3f0"
        return observed

    monkeypatch.setattr(h.api, "_read", changed)
    result = await SandboxService(h.backend, store, clock=lambda: CREATED).recover_pending_once()
    assert result.error is None
    assert (result.items[0].outcome, result.items[0].reason) == ("skipped", "pending_record_changed")
    assert await store.get(SID) == record
    await unchanged(h, store, record, credential)


@pytest.mark.anyio
async def test_concurrent_record_commit_invalidates_original_recovery(recovery_harness, tmp_path):
    """A second SQLite connection changing the saved version cannot be overwritten by a late recovery check."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)
    h.backend.bind_local_entry_manager(h.manager)
    await h.manager.open(SID)
    entered, release = threading.Event(), threading.Event()
    h.api.barriers[("list", "pod")] = (entered, release)
    operation = asyncio.create_task(SandboxService(h.backend, store, clock=lambda: CREATED).recover_pending_once())
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        independent = SQLiteLifecycleStore(store.database_path)
        active = await independent.activate(SID, TXN, expected_version=0)
        release.set()
        result = await operation
        assert (result.items[0].outcome, result.items[0].reason) == ("skipped", "pending_record_changed")
        assert await store.get(SID) == active
        await unchanged(h, store, record, credential)
    finally:
        release.set()
        await asyncio.gather(operation, return_exceptions=True)
        await h.manager.close_all()


@pytest.mark.anyio
async def test_unconfirmed_entry_close_retains_application_lock(recovery_harness, tmp_path):
    """A startup failure cannot discard a failed-close session or release SQLite ownership prematurely."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path, store_type=RecoveryStore)
    store.fail_after_commit = True
    h.close_error = True
    app = application(h, store)
    contender = SQLiteRunLock(store.database_path)
    try:
        with pytest.raises(LocalEntryError) as captured:
            async with app.router.lifespan_context(app):
                pytest.fail("uncertain activation cannot accept requests")
        assert captured.value.code == "local_entry_cleanup_failed"
        assert not h.manager.is_closed and h.manager.session_count == 1
        assert app.state.kubernetes_entry_run_lock is not None
        with pytest.raises(LifecycleRunLockBusy):
            contender.acquire()
        assert await store.get(SID) == replace(record, management_state="active", version=1)
        await unchanged(h, store, record, credential)
    finally:
        h.close_error = False
        await h.manager.close_all()
        app.state.kubernetes_entry_run_lock.release()
        contender.release()
