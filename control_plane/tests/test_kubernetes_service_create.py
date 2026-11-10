"""Real Service and temporary SQLite creation/compensation through the Kubernetes fake SDK."""

import asyncio
from dataclasses import replace
from types import SimpleNamespace

import pytest
from kubernetes import client

from control_plane.core.errors import (
    CreationResourceState, SandboxCleanupFailed, SandboxCreateFailed, SandboxCreateUncertain,
    SandboxMetadataCleanupFailed, SandboxMetadataConflict, SandboxMetadataUnavailable,
    SandboxDeleteConflict,
)
from control_plane.core.models import SandboxAccessCredential
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore, LifecycleStoreError
from kubernetes_proxy_fakes import FakeProxyClient
from test_kubernetes_proxy_lifecycle import proxy_clock, runtime
from test_kubernetes_local_entry_manager import manager_harness
from test_kubernetes_query import CREATED, SID, TXN


@pytest.fixture
def anyio_backend():
    """Run real threaded SQLite and SDK fake operations under asyncio."""
    return "asyncio"


class ActivationStore(SQLiteLifecycleStore):
    """Inject activation outcomes around actual SQLite commits, never fabricate successful metadata."""

    def __init__(self, path):
        """Keep fault controls inert until the Service performs its real activation."""
        super().__init__(path)
        self.mode = None
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.release.set()
        self.replace_on_delete = False

    async def activate(self, sandbox_id, transaction_id, *, expected_version):
        """Model proven missing/conflicting records, uncertain commits and cancellation after a durable active write."""
        if self.mode in {"missing", "conflict"}:
            changed = await self.mark_deleting(sandbox_id, transaction_id, expected_version=expected_version)
            if self.mode == "missing":
                await super().delete(sandbox_id, transaction_id, expected_version=changed.version)
        if self.mode == "unknown-before":
            raise LifecycleStoreError("private database diagnostic")
        active = await super().activate(sandbox_id, transaction_id, expected_version=expected_version)
        if self.mode == "unknown-after":
            raise LifecycleStoreError("private database diagnostic")
        if self.mode == "held":
            self.entered.set()
            await self.release.wait()
        return active

    async def delete(self, sandbox_id, transaction_id, *, expected_version):
        """Expose a real old-holder CAS conflict without allowing it to erase a new transaction's credential."""
        if self.replace_on_delete:
            self.replace_on_delete = False
            current = await self.get(sandbox_id)
            await super().delete(sandbox_id, transaction_id, expected_version=expected_version)
            replacement = replace(current, transaction_id="txn_" + "f" * 32, version=0, management_state="pending")
            await self.create(replacement, credential=SandboxAccessCredential(sandbox_id, replacement.transaction_id, "Z" * 43))
        await super().delete(sandbox_id, transaction_id, expected_version=expected_version)


@pytest.fixture
async def creation(tmp_path, proxy_clock):
    """Use a real isolated database and real Runtime while observing Service prewrite and compensation arguments."""
    store = ActivationStore(tmp_path / "service.sqlite3")
    await store.initialize()
    api = FakeProxyClient()
    backend = runtime(api)
    h = SimpleNamespace(store=store, api=api, backend=backend, specs=[], deletes=[], writes=[])
    original_create, original_delete = backend.create, backend.delete

    async def create(spec):
        """Check actual atomic pending and credential state before invoking the real Kubernetes transaction."""
        h.specs.append(spec)
        record = await store.get(spec.sandbox_id)
        credential = await store.get_credential(spec.sandbox_id, spec.transaction_id)
        assert record.management_state == "pending" and record.version == 0
        assert credential.access_token == spec.access_token
        h.writes.append(record)
        await original_create(spec)

    async def delete(sandbox_id, **kwargs):
        """Observe actual Service compensation flags while retaining Runtime identity and cleanup checks."""
        h.deletes.append((sandbox_id, kwargs))
        await original_delete(sandbox_id, **kwargs)

    backend.create, backend.delete = create, delete
    h.service = SandboxService(backend, store, id_factory=lambda: SID, transaction_id_factory=lambda: TXN,
                                clock=lambda: CREATED, max_instances=1)
    yield h


@pytest.mark.anyio
async def test_original_missing_activation_is_compensated_through_real_service(creation):
    """Reproduce the former delete_workspace=True rejection after a proven missing activation record."""
    h = creation
    h.store.mode = "missing"
    with pytest.raises(SandboxMetadataConflict):
        await h.service.create()
    assert h.deletes == [(SID, {"delete_workspace": True, "expected_transaction_id": TXN})]
    assert not h.api.objects()
    assert await h.store.get(SID) is None and await h.store.get_credential(SID, TXN) is None
    assert (await h.store.list_sandboxes()).managed_count == 0


@pytest.mark.anyio
@pytest.mark.parametrize("delete_workspace", [False, True])
async def test_service_success_commits_active_and_deletes_original_credential(creation, delete_workspace):
    """Successful activation and cleanup use actual SQLite identity, capacity and credential CAS semantics."""
    h = creation
    created = await h.service.create()
    record = await h.store.get(created.sandbox_id)
    credential = await h.store.get_credential(created.sandbox_id, record.transaction_id)
    assert record.management_state == "active" and record.version == 1
    assert record.transaction_id == h.specs[0].transaction_id and credential.access_token == h.specs[0].access_token
    assert len(h.api.objects()) == 8 and len(h.writes) == 1
    observed = await h.service.get(created.sandbox_id)
    assert observed.transaction_id == record.transaction_id and observed.expires_at == record.expires_at
    await h.service.delete(created.sandbox_id, delete_workspace=delete_workspace)
    assert not h.api.objects() and await h.store.get(created.sandbox_id) is None
    assert await h.store.get_credential(created.sandbox_id, record.transaction_id) is None
    assert (await h.store.list_sandboxes()).managed_count == 0
    # The same one-slot Service can create again only after confirmed metadata cleanup.
    await h.service.create()
    await h.service.delete(SID)


@pytest.mark.anyio
async def test_conflicting_activation_cleans_resources_but_preserves_newer_version(creation):
    """A real SQLite CAS conflict authorizes only resource compensation, not deletion of newer metadata."""
    h = creation
    h.store.mode = "conflict"
    with pytest.raises(SandboxMetadataConflict):
        await h.service.create()
    record = await h.store.get(SID)
    assert record.management_state == "deleting" and record.version == 1
    assert await h.store.get_credential(SID, TXN) is not None
    assert not h.api.objects() and h.deletes[0][1]["delete_workspace"] is True
    await h.service.delete(SID)
    assert await h.store.get(SID) is None


@pytest.mark.anyio
@pytest.mark.parametrize("mode,state", [("unknown-before", "pending"), ("unknown-after", "active")])
async def test_unknown_activation_outcome_keeps_resources_metadata_and_capacity(mode, state, creation):
    """A database exception does not prove absence of an activation commit or justify destructive compensation."""
    h = creation
    h.store.mode = mode
    with pytest.raises(SandboxMetadataUnavailable):
        await h.service.create()
    assert (await h.store.get(SID)).management_state == state
    assert await h.store.get_credential(SID, TXN) is not None
    assert len(h.api.objects()) == 8 and not h.deletes
    assert (await h.store.list_sandboxes()).managed_count == 1
    await h.service.delete(SID)


@pytest.mark.anyio
@pytest.mark.parametrize("scenario,evidence", [("unchanged", CreationResourceState.UNCHANGED),
    ("rolled-back", CreationResourceState.ROLLED_BACK), ("remaining", CreationResourceState.REMAINING),
    ("unknown", CreationResourceState.UNKNOWN)])
async def test_actual_runtime_failure_evidence_controls_metadata_retention(scenario, evidence, creation):
    """Real transaction evidence, rather than the exception's name, decides whether pending and secret survive."""
    h = creation
    if scenario == "unchanged":
        h.api.no_commit.add("network-policy")
        h.api.faults[("create", "network-policy")] = client.ApiException(status=422)
    elif scenario == "rolled-back":
        h.api.startup["proxy"] = "failed"
    elif scenario == "remaining":
        h.api.startup["proxy"] = "failed"
        h.api.stuck.add("proxy")
    else:
        h.api.no_commit.add("proxy-service")
        h.api.faults[("create", "proxy-service")] = TimeoutError("private backend diagnostic")
    with pytest.raises((SandboxCreateFailed, SandboxCleanupFailed, SandboxCreateUncertain)) as captured:
        await h.service.create()
    assert captured.value.creation_failure.resource_state is evidence
    retained = evidence in {CreationResourceState.REMAINING, CreationResourceState.UNKNOWN}
    assert (await h.store.get(SID) is not None) is retained
    assert (await h.store.get_credential(SID, TXN) is not None) is retained
    assert (await h.store.list_sandboxes()).managed_count == int(retained)
    h.api.stuck.clear()
    h.api.faults.clear()
    if retained:
        await h.service.delete(SID)
    assert not h.api.objects() and await h.store.get(SID) is None


@pytest.mark.anyio
async def test_repeated_cancel_after_activation_commit_finishes_full_compensation(creation):
    """A committed active result arriving after cancellation is reclaimed through Service's private cleanup path."""
    h = creation
    h.store.mode = "held"
    h.store.release.clear()
    creating = asyncio.create_task(h.service.create())
    await h.store.entered.wait()
    record = await h.store.get(SID)
    assert record.management_state == "active" and len(h.api.objects()) == 8
    creating.cancel()
    await asyncio.sleep(0)
    creating.cancel()
    await asyncio.sleep(0)
    assert not creating.done() and not h.deletes
    h.store.release.set()
    with pytest.raises(asyncio.CancelledError):
        await creating
    assert h.deletes[0][1]["delete_workspace"] is True
    assert not h.api.objects() and await h.store.get(SID) is None
    assert await h.store.get_credential(SID, TXN) is None


@pytest.mark.anyio
async def test_cleanup_failure_retains_metadata_and_secret_for_retry(creation):
    """Failure to remove an original Pod cannot release its lifecycle capacity or credential evidence."""
    h = creation
    h.store.mode = "held"
    h.store.release.clear()
    creating = asyncio.create_task(h.service.create())
    await h.store.entered.wait()
    h.api.stuck.add("proxy")
    creating.cancel()
    h.store.release.set()
    with pytest.raises(SandboxCleanupFailed):
        await creating
    assert (await h.store.get(SID)).management_state == "deleting"
    assert await h.store.get_credential(SID, TXN) is not None
    assert (await h.store.list_sandboxes()).managed_count == 1
    h.api.stuck.clear()
    await h.service.delete(SID)
    assert not h.api.objects() and await h.store.get(SID) is None


@pytest.mark.anyio
async def test_metadata_cas_cleanup_cannot_erase_replacement_transaction(creation):
    """A late old-holder delete cannot remove a new record or its independently persisted Token."""
    h = creation
    h.store.mode = "held"
    h.store.release.clear()
    h.store.replace_on_delete = True
    creating = asyncio.create_task(h.service.create())
    await h.store.entered.wait()
    creating.cancel()
    h.store.release.set()
    with pytest.raises(SandboxMetadataCleanupFailed):
        await creating
    record = await h.store.get(SID)
    assert record.transaction_id != TXN and record.management_state == "pending"
    credential = await h.store.get_credential(SID, record.transaction_id)
    assert credential.access_token == "Z" * 43 and not h.api.objects()
    await h.service.delete(SID)


@pytest.mark.parametrize("delete_workspace", [False, True])
@pytest.mark.parametrize("fault", ["pvc", "hostpath", "mixed-source", "unknown-version", "binding"])
@pytest.mark.anyio
async def test_temporary_cleanup_flag_does_not_admit_unknown_storage_or_layout(delete_workspace, fault, creation):
    """Accepting the private cleanup flag cannot authorize unsupported mounts, workspace bindings or versions."""
    h = creation
    await h.service.create()
    if fault == "pvc":
        h.api.pod.spec.volumes[0] = client.V1Volume(name="workspace", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name="not-managed"))
    elif fault == "hostpath":
        h.api.pod.spec.volumes[0] = client.V1Volume(name="workspace", host_path=client.V1HostPathVolumeSource(path="/not-a-supported-mount"))
    elif fault == "mixed-source":
        h.api.pod.spec.volumes[0].persistent_volume_claim = client.V1PersistentVolumeClaimVolumeSource(claim_name="not-managed")
    elif fault == "unknown-version":
        h.api.pod.metadata.annotations["io.agent-sandbox.kubernetes-proxy-version"] = "unknown"
    else:
        h.api.pod.metadata.annotations["io.agent-sandbox.workspace-id"] = "wsp_" + "f" * 32
    with pytest.raises(SandboxDeleteConflict):
        await h.backend.delete(SID, delete_workspace=delete_workspace, expected_transaction_id=TXN)
    assert not h.api.delete_calls
    assert await h.store.get_credential(SID, TXN) is not None


@pytest.mark.anyio
@pytest.mark.parametrize("value", [0, 1, "true", None, [], {}])
async def test_invalid_cleanup_flag_is_rejected_before_client_or_session_work(value):
    """Only an actual bool can select backend-private temporary cleanup semantics."""
    def forbidden(**kwargs):
        """Reject client acquisition on invalid Runtime arguments."""
        pytest.fail("invalid cleanup argument reached a client")

    with pytest.raises(ValueError):
        await runtime(None, client_factory=forbidden).delete(SID, delete_workspace=value)


@pytest.mark.anyio
async def test_late_completed_runtime_result_is_compensated_after_request_cancel(creation, monkeypatch):
    """Cancel after the real Runtime task is done but before Service receives it, exercising the second rollback."""
    import threading
    from control_plane.core import service as service_module
    h = creation
    original = service_module.wait_for_task
    completed = asyncio.Event()
    calls = 0

    async def hold_runtime_result(task):
        """Pause only Service's second wait, after the original Runtime worker has succeeded completely."""
        nonlocal calls
        result = await original(task)
        calls += 1
        if calls == 2:
            completed.set()
            await asyncio.Event().wait()
        return result

    monkeypatch.setattr(service_module, "wait_for_task", hold_runtime_result)
    entered, release = threading.Event(), threading.Event()
    h.api.barriers[("delete", "proxy")] = (entered, release)
    creating = asyncio.create_task(h.service.create())
    try:
        await completed.wait()
        assert (await h.store.get(SID)).management_state == "pending"
        creating.cancel()
        assert await asyncio.to_thread(entered.wait, 1)
        creating.cancel()
        await asyncio.sleep(0)
        assert not creating.done() and await h.store.get_credential(SID, TXN) is not None
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await creating
    assert h.deletes[0][1]["delete_workspace"] is True
    assert not h.api.objects() and await h.store.get(SID) is None
    assert (await h.store.list_sandboxes()).managed_count == 0


@pytest.mark.anyio
async def test_session_close_failure_preserves_service_recovery_evidence(creation, manager_harness):
    """Private compensation must retain lifecycle and credential evidence if the owned entry cannot close."""
    from control_plane.runtime.kubernetes.local_entry_target import resolve_local_entry_target
    h = creation
    control = manager_harness
    control.backend = h.backend
    manager = control.make()
    h.backend.bind_local_entry_manager(manager)
    h.store.mode = "held"
    h.store.release.clear()
    creating = asyncio.create_task(h.service.create())
    try:
        await h.store.entered.wait()
        control.targets[SID] = await resolve_local_entry_target(h.backend, SID)
        await manager.open(SID)
        control.close_error = True
        creating.cancel()
        h.store.release.set()
        with pytest.raises(SandboxCleanupFailed):
            await creating
        assert manager.session_count == 1 and not h.api.delete_calls
        assert (await h.store.get(SID)).management_state == "deleting"
        assert await h.store.get_credential(SID, TXN) is not None
        control.close_error = False
        await h.service.delete(SID)
        assert not h.api.objects() and manager.session_count == 0
        assert await h.store.get(SID) is None
    finally:
        h.store.release.set()
        control.close_error = False
        if not creating.done():
            creating.cancel()
        await asyncio.gather(creating, return_exceptions=True)
        await manager.close_all()
