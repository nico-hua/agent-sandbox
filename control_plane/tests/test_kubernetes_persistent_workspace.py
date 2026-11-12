"""Version-three private persistence through real Runtime orchestration and offline SDK state."""

import asyncio
import copy
import json
import threading

import pytest
from kubernetes import client

from control_plane.core.errors import (
    CreationResourceState, KubernetesConfigurationError, SandboxCleanupFailed,
    SandboxCreateFailed, SandboxDeleteConflict, SandboxQueryFailed,
)
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from kubernetes_proxy_fakes import FakeProxyClient, PREFIX, ROLE
from test_kubernetes_proxy_lifecycle import authenticated_spec, mutations, proxy_clock, runtime
from test_kubernetes_query import SID, TXN, CREATED

SC = "agent-sandbox-workspace"
VERSION = PREFIX + "kubernetes-proxy-version"
RECORD = PREFIX + "proxy-resource-uids"
STORAGE_CLASS = PREFIX + "private-workspace-storage-class"
WORKSPACE_UID = PREFIX + "private-workspace-uid"


@pytest.fixture
def persistence_clock(proxy_clock, monkeypatch):
    """Include PVC request and rollback budgets in the existing deterministic worker clock."""
    from control_plane.runtime.kubernetes import private_workspace
    monkeypatch.setattr(private_workspace, "monotonic", lambda: proxy_clock.now)

    def wait(seconds):
        """Advance PVC polling without sleeping or changing asyncio's real clock."""
        proxy_clock.now += seconds
    monkeypatch.setattr(private_workspace, "sleep", wait)
    for key in ("OPERATION_TIMEOUT_SECONDS", "RECONCILE_TIMEOUT_SECONDS", "DELETE_TIMEOUT_SECONDS"):
        monkeypatch.setattr(private_workspace, key, 0.4)
    return proxy_clock


def persistent(api, **changes):
    """Enable storage only through the trusted constructor configuration under test."""
    return runtime(api, workspace_storage_class=SC, **changes)


@pytest.mark.parametrize("value", ["", True, 1, " bad", "bad\n", "a/b", "a..b", "X", "a" * 64])
def test_invalid_persistent_configuration_is_inert(value):
    """Reject invalid storage values before obtaining a client or touching external configuration."""
    with pytest.raises(KubernetesConfigurationError):
        runtime(None, workspace_storage_class=value)


def test_persistence_requires_authenticated_proxy():
    """A storage setting cannot silently activate an unsupported three-resource layout."""
    with pytest.raises(KubernetesConfigurationError):
        runtime(None, proxy_image=None, workspace_storage_class=SC)


@pytest.mark.anyio
async def test_nine_resources_mount_and_original_bindings(persistence_clock):
    """PVC is accepted before the consumer and becomes a durable member of every resource receipt."""
    api = FakeProxyClient()
    backend = persistent(api)
    await backend.create(authenticated_spec())
    assert len(api.objects()) == 9
    order = mutations(api, "create")
    assert order.index("private-workspace") < order.index("container")
    pvc = api.by_role("private-workspace")
    assert pvc.metadata.owner_references in (None, [])
    for resource in api.objects():
        annotations = resource.metadata.annotations
        assert annotations[VERSION] == "3" and annotations[STORAGE_CLASS] == SC
        assert annotations[WORKSPACE_UID] == pvc.metadata.uid
        receipt = json.loads(annotations[RECORD])
        assert len(receipt) == 9 and receipt["workspace"] == pvc.metadata.uid
    workspace = next(volume for volume in api.pod.spec.volumes if volume.name == "workspace")
    assert workspace.empty_dir is None and workspace.persistent_volume_claim.claim_name == pvc.metadata.name
    assert workspace.persistent_volume_claim.read_only in (None, False)
    assert all(volume.persistent_volume_claim is None for volume in api.proxy_pod.spec.volumes)
    assert next(volume for volume in api.pod.spec.volumes if volume.name == "tmp").empty_dir is not None
    observed = await runtime(api, proxy_image=None).get(SID)
    assert observed.status == "ready" and observed.workspace_id is None and observed.workspace is None


@pytest.mark.anyio
@pytest.mark.parametrize("delete_workspace", [False, True])
async def test_default_retention_then_explicit_original_pvc_cleanup(delete_workspace, persistence_clock):
    """Restarted Runtime uses original metadata, keeps default data and permits a later explicit purge."""
    api = FakeProxyClient()
    await persistent(api).create(authenticated_spec())
    original = api.by_role("private-workspace").metadata.uid
    restarted = runtime(api, proxy_image=None)
    await restarted.delete(SID, expected_transaction_id=TXN, delete_workspace=delete_workspace)
    assert len(api.objects()) == (0 if delete_workspace else 1)
    if not delete_workspace:
        assert api.by_role("private-workspace").metadata.uid == original
        await restarted.delete(SID, expected_transaction_id=TXN)
        assert len(api.objects()) == 1
        await restarted.delete(SID, expected_transaction_id=TXN, delete_workspace=True)
    assert not api.objects()
    deleted = mutations(api, "delete")
    assert deleted.index("proxy") < deleted.index("container") < deleted.index("private-workspace")
    await restarted.delete(SID, expected_transaction_id=TXN, delete_workspace=True)


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["missing", "uid", "class", "size", "mount", "binding", "version"])
async def test_persistent_query_rejects_storage_tampering(change, persistence_clock):
    """Neither Agent readiness nor a valid proxy can hide missing or changed private storage."""
    api = FakeProxyClient()
    await persistent(api).create(authenticated_spec())
    pvc = api.by_role("private-workspace")
    if change == "missing":
        api.workspace = None
    elif change == "uid":
        pvc.metadata.uid = "00000000-0000-4000-8000-000000000001"
    elif change == "class":
        pvc.spec.storage_class_name = "changed"
    elif change == "size":
        pvc.spec.resources.requests["storage"] = "2Gi"
    elif change == "mount":
        api.pod.spec.volumes[0].persistent_volume_claim.claim_name = "other-volume"
    elif change == "binding":
        pvc.status.phase = "Pending"
    else:
        api.pod.metadata.annotations[VERSION] = "2"
    with pytest.raises((SandboxQueryFailed, SandboxDeleteConflict)):
        await runtime(api, proxy_image=None).get(SID)


@pytest.mark.anyio
@pytest.mark.parametrize("role", ["private-workspace", "network-policy", "agent-service", "container", "proxy-access",
                                  "proxy-isolation", "agent-proxy-ingress", "proxy-service", "proxy"])
@pytest.mark.parametrize("accepted", [False, True])
async def test_every_post_failure_compensates_all_original_resources(role, accepted, persistence_clock):
    """Known rejection or lost acknowledgement never produces a false complete rollback with storage remaining."""
    api = FakeProxyClient()
    api.faults[("create", role)] = TimeoutError() if accepted else client.ApiException(status=409)
    if not accepted:
        api.no_commit.add(role)
    with pytest.raises(SandboxCreateFailed) as failure:
        await persistent(api).create(authenticated_spec())
    expected = CreationResourceState.UNCHANGED if role == "private-workspace" and not accepted else CreationResourceState.ROLLED_BACK
    assert failure.value.creation_failure.resource_state == expected
    assert not api.objects()
    assert mutations(api, "create").count(role) == 1


@pytest.mark.anyio
async def test_unconfirmed_agent_cleanup_keeps_pvc_and_recovery_identity(persistence_clock):
    """A stuck consumer prevents storage cleanup and cannot be labeled ROLLED_BACK."""
    api = FakeProxyClient()
    api.startup["proxy"] = "image"
    api.stuck.add("container")
    with pytest.raises(SandboxCleanupFailed) as failure:
        await persistent(api).create(authenticated_spec())
    assert failure.value.creation_failure.resource_state == CreationResourceState.REMAINING
    assert api.by_role("private-workspace") is not None
    assert "private-workspace" not in mutations(api, "delete")


@pytest.mark.anyio
async def test_pvc_removal_failure_is_not_complete_rollback(persistence_clock):
    """An accepted PVC still retained by the API keeps creation failure and restoration evidence."""
    api = FakeProxyClient()
    api.startup["proxy"] = "image"
    api.stuck.add("private-workspace")
    with pytest.raises(SandboxCleanupFailed) as failure:
        await persistent(api).create(authenticated_spec())
    assert failure.value.creation_failure.resource_state == CreationResourceState.REMAINING
    assert api.by_role("private-workspace") is not None


@pytest.mark.anyio
async def test_real_service_activation_conflict_compensates_persistent_storage(tmp_path, persistence_clock):
    """Real SQLite activation CAS failure enters Service's delete_workspace=True resource compensation."""
    from test_kubernetes_service_create import ActivationStore
    store = ActivationStore(tmp_path / "isolated.sqlite3")
    await store.initialize()
    store.mode = "missing"
    api = FakeProxyClient()
    service = SandboxService(persistent(api), store, id_factory=lambda: SID,
                             transaction_id_factory=lambda: TXN, clock=lambda: CREATED)
    from control_plane.core.errors import SandboxMetadataConflict
    with pytest.raises(SandboxMetadataConflict):
        await service.create()
    assert not api.objects()
    assert await store.get(SID) is None and await store.get_credential(SID, TXN) is None


@pytest.mark.anyio
async def test_real_service_default_delete_keeps_only_private_pvc(tmp_path, persistence_clock):
    """Private PVC does not become a public workspace binding or retain lifecycle capacity after successful deletion."""
    store = SQLiteLifecycleStore(tmp_path / "isolated.sqlite3")
    await store.initialize()
    api = FakeProxyClient()
    backend = persistent(api)
    service = SandboxService(backend, store, id_factory=lambda: SID,
                             transaction_id_factory=lambda: TXN, clock=lambda: CREATED)
    created = await service.create()
    assert created.workspace_id is None
    record = await store.get(SID)
    assert record.management_state == "active" and record.workspace_id is None
    await service.delete(SID)
    assert len(api.objects()) == 1 and api.by_role("private-workspace") is not None
    assert await store.get(SID) is None and await store.get_credential(SID, TXN) is None
    assert (await store.list_sandboxes()).managed_count == 0
    await backend.delete(SID, expected_transaction_id=TXN, delete_workspace=True)


@pytest.mark.anyio
@pytest.mark.parametrize("role", ["private-workspace", "network-policy", "container", "proxy-access", "proxy"])
async def test_repeated_cancel_keeps_ownership_until_pvc_compensation(role, persistence_clock):
    """Cancellation at storage or later POSTs waits for original compute and PVC cleanup before propagating."""
    api = FakeProxyClient()
    entered, release = threading.Event(), threading.Event()
    api.barriers[("create", role)] = (entered, release)
    operation = asyncio.create_task(persistent(api).create(authenticated_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        operation.cancel()
        await asyncio.sleep(0)
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done() and not api.closed.is_set()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert not api.objects() and api.closed.is_set()
    assert mutations(api, "create").count(role) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("role", ["private-workspace", "network-policy", "container", "proxy-access", "proxy"])
async def test_persistent_metadata_publish_failure_cleans_original_pvc(role, persistence_clock):
    """A failed UID-guarded record publication cannot leave storage while claiming complete rollback."""
    api = FakeProxyClient()
    api.faults[("patch", role)] = client.ApiException(status=409)
    with pytest.raises(SandboxCreateFailed) as failure:
        await persistent(api).create(authenticated_spec())
    assert failure.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
    assert not api.objects()


@pytest.mark.anyio
async def test_private_storage_rejects_unknown_role_and_consuming_foreign_pod(persistence_clock):
    """Extra PVC roles cannot hide in discovery; non-platform consumers still prevent explicit data removal."""
    api = FakeProxyClient()
    await persistent(api).create(authenticated_spec())
    original = api.by_role("private-workspace")
    unexpected = copy.deepcopy(original)
    unexpected.metadata.labels[ROLE] = "unsupported-workspace"
    api.inventories["persistent_volume_claim"] = [unexpected]
    with pytest.raises(SandboxDeleteConflict):
        await runtime(api, proxy_image=None).delete(SID, expected_transaction_id=TXN, delete_workspace=True)
    assert not mutations(api, "delete")
    api.inventories.clear()
    await runtime(api, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    consumer = client.V1Pod(metadata=client.V1ObjectMeta(name="foreign", namespace=original.metadata.namespace),
        spec=client.V1PodSpec(containers=[], volumes=[client.V1Volume(name="workspace",
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name=original.metadata.name))]))
    api.inventories["pod"] = [consumer]
    with pytest.raises(SandboxDeleteConflict):
        await runtime(api, proxy_image=None).delete(SID, expected_transaction_id=TXN, delete_workspace=True)
    assert api.by_role("private-workspace") is original
    api.inventories.clear()
    await runtime(api, proxy_image=None).delete(SID, expected_transaction_id=TXN, delete_workspace=True)


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["unknown-before", "unknown-after"])
async def test_uncertain_activation_preserves_persistent_data_and_credentials(mode, tmp_path, persistence_clock):
    """Uncertain SQLite commits retain all nine resources and recovery credentials rather than compensating blindly."""
    from test_kubernetes_service_create import ActivationStore
    from control_plane.core.errors import SandboxMetadataUnavailable
    store = ActivationStore(tmp_path / "uncertain.sqlite3")
    await store.initialize()
    store.mode = mode
    api = FakeProxyClient()
    service = SandboxService(persistent(api), store, id_factory=lambda: SID,
                             transaction_id_factory=lambda: TXN, clock=lambda: CREATED)
    with pytest.raises(SandboxMetadataUnavailable):
        await service.create()
    assert len(api.objects()) == 9 and await store.get(SID) is not None
    assert await store.get_credential(SID, TXN) is not None and not mutations(api, "delete")
    store.mode = None
    await service.delete(SID, delete_workspace=True)


@pytest.mark.anyio
async def test_api_omitted_pvc_readonly_false_is_a_supported_default(persistence_clock):
    """Kubernetes omits the PVC-source readOnly=false scalar without changing writable-mount semantics."""
    class DefaultingClient(FakeProxyClient):
        """Apply the API Server's actual PVC source scalar serialization to accepted Pods."""

        def _create(self, kind, namespace, kwargs):
            """Omit only the default-false PVC field after the original transaction has committed."""
            response = super()._create(kind, namespace, kwargs)
            if kind == "pod" and response.metadata.labels[ROLE] == "container":
                self.pod.spec.volumes[0].persistent_volume_claim.read_only = None
                response.spec.volumes[0].persistent_volume_claim.read_only = None
            return response
    api = DefaultingClient()
    await persistent(api).create(authenticated_spec())
    assert (await runtime(api, proxy_image=None).get(SID)).status == "ready"
    await runtime(api, proxy_image=None).delete(SID, delete_workspace=True, expected_transaction_id=TXN)
    assert not api.objects()


@pytest.mark.anyio
async def test_never_visible_pvc_post_keeps_unknown_evidence_and_pending_metadata(tmp_path, persistence_clock):
    """Repeated 404 after a timed-out POST does not authorize releasing metadata, credentials or capacity."""
    api = FakeProxyClient()
    api.no_commit.add("private-workspace")
    api.faults[("create", "private-workspace")] = TimeoutError()
    store = SQLiteLifecycleStore(tmp_path / "unknown-post.sqlite3")
    await store.initialize()
    service = SandboxService(persistent(api), store, id_factory=lambda: SID,
                             transaction_id_factory=lambda: TXN, clock=lambda: CREATED)
    with pytest.raises(SandboxCleanupFailed) as failure:
        await service.create()
    assert failure.value.creation_failure.resource_state == CreationResourceState.UNKNOWN
    assert (await store.get(SID)).management_state == "pending"
    assert await store.get_credential(SID, TXN) is not None
    assert (await store.list_sandboxes()).managed_count == 1
    assert mutations(api, "create") == ["private-workspace"]


@pytest.mark.anyio
@pytest.mark.parametrize("retained", [False, True])
async def test_cancelled_pvc_delete_timeout_requires_positive_cleanup_evidence(retained, persistence_clock):
    """Lost DELETE acknowledgements may cancel cleanly only after confirmed absence, never while PVC remains."""
    api = FakeProxyClient()
    api.faults[("delete", "private-workspace")] = TimeoutError()
    if retained:
        api.stuck.add("private-workspace")
    entered, release = threading.Event(), threading.Event()
    api.barriers[("create", "private-workspace")] = (entered, release)
    operation = asyncio.create_task(persistent(api).create(authenticated_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        operation.cancel()
        await asyncio.sleep(0)
        operation.cancel()
    finally:
        release.set()
    if retained:
        with pytest.raises(SandboxCleanupFailed) as failure:
            await operation
        assert failure.value.creation_failure.resource_state == CreationResourceState.REMAINING
        assert api.by_role("private-workspace") is not None
    else:
        with pytest.raises(asyncio.CancelledError):
            await operation
        assert not api.objects()


@pytest.mark.anyio
async def test_retained_pvc_partial_receipt_still_protects_original_uid(persistence_clock):
    """One self receipt authorizes its original leftover PVC, but never a same-name replacement."""
    api = FakeProxyClient()
    await persistent(api).create(authenticated_spec())
    backend = runtime(api, proxy_image=None)
    await backend.delete(SID, expected_transaction_id=TXN)
    pvc = api.by_role("private-workspace")
    original = pvc.metadata.uid
    pvc.metadata.annotations[RECORD] = json.dumps({"workspace": original})
    pvc.metadata.uid = "00000000-0000-4000-8000-000000000001"
    with pytest.raises(SandboxDeleteConflict):
        await backend.delete(SID, expected_transaction_id=TXN, delete_workspace=True)
    pvc.metadata.uid = original
    await backend.delete(SID, expected_transaction_id=TXN, delete_workspace=True)
    assert not api.objects()


@pytest.mark.anyio
@pytest.mark.parametrize("mutation", ["sub_path", "read_only", "mount_propagation"])
async def test_query_rejects_workspace_mount_semantics_changes(mutation, persistence_clock):
    """An exact claim name alone is insufficient when an API update changes how its data is exposed."""
    api = FakeProxyClient()
    await persistent(api).create(authenticated_spec())
    mount = next(mount for mount in api.pod.spec.containers[0].volume_mounts if mount.name == "workspace")
    setattr(mount, mutation, {"sub_path": "part", "read_only": True, "mount_propagation": "Bidirectional"}[mutation])
    with pytest.raises((SandboxQueryFailed, SandboxDeleteConflict)):
        await runtime(api, proxy_image=None).get(SID)


@pytest.mark.anyio
async def test_credential_damage_does_not_prevent_owned_private_storage_cleanup(persistence_clock):
    """Execution health/auth metadata is required for delivery, while original PVC identity still permits cleanup."""
    api = FakeProxyClient()
    await persistent(api).create(authenticated_spec())
    api.by_role("private-workspace").metadata.annotations[PREFIX + "proxy-token-sha256"] = "damaged"
    backend = runtime(api, proxy_image=None)
    with pytest.raises((SandboxQueryFailed, SandboxDeleteConflict)):
        await backend.get(SID)
    await backend.delete(SID, expected_transaction_id=TXN, delete_workspace=True)
    assert not api.objects()
