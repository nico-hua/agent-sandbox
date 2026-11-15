"""Independent PVC mount identity and instance-only lifecycle using real protocols and offline SDK objects."""

from dataclasses import replace
import copy
import json
import httpx
import asyncio
import threading
from datetime import timedelta
from types import SimpleNamespace

import pytest
from kubernetes import client

from control_plane.core.models import WorkspaceBinding
from control_plane.core.models import SandboxCreateOptions
from control_plane.core.service import SandboxService
from control_plane.core.errors import (
    CreationResourceState, SandboxCreateFailed, SandboxCleanupFailed, SandboxQueryFailed,
    SandboxDeleteConflict, SandboxMetadataConflict, SandboxMetadataUnavailable, WorkspaceConflict,
)
from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
from test_kubernetes_workspace_create import original_receipt
from test_kubernetes_workspace_spec import record, NAMESPACE, SC
from test_kubernetes_proxy_lifecycle import authenticated_spec, proxy_clock, runtime, mutations
from kubernetes_proxy_fakes import FakeProxyClient, ROLE
from test_kubernetes_workspace_spec import UID as WORKSPACE_UID
from test_kubernetes_auto_entry import automatic
from test_kubernetes_local_entry_manager import manager_harness
from test_kubernetes_persistent_workspace import persistence_clock
from test_kubernetes_service_create import ActivationStore
from test_kubernetes_query import CREATED
from test_kubernetes_workspace_resource import consumer


@pytest.fixture
async def binding_service(tmp_path, proxy_clock):
    """Use real atomic occupancy/receipt storage and real Runtime orchestration with an offline SDK."""
    store = ActivationStore(tmp_path / 'binding.sqlite3')
    await store.initialize()
    api = BoundClient(existing=False)
    backend = bound_runtime(api)
    service = SandboxService(backend, store, clock=lambda: CREATED)
    workspace = await service.create_workspace()
    return SimpleNamespace(store=store, api=api, backend=backend, service=service, workspace=workspace)


class BoundClient(FakeProxyClient):
    """Keep external workspace ownership separate from all sandbox-owned resources in the existing fake API."""

    def __init__(self, *, existing=True):
        """Start with a valid independent Pending PVC or leave creation to the real Service path."""
        super().__init__()
        from test_kubernetes_workspace_spec import live_pvc
        self.external = live_pvc() if existing else None
        if self.external is not None:
            self.external.status = client.V1PersistentVolumeClaimStatus(phase='Pending')

    def objects(self, kind=None):
        """Include the external object for namespace scans while preserving its independent labels and slot."""
        result = super().objects(kind)
        if self.external is not None and kind in (None, 'persistent_volume_claim'):
            result.append(self.external)
        return result

    def _store(self, kind, resource, *, role=None):
        """Route independent storage mutations to its own slot; base cleanup must never target it."""
        role = role if role is not None else resource.metadata.labels[ROLE]
        if kind == 'persistent_volume_claim' and role == 'workspace':
            self.external = resource
        else:
            super()._store(kind, resource, role=role)

    def _list(self, kind, namespace, kwargs):
        """Supply actual PVC list TypeMeta required by independent-resource identity discovery."""
        result = super()._list(kind, namespace, kwargs)
        if kind == 'persistent_volume_claim':
            result.api_version, result.kind = 'v1', 'PersistentVolumeClaimList'
        return result

    def _create(self, kind, namespace, kwargs):
        """Allow independent POSTs and model WFFC binding only when an actual Agent consumer is submitted."""
        body = kwargs['body']
        if kind == 'persistent_volume_claim' and body.metadata.labels[ROLE] == 'workspace':
            assert self.external is None
            self.external = copy.deepcopy(body)
            self.external.metadata.uid, self.external.metadata.resource_version = WORKSPACE_UID, '7'
            self.external.status = client.V1PersistentVolumeClaimStatus(phase='Pending')
            self.trace.append(('create', 'workspace'))
            return copy.deepcopy(self.external)
        result = super()._create(kind, namespace, kwargs)
        if (kind == 'pod' and body.metadata.labels[ROLE] == 'container' and self.external is not None
            and any(v.persistent_volume_claim is not None and v.persistent_volume_claim.claim_name == self.external.metadata.name
                    for v in body.spec.volumes or [])):
            self.external.spec.volume_name = 'pvc-original-workspace'
            self.external.status = client.V1PersistentVolumeClaimStatus(phase='Bound', access_modes=['ReadWriteOnce'],
                                                                         capacity={'storage': '1Gi'})
        return result


def bound_runtime(api, **changes):
    """Use the original content digest and private-storage config to test explicit external-mode precedence."""
    options = dict(environment_images={'basic': 'example/agent@' + record().image_id}, workspace_storage_class=SC)
    options.update(changes)
    return runtime(api, **options)


def bound_spec(**changes):
    """Bind a fixed sandbox transaction to an independent original without replacing workspace business identity."""
    base = authenticated_spec()
    workspace = record()
    binding = WorkspaceBinding(workspace.workspace_id, workspace.creation_token, workspace.volume_name,
        workspace.image_id, 2, base.sandbox_id, base.transaction_id)
    values = dict(volume_name=workspace.volume_name, workspace=binding, workspace_receipt=original_receipt())
    values.update(changes)
    return replace(base, **values)


def test_bound_spec_uses_exact_external_pvc_without_private_volume():
    """Only the deterministic original claim is mounted; a configured private StorageClass cannot create a second claim."""
    spec = bound_spec()
    pod = build_pod_spec(spec, namespace=NAMESPACE, agent_image='example/agent@' + spec.workspace.image_id,
                         workspace_storage_class=SC)
    volume = next(v for v in pod.spec.volumes if v.name == 'workspace')
    assert volume.persistent_volume_claim.claim_name == 'agent-sandbox-wsp-' + record().workspace_id[4:]
    assert volume.empty_dir is None and volume.persistent_volume_claim.read_only in (None, False)
    assert next(v for v in pod.spec.volumes if v.name == 'tmp').empty_dir is not None
    assert 'io.agent-sandbox.private-workspace-storage-class' not in pod.metadata.annotations
    assert 'io.agent-sandbox.workspace-binding' in pod.metadata.annotations
    assert pod.spec.security_context.run_as_user == pod.spec.security_context.fs_group == 1000


def test_receipt_without_binding_is_rejected():
    """An internal receipt must never silently add storage to an unbound sandbox specification."""
    with pytest.raises(ValueError):
        authenticated_spec(workspace_receipt=original_receipt())


def test_binding_metadata_is_independent_of_mutable_occupancy_version():
    """Persisted original storage identity is stable across holder management version changes."""
    first = bound_spec()
    second = replace(first, workspace=replace(first.workspace, version=first.workspace.version + 1))
    one = build_pod_spec(first, namespace=NAMESPACE, agent_image='example/agent@' + first.workspace.image_id)
    two = build_pod_spec(second, namespace=NAMESPACE, agent_image='example/agent@' + second.workspace.image_id)
    assert one.metadata.annotations['io.agent-sandbox.workspace-binding'] == two.metadata.annotations['io.agent-sandbox.workspace-binding']


@pytest.mark.anyio
async def test_bound_runtime_owns_eight_resources_and_preserves_external_pvc(proxy_clock):
    """The new layout has eight owned receipts; external PVC never becomes a private transaction resource."""
    api = BoundClient()
    original = copy.deepcopy(api.external)
    spec = bound_spec()
    await bound_runtime(api).create(spec)
    assert len(api.objects()) == 9 and api.workspace is None
    assert 'private-workspace' not in mutations(api, 'create') and 'workspace' not in mutations(api, 'create')
    for resource in [r for r in api.objects() if r is not api.external]:
        assert resource.metadata.annotations['io.agent-sandbox.kubernetes-proxy-version'] == '4'
        assert set(json.loads(resource.metadata.annotations['io.agent-sandbox.proxy-resource-uids'])) == {
            'agent', 'policy', 'service', 'secret', 'proxy', 'ingress', 'isolation', 'proxy_service'}
    assert api.external.metadata == original.metadata
    observed = await runtime(api, proxy_image=None).get(spec.sandbox_id)
    assert observed.status == 'ready' and observed.workspace_id == spec.workspace.workspace_id
    assert replace(observed.workspace, version=spec.workspace.version) == spec.workspace
    await runtime(api, proxy_image=None).delete(spec.sandbox_id, expected_transaction_id=spec.transaction_id,
                                               expected_workspace=spec.workspace)
    assert api.objects() == [api.external] and api.external.metadata.uid == WORKSPACE_UID


@pytest.mark.anyio
async def test_bound_integrity_inspection_treats_reference_separately(proxy_clock):
    """Complete new layouts are all_present; deleting instance resources does not turn retained external data into remnants."""
    api = BoundClient()
    spec = bound_spec()
    await bound_runtime(api).create(spec)
    result = await runtime(api, proxy_image=None).inspect_resources(spec.sandbox_id,
        expected_transaction_id=spec.transaction_id, expected_workspace=spec.workspace)
    assert result.classification == 'all_present'


@pytest.mark.anyio
async def test_local_entry_target_pins_external_original_uid(proxy_clock):
    """Entry identity includes the external reference without treating it as a deletable instance resource."""
    from control_plane.runtime.kubernetes.local_entry_target import resolve_local_entry_target
    api = BoundClient()
    spec = bound_spec()
    backend = bound_runtime(api)
    await backend.create(spec)
    target = await resolve_local_entry_target(backend, spec.sandbox_id)
    assert target.layout_version == '4'
    assert dict(target.resource_uids)['workspace_ref'] == WORKSPACE_UID
    assert target.workspace_receipt == spec.workspace_receipt


@pytest.mark.anyio
@pytest.mark.parametrize('change', ['missing', 'uid'])
async def test_survivor_inspection_still_checks_external_original(change, proxy_clock):
    """A surviving policy/Secret/Service reference remains verifiable even after the Agent disappears."""
    api = BoundClient()
    spec = bound_spec()
    backend = bound_runtime(api)
    await backend.create(spec)
    api.pod = None
    if change == 'missing':
        api.external = None
    else:
        api.external.metadata.uid = '55667ab6-3f02-40ba-9b29-b1967503a3f0'
    observed = await backend.inspect_resources(spec.sandbox_id, expected_transaction_id=spec.transaction_id,
                                               expected_workspace=spec.workspace)
    if change == 'missing':
        assert observed.classification == 'partial'
        assert 'independent-workspace' in observed.missing_resources
    else:
        assert observed.classification == 'conflict'


@pytest.mark.anyio
async def test_public_service_occupancy_entry_delete_and_rebind(automatic, monkeypatch):
    """The real HTTP/Service/SQLite chain creates binding receipts, auto entry, exclusive occupancy and release."""
    h = automatic
    h.api = BoundClient(existing=False)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_AGENT_IMAGE', 'example/agent@' + record().image_id)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS', SC)
    app = h.make()
    headers = {'Authorization': 'Bearer synthetic-auto-key'}
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control') as caller:
            created = await caller.post('/v1/workspaces', headers=headers, json={})
            assert created.status_code == 201
            wid = created.json()['workspace_id']
            original_uid = h.api.external.metadata.uid
            instance = await caller.post('/v1/sandboxes', headers=headers, json={'workspace_id': wid})
            assert instance.status_code == 201
            sid = instance.json()['sandbox_id']
            assert instance.json()['workspace_id'] == wid
            assert (await caller.get('/v1/sandboxes/' + sid, headers=headers)).json()['workspace_id'] == wid
            endpoint = await caller.get('/v1/sandboxes/' + sid + '/endpoint', headers=headers)
            assert endpoint.status_code == 200 and endpoint.json()['auth_required'] is True
            assert h.sessions[-1].target.workspace_receipt.pvc_uid == original_uid
            assert (await caller.post('/v1/sandboxes', headers=headers, json={'workspace_id': wid})).status_code == 409
            assert (await caller.delete('/v1/workspaces/' + wid, headers=headers)).status_code == 409
            assert (await caller.delete('/v1/sandboxes/' + sid + '?delete_workspace=true', headers=headers)).status_code == 409
            assert (await caller.delete('/v1/sandboxes/' + sid, headers=headers)).status_code == 204
            assert h.sessions[-1].closed.is_set() and h.api.external.metadata.uid == original_uid
            assert (await caller.get('/v1/workspaces/' + wid, headers=headers)).json()['status'] == 'available'
            another = await caller.post('/v1/sandboxes', headers=headers, json={'workspace_id': wid})
            assert another.status_code == 201 and another.json()['sandbox_id'] != sid
            assert h.api.external.metadata.uid == original_uid and h.api.workspace is None
            assert (await caller.delete('/v1/sandboxes/' + another.json()['sandbox_id'], headers=headers)).status_code == 204
            assert (await caller.delete('/v1/workspaces/' + wid, headers=headers)).status_code == 204
    assert not h.api.objects()


@pytest.mark.parametrize('change', ['subpath', 'readonly', 'propagation', 'claim', 'extra', 'hostpath', 'image', 'version', 'downgrade'])
def test_external_mount_rejects_ambiguous_or_modified_layout(change):
    """No alternate claim, mount feature, image or downgraded metadata can pass the original reference contract."""
    from control_plane.runtime.kubernetes.proxy_resources import mark_proxy_layout
    from control_plane.runtime.kubernetes.pod_delete import verify_workspace_volumes
    spec = bound_spec()
    pod = build_pod_spec(spec, namespace=NAMESPACE, agent_image='example/agent@' + spec.workspace.image_id)
    mark_proxy_layout(pod, spec.access_token)
    mount = pod.spec.containers[0].volume_mounts[0]
    if change == 'subpath':
        mount.sub_path = 'another-directory'
    elif change == 'readonly':
        mount.read_only = True
    elif change == 'propagation':
        mount.mount_propagation = 'Bidirectional'
    elif change == 'claim':
        pod.spec.volumes[0].persistent_volume_claim.claim_name = 'another-claim'
    elif change == 'extra':
        pod.spec.containers[0].volume_mounts.append(client.V1VolumeMount(name='workspace', mount_path='/elsewhere'))
    elif change == 'hostpath':
        pod.spec.volumes[0].host_path = client.V1HostPathVolumeSource(path='/')
    elif change == 'image':
        pod.spec.containers[0].image = 'different/image:tag'
    elif change == 'version':
        pod.metadata.annotations['io.agent-sandbox.kubernetes-proxy-version'] = '5'
    else:
        del pod.metadata.annotations['io.agent-sandbox.workspace-binding']
        pod.metadata.annotations['io.agent-sandbox.kubernetes-proxy-version'] = '2'
    with pytest.raises((SandboxDeleteConflict, SandboxQueryFailed)):
        verify_workspace_volumes(pod)


@pytest.mark.anyio
@pytest.mark.parametrize('change', ['uid', 'namespace', 'image', 'foreign-consumer', 'terminal-consumer'])
async def test_create_preflight_rejects_before_owned_resource_posts(change, proxy_clock):
    """Original target mismatches and real consumers cannot be hidden by ReadWriteOnce or database occupancy."""
    api = BoundClient()
    spec = bound_spec()
    if change == 'uid':
        api.external.metadata.uid = '55667ab6-3f02-40ba-9b29-b1967503a3f0'
    elif change == 'namespace':
        spec = replace(spec, workspace_receipt=replace(spec.workspace_receipt, namespace='other-namespace'))
    elif change == 'image':
        backend = runtime(api, environment_images={'basic': 'different/agent@sha256:' + 'e' * 64})
    else:
        api.inventories['pod'] = [consumer('Succeeded' if change == 'terminal-consumer' else 'Running')]
    backend = backend if change == 'image' else bound_runtime(api)
    with pytest.raises(Exception) as failed:
        await backend.create(spec)
    assert failed.value.creation_failure.resource_state is CreationResourceState.UNCHANGED
    assert not mutations(api, 'create') and api.external is not None


@pytest.mark.anyio
@pytest.mark.parametrize('change', ['missing', 'uid', 'mount', 'image', 'binding'])
async def test_query_inspection_and_entry_refuse_replaced_or_altered_storage(change, proxy_clock):
    """Readiness never conceals missing originals or altered actual mounts and their immutable reference."""
    from control_plane.runtime.kubernetes.local_entry_target import resolve_local_entry_target
    api = BoundClient()
    spec = bound_spec()
    backend = bound_runtime(api)
    await backend.create(spec)
    if change == 'missing':
        api.external = None
    elif change == 'uid':
        api.external.metadata.uid = '55667ab6-3f02-40ba-9b29-b1967503a3f0'
    elif change == 'mount':
        api.pod.spec.containers[0].volume_mounts[0].sub_path = 'other'
    elif change == 'image':
        api.pod.spec.containers[0].image = 'another/agent:tag'
    else:
        raw = json.loads(api.pod.metadata.annotations['io.agent-sandbox.workspace-binding'])
        raw['pvc_uid'] = '55667ab6-3f02-40ba-9b29-b1967503a3f0'
        api.pod.metadata.annotations['io.agent-sandbox.workspace-binding'] = json.dumps(raw)
    with pytest.raises(SandboxQueryFailed):
        await backend.get(spec.sandbox_id)
    with pytest.raises(SandboxQueryFailed):
        await resolve_local_entry_target(backend, spec.sandbox_id)
    inspection = await backend.inspect_resources(spec.sandbox_id, expected_transaction_id=spec.transaction_id,
                                                  expected_workspace=spec.workspace)
    assert inspection.classification == ('partial' if change == 'missing' else 'conflict')
    if change == 'missing':
        assert 'independent-workspace' in inspection.missing_resources


@pytest.mark.anyio
async def test_pending_external_storage_is_complete_without_ready_state(proxy_clock):
    """Resource integrity does not demand Bound or Pod readiness; runtime query still refuses false ready evidence."""
    api = BoundClient()
    spec = bound_spec()
    backend = bound_runtime(api)
    await backend.create(spec)
    api.external.spec.volume_name = None
    api.external.status = client.V1PersistentVolumeClaimStatus(phase='Pending')
    api.startup['container'] = 'pending'
    observed = await backend.get(spec.sandbox_id)
    assert observed.status == 'starting'
    inspection = await backend.inspect_resources(spec.sandbox_id, expected_transaction_id=spec.transaction_id,
                                                  expected_workspace=spec.workspace)
    assert inspection.classification == 'all_present'


@pytest.mark.anyio
@pytest.mark.parametrize('fault_role', ['network-policy', 'agent-service', 'container', 'proxy-access', 'proxy-service', 'proxy'])
async def test_each_create_failure_rolls_back_only_instance_resources(binding_service, fault_role):
    """All resource stages can fail while retaining original PVC, receipt and a correctly released database holder."""
    h = binding_service
    h.api.faults[('create', fault_role)] = RuntimeError('controlled offline failure')
    with pytest.raises(SandboxCreateFailed) as failed:
        await h.service.create(SandboxCreateOptions(workspace_id=h.workspace.workspace_id))
    assert failed.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    current = await h.store.get_workspace(h.workspace.workspace_id)
    assert current.sandbox_id is None and current.management_state == 'available'
    assert h.api.objects() == [h.api.external]
    assert h.api.external.metadata.uid == WORKSPACE_UID and 'workspace' not in mutations(h.api, 'delete')
    assert await h.store.get_workspace_receipt(current.workspace_id, current.creation_token, expected_version=current.version)


@pytest.mark.anyio
@pytest.mark.parametrize('mode', ['conflict', 'unknown-before', 'unknown-after'])
async def test_activation_results_keep_conservative_resource_and_occupancy_facts(binding_service, mode):
    """Proven CAS conflicts reclaim only compute; uncertain commit results retain the original active/pending holder."""
    h = binding_service
    h.store.mode = mode
    with pytest.raises(SandboxMetadataConflict if mode == 'conflict' else SandboxMetadataUnavailable):
        await h.service.create(SandboxCreateOptions(workspace_id=h.workspace.workspace_id))
    current = await h.store.get_workspace(h.workspace.workspace_id)
    assert h.api.external.metadata.uid == WORKSPACE_UID and 'workspace' not in mutations(h.api, 'delete')
    if mode == 'conflict':
        # The competing metadata version remains; occupancy cannot be released by the stale creator.
        assert current.sandbox_id is not None and h.api.objects() == [h.api.external]
        h.store.mode = None
        await h.service.delete(current.sandbox_id)
        assert (await h.store.get_workspace(current.workspace_id)).sandbox_id is None
    else:
        assert current.sandbox_id is not None and h.api.pod is not None


@pytest.mark.anyio
async def test_concurrent_creations_compete_for_one_durable_workspace(binding_service):
    """A deterministic in-flight first POST cannot grant a second transaction the same original PVC."""
    h = binding_service
    entered, release = threading.Event(), threading.Event()
    h.api.barriers[('create', 'container')] = entered, release
    first = asyncio.create_task(h.service.create(SandboxCreateOptions(workspace_id=h.workspace.workspace_id)))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        with pytest.raises(WorkspaceConflict):
            await h.service.create(SandboxCreateOptions(workspace_id=h.workspace.workspace_id))
        current = await h.store.get_workspace(h.workspace.workspace_id)
        assert current.sandbox_id is not None and current.version == 2
    finally:
        release.set()
    created = await first
    assert current.sandbox_id == created.sandbox_id
    await h.service.delete(created.sandbox_id)
    assert (await h.store.get_workspace(h.workspace.workspace_id)).version == 3


@pytest.mark.anyio
async def test_repeated_cancel_waits_for_rollback_before_releasing_original(binding_service):
    """A submitted or late Pod remains owned until SDK rollback and the atomic holder release have settled."""
    h = binding_service
    entered, release = threading.Event(), threading.Event()
    h.api.barriers[('create', 'container')] = entered, release
    creating = asyncio.create_task(h.service.create(SandboxCreateOptions(workspace_id=h.workspace.workspace_id)))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        creating.cancel()
        await asyncio.sleep(0)
        creating.cancel()
        assert (await h.store.get_workspace(h.workspace.workspace_id)).sandbox_id is not None
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await creating
    current = await h.store.get_workspace(h.workspace.workspace_id)
    assert current.sandbox_id is None and h.api.objects() == [h.api.external]
    assert 'workspace' not in mutations(h.api, 'delete')


@pytest.mark.anyio
async def test_failed_instance_cleanup_retains_holder_and_retries(binding_service):
    """A blocked proxy deletion retains the durable holder until the retry confirms both consumer Pods gone."""
    h = binding_service
    created = await h.service.create(SandboxCreateOptions(workspace_id=h.workspace.workspace_id))
    h.api.stuck.add('proxy')
    with pytest.raises(Exception):
        await h.service.delete(created.sandbox_id)
    current = await h.store.get_workspace(h.workspace.workspace_id)
    assert current.sandbox_id == created.sandbox_id and h.api.external is not None
    h.api.stuck.clear()
    await h.service.delete(created.sandbox_id)
    assert (await h.store.get_workspace(h.workspace.workspace_id)).sandbox_id is None


@pytest.mark.anyio
async def test_expiry_reaping_releases_holder_but_never_original_pvc(binding_service):
    """Reaping uses the same exact instance cleanup and Store release; workspace expires metadata never deletes data."""
    h = binding_service
    created = await h.service.create(SandboxCreateOptions(workspace_id=h.workspace.workspace_id, ttl_seconds=60))
    h.service._clock = lambda: CREATED + timedelta(seconds=61)
    result = await h.service.reap_expired_once()
    assert result.error is None
    assert await h.store.get(created.sandbox_id) is None
    assert (await h.store.get_workspace(h.workspace.workspace_id)).sandbox_id is None
    assert h.api.objects() == [h.api.external]


@pytest.mark.anyio
@pytest.mark.parametrize('failure', ['timeout', 'process', 'close', 'resources'])
async def test_auto_entry_failure_preserves_external_storage_and_durable_holder(automatic, monkeypatch, failure):
    """Entry compensation releases only proven-clean holders and never owns or deletes independent data."""
    from control_plane.runtime.kubernetes import kubernetes_runtime
    h = automatic
    h.api = BoundClient(existing=False)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_AGENT_IMAGE', 'example/agent@' + record().image_id)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS', SC)
    app = h.make()
    async with app.router.lifespan_context(app):
        service, store = app.state.service, app.state.lifecycle_store
        workspace = await service.create_workspace()
        if failure == 'timeout':
            monkeypatch.setattr(kubernetes_runtime, 'ENTRY_READY_TIMEOUT_SECONDS', 0)
        else:
            h.open_error = True
        if failure == 'close':
            h.close_error = True
        if failure == 'resources':
            h.api.stuck.add('container')
        uncertain = failure in {'close', 'resources'}
        try:
            with pytest.raises(SandboxCleanupFailed if uncertain else SandboxCreateFailed) as caught:
                await service.create(SandboxCreateOptions(workspace_id=workspace.workspace_id))
            current = await store.get_workspace(workspace.workspace_id)
            assert h.api.external.metadata.uid == WORKSPACE_UID and 'workspace' not in mutations(h.api, 'delete')
            if uncertain:
                assert caught.value.creation_failure.resource_state in {CreationResourceState.REMAINING, CreationResourceState.UNKNOWN}
                assert current.sandbox_id is not None
                assert (await store.get(current.sandbox_id)).management_state == 'pending'
            else:
                assert caught.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
                assert current.sandbox_id is None and h.api.objects() == [h.api.external]
        finally:
            h.close_error = h.open_error = False
            h.api.stuck.clear()
            current = await store.get_workspace(workspace.workspace_id)
            if current.sandbox_id is not None:
                await service.delete(current.sandbox_id)
        assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is None
        assert h.api.objects() == [h.api.external]


@pytest.mark.anyio
async def test_cancel_after_activation_commit_releases_only_original_holder(binding_service):
    """Late active commit plus repeated cancellation settles compensation before releasing the occupied workspace."""
    h = binding_service
    h.store.mode = 'held'
    h.store.release.clear()
    creating = asyncio.create_task(h.service.create(SandboxCreateOptions(workspace_id=h.workspace.workspace_id)))
    try:
        await asyncio.wait_for(h.store.entered.wait(), 3)
        creating.cancel()
        await asyncio.sleep(0)
        creating.cancel()
        held = await h.store.get_workspace(h.workspace.workspace_id)
        assert held.sandbox_id is not None
        assert (await h.store.get(held.sandbox_id)).management_state == 'active'
    finally:
        h.store.release.set()
    with pytest.raises(asyncio.CancelledError):
        await creating
    assert (await h.store.get_workspace(h.workspace.workspace_id)).sandbox_id is None
    assert h.api.objects() == [h.api.external]


@pytest.mark.anyio
@pytest.mark.parametrize('committed', [False, True])
async def test_metadata_delete_failure_respects_atomic_holder_commit(binding_service, monkeypatch, committed):
    """A lost cleanup acknowledgement cannot release an uncommitted holder or undo a committed release."""
    from control_plane.storage.lifecycle_store import LifecycleStoreError
    from control_plane.core.errors import SandboxMetadataCleanupFailed
    h = binding_service
    created = await h.service.create(SandboxCreateOptions(workspace_id=h.workspace.workspace_id))
    remove = h.store.delete

    async def failing_delete(*args, **kwargs):
        """Inject failure immediately before or after the real SQLite record/credential/occupancy transaction."""
        if committed:
            await remove(*args, **kwargs)
        raise LifecycleStoreError('controlled cleanup acknowledgement')

    monkeypatch.setattr(h.store, 'delete', failing_delete)
    with pytest.raises(SandboxMetadataCleanupFailed):
        await h.service.delete(created.sandbox_id)
    current = await h.store.get_workspace(h.workspace.workspace_id)
    assert (current.sandbox_id is None) == committed
    assert h.api.objects() == [h.api.external]
    monkeypatch.setattr(h.store, 'delete', remove)
    await h.service.delete(created.sandbox_id)
    assert (await h.store.get_workspace(current.workspace_id)).sandbox_id is None
    assert h.api.external.metadata.uid == WORKSPACE_UID
