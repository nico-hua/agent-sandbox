"""Runtime controlled egress: persistent original facts and shared transactions with offline SDKs."""

import json
from dataclasses import replace

import pytest

from control_plane.core.errors import KubernetesConfigurationError, KubernetesOperationUnsupported
from control_plane.core.models import SandboxNetwork
from control_plane.runtime.configuration import build_runtime, load_backend_configuration
from test_backend_configuration import configure_kubernetes
from test_kubernetes_proxy_lifecycle import runtime, authenticated_spec, proxy_clock
from test_kubernetes_auto_entry import automatic
from test_kubernetes_local_entry_manager import manager_harness
from test_kubernetes_persistent_workspace import persistence_clock
from kubernetes_proxy_fakes import FakeProxyClient

CONFIG = dict(egress_image='agent-sandbox-egress:dev', egress_dns_namespace='kube-system',
              egress_dns_pod_labels={'k8s-app': 'kube-dns'},
              egress_cluster_cidrs=['10.244.0.0/16', '10.96.0.0/12', '192.168.49.0/24'])


def test_trusted_egress_configuration_is_frozen_and_formally_assembled(monkeypatch):
    """Formal configuration cannot silently disable an explicit group or reread changed environment."""
    configure_kubernetes(monkeypatch)
    for key, value in CONFIG.items():
        monkeypatch.setenv('AGENT_SANDBOX_K8S_' + key.upper(), value if isinstance(value, str) else json.dumps(value))
    configuration = load_backend_configuration()
    monkeypatch.setenv('AGENT_SANDBOX_K8S_EGRESS_IMAGE', 'different:dev')
    backend = build_runtime(configuration)
    assert backend.egress_configuration.image == 'agent-sandbox-egress:dev'
    assert backend.egress_configuration.dns_pod_labels == (('k8s-app', 'kube-dns'),)


@pytest.mark.parametrize('key', list(CONFIG))
def test_partial_formal_configuration_is_rejected(monkeypatch, key):
    """One egress input alone fails before a client can be acquired."""
    configure_kubernetes(monkeypatch)
    value = CONFIG[key]
    monkeypatch.setenv('AGENT_SANDBOX_K8S_' + key.upper(), value if isinstance(value, str) else json.dumps(value))
    with pytest.raises(ValueError):
        build_runtime(load_backend_configuration())


@pytest.mark.parametrize('key,value', [('egress_image',''), ('egress_image','repo:'),
    ('egress_dns_pod_labels',{}), ('egress_cluster_cidrs',[]), ('egress_cluster_cidrs',['0.0.0.0/0']),
    ('egress_dns_namespace','bad namespace')])
def test_invalid_trusted_group_has_no_client_side_effects(key, value):
    """Bad trusted network values are rejected at construction without backend I/O."""
    with pytest.raises(KubernetesConfigurationError):
        runtime(None, **{**CONFIG, key: value})


@pytest.mark.anyio
async def test_unconfigured_egress_request_never_creates_resources(proxy_clock):
    """The default boundary stays closed for a syntactically valid allowlist."""
    api = FakeProxyClient()
    with pytest.raises(KubernetesOperationUnsupported):
        await runtime(api).create(authenticated_spec(network=SandboxNetwork(['example.com'])))
    assert not api.objects() and not api.create_calls


class RuntimeEgressClient(FakeProxyClient):
    """Add separate component slots to the existing realistic multi-resource offline API."""

    def __init__(self):
        """Retain all legacy fault/barrier controls and distinct Egress objects."""
        super().__init__()
        self.egress_objects = {}

    def objects(self, kind=None):
        """Include every role in unfiltered bounded discovery, without aliasing Agent/proxy slots."""
        result = super().objects(kind)
        result += [obj for (typ, _), obj in self.egress_objects.items() if kind is None or typ == kind]
        return result

    def _store(self, kind, resource, *, role=None):
        """Keep Egress roles independent; normal base fake methods enforce UID/RV and fault semantics."""
        role = role if role is not None else resource.metadata.labels['io.agent-sandbox.resource']
        if role in {'egress','egress-service','agent-egress','egress-isolation'}:
            if resource is None:
                self.egress_objects.pop((kind,role), None)
            else:
                self.egress_objects[kind,role] = resource
        else:
            super()._store(kind, resource, role=role)

    def _list(self, kind, namespace, kwargs):
        """Return real collection TypeMeta required by strict Egress observations."""
        result = super()._list(kind, namespace, kwargs)
        names = {'pod':'Pod','service':'Service','network_policy':'NetworkPolicy'}
        if kind in names:
            result.api_version = 'networking.k8s.io/v1' if kind == 'network_policy' else 'v1'
            result.kind = names[kind] + 'List'
        return result


@pytest.fixture
def egress_api(monkeypatch, proxy_clock):
    """Assign canonical original UIDs and deterministic bounded component cleanup without sleeping."""
    from kubernetes_proxy_fakes import ROLE_UIDS
    from uuid import UUID
    import control_plane.runtime.kubernetes.egress_resources as module
    import control_plane.runtime.kubernetes.proxy_resources as proxy
    for index,role in enumerate(('egress','egress-service','agent-egress','egress-isolation'), 100):
        monkeypatch.setitem(ROLE_UIDS, role, str(UUID(int=index)))
    monkeypatch.setattr(module,'monotonic',proxy.monotonic)
    monkeypatch.setattr(module,'sleep',proxy.sleep)
    return RuntimeEgressClient()


@pytest.mark.anyio
async def test_runtime_creates_persisted_component_and_rebuilds_without_configuration(egress_api):
    """Original component UIDs/config survive Runtime reconstruction; cleanup must remove all twelve resources."""
    api = egress_api
    spec = authenticated_spec(network=SandboxNetwork(['example.com']))
    await runtime(api, **CONFIG).create(spec)
    assert len(api.objects()) == 12
    agent = api.by_role('container')
    annotations = agent.metadata.annotations
    assert annotations['io.agent-sandbox.kubernetes-egress-version'] == '1'
    record = json.loads(annotations['io.agent-sandbox.proxy-resource-uids'])
    assert {'egress_pod','egress_service','egress_agent_policy','egress_isolation'} <= set(record)
    env = {item.name:item.value for item in agent.spec.containers[0].env}
    assert env['HTTP_PROXY'] == 'http://' + api.by_role('egress-service').spec.cluster_ip + ':18084'
    creates = [role for verb,role in api.trace if verb=='create']
    assert creates.index('egress-isolation') < creates.index('egress') < creates.index('container')
    old = runtime(api, proxy_image=None)
    assert (await old.get(spec.sandbox_id)).status == 'ready'
    assert (await old.inspect_resources(spec.sandbox_id, expected_transaction_id=spec.transaction_id)).classification == 'all_present'
    await old.delete(spec.sandbox_id, expected_transaction_id=spec.transaction_id)
    assert not api.objects()
    deletes = [role for verb,role in api.trace if verb=='delete']
    assert deletes.index('proxy') < deletes.index('container') < deletes.index('egress') < deletes.index('egress-isolation')


@pytest.mark.anyio
async def test_configuration_without_allowlist_preserves_original_layout(egress_api):
    """Trusted egress settings alone never activate extra policies or Agent proxy environment."""
    await runtime(egress_api, **CONFIG).create(authenticated_spec())
    assert len(egress_api.objects()) == 8 and not egress_api.egress_objects
    assert not egress_api.pod.spec.containers[0].env


@pytest.mark.parametrize('role', ['egress-isolation','agent-egress','egress-service','egress','network-policy',
    'agent-service','container','proxy-access','proxy-isolation','agent-proxy-ingress','proxy-service','proxy'])
@pytest.mark.anyio
async def test_each_post_rejection_rolls_back_complete_transaction(egress_api, role):
    """Every definite API rejection cleans original component receipts and all accepted instance resources."""
    from kubernetes import client
    from control_plane.core.errors import SandboxError, CreationResourceState
    api = egress_api
    api.faults['create',role] = client.ApiException(status=403)
    api.no_commit.add(role)
    with pytest.raises(SandboxError) as failed:
        await runtime(api, **CONFIG).create(authenticated_spec(network=SandboxNetwork(['example.com'])))
    assert failed.value.creation_failure.resource_state in {CreationResourceState.UNCHANGED,CreationResourceState.ROLLED_BACK}
    assert not api.objects()
    assert [r for v,r in api.trace if v=='create'].count(role) == 1


@pytest.mark.parametrize('role',['egress-isolation','egress-service','egress','container','proxy'])
@pytest.mark.anyio
async def test_lost_original_post_never_becomes_candidate_delete_authority(egress_api, role):
    """Response loss must retain UNKNOWN and pending recovery facts, even when a matching candidate appears."""
    from control_plane.core.errors import SandboxError, CreationResourceState
    api = egress_api
    api.faults['create',role] = TimeoutError('sensitive upstream response')
    with pytest.raises(SandboxError) as failed:
        await runtime(api, **CONFIG).create(authenticated_spec(network=SandboxNetwork(['example.com'])))
    assert failed.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert api.by_role(role) is not None
    assert ('delete',role) not in api.trace
    if role in {'egress','container','proxy'}:
        assert api.by_role('egress-isolation') is not None
    assert 'sensitive' not in str(failed.value)


@pytest.mark.parametrize('role',['egress-isolation','agent-egress','egress-service','egress','network-policy','agent-service','container','proxy-access','proxy-isolation','agent-proxy-ingress','proxy-service','proxy'])
@pytest.mark.anyio
async def test_publication_failure_cleans_all_original_receipts(egress_api, role):
    """A UID-guarded metadata publication failure cannot hide component leftovers behind main Pod cleanup."""
    from kubernetes import client
    from control_plane.core.errors import SandboxError, CreationResourceState
    api = egress_api
    api.faults['patch',role] = client.ApiException(status=409)
    with pytest.raises(SandboxError) as failed:
        await runtime(api, **CONFIG).create(authenticated_spec(network=SandboxNetwork(['example.com'])))
    assert failed.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    assert not api.objects()


@pytest.mark.parametrize('change',['missing','uid','selector','facts','record','env','version'])
@pytest.mark.anyio
async def test_tampering_is_rejected_by_query_inspection_and_entry(egress_api, change):
    """Altered original egress facts/configuration must never result in a ready query or delivered local target."""
    from control_plane.core.errors import SandboxQueryFailed
    from control_plane.runtime.kubernetes.local_entry_target import resolve_local_entry_target
    from uuid import UUID
    api = egress_api
    spec = authenticated_spec(network=SandboxNetwork(['example.com']))
    await runtime(api, **CONFIG).create(spec)
    if change=='missing':
        api._store('service',None,role='egress-service')
    elif change=='uid':
        api.by_role('egress').metadata.uid=str(UUID(int=800))
    elif change=='selector':
        api.by_role('egress-service').spec.selector['io.agent-sandbox.resource']='container'
    elif change=='facts':
        facts=json.loads(api.pod.metadata.annotations['io.agent-sandbox.egress-runtime-facts'])
        facts['domains']=['other.example']
        api.pod.metadata.annotations['io.agent-sandbox.egress-runtime-facts']=json.dumps(facts)
    elif change=='record':
        api.pod.metadata.annotations['io.agent-sandbox.proxy-resource-uids']='{}'
    elif change=='env':
        api.pod.spec.containers[0].env[0].value='http://10.96.99.99:18084'
    else:
        api.pod.metadata.annotations['io.agent-sandbox.kubernetes-egress-version']='unknown'
    backend=runtime(api,proxy_image=None)
    with pytest.raises(SandboxQueryFailed):
        await backend.get(spec.sandbox_id)
    assert (await backend.inspect_resources(spec.sandbox_id, expected_transaction_id=spec.transaction_id)).classification in {'partial','conflict'}
    with pytest.raises(SandboxQueryFailed):
        await resolve_local_entry_target(backend,spec.sandbox_id)


@pytest.mark.anyio
async def test_pending_health_does_not_invalidate_integrity(egress_api):
    """A complete but unready Egress is an intact instance, while ready/endpoint delivery must wait."""
    api=egress_api
    spec=authenticated_spec(network=SandboxNetwork(['example.com']))
    backend=runtime(api,**CONFIG)
    await backend.create(spec)
    api.startup['egress']='not-ready'
    assert (await backend.get(spec.sandbox_id)).status=='running'
    assert (await backend.inspect_resources(spec.sandbox_id,expected_transaction_id=spec.transaction_id)).classification=='all_present'


@pytest.mark.anyio
async def test_pod_cleanup_not_confirmed_keeps_component_and_restrictions(egress_api):
    """Failed consumer cleanup cannot revoke outbound isolation or remove its component Service/Pod."""
    from control_plane.core.errors import SandboxError
    api=egress_api
    spec=authenticated_spec(network=SandboxNetwork(['example.com']))
    backend=runtime(api,**CONFIG)
    await backend.create(spec)
    api.stuck.add('container')
    with pytest.raises(SandboxError):
        await backend.delete(spec.sandbox_id,expected_transaction_id=spec.transaction_id)
    assert len(api.egress_objects)==4
    assert not any(v=='delete' and r.startswith('egress') for v,r in api.trace)
    api.stuck.clear()
    await runtime(api,proxy_image=None).delete(spec.sandbox_id,expected_transaction_id=spec.transaction_id)
    assert not api.objects()


@pytest.mark.anyio
async def test_private_pvc_and_external_workspace_remain_distinct_with_egress(egress_api,persistence_clock,monkeypatch):
    """Private rollback storage is owned; independent data remains external across egress creation/deletion."""
    from test_kubernetes_persistent_workspace import SC
    spec=authenticated_spec(network=SandboxNetwork(['example.com']))
    backend=runtime(egress_api,workspace_storage_class=SC,**CONFIG)
    await backend.create(spec)
    assert len(egress_api.objects())==13
    assert (await backend.get(spec.sandbox_id)).status=='ready'
    await backend.delete(spec.sandbox_id,expected_transaction_id=spec.transaction_id)
    assert [x.metadata.labels['io.agent-sandbox.resource'] for x in egress_api.objects()]==['private-workspace']
    await runtime(egress_api,proxy_image=None).delete(spec.sandbox_id,expected_transaction_id=spec.transaction_id,delete_workspace=True)
    assert not egress_api.objects()


@pytest.mark.anyio
async def test_external_workspace_with_egress_uses_reference_not_rollback_set(egress_api,persistence_clock):
    """External PVC identity survives creation, entry-target extraction, deletion and a rebuilt Runtime."""
    from test_kubernetes_workspace_binding import BoundClient, bound_spec
    from control_plane.runtime.kubernetes.local_entry_target import resolve_local_entry_target

    class BoundEgressClient(RuntimeEgressClient,BoundClient):
        """Combine separate external-PVC and component slots with the same real occupancy contract."""

    api=BoundEgressClient()
    spec=bound_spec(network=SandboxNetwork(['example.com']))
    backend=runtime(api,environment_images={'basic':'example/agent@'+spec.workspace.image_id},**CONFIG)
    await backend.create(spec)
    assert len(api.objects())==13 and api.workspace is None
    target=await resolve_local_entry_target(backend,spec.sandbox_id)
    assert target.egress_version=='1' and len(target.resource_uids)==13
    assert (await backend.get(spec.sandbox_id)).workspace_id==spec.workspace.workspace_id
    assert (await backend.inspect_resources(spec.sandbox_id,expected_transaction_id=spec.transaction_id,
                                           expected_workspace=spec.workspace)).classification=='all_present'
    original=api.external.metadata.uid
    await runtime(api,proxy_image=None).delete(spec.sandbox_id,expected_transaction_id=spec.transaction_id,
                                              expected_workspace=spec.workspace)
    assert api.objects()==[api.external] and api.external.metadata.uid==original


@pytest.mark.parametrize('persistent',[False,True])
@pytest.mark.anyio
async def test_formal_http_egress_entry_and_atomic_activation(egress_api,automatic,monkeypatch,persistent):
    """Real HTTP/Service/SQLite activates only after automatic entry and returns an original verified target."""
    import httpx
    from control_plane.runtime.kubernetes.local_entry_target import resolve_local_entry_target
    from test_kubernetes_auto_entry import identified_app
    h=automatic
    h.api=egress_api
    for key,value in CONFIG.items():
        monkeypatch.setenv('AGENT_SANDBOX_K8S_'+key.upper(),value if isinstance(value,str) else json.dumps(value))
    if persistent:
        monkeypatch.setenv('AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS','agent-sandbox-workspace')
    app=identified_app(h)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://control') as http:
            headers={'Authorization':'Bearer synthetic-auto-key'}
            made=await http.post('/v1/sandboxes',headers=headers,json={'network':{'allow_domains':['example.com']}})
            assert made.status_code==201
            sid=made.json()['sandbox_id']
            stored=await app.state.lifecycle_store.get(sid)
            assert stored.management_state=='active' and len(h.sessions)==1
            endpoint=await http.get(f'/v1/sandboxes/{sid}/endpoint',headers=headers)
            assert endpoint.status_code==200 and endpoint.headers['cache-control']=='no-store'
            assert (await resolve_local_entry_target(app.state.runtime,sid)).egress_version=='1'
            deleted=await http.delete(f'/v1/sandboxes/{sid}',headers=headers,params={'delete_workspace':'true'})
            assert deleted.status_code==204
            assert not egress_api.objects() and await app.state.lifecycle_store.get(sid) is None
    assert all(not session.is_active for session in h.sessions)


@pytest.mark.anyio
async def test_entry_launch_failure_does_not_activate_or_leave_egress(egress_api,automatic,monkeypatch):
    """Entry failure must close its owned session, roll back Egress and remove pending only after confirmation."""
    import httpx
    from test_kubernetes_auto_entry import identified_app
    h=automatic
    h.api,h.open_error=egress_api,True
    for key,value in CONFIG.items():
        monkeypatch.setenv('AGENT_SANDBOX_K8S_'+key.upper(),value if isinstance(value,str) else json.dumps(value))
    app=identified_app(h)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://control') as http:
            made=await http.post('/v1/sandboxes',headers={'Authorization':'Bearer synthetic-auto-key'},
                                 json={'network':{'allow_domains':['example.com']}})
            assert made.status_code>=400
            assert not egress_api.objects()
            from test_kubernetes_query import SID
            assert await app.state.lifecycle_store.get(SID) is None
    assert all(not s.is_active for s in h.sessions)


@pytest.mark.parametrize('role',['egress','container','proxy'])
@pytest.mark.anyio
async def test_repeated_cancel_settles_late_sdk_original_and_all_resources(egress_api,role):
    """Cancelled async waiters cannot abandon a late POST UID or skip dependent Egress cleanup."""
    import asyncio,threading
    api=egress_api
    entered,release=threading.Event(),threading.Event()
    api.barriers['create',role]=(entered,release)
    spec=authenticated_spec(network=SandboxNetwork(['example.com']))
    pending=asyncio.create_task(runtime(api,**CONFIG).create(spec))
    await asyncio.to_thread(entered.wait,2)
    assert entered.is_set()
    pending.cancel()
    await asyncio.sleep(0)
    pending.cancel()
    assert not pending.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert not api.objects()


@pytest.mark.parametrize('raw',['{}','{"version":"1","version":"1"}', 'x'*20001,
    '{"version":"99"}', '{"version":true}', '[]'])
def test_facts_decoder_rejects_unknown_duplicate_and_unbounded_metadata(raw):
    """Malformed persisted data cannot be reconstructed into a trusted component or delete authorization."""
    from control_plane.runtime.kubernetes.egress_runtime import decode_facts
    from control_plane.core.errors import SandboxQueryFailed
    with pytest.raises(SandboxQueryFailed):
        decode_facts(raw)


@pytest.mark.parametrize('field',['env','service_address'])
@pytest.mark.anyio
async def test_partial_layout_keeps_independently_visible_routing_conflicts(egress_api,field):
    """Missing one companion cannot hide contradictory Agent proxy or original Service address facts."""
    api=egress_api
    spec=authenticated_spec(network=SandboxNetwork(['example.com']))
    backend=runtime(api,**CONFIG)
    await backend.create(spec)
    api._store('secret',None,role='proxy-access')
    if field=='env':
        api.pod.spec.containers[0].env[0].value='http://10.96.99.99:18084'
    else:
        for resource in api.objects():
            raw=json.loads(resource.metadata.annotations['io.agent-sandbox.egress-runtime-facts'])
            raw['service_ipv4']='10.96.99.99'
            resource.metadata.annotations['io.agent-sandbox.egress-runtime-facts']=json.dumps(raw)
    assert (await backend.inspect_resources(spec.sandbox_id,expected_transaction_id=spec.transaction_id)).classification=='conflict'


@pytest.mark.parametrize('role',['private-workspace','agent-service','network-policy'])
@pytest.mark.parametrize('replacement',[False,True])
@pytest.mark.anyio
async def test_new_layout_never_adopts_lost_storage_or_base_response(egress_api,persistence_clock,monkeypatch,role,replacement):
    """No new composite transaction may promote a later same-name resource into an original POST receipt."""
    from control_plane.core.errors import SandboxError,CreationResourceState
    from uuid import UUID
    api=egress_api
    fault=api._raise_fault

    def lose(method,target):
        """Drop the response after optional same-metadata replacement; this is never an original acknowledgement."""
        if method=='create' and target==role:
            if replacement:
                api.by_role(role).metadata.uid=str(UUID(int=9999))
            raise TimeoutError('sensitive original response lost')
        fault(method,target)

    monkeypatch.setattr(api,'_raise_fault',lose)
    spec=authenticated_spec(network=SandboxNetwork(['example.com']))
    with pytest.raises(SandboxError) as failed:
        await runtime(api,workspace_storage_class='agent-sandbox-workspace',**CONFIG).create(spec)
    assert failed.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert api.by_role(role) is not None and ('delete',role) not in api.trace


@pytest.mark.anyio
async def test_replacement_after_consumer_absence_keeps_egress_and_base_protection(egress_api,monkeypatch):
    """A replacement positively observed after original absence cannot lose isolation during dependent cleanup."""
    import copy
    from uuid import UUID
    from kubernetes import client
    from control_plane.core.errors import SandboxError
    api=egress_api
    spec=authenticated_spec(network=SandboxNetwork(['example.com']))
    backend=runtime(api,**CONFIG)
    await backend.create(spec)
    replacement=copy.deepcopy(api.pod)
    replacement.metadata.uid=str(UUID(int=88888))
    find=api._find
    inserted=False

    def replace_after_absence(kind,name,namespace):
        """Return one genuine original-absence result while a new same-name UID becomes visible afterward."""
        nonlocal inserted
        try:
            return find(kind,name,namespace)
        except client.ApiException as error:
            if error.status==404 and kind=='pod' and name==replacement.metadata.name and ('delete','container') in api.trace and not inserted:
                inserted=True
                api._store('pod',replacement)
            raise

    monkeypatch.setattr(api,'_find',replace_after_absence)
    with pytest.raises(SandboxError):
        await backend.delete(spec.sandbox_id,expected_transaction_id=spec.transaction_id)
    assert inserted and api.pod.metadata.uid==replacement.metadata.uid
    assert api.by_role('network-policy') is not None and api.by_role('egress-isolation') is not None
    assert len(api.egress_objects)==4


def test_direct_trusted_inputs_are_copied_before_external_mutation():
    """Changing a caller's labels/CIDR containers cannot reconfigure an existing runtime."""
    options={**CONFIG,'egress_dns_pod_labels':dict(CONFIG['egress_dns_pod_labels']),
             'egress_cluster_cidrs':list(CONFIG['egress_cluster_cidrs'])}
    backend=runtime(None,**options)
    options['egress_dns_pod_labels'].clear()
    options['egress_cluster_cidrs'].append('0.0.0.0/0')
    assert backend.egress_configuration.dns_pod_labels==( ('k8s-app','kube-dns'), )
    assert backend.egress_configuration.cluster_cidrs==('10.244.0.0/16','10.96.0.0/12','192.168.49.0/24')


@pytest.mark.parametrize('mode',['missing','unknown-before','unknown-after'])
@pytest.mark.anyio
async def test_activation_compensation_preserves_full_egress_and_metadata_facts(egress_api,tmp_path,mode):
    """Definite activation failure rolls back all resources; ambiguous commits keep resources and original credentials."""
    from test_kubernetes_service_create import ActivationStore
    from test_kubernetes_query import CREATED,SID,TXN
    from control_plane.core.service import SandboxService
    from control_plane.core.models import SandboxCreateOptions
    from control_plane.core.errors import SandboxError
    store=ActivationStore(tmp_path/'activation.sqlite3')
    await store.initialize()
    store.mode=mode
    service=SandboxService(runtime(egress_api,**CONFIG),store,id_factory=lambda:SID,
                           transaction_id_factory=lambda:TXN,clock=lambda:CREATED)
    with pytest.raises(SandboxError):
        await service.create(SandboxCreateOptions(network=SandboxNetwork(['example.com'])))
    record=await store.get(SID)
    if mode=='missing':
        assert not egress_api.objects() and record is None
        assert await store.get_credential(SID,TXN) is None
    else:
        assert len(egress_api.objects())==12 and record is not None
        assert await store.get_credential(SID,TXN) is not None


@pytest.mark.anyio
async def test_ttl_cleanup_and_late_database_failure_can_retry_original_egress(egress_api,tmp_path,monkeypatch):
    """A failed metadata CAS preserves deleting recovery facts after resource cleanup; retry never invents resources."""
    from datetime import timedelta
    from test_kubernetes_query import CREATED,SID,TXN
    from control_plane.core.service import SandboxService
    from control_plane.core.models import SandboxCreateOptions
    from control_plane.storage.lifecycle_store import SQLiteLifecycleStore,LifecycleStoreError
    store=SQLiteLifecycleStore(tmp_path/'ttl.sqlite3')
    await store.initialize()
    now=[CREATED]
    backend=runtime(egress_api,**CONFIG)
    service=SandboxService(backend,store,id_factory=lambda:SID,transaction_id_factory=lambda:TXN,clock=lambda:now[0])
    made=await service.create(SandboxCreateOptions(network=SandboxNetwork(['example.com'])))
    delete=store.delete
    broken=True

    async def fail_before(*args,**kwargs):
        """Fail only the original metadata cleanup before commit, keeping the real deleting record and credential."""
        if broken:
            raise LifecycleStoreError('private SQLite failure')
        return await delete(*args,**kwargs)

    monkeypatch.setattr(store,'delete',fail_before)
    now[0]=made.expires_at+timedelta(seconds=1)
    outcome=await service.reap_expired_once()
    assert outcome.failed==1 and not egress_api.objects()
    assert (await store.get(SID)).management_state=='deleting'
    assert await store.get_credential(SID,TXN) is not None
    broken=False
    outcome=await service.reap_expired_once()
    assert outcome.succeeded==1 and await store.get(SID) is None
    assert await store.get_credential(SID,TXN) is None


@pytest.mark.parametrize('marker,value',[('kubernetes-egress-version','1'),('egress-runtime-facts','{}')])
@pytest.mark.anyio
async def test_unversioned_legacy_layout_cannot_hide_new_egress_declarations(egress_api,marker,value):
    """Unknown/new declarations on a historical three-resource Pod must fail rather than look like disabled egress."""
    from control_plane.core.errors import SandboxQueryFailed
    api=egress_api
    backend=runtime(api,proxy_image=None)
    spec=authenticated_spec()
    await backend.create(spec)
    api.pod.metadata.annotations['io.agent-sandbox.'+marker]=value
    with pytest.raises(SandboxQueryFailed):
        await backend.get(spec.sandbox_id)


@pytest.mark.parametrize('raw',['NaN','Infinity','-Infinity','{"a":1,"a":2}'])
def test_trusted_json_rejects_nonstandard_numbers_and_duplicate_configuration(raw):
    """Trusted configuration JSON remains strict even before field-specific policy validation runs."""
    from control_plane.runtime.kubernetes.egress_runtime import strict_json
    with pytest.raises(KubernetesConfigurationError):
        strict_json(raw)


@pytest.mark.parametrize('role',['container','proxy','egress'])
@pytest.mark.anyio
async def test_component_spec_version_is_scoped_to_its_declared_role(egress_api,role):
    """A recognized field on the wrong component or an unknown Egress spec version is still a conflict."""
    from control_plane.core.errors import SandboxQueryFailed
    api=egress_api
    spec=authenticated_spec(network=SandboxNetwork(['example.com']))
    backend=runtime(api,**CONFIG)
    await backend.create(spec)
    api.by_role(role).metadata.annotations['io.agent-sandbox.egress-spec-version']='unknown' if role=='egress' else '1'
    with pytest.raises(SandboxQueryFailed):
        await backend.get(spec.sandbox_id)
    assert (await backend.inspect_resources(spec.sandbox_id,expected_transaction_id=spec.transaction_id)).classification=='conflict'


@pytest.mark.anyio
async def test_status_only_rv_conflict_can_publish_original_receipts_once(egress_api,monkeypatch):
    """A rejected metadata CAS may retry once only after original UID/platform facts stay unchanged and RV advances."""
    from kubernetes import client
    api=egress_api
    patch=api._patch
    advanced=set()

    def drift(kind,name,namespace,kwargs):
        """Model a status write between GET and guarded PATCH without changing any platform metadata."""
        resource=api._find(kind,name,namespace)
        role=resource.metadata.labels['io.agent-sandbox.resource']
        if kind=='pod' and role not in advanced:
            advanced.add(role)
            resource.metadata.resource_version=str(int(resource.metadata.resource_version)+1)
            raise client.ApiException(status=422)
        return patch(kind,name,namespace,kwargs)

    monkeypatch.setattr(api,'_patch',drift)
    spec=authenticated_spec(network=SandboxNetwork(['example.com']))
    backend=runtime(api,**CONFIG)
    await backend.create(spec)
    assert (await backend.get(spec.sandbox_id)).status=='ready'
    assert advanced=={'container','proxy','egress'}
    creates=[r for method,r in api.trace if method=='create']
    assert len(creates)==len(set(creates))==12


@pytest.mark.anyio
async def test_rejected_cas_with_platform_change_is_never_overwritten(egress_api,monkeypatch):
    """A newer RV is insufficient when immutable platform facts changed; stop instead of refreshing them away."""
    from kubernetes import client
    from control_plane.core.errors import SandboxError
    api=egress_api
    patch=api._patch
    attempts=0

    def changed(kind,name,namespace,kwargs):
        """Advance metadata as a conflicting platform writer before rejecting the original guarded patch."""
        nonlocal attempts
        item=api._find(kind,name,namespace)
        if item.metadata.labels['io.agent-sandbox.resource']=='egress':
            attempts+=1
            item.metadata.resource_version=str(int(item.metadata.resource_version)+1)
            item.metadata.annotations['io.agent-sandbox.expires-at']='2099-01-01T00:00:00Z'
            raise client.ApiException(status=422)
        return patch(kind,name,namespace,kwargs)

    monkeypatch.setattr(api,'_patch',changed)
    with pytest.raises(SandboxError):
        await runtime(api,**CONFIG).create(authenticated_spec(network=SandboxNetwork(['example.com'])))
    assert attempts==1
    assert api.by_role('egress').metadata.annotations['io.agent-sandbox.expires-at']=='2099-01-01T00:00:00Z'


@pytest.mark.parametrize('failure,expected', [('permission','KubernetesPermissionDenied'),('timeout','KubernetesTimeout')])
@pytest.mark.anyio
async def test_component_backend_failure_during_recheck_retains_unavailable_category(egress_api,monkeypatch,failure,expected):
    """A second-read API failure must remain backend unavailability, not a synthetic routing conflict or 404."""
    from kubernetes import client
    from control_plane.core import errors
    api=egress_api
    spec=authenticated_spec(network=SandboxNetwork(['example.com']))
    backend=runtime(api,**CONFIG)
    await backend.create(spec)
    read=api._read
    reads=0

    def unavailable(kind,name,namespace,kwargs):
        """Allow initial inventory, then fail only the delegated component's required fresh observation."""
        nonlocal reads
        if kind=='service' and name.endswith('-egress'):
            reads+=1
            if reads==2:
                if failure=='permission':
                    raise client.ApiException(status=403)
                raise TimeoutError('sensitive transport diagnostic')
        return read(kind,name,namespace,kwargs)

    monkeypatch.setattr(api,'_read',unavailable)
    with pytest.raises(getattr(errors,expected)):
        await backend.get(spec.sandbox_id)
