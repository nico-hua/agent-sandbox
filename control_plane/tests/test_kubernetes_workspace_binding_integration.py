"""Double-opt-in formal HTTP A/B reuse of one original PVC with independent UID cleanup and no data copying."""

import asyncio
import os
import secrets
import sys
import time
from pathlib import Path

import httpx
import pytest
from kubernetes import client

from control_plane.app import create_app
from control_plane.core.errors import SandboxError
from control_plane.runtime.kubernetes import kubernetes_runtime, proxy_resources
from control_plane.runtime.kubernetes.pod_query import sandbox_pod_name
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_workspace_api_integration import _cached_manifest_digest, RecordingCore
from test_kubernetes_workspace_resource_integration import _cleanup, _owned, PREFIX, SC, pytestmark as workspace_marks
from test_kubernetes_proxy_lifecycle_integration import _RuntimeResources
from test_kubernetes_proxy_integration import _RedactedSecret, _RedactedText, _RedactedMapping, _ROLE
from test_kubernetes_query_integration import _verify_owned
from test_kubernetes_local_entry_integration import _assert_port_closed

pytestmark = [mark for mark in workspace_marks if mark.name != 'timeout'] + [pytest.mark.timeout(300)]


class StrictRegistry(_RuntimeResources):
    """Independently clean only exact original POST UIDs, never adopting a later same-name candidate."""

    def delete(self, entry):
        """Refuse unknown originals while still allowing proven absence of an unacknowledged test POST."""
        if entry['uid'] is None:
            facade = self.api.networking if entry['kind'] == 'network_policy' else self.api
            try:
                getattr(facade, 'read_namespaced_' + entry['kind'])(entry['expected']['name'],
                    entry['expected']['namespace'], _request_timeout=(0.5, 1))
            except client.ApiException as error:
                if error.status == 404:
                    return
                raise
            raise AssertionError('original UID unknown; independent cleanup refused adoption')
        super().delete(entry)


class BindingFacade:
    """Record own resource POSTs and constrain scheduling without altering production ownership decisions."""

    def __init__(self, api, registry, hostname, namespace, digest, originals, *, networking=False):
        """Borrow the worker's existing pool; root cleanup has a separate independently owned client."""
        self.api, self.registry, self.hostname = api, registry, hostname
        self.namespace, self.digest, self.originals = namespace, digest, originals
        if not networking:
            self.networking = BindingFacade(api.networking, registry, hostname, namespace, digest, originals, networking=True)

    def __getattr__(self, name):
        """Delegate ordinary reads and guarded writes, redacting every Secret diagnostic object."""
        operation = getattr(self.api, name)
        if not name.startswith('create_namespaced_') and 'secret' not in name:
            return operation

        def recorded(*args, **kwargs):
            """Capture original metadata/UID before tested code; never create test instances outside HTTP."""
            if name == 'create_namespaced_persistent_volume_claim':
                body = kwargs['body'] if 'body' in kwargs else args[1]
                assert body.metadata.labels[_ROLE] == 'workspace', 'bound instance attempted an extra private PVC'
                return RecordingCore(self.api, self.namespace, self.digest, self.originals).create_namespaced_persistent_volume_claim(
                    *args, **kwargs)
            entry = None
            if name.startswith('create_namespaced_'):
                kind = name.removeprefix('create_namespaced_')
                body = kwargs['body'] if 'body' in kwargs else args[1]
                if kind == 'secret':
                    body.__class__ = _RedactedSecret
                if kind == 'pod':
                    assert body.spec.node_name is None, 'nodeName would bypass WaitForFirstConsumer'
                    body.spec.node_selector = {'kubernetes.io/hostname': self.hostname}
                    assert body.spec.containers[0].image_pull_policy == 'Never'
                entry = self.registry.prepare(kind, body)
            try:
                result = operation(*args, **kwargs)
            except client.ApiException as error:
                if 'secret' in name:
                    raise client.ApiException(status=error.status, reason='Secret diagnostics suppressed') from None
                raise
            if 'secret' in name:
                if isinstance(result, client.V1Secret):
                    result.__class__ = _RedactedSecret
                for item in result.items or [] if isinstance(result, client.V1SecretList) else []:
                    item.__class__ = _RedactedSecret
            if entry is not None:
                _verify_owned(result, entry['expected'], None)
                entry['uid'] = result.metadata.uid
            return result

        return recorded


@pytest.fixture
def binding_cluster(monkeypatch):
    """Use only the explicitly opted-in existing cluster, cached content and original-UID fallback registries."""
    path = Path(os.environ.get('KUBERNETES_TEST_KUBECONFIG', str(Path.home() / '.kube' / 'config')))
    context, namespace = os.environ['KUBERNETES_TEST_CONTEXT'], os.environ['KUBERNETES_TEST_NAMESPACE']
    digest = _cached_manifest_digest()
    factory = kubernetes_runtime.create_probe_client
    api = factory(kubeconfig_path=path, context=context)
    originals, processes = [], []
    registry = StrictRegistry(api)
    volume = {}
    try:
        node = api.read_node('agent-sandbox-k8s', _request_timeout=(0.5, 1))
        assert any(c.type == 'Ready' and c.status == 'True' for c in node.status.conditions)
        hostname = node.metadata.labels['kubernetes.io/hostname']
        storage = client.StorageV1Api(api.api_client).read_storage_class(SC, _request_timeout=(0.5, 1))
        assert storage.provisioner == 'hostpath.csi.k8s.io' and storage.volume_binding_mode == 'WaitForFirstConsumer'
        assert storage.reclaim_policy == 'Delete'
        original_proxy = proxy_resources.build_proxy_pod

        def scheduled_proxy(*args, **kwargs):
            """Keep submitted and expected proxy specs under the same trusted fixture scheduling restriction."""
            pod = original_proxy(*args, **kwargs)
            pod.spec.node_selector = {'kubernetes.io/hostname': hostname}
            return pod

        def recording_factory(**kwargs):
            """Preserve lazy worker-owned pools while recording independent original POST evidence."""
            return BindingFacade(factory(**kwargs), registry, hostname, namespace, digest, originals)

        monkeypatch.setattr(proxy_resources, 'build_proxy_pod', scheduled_proxy)
        monkeypatch.setattr(kubernetes_runtime, 'create_probe_client', recording_factory)
        spawn = asyncio.create_subprocess_exec

        async def owned_spawn(*args, **kwargs):
            """Retain exact self-owned kubectl handles for independent final process recovery."""
            process = await spawn(*args, **kwargs)
            processes.append(process)
            return process

        monkeypatch.setattr(asyncio, 'create_subprocess_exec', owned_spawn)
        yield dict(api=api, namespace=namespace, context=context, path=path, digest=digest,
                   registry=registry, originals=originals, processes=processes, volume=volume, hostname=hostname)
    finally:
        primary = sys.exc_info()[1]
        try:
            for process in processes:
                if process.returncode is None:
                    process.terminate()
                    process_loop = process._loop
                    assert not process_loop.is_closed(), 'unclosed test process lost its event loop'
            registry.cleanup()
            for record, original in originals:
                _cleanup(api, record, namespace, original)
        except BaseException:
            if primary is not None:
                primary.add_note('independent binding resource cleanup did not confirm absence')
            else:
                raise
        finally:
            api.api_client.close()


@pytest.mark.anyio
async def test_http_workspace_a_b_reuses_original_pvc_data(binding_cluster, tmp_path, monkeypatch):
    """Only formal authenticated HTTP creates workspace/A/B and automatically opens each entry before activation."""
    h = binding_cluster
    for name in ('PYTHON', 'NODE'):
        monkeypatch.delenv('AGENT_SANDBOX_K8S_' + name + '_IMAGE', raising=False)
    for key, value in {
        'AGENT_SANDBOX_RUNTIME': 'kubernetes', 'AGENT_SANDBOX_K8S_KUBECONFIG': str(h['path']),
        'AGENT_SANDBOX_K8S_CONTEXT': h['context'], 'AGENT_SANDBOX_K8S_NAMESPACE': h['namespace'],
        'AGENT_SANDBOX_K8S_AGENT_IMAGE': 'docker.io/library/agent-sandbox-basic@' + h['digest'],
        'AGENT_SANDBOX_K8S_PROXY_IMAGE': 'agent-sandbox-proxy-upstream-test:dev',
        'AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS': SC,
    }.items():
        monkeypatch.setenv(key, value)
    store = SQLiteLifecycleStore(tmp_path / 'reuse-http.sqlite3')
    admin = _RedactedText(secrets.token_urlsafe(32))
    headers = _RedactedMapping({'Authorization': _RedactedText('Bearer ' + admin)})
    app = create_app(store=store, api_key=str(admin), reaper_enabled=False, sandbox_query_timeout=10)
    manager = app.state.kubernetes_entry_manager
    create = app.state.runtime.create
    failures = []

    async def observe(spec):
        """Keep only fixed failure phase/state diagnostics if the actual HTTP creation fails."""
        try:
            await create(spec)
        except SandboxError as error:
            info = error.creation_failure
            failures.append((type(error).__name__, info.phase if info else None,
                             info.resource_state.value if info else None))
            raise

    monkeypatch.setattr(app.state.runtime, 'create', observe)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control',
                                         trust_env=False, timeout=120) as control:
                made = await control.post('/v1/workspaces', headers=headers, json={})
                assert made.status_code == 201
                wid = made.json()['workspace_id']
                record, original = h['originals'][0]
                assert wid == record.workspace_id
                original_uid = original['uid']
                path = '/v1/workspaces/' + wid
                contents = b'original-data\x00\xff\n'
                filename = 'reuse.bin'
                address_a = None
                for index in range(2):
                    made = await control.post('/v1/sandboxes', headers=headers, json={'workspace_id': wid})
                    assert made.status_code == 201, failures
                    sid = made.json()['sandbox_id']
                    assert made.json()['workspace_id'] == wid
                    assert (await control.get('/v1/sandboxes/' + sid + '/readyz', headers=headers)).status_code == 200
                    endpoint = await control.get('/v1/sandboxes/' + sid + '/endpoint', headers=headers)
                    assert endpoint.status_code == 200 and endpoint.headers['cache-control'] == 'no-store'
                    body = _RedactedMapping(endpoint.json())
                    token = _RedactedText(body.pop('access_token'))
                    url = body['url']
                    auth = _RedactedMapping({'Authorization': _RedactedText('Bearer ' + token)})
                    assert manager.session_count == 1
                    active = await store.get(sid)
                    assert active.management_state == 'active' and active.workspace_id == wid
                    observed = await control.get('/v1/sandboxes/' + sid, headers=headers)
                    assert observed.status_code == 200 and observed.json()['workspace_id'] == wid
                    pod = h['api'].read_namespaced_pod(sandbox_pod_name(sid), h['namespace'], _request_timeout=(0.5, 1))
                    assert pod.spec.node_selector == {'kubernetes.io/hostname': h['hostname']}
                    assert pod.spec.node_name and pod.status.phase == 'Running'
                    if index == 0:
                        node_a = pod.spec.node_name
                        address_a = url
                    else:
                        assert pod.spec.node_name == node_a
                    async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=10) as data:
                        assert (await data.get(url + '/v1/files', params={'path': filename})).status_code == 401
                        command = await data.post(url + '/v1/commands:run', headers=auth,
                            json={'argv': ['id', '-u'], 'timeout_ms': 3000})
                        assert command.status_code == 200 and command.json()['stdout'].strip() == '1000'
                        if index == 0:
                            upload = await data.post(url + '/v1/files', params={'path': filename}, headers=auth, content=contents)
                            assert upload.status_code == 201
                            assert (await control.post('/v1/sandboxes', headers=headers, json={'workspace_id': wid})).status_code == 409
                            assert (await control.delete(path, headers=headers)).status_code == 409
                            assert (await control.delete('/v1/sandboxes/' + sid, headers=headers,
                                                         params={'delete_workspace': 'true'})).status_code == 409
                        downloaded = await data.get(url + '/v1/files', params={'path': filename}, headers=auth)
                        assert downloaded.status_code == 200 and downloaded.content == contents
                        if index == 1:
                            changed = await data.post(url + '/v1/commands:run', headers=auth,
                                json={'argv': ['sh', '-ec', 'printf continued >> /workspace/reuse.bin'], 'timeout_ms': 3000})
                            assert changed.status_code == 200 and changed.json()['exit_code'] == 0
                            reread = await data.get(url + '/v1/files', params={'path': filename}, headers=auth)
                            assert reread.content == contents + b'continued'
                    pvc = h['api'].read_namespaced_persistent_volume_claim('agent-sandbox-wsp-' + wid[4:], h['namespace'],
                                                                           _request_timeout=(0.5, 1))
                    _owned(pvc, record, h['namespace'], original_uid)
                    assert pvc.status.phase == 'Bound'
                    pv = h['api'].read_persistent_volume(pvc.spec.volume_name, _request_timeout=(0.5, 1))
                    assert pv.spec.claim_ref.uid == original_uid and pv.spec.persistent_volume_reclaim_policy == 'Delete'
                    h['volume'].update(name=pv.metadata.name, uid=pv.metadata.uid)
                    deleted = await control.delete('/v1/sandboxes/' + sid, headers=headers)
                    assert deleted.status_code == 204 and manager.session_count == 0
                    assert await store.get(sid) is None and await store.get_credential(sid, active.transaction_id) is None
                    assert (await control.get(path, headers=headers)).json()['status'] == 'available'
                    kept = h['api'].read_namespaced_persistent_volume_claim(pvc.metadata.name, h['namespace'], _request_timeout=(0.5, 1))
                    assert kept.metadata.uid == original_uid
                    await _assert_port_closed(url)
                assert address_a is not None and len(h['processes']) == 2
                assert (await control.delete(path, headers=headers)).status_code == 204
                assert (await control.get(path, headers=headers)).status_code == 404
                await asyncio.to_thread(h['registry'].assert_absent)
        assert manager.is_closed and all(p.returncode is not None for p in h['processes'])
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                pv = h['api'].read_persistent_volume(h['volume']['name'], _request_timeout=(0.5, 1))
            except client.ApiException as error:
                assert error.status == 404
                break
            assert pv.metadata.uid == h['volume']['uid']
            await asyncio.sleep(0.2)
        else:
            raise AssertionError('associated PV controller reclamation not confirmed; no direct PV deletion attempted')
    finally:
        # Keep the running loop until every owned session/process has settled;
        # the fixture independently reclaims original Kubernetes UIDs afterwards.
        primary = sys.exc_info()[1]
        try:
            await manager.close_all()
        except BaseException:
            if primary is None:
                raise
            primary.add_note('application session cleanup failed; independent process cleanup follows')
        finally:
            for process in h['processes']:
                if process.returncode is None:
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), 5)
                    except TimeoutError:
                        process.kill()
                        await asyncio.wait_for(process.wait(), 5)
