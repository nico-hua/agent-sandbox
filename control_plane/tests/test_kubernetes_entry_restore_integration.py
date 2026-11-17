"""Double-opt-in HTTP creation and active entry recovery on the existing Minikube profile."""

import asyncio
import json
import secrets
import time

import httpx
import pytest
from kubernetes import client

from control_plane.app import create_app
from control_plane.core.errors import SandboxError
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_egress_runtime_integration import (
    egress_cluster, IMAGE, command, PREFIX,
)
from test_kubernetes_workspace_resource_integration import pytestmark as resource_marks
from test_kubernetes_proxy_integration import _RedactedText

pytestmark = [mark for mark in resource_marks if mark.name != 'timeout'] + [pytest.mark.timeout(360)]


@pytest.mark.anyio
@pytest.mark.parametrize('combined', [False, True])
async def test_http_created_active_restarts_without_initial_ids(egress_cluster, tmp_path, monkeypatch, combined):
    """Restore an original eight-resource or bound-workspace/Egress instance using persisted facts and credentials."""
    h = egress_cluster
    values = {
        'RUNTIME':'kubernetes', 'K8S_KUBECONFIG':str(h['path']), 'K8S_CONTEXT':h['context'],
        'K8S_NAMESPACE':h['namespace'], 'K8S_AGENT_IMAGE':'agent-sandbox-basic:dev',
        'K8S_PYTHON_IMAGE':IMAGE, 'K8S_PROXY_IMAGE':'agent-sandbox-proxy-upstream-test:dev',
        'K8S_WORKSPACE_STORAGE_CLASS':'agent-sandbox-workspace',
    }
    for field in ('EGRESS_IMAGE','EGRESS_DNS_NAMESPACE','EGRESS_DNS_POD_LABELS','EGRESS_CLUSTER_CIDRS','ENTRY_RESTORE_ENABLED'):
        monkeypatch.delenv('AGENT_SANDBOX_K8S_'+field, raising=False)
    if combined:
        values.update({
            'K8S_EGRESS_IMAGE':'agent-sandbox-egress:dev','K8S_EGRESS_DNS_NAMESPACE':'kube-system',
            'K8S_EGRESS_DNS_POD_LABELS':json.dumps({'k8s-app':'kube-dns'}),
            'K8S_EGRESS_CLUSTER_CIDRS':json.dumps(['10.244.0.0/16','10.96.0.0/12','192.168.49.0/24']),
        })
    for key,value in values.items():
        monkeypatch.setenv('AGENT_SANDBOX_'+key,value)
    if not combined:
        monkeypatch.delenv('AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS', raising=False)
    database = tmp_path/'entry-restore.sqlite3'
    admin = _RedactedText(secrets.token_urlsafe(32))
    headers = {'Authorization':'Bearer '+admin}
    app = create_app(api_key=str(admin), database_path=database, reaper_enabled=False, sandbox_query_timeout=10)
    manager = app.state.kubernetes_entry_manager
    creation_failures = []
    create = app.state.runtime.create

    async def observed(spec):
        """Report only fixed categories and resource evidence for unrelated baseline creation failures."""
        try:
            return await create(spec)
        except SandboxError as error:
            info = error.creation_failure
            creation_failures.append((type(error).__name__, info.phase if info else None,
                info.resource_state.value if info else None, tuple(h['locations'])))
            raise

    monkeypatch.setattr(app.state.runtime, 'create', observed)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://control',trust_env=False,timeout=120) as http:
                wid = None
                if combined:
                    made = await http.post('/v1/workspaces',headers=headers,json={'environment':'python'})
                    assert made.status_code == 201
                    wid = made.json()['workspace_id']
                options = {'environment':'python'}
                if combined:
                    options.update(workspace_id=wid, network={'allow_domains':['example.com']})
                made = await http.post('/v1/sandboxes',headers=headers,json=options)
                assert made.status_code == 201, creation_failures
                sid = made.json()['sandbox_id']
                endpoint = await http.get(f'/v1/sandboxes/{sid}/endpoint',headers=headers)
                assert endpoint.status_code == 200
                value = endpoint.json()
                token = _RedactedText(value['access_token'])
                original = await app.state.lifecycle_store.get(sid)
                original_credential = await app.state.lifecycle_store.get_credential(sid,original.transaction_id)
                assert await command(value['url'],token,
                    "import pathlib,json\np=pathlib.Path('/workspace/restart-proof.txt');p.write_text('original-data');print(json.dumps({'data':p.read_text()}))") == {'data':'original-data'}
                # Keep UIDs/objects private: fixture retains original POST authority independently.
                owned = [(entry['kind'],entry['name'],entry['uid']) for entry in h['registry'].entries]
                h['registry'].observe_volumes()
    finally:
        await manager.close_all()
    assert manager.is_closed and all(process.returncode is not None for process in h['processes'])
    old_process_count = len(h['processes'])
    # Remove all creation-only settings that an old instance must no longer need.
    for field in ('PYTHON_IMAGE','WORKSPACE_STORAGE_CLASS','EGRESS_IMAGE','EGRESS_DNS_NAMESPACE',
                  'EGRESS_DNS_POD_LABELS','EGRESS_CLUSTER_CIDRS'):
        monkeypatch.delenv('AGENT_SANDBOX_K8S_'+field,raising=False)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_ENTRY_RESTORE_ENABLED','true')
    restored = create_app(api_key=str(admin),database_path=database,reaper_enabled=False,sandbox_query_timeout=10)
    restored_manager = restored.state.kubernetes_entry_manager
    try:
        async with restored.router.lifespan_context(restored):
            result = restored.state.kubernetes_entry_restore_result
            assert result.restored == 1 and result.error is None, repr(result)
            assert await restored.state.lifecycle_store.get(sid) == original
            same_credential = await restored.state.lifecycle_store.get_credential(sid,original.transaction_id) == original_credential
            assert same_credential, 'restoration changed the persisted credential'
            assert len(h['processes']) == old_process_count+1
            assert [(entry['kind'],entry['name'],entry['uid']) for entry in h['registry'].entries] == owned
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restored),base_url='http://control',trust_env=False,timeout=120) as http:
                assert (await http.get(f'/v1/sandboxes/{sid}/endpoint')).status_code == 401
                endpoint = await http.get(f'/v1/sandboxes/{sid}/endpoint',headers=headers)
                assert endpoint.status_code == 200 and endpoint.headers['cache-control'] == 'no-store'
                value = endpoint.json()
                same_token = value['access_token'] == token
                assert same_token, 'restoration changed the instance token'
                assert await command(value['url'],token,
                    "import pathlib,json\np=pathlib.Path('/workspace/restart-proof.txt');data=p.read_text();p.write_text(data+'-continued');print(json.dumps({'data':data}))") == {'data':'original-data'}
                assert (await http.get(f'/v1/sandboxes/{sid}/endpoint',headers=headers)).status_code == 200
                assert len(h['processes']) == old_process_count+1
                deleted = await http.delete(f'/v1/sandboxes/{sid}',headers=headers,
                    params={} if combined else {'delete_workspace':'true'})
                assert deleted.status_code == 204
                assert await restored.state.lifecycle_store.get(sid) is None
                if wid:
                    assert (await http.get(f'/v1/workspaces/{wid}',headers=headers)).status_code == 200
                    assert (await http.delete(f'/v1/workspaces/{wid}',headers=headers)).status_code == 204
    finally:
        await restored_manager.close_all()
    await asyncio.to_thread(h['registry'].cleanup)
    assert all(h['registry'].read(entry) is None for entry in h['registry'].entries)
    assert all(process.returncode is not None for process in h['processes'])
    discovery = client.DiscoveryV1Api(h['api'].api_client)
    for entry in h['registry'].entries:
        if entry['kind'] == 'service':
            deadline = time.monotonic()+10
            while time.monotonic() < deadline:
                page = discovery.list_namespaced_endpoint_slice(h['namespace'],
                    label_selector='kubernetes.io/service-name='+entry['name'], limit=3, _request_timeout=(0.5,1))
                assert not page.metadata._continue
                if not page.items:
                    break
                await asyncio.sleep(0.1)
            else:
                raise AssertionError('original Service EndpointSlice cleanup not confirmed')
