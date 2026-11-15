"""Trusted digest parsing and authenticated public workspace lifecycle without cluster access."""

import asyncio
import builtins
import socket
import subprocess
import threading
from pathlib import Path

import httpx
import pytest
from kubernetes import client

from control_plane.app import create_app
from control_plane.core.errors import (
    KubernetesConfigurationError, KubernetesOperationUnsupported, SandboxEnvironmentNotRegistered,
)
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from test_kubernetes_workspace_create import OwnedAPI, store
from test_kubernetes_workspace_spec import NAMESPACE, SC, UID
from test_kubernetes_workspace_resource import clock

DIGEST = 'sha256:' + 'b' * 64
KEY = 'workspace-images-test-key'
HEADERS = {'Authorization': 'Bearer ' + KEY}


def backend(api, *, environment='basic', image='example/agent@' + DIGEST, storage_class=SC):
    """Freeze trusted environment/storage configuration and retain only an injected offline factory."""
    def factory(**kwargs):
        """Return the fake client only for PVC operations, never for reference parsing."""
        api.threads.append(threading.get_ident())
        return api

    return KubernetesSandboxRuntime(kubeconfig_path='/not-read', context='explicit', namespace=NAMESPACE,
        environment_images={environment: image}, proxy_image='proxy:dev', workspace_storage_class=storage_class,
        client_factory=factory)


@pytest.mark.anyio
@pytest.mark.parametrize('environment', ['basic', 'python', 'node'])
@pytest.mark.parametrize('repository', ['example/agent', 'registry.example:5443/project/agent:release'])
async def test_digest_reference_is_resolved_without_client(environment, repository):
    """Only the reference's actual SHA256 suffix becomes workspace identity; registry ports and tags are supported."""
    api = OwnedAPI()
    result = await backend(api, environment=environment, image=repository + '@' + DIGEST).resolve_environment_image(environment)
    assert result == DIGEST and api.calls == [] and api.threads == []


@pytest.mark.anyio
@pytest.mark.parametrize('image,storage_class', [('example/agent:dev', SC), ('example/agent@' + DIGEST, None)])
async def test_unsatisfied_workspace_preconditions_rejected_without_client(image, storage_class):
    """Tags and missing storage configuration cannot trigger any resource or immutable-ID fabrication."""
    api = OwnedAPI()
    with pytest.raises(KubernetesOperationUnsupported):
        await backend(api, image=image, storage_class=storage_class).resolve_environment_image('basic')
    assert api.calls == [] and api.threads == []


@pytest.mark.anyio
@pytest.mark.parametrize('environment', ['basic', 'python', 'node'])
async def test_public_workspace_http_lifecycle_uses_reference_digest(store, environment):
    """Real Service writes pending, confirms the original PVC receipt and serves GET/DELETE without internal bypass."""
    api = OwnedAPI()
    runtime = backend(api, environment=environment)
    app = create_app(runtime=runtime, store=store, api_key=KEY, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control') as caller:
        response = await caller.post('/v1/workspaces', headers=HEADERS, json={'environment': environment})
        assert response.status_code == 201
        body = response.json()
        assert set(body) == {'workspace_id', 'status', 'sandbox_id', 'created_at'}
        assert body['status'] == 'available' and body['sandbox_id'] is None
        saved = await store.get_workspace(body['workspace_id'])
        receipt = await store.get_workspace_receipt(saved.workspace_id, saved.creation_token, expected_version=1)
        assert saved.image_id == receipt.image_id == DIGEST and receipt.pvc_uid == UID
        assert api.pvc.status.phase == 'Pending'
        path = '/v1/workspaces/' + saved.workspace_id
        assert (await caller.get(path, headers=HEADERS)).status_code == 200
        assert (await caller.delete(path, headers=HEADERS)).status_code == 204
        assert (await caller.delete(path, headers=HEADERS)).status_code == 204
    assert api.pvc is None and await store.get_workspace(saved.workspace_id) is None


@pytest.mark.anyio
async def test_formal_configuration_is_frozen_for_public_workspace_creation(store, monkeypatch):
    """The actual app factory uses one trusted digest snapshot, never later environment variable changes."""
    from test_backend_configuration import configure_kubernetes
    from control_plane.runtime.kubernetes import kubernetes_runtime as runtime_module

    api = OwnedAPI()
    configure_kubernetes(monkeypatch)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_AGENT_IMAGE', 'example/basic@' + DIGEST)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_PYTHON_IMAGE', 'example/python:release@' + DIGEST)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS', SC)

    def factory(**kwargs):
        """Inject only the offline SDK boundary while retaining the real selected-backend configuration path."""
        return api

    monkeypatch.setattr(runtime_module, 'create_probe_client', factory)
    app = create_app(store=store, api_key=KEY, reaper_enabled=False)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_PYTHON_IMAGE', 'different/python:latest')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control') as caller:
        response = await caller.post('/v1/workspaces', headers=HEADERS, json={'environment': 'python'})
    assert response.status_code == 201
    saved = await store.get_workspace(response.json()['workspace_id'])
    assert saved.image_id == DIGEST


@pytest.mark.anyio
async def test_proxyless_workspace_binding_is_rejected_before_identity_or_occupancy(store, monkeypatch):
    """A Runtime without the complete authenticated layout still rejects binding before identity or occupancy."""
    api = OwnedAPI()
    app = create_app(runtime=backend(api), store=store, api_key=KEY, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control') as caller:
        created = await caller.post('/v1/workspaces', headers=HEADERS, json={})
        assert created.status_code == 201
        wid = created.json()['workspace_id']
        api.calls.clear()
        unsupported = KubernetesSandboxRuntime(kubeconfig_path='/not-read', context='explicit', namespace=NAMESPACE,
                                               environment_images={'basic': 'example/agent@' + DIGEST})
        app.state.service._runtime = unsupported

        def forbidden(*args, **kwargs):
            """Catch any identity allocation or persisted occupancy/instance write after the unsupported request."""
            pytest.fail('unsupported binding allocated identity or wrote metadata')

        monkeypatch.setattr(app.state.service, '_id_factory', forbidden)
        monkeypatch.setattr(store, 'create_with_workspace', forbidden)
        monkeypatch.setattr(store, 'create', forbidden)
        response = await caller.post('/v1/sandboxes', headers=HEADERS, json={'workspace_id': wid})
        assert response.status_code == 501
        assert response.json()['error']['code'] == 'runtime_operation_unsupported'
        saved = await store.get_workspace(wid)
        assert saved.version == 1 and saved.sandbox_id is None
        assert api.calls == []


@pytest.mark.anyio
@pytest.mark.parametrize('environment', ['tools', 'python', None, [], 'BASIC'])
async def test_unregistered_environment_does_not_resolve_or_access_sdk(environment):
    """An absent environment never falls back to the basic image or allocates backend resources."""
    api = OwnedAPI()
    with pytest.raises(SandboxEnvironmentNotRegistered):
        await backend(api).resolve_environment_image(environment)
    assert api.calls == [] and api.threads == []


@pytest.mark.parametrize('image', [
    'example/agent@', 'example/agent@sha256:', 'example/agent@sha256:' + 'a' * 63,
    'example/agent@sha256:' + 'A' * 64, 'example/agent@sha256:' + 'g' * 64,
    'example/agent@sha512:' + 'a' * 128, 'example/agent@@' + DIGEST,
    'example/agent@' + DIGEST + '\n', '@' + DIGEST, 'https://registry/agent@' + DIGEST,
])
def test_invalid_references_rejected_during_inert_configuration(image):
    """Malformed, incomplete or unsupported digests cannot survive the existing image syntax validator."""
    api = OwnedAPI()
    with pytest.raises(KubernetesConfigurationError) as failed:
        backend(api, image=image)
    assert image not in str(failed.value)
    assert api.calls == [] and api.threads == []


@pytest.mark.anyio
async def test_reference_resolution_has_no_external_io_or_new_client(monkeypatch):
    """Pure configuration parsing cannot open files, spawn a process, create a socket or acquire an SDK/Docker client."""
    import docker

    runtime = backend(OwnedAPI())

    def forbidden(*args, **kwargs):
        """Catch external boundaries that a reference-only parse must never cross."""
        pytest.fail('reference parsing performed external I/O')

    with monkeypatch.context() as patch:
        patch.setattr(runtime, '_client_factory', forbidden)
        patch.setattr(builtins, 'open', forbidden)
        patch.setattr(Path, 'open', forbidden)
        patch.setattr(socket, 'socket', forbidden)
        patch.setattr(subprocess, 'Popen', forbidden)
        patch.setattr(docker, 'from_env', forbidden)
        assert await runtime.resolve_environment_image('basic') == DIGEST


@pytest.mark.anyio
@pytest.mark.parametrize('case', ['tag', 'storage', 'environment'])
async def test_http_preconditions_fail_before_identity_pending_or_receipt(store, monkeypatch, case):
    """Rejected configuration never creates a meaningless pending workspace or invokes any storage write."""
    from control_plane.core import service as service_module

    api = OwnedAPI()
    runtime = backend(api, image='example/agent:dev' if case == 'tag' else 'example/agent@' + DIGEST,
                      storage_class=None if case == 'storage' else SC)
    app = create_app(runtime=runtime, store=store, api_key=KEY, reaper_enabled=False)

    def forbidden(*args, **kwargs):
        """Reject all identity and metadata write boundaries before an unsupported request can enter pending."""
        pytest.fail('rejected workspace creation allocated identity or wrote metadata')

    monkeypatch.setattr(service_module.secrets, 'token_hex', forbidden)
    for method in ('create_workspace', 'save_workspace_receipt', 'confirm_workspace_with_receipt', 'confirm_workspace'):
        monkeypatch.setattr(store, method, forbidden)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control') as caller:
        response = await caller.post('/v1/workspaces', headers=HEADERS,
                                     json={'environment': 'python' if case == 'environment' else 'basic'})
    assert response.status_code == (400 if case == 'environment' else 501)
    assert api.calls == [] and api.threads == []


@pytest.mark.anyio
async def test_frozen_mapping_and_original_receipt_survive_new_mapping(store):
    """Creation uses a private frozen reference; later removal of that environment cannot block original PVC management."""
    api = OwnedAPI()
    source = {'python': 'example/python@' + DIGEST}
    runtime = KubernetesSandboxRuntime(kubeconfig_path='/not-read', context='explicit', namespace=NAMESPACE,
        environment_images=source, proxy_image='proxy:dev', workspace_storage_class=SC, client_factory=lambda **kw: api)
    source['python'] = 'different/python@sha256:' + 'f' * 64
    assert await runtime.resolve_environment_image('python') == DIGEST
    app = create_app(runtime=runtime, store=store, api_key=KEY, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control') as caller:
        created = await caller.post('/v1/workspaces', headers=HEADERS, json={'environment': 'python'})
    assert created.status_code == 201
    wid = created.json()['workspace_id']
    changed = create_app(runtime=backend(api, image='different/basic:dev', storage_class=None), store=store,
                         api_key=KEY, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=changed), base_url='http://control') as caller:
        assert (await caller.get('/v1/workspaces/' + wid, headers=HEADERS)).status_code == 200
        assert (await caller.delete('/v1/workspaces/' + wid, headers=HEADERS)).status_code == 204


@pytest.mark.anyio
@pytest.mark.parametrize('mode', ['reject', 'lost', 'close'])
async def test_public_create_failure_preserves_pending_and_only_original_receipts(store, mode, clock):
    """Public digest creation retains existing failure evidence: candidates are never promoted, originals survive close errors."""
    api = OwnedAPI()
    if mode == 'reject':
        api.reject = client.ApiException(status=403)
    elif mode == 'lost':
        api.post_error = TimeoutError()
    else:
        api.close_error = OSError('private connection details')
    app = create_app(runtime=backend(api), store=store, api_key=KEY, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control') as caller:
        response = await caller.post('/v1/workspaces', headers=HEADERS, json={})
    assert response.status_code == 500 and 'private' not in response.text
    wid = response.json()['error']['workspace_id']
    saved = await store.get_workspace(wid)
    assert saved.management_state == 'pending' and saved.version == 0
    receipt = await store.get_workspace_receipt(wid, saved.creation_token, expected_version=0)
    assert (receipt is not None) == (mode == 'close')
    assert not any(call[0] == 'delete' for call in api.calls)


@pytest.mark.anyio
async def test_http_create_repeated_cancel_preserves_late_original_receipt(store):
    """Caller cancellation waits for late PVC/atomic confirmation and never deletes the newly created data."""
    api = OwnedAPI()
    entered, release = threading.Event(), threading.Event()

    def block():
        """Hold the actual fake POST until both cancellation signals have been sent."""
        entered.set()
        assert release.wait(5)

    api.before_post = block
    app = create_app(runtime=backend(api), store=store, api_key=KEY, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control') as caller:
        task = asyncio.create_task(caller.post('/v1/workspaces', headers=HEADERS, json={}))
        try:
            assert await asyncio.to_thread(entered.wait, 5)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
            assert not task.done()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    wid = api.pvc.metadata.labels['io.agent-sandbox.workspace-id']
    saved = await store.get_workspace(wid)
    receipt = await store.get_workspace_receipt(wid, saved.creation_token, expected_version=1)
    assert saved.management_state == 'available' and receipt.pvc_uid == UID and receipt.image_id == DIGEST
    assert not any(call[0] == 'delete' for call in api.calls)
