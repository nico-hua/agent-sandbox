"""Double-gated internal creation followed by authenticated receipt-based HTTP access and deletion."""

import os
import secrets
from pathlib import Path

import httpx
import pytest
from kubernetes import client

from control_plane.app import create_app
from control_plane.core.service import SandboxService
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime, create_probe_client
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_workspace_resource_integration import owned_workspace, RecordingAPI, pytestmark


@pytest.mark.anyio
async def test_rebuilt_service_and_authenticated_http_manage_original_workspace(owned_workspace, tmp_path):
    """Use only a trusted internal create, then manage the original Pending PVC through public GET/DELETE."""
    _, cleanup_api, record, namespace, original = owned_workspace
    context = os.environ['KUBERNETES_TEST_CONTEXT']
    path = Path(os.environ.get('KUBERNETES_TEST_KUBECONFIG', str(Path.home() / '.kube' / 'config')))
    store = SQLiteLifecycleStore(tmp_path / 'workspace-access.sqlite3')
    await store.initialize()

    def factory(**kwargs):
        """Record the original POST UID independently for the fixture's safe fallback cleanup."""
        return RecordingAPI(create_probe_client(**kwargs), record, namespace, original)

    creating = KubernetesSandboxRuntime(kubeconfig_path=path, context=context, namespace=namespace,
        proxy_image='agent-sandbox-proxy-upstream-test:dev', workspace_storage_class='agent-sandbox-workspace',
        client_factory=factory)
    available = await SandboxService(creating, store=store)._create_workspace_record(record)
    assert available.management_state == 'available'
    saved = await store.get_workspace_receipt(record.workspace_id, record.creation_token, expected_version=1)
    assert saved.pvc_uid == original['uid'] and saved.original_uid_verified is True
    reopened = SQLiteLifecycleStore(store.database_path)
    rebuilt = KubernetesSandboxRuntime(kubeconfig_path=path, context=context, namespace=namespace)
    assert await SandboxService(rebuilt, store=reopened).get_workspace(record.workspace_id) == available
    key = secrets.token_urlsafe(32)
    app = create_app(runtime=rebuilt, store=reopened, api_key=key, reaper_enabled=False,
                     pending_recovery_enabled=False)
    route = '/v1/workspaces/' + record.workspace_id
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control') as caller:
            assert (await caller.get(route)).status_code == 401
            assert (await caller.delete(route, headers={'Authorization': 'Bearer incorrect-test-key'})).status_code == 401
            headers = {'Authorization': 'Bearer ' + key}
            response = await caller.get(route, headers=headers)
            assert response.status_code == 200
            body = response.json()
            assert set(body) == {'workspace_id', 'status', 'sandbox_id', 'created_at'}
            assert body['workspace_id'] == record.workspace_id and body['status'] == 'available'
            pvc = cleanup_api.read_namespaced_persistent_volume_claim(saved.pvc_name, namespace, _request_timeout=(0.5, 1))
            assert pvc.metadata.uid == original['uid'] and pvc.status.phase == 'Pending'
            assert pvc.spec.volume_name in (None, '')
            assert (await caller.delete(route, headers=headers)).status_code == 204
            assert (await caller.get(route, headers=headers)).status_code == 404
            assert (await caller.delete(route, headers=headers)).status_code == 204
    assert await reopened.get_workspace(record.workspace_id) is None
    # Verify receipt cascade independently; do not recreate metadata or fake an original UID.
    with reopened._connection() as connection:
        assert connection.execute('SELECT COUNT(*) FROM workspace_resource_receipts WHERE workspace_id = ?',
                                  (record.workspace_id,)).fetchone()[0] == 0
    assert cleanup_api.read_namespace(namespace, _request_timeout=(0.5, 1)).metadata.name == namespace
    with pytest.raises(client.ApiException) as gone:
        cleanup_api.read_namespaced_persistent_volume_claim(saved.pvc_name, namespace, _request_timeout=(0.5, 1))
    assert gone.value.status == 404
    # The fixture retains independent complete-ownership/original-UID cleanup,
    # never deletes a PV and does not rely on this test's Runtime delete for safety.
