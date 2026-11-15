"""Opt-in internal Service creation with a temporary database and independently owned PVC cleanup."""

import os
from pathlib import Path

import pytest

from control_plane.core.service import SandboxService
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime, create_probe_client
from control_plane.runtime.kubernetes.workspaces import WorkspaceResource
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_workspace_resource_integration import owned_workspace, RecordingAPI, pytestmark


@pytest.mark.anyio
async def test_service_internal_creation_persists_original_receipt_across_store_rebuild(owned_workspace, tmp_path):
    """Create through real Service and Store, then verify using the saved original POST UID, without consumers."""
    _, cleanup_api, record, namespace, original = owned_workspace
    path = Path(os.environ.get('KUBERNETES_TEST_KUBECONFIG', str(Path.home() / '.kube' / 'config')))
    store = SQLiteLifecycleStore(tmp_path / 'internal-workspace.sqlite3')
    await store.initialize()

    def factory(**kwargs):
        """Acquire a worker-owned SDK pool and independently record its original POST for fixture cleanup."""
        assert store._get_workspace_sync(record.workspace_id) == record
        return RecordingAPI(create_probe_client(**kwargs), record, namespace, original)

    runtime = KubernetesSandboxRuntime(kubeconfig_path=path, context=os.environ['KUBERNETES_TEST_CONTEXT'],
        namespace=namespace, proxy_image='agent-sandbox-proxy-upstream-test:dev',
        workspace_storage_class='agent-sandbox-workspace', client_factory=factory)
    # This trusted record supplies a real cached image identity. Public image
    # resolution and HTTP workspace creation remain explicitly unsupported.
    result = await SandboxService(runtime, store=store)._create_workspace_record(record)
    assert result.management_state == 'available' and result.version == 1
    reopened = SQLiteLifecycleStore(store.database_path)
    saved = await reopened.get_workspace_receipt(record.workspace_id, record.creation_token, expected_version=1)
    assert saved.pvc_uid == original['uid'] and saved.namespace == namespace
    assert saved.pvc_name == 'agent-sandbox-wsp-' + record.workspace_id[4:]
    assert saved.storage_class == 'agent-sandbox-workspace' and saved.original_uid_verified is True
    inspector = WorkspaceResource(cleanup_api, record, namespace=namespace, storage_class=saved.storage_class)
    observed = inspector.inspect(expected_uid=saved.pvc_uid)
    assert observed.uid == saved.pvc_uid and observed.original_uid_verified is True
    pvc = cleanup_api.read_namespaced_persistent_volume_claim(saved.pvc_name, namespace, _request_timeout=(0.5, 1))
    assert pvc.status is None or pvc.status.phase in (None, 'Pending')
    assert pvc.spec.volume_name in (None, '')
    # The imported fixture independently cleans using the captured original
    # POST UID, never the Runtime's still-unsupported delete_workspace method.
