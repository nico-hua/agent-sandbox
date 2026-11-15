"""Double-opt-in independent PVC lifecycle without consumers or direct PV mutation."""

import os
import re
import secrets
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest
from kubernetes import client

from control_plane.core.errors import CreationResourceState, WorkspaceConflict
from control_plane.core.models import WorkspaceRecord

pytestmark = [
    pytest.mark.kubernetes_integration,
    pytest.mark.kubernetes_resource_integration,
    pytest.mark.timeout(150),
    pytest.mark.skipif(
        os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
        or os.getenv("RUN_KUBERNETES_RESOURCE_INTEGRATION") != "1"
        or os.getenv("KUBERNETES_TEST_CONTEXT") != "agent-sandbox-k8s"
        or os.getenv("KUBERNETES_TEST_NAMESPACE") != "agent-sandbox-test",
        reason="both opt-ins and explicit existing local context/namespace are required",
    ),
]
PREFIX = "io.agent-sandbox."
SC = "agent-sandbox-workspace"


def _cached_image_id() -> str:
    """Read only the actual cached Agent image identity, never hash a tag, build, load or pull an image."""
    result = subprocess.run([
        "minikube", "-p", "agent-sandbox-k8s", "ssh", "--node=agent-sandbox-k8s", "--",
        "sudo", "crictl", "inspecti", "--output", "go-template", "--template", "{{.status.id}}",
        "docker.io/library/agent-sandbox-basic:dev",
    ], capture_output=True, text=True, timeout=20)
    identity = result.stdout.strip()
    assert result.returncode == 0 and re.fullmatch(r"sha256:[0-9a-f]{64}", identity), "cached Agent image identity unavailable"
    return identity


def _owned(pvc, record, namespace, original_uid):
    """Independently check exact original UID and complete workspace ownership before any fixture cleanup."""
    metadata = pvc.metadata
    assert metadata.name == "agent-sandbox-wsp-" + record.workspace_id[4:] and metadata.namespace == namespace
    assert type(metadata.uid) is str and metadata.uid, "workspace UID missing"
    if original_uid is not None:
        assert metadata.uid == original_uid, "PVC replaced; refusing fixture cleanup"
    labels = {PREFIX + "managed": "true", PREFIX + "project": "agent-sandbox", PREFIX + "resource": "workspace",
              PREFIX + "workspace-id": record.workspace_id, PREFIX + "workspace-token": record.creation_token}
    annotations = {PREFIX + "workspace-version": "1", PREFIX + "workspace-image-id": record.image_id,
                   PREFIX + "created-at": record.created_at.isoformat().replace("+00:00", "Z")}
    for actual, expected in ((metadata.labels, labels), (metadata.annotations, annotations)):
        assert isinstance(actual, dict) and all(actual.get(key) == value for key, value in expected.items())
        assert not any(key.startswith(PREFIX) and key not in expected for key in actual)
    assert metadata.owner_references in (None, [])
    assert isinstance(metadata.resource_version, str) and metadata.resource_version
    return metadata.uid


def _read(api, name, namespace):
    """Treat only an object 404 as absent; callers independently confirmed their exact test namespace."""
    try:
        return api.read_namespaced_persistent_volume_claim(name, namespace, _request_timeout=(0.5, 1))
    except client.ApiException as error:
        if error.status == 404:
            return None
        raise


class RecordingAPI:
    """Capture the original POST UID before the tested resource helper can validate or fail."""

    def __init__(self, api, record, namespace, receipt):
        """Borrow the fixture-owned transport without sharing ownership with the tested helper."""
        self.api, self.record, self.namespace, self.receipt = api, record, namespace, receipt

    def __getattr__(self, key):
        """Delegate actual CoreV1 operations to the existing independently owned client."""
        return getattr(self.api, key)

    def create_namespaced_persistent_volume_claim(self, namespace, body, **kwargs):
        """Retain positive original identity evidence without ever adopting a later exact-name read."""
        self.receipt["submitted"] = True
        pvc = self.api.create_namespaced_persistent_volume_claim(namespace, body, **kwargs)
        self.receipt["uid"] = _owned(pvc, self.record, self.namespace, None)
        return pvc


def _cleanup(api, record, namespace, receipt):
    """Independently remove only this original, unconsumed PVC and observe any associated PV reclamation."""
    name = "agent-sandbox-wsp-" + record.workspace_id[4:]
    assert api.read_namespace(namespace, _request_timeout=(0.5, 1)).metadata.name == namespace
    pvc = _read(api, name, namespace)
    volume = None
    volume_uid = None
    if pvc is not None:
        assert receipt["uid"] is not None, "original UID unavailable; PVC intentionally retained"
        _owned(pvc, record, namespace, receipt["uid"])
        volume = pvc.spec.volume_name
        if volume is not None:
            pv = api.read_persistent_volume(volume, _request_timeout=(0.5, 1))
            volume_uid = pv.metadata.uid
            reference = pv.spec.claim_ref
            assert reference is not None and (reference.name, reference.namespace, reference.uid) == (
                name, namespace, receipt["uid"])
            assert pv.spec.persistent_volume_reclaim_policy == "Delete"
        pods = api.list_namespaced_pod(namespace, limit=101, watch=False, _request_timeout=(0.5, 1))
        assert len(pods.items) <= 100 and pods.metadata._continue in (None, "")
        assert pods.metadata.remaining_item_count in (None, 0)
        assert all(not (volume.persistent_volume_claim is not None
                       and volume.persistent_volume_claim.claim_name == name)
                   for pod in pods.items for volume in pod.spec.volumes or [])
        if pvc.metadata.deletion_timestamp is None:
            api.delete_namespaced_persistent_volume_claim(name, namespace, body=client.V1DeleteOptions(
                preconditions=client.V1Preconditions(uid=receipt["uid"], resource_version=pvc.metadata.resource_version)),
                _request_timeout=(0.5, 2))
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            remaining = _read(api, name, namespace)
            if remaining is None:
                break
            _owned(remaining, record, namespace, receipt["uid"])
            time.sleep(0.1)
        else:
            raise AssertionError("original test PVC cleanup incomplete")
    if volume is not None:
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                pv = api.read_persistent_volume(volume, _request_timeout=(0.5, 1))
            except client.ApiException as error:
                if error.status == 404:
                    break
                raise
            assert pv.metadata.uid == volume_uid, "PV was replaced; observation stopped"
            reference = pv.spec.claim_ref
            assert reference is not None and (reference.name, reference.namespace, reference.uid) == (
                name, namespace, receipt["uid"]), "PV no longer belongs to this test"
            assert pv.spec.persistent_volume_reclaim_policy == "Delete"
            time.sleep(0.1)
        else:
            raise AssertionError("test PV reclamation incomplete; no direct PV deletion attempted")


@pytest.fixture
def owned_workspace():
    """Prepare unique identity after explicit gates, with independent original-UID cleanup and client closure."""
    from control_plane.runtime.kubernetes.kubernetes_runtime import create_probe_client
    from control_plane.runtime.kubernetes.workspaces import WorkspaceResource

    path = Path(os.environ.get("KUBERNETES_TEST_KUBECONFIG", str(Path.home() / ".kube" / "config")))
    namespace = os.environ["KUBERNETES_TEST_NAMESPACE"]
    api = create_probe_client(kubeconfig_path=path, context=os.environ["KUBERNETES_TEST_CONTEXT"])
    record = None
    receipt = {"submitted": False, "uid": None}
    try:
        assert api.read_namespace(namespace, _request_timeout=(0.5, 1)).metadata.name == namespace
        storage = client.StorageV1Api(api.api_client).read_storage_class(SC, _request_timeout=(0.5, 1))
        assert storage.provisioner == "hostpath.csi.k8s.io"
        assert storage.volume_binding_mode == "WaitForFirstConsumer" and storage.reclaim_policy == "Delete"
        wid = "wsp_" + secrets.token_hex(16)
        record = WorkspaceRecord(wid, "wct_" + secrets.token_hex(16), "agent-sandbox-" + wid,
                                 _cached_image_id(), datetime.now(UTC))
        resource = WorkspaceResource(RecordingAPI(api, record, namespace, receipt), record,
                                     namespace=namespace, storage_class=SC)
        yield resource, api, record, namespace, receipt
    finally:
        primary = sys.exc_info()[1]
        try:
            if record is not None and receipt["submitted"]:
                _cleanup(api, record, namespace, receipt)
        except BaseException:
            if primary is not None:
                primary.add_note("independent workspace fixture cleanup did not confirm absence")
            else:
                raise
        finally:
            api.api_client.close()


def test_pending_workspace_original_uid_inspection_and_idempotent_deletion(owned_workspace):
    """Create a real PVC without consumers, inspect its fixed UID, reject replacement identity and delete safely."""
    resource, api, record, namespace, receipt = owned_workspace
    assert resource.uid is None and not resource.submitted
    created = resource.create()
    assert created.uid == resource.uid == receipt["uid"]
    assert created.original_uid_verified is True
    pvc = _read(api, resource.name, namespace)
    _owned(pvc, record, namespace, created.uid)
    assert pvc.status is None or pvc.status.phase in (None, "Pending")
    assert pvc.spec.volume_name in (None, ""), "WFFC test unexpectedly acquired a PV without a consumer"
    inspected = resource.inspect(expected_uid=created.uid)
    assert inspected.uid == created.uid
    with pytest.raises(WorkspaceConflict):
        resource.delete(expected_uid="55667ab6-3f02-40ba-9b29-b1967503a3f0")
    assert _read(api, resource.name, namespace).metadata.uid == created.uid
    resource.delete(expected_uid=created.uid)
    assert resource.inspect(expected_uid=created.uid) is None
    resource.delete(expected_uid=created.uid)
    assert _read(api, resource.name, namespace) is None
    assert resource.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
