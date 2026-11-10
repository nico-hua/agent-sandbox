"""Double-opt-in same-node PVC persistence, with independent UID-guarded cleanup.

This exercises a private PVC foundation only: ReadWriteOnce does not establish
application exclusivity, and the requested capacity does not prove a hard quota.
"""

import os
import secrets
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from kubernetes import client

from control_plane.core.models import SandboxSpec, format_utc_datetime

pytestmark = [
    pytest.mark.kubernetes_integration,
    pytest.mark.kubernetes_resource_integration,
    pytest.mark.timeout(300),
    pytest.mark.skipif(
        os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
        or os.getenv("RUN_KUBERNETES_RESOURCE_INTEGRATION") != "1"
        or os.getenv("KUBERNETES_TEST_CONTEXT") != "agent-sandbox-k8s"
        or os.getenv("KUBERNETES_TEST_NAMESPACE") != "agent-sandbox-test",
        reason="both Kubernetes opt-ins and the explicit existing local test context/namespace are required",
    ),
]

_IMAGE = "agent-sandbox-basic:dev"
_STORAGE_CLASS = "agent-sandbox-workspace"
_VERSION_ANNOTATION = "io.agent-sandbox.private-workspace-version"


def _verify_owned(resource, expected: dict, uid: str | None) -> str:
    """Require the exact original resource identity and every test ownership field."""
    metadata = resource.metadata
    assert (metadata.name, metadata.namespace) == (expected["name"], expected["namespace"]), "resource target changed"
    assert isinstance(metadata.uid, str) and metadata.uid, "resource UID is missing"
    if uid is not None:
        assert metadata.uid == uid, "resource was replaced; refusing mutation"
    assert all((metadata.labels or {}).get(key) == value for key, value in expected["labels"].items()), "resource ownership changed"
    assert all((metadata.annotations or {}).get(key) == value for key, value in expected["annotations"].items()), "resource lifecycle annotations changed"
    return metadata.uid


def _read_optional(read, name: str, namespace: str | None = None):
    """Read an exact resource under a transport bound, treating only 404 as absence."""
    arguments = (name,) if namespace is None else (name, namespace)
    try:
        return read(*arguments, _request_timeout=(0.5, 1))
    except client.ApiException as error:
        if error.status == 404:
            return None
        raise


def _delete_owned(api, entry: dict, *, kind: str) -> None:
    """Independently delete a Pod or PVC using exact UID/resourceVersion preconditions."""
    expected = entry["expected"]
    if kind == "Pod":
        read, delete = api.read_namespaced_pod, api.delete_namespaced_pod
    else:
        assert kind == "PVC"
        read = api.read_namespaced_persistent_volume_claim
        delete = api.delete_namespaced_persistent_volume_claim
    resource = _read_optional(read, expected["name"], expected["namespace"])
    if resource is None:
        return
    uid = _verify_owned(resource, expected, entry["uid"])
    entry["uid"] = uid
    assert isinstance(resource.metadata.resource_version, str) and resource.metadata.resource_version
    delete(
        expected["name"], expected["namespace"],
        body=client.V1DeleteOptions(preconditions=client.V1Preconditions(
            uid=uid, resource_version=resource.metadata.resource_version,
        )),
        _request_timeout=(0.5, 2),
    )
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        remaining = _read_optional(read, expected["name"], expected["namespace"])
        if remaining is None:
            return
        _verify_owned(remaining, expected, uid)
        time.sleep(0.2)
    raise AssertionError(f"{kind} cleanup incomplete; name={expected['name']} uid={uid}")


def _loaded_image_node(api) -> tuple[str, str]:
    """Choose a Ready schedulable untainted node with the existing cached basic image."""
    nodes = api.list_node(limit=10, _request_timeout=(0.5, 1))
    assert not nodes.metadata._continue, "node inventory exceeded the test bound"
    for node in sorted(nodes.items, key=lambda item: item.metadata.name):
        if node.spec.unschedulable or node.spec.taints:
            continue
        if not any(condition.type == "Ready" and condition.status == "True"
                   for condition in node.status.conditions or []):
            continue
        images = {name for image in node.status.images or [] for name in image.names or []}
        hostname = (node.metadata.labels or {}).get("kubernetes.io/hostname")
        if hostname and images.intersection({_IMAGE, "docker.io/library/" + _IMAGE}):
            return node.metadata.name, hostname
    pytest.fail("no Ready schedulable node has the required cached basic image; no images were loaded")


def _verify_volume(api, pvc, entry: dict, volume: dict) -> None:
    """Pin the original PV UID and require its exact namespace/name/UID claimRef."""
    uid = _verify_owned(pvc, entry["expected"], entry["uid"])
    assert pvc.status.phase == "Bound" and pvc.spec.volume_name, "PVC has not bound"
    name = pvc.spec.volume_name
    if volume["name"] is not None:
        assert volume["name"] == name, "PVC changed its backing PV"
    volume["name"] = name
    pv = api.read_persistent_volume(name, _request_timeout=(0.5, 1))
    assert pv.metadata.name == name and isinstance(pv.metadata.uid, str) and pv.metadata.uid
    if volume["uid"] is not None:
        assert volume["uid"] == pv.metadata.uid, "backing PV was replaced"
    # Remember the identifier even if a later structural assertion fails; cleanup
    # can observe its fate without ever taking ownership of a cluster-scoped PV.
    volume.update(name=name, uid=pv.metadata.uid)
    reference = pv.spec.claim_ref
    assert reference is not None
    assert (reference.name, reference.namespace, reference.uid) == (
        entry["expected"]["name"], entry["expected"]["namespace"], uid,
    ), "PV claimRef does not identify the original PVC"
    assert pv.spec.storage_class_name == _STORAGE_CLASS
    assert pv.spec.persistent_volume_reclaim_policy == "Delete"


def _wait_volume_absent(api, volume: dict) -> None:
    """Observe deletion of the original PV under Delete reclaim policy without deleting it."""
    if volume["name"] is None:
        return
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        remaining = _read_optional(api.read_persistent_volume, volume["name"])
        if remaining is None:
            return
        assert remaining.metadata.uid == volume["uid"], "PV name was reused; original observation ended ambiguously"
        time.sleep(0.2)
    raise AssertionError(f"PV reclamation incomplete; name={volume['name']} uid={volume['uid']}")


def _wait_succeeded(api, entry: dict, node: str):
    """Wait for the exact controlled consumer to succeed on the selected node without reading logs."""
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        pod = api.read_namespaced_pod(entry["expected"]["name"], entry["expected"]["namespace"],
                                      _request_timeout=(0.5, 1))
        _verify_owned(pod, entry["expected"], entry["uid"])
        assert pod.status.phase != "Failed", "controlled workspace consumer failed"
        if pod.status.phase == "Succeeded":
            assert pod.spec.node_name == node, "consumer ran on a different node"
            assert len(pod.status.container_statuses or []) == 1
            terminated = pod.status.container_statuses[0].state.terminated
            assert terminated is not None and terminated.exit_code == 0, "workspace command did not succeed"
            return pod
        time.sleep(0.2)
    pytest.fail(f"consumer did not succeed within bound; name={entry['expected']['name']} uid={entry['uid']}")


@pytest.fixture
def owned_private_workspace():
    """Own one PVC and sequential consumers, independently cleaning Pods before the PVC."""
    # Imports and kubeconfig access intentionally occur only after pytest's
    # double-opt-in/local-target guard has skipped all default executions.
    from control_plane.runtime.kubernetes.kubernetes_runtime import create_probe_client
    from control_plane.runtime.kubernetes.private_workspace import PrivateWorkspaceResource, private_workspace_name

    path = Path(os.environ.get("KUBERNETES_TEST_KUBECONFIG", str(Path.home() / ".kube" / "config")))
    namespace = os.environ["KUBERNETES_TEST_NAMESPACE"]
    api = create_probe_client(kubeconfig_path=path, context=os.environ["KUBERNETES_TEST_CONTEXT"])
    created = datetime.now(UTC)
    spec = SandboxSpec(
        "sbx_" + secrets.token_hex(16), "unused", "unused", "unused",
        transaction_id="txn_" + secrets.token_hex(16), created_at=created,
        expires_at=created + timedelta(minutes=30),
    )
    annotations = {"io.agent-sandbox.created-at": format_utc_datetime(spec.created_at),
                   "io.agent-sandbox.expires-at": format_utc_datetime(spec.expires_at)}
    claim = {
        "expected": {"name": private_workspace_name(spec.sandbox_id), "namespace": namespace,
                     "labels": spec.labels_for("workspace") | {"io.agent-sandbox.resource": "private-workspace"},
                     "annotations": annotations | {_VERSION_ANNOTATION: "1"}},
        "uid": None, "submitted": False,
    }
    consumers, volume = [], {"name": None, "uid": None}

    def create_consumer(role: str, command: str):
        """Register one constrained test Pod before POST, allowing guarded cleanup after response loss."""
        expected = {"name": "workspace-" + role + "-" + secrets.token_hex(12), "namespace": namespace,
                    "labels": spec.labels_for("workspace") | {"io.agent-sandbox.resource": "private-workspace-test-consumer"},
                    "annotations": dict(annotations)}
        entry = {"expected": expected, "uid": None}
        assert _read_optional(api.read_namespaced_pod, expected["name"], namespace) is None
        manifest = {"apiVersion": "v1", "kind": "Pod", "metadata": expected, "spec": {
            # nodeName bypasses scheduling and breaks WaitForFirstConsumer provisioning.
            "nodeSelector": {"kubernetes.io/hostname": hostname}, "restartPolicy": "Never",
            "activeDeadlineSeconds": 55, "terminationGracePeriodSeconds": 2,
            "automountServiceAccountToken": False, "enableServiceLinks": False,
            "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000,
                                "fsGroup": 1000, "seccompProfile": {"type": "RuntimeDefault"}},
            "containers": [{"name": "workspace-check", "image": _IMAGE, "imagePullPolicy": "Never",
                            "command": ["/bin/sh", "-ec", command],
                            "securityContext": {"allowPrivilegeEscalation": False,
                                                "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                            "resources": {"requests": {"cpu": "100m", "memory": "64Mi"},
                                          "limits": {"cpu": "500m", "memory": "128Mi"}},
                            "volumeMounts": [{"name": "workspace", "mountPath": "/workspace"}]}],
            "volumes": [{"name": "workspace", "persistentVolumeClaim": {"claimName": claim["expected"]["name"]}}],
        }}
        consumers.append(entry)
        pod = api.create_namespaced_pod(namespace, body=manifest, _request_timeout=(0.5, 5))
        entry["uid"] = _verify_owned(pod, expected, None)
        return entry

    try:
        storage = client.StorageV1Api(api.api_client).read_storage_class(_STORAGE_CLASS, _request_timeout=(0.5, 1))
        assert storage.metadata.name == _STORAGE_CLASS
        assert storage.provisioner == "hostpath.csi.k8s.io"
        assert storage.volume_binding_mode == "WaitForFirstConsumer" and storage.reclaim_policy == "Delete"
        node, hostname = _loaded_image_node(api)
        assert _read_optional(api.read_namespaced_persistent_volume_claim, claim["expected"]["name"], namespace) is None
        resource = PrivateWorkspaceResource(api, spec, namespace=namespace, storage_class=_STORAGE_CLASS)
        yield api, resource, claim, volume, node, create_consumer
    finally:
        primary = sys.exc_info()[1]
        failures = []
        try:
            for entry in reversed(consumers):
                try:
                    _delete_owned(api, entry, kind="Pod")
                except Exception as error:
                    failures.append(f"Pod name={entry['expected']['name']} uid={entry['uid']} error={type(error).__name__}")
            # Retain storage whenever any controlled consumer could still refer
            # to it; do not rely on the helper under test to make this decision.
            if claim["submitted"] and not failures:
                try:
                    pvc = _read_optional(api.read_namespaced_persistent_volume_claim, claim["expected"]["name"], namespace)
                    if pvc is not None:
                        claim["uid"] = _verify_owned(pvc, claim["expected"], claim["uid"])
                        if pvc.status.phase == "Bound":
                            _verify_volume(api, pvc, claim, volume)
                    _delete_owned(api, claim, kind="PVC")
                    _wait_volume_absent(api, volume)
                except Exception as error:
                    failures.append(f"PVC name={claim['expected']['name']} uid={claim['uid']} error={type(error).__name__}")
            if failures:
                message = "private workspace cleanup incomplete: " + "; ".join(failures)
                message += f"; PVC name={claim['expected']['name']} uid={claim['uid']}; PV name={volume['name']} uid={volume['uid']}"
                if primary is not None:
                    primary.add_note(message)
                else:
                    pytest.fail(message)
        finally:
            api.api_client.close()


def test_private_workspace_persists_after_original_consumer_is_deleted(owned_private_workspace):
    """Write, replace the same-node consumer, read/modify, then observe original PVC/PV reclamation."""
    api, resource, claim, volume, node, create_consumer = owned_private_workspace
    assert resource.uid is None and resource.submitted is False
    claim["submitted"] = True
    observed = resource.create()
    claim["uid"] = observed.uid
    assert resource.uid == observed.uid and resource.submitted is True
    assert observed.phase in {"Pending", "Bound"}
    assert resource.inspect(expected_uid=observed.uid).uid == observed.uid
    filename, original, updated = "persistence-" + secrets.token_hex(12), secrets.token_hex(24), secrets.token_hex(24)
    target = "/workspace/" + filename
    writer = create_consumer("writer", f'test "$(id -u)" = 1000; test "$(id -g)" = 1000; printf %s {original} > {target}; test "$(cat {target})" = {original}')
    _wait_succeeded(api, writer, node)
    _delete_owned(api, writer, kind="Pod")
    assert _read_optional(api.read_namespaced_pod, writer["expected"]["name"], claim["expected"]["namespace"]) is None
    pvc = api.read_namespaced_persistent_volume_claim(claim["expected"]["name"], claim["expected"]["namespace"],
                                                    _request_timeout=(0.5, 1))
    _verify_volume(api, pvc, claim, volume)
    bound = resource.inspect(expected_uid=observed.uid)
    assert bound.uid == observed.uid and bound.phase == "Bound" and bound.volume_name == volume["name"]
    reader = create_consumer("reader", f'test "$(id -u)" = 1000; test "$(id -g)" = 1000; test "$(cat {target})" = {original}; printf %s {updated} >> {target}; test "$(cat {target})" = {original}{updated}')
    _wait_succeeded(api, reader, node)
    _delete_owned(api, reader, kind="Pod")
    assert _read_optional(api.read_namespaced_pod, reader["expected"]["name"], claim["expected"]["namespace"]) is None
    original_pvc = api.read_namespaced_persistent_volume_claim(claim["expected"]["name"], claim["expected"]["namespace"],
                                                             _request_timeout=(0.5, 1))
    _verify_volume(api, original_pvc, claim, volume)
    resource.delete(expected_uid=observed.uid)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if resource.inspect(expected_uid=observed.uid) is None:
            break
        time.sleep(0.2)
    else:
        pytest.fail(f"original PVC deletion incomplete; name={claim['expected']['name']} uid={observed.uid}")
    assert _read_optional(api.read_namespaced_persistent_volume_claim, claim["expected"]["name"], claim["expected"]["namespace"]) is None
    _wait_volume_absent(api, volume)
