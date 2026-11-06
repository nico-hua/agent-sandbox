"""Double-opt-in tests owning one uniquely labeled Pod; Runtime queries remain read-only."""

import asyncio
import os
import secrets
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from kubernetes import client

from control_plane.core.errors import SandboxNotFound
from control_plane.core.models import SandboxLifecycleRecord, format_utc_datetime
from control_plane.core.service import SandboxService
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime, create_probe_client
from control_plane.runtime.kubernetes.pod_query import sandbox_pod_name
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore

pytestmark = [
    pytest.mark.kubernetes_integration,
    pytest.mark.kubernetes_resource_integration,
    pytest.mark.timeout(100),
    pytest.mark.skipif(
        os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
        or os.getenv("RUN_KUBERNETES_RESOURCE_INTEGRATION") != "1"
        or not os.getenv("KUBERNETES_TEST_CONTEXT")
        or not os.getenv("KUBERNETES_TEST_NAMESPACE"),
        reason="both explicit Kubernetes opt-ins, context and namespace are required for test Pod creation",
    ),
]

_IMAGE = "agent-sandbox-basic:dev"


def _loaded_image_node(api) -> str:
    """Select an existing node that already holds the Agent image; never pull, build or load images."""
    nodes = api.list_node(limit=4, _request_timeout=(0.5, 0.5))
    assert not nodes.metadata._continue, "test cluster inventory must fit in one page"
    for node in sorted(nodes.items, key=lambda item: item.metadata.name, reverse=True):
        names = {name for image in (node.status.images or []) for name in image.names or []}
        if any(name in names for name in (_IMAGE, "docker.io/library/" + _IMAGE)):
            return node.metadata.name
    pytest.fail("required preloaded Agent image is missing; resource integration was not verified")


def _verify_owned(pod, expected: dict, uid: str | None) -> None:
    """Require exact target, original UID when known, every ownership label and initial business annotation."""
    metadata = pod.metadata
    assert metadata.name == expected["name"] and metadata.namespace == expected["namespace"], "cleanup target changed"
    assert isinstance(metadata.uid, str) and metadata.uid, "cleanup Pod UID is missing"
    if uid is not None:
        assert metadata.uid == uid, "cleanup Pod was replaced; refusing to delete"
    assert all((metadata.labels or {}).get(key) == value for key, value in expected["labels"].items()), "cleanup ownership labels changed"
    assert all((metadata.annotations or {}).get(key) == value for key, value in expected["annotations"].items()), "cleanup lifecycle annotations changed"


def _delete_owned(api, expected: dict, uid: str | None) -> None:
    """Delete only the test identity with UID preconditions, then observe disappearance within a bound."""
    try:
        pod = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
    except client.ApiException as error:
        if error.status == 404:
            return
        raise
    _verify_owned(pod, expected, uid)
    uid = pod.metadata.uid
    api.delete_namespaced_pod(
        expected["name"], expected["namespace"],
        body=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=uid)),
        _request_timeout=(0.5, 2),
    )
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            remaining = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        except client.ApiException as error:
            if error.status == 404:
                return
            raise
        _verify_owned(remaining, expected, uid)
        time.sleep(0.1)
    pytest.fail(f"test Pod cleanup incomplete; retained name={expected['name']} uid={uid}")


@pytest.fixture
def owned_primary_pod():
    """Create and finally clean one isolated single-Agent Pod after both opt-in guards have passed."""
    path = Path(os.environ.get("KUBERNETES_TEST_KUBECONFIG", str(Path.home() / ".kube" / "config")))
    context, namespace = os.environ["KUBERNETES_TEST_CONTEXT"], os.environ["KUBERNETES_TEST_NAMESPACE"]
    api = create_probe_client(kubeconfig_path=path, context=context)
    sandbox_id, transaction_id = "sbx_" + secrets.token_hex(16), "txn_" + secrets.token_hex(16)
    # These deliberately differ from metadata.creationTimestamp, and expiration
    # remains a business observation even when it has already passed.
    created = datetime(2026, 10, 19, 8, 0, 0, 123456, tzinfo=UTC)
    record = SandboxLifecycleRecord(sandbox_id, transaction_id, created, created + timedelta(minutes=30))
    expected = {
        "name": sandbox_pod_name(sandbox_id), "namespace": namespace,
        "labels": {"io.agent-sandbox.managed": "true", "io.agent-sandbox.project": "agent-sandbox",
            "io.agent-sandbox.sandbox-id": sandbox_id, "io.agent-sandbox.transaction-id": transaction_id,
            "io.agent-sandbox.resource": "container"},
        "annotations": {"io.agent-sandbox.created-at": format_utc_datetime(record.created_at),
                        "io.agent-sandbox.expires-at": format_utc_datetime(record.expires_at)},
    }
    submitted, uid = False, None
    try:
        node = _loaded_image_node(api)
        manifest = {"apiVersion": "v1", "kind": "Pod", "metadata": expected, "spec": {
            "nodeName": node, "restartPolicy": "Never", "terminationGracePeriodSeconds": 2,
            "automountServiceAccountToken": False, "enableServiceLinks": False,
            "securityContext": {"runAsNonRoot": True, "runAsUser": 1000, "runAsGroup": 1000},
            "containers": [{"name": "agent", "image": _IMAGE, "imagePullPolicy": "Never",
                "command": ["/usr/local/bin/agent"], "args": ["-listen", "0.0.0.0:8080"],
                "readinessProbe": {"httpGet": {"path": "/healthz", "port": 8080}, "periodSeconds": 1, "timeoutSeconds": 1},
                "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}},
                "resources": {"requests": {"cpu": "100m", "memory": "64Mi"}, "limits": {"cpu": "500m", "memory": "128Mi"}},
                "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
            }], "volumes": [{"name": "tmp", "emptyDir": {"medium": "Memory", "sizeLimit": "32Mi"}}],
        }}
        submitted = True
        pod = api.create_namespaced_pod(namespace, body=manifest, _request_timeout=(0.5, 5))
        _verify_owned(pod, expected, None)
        uid = pod.metadata.uid
        runtime = KubernetesSandboxRuntime(kubeconfig_path=path, context=context, namespace=namespace)
        yield runtime, record, api, expected, uid
    finally:
        try:
            if submitted:
                # If creation's response was lost, locate only this unique name
                # and complete ownership identity before acquiring its cleanup UID.
                _delete_owned(api, expected, uid)
        finally:
            api.api_client.close()


def test_real_primary_pod_and_service_query_are_read_only_and_survive_record_reload(owned_primary_pod, tmp_path):
    """Observe a real ready Agent Pod, merge renewed metadata, then prove absence after fixture-owned deletion."""
    runtime, record, api, expected, uid = owned_primary_pod

    async def verify():
        """Use a real temporary database while every Runtime operation remains read-only."""
        store = SQLiteLifecycleStore(tmp_path / "kubernetes-query.sqlite3")
        await store.initialize()
        await store.create(record)
        active = await store.activate(record.sandbox_id, record.transaction_id, expected_version=0)
        renewed = await store.update_expiration(
            record.sandbox_id, record.transaction_id, expected_version=active.version,
            expires_at=record.expires_at + timedelta(hours=2),
        )
        deadline = time.monotonic() + 45
        while True:
            observed = await runtime.get(record.sandbox_id)
            if observed.status == "ready":
                break
            assert observed.status in {"starting", "running"}, f"Agent did not become ready: {observed.reason}"
            assert time.monotonic() < deadline, "Agent readiness was not observed within the test bound"
            await asyncio.sleep(0.1)
        assert observed.created_at == record.created_at and observed.expires_at == record.expires_at
        assert observed.transaction_id == record.transaction_id and observed.workspace_id is None
        before = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        _verify_owned(before, expected, uid)
        service = SandboxService(runtime, store)
        assert await service.get(record.sandbox_id) == replace(observed, expires_at=renewed.expires_at)
        restarted = SandboxService(runtime, SQLiteLifecycleStore(store.database_path))
        assert (await restarted.get(record.sandbox_id)).expires_at == renewed.expires_at
        assert await store.get(record.sandbox_id) == renewed
        after = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        _verify_owned(after, expected, uid)
        assert before.metadata.labels == after.metadata.labels and before.metadata.annotations == after.metadata.annotations
        assert before.spec.to_dict() == after.spec.to_dict()
        _delete_owned(api, expected, uid)
        with pytest.raises(SandboxNotFound):
            await runtime.get(record.sandbox_id)
        with pytest.raises(SandboxNotFound):
            await service.get(record.sandbox_id)
        assert await store.get(record.sandbox_id) == renewed

    asyncio.run(verify())
