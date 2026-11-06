"""Double-opt-in API admission validation only; no Pod is persisted or started."""

import os
import secrets
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from kubernetes import client
from kubernetes.utils.quantity import parse_quantity

from control_plane.core.models import SandboxSpec
from control_plane.runtime.kubernetes.kubernetes_runtime import create_probe_client
from control_plane.runtime.kubernetes.pod_spec import build_pod_spec

pytestmark = [
    pytest.mark.kubernetes_integration,
    pytest.mark.kubernetes_resource_integration,
    pytest.mark.timeout(15),
    pytest.mark.skipif(
        os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
        or os.getenv("RUN_KUBERNETES_RESOURCE_INTEGRATION") != "1"
        or not os.getenv("KUBERNETES_TEST_CONTEXT")
        or not os.getenv("KUBERNETES_TEST_NAMESPACE"),
        reason="both Kubernetes opt-ins, explicit context and namespace are required for server-side dry-run",
    ),
]


def test_server_side_dry_run_accepts_spec_without_persisting_pod():
    """Submit only dry_run=All and verify the unique name is absent before and after API admission."""
    namespace = os.environ["KUBERNETES_TEST_NAMESPACE"]
    context = os.environ["KUBERNETES_TEST_CONTEXT"]
    path = Path(os.environ.get("KUBERNETES_TEST_KUBECONFIG", str(Path.home() / ".kube" / "config")))
    created = datetime(2026, 11, 6, 8, 0, 0, 123456, tzinfo=UTC)
    spec = SandboxSpec(
        sandbox_id="sbx_" + secrets.token_hex(16), transaction_id="txn_" + secrets.token_hex(16),
        container_name="unused", network_name="unused", volume_name="unused",
        created_at=created, expires_at=created + timedelta(minutes=30),
    )
    pod = build_pod_spec(spec, namespace=namespace, agent_image="agent-sandbox-basic:dev")
    api = create_probe_client(kubeconfig_path=path, context=context)
    try:
        target = api.read_namespace(namespace, _request_timeout=(0.5, 0.5))
        assert target.metadata.name == namespace
        with pytest.raises(client.ApiException) as before:
            api.read_namespaced_pod(pod.metadata.name, namespace, _request_timeout=(0.5, 0.5))
        assert before.value.status == 404
        # This test has no non-dry-run submission or deletion fallback. Admission
        # success does not exercise the image, kubelet, mount permissions or Agent.
        admitted = api.create_namespaced_pod(
            namespace, body=pod, dry_run="All", _request_timeout=(0.5, 2),
        )
        with pytest.raises(client.ApiException) as after:
            api.read_namespaced_pod(pod.metadata.name, namespace, _request_timeout=(0.5, 0.5))
        assert after.value.status == 404
        assert admitted.metadata.name == pod.metadata.name and admitted.metadata.namespace == namespace
        assert admitted.metadata.labels == pod.metadata.labels
        assert all(admitted.metadata.annotations.get(key) == value for key, value in pod.metadata.annotations.items())
        assert admitted.spec.restart_policy == "Never" and len(admitted.spec.containers) == 1
        assert admitted.spec.security_context.run_as_user == admitted.spec.security_context.fs_group == 1000
        # The API canonicalizes quantities (for example 1000m -> 1). Compare
        # exact numeric quantities rather than requiring the original spelling.
        for field in ("requests", "limits"):
            quantities = getattr(admitted.spec.containers[0].resources, field)
            requested = getattr(pod.spec.containers[0].resources, field)
            assert set(quantities) == set(requested)
            assert all(parse_quantity(quantities[key]) == parse_quantity(value) for key, value in requested.items())
    finally:
        api.api_client.close()
