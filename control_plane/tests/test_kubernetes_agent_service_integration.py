"""Double-opt-in Agent Service discovery, live deny-all controls and independently owned cleanup."""

import asyncio
import ipaddress
import os
import secrets
import sys
import time
from dataclasses import replace

import pytest
from kubernetes import client

from control_plane.core.errors import KubernetesOperationUnsupported
from control_plane.runtime.kubernetes.agent_service import agent_service_name
from control_plane.runtime.kubernetes.network_policy import network_policy_name
from control_plane.runtime.kubernetes.pod_query import (
    AGENT_SERVICE_UID_ANNOTATION,
    AGENT_SERVICE_VERSION,
    AGENT_SERVICE_VERSION_ANNOTATION,
    NETWORK_LAYOUT_LABEL,
)
from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
from test_kubernetes_create_integration import (
    owned_runtime_creation,
    pytestmark as creation_marks,
    test_missing_cached_image_fails_startup_and_confirms_precise_rollback as _missing_image_case,
)
from test_kubernetes_network_integration import _eventually_blocked, _http_probe, _ready_pod
from test_kubernetes_query_integration import _delete_owned, _verify_owned

pytestmark = [
    *creation_marks,
    pytest.mark.timeout(180),
    pytest.mark.skipif(
        os.getenv("KUBERNETES_TEST_CONTEXT") != "agent-sandbox-k8s"
        or os.getenv("KUBERNETES_TEST_NAMESPACE") != "agent-sandbox-test",
        reason="Agent Service integration requires the explicit existing local test context and namespace",
    ),
]


def _ready_endpoint_slice(api, service, pod):
    """Read bounded EndpointSlice pages until the exact original Pod UID and IP form a ready backend."""
    discovery = client.DiscoveryV1Api(api.api_client)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        slices = discovery.list_namespaced_endpoint_slice(
            service.metadata.namespace, limit=10,
            label_selector="kubernetes.io/service-name=" + service.metadata.name,
            _request_timeout=(0.5, 1),
        )
        assert not slices.metadata._continue, "test Service EndpointSlice inventory exceeded its bound"
        ready = []
        for item in slices.items:
            assert item.metadata.labels["kubernetes.io/service-name"] == service.metadata.name
            assert any(owner.kind == "Service" and owner.api_version == "v1"
                       and owner.name == service.metadata.name and owner.uid == service.metadata.uid
                       for owner in item.metadata.owner_references or []), "EndpointSlice owner changed"
            assert item.address_type == "IPv4"
            assert len(item.ports or []) == 1
            assert item.ports[0].port == 8080 and item.ports[0].protocol == "TCP"
            for endpoint in item.endpoints or []:
                target = endpoint.target_ref
                assert target is not None and target.kind == "Pod"
                assert (target.namespace, target.name, target.uid) == (
                    pod.metadata.namespace, pod.metadata.name, pod.metadata.uid,
                ), "EndpointSlice points at another Pod identity"
                assert endpoint.addresses == [pod.status.pod_ip]
                if endpoint.conditions is not None and endpoint.conditions.ready is True:
                    ready.append(endpoint)
        if ready:
            assert len(ready) == 1
            return
        time.sleep(0.1)
    pytest.fail("the ready Agent Pod did not appear as its Service's ready EndpointSlice backend")


def _assert_resources_absent(api, spec, expected):
    """Confirm all three exact resource names are gone using independent bounded SDK reads."""
    operations = (
        (api.read_namespaced_pod, expected["name"]),
        (api.read_namespaced_service, agent_service_name(spec.sandbox_id)),
        (api.networking.read_namespaced_network_policy, network_policy_name(spec.sandbox_id)),
    )
    for read, name in operations:
        with pytest.raises(client.ApiException) as captured:
            read(name, expected["namespace"], _request_timeout=(0.5, 0.5))
        assert captured.value.status == 404, f"resource {name} remains after transaction cleanup"


def test_agent_service_has_ready_backend_but_keeps_default_deny(owned_runtime_creation):
    """Observe a real ClusterIP and ready backend while live controls prove its ingress remains denied."""
    make_runtime, spec, api, expected, receipt, path, context = owned_runtime_creation
    runtime = make_runtime()
    controls = []

    def create_control(node):
        """Register one unique ordinary Agent Pod before POST so response loss still has guarded cleanup."""
        control_spec = replace(spec, sandbox_id="sbx_" + secrets.token_hex(16),
                               transaction_id="txn_" + secrets.token_hex(16))
        manifest = build_pod_spec(control_spec, namespace=expected["namespace"], agent_image="agent-sandbox-basic:dev")
        manifest.metadata.labels.pop(NETWORK_LAYOUT_LABEL)
        manifest.metadata.labels["io.agent-sandbox.resource"] = "network-test-control"
        manifest.metadata.annotations.pop(AGENT_SERVICE_VERSION_ANNOTATION, None)
        manifest.spec.node_name = node
        entry = {
            "expected": {"name": manifest.metadata.name, "namespace": manifest.metadata.namespace,
                         "labels": dict(manifest.metadata.labels), "annotations": dict(manifest.metadata.annotations)},
            "uid": None,
        }
        controls.append(entry)
        response = api.create_namespaced_pod(expected["namespace"], body=manifest, _request_timeout=(0.5, 2))
        _verify_owned(response, entry["expected"], None)
        entry["uid"] = response.metadata.uid
        ready = _ready_pod(api, entry["expected"], entry["uid"])
        entry["ip"] = ready.status.pod_ip
        return entry

    async def verify():
        """Create through Runtime, verify read-only discovery and actual traffic, then delete the transaction."""
        assert await runtime.create(spec) is None
        pod = _ready_pod(api, expected, receipt["uid"])
        assert (await runtime.get(spec.sandbox_id)).status == "ready"
        service = api.read_namespaced_service(agent_service_name(spec.sandbox_id), expected["namespace"],
                                              _request_timeout=(0.5, 0.5))
        service_expected = {
            **expected, "name": agent_service_name(spec.sandbox_id),
            "labels": {**expected["labels"], "io.agent-sandbox.resource": "agent-service"},
            "annotations": {
                key: expected["annotations"][key]
                for key in ("io.agent-sandbox.created-at", "io.agent-sandbox.expires-at")
            } | {AGENT_SERVICE_VERSION_ANNOTATION: AGENT_SERVICE_VERSION},
        }
        assert receipt["service_uid"] is not None
        _verify_owned(service, service_expected, receipt["service_uid"])
        assert pod.metadata.annotations[AGENT_SERVICE_UID_ANNOTATION] == service.metadata.uid
        assert pod.metadata.annotations[AGENT_SERVICE_VERSION_ANNOTATION] == AGENT_SERVICE_VERSION
        assert service.spec.type == "ClusterIP" and service.spec.publish_not_ready_addresses in (None, False)
        assert ipaddress.ip_address(service.spec.cluster_ip).version == 4
        assert service.spec.selector == expected["labels"] and len(service.spec.selector) == 6
        assert len(service.spec.ports) == 1
        port = service.spec.ports[0]
        assert (port.protocol, port.port, port.target_port) == ("TCP", 8080, 8080)
        assert not port.node_port and not service.spec.external_ips and not service.spec.external_name
        _ready_endpoint_slice(api, service, pod)
        policy = api.networking.read_namespaced_network_policy(network_policy_name(spec.sandbox_id),
                                                              expected["namespace"], _request_timeout=(0.5, 0.5))
        assert policy.spec.pod_selector.match_labels == expected["labels"]
        assert set(policy.spec.policy_types) == {"Ingress", "Egress"}
        assert not policy.spec.ingress and not policy.spec.egress
        with pytest.raises(KubernetesOperationUnsupported):
            await runtime.get_endpoint(spec.sandbox_id)

        nodes = api.list_node(limit=4, _request_timeout=(0.5, 0.5))
        assert not nodes.metadata._continue
        other = next(node.metadata.name for node in nodes.items if node.metadata.name != pod.spec.node_name
                     and not node.spec.unschedulable and not node.spec.taints)
        target = create_control(pod.spec.node_name)
        source = create_control(other)
        assert _http_probe(api, source, target["ip"], path, context), "ordinary Agent positive control is unreachable"
        _eventually_blocked(lambda: _http_probe(api, source, service.spec.cluster_ip, path, context))
        assert _http_probe(api, source, target["ip"], path, context), "positive control lost network reachability"
        # Recheck the exact ready backend after the denied request: missing or
        # unready endpoints must not count as evidence of NetworkPolicy enforcement.
        current_pod = _ready_pod(api, expected, receipt["uid"])
        _ready_endpoint_slice(api, service, current_pod)
        assert (await runtime.get(spec.sandbox_id)).status == "ready"
        await runtime.delete(spec.sandbox_id, expected_transaction_id=spec.transaction_id)
        _assert_resources_absent(api, spec, expected)

    try:
        asyncio.run(verify())
    finally:
        primary = sys.exc_info()[1]
        failures = []
        for entry in reversed(controls):
            try:
                _delete_owned(api, entry["expected"], entry["uid"])
            except Exception:
                failures.append(f"{entry['expected']['name']} uid={entry['uid']}")
        if failures:
            message = "test control cleanup incomplete: " + ", ".join(failures)
            if primary is not None:
                primary.add_note(message)
            else:
                pytest.fail(message)


def test_missing_cached_image_leaves_no_agent_service(owned_runtime_creation):
    """Require never-pull startup failure to remove the original Pod, Service and NetworkPolicy."""
    _missing_image_case(owned_runtime_creation)
    _, spec, api, expected, receipt, _, _ = owned_runtime_creation
    assert receipt["service_uid"] is not None
    _assert_resources_absent(api, spec, expected)
