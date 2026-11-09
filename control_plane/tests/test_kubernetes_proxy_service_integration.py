"""Double-opt-in proxy ClusterIP checks with independently captured original UID cleanup."""

import asyncio
import ipaddress
import json
import time

import pytest
from kubernetes import client

from control_plane.core.errors import KubernetesOperationUnsupported, SandboxNotFound
from control_plane.runtime.kubernetes.pod_query import sandbox_pod_name
from test_kubernetes_agent_service_integration import _ready_endpoint_slice
from test_kubernetes_network_integration import _eventually_blocked, _ready_pod
from test_kubernetes_proxy_integration import (
    _PROXY_IMAGE, _PYTHON_IMAGE, _ROLE, _RedactedMapping, _control_pod, _request,
)
from test_kubernetes_proxy_lifecycle_integration import (
    _assert_network_boundaries, _assert_real_protocol, owned_proxy_lifecycle,
    pytestmark as lifecycle_marks, runtime_proxy_chain,
)
from test_kubernetes_query_integration import _verify_owned

pytestmark = lifecycle_marks
_LAYOUT = "io.agent-sandbox.kubernetes-proxy-version"
_RECEIPTS = "io.agent-sandbox.proxy-resource-uids"


def _original_proxy_service(chain):
    """Read the original Service and verify its private route and persisted eight-resource bindings."""
    entry = chain["managed"].role("proxy-service")
    service = chain["api"].read_namespaced_service(
        entry["expected"]["name"], chain["namespace"], _request_timeout=(0.5, 0.5))
    _verify_owned(service, entry["expected"], entry["uid"])
    assert service.metadata.name == sandbox_pod_name(chain["spec"].sandbox_id) + "-proxy"
    assert service.metadata.labels[_ROLE] == "proxy-service"
    assert service.metadata.annotations[_LAYOUT] == "2"
    assert service.spec.type == "ClusterIP"
    assert ipaddress.ip_address(service.spec.cluster_ip).version == 4
    assert service.spec.selector == dict(chain["proxy"].metadata.labels)
    assert len(service.spec.selector) == 6
    assert service.spec.publish_not_ready_addresses in (None, False)
    assert len(service.spec.ports) == 1
    port = service.spec.ports[0]
    assert (port.name, port.protocol, port.port, port.target_port) == ("proxy-http", "TCP", 8080, 8080)
    assert not port.node_port and not service.spec.external_ips and not service.spec.external_name
    keys = {"container": "agent", "network-policy": "policy", "agent-service": "service",
            "proxy-access": "secret", "proxy": "proxy", "agent-proxy-ingress": "ingress",
            "proxy-isolation": "isolation", "proxy-service": "proxy_service"}
    expected = {keys[item["expected"]["labels"][_ROLE]]: item["uid"] for item in chain["managed"].entries}
    assert json.loads(service.metadata.annotations[_RECEIPTS]) == expected
    assert chain["agent"].metadata.annotations[_LAYOUT] == "2"
    assert json.loads(chain["agent"].metadata.annotations[_RECEIPTS]) == expected
    _ready_endpoint_slice(chain["api"], service, chain["proxy"])
    return service


def _endpoint_slices_absent(api, service):
    """Observe bounded controller cleanup without deleting or adopting EndpointSlices ourselves."""
    discovery = client.DiscoveryV1Api(api.api_client)
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        result = discovery.list_namespaced_endpoint_slice(
            service.metadata.namespace, limit=10,
            label_selector="kubernetes.io/service-name=" + service.metadata.name,
            _request_timeout=(0.5, 1))
        assert not result.metadata._continue, "test Service EndpointSlice inventory exceeded its bound"
        for item in result.items:
            assert item.metadata.labels["kubernetes.io/service-name"] == service.metadata.name
            assert any(owner.kind == "Service" and owner.api_version == "v1"
                       and owner.name == service.metadata.name and owner.uid == service.metadata.uid
                       for owner in item.metadata.owner_references or []), "EndpointSlice owner changed"
        if not result.items:
            return
        time.sleep(0.1)
    pytest.fail("original proxy Service EndpointSlices remained after Service deletion")


def test_runtime_proxy_cluster_ip_protocol_deny_and_unconfigured_restart_cleanup(runtime_proxy_chain):
    """Verify ready discovery, denied ordinary clients and authenticated protocol through the proxy Service."""
    chain = runtime_proxy_chain
    service = _original_proxy_service(chain)
    address = service.spec.cluster_ip
    assert _request(chain, address, "/healthz")["status"] == 200
    unallowed_manifest = _control_pod(
        chain["spec"], chain["namespace"], chain["node"], image=_PYTHON_IMAGE, role="proxy-test-unallowed")
    # This additional HTTP probe must fit alongside the original chain on the
    # cached-image node without reserving a full extra CPU.
    unallowed_manifest.spec.containers[0].resources.requests["cpu"] = "100m"
    unallowed_manifest.spec.containers[0].resources.limits["cpu"] = "100m"
    unallowed = chain["registry"].create("pod", unallowed_manifest)
    _ready_pod(chain["api"], unallowed["expected"], unallowed["uid"])
    denied_chain = _RedactedMapping({**chain, "client": unallowed})
    assert _request(denied_chain, chain["control_ip"], "/healthz")["status"] == 200
    _eventually_blocked(lambda: _request(denied_chain, address, "/healthz")["status"] != 0)
    # The denied client can still reach an ordinary server, and the exact
    # original proxy remains a ready endpoint reachable by the allowed client.
    assert _request(denied_chain, chain["control_ip"], "/healthz")["status"] == 200
    _ready_endpoint_slice(chain["api"], service, chain["proxy"])
    assert _request(chain, address, "/healthz")["status"] == 200
    _assert_real_protocol(chain, address)
    _assert_network_boundaries(chain)
    restarted = chain["make_runtime"]()

    async def verify():
        """Read and delete all original bindings without requiring a configured creation image."""
        observed = await restarted.get(chain["spec"].sandbox_id)
        assert observed.status == "ready"
        assert (observed.transaction_id, observed.created_at, observed.expires_at) == (
            chain["spec"].transaction_id, chain["spec"].created_at, chain["spec"].expires_at)
        with pytest.raises(KubernetesOperationUnsupported):
            await restarted.get_endpoint(chain["spec"].sandbox_id)
        await restarted.delete(chain["spec"].sandbox_id, expected_transaction_id=chain["spec"].transaction_id)
        chain["managed"].assert_absent()
        with pytest.raises(SandboxNotFound):
            await restarted.get(chain["spec"].sandbox_id)
        await restarted.delete(chain["spec"].sandbox_id, expected_transaction_id=chain["spec"].transaction_id)

    asyncio.run(verify())
    _endpoint_slices_absent(chain["api"], service)


def test_unconfigured_runtime_recovers_only_original_proxy_service(owned_proxy_lifecycle):
    """Recover a Service-only partial transaction using its original complete persisted UID receipt."""
    chain = owned_proxy_lifecycle
    runtime = chain["make_runtime"](proxy_image=_PROXY_IMAGE)
    assert asyncio.run(runtime.create(chain["spec"])) is None
    chain["managed"].assert_complete()
    for role in ("container", "proxy"):
        entry = chain["managed"].role(role)
        chain["agent" if role == "container" else "proxy"] = _ready_pod(
            chain["api"], entry["expected"], entry["uid"])
    service = _original_proxy_service(chain)
    # Keep isolation until both original workloads have disappeared. All
    # deletions use the independent registry's original UID and full metadata.
    for role in ("proxy", "container", "proxy-access", "agent-service", "agent-proxy-ingress",
                 "proxy-isolation", "network-policy"):
        chain["managed"].delete(chain["managed"].role(role))
    restarted = chain["make_runtime"]()

    async def verify():
        """Treat the missing Agent as absent but retain authority to clean its sole original companion."""
        with pytest.raises(SandboxNotFound):
            await restarted.get(chain["spec"].sandbox_id)
        await restarted.delete(chain["spec"].sandbox_id, expected_transaction_id=chain["spec"].transaction_id)
        chain["managed"].assert_absent()
        await restarted.delete(chain["spec"].sandbox_id, expected_transaction_id=chain["spec"].transaction_id)

    asyncio.run(verify())
    _endpoint_slices_absent(chain["api"], service)
