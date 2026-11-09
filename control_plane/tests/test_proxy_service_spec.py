"""Pure proxy-facing Service contracts, ownership and conservative routing validation."""

from copy import deepcopy
from datetime import datetime, timezone

import pytest
from kubernetes import client

from control_plane.core.errors import SandboxDeleteConflict, SandboxQueryFailed
from control_plane.runtime.kubernetes.pod_query import LABEL_PREFIX
from test_kubernetes_agent_service import owned_triple
from test_kubernetes_query import NAMESPACE, SID, TXN

PROXY_SERVICE_UID = "d1add0a2-3948-44ab-8055-60e83f0bcf4c"


def agent_pod():
    """Build an observed Agent with independent business identity and Runtime v2 metadata."""
    pod = owned_triple().pod
    pod.metadata.annotations.update({
        LABEL_PREFIX + "kubernetes-proxy-version": "2",
        LABEL_PREFIX + "proxy-token-sha256": "a" * 64,
        LABEL_PREFIX + "proxy-image": "agent-sandbox-proxy:dev",
        LABEL_PREFIX + "proxy-resource-uids": '{"agent":"' + pod.metadata.uid + '"}',
        "example.org/annotation": "unrelated",
    })
    return pod


def observed_service(pod):
    """Add only API-assigned identity and a routable cluster address to the pure Service body."""
    from control_plane.runtime.kubernetes.proxy_service import build_proxy_service
    service = build_proxy_service(pod)
    service.metadata.uid, service.metadata.resource_version = PROXY_SERVICE_UID, "42"
    service.spec.cluster_ip = "10.96.0.43"
    service.spec.cluster_ips = ["10.96.0.43"]
    service.spec.ip_families = ["IPv4"]
    service.spec.ip_family_policy = "SingleStack"
    return service


def test_proxy_service_is_unallocated_internal_and_selects_only_its_proxy():
    """A new Service must select this transaction's proxy without leaking Agent bindings or credentials."""
    from control_plane.runtime.kubernetes.proxy_service import build_proxy_service, proxy_service_name
    pod = agent_pod()
    pod.metadata.labels["example.org/extra"] = "not-an-identity-label"
    before = deepcopy(pod.to_dict())
    service = build_proxy_service(pod)
    expected_labels = {
        LABEL_PREFIX + "managed": "true", LABEL_PREFIX + "project": "agent-sandbox",
        LABEL_PREFIX + "sandbox-id": SID, LABEL_PREFIX + "transaction-id": TXN,
        LABEL_PREFIX + "resource": "proxy-service", LABEL_PREFIX + "kubernetes-network-version": "1",
    }
    assert service.kind == "Service" and service.api_version == "v1"
    assert service.metadata.name == proxy_service_name(SID) == "agent-sandbox-" + SID.replace("_", "-") + "-proxy"
    assert service.metadata.namespace == NAMESPACE
    assert service.metadata.labels == expected_labels
    assert service.metadata.annotations == {
        key: value for key, value in pod.metadata.annotations.items()
        if key in {LABEL_PREFIX + name for name in (
            "created-at", "expires-at", "kubernetes-proxy-version", "proxy-token-sha256", "proxy-image")}
    }
    assert service.spec.selector == {**expected_labels, LABEL_PREFIX + "resource": "proxy"}
    assert service.spec.type == "ClusterIP" and service.spec.cluster_ip is None and service.spec.cluster_ips is None
    assert service.spec.publish_not_ready_addresses is False
    assert len(service.spec.ports) == 1
    port = service.spec.ports[0]
    assert (port.name, port.protocol, port.port, port.target_port, port.node_port) == ("proxy-http", "TCP", 8080, 8080, None)
    assert service.spec.external_ips is service.spec.external_name is service.spec.load_balancer_ip is None
    assert pod.to_dict() == before
    service.metadata.labels.clear()
    service.metadata.annotations.clear()
    service.spec.selector.clear()
    assert pod.to_dict() == before and len(build_proxy_service(pod).spec.selector) == 6


def test_proxy_service_builder_has_no_io(monkeypatch):
    """Pure construction may not allocate a client, load configuration or access files."""
    from pathlib import Path
    from kubernetes import config
    from control_plane.runtime.kubernetes.proxy_service import build_proxy_service
    pod = agent_pod()

    def forbidden(*args, **kwargs):
        """Make accidental client creation or configuration/file access fail at the call site."""
        pytest.fail("unexpected Service builder I/O")

    monkeypatch.setattr(client, "ApiClient", forbidden)
    monkeypatch.setattr(config, "load_kube_config", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    assert build_proxy_service(pod).spec.ports[0].port == 8080


@pytest.mark.parametrize("fault", ["uid", "role", "transaction", "namespace", "created", "expires", "version", "deleting"])
def test_proxy_service_builder_rejects_invalid_observed_agent(fault):
    """Unowned, incomplete or legacy Agent observations cannot produce a Runtime v2 Service."""
    from control_plane.runtime.kubernetes.proxy_service import build_proxy_service
    pod = agent_pod()
    if fault == "uid":
        pod.metadata.uid = None
    elif fault == "role":
        pod.metadata.labels[LABEL_PREFIX + "resource"] = "proxy"
    elif fault == "transaction":
        pod.metadata.labels[LABEL_PREFIX + "transaction-id"] = "invalid"
    elif fault == "namespace":
        pod.metadata.name = "other-agent"
    elif fault == "created":
        pod.metadata.annotations[LABEL_PREFIX + "created-at"] = "2026-01-01T00:00:00+08:00"
    elif fault == "expires":
        pod.metadata.annotations.pop(LABEL_PREFIX + "expires-at")
    elif fault == "version":
        pod.metadata.annotations[LABEL_PREFIX + "kubernetes-proxy-version"] = "1"
    else:
        pod.metadata.deletion_timestamp = datetime.now(timezone.utc)
    with pytest.raises(SandboxQueryFailed):
        build_proxy_service(pod)


def test_proxy_service_verifies_without_proxy_pod_or_endpoints():
    """Original-UID routing verification succeeds before a proxy or EndpointSlice has been created."""
    from control_plane.runtime.kubernetes.proxy_service import verify_proxy_service
    pod = agent_pod()
    service = observed_service(pod)
    verify_proxy_service(service, pod, expected_uid=PROXY_SERVICE_UID)
    service.spec.cluster_ips = ["10.96.0.43", "fd00::43"]
    service.spec.ip_families = ["IPv4", "IPv6"]
    service.spec.ip_family_policy = "PreferDualStack"
    verify_proxy_service(service, pod, expected_uid=PROXY_SERVICE_UID)


@pytest.mark.parametrize("attribute,value", [
    ("type", "NodePort"), ("type", "LoadBalancer"), ("type", "ExternalName"),
    ("cluster_ip", "None"), ("cluster_ip", None), ("cluster_ip", "127.0.0.1"),
    ("cluster_ips", ["10.96.0.99"]), ("ip_families", ["IPv6"]),
    ("selector", {}), ("ports", []), ("publish_not_ready_addresses", True),
    ("external_ips", ["1.1.1.1"]), ("external_name", "external.example"),
    ("load_balancer_ip", "1.1.1.1"), ("load_balancer_class", "external"),
    ("load_balancer_source_ranges", ["0.0.0.0/0"]), ("health_check_node_port", 30080),
    ("allocate_load_balancer_node_ports", True), ("external_traffic_policy", "Local"),
    ("internal_traffic_policy", "Local"),
])
def test_proxy_service_rejects_routing_or_external_exposure_drift(attribute, value):
    """Correct ownership must not conceal headless, redirected or externally exposed routing."""
    from control_plane.runtime.kubernetes.proxy_service import verify_proxy_service
    pod = agent_pod()
    service = observed_service(pod)
    setattr(service.spec, attribute, value)
    with pytest.raises(SandboxQueryFailed):
        verify_proxy_service(service, pod, expected_uid=PROXY_SERVICE_UID)


@pytest.mark.parametrize("attribute,value", [
    ("name", "agent-http"), ("protocol", "UDP"), ("port", 9000),
    ("target_port", "proxy-http"), ("target_port", 9000), ("node_port", 30080),
    ("app_protocol", "custom"),
])
def test_proxy_service_rejects_modified_port(attribute, value):
    """Only the named numeric TCP 8080 proxy port is a supported routing contract."""
    from control_plane.runtime.kubernetes.proxy_service import verify_proxy_service
    pod = agent_pod()
    service = observed_service(pod)
    setattr(service.spec.ports[0], attribute, value)
    with pytest.raises(SandboxQueryFailed):
        verify_proxy_service(service, pod, expected_uid=PROXY_SERVICE_UID)


@pytest.mark.parametrize("fault", ["uid", "transaction", "created", "expires", "version", "deleting", "agent-version"])
def test_proxy_service_rejects_replacement_or_business_identity_drift(fault):
    """A same-name replacement or altered transaction/time/layout cannot satisfy the original binding."""
    from control_plane.runtime.kubernetes.proxy_service import verify_proxy_service
    pod = agent_pod()
    service = observed_service(pod)
    if fault == "uid":
        service.metadata.uid = "37cd013e-c551-4924-90b6-55b2ad187244"
    elif fault == "transaction":
        service.metadata.labels[LABEL_PREFIX + "transaction-id"] = "txn_" + "f" * 32
    elif fault in {"created", "expires"}:
        service.metadata.annotations[LABEL_PREFIX + fault + "-at"] = "2026-01-01T00:00:00Z"
    elif fault == "version":
        service.metadata.annotations[LABEL_PREFIX + "kubernetes-proxy-version"] = "1"
    elif fault == "agent-version":
        pod.metadata.annotations[LABEL_PREFIX + "kubernetes-proxy-version"] = "1"
    else:
        service.metadata.deletion_timestamp = datetime.now(timezone.utc)
    with pytest.raises(SandboxQueryFailed):
        verify_proxy_service(service, pod, expected_uid=PROXY_SERVICE_UID)


@pytest.mark.parametrize("fault", ["namespace", "name", "uid", "resource-version", "role", "managed", "unknown-marker", "label-version", "version", "missing-version", "times"])
def test_proxy_service_identity_rejects_ambiguous_cleanup_ownership(fault):
    """Cleanup requires exact name/namespace, canonical ownership, timestamps and Runtime v2."""
    from control_plane.runtime.kubernetes.proxy_service import proxy_service_identity
    service = observed_service(agent_pod())
    if fault in {"namespace", "name", "uid"}:
        setattr(service.metadata, fault, "invalid")
    elif fault == "resource-version":
        service.metadata.resource_version = "42\n"
    elif fault in {"role", "managed"}:
        key = "resource" if fault == "role" else "managed"
        service.metadata.labels[LABEL_PREFIX + key] = "foreign"
    elif fault == "unknown-marker":
        service.metadata.annotations[LABEL_PREFIX + "future-runtime"] = "1"
    elif fault == "label-version":
        service.metadata.labels[LABEL_PREFIX + "kubernetes-proxy-version"] = "2"
    elif fault == "version":
        service.metadata.annotations[LABEL_PREFIX + "kubernetes-proxy-version"] = "1"
    elif fault == "missing-version":
        service.metadata.annotations.pop(LABEL_PREFIX + "kubernetes-proxy-version")
    else:
        service.metadata.annotations.pop(LABEL_PREFIX + "expires-at")
    with pytest.raises(SandboxDeleteConflict):
        proxy_service_identity(service, SID, NAMESPACE, TXN)


def test_proxy_service_cleanup_identity_does_not_require_intact_routing_or_digest():
    """Cleanup retains exact ownership despite drift and requires no unrelated Agent Service annotation."""
    from control_plane.runtime.kubernetes.proxy_service import proxy_service_identity
    service = observed_service(agent_pod())
    service.spec.type = "NodePort"
    service.metadata.annotations.pop(LABEL_PREFIX + "proxy-token-sha256")
    uid, transaction, created, expires = proxy_service_identity(service, SID, NAMESPACE, TXN)
    assert (uid, transaction) == (PROXY_SERVICE_UID, TXN)
    assert created <= expires
