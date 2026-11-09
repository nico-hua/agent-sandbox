"""Offline trusted proxy, Secret reference and least-privilege network specification contracts."""

import copy
import json

import pytest
from kubernetes import client

from test_kubernetes_agent_service import owned_triple
from test_kubernetes_query import SID, TXN, NAMESPACE

IMAGE = "agent-sandbox-proxy-upstream-test:dev"


def test_proxy_pod_uses_only_verified_ipv4_service_and_secret_reference():
    """Replacing the selected Service or inlining the token must fail the proxy's trust contract."""
    from control_plane.runtime.kubernetes.proxy_spec import build_proxy_pod, proxy_secret_name
    fake = owned_triple()
    proxy = build_proxy_pod(fake.pod, fake.services.service, proxy_image=IMAGE)
    assert proxy.metadata.name.endswith("-proxy") and proxy.metadata.namespace == NAMESPACE
    assert len(proxy.spec.containers) == 1
    container = proxy.spec.containers[0]
    env = {item.name: item for item in container.env}
    assert env["SANDBOX_AGENT_UPSTREAM_IPV4"].value == "10.96.0.42"
    assert env["SANDBOX_PROXY_AUTH_VERSION"].value == "1"
    token = env["SANDBOX_ACCESS_TOKEN"]
    assert token.value is None and token.value_from.secret_key_ref.name == proxy_secret_name(SID)
    assert token.value_from.secret_key_ref.key == "access-token"
    assert token.value_from.secret_key_ref.optional is False
    assert container.image == IMAGE and container.image_pull_policy == "Never"
    assert container.readiness_probe.http_get.path == "/healthz"
    assert container.readiness_probe.http_get.port == 8080
    assert proxy.spec.automount_service_account_token is False and proxy.spec.enable_service_links is False
    assert proxy.spec.host_network is proxy.spec.host_pid is proxy.spec.host_ipc is False
    assert proxy.spec.restart_policy == "Never"
    assert proxy.spec.security_context.run_as_non_root is True
    assert proxy.spec.security_context.seccomp_profile.type == "RuntimeDefault"
    assert container.security_context.allow_privilege_escalation is False
    assert container.security_context.read_only_root_filesystem is True
    assert container.security_context.capabilities.drop == ["ALL"]
    assert set(container.resources.limits) == {"cpu", "memory"}
    assert [(v.name, v.empty_dir.medium) for v in proxy.spec.volumes] == [("tmp", "Memory")]
    assert [(v.name, v.mount_path) for v in container.volume_mounts] == [("tmp", "/tmp")]


def test_secret_is_immutable_and_never_referenced_by_agent():
    """Only a separately owned opaque Secret contains token bytes; neither Pod contains them inline."""
    from control_plane.runtime.kubernetes.proxy_spec import build_proxy_secret, build_proxy_pod
    import base64
    fake = owned_triple()
    original = copy.deepcopy(fake.pod.to_dict())
    token = "Q" * 43
    secret = build_proxy_secret(fake.pod, access_token=token)
    assert secret.type == "Opaque" and secret.immutable is True
    assert base64.b64decode(secret.data["access-token"]) == token.encode("ascii")
    assert secret.string_data is None
    proxy = build_proxy_pod(fake.pod, fake.services.service, proxy_image=IMAGE)
    assert token not in json.dumps(proxy.to_dict())
    assert fake.pod.to_dict() == original
    assert fake.pod.spec.containers[0].env in (None, [])


def test_network_rules_select_only_this_proxy_and_agent_tcp_port():
    """Changing a peer to all Pods, another transaction or any port would break the least-privilege contract."""
    from control_plane.runtime.kubernetes.proxy_spec import build_proxy_network_policies
    fake = owned_triple()
    ingress, isolated = build_proxy_network_policies(fake.pod)
    agent_labels = dict(fake.pod.metadata.labels)
    proxy_labels = {**agent_labels, "io.agent-sandbox.resource": "proxy"}
    assert ingress.spec.pod_selector.match_labels == agent_labels
    assert ingress.spec.policy_types == ["Ingress"]
    assert len(ingress.spec.ingress) == 1 and not ingress.spec.egress
    assert len(ingress.spec.ingress[0]._from) == 1
    peer = ingress.spec.ingress[0]._from[0]
    assert peer.pod_selector.match_labels == proxy_labels
    assert peer.namespace_selector is None and peer.ip_block is None
    port = ingress.spec.ingress[0].ports
    assert len(port) == 1 and (port[0].protocol, port[0].port) == ("TCP", 8080)
    assert port[0].end_port is None
    assert isolated.spec.pod_selector.match_labels == proxy_labels
    assert isolated.spec.policy_types == ["Ingress", "Egress"]
    assert isolated.spec.ingress == [] and len(isolated.spec.egress) == 1
    peer = isolated.spec.egress[0].to
    assert len(peer) == 1 and peer[0].pod_selector.match_labels == agent_labels
    assert peer[0].namespace_selector is None and peer[0].ip_block is None
    port = isolated.spec.egress[0].ports
    assert len(port) == 1 and (port[0].protocol, port[0].port) == ("TCP", 8080)


@pytest.mark.parametrize("value", [None, "", "Q" * 42, "Q" * 44, "a b" * 14 + "x", "Q" * 42 + "\n", True, 123])
def test_secret_rejects_invalid_token_without_echoing_it(value):
    """Malformed credentials cannot be embedded into either a Secret or proxy startup configuration."""
    from control_plane.core.errors import KubernetesConfigurationError
    from control_plane.runtime.kubernetes.proxy_spec import build_proxy_secret
    fake = owned_triple()
    with pytest.raises(KubernetesConfigurationError) as captured:
        build_proxy_secret(fake.pod, access_token=value)
    if type(value) is str and len(value) >= 40:
        credential_hidden = value not in str(captured.value)
        assert credential_hidden, "configuration failure exposed credential input"


@pytest.mark.parametrize("fault", ["uid", "selector", "port", "namespace", "missing-binding", "headless", "ipv6"])
def test_proxy_rejects_unverified_or_non_ipv4_service(fault):
    """An arbitrary or replaced Service must never supply a trusted Nginx upstream."""
    from control_plane.core.errors import KubernetesConfigurationError, SandboxQueryFailed
    from control_plane.runtime.kubernetes.proxy_spec import build_proxy_pod
    fake = owned_triple()
    service = fake.services.service
    if fault == "uid":
        service.metadata.uid = "d5667ab6-3f02-40ba-9b29-b1967503a3f0"
    elif fault == "selector":
        service.spec.selector["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    elif fault == "port":
        service.spec.ports[0].target_port = 9090
    elif fault == "namespace":
        service.metadata.namespace = "elsewhere"
    elif fault == "missing-binding":
        fake.pod.metadata.annotations.pop("io.agent-sandbox.agent-service-uid")
    elif fault == "headless":
        service.spec.cluster_ip = "None"
    else:
        service.spec.cluster_ip = "fd00::2"
        service.spec.cluster_ips = ["fd00::2"]
        service.spec.ip_families = ["IPv6"]
    with pytest.raises((KubernetesConfigurationError, SandboxQueryFailed)):
        build_proxy_pod(fake.pod, service, proxy_image=IMAGE)


def test_companion_builds_have_no_io_or_mutable_state_sharing(monkeypatch):
    """Building trusted specs never loads cluster configuration, writes files or alters existing resources."""
    from pathlib import Path
    from kubernetes import config
    from control_plane.runtime.kubernetes.proxy_spec import build_proxy_pod, build_proxy_secret, build_proxy_network_policies
    fake = owned_triple()
    original_pod, original_service = copy.deepcopy(fake.pod.to_dict()), copy.deepcopy(fake.services.service.to_dict())

    def forbidden(*args, **kwargs):
        """Expose any accidental configuration, file or client creation by the pure builders."""
        pytest.fail("unexpected builder I/O")

    monkeypatch.setattr(client, "ApiClient", forbidden)
    monkeypatch.setattr(config, "load_kube_config", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    first = build_proxy_pod(fake.pod, fake.services.service, proxy_image=IMAGE)
    second = build_proxy_pod(fake.pod, fake.services.service, proxy_image=IMAGE)
    secret = build_proxy_secret(fake.pod, access_token="Q" * 43)
    ingress, egress = build_proxy_network_policies(fake.pod)
    first.metadata.labels.clear()
    first.spec.containers[0].env.clear()
    ingress.spec.ingress[0]._from[0].pod_selector.match_labels.clear()
    assert egress.spec.pod_selector.match_labels["io.agent-sandbox.transaction-id"] == TXN
    assert second.spec.containers[0].env and second.metadata.labels
    assert secret.metadata.labels["io.agent-sandbox.resource"] == "proxy-access"
    assert fake.pod.to_dict() == original_pod and fake.services.service.to_dict() == original_service
