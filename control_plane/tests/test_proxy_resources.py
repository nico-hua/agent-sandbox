"""Focused credential redaction and proxy layout declaration contracts."""

import base64
import hashlib
from threading import Event

import pytest

from control_plane.core.errors import KubernetesConfigurationError
from test_kubernetes_agent_service import owned_triple


def test_secret_repr_redacts_both_plaintext_and_encoded_credential():
    """SDK object diagnostics must never reveal the token or its reversible encoding."""
    from control_plane.runtime.kubernetes.proxy_spec import build_proxy_secret
    token = "Q" * 43
    secret = build_proxy_secret(owned_triple().pod, access_token=token)
    assert token not in repr(secret)
    assert base64.b64encode(token.encode()).decode() not in repr(secret)
    assert "access-token" in secret.to_dict()["data"]


def test_mark_proxy_layout_declares_only_version_and_expected_digest():
    """The Agent records credential identity before POST without carrying credential bytes."""
    from control_plane.runtime.kubernetes.proxy_resources import mark_proxy_layout
    pod = owned_triple().pod
    mark_proxy_layout(pod, "Q" * 43)
    assert pod.metadata.annotations["io.agent-sandbox.kubernetes-proxy-version"] == "2"
    assert pod.metadata.annotations["io.agent-sandbox.proxy-token-sha256"] == hashlib.sha256(b"Q" * 43).hexdigest()
    assert "Q" * 43 not in repr(pod)
    with pytest.raises(KubernetesConfigurationError):
        mark_proxy_layout(pod, "Q" * 42)


def test_query_rejects_proxy_image_changed_after_creation():
    """A fresh manager must compare the mutable Pod image against the persisted trusted image."""
    from control_plane.core.errors import SandboxQueryFailed
    from control_plane.runtime.kubernetes.pod_create import PodCreation
    from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
    from control_plane.runtime.kubernetes.proxy_resources import ProxyResources
    from kubernetes_proxy_fakes import FakeProxyClient
    from test_kubernetes_pod_spec import IMAGE, sandbox_spec
    from test_kubernetes_query import NAMESPACE, SID, TXN

    fake = FakeProxyClient()
    pod = build_pod_spec(sandbox_spec(access_token="Q" * 43), namespace=NAMESPACE, agent_image=IMAGE)
    PodCreation(pod, Event(), proxy_image="agent-sandbox-proxy:dev", access_token="Q" * 43).run(fake)
    fake.proxy_pod.spec.containers[0].image = "unexpected-image:dev"
    with pytest.raises(SandboxQueryFailed):
        ProxyResources(fake, SID, NAMESPACE, TXN).query(fake.pod)


def test_fresh_cleanup_uses_anchor_uid_after_companion_record_patch_failed(monkeypatch):
    """An anchor's original UID safely identifies a companion whose own metadata PATCH failed."""
    from kubernetes import client
    from control_plane.core.errors import SandboxCleanupFailed
    from control_plane.runtime.kubernetes import pod_delete
    from control_plane.runtime.kubernetes.pod_create import PodCreation
    from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
    from control_plane.runtime.kubernetes.proxy_resources import ProxyResources
    from kubernetes_proxy_fakes import FakeProxyClient
    from test_kubernetes_pod_spec import IMAGE, sandbox_spec
    from test_kubernetes_query import NAMESPACE, SID, TXN

    monkeypatch.setattr(pod_delete, "DELETE_TIMEOUT_SECONDS", 0.01)
    fake = FakeProxyClient()
    fake.faults[("patch", "proxy-isolation")] = client.ApiException(status=409)
    fake.stuck.add("container")
    pod = build_pod_spec(sandbox_spec(access_token="Q" * 43), namespace=NAMESPACE, agent_image=IMAGE)
    with pytest.raises(SandboxCleanupFailed):
        PodCreation(pod, Event(), proxy_image="agent-sandbox-proxy:dev", access_token="Q" * 43).run(fake)
    assert fake.by_role("proxy-isolation") is not None
    assert "io.agent-sandbox.proxy-resource-uids" not in fake.by_role("proxy-isolation").metadata.annotations
    fake.faults.clear()
    fake.stuck.clear()
    ProxyResources(fake, SID, NAMESPACE, TXN).delete_all()
    assert not fake.objects()


@pytest.mark.parametrize("digest", [None, "malformed", "0" * 64])
def test_secret_digest_corruption_fails_query_but_preserves_cleanup(digest):
    """Credential integrity is required for readiness, while original UID receipts remain cleanup authority."""
    from control_plane.core.errors import SandboxQueryFailed
    from control_plane.runtime.kubernetes.pod_create import PodCreation
    from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
    from control_plane.runtime.kubernetes.proxy_resources import ProxyResources
    from kubernetes_proxy_fakes import FakeProxyClient
    from test_kubernetes_pod_spec import IMAGE, sandbox_spec
    from test_kubernetes_query import NAMESPACE, SID, TXN

    fake = FakeProxyClient()
    pod = build_pod_spec(sandbox_spec(access_token="Q" * 43), namespace=NAMESPACE, agent_image=IMAGE)
    PodCreation(pod, Event(), proxy_image="agent-sandbox-proxy:dev", access_token="Q" * 43).run(fake)
    if digest is None:
        fake.secret.metadata.annotations.pop("io.agent-sandbox.proxy-token-sha256")
    else:
        fake.secret.metadata.annotations["io.agent-sandbox.proxy-token-sha256"] = digest
    with pytest.raises(SandboxQueryFailed):
        ProxyResources(fake, SID, NAMESPACE, TXN).query(fake.pod)
    ProxyResources(fake, SID, NAMESPACE, TXN).delete_all()
    assert not fake.objects()


def test_creation_rechecks_agent_after_proxy_starts(monkeypatch):
    """An Agent failing during proxy creation cannot produce a successful creation result."""
    from control_plane.core.errors import SandboxCreateFailed
    from control_plane.runtime.kubernetes.pod_create import PodCreation
    from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
    from kubernetes_proxy_fakes import FakeProxyClient
    from test_kubernetes_pod_spec import IMAGE, sandbox_spec
    from test_kubernetes_query import NAMESPACE

    fake = FakeProxyClient()
    original_create = fake._create

    def fail_agent_after_proxy_post(kind, namespace, kwargs):
        """Keep normal proxy startup while exposing an Agent failure on the next fresh observation."""
        result = original_create(kind, namespace, kwargs)
        if kwargs["body"].metadata.labels["io.agent-sandbox.resource"] == "proxy":
            fake.startup["container"] = "failed"
        return result

    monkeypatch.setattr(fake, "_create", fail_agent_after_proxy_post)
    pod = build_pod_spec(sandbox_spec(access_token="Q" * 43), namespace=NAMESPACE, agent_image=IMAGE)
    with pytest.raises(SandboxCreateFailed):
        PodCreation(pod, Event(), proxy_image="agent-sandbox-proxy:dev", access_token="Q" * 43).run(fake)
    assert not fake.objects()
