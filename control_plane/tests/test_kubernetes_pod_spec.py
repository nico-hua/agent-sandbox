"""Offline contract tests for deterministic, constrained single-Agent Pod construction."""

import builtins
import copy
import json
import os
import secrets
import socket
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from kubernetes import client, config

from control_plane.core.errors import (
    KubernetesConfigurationError, KubernetesOperationUnsupported, SandboxNotFound,
)
from control_plane.core.models import (
    SandboxNetwork, SandboxResources, SandboxSpec, WorkspaceBinding,
)
from control_plane.runtime.kubernetes.pod_query import (
    NETWORK_LAYOUT_LABEL, NETWORK_LAYOUT_VERSION, query_result, sandbox_pod_name,
)
from test_kubernetes_query import CREATED, EXPIRES, LABELS, NAMESPACE, SID, TXN, UID, primary_pod, query_runtime

IMAGE = "agent-sandbox-basic:dev"


def sandbox_spec(**changes):
    """Provide explicit business identity and Docker-only names that cannot become Kubernetes resources."""
    fields = dict(
        sandbox_id=SID, transaction_id=TXN, container_name="docker_container_only",
        network_name="docker_network_only", volume_name="docker_volume_only",
        created_at=CREATED, expires_at=EXPIRES,
    )
    fields.update(changes)
    return SandboxSpec(**fields)


def build(spec=None, **changes):
    """Call the public pure builder with explicitly configured backend values."""
    from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
    fields = dict(namespace=NAMESPACE, agent_image=IMAGE)
    fields.update(changes)
    return build_pod_spec(sandbox_spec() if spec is None else spec, **fields)


def test_pod_spec_preserves_explicit_identity_and_microsecond_utc_times():
    """The builder reuses ownership conventions and business time without allocating new identity."""
    spec = sandbox_spec()
    pod = build(spec)
    assert type(pod) is client.V1Pod
    assert pod.api_version == "v1" and pod.kind == "Pod" and pod.status is None
    assert pod.metadata.name == sandbox_pod_name(SID) and pod.metadata.namespace == NAMESPACE
    assert pod.metadata.uid is None and pod.metadata.resource_version is None
    assert pod.metadata.labels == {**LABELS, NETWORK_LAYOUT_LABEL: NETWORK_LAYOUT_VERSION}
    assert pod.metadata.annotations == {
        "io.agent-sandbox.created-at": "2026-10-19T08:00:00.123456Z",
        "io.agent-sandbox.expires-at": "2026-10-19T08:30:00.123456Z",
    }
    encoded = json.dumps(pod.to_dict(serialize=True))
    assert all(name not in encoded for name in (spec.container_name, spec.network_name, spec.volume_name))


def test_pod_spec_fixes_agent_startup_and_readiness_without_restarts():
    """Only one Agent container starts directly, with the same health contract used by get()."""
    pod = build()
    assert pod.spec.restart_policy == "Never"
    assert pod.spec.init_containers in (None, []) and pod.spec.ephemeral_containers in (None, [])
    assert len(pod.spec.containers) == 1
    agent = pod.spec.containers[0]
    assert agent.name == "agent" and agent.image == IMAGE and agent.image_pull_policy == "Never"
    assert agent.command == ["/usr/local/bin/agent"] and agent.args == ["-listen", "0.0.0.0:8080"]
    assert agent.working_dir == "/workspace"
    assert len(agent.ports) == 1 and agent.ports[0].container_port == 8080
    assert agent.ports[0].protocol == "TCP" and agent.ports[0].host_port is None
    probe = agent.readiness_probe
    assert probe.http_get.path == "/healthz" and probe.http_get.port == 8080 and probe.http_get.scheme == "HTTP"
    assert probe.http_get.host in (None, "") and probe.http_get.http_headers in (None, [])
    assert probe.timeout_seconds == 1 and probe.period_seconds == 1
    assert agent.liveness_probe is None and agent.startup_probe is None


@pytest.mark.parametrize("cpu,memory", [(1000, 256), (100, 128), (2000, 2048), (123, 333)])
def test_pod_resources_preserve_millicores_and_mib_for_requests_and_limits(cpu, memory):
    """Default, boundary and fractional-CPU inputs retain exact integer Kubernetes quantities."""
    resources = SandboxResources(cpu_millis=cpu, memory_mib=memory)
    pod = build(sandbox_spec(resources=resources))
    result = pod.spec.containers[0].resources
    assert result.requests == result.limits == {"cpu": f"{cpu}m", "memory": f"{memory}Mi"}
    assert result.requests is not result.limits


def test_pod_security_context_and_service_access_are_explicitly_restricted():
    """Pod and container settings drop host access, privilege escalation and credential injection."""
    pod = build()
    security = pod.spec.security_context
    assert security.run_as_non_root is True and security.run_as_user == security.run_as_group == 1000
    assert security.fs_group == 1000 and security.seccomp_profile.type == "RuntimeDefault"
    container = pod.spec.containers[0].security_context
    assert container.privileged is False and container.allow_privilege_escalation is False
    assert container.read_only_root_filesystem is True and container.capabilities.drop == ["ALL"]
    assert container.capabilities.add in (None, [])
    assert pod.spec.host_network is False and pod.spec.host_pid is False and pod.spec.host_ipc is False
    assert pod.spec.automount_service_account_token is False and pod.spec.enable_service_links is False
    assert pod.spec.containers[0].env in (None, []) and pod.spec.containers[0].env_from in (None, [])
    assert pod.spec.node_name is None and pod.spec.image_pull_secrets in (None, [])


def test_bounded_temporary_volumes_use_fs_group_without_privileged_initializers():
    """Only two emptyDir volumes provide non-root writable mounts on the otherwise read-only root."""
    pod = build()
    volumes = {volume.name: volume for volume in pod.spec.volumes}
    mounts = {mount.mount_path: mount for mount in pod.spec.containers[0].volume_mounts}
    assert set(volumes) == {"workspace", "tmp"} and set(mounts) == {"/workspace", "/tmp"}
    assert mounts["/workspace"].name == "workspace" and mounts["/tmp"].name == "tmp"
    assert all(mount.read_only is False and mount.sub_path is None for mount in mounts.values())
    assert volumes["workspace"].empty_dir.size_limit == "1Gi" and volumes["workspace"].empty_dir.medium in (None, "")
    assert volumes["tmp"].empty_dir.size_limit == "64Mi" and volumes["tmp"].empty_dir.medium == "Memory"
    assert all(volume.host_path is None and volume.persistent_volume_claim is None and volume.csi is None
               and volume.secret is None and volume.projected is None for volume in volumes.values())
    assert pod.spec.security_context.fs_group == pod.spec.security_context.run_as_group == 1000
    assert not pod.spec.init_containers


@pytest.mark.parametrize("value", [None, "", 1, True, "sbx_bad", SID.upper(), SID + "/x"])
def test_invalid_sandbox_identity_is_rejected(value):
    """Malformed public IDs cannot become resource names or ownership labels."""
    with pytest.raises(SandboxNotFound):
        build(sandbox_spec(sandbox_id=value))


@pytest.mark.parametrize("value", [None, "", 1, True, "txn_bad", TXN.upper(), TXN + "x"])
def test_missing_or_invalid_transaction_is_not_regenerated(value):
    """A missing or malformed supplied transaction fails without inventing ownership."""
    with pytest.raises(ValueError):
        build(sandbox_spec(transaction_id=value))


@pytest.mark.parametrize("field,value", [
    ("created_at", None), ("expires_at", None),
    ("created_at", datetime(2026, 10, 19)),
    ("created_at", CREATED.astimezone(timezone(timedelta(hours=1)))),
    ("expires_at", CREATED - timedelta(microseconds=1)),
    ("created_at", "2026-10-19T08:00:00Z"),
])
def test_forged_or_incomplete_times_are_revalidated(field, value):
    """The builder reuses core UTC validation even if an otherwise frozen input was forged."""
    spec = sandbox_spec()
    object.__setattr__(spec, field, value)
    with pytest.raises(ValueError):
        build(spec)


def test_legacy_missing_time_pair_is_not_silently_filled():
    """Legacy absence accepted by SandboxSpec is insufficient for a new Kubernetes Pod."""
    spec = sandbox_spec(created_at=None, expires_at=None)
    with pytest.raises(ValueError):
        build(spec)


@pytest.mark.parametrize("field,value", [("cpu_millis", True), ("cpu_millis", 99), ("cpu_millis", 2001),
                                       ("memory_mib", 127), ("memory_mib", 2049), ("memory_mib", 256.0)])
def test_forged_resource_values_cannot_bypass_core_bounds(field, value):
    """Recheck exact integers and existing resource bounds before converting quantities."""
    spec = sandbox_spec()
    object.__setattr__(spec.resources, field, value)
    with pytest.raises(ValueError):
        build(spec)


@pytest.mark.parametrize("environment", ["python", "node", "tools"])
def test_unsupported_environment_is_not_silently_mapped_to_basic(environment):
    """The first Pod contract supports only the basic environment, regardless of trusted image selection."""
    with pytest.raises(KubernetesOperationUnsupported):
        build(sandbox_spec(environment=environment))


def test_independent_workspace_binding_is_not_ignored():
    """Persistent workspace identity cannot be replaced with an empty temporary workspace."""
    binding = WorkspaceBinding(
        "wsp_" + "a" * 32, "wct_" + "b" * 32, "agent-sandbox-wsp_" + "a" * 32,
        "sha256:" + "c" * 64, 1, SID, TXN,
    )
    with pytest.raises(KubernetesOperationUnsupported):
        build(sandbox_spec(workspace=binding, volume_name=binding.volume_name))


def test_egress_policy_is_not_silently_discarded():
    """A requested domain policy must fail until the Kubernetes network boundary is implemented."""
    with pytest.raises(KubernetesOperationUnsupported):
        build(sandbox_spec(network=SandboxNetwork(["example.com"])))


@pytest.mark.parametrize("namespace", [None, "", True, 1, "UPPER", "a/b", "a.b", " name", "x" * 64])
def test_namespace_uses_the_existing_explicit_target_constraints(namespace):
    """Invalid namespaces fail as backend configuration without consulting an ambient context."""
    with pytest.raises(KubernetesConfigurationError):
        build(namespace=namespace)


@pytest.mark.parametrize("image", [None, "", 1, True, " image", "image ", "bad\nimage", "bad\x00image", "镜像", "x" * 513,
                                 "https://registry.invalid/image", "--argument", "image;command", "image?query"])
def test_trusted_image_configuration_is_strict_and_bounded(image):
    """A trusted image argument still requires nonempty bounded ASCII with no whitespace or controls."""
    with pytest.raises(KubernetesConfigurationError):
        build(agent_image=image)


@pytest.mark.parametrize("image", ["registry.example.com:5000/team/agent:v1", "registry.example.com/agent@sha256:" + "a" * 64])
def test_image_is_supplied_only_by_the_trusted_backend_without_resolution(image):
    """The pure builder preserves a supplied tag or digest rather than reading Docker or a registry."""
    assert build(agent_image=image).spec.containers[0].image == image


@pytest.mark.parametrize("spec", [None, {}, object(), "spec"])
def test_non_spec_inputs_fail_without_inventing_defaults(spec):
    """Only a real immutable SandboxSpec may enter the pure builder."""
    from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
    with pytest.raises(ValueError):
        build_pod_spec(spec, namespace=NAMESPACE, agent_image=IMAGE)


def test_credentials_and_host_environment_never_enter_serialized_pod(monkeypatch):
    """Building with an instance token and host secrets cannot inject them into any Pod field."""
    token, admin, config_path = "A" * 43, "test-admin-secret", "/private/test-kubeconfig"
    monkeypatch.setenv("AGENT_SANDBOX_API_KEY", admin)
    monkeypatch.setenv("KUBECONFIG", config_path)
    monkeypatch.setenv("HTTP_PROXY", "http://private-proxy.invalid")
    pod = build(sandbox_spec(access_token=token))
    encoded = json.dumps(pod.to_dict(serialize=True))
    assert all(value not in encoded for value in (token, admin, config_path, "private-proxy.invalid",
                                                "AGENT_SANDBOX_API_KEY", "KUBECONFIG", "access_token"))
    assert pod.spec.containers[0].env in (None, [])


def test_builder_does_not_read_files_environment_or_open_any_client(monkeypatch):
    """Guard actual I/O entry points, including ambient environment reads and random ID generation."""
    from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
    spec = sandbox_spec()

    def forbidden(*args, **kwargs):
        """Fail any accidental I/O or identity allocation inside a pure specification build."""
        raise AssertionError("specification builder performed external access")

    with monkeypatch.context() as guard:
        # The official SDK lazily imports API classes. Resolve these test-only
        # guards before blocking environment access during the actual build.
        guard.setattr(client, "ApiClient", forbidden)
        guard.setattr(client, "CoreV1Api", forbidden)
        guard.setattr(builtins, "open", forbidden)
        guard.setattr(Path, "open", forbidden)
        guard.setattr(os, "getenv", forbidden)
        guard.setattr(type(os.environ), "__getitem__", forbidden)
        guard.setattr(type(os.environ), "__iter__", forbidden)
        guard.setattr(socket, "socket", forbidden)
        guard.setattr(config, "load_kube_config", forbidden)
        guard.setattr(sqlite3, "connect", forbidden)
        guard.setattr(secrets, "token_hex", forbidden)
        pod = build_pod_spec(spec, namespace=NAMESPACE, agent_image=IMAGE)
    assert pod.metadata.labels == {**LABELS, NETWORK_LAYOUT_LABEL: NETWORK_LAYOUT_VERSION}


def test_repeated_builds_share_no_mutable_state_or_sdk_objects():
    """Mutating one complete output leaves both a second build and the original frozen input untouched."""
    spec = sandbox_spec()
    original = copy.deepcopy(spec)
    first, second = build(spec), build(spec)
    expected = copy.deepcopy(second.to_dict())
    assert first == second
    first.metadata.labels["test"] = "changed"
    first.metadata.annotations.clear()
    first.spec.containers[0].args.append("changed")
    first.spec.containers[0].resources.limits["cpu"] = "changed"
    first.spec.containers[0].security_context.capabilities.drop.clear()
    first.spec.containers[0].readiness_probe.http_get.path = "/changed"
    first.spec.containers[0].ports[0].container_port = 9999
    first.spec.volumes[0].empty_dir.size_limit = "2Gi"
    first.spec.containers[0].volume_mounts[0].mount_path = "/changed"
    first.spec.security_context.seccomp_profile.type = "Unconfined"
    assert second.to_dict() == expected and build(spec).to_dict() == expected and spec == original


@pytest.mark.anyio
async def test_generated_layout_is_compatible_with_existing_get_and_delete_contracts():
    """After simulated API-assigned facts, existing Runtime query and deletion accept the generated layout."""
    from test_kubernetes_delete import DeleteClient
    pod = build()
    pod.metadata.uid, pod.metadata.resource_version = UID, "100"
    pod.spec.node_name = "test-worker"
    pod.status = primary_pod().status
    result = query_result(pod, SID, NAMESPACE)
    assert result.status == "ready" and result.transaction_id == TXN
    assert result.created_at == CREATED and result.expires_at == EXPIRES
    fake = DeleteClient()
    fake.pod = pod
    await query_runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert fake.pod is None and len(fake.delete_calls) == 1
