"""Pure specifications for a separately managed authenticated proxy experiment, outside Runtime lifecycle."""

import base64
import re
from ipaddress import IPv4Address

from kubernetes import client

from control_plane.core.errors import KubernetesConfigurationError, SandboxQueryFailed
from control_plane.runtime.kubernetes.agent_service import verify_agent_service
from control_plane.runtime.kubernetes.pod_query import (
    LABEL_PREFIX, agent_service_layout, pod_identity, primary_pod_labels, sandbox_pod_name,
)
from control_plane.runtime.kubernetes.pod_spec import _validate_backend_configuration

PROXY_UPSTREAM_VERSION = "1"
PROXY_UPSTREAM_ANNOTATION = LABEL_PREFIX + "proxy-upstream-version"
PROXY_UID = 101
TOKEN_KEY = "access-token"


def proxy_pod_name(sandbox_id: str) -> str:
    """Derive the deterministic proxy name without accepting arbitrary Kubernetes resource names."""
    return sandbox_pod_name(sandbox_id) + "-proxy"


def proxy_secret_name(sandbox_id: str) -> str:
    """Keep the trusted proxy credential separate from the main Pod's identity and environment."""
    return sandbox_pod_name(sandbox_id) + "-access"


def _source_identity(pod) -> tuple[str, str]:
    """Require an observed, owned Agent in the supported Service layout before building trusted companions."""
    try:
        sandbox_id = pod.metadata.labels[LABEL_PREFIX + "sandbox-id"]
        _, transaction, _, _ = pod_identity(pod, sandbox_id, pod.metadata.namespace)
        if not agent_service_layout(pod.metadata) or pod.metadata.deletion_timestamp is not None:
            raise SandboxQueryFailed()
        return sandbox_id, transaction
    except (AttributeError, KeyError, TypeError, ValueError):
        raise SandboxQueryFailed() from None


def _metadata(pod, *, name: str, role: str) -> client.V1ObjectMeta:
    """Copy only canonical ownership and original times, excluding tokens and unrelated annotations."""
    sandbox_id, transaction = _source_identity(pod)
    return client.V1ObjectMeta(
        name=name, namespace=pod.metadata.namespace,
        labels={**primary_pod_labels(sandbox_id, transaction), LABEL_PREFIX + "resource": role},
        annotations={**{LABEL_PREFIX + key: pod.metadata.annotations[LABEL_PREFIX + key]
                       for key in ("created-at", "expires-at")},
                     PROXY_UPSTREAM_ANNOTATION: PROXY_UPSTREAM_VERSION},
    )


def build_proxy_secret(pod, *, access_token: str) -> client.V1Secret:
    """Build one immutable credential Secret; encoding supplies transport syntax rather than encryption."""
    sandbox_id, _ = _source_identity(pod)
    if type(access_token) is not str or re.fullmatch(r"[A-Za-z0-9_-]{43}", access_token) is None:
        raise KubernetesConfigurationError()
    return client.V1Secret(
        api_version="v1", kind="Secret", type="Opaque", immutable=True,
        metadata=_metadata(pod, name=proxy_secret_name(sandbox_id), role="proxy-access"),
        data={TOKEN_KEY: base64.b64encode(access_token.encode("ascii")).decode("ascii")},
    )


def build_proxy_pod(pod, service, *, proxy_image: str) -> client.V1Pod:
    """Point a constrained trusted proxy at a verified IPv4 Service and reference its independent Secret."""
    sandbox_id, _ = _source_identity(pod)
    _validate_backend_configuration(pod.metadata.namespace, proxy_image)
    verify_agent_service(service, pod)
    try:
        address = str(IPv4Address(service.spec.cluster_ip))
        if address != service.spec.cluster_ip:
            raise ValueError()
    except (AttributeError, TypeError, ValueError):
        raise KubernetesConfigurationError() from None
    container = client.V1Container(
        name="proxy", image=proxy_image, image_pull_policy="Never",
        command=["/usr/local/bin/proxy-entrypoint"],
        ports=[client.V1ContainerPort(name="proxy-http", container_port=8080, protocol="TCP")],
        env=[
            client.V1EnvVar(name="SANDBOX_AGENT_UPSTREAM_IPV4", value=address),
            client.V1EnvVar(name="SANDBOX_PROXY_AUTH_VERSION", value="1"),
            client.V1EnvVar(name="SANDBOX_ACCESS_TOKEN", value_from=client.V1EnvVarSource(
                secret_key_ref=client.V1SecretKeySelector(
                    name=proxy_secret_name(sandbox_id), key=TOKEN_KEY, optional=False,
                ),
            )),
        ],
        readiness_probe=client.V1Probe(
            http_get=client.V1HTTPGetAction(path="/healthz", port=8080, scheme="HTTP"),
            period_seconds=1, timeout_seconds=1, failure_threshold=3,
        ),
        resources=client.V1ResourceRequirements(
            requests={"cpu": "100m", "memory": "64Mi"}, limits={"cpu": "100m", "memory": "64Mi"},
        ),
        security_context=client.V1SecurityContext(
            privileged=False, allow_privilege_escalation=False, read_only_root_filesystem=True,
            capabilities=client.V1Capabilities(drop=["ALL"]),
        ),
        volume_mounts=[client.V1VolumeMount(name="tmp", mount_path="/tmp", read_only=False)],
    )
    return client.V1Pod(
        api_version="v1", kind="Pod", metadata=_metadata(pod, name=proxy_pod_name(sandbox_id), role="proxy"),
        spec=client.V1PodSpec(
            containers=[container], restart_policy="Never", host_network=False, host_pid=False, host_ipc=False,
            automount_service_account_token=False, enable_service_links=False,
            security_context=client.V1PodSecurityContext(
                run_as_non_root=True, run_as_user=PROXY_UID, run_as_group=PROXY_UID, fs_group=PROXY_UID,
                seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
            ),
            volumes=[client.V1Volume(name="tmp", empty_dir=client.V1EmptyDirVolumeSource(
                medium="Memory", size_limit="64Mi",
            ))],
        ),
    )


def build_proxy_network_policies(pod) -> tuple[client.V1NetworkPolicy, client.V1NetworkPolicy]:
    """Allow only the same-transaction proxy-to-Agent TCP flow; client ingress remains fixture-owned."""
    sandbox_id, transaction = _source_identity(pod)
    agent_labels = primary_pod_labels(sandbox_id, transaction)
    proxy_labels = {**agent_labels, LABEL_PREFIX + "resource": "proxy"}
    agent_ingress = client.V1NetworkPolicy(
        api_version="networking.k8s.io/v1", kind="NetworkPolicy",
        metadata=_metadata(pod, name=sandbox_pod_name(sandbox_id) + "-proxy-in", role="agent-proxy-ingress"),
        spec=client.V1NetworkPolicySpec(
            pod_selector=client.V1LabelSelector(match_labels=dict(agent_labels)), policy_types=["Ingress"],
            ingress=[client.V1NetworkPolicyIngressRule(
                _from=[client.V1NetworkPolicyPeer(pod_selector=client.V1LabelSelector(match_labels=dict(proxy_labels)))],
                ports=[client.V1NetworkPolicyPort(protocol="TCP", port=8080)],
            )],
        ),
    )
    proxy_isolation = client.V1NetworkPolicy(
        api_version="networking.k8s.io/v1", kind="NetworkPolicy",
        metadata=_metadata(pod, name=sandbox_pod_name(sandbox_id) + "-proxy-deny", role="proxy-isolation"),
        spec=client.V1NetworkPolicySpec(
            pod_selector=client.V1LabelSelector(match_labels=dict(proxy_labels)),
            policy_types=["Ingress", "Egress"], ingress=[],
            egress=[client.V1NetworkPolicyEgressRule(
                to=[client.V1NetworkPolicyPeer(pod_selector=client.V1LabelSelector(match_labels=dict(agent_labels)))],
                ports=[client.V1NetworkPolicyPort(protocol="TCP", port=8080)],
            )],
        ),
    )
    return agent_ingress, proxy_isolation
