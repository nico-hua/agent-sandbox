"""Pure construction of the constrained basic-environment, temporary-workspace Agent Pod."""

import re
from dataclasses import replace

from kubernetes import client

from control_plane.core.errors import KubernetesConfigurationError, KubernetesOperationUnsupported
from control_plane.core.models import SandboxSpec, format_utc_datetime
from control_plane.runtime.kubernetes.pod_query import (
    AGENT_CONTAINER_NAME, LABEL_PREFIX, NETWORK_LAYOUT_LABEL, NETWORK_LAYOUT_VERSION, sandbox_pod_name,
)

_SANDBOX_UID = 1000
_SANDBOX_GID = 1000
_WORKSPACE_SIZE = "1Gi"
_TMP_SIZE = "64Mi"


def _validate_backend_configuration(namespace: str, agent_image: str) -> None:
    """Require an explicit namespace and bounded trusted image text without resolving either target."""
    if (
        type(namespace) is not str
        or re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", namespace) is None
        or type(agent_image) is not str or not 1 <= len(agent_image) <= 512
        or not agent_image.isascii()
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/:@-]*", agent_image) is None
        or "://" in agent_image
    ):
        raise KubernetesConfigurationError()


def build_pod_spec(spec: SandboxSpec, *, namespace: str, agent_image: str) -> client.V1Pod:
    """Build fresh SDK objects from explicit identity and trusted image configuration, with no external I/O."""
    _validate_backend_configuration(namespace, agent_image)
    if type(spec) is not SandboxSpec:
        raise ValueError("spec must be SandboxSpec")
    # Reconstruct with every supplied field: reuse core's strict validation
    # without sampling clocks, invoking default identity factories or mutating input.
    spec = replace(spec)
    name = sandbox_pod_name(spec.sandbox_id)
    if type(spec.transaction_id) is not str or re.fullmatch(r"txn_[0-9a-f]{32}", spec.transaction_id) is None:
        raise ValueError("invalid transaction_id")
    if spec.created_at is None or spec.expires_at is None:
        raise ValueError("Pod specifications require both lifecycle times")
    if spec.environment != "basic" or spec.workspace is not None or spec.network.allow_domains:
        raise KubernetesOperationUnsupported()

    quantities = {"cpu": f"{spec.resources.cpu_millis}m", "memory": f"{spec.resources.memory_mib}Mi"}
    agent = client.V1Container(
        name=AGENT_CONTAINER_NAME, image=agent_image, image_pull_policy="Never",
        command=["/usr/local/bin/agent"], args=["-listen", "0.0.0.0:8080"], working_dir="/workspace",
        ports=[client.V1ContainerPort(name="agent-http", container_port=8080, protocol="TCP")],
        readiness_probe=client.V1Probe(
            http_get=client.V1HTTPGetAction(path="/healthz", port=8080, scheme="HTTP"),
            period_seconds=1, timeout_seconds=1, failure_threshold=3,
        ),
        resources=client.V1ResourceRequirements(requests=dict(quantities), limits=dict(quantities)),
        security_context=client.V1SecurityContext(
            privileged=False, allow_privilege_escalation=False, read_only_root_filesystem=True,
            capabilities=client.V1Capabilities(drop=["ALL"]),
        ),
        volume_mounts=[
            client.V1VolumeMount(name="workspace", mount_path="/workspace", read_only=False),
            client.V1VolumeMount(name="tmp", mount_path="/tmp", read_only=False),
        ],
    )
    return client.V1Pod(
        api_version="v1", kind="Pod",
        metadata=client.V1ObjectMeta(
            name=name, namespace=namespace,
            labels={**spec.labels_for("container"), NETWORK_LAYOUT_LABEL: NETWORK_LAYOUT_VERSION},
            annotations={
                LABEL_PREFIX + "created-at": format_utc_datetime(spec.created_at),
                LABEL_PREFIX + "expires-at": format_utc_datetime(spec.expires_at),
            },
        ),
        spec=client.V1PodSpec(
            containers=[agent], restart_policy="Never", host_network=False, host_pid=False, host_ipc=False,
            automount_service_account_token=False, enable_service_links=False,
            security_context=client.V1PodSecurityContext(
                run_as_non_root=True, run_as_user=_SANDBOX_UID, run_as_group=_SANDBOX_GID,
                # Kubelet supplies volume group permissions; no root initializer
                # or user workload chown is needed for these fresh emptyDir mounts.
                fs_group=_SANDBOX_GID, seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
            ),
            volumes=[
                client.V1Volume(name="workspace", empty_dir=client.V1EmptyDirVolumeSource(size_limit=_WORKSPACE_SIZE)),
                client.V1Volume(name="tmp", empty_dir=client.V1EmptyDirVolumeSource(medium="Memory", size_limit=_TMP_SIZE)),
            ],
        ),
    )
