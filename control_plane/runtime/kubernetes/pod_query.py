"""Pure validation of managed primary Pod identity and conservative Agent state observations."""

import re
from datetime import datetime
from uuid import UUID

from control_plane.core.errors import SandboxNotFound, SandboxQueryFailed
from control_plane.core.models import SandboxQueryResult

AGENT_CONTAINER_NAME = "agent"
LABEL_PREFIX = "io.agent-sandbox."
_UTC_TIMESTAMP = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,6})?(?:Z|\+00:00)"
)
_WAITING_STATES = {
    "ContainerCreating": ("starting", "agent_container_creating", "Agent container is being created"),
    "PodInitializing": ("starting", "agent_initializing", "Agent container is initializing"),
    "ErrImagePull": ("failed", "agent_image_pull_failed", "Agent image could not be pulled"),
    "ImagePullBackOff": ("failed", "agent_image_pull_backoff", "Agent image pull is backing off"),
    "ErrImageNeverPull": ("failed", "agent_image_unavailable", "Agent image is unavailable and pulling is disabled"),
    "InvalidImageName": ("failed", "agent_invalid_image", "Agent image name is invalid"),
    "CreateContainerConfigError": ("failed", "agent_config_error", "Agent container configuration is invalid"),
    "CreateContainerError": ("failed", "agent_create_error", "Agent container could not be created"),
    "RunContainerError": ("failed", "agent_run_error", "Agent container could not be started"),
    "CrashLoopBackOff": ("failed", "agent_crash_loop", "Agent container is in a crash loop"),
}


def sandbox_pod_name(sandbox_id: str) -> str:
    """Map a validated public ID to the deterministic DNS-compatible primary Pod name."""
    if type(sandbox_id) is not str or re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
        raise SandboxNotFound()
    return "agent-sandbox-" + sandbox_id.replace("_", "-")


def primary_pod_selector(sandbox_id: str) -> str:
    """Locate all primary candidates without filtering out conflicting transactions or names."""
    sandbox_pod_name(sandbox_id)
    return (
        f"{LABEL_PREFIX}managed=true,{LABEL_PREFIX}project=agent-sandbox,"
        f"{LABEL_PREFIX}sandbox-id={sandbox_id},{LABEL_PREFIX}resource=container"
    )


def pod_identity(pod, sandbox_id: str, namespace: str) -> tuple[str, str, datetime, datetime]:
    """Require complete ownership, exact target identity and original ordered UTC business times."""
    try:
        metadata = pod.metadata
        if metadata.name != sandbox_pod_name(sandbox_id) or metadata.namespace != namespace:
            raise SandboxQueryFailed()
        if type(metadata.uid) is not str or str(UUID(metadata.uid)) != metadata.uid:
            raise SandboxQueryFailed()
        labels, annotations = metadata.labels, metadata.annotations
        if not isinstance(labels, dict) or not isinstance(annotations, dict):
            raise SandboxQueryFailed()
        expected = {"managed": "true", "project": "agent-sandbox", "sandbox-id": sandbox_id, "resource": "container"}
        if any(labels.get(LABEL_PREFIX + key) != value for key, value in expected.items()):
            raise SandboxQueryFailed()
        transaction_id = labels.get(LABEL_PREFIX + "transaction-id")
        if type(transaction_id) is not str or re.fullmatch(r"txn_[0-9a-f]{32}", transaction_id) is None:
            raise SandboxQueryFailed()
        # Binding metadata is not legacy absence. This backend cannot verify its
        # persistent occupancy or storage identity in the current implementation.
        if any(key.startswith(LABEL_PREFIX + "workspace-") for key in (*labels, *annotations)):
            raise SandboxQueryFailed()
        times = []
        for name in ("created-at", "expires-at"):
            value = annotations.get(LABEL_PREFIX + name)
            if type(value) is not str or _UTC_TIMESTAMP.fullmatch(value) is None:
                raise SandboxQueryFailed()
            times.append(datetime.fromisoformat(value))
        created_at, expires_at = times
        if expires_at < created_at:
            raise SandboxQueryFailed()
        return metadata.uid, transaction_id, created_at, expires_at
    except (AttributeError, KeyError, TypeError, ValueError):
        raise SandboxQueryFailed() from None


def _valid_observation_time(value) -> bool:
    """Require SDK observation timestamps to be aware without comparing them to business creation time."""
    return isinstance(value, datetime) and value.utcoffset() is not None


def _agent_probe(container) -> bool:
    """Recognize only an unredirected HTTP health probe to the fixed Agent port and path."""
    probe = container.readiness_probe
    if probe is None:
        return False
    http = probe.http_get
    if (
        http is None or type(http.port) is not int or http.port != 8080 or http.path != "/healthz"
        or http.scheme not in (None, "HTTP") or http.host not in (None, "")
        or http.http_headers not in (None, [])
        or probe._exec is not None or probe.tcp_socket is not None or probe.grpc is not None
    ):
        raise SandboxQueryFailed()
    return True


def _conditions(status) -> dict[str, str]:
    """Require unambiguous condition observations while allowing normal early Pending absence."""
    values = status.conditions
    if values is None:
        return {}
    if not isinstance(values, list):
        raise SandboxQueryFailed()
    result = {}
    for condition in values:
        if (
            type(condition.type) is not str or not condition.type or condition.type in result
            or condition.status not in ("True", "False", "Unknown")
        ):
            raise SandboxQueryFailed()
        result[condition.type] = condition.status
    return result


def _terminated_state(terminated, phase: str, pod_reason: str | None) -> tuple[str, str, str]:
    """Distinguish consistent normal completion from explicit failure and contradictory terminal facts."""
    code = terminated.exit_code
    if type(code) is not int or code < 0 or not _valid_observation_time(terminated.finished_at):
        raise SandboxQueryFailed()
    if terminated.started_at is not None and (
        not _valid_observation_time(terminated.started_at) or terminated.finished_at < terminated.started_at
    ):
        raise SandboxQueryFailed()
    if terminated.signal is not None and (type(terminated.signal) is not int or terminated.signal < 0):
        raise SandboxQueryFailed()
    failed = code != 0 or terminated.reason == "OOMKilled" or bool(terminated.signal)
    if phase == "Pending" or (phase == "Succeeded" and failed):
        raise SandboxQueryFailed()
    if failed:
        if terminated.reason == "OOMKilled":
            return "failed", "agent_oom_killed", "Agent container was terminated by OOM"
        return "failed", "agent_exited_nonzero", "Agent container terminated unsuccessfully"
    if phase == "Succeeded" and terminated.reason in (None, "", "Completed"):
        return "stopped", "agent_completed", "Agent container completed successfully"
    if phase == "Failed" and pod_reason in ("Evicted", "DeadlineExceeded"):
        return "failed", "pod_failed", "Sandbox Pod failed"
    raise SandboxQueryFailed()


def _observed_state(pod) -> tuple[str, str, str]:
    """Map validated single-Agent Pod facts without treating incomplete evidence as healthy."""
    spec, status = pod.spec, pod.status
    if (
        not isinstance(spec.containers, list) or len(spec.containers) != 1
        or spec.containers[0].name != AGENT_CONTAINER_NAME
        or spec.init_containers not in (None, []) or spec.ephemeral_containers not in (None, [])
        or status.init_container_statuses not in (None, []) or status.ephemeral_container_statuses not in (None, [])
        or status.phase not in ("Pending", "Running", "Succeeded", "Failed")
    ):
        raise SandboxQueryFailed()
    has_probe = _agent_probe(spec.containers[0])
    conditions = _conditions(status)
    ready = conditions.get("Ready")
    if ready == "Unknown" or (status.phase == "Running" and ready is None):
        raise SandboxQueryFailed()
    if status.phase != "Running" and ready == "True":
        raise SandboxQueryFailed()
    statuses = status.container_statuses
    if statuses in (None, []):
        if status.phase == "Failed":
            return "failed", "pod_failed", "Sandbox Pod failed"
        if status.phase == "Pending":
            if spec.node_name in (None, "") and conditions.get("PodScheduled") == "True":
                raise SandboxQueryFailed()
            return ("starting", "pod_unscheduled", "Sandbox Pod is not scheduled") if not spec.node_name else (
                "starting", "pod_pending", "Sandbox Pod is waiting for its Agent container")
        raise SandboxQueryFailed()
    if not isinstance(statuses, list) or len(statuses) != 1 or statuses[0].name != AGENT_CONTAINER_NAME:
        raise SandboxQueryFailed()
    agent = statuses[0]
    if (
        type(agent.ready) is not bool or type(agent.restart_count) is not int or agent.restart_count < 0
        or type(spec.node_name) is not str or not spec.node_name
        or (ready == "True" and not agent.ready)
        or conditions.get("PodScheduled") in ("False", "Unknown")
    ):
        raise SandboxQueryFailed()
    state = agent.state
    if sum(value is not None for value in (state.running, state.waiting, state.terminated)) != 1:
        raise SandboxQueryFailed()
    if state.terminated is not None:
        if agent.ready:
            raise SandboxQueryFailed()
        return _terminated_state(state.terminated, status.phase, status.reason)
    if status.phase in ("Succeeded", "Failed"):
        raise SandboxQueryFailed()
    if state.waiting is not None:
        if agent.ready or ready == "True" or state.waiting.reason not in _WAITING_STATES:
            raise SandboxQueryFailed()
        return _WAITING_STATES[state.waiting.reason]
    if status.phase != "Running" or not _valid_observation_time(state.running.started_at):
        raise SandboxQueryFailed()
    if not has_probe:
        return "running", "agent_readiness_probe_missing", "Agent readiness probe is not configured"
    if not agent.ready:
        return "running", "agent_not_ready", "Agent readiness check is not passing"
    if ready != "True":
        return "running", "pod_not_ready", "Sandbox Pod readiness is not passing"
    return "ready", "agent_readiness_passed", "Agent readiness check passed"


def query_result(pod, sandbox_id: str, namespace: str) -> SandboxQueryResult:
    """Build the common result from verified metadata and a conservative read-only observation."""
    try:
        _, transaction_id, created_at, expires_at = pod_identity(pod, sandbox_id, namespace)
        status, reason, message = _observed_state(pod)
        if pod.metadata.deletion_timestamp is not None:
            if not _valid_observation_time(pod.metadata.deletion_timestamp):
                raise SandboxQueryFailed()
            if status != "failed":
                status = "running" if status == "ready" else status
                reason, message = "pod_deleting", "Sandbox Pod is being deleted"
        return SandboxQueryResult(
            sandbox_id, status, reason, message, created_at=created_at,
            expires_at=expires_at, transaction_id=transaction_id,
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        raise SandboxQueryFailed() from None
