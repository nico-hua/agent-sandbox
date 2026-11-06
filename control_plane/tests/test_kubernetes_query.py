"""Offline, bounded primary Pod discovery, identity and state observations."""

import asyncio
import copy
import threading
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from kubernetes import client

from control_plane.core.errors import (
    KubernetesConfigurationError, KubernetesConnectionFailed, KubernetesPermissionDenied,
    KubernetesTimeout, SandboxNotFound, SandboxQueryFailed,
)
from control_plane.core.models import SandboxQueryResult
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from test_kubernetes_runtime import FakeClient

SID = "sbx_0123456789abcdef0123456789abcdef"
TXN = "txn_0123456789abcdef0123456789abcdef"
NAMESPACE = "test-sandboxes"
POD_NAME = "agent-sandbox-sbx-0123456789abcdef0123456789abcdef"
UID = "fe667ab6-3f02-40ba-9b29-b1967503a3f0"
CREATED = datetime(2026, 10, 19, 8, 0, 0, 123456, tzinfo=UTC)
EXPIRES = datetime(2026, 10, 19, 8, 30, 0, 123456, tzinfo=UTC)
LABELS = {
    "io.agent-sandbox.managed": "true", "io.agent-sandbox.project": "agent-sandbox",
    "io.agent-sandbox.sandbox-id": SID, "io.agent-sandbox.resource": "container",
    "io.agent-sandbox.transaction-id": TXN,
}


def primary_pod():
    """Construct a realistic single-Agent Pod with independent business and Kubernetes timestamps."""
    return client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=POD_NAME, namespace=NAMESPACE, uid=UID, resource_version="100",
            labels=dict(LABELS), creation_timestamp=datetime(2026, 11, 6, tzinfo=UTC),
            annotations={"io.agent-sandbox.created-at": "2026-10-19T08:00:00.123456Z",
                         "io.agent-sandbox.expires-at": "2026-10-19T08:30:00.123456Z"},
        ),
        spec=client.V1PodSpec(node_name="test-worker", containers=[client.V1Container(
            name="agent", image="agent-sandbox-basic:dev",
            readiness_probe=client.V1Probe(http_get=client.V1HTTPGetAction(path="/healthz", port=8080)),
        )]),
        status=client.V1PodStatus(phase="Running", conditions=[client.V1PodCondition(type="Ready", status="True")],
            container_statuses=[client.V1ContainerStatus(name="agent", image="agent-sandbox-basic:dev",
                image_id="sha256:test-image", ready=True, restart_count=0,
                state=client.V1ContainerState(running=client.V1ContainerStateRunning(started_at=CREATED)))]),
    )


class QueryClient(FakeClient):
    """Record only allowed list/read calls and preserve separate discovery and fresh observations."""

    def __init__(self, pod=None):
        """Prepare a valid Pod and optional query, paging, race and transport faults."""
        super().__init__()
        self.pod = primary_pod() if pod is None else pod
        self.items = [copy.deepcopy(self.pod)]
        self.continuation = None
        self.list_error = None
        self.read_error = None
        self.read_calls = []

    def list_namespaced_pod(self, namespace, **kwargs):
        """Return a bounded discovery snapshot; no default test consults a real cluster."""
        self.calls.append((namespace, kwargs))
        self.events.append(("list", threading.get_ident()))
        if self.list_error is not None:
            raise self.list_error
        return client.V1PodList(items=self.items, metadata=client.V1ListMeta(_continue=self.continuation))

    def read_namespaced_pod(self, name, namespace, **kwargs):
        """Return the exact Pod observation after controlled blocking or disappearance."""
        self.read_calls.append((name, namespace, kwargs))
        self.events.append(("read", threading.get_ident()))
        if self.entered is not None:
            self.entered.set()
        if self.release is not None and not self.release.wait(3):
            raise RuntimeError("test did not release the worker")
        if self.read_error is not None:
            raise self.read_error
        return self.pod


def query_runtime(fake, factory=None):
    """Use an explicitly scoped Runtime and an entirely offline SDK acquisition boundary."""
    return KubernetesSandboxRuntime(
        kubeconfig_path="/never-read", context="explicit-test", namespace=NAMESPACE,
        client_factory=(lambda **_: fake) if factory is None else factory,
    )


@pytest.mark.anyio
async def test_get_reads_one_primary_with_business_times_and_exact_identity():
    """A valid snapshot keeps business time precision and performs only bounded discovery and exact read."""
    fake = QueryClient()
    result = await query_runtime(fake).get(SID)
    assert result == SandboxQueryResult(SID, "ready", "agent_readiness_passed", "Agent readiness check passed",
        created_at=CREATED, expires_at=EXPIRES, transaction_id=TXN)
    assert fake.calls == [(NAMESPACE, {
        "label_selector": "io.agent-sandbox.managed=true,io.agent-sandbox.project=agent-sandbox,"
                          f"io.agent-sandbox.sandbox-id={SID},io.agent-sandbox.resource=container",
        "limit": 2, "watch": False, "_request_timeout": (0.5, 0.5),
    })]
    assert fake.read_calls == [(POD_NAME, NAMESPACE, {"_request_timeout": (0.5, 0.5)})]
    assert [stage for stage, _ in fake.events] == ["list", "read", "close"]
    assert len({thread for _, thread in fake.events}) == 1 and fake.events[0][1] != threading.get_ident()


@pytest.mark.anyio
@pytest.mark.parametrize("sandbox_id", [None, 123, "", "sbx_bad", SID.upper(), SID + "/../x", "sbx_" + "f" * 33])
async def test_invalid_id_is_not_found_before_client_creation(sandbox_id):
    """Reject malformed public IDs before any kubeconfig, SDK or namespace access."""
    def forbidden(**kwargs):
        """Guard the resource-free invalid-ID path."""
        pytest.fail("invalid ID created a client")

    with pytest.raises(SandboxNotFound):
        await query_runtime(None, factory=forbidden).get(sandbox_id)


@pytest.mark.anyio
@pytest.mark.parametrize("scenario,expected", [
    ("empty", SandboxNotFound), ("duplicate", SandboxQueryFailed),
    ("continuation", SandboxQueryFailed), ("empty-continuation", SandboxQueryFailed),
    ("namespace-404", KubernetesConfigurationError), ("pod-404", SandboxNotFound),
    ("uid-changed", SandboxQueryFailed), ("transaction-changed", SandboxQueryFailed),
    ("time-changed", SandboxQueryFailed),
])
async def test_bounded_discovery_and_fresh_read_never_adopt_ambiguous_or_replaced_pods(scenario, expected):
    """Missing namespaces, pagination, duplicates and replacement races remain distinct without extra reads."""
    fake = QueryClient()
    if scenario == "empty":
        fake.items = []
    elif scenario == "duplicate":
        fake.items.append(copy.deepcopy(fake.pod))
    elif scenario in {"continuation", "empty-continuation"}:
        fake.continuation = "next-page"
        if scenario == "empty-continuation":
            fake.items = []
    elif scenario == "namespace-404":
        fake.list_error = client.ApiException(status=404, reason="private namespace")
    elif scenario == "pod-404":
        fake.read_error = client.ApiException(status=404, reason="private Pod")
    elif scenario == "uid-changed":
        fake.pod.metadata.uid = "a121bbbd-f6fd-4f48-bceb-b02c9182f54b"
    elif scenario == "transaction-changed":
        fake.pod.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    elif scenario == "time-changed":
        fake.pod.metadata.annotations["io.agent-sandbox.expires-at"] = "2026-10-19T09:00:00Z"
    fake.close_error = RuntimeError("private cleanup failure")
    with pytest.raises(expected) as captured:
        await query_runtime(fake).get(SID)
    assert "private" not in str(captured.value) and fake.closed.is_set()
    assert len(fake.calls) == 1 and len(fake.read_calls) <= 1


@pytest.mark.anyio
@pytest.mark.parametrize("fault", [
    "labels-missing", "managed", "project", "sandbox-id", "role", "transaction", "namespace",
    "pod-name", "uid", "annotations", "created-missing", "expires-missing", "timestamp-format",
    "timestamp-zone", "timestamp-precision", "timestamp-order", "workspace-label", "workspace-annotation",
    "agent-name", "containers-missing", "extra-container", "init-container", "container-status-name",
])
async def test_identity_metadata_and_target_container_are_required(fault):
    """Malformed managed objects never become legacy instances or acquire invented identities and times."""
    pod = primary_pod()
    if fault == "labels-missing":
        pod.metadata.labels = None
    elif fault in {"managed", "project", "sandbox-id", "role", "transaction"}:
        key = {"role": "resource", "transaction": "transaction-id"}.get(fault, fault)
        pod.metadata.labels["io.agent-sandbox." + key] = "invalid"
    elif fault == "namespace":
        pod.metadata.namespace = "other-namespace"
    elif fault == "pod-name":
        pod.metadata.name = SID
    elif fault == "uid":
        pod.metadata.uid = ""
    elif fault == "annotations":
        pod.metadata.annotations = None
    elif fault in {"created-missing", "expires-missing"}:
        del pod.metadata.annotations["io.agent-sandbox." + fault.split("-")[0] + "-at"]
    elif fault.startswith("timestamp-"):
        pod.metadata.annotations["io.agent-sandbox.expires-at"] = {
            "timestamp-format": "2026-10-19 08:30:00Z", "timestamp-zone": "2026-10-19T16:30:00+08:00",
            "timestamp-precision": "2026-10-19T08:30:00.1234567Z", "timestamp-order": "2026-10-19T07:59:59Z",
        }[fault]
    elif fault.startswith("workspace-"):
        markers = pod.metadata.labels if fault == "workspace-label" else pod.metadata.annotations
        markers["io.agent-sandbox.workspace-id"] = "wsp_" + "a" * 32
    elif fault == "agent-name":
        pod.spec.containers[0].name = "other"
    elif fault == "containers-missing":
        pod.spec.containers = []
    elif fault == "extra-container":
        pod.spec.containers.append(client.V1Container(name="sidecar"))
    elif fault == "init-container":
        pod.spec.init_containers = [client.V1Container(name="init")]
    elif fault == "container-status-name":
        pod.status.container_statuses[0].name = "other"
    fake = QueryClient(pod)
    with pytest.raises(SandboxQueryFailed):
        await query_runtime(fake).get(SID)
    assert fake.closed.is_set()


def pod_in_state(scenario):
    """Construct independently specified Pod/container facts for each expected state."""
    pod = primary_pod()
    agent = pod.status.container_statuses[0]
    ready = pod.status.conditions[0]
    if scenario == "unscheduled":
        pod.spec.node_name = None
        pod.status.phase, pod.status.container_statuses, pod.status.conditions = "Pending", None, None
    elif scenario.startswith("waiting-"):
        reason = scenario.removeprefix("waiting-")
        agent.state = client.V1ContainerState(waiting=client.V1ContainerStateWaiting(reason=reason))
        agent.ready, ready.status, pod.status.phase = False, "False", "Pending"
    elif scenario == "running":
        agent.ready, ready.status = False, "False"
    elif scenario == "no-probe":
        pod.spec.containers[0].readiness_probe = None
    elif scenario == "pod-not-ready":
        ready.status = "False"
    elif scenario == "deleting":
        pod.metadata.deletion_timestamp = CREATED
    elif scenario.startswith("terminated-"):
        code = 7 if scenario == "terminated-nonzero" else 137 if scenario == "terminated-oom" else 0
        reason = "OOMKilled" if scenario == "terminated-oom" else "Error" if code else "Completed"
        agent.state = client.V1ContainerState(terminated=client.V1ContainerStateTerminated(
            exit_code=code, reason=reason, started_at=CREATED, finished_at=EXPIRES))
        agent.ready, ready.status, pod.status.phase = False, "False", "Failed" if code else "Succeeded"
    elif scenario == "pod-failed":
        pod.status.phase, pod.status.container_statuses = "Failed", None
        ready.status = "False"
    return pod


@pytest.mark.anyio
@pytest.mark.parametrize("scenario,status,reason", [
    ("unscheduled", "starting", "pod_unscheduled"),
    ("waiting-ContainerCreating", "starting", "agent_container_creating"),
    ("waiting-PodInitializing", "starting", "agent_initializing"),
    ("waiting-ErrImagePull", "failed", "agent_image_pull_failed"),
    ("waiting-ImagePullBackOff", "failed", "agent_image_pull_backoff"),
    ("waiting-ErrImageNeverPull", "failed", "agent_image_unavailable"),
    ("waiting-InvalidImageName", "failed", "agent_invalid_image"),
    ("waiting-CreateContainerConfigError", "failed", "agent_config_error"),
    ("waiting-CreateContainerError", "failed", "agent_create_error"),
    ("waiting-RunContainerError", "failed", "agent_run_error"),
    ("waiting-CrashLoopBackOff", "failed", "agent_crash_loop"),
    ("running", "running", "agent_not_ready"), ("no-probe", "running", "agent_readiness_probe_missing"),
    ("pod-not-ready", "running", "pod_not_ready"), ("ready", "ready", "agent_readiness_passed"),
    ("deleting", "running", "pod_deleting"), ("terminated-zero", "stopped", "agent_completed"),
    ("terminated-nonzero", "failed", "agent_exited_nonzero"), ("terminated-oom", "failed", "agent_oom_killed"),
    ("pod-failed", "failed", "pod_failed"),
])
async def test_explicit_state_mapping_and_fixed_reasons(scenario, status, reason):
    """Only matching readiness evidence grants ready; terminal and known waiting facts have stable reasons."""
    pod = pod_in_state(scenario)
    pod.status.message = "private SDK diagnostic"
    if pod.status.container_statuses and pod.status.container_statuses[0].state.waiting:
        pod.status.container_statuses[0].state.waiting.message = "private pull credentials"
    fake = QueryClient(pod)
    before = copy.deepcopy(pod.to_dict())
    result = await query_runtime(fake).get(SID)
    assert (result.status, result.reason) == (status, reason)
    assert "private" not in result.message and result.status != "paused"
    assert result.created_at == CREATED and result.expires_at == EXPIRES
    assert pod.to_dict() == before


@pytest.mark.anyio
@pytest.mark.parametrize("fault", [
    "unknown-phase", "missing-state", "multiple-states", "missing-status", "duplicate-status",
    "ready-contradiction", "unknown-ready", "missing-ready", "succeeded-running", "failed-running",
    "bad-probe-port", "bad-probe-path", "bad-probe-host", "bad-probe-exec", "unknown-waiting",
    "succeeded-nonzero", "running-zero-exit", "negative-exit", "bad-running-time",
])
async def test_unknown_or_contradictory_facts_fail_closed(fault):
    """Malformed status and readiness evidence must never be guessed healthy or silently stopped."""
    pod = primary_pod()
    agent = pod.status.container_statuses[0]
    if fault == "unknown-phase":
        pod.status.phase = "Unknown"
    elif fault == "missing-state":
        agent.state = client.V1ContainerState()
    elif fault == "multiple-states":
        agent.state.waiting = client.V1ContainerStateWaiting(reason="ContainerCreating")
    elif fault == "missing-status":
        pod.status.container_statuses = None
    elif fault == "duplicate-status":
        pod.status.container_statuses.append(copy.deepcopy(agent))
    elif fault == "ready-contradiction":
        agent.ready = False
    elif fault == "unknown-ready":
        pod.status.conditions[0].status = "Unknown"
    elif fault == "missing-ready":
        pod.status.conditions = None
    elif fault in {"succeeded-running", "failed-running"}:
        pod.status.phase = "Succeeded" if fault.startswith("succeeded") else "Failed"
    elif fault.startswith("bad-probe-"):
        probe = pod.spec.containers[0].readiness_probe
        if fault == "bad-probe-port":
            probe.http_get.port = 9000
        elif fault == "bad-probe-path":
            probe.http_get.path = "/other"
        elif fault == "bad-probe-host":
            probe.http_get.host = "other-pod"
        else:
            probe.http_get = None
            probe._exec = client.V1ExecAction(command=["true"])
    elif fault == "unknown-waiting":
        pod = pod_in_state("waiting-UnrecognizedReason")
    elif fault in {"succeeded-nonzero", "running-zero-exit", "negative-exit"}:
        pod = pod_in_state("terminated-nonzero" if fault != "running-zero-exit" else "terminated-zero")
        if fault == "negative-exit":
            pod.status.container_statuses[0].state.terminated.exit_code = -1
        else:
            pod.status.phase = "Succeeded" if fault == "succeeded-nonzero" else "Running"
    elif fault == "bad-running-time":
        agent.state.running.started_at = None
    with pytest.raises(SandboxQueryFailed):
        await query_runtime(QueryClient(pod)).get(SID)


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["list", "read", "close"])
@pytest.mark.parametrize("kind,expected", [("permission", KubernetesPermissionDenied), ("timeout", KubernetesTimeout), ("network", KubernetesConnectionFailed)])
async def test_transport_errors_remain_typed_and_close_does_not_mask_them(stage, kind, expected):
    """Discovery and read failures preserve backend categories rather than looking like absent Pods."""
    fake = QueryClient()
    error = {"permission": client.ApiException(status=403, reason="private credentials"),
             "timeout": TimeoutError("private API URL"), "network": ConnectionError("private socket")}[kind]
    if stage == "close":
        fake.close_error = error
        expected = KubernetesConnectionFailed
    else:
        setattr(fake, stage + "_error", error)
        fake.close_error = RuntimeError("private closing error")
    with pytest.raises(expected) as captured:
        await query_runtime(fake).get(SID)
    assert "private" not in str(captured.value) and fake.closed.is_set()


@pytest.mark.anyio
async def test_query_repeated_cancellation_waits_for_read_and_close():
    """A canceled query retains ownership until the exact Pod read and worker cleanup finish."""
    fake = QueryClient()
    fake.entered, fake.release = threading.Event(), threading.Event()
    operation = asyncio.create_task(query_runtime(fake).get(SID))
    try:
        assert await asyncio.to_thread(fake.entered.wait, 1)
        operation.cancel()
        await asyncio.sleep(0)
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done() and not fake.closed.is_set()
    finally:
        fake.release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert fake.closed.is_set() and len({thread for _, thread in fake.events}) == 1
