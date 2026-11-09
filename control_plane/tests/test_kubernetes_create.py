"""Offline single-Pod creation, pinned rollback and deterministic cancellation races."""

import asyncio
import copy
import threading

import pytest
from kubernetes import client

from control_plane.core.errors import (
    CreationResourceState, KubernetesConfigurationError, KubernetesOperationUnsupported,
    SandboxCleanupFailed, SandboxCreateFailed,
)
from control_plane.core.models import SandboxNetwork
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from test_kubernetes_delete import DeleteClient, deletion_clock
from test_kubernetes_pod_spec import IMAGE, sandbox_spec
from test_kubernetes_query import CREATED, NAMESPACE, SID, TXN, UID, primary_pod


class CreateClient(DeleteClient):
    """Keep accepted server objects separate from POST responses, failures and later observations."""

    def __init__(self):
        """Start with an empty namespace and optional staged startup, commit and worker barriers."""
        super().__init__()
        self.pod = None
        self.create_calls = []
        self.commit = True
        self.create_error = None
        self.response_fault = None
        self.startup = "running"
        self.pending_reads = self.missing_reads = 0
        self.create_entered = self.create_release = None
        self.start_entered = self.start_release = None
        self.close_entered = self.close_release = None

    def create_namespaced_pod(self, namespace, **kwargs):
        """Accept at most one request, optionally commit it and then lose or corrupt only the response."""
        self.create_calls.append((namespace, kwargs))
        self.events.append(("create", threading.get_ident()))
        if self.create_entered is not None:
            self.create_entered.set()
        if self.create_release is not None and not self.create_release.wait(3):
            raise AssertionError("test did not release POST")
        if self.commit:
            self.pod = copy.deepcopy(kwargs["body"])
            self.pod.metadata.uid, self.pod.metadata.resource_version = UID, "100"
            self.pod.spec.node_name = "test-worker"
            self.pod.status = primary_pod().status
        if self.create_error is not None:
            raise self.create_error
        response = copy.deepcopy(self.pod)
        if self.response_fault == "transaction":
            response.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
        elif self.response_fault == "uid":
            response.metadata.uid = "invalid"
        elif self.response_fault == "command":
            response.spec.containers[0].command = ["/unexpected"]
        return response

    def read_namespaced_pod(self, name, namespace, **kwargs):
        """Simulate delayed visibility, startup transitions, replacement and blocking reads."""
        if self.create_calls and not self.delete_calls:
            if self.start_entered is not None:
                self.start_entered.set()
            if self.start_release is not None and not self.start_release.wait(3):
                raise AssertionError("test did not release startup read")
            if self.missing_reads:
                self.missing_reads -= 1
                raise client.ApiException(status=404)
            if self.pod is not None:
                if self.pending_reads or self.startup == "pending":
                    self.pending_reads = max(0, self.pending_reads - 1)
                    pod = copy.deepcopy(self.pod)
                    pod.status = client.V1PodStatus(phase="Pending")
                    return pod
                if self.startup == "failed":
                    self.pod.status.phase = "Failed"
                elif self.startup == "exited":
                    self.pod.status.container_statuses[0].state = client.V1ContainerState(
                        terminated=client.V1ContainerStateTerminated(exit_code=0, finished_at=CREATED),
                    )
                elif self.startup == "image":
                    self.pod.status.phase = "Pending"
                    self.pod.status.container_statuses[0].state = client.V1ContainerState(
                        waiting=client.V1ContainerStateWaiting(reason="ErrImageNeverPull"),
                    )
                elif self.startup == "deleting":
                    self.pod.metadata.deletion_timestamp = CREATED
                elif self.startup == "replacement":
                    self.pod.metadata.uid = "ce667ab6-3f02-40ba-9b29-b1967503a3f0"
                elif self.startup == "not-ready":
                    self.pod.status.conditions[0].status = "False"
                    self.pod.status.container_statuses[0].ready = False
        return super().read_namespaced_pod(name, namespace, **kwargs)

    def close(self):
        """Expose the late-success cancellation window while still owning the original client."""
        if self.close_entered is not None:
            self.close_entered.set()
        if self.close_release is not None and not self.close_release.wait(3):
            raise AssertionError("test did not release client close")
        super().close()


def runtime(fake, **changes):
    """Assemble an inert backend whose configured client factory stays entirely offline."""
    options = dict(kubeconfig_path="/never-read", context="explicit-test", namespace=NAMESPACE,
                   client_factory=lambda **_: fake)
    options.update(changes)
    return KubernetesSandboxRuntime(**options)


@pytest.fixture
def creation_clock(monkeypatch, deletion_clock):
    """Use one fake monotonic clock for startup, reconciliation and independent deletion budgets."""
    from control_plane.runtime.kubernetes import pod_create
    from control_plane.runtime.kubernetes import network_policy
    clock = deletion_clock

    def now():
        """Share deterministic time without patching the event loop's clock."""
        return clock.now

    def wait(seconds):
        """Advance only the synchronous worker's bounded polling budget."""
        clock.waits.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(pod_create, "monotonic", now)
    monkeypatch.setattr(pod_create, "sleep", wait)
    monkeypatch.setattr(pod_create, "START_TIMEOUT_SECONDS", 0.4)
    monkeypatch.setattr(pod_create, "RECONCILE_TIMEOUT_SECONDS", 0.3)
    monkeypatch.setattr(network_policy, "monotonic", now)
    monkeypatch.setattr(network_policy, "sleep", wait)
    monkeypatch.setattr(network_policy, "POLICY_TIMEOUT_SECONDS", 0.3)
    return clock


@pytest.mark.anyio
@pytest.mark.parametrize("startup", ["running", "not-ready"])
async def test_create_posts_once_then_accepts_running_without_waiting_ready(startup, creation_clock):
    """Creation success requires real running evidence but deliberately permits readiness=False."""
    fake = CreateClient()
    fake.startup, fake.pending_reads = startup, 2
    assert await runtime(fake).create(sandbox_spec()) is None
    assert len(fake.create_calls) == 1 and fake.pod is not None and not fake.delete_calls
    namespace, request = fake.create_calls[0]
    assert namespace == NAMESPACE and "dry_run" not in request
    assert request["body"].spec.containers[0].image == IMAGE
    assert request["body"].spec.containers[0].image_pull_policy == "Never"
    assert creation_clock.waits and fake.closed.is_set()
    assert len({thread for _, thread in fake.events}) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("spec", [sandbox_spec(environment="python"), sandbox_spec(network=SandboxNetwork(["example.com"]))])
async def test_unsupported_specs_never_create_a_client(spec):
    """Unsupported features fail before acquisition and before any cluster observation."""
    def forbidden(**_):
        """Detect side effects before specification validation."""
        pytest.fail("invalid spec acquired a client")

    with pytest.raises(KubernetesOperationUnsupported):
        await runtime(None, client_factory=forbidden).create(spec)


@pytest.mark.parametrize("image", [None, "", True, " bad", "bad\nimage"])
def test_agent_image_configuration_is_validated_during_inert_assembly(image):
    """Trusted image configuration is checked without accessing its existence or kubeconfig."""
    with pytest.raises(KubernetesConfigurationError):
        runtime(None, agent_image=image)


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["owned", "foreign", "duplicate", "paged"])
async def test_preflight_conflicts_never_post_or_delete(fault):
    """Any existing deterministic target or ambiguous inventory remains untouched."""
    fake = CreateClient()
    fake.pod = primary_pod()
    if fault == "foreign":
        fake.inventory = []
        fake.pod.metadata.labels["io.agent-sandbox.project"] = "other"
    elif fault == "duplicate":
        fake.inventory = [fake.pod, copy.deepcopy(fake.pod)]
    elif fault == "paged":
        fake.continuation = "another-page"
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNCHANGED
    assert not fake.create_calls and not fake.delete_calls and fake.pod is not None


@pytest.mark.anyio
@pytest.mark.parametrize("status", [400, 401, 403, 404, 409, 422, 429])
async def test_explicit_pod_rejection_cleans_policy_without_adopting_a_pod(status):
    """A rejected Pod POST still requires cleanup of the already-created NetworkPolicy."""
    fake = CreateClient()
    fake.commit = False
    fake.create_error = client.ApiException(status=status, reason="private response")
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    assert len(fake.create_calls) == 1 and not fake.delete_calls and fake.networking.policy is None
    assert "private" not in str(captured.value)


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["transaction", "uid", "command"])
async def test_invalid_post_response_cannot_succeed_but_can_clean_verified_original(fault, creation_clock):
    """Corrupt response data causes failure; only fresh matching ownership can authorize rollback."""
    fake = CreateClient()
    fake.response_fault = fault
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    assert fake.pod is None and len(fake.create_calls) == len(fake.delete_calls) == 1
    assert fake.delete_calls[0][2]["body"].preconditions.uid == UID


@pytest.mark.anyio
@pytest.mark.parametrize("startup", ["failed", "exited", "image", "pending", "deleting"])
async def test_failed_or_timed_out_startup_cleans_original_uid(startup, creation_clock):
    """Failed, unusable and timed-out starts finish precise cleanup before reporting rolled_back."""
    fake = CreateClient()
    fake.startup = startup
    if startup == "deleting":
        # An independent delete can already be in progress; bounded confirmation
        # may fail instead of sending DELETE a second time.
        fake.remove_on_delete = False
    expected = SandboxCleanupFailed if startup == "deleting" else SandboxCreateFailed
    with pytest.raises(expected) as captured:
        await runtime(fake).create(sandbox_spec())
    state = captured.value.creation_failure.resource_state
    assert state is (CreationResourceState.REMAINING if startup == "deleting" else CreationResourceState.ROLLED_BACK)
    assert len(fake.create_calls) == 1
    if startup != "deleting":
        assert fake.pod is None and fake.delete_calls[0][2]["body"].preconditions.uid == UID


@pytest.mark.anyio
async def test_replaced_uid_is_never_rolled_back(creation_clock):
    """The original POST UID remains pinned even when a replacement preserves every other label."""
    fake = CreateClient()
    fake.startup = "replacement"
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert not fake.delete_calls and fake.pod.metadata.uid != UID


@pytest.mark.anyio
@pytest.mark.parametrize("committed", [False, True])
async def test_lost_post_response_never_reposts_or_infers_absence_from_404(committed, creation_clock):
    """Repeated temporary absence cannot prove unchanged after an ambiguous POST result."""
    fake = CreateClient()
    fake.commit = committed
    fake.create_error = TimeoutError("private API URL")
    fake.missing_reads = 2
    expected = SandboxCreateFailed if committed else SandboxCleanupFailed
    with pytest.raises(expected) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is (
        CreationResourceState.ROLLED_BACK if committed else CreationResourceState.UNKNOWN)
    assert len(fake.create_calls) == 1 and fake.closed.is_set()
    assert len(fake.delete_calls) == int(committed) and "private" not in str(captured.value)


@pytest.mark.anyio
async def test_cleanup_failure_keeps_remaining_and_close_cannot_overwrite_it(creation_clock):
    """A stuck owned Pod stays remaining even if client close also fails."""
    fake = CreateClient()
    fake.startup, fake.remove_on_delete = "failed", False
    fake.close_error = RuntimeError("private close")
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert fake.pod is not None and len(fake.delete_calls) == 1


@pytest.mark.anyio
async def test_close_failure_after_success_reports_existing_resource():
    """A close-only failure cannot be converted into a connection error claiming no creation happened."""
    fake = CreateClient()
    fake.close_error = RuntimeError("private close")
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert fake.pod is not None and "private" not in str(captured.value)


@pytest.mark.anyio
async def test_api_default_omissions_and_quantity_normalization_do_not_change_the_spec():
    """API omitempty false fields retain their documented defaults without weakening explicit deny settings."""
    fake = CreateClient()
    original = fake.create_namespaced_pod

    def accepted_defaults(*args, **kwargs):
        """Simulate the API response omitting default-false fields and normalizing millicores."""
        response = original(*args, **kwargs)
        for pod in (response, fake.pod):
            pod.spec.host_network = pod.spec.host_pid = pod.spec.host_ipc = None
            container = pod.spec.containers[0]
            container.security_context.privileged = None
            container.resources.requests["cpu"] = container.resources.limits["cpu"] = "1"
            for mount in container.volume_mounts:
                mount.read_only = None
        return response

    fake.create_namespaced_pod = accepted_defaults
    await runtime(fake).create(sandbox_spec())
    assert fake.pod is not None and not fake.delete_calls


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["post", "startup", "close", "rollback"])
@pytest.mark.parametrize("repeat", [False, True])
async def test_cancellation_waits_for_inflight_work_and_pinned_rollback(stage, repeat, creation_clock):
    """Every cancellation window retains thread ownership until the original UID has been cleaned."""
    fake = CreateClient()
    entered, release = threading.Event(), threading.Event()
    if stage == "post":
        fake.create_entered, fake.create_release = entered, release
    elif stage == "startup":
        fake.start_entered, fake.start_release = entered, release
    elif stage == "close":
        fake.close_entered, fake.close_release = entered, release
    else:
        fake.startup = "failed"
        fake.delete_entered, fake.delete_release = entered, release
    operation = asyncio.create_task(runtime(fake).create(sandbox_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        operation.cancel()
        await asyncio.sleep(0)
        if repeat:
            operation.cancel()
            await asyncio.sleep(0)
        assert not operation.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert fake.pod is None and fake.closed.is_set()
    assert len(fake.create_calls) == len(fake.delete_calls) == 1
    assert fake.delete_calls[0][2]["body"].preconditions.uid == UID


@pytest.mark.anyio
@pytest.mark.parametrize("uncertain", [False, True])
async def test_cancellation_with_unsafe_cleanup_returns_domain_failure(uncertain, creation_clock):
    """Cancellation never hides a remaining object or an unobservable POST outcome."""
    fake = CreateClient()
    entered, release = threading.Event(), threading.Event()
    fake.create_entered, fake.create_release = entered, release
    fake.remove_on_delete = False
    if uncertain:
        fake.commit = False
        fake.create_error = TimeoutError("private response")
    operation = asyncio.create_task(runtime(fake).create(sandbox_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        operation.cancel()
        await asyncio.sleep(0)
        operation.cancel()
    finally:
        release.set()
    with pytest.raises(SandboxCleanupFailed) as captured:
        await operation
    assert captured.value.creation_failure.resource_state is (
        CreationResourceState.UNKNOWN if uncertain else CreationResourceState.REMAINING)


@pytest.mark.anyio
async def test_cancellation_during_preflight_does_not_submit():
    """An in-flight read settles and closes before cancellation returns, without a subsequent POST."""
    fake = CreateClient()
    entered, release = threading.Event(), threading.Event()
    original = fake.read_namespace

    def blocked_namespace(*args, **kwargs):
        """Hold the first preflight read until the async caller has requested cancellation."""
        entered.set()
        assert release.wait(3)
        return original(*args, **kwargs)

    fake.read_namespace = blocked_namespace
    operation = asyncio.create_task(runtime(fake).create(sandbox_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        operation.cancel()
        await asyncio.sleep(0)
        operation.cancel()
        assert not operation.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert not fake.create_calls and not fake.delete_calls and fake.closed.is_set()


@pytest.mark.anyio
async def test_cancel_after_worker_has_fully_completed_compensates_its_receipt(monkeypatch):
    """A completed worker still yields its original UID when cancellation wins async result delivery."""
    from control_plane.runtime.kubernetes import kubernetes_runtime as runtime_module
    original_wait = runtime_module.wait_for_task

    async def cancel_at_delivery(worker):
        """Deliver cancellation immediately after the actual worker and client have completed."""
        result = await original_wait(worker)
        asyncio.current_task().cancel()
        await asyncio.sleep(0)
        return result

    monkeypatch.setattr(runtime_module, "wait_for_task", cancel_at_delivery)
    fake = CreateClient()
    with pytest.raises(asyncio.CancelledError):
        await runtime(fake).create(sandbox_spec())
    assert fake.pod is None and len(fake.delete_calls) == 1
    assert fake.delete_calls[0][2]["body"].preconditions.uid == UID


@pytest.mark.anyio
async def test_transient_404_after_create_response_does_not_mean_startup_success(creation_clock):
    """Known-UID startup tolerates transient reads but returns only after running evidence arrives."""
    fake = CreateClient()
    fake.missing_reads = 2
    await runtime(fake).create(sandbox_spec())
    assert len(creation_clock.waits) >= 2 and fake.pod is not None and not fake.delete_calls


@pytest.mark.anyio
async def test_ambiguous_post_and_unavailable_confirmation_remain_unknown(creation_clock):
    """When no reliable read is possible after POST, neither unchanged nor rolled_back may be invented."""
    fake = CreateClient()
    original = fake.create_namespaced_pod

    def lose_response_and_reads(*args, **kwargs):
        """Commit the Pod, then make the response and every confirmation unavailable."""
        original(*args, **kwargs)
        fake.list_error = fake.read_error = ConnectionError("private API URL")
        raise TimeoutError("private POST response")

    fake.create_namespaced_pod = lose_response_and_reads
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert fake.pod is not None and not fake.delete_calls and len(fake.create_calls) == 1


@pytest.mark.anyio
async def test_lost_response_reconciliation_stops_after_observing_an_identity_conflict(creation_clock):
    """A foreign observation cannot be forgotten and replaced by adopting a later matching object."""
    fake = CreateClient()
    fake.create_error = TimeoutError()
    original = fake.read_namespaced_pod
    observations = []

    def foreign_first(*args, **kwargs):
        """Expose one conflicting transaction before subsequent reads would match the request."""
        result = original(*args, **kwargs)
        if fake.create_calls and not observations:
            result.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
            observations.append("conflict")
        return result

    fake.read_namespaced_pod = foreign_first
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert observations == ["conflict"] and fake.pod is not None and not fake.delete_calls


@pytest.mark.anyio
async def test_post_409_does_not_delete_the_racing_conflict_object():
    """A new object inserted after empty preflight is never adopted merely because POST returned conflict."""
    fake = CreateClient()

    def racing_conflict(namespace, **kwargs):
        """Expose another transaction at the deterministic name while rejecting this POST."""
        fake.create_calls.append((namespace, kwargs))
        fake.pod = primary_pod()
        fake.pod.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
        raise client.ApiException(status=409)

    fake.create_namespaced_pod = racing_conflict
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert fake.pod is not None and not fake.delete_calls and fake.networking.policy is not None


@pytest.mark.anyio
@pytest.mark.parametrize("field", ["token-mount", "service-env", "escalation", "readonly-root"])
async def test_omitted_explicit_security_denials_are_not_treated_as_safe_defaults(field):
    """Only documented default-false fields may be omitted; required restrictions cannot disappear."""
    fake = CreateClient()
    original = fake.create_namespaced_pod

    def unsafe_response(*args, **kwargs):
        """Corrupt one explicit safety setting without changing committed ownership."""
        response = original(*args, **kwargs)
        if field == "token-mount":
            response.spec.automount_service_account_token = None
        elif field == "service-env":
            response.spec.enable_service_links = None
        elif field == "escalation":
            response.spec.containers[0].security_context.allow_privilege_escalation = None
        else:
            response.spec.containers[0].security_context.read_only_root_filesystem = None
        return response

    fake.create_namespaced_pod = unsafe_response
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    assert fake.pod is None
