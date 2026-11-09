"""Offline deny-all policy contracts and dual-resource lifecycle safety."""

import asyncio
import copy
import threading

import pytest
from kubernetes import client

from control_plane.core.errors import CreationResourceState, SandboxCleanupFailed, SandboxCreateFailed, SandboxDeleteConflict, SandboxQueryFailed
from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
from test_kubernetes_pod_spec import IMAGE, sandbox_spec
from test_kubernetes_query import NAMESPACE, SID, TXN, UID, primary_pod
from test_kubernetes_create import CreateClient, creation_clock, runtime
from test_kubernetes_delete import deletion_clock
from kubernetes_network_fakes import POLICY_UID
from control_plane.runtime.kubernetes.network_policy import build_network_policy, network_policy_name
from control_plane.runtime.kubernetes.pod_query import NETWORK_LAYOUT_LABEL, NETWORK_POLICY_UID_ANNOTATION


def owned_pair():
    """Construct fresh real SDK objects for an already-started, bound dual-resource instance."""
    fake = CreateClient()
    fake.pod = build_pod_spec(sandbox_spec(), namespace=NAMESPACE, agent_image=IMAGE)
    fake.pod.metadata.uid, fake.pod.metadata.resource_version = UID, "100"
    fake.pod.spec.node_name = "test-worker"
    fake.pod.status = primary_pod().status
    fake.networking.policy = build_network_policy(fake.pod)
    fake.networking.policy.metadata.uid = POLICY_UID
    fake.networking.policy.metadata.resource_version = "200"
    fake.pod.metadata.annotations[NETWORK_POLICY_UID_ANNOTATION] = POLICY_UID
    return fake


def test_policy_selects_only_the_main_pod_and_transaction_with_no_allow_rules():
    """Both directions are explicitly denied for exactly one marked sandbox transaction."""
    pod = build_pod_spec(sandbox_spec(), namespace=NAMESPACE, agent_image=IMAGE)
    policy = build_network_policy(pod)
    assert policy.api_version == "networking.k8s.io/v1" and policy.kind == "NetworkPolicy"
    assert policy.metadata.name == network_policy_name(SID) and policy.metadata.namespace == NAMESPACE
    assert policy.spec.pod_selector.match_labels == pod.metadata.labels
    assert policy.spec.pod_selector.match_expressions in (None, [])
    assert policy.spec.policy_types == ["Ingress", "Egress"]
    assert policy.spec.ingress == policy.spec.egress == []
    assert policy.metadata.annotations == pod.metadata.annotations
    assert policy.metadata.labels["io.agent-sandbox.resource"] == "network-policy"
    assert policy.metadata.labels["io.agent-sandbox.transaction-id"] == TXN


def test_policy_builds_share_no_mutable_state():
    """Changing policy selectors, labels or rules never changes a Pod or another policy."""
    pod = build_pod_spec(sandbox_spec(), namespace=NAMESPACE, agent_image=IMAGE)
    first, second = build_network_policy(pod), build_network_policy(pod)
    original = copy.deepcopy(pod.to_dict())
    first.spec.pod_selector.match_labels.clear()
    first.spec.ingress.append(client.V1NetworkPolicyIngressRule())
    first.metadata.annotations.clear()
    assert second.spec.pod_selector.match_labels == pod.metadata.labels
    assert second.spec.ingress == [] and pod.to_dict() == original


@pytest.mark.anyio
async def test_policy_is_verified_before_the_one_pod_post_and_cleanup_is_reversed():
    """The policy precedes the workload, and deletion waits for Pod absence before policy removal."""
    fake = CreateClient()
    await runtime(fake).create(sandbox_spec())
    events = [name for name, _ in fake.events]
    assert events.index("policy-create") < events.index("create")
    assert fake.pod.metadata.annotations[NETWORK_POLICY_UID_ANNOTATION] == POLICY_UID
    assert (await runtime(fake).get(SID)).status == "ready"
    await runtime(fake).delete(SID, expected_transaction_id=TXN)
    events = [name for name, _ in fake.events]
    assert events.index("delete") < events.index("policy-delete")
    policy_delete = next(call for call in fake.networking.calls if call[0] == "delete")
    assert policy_delete[2]["body"].preconditions.uid == POLICY_UID
    assert fake.pod is fake.networking.policy is None


@pytest.mark.anyio
@pytest.mark.parametrize("status", [403, 409, 422])
async def test_definite_policy_rejection_never_posts_a_pod_or_adopts_a_policy(status):
    """No resources are attributed to this transaction after a definite policy POST rejection."""
    fake = CreateClient()
    fake.networking.commit = False
    fake.networking.create_error = client.ApiException(status=status)
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNCHANGED
    assert not fake.create_calls and fake.networking.policy is None
    assert not any(call[0] == "delete" for call in fake.networking.calls)


@pytest.mark.anyio
@pytest.mark.parametrize("committed", [True, False])
async def test_lost_policy_response_requires_confirmation_without_posting_a_pod(committed, creation_clock):
    """A lost policy response cannot be treated as unchanged merely because no Pod was submitted."""
    fake = CreateClient()
    fake.networking.commit = committed
    fake.networking.create_error = TimeoutError("private response")
    with pytest.raises(SandboxCreateFailed if committed else SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is (
        CreationResourceState.ROLLED_BACK if committed else CreationResourceState.UNKNOWN)
    assert not fake.create_calls and fake.networking.policy is None
    assert len([call for call in fake.networking.calls if call[0] == "create"]) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["missing", "ingress", "egress", "selector", "transaction", "owner", "role", "types", "version", "replacement", "unbound", "deleting"])
async def test_query_cannot_report_ready_for_missing_or_weakened_policy(fault):
    """A marked Pod requires its original policy UID, exact selector, identity and zero allow rules."""
    fake = owned_pair()
    policy = fake.networking.policy
    if fault == "missing":
        fake.networking.policy = None
    elif fault == "ingress":
        policy.spec.ingress = [client.V1NetworkPolicyIngressRule()]
    elif fault == "egress":
        policy.spec.egress = [client.V1NetworkPolicyEgressRule()]
    elif fault == "selector":
        policy.spec.pod_selector.match_labels.pop("io.agent-sandbox.transaction-id")
    elif fault == "transaction":
        policy.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    elif fault == "owner":
        policy.metadata.labels["io.agent-sandbox.project"] = "other"
    elif fault == "role":
        policy.metadata.labels["io.agent-sandbox.resource"] = "other"
    elif fault == "types":
        policy.spec.policy_types = ["Ingress"]
    elif fault == "version":
        policy.metadata.labels[NETWORK_LAYOUT_LABEL] = "unknown"
    elif fault == "replacement":
        policy.metadata.uid = "b4667ab6-3f02-40ba-9b29-b1967503a3f0"
    elif fault == "unbound":
        fake.pod.metadata.annotations.pop(NETWORK_POLICY_UID_ANNOTATION)
    else:
        policy.metadata.deletion_timestamp = fake.pod.status.container_statuses[0].state.running.started_at
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake).get(SID)


@pytest.mark.anyio
@pytest.mark.parametrize("layout", ["both", "pod-only", "policy-only", "none", "legacy"])
async def test_partial_and_legacy_deletion_is_idempotent(layout):
    """Missing one resource never prevents cleanup of the remaining correctly owned resource."""
    fake = owned_pair()
    if layout in {"policy-only", "none"}:
        fake.pod = None
    if layout in {"pod-only", "none", "legacy"}:
        fake.networking.policy = None
    if layout == "legacy":
        fake.pod = primary_pod()
    await runtime(fake).delete(SID, expected_transaction_id=TXN)
    await runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert fake.pod is fake.networking.policy is None


@pytest.mark.anyio
async def test_weakened_policy_does_not_prevent_ownership_checked_cleanup():
    """Rule integrity is required for ready, but cleanup still removes owned relaxed policies after the Pod."""
    fake = owned_pair()
    fake.networking.policy.spec.egress = [client.V1NetworkPolicyEgressRule()]
    await runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert fake.pod is fake.networking.policy is None


@pytest.mark.anyio
async def test_stuck_or_uncertain_pod_preserves_policy(creation_clock):
    """Cleanup failure never revokes network protection while the original workload may remain."""
    fake = CreateClient()
    fake.startup, fake.remove_on_delete = "failed", False
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert fake.pod is not None and fake.networking.policy is not None
    assert not any(call[0] == "delete" for call in fake.networking.calls)


@pytest.mark.anyio
async def test_unknown_pod_post_keeps_confirmed_policy(creation_clock):
    """Repeated Pod 404 after an ambiguous POST is insufficient to release its policy."""
    fake = CreateClient()
    fake.commit = False
    fake.create_error = TimeoutError()
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert fake.networking.policy is not None
    assert not any(call[0] == "delete" for call in fake.networking.calls)


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["policy-create", "policy-delete"])
async def test_repeated_cancel_settles_both_resources(stage, creation_clock):
    """Neither policy POST nor final policy removal loses its worker during repeated cancellation."""
    fake = CreateClient()
    entered, release = threading.Event(), threading.Event()
    if stage == "policy-create":
        fake.networking.create_entered, fake.networking.create_release = entered, release
    else:
        fake.startup = "failed"
        fake.networking.delete_entered, fake.networking.delete_release = entered, release
    operation = asyncio.create_task(runtime(fake).create(sandbox_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        operation.cancel()
        await asyncio.sleep(0)
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert fake.pod is fake.networking.policy is None and fake.closed.is_set()


@pytest.mark.anyio
async def test_policy_uid_replacement_after_pod_cleanup_is_not_deleted(creation_clock):
    """A same-name new policy cannot be adopted during rollback even with matching transaction labels."""
    fake = CreateClient()
    fake.startup = "failed"
    original = fake.delete_namespaced_pod

    def replace_policy_after_pod(*args, **kwargs):
        """Replace only the policy UID after the original Pod has been removed."""
        result = original(*args, **kwargs)
        fake.networking.policy.metadata.uid = "b4667ab6-3f02-40ba-9b29-b1967503a3f0"
        return result

    fake.delete_namespaced_pod = replace_policy_after_pod
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert fake.pod is None and fake.networking.policy is not None
    assert not any(call[0] == "delete" for call in fake.networking.calls)


@pytest.mark.anyio
async def test_public_delete_rejects_policy_uid_different_from_pod_binding():
    """A replacement policy is not acquired as this Pod's cleanup target after control-plane restart."""
    fake = owned_pair()
    fake.networking.policy.metadata.uid = "b4667ab6-3f02-40ba-9b29-b1967503a3f0"
    with pytest.raises(SandboxDeleteConflict):
        await runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert not fake.delete_calls and not any(call[0] == "delete" for call in fake.networking.calls)


@pytest.mark.anyio
@pytest.mark.parametrize("conflict", ["owned", "foreign", "duplicate"])
async def test_preexisting_policy_prevents_both_posts_and_is_never_cleaned(conflict):
    """Policy preflight refuses all existing identities without adopting them for rollback."""
    fake = owned_pair()
    fake.pod = None
    if conflict == "foreign":
        fake.networking.inventory = []
        fake.networking.policy.metadata.labels["io.agent-sandbox.project"] = "other"
    elif conflict == "duplicate":
        fake.networking.inventory = [fake.networking.policy, copy.deepcopy(fake.networking.policy)]
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNCHANGED
    assert not fake.create_calls and not any(call[0] in {"create", "delete"} for call in fake.networking.calls)


@pytest.mark.anyio
async def test_relaxed_policy_creation_response_fails_before_pod_but_can_be_cleaned():
    """Successful policy POST is insufficient when the actual returned rules allow traffic."""
    fake = CreateClient()
    original = fake.networking.create_namespaced_network_policy

    def relaxed_response(*args, **kwargs):
        """Return and store an owned but unsafe policy without any Pod creation."""
        original(*args, **kwargs)
        fake.networking.policy.spec.ingress = [client.V1NetworkPolicyIngressRule()]
        return copy.deepcopy(fake.networking.policy)

    fake.networking.create_namespaced_network_policy = relaxed_response
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    assert not fake.create_calls and fake.networking.policy is None


@pytest.mark.anyio
async def test_policy_cleanup_failure_cannot_be_reclassified_by_client_close(creation_clock):
    """An owned policy remaining after Pod cleanup is not unchanged, even with a simultaneous close failure."""
    fake = CreateClient()
    fake.startup = "failed"
    fake.networking.remove_on_delete = False
    fake.close_error = RuntimeError("private closing error")
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert fake.pod is None and fake.networking.policy is not None
    assert captured.value.creation_failure.cleanup_issues[-1].resource_kind == "network-policy"


@pytest.mark.anyio
@pytest.mark.parametrize("version", [None, "", "2"])
async def test_unknown_pod_layout_never_claims_ready_or_deletes_resources(version):
    """Explicit invalid versions are not downgraded into the legacy single-Pod shape."""
    fake = owned_pair()
    fake.pod.metadata.labels[NETWORK_LAYOUT_LABEL] = version
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake).get(SID)
    with pytest.raises(SandboxDeleteConflict):
        await runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert not fake.delete_calls
