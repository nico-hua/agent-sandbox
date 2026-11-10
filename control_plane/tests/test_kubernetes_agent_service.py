"""Offline Agent ClusterIP Service contract and three-resource lifecycle tests."""

import asyncio
import copy
import json
import threading

import pytest
from kubernetes import client

from control_plane.core.errors import (
    SandboxEndpointUnavailable,
    CreationResourceState, KubernetesOperationUnsupported, SandboxCleanupFailed,
    SandboxCreateFailed, SandboxDeleteConflict, SandboxQueryFailed,
)
from control_plane.runtime.kubernetes.network_policy import build_network_policy
from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
from test_kubernetes_pod_spec import IMAGE, sandbox_spec
from test_kubernetes_query import NAMESPACE, SID, TXN, UID, primary_pod
from test_kubernetes_create import CreateClient, creation_clock, runtime
from test_kubernetes_delete import deletion_clock
from kubernetes_network_fakes import POLICY_UID
from kubernetes_service_fakes import SERVICE_UID


def test_agent_service_spec_is_internal_and_does_not_change_network_selector():
    """Expose only one unallocated internal TCP Agent port without adding Pod selector labels."""
    from control_plane.runtime.kubernetes.agent_service import build_agent_service, agent_service_name
    pod = build_pod_spec(sandbox_spec(access_token="A" * 43), namespace=NAMESPACE, agent_image=IMAGE)
    policy = build_network_policy(pod)
    service = build_agent_service(pod)
    assert service.kind == "Service" and service.api_version == "v1"
    assert service.metadata.name == agent_service_name(SID) and service.metadata.namespace == NAMESPACE
    assert service.metadata.labels["io.agent-sandbox.resource"] == "agent-service"
    assert service.spec.type == "ClusterIP" and service.spec.cluster_ip is None
    assert service.spec.selector == policy.spec.pod_selector.match_labels == pod.metadata.labels
    assert len(service.spec.selector) == 6
    assert len(service.spec.ports) == 1
    port = service.spec.ports[0]
    assert (port.protocol, port.port, port.target_port) == ("TCP", 8080, 8080)
    assert port.node_port is None
    assert service.spec.publish_not_ready_addresses is False
    assert service.spec.external_ips in (None, []) and service.spec.external_name is None
    assert service.spec.load_balancer_ip is None
    assert "A" * 43 not in json.dumps(service.to_dict())


@pytest.mark.anyio
async def test_three_resources_create_in_order_and_query_checks_service(creation_clock):
    """Create one internal Service before the Pod, bind its UID and preserve ready observation."""
    fake = CreateClient()
    await runtime(fake).create(sandbox_spec())
    events = [event for event, _ in fake.events]
    assert events.index("policy-create") < events.index("service-create") < events.index("create")
    assert fake.pod.metadata.annotations["io.agent-sandbox.agent-service-uid"] == SERVICE_UID
    assert (await runtime(fake).get(SID)).status == "ready"
    fake.services.service = None
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake).get(SID)


@pytest.mark.anyio
async def test_delete_three_resources_in_reverse_order_and_idempotent(creation_clock):
    """Confirm each predecessor gone before removing its internal Service and final protection."""
    fake = CreateClient()
    await runtime(fake).create(sandbox_spec())
    await runtime(fake).delete(SID, expected_transaction_id=TXN)
    events = [event for event, _ in fake.events]
    assert events.index("delete") < events.index("service-delete") < events.index("policy-delete")
    assert fake.pod is fake.services.service is fake.networking.policy is None
    await runtime(fake).delete(SID, expected_transaction_id=TXN)


def owned_triple():
    """Construct independent accepted objects with original bindings, avoiding SDK or network I/O."""
    from control_plane.runtime.kubernetes.agent_service import build_agent_service
    from test_kubernetes_network import owned_pair
    fake = owned_pair()
    fake.pod.metadata.annotations["io.agent-sandbox.agent-service-version"] = "1"
    fake.create_namespaced_service(NAMESPACE, body=build_agent_service(fake.pod))
    fake.pod.metadata.annotations["io.agent-sandbox.agent-service-uid"] = SERVICE_UID
    return fake


def test_agent_service_builds_are_independent():
    """Service dictionaries and ports never share mutable state with the Pod or another build."""
    from control_plane.runtime.kubernetes.agent_service import build_agent_service
    pod = build_pod_spec(sandbox_spec(), namespace=NAMESPACE, agent_image=IMAGE)
    before = copy.deepcopy(pod.to_dict())
    first, second = build_agent_service(pod), build_agent_service(pod)
    first.spec.selector.clear()
    first.metadata.labels.clear()
    first.metadata.annotations.clear()
    first.spec.ports[0].port = 9000
    assert pod.to_dict() == before and second.spec.selector == pod.metadata.labels
    assert second.spec.ports[0].port == 8080 and len(second.metadata.annotations) == 3


@pytest.mark.anyio
@pytest.mark.parametrize("attribute,value", [
    ("type", "NodePort"), ("type", "LoadBalancer"), ("type", "ExternalName"),
    ("cluster_ip", "None"), ("cluster_ip", None), ("cluster_ip", "127.0.0.1"),
    ("external_ips", ["1.1.1.1"]), ("external_name", "other.example"),
    ("load_balancer_ip", "1.1.1.1"), ("load_balancer_class", "untrusted"),
    ("publish_not_ready_addresses", True), ("selector", {}),
    ("cluster_ips", ["10.96.0.99"]), ("ip_families", ["IPv6"]),
])
async def test_get_rejects_modified_service_routing(attribute, value):
    """Label ownership cannot confer normal status after routing or exposure changes."""
    fake = owned_triple()
    setattr(fake.services.service.spec, attribute, value)
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake).get(SID)
    assert not fake.delete_calls
    assert not any(method == "delete" for method, _, _ in fake.services.calls)


@pytest.mark.anyio
@pytest.mark.parametrize("attribute,value", [
    ("port", 8081), ("target_port", "agent-http"), ("target_port", 9000),
    ("protocol", "UDP"), ("node_port", 30080), ("app_protocol", "custom"),
])
async def test_get_rejects_modified_service_port(attribute, value):
    """Only one exact numeric TCP 8080 mapping is accepted even with matching ownership."""
    fake = owned_triple()
    setattr(fake.services.service.spec.ports[0], attribute, value)
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake).get(SID)


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["missing", "uid", "transaction", "time", "namespace", "version", "unknown", "binding", "role", "extra-port"])
async def test_get_refuses_inconsistent_service_identity(fault):
    """Invalid Service evidence never silently downgrades a new Pod to a historical layout."""
    fake = owned_triple()
    service = fake.services.service
    if fault == "missing":
        fake.services.service = None
    elif fault == "uid":
        service.metadata.uid = "d5667ab6-3f02-40ba-9b29-b1967503a3f0"
    elif fault == "transaction":
        service.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    elif fault == "time":
        service.metadata.annotations["io.agent-sandbox.expires-at"] = "2026-10-20T09:00:00Z"
    elif fault == "namespace":
        service.metadata.namespace = "other"
    elif fault == "version":
        fake.pod.metadata.annotations["io.agent-sandbox.agent-service-version"] = "2"
    elif fault == "unknown":
        service.metadata.annotations["io.agent-sandbox.future-service-mode"] = "1"
    elif fault == "binding":
        fake.pod.metadata.annotations.pop("io.agent-sandbox.agent-service-uid")
    elif fault == "role":
        service.metadata.labels["io.agent-sandbox.resource"] = "proxy"
    else:
        service.spec.ports.append(client.V1ServicePort(port=9000, target_port=9000))
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake).get(SID)


@pytest.mark.anyio
async def test_api_defaulted_internal_fields_and_dual_stack_are_accepted():
    """Known omitted defaults and allocated dual-stack addresses need not serialize like the request."""
    fake = owned_triple()
    spec = fake.services.service.spec
    spec.publish_not_ready_addresses = None
    spec.cluster_ips = [spec.cluster_ip, "fd00::12"]
    spec.ip_families = ["IPv4", "IPv6"]
    spec.ip_family_policy = "PreferDualStack"
    assert (await runtime(fake).get(SID)).status == "ready"


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["owned", "foreign", "paged", "multiple"])
async def test_service_conflict_precedes_any_mutation(fault):
    """Inventory and exact-name Service conflicts prevent even NetworkPolicy creation."""
    fake = CreateClient()
    fake.services.service = copy.deepcopy(owned_triple().services.service)
    if fault == "foreign":
        fake.services.service.metadata.labels = {}
        fake.services.inventory = []
    elif fault == "paged":
        fake.services.continuation = "more"
    elif fault == "multiple":
        fake.services.inventory = [fake.services.service, copy.deepcopy(fake.services.service)]
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNCHANGED
    assert not fake.create_calls
    assert not any(name.endswith("create") or name.endswith("delete") for name, _ in fake.events)


@pytest.mark.anyio
@pytest.mark.parametrize("committed", [False, True])
async def test_service_lost_create_response(committed, creation_clock):
    """Positive discovery can resolve a lost response; empty reads never prove no mutation."""
    fake = CreateClient()
    fake.services.commit = committed
    fake.services.create_error = ConnectionError("private transport detail")
    expected = CreationResourceState.ROLLED_BACK if committed else CreationResourceState.UNKNOWN
    with pytest.raises((SandboxCreateFailed, SandboxCleanupFailed)) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is expected
    assert not fake.create_calls and fake.services.service is None
    assert len([c for c in fake.services.calls if c[0] == "create"]) == 1
    assert (fake.networking.policy is None) is committed
    assert "private transport detail" not in str(captured.value)


@pytest.mark.anyio
@pytest.mark.parametrize("status", [403, 409, 422])
async def test_definite_service_rejection_rolls_back_policy(status, creation_clock):
    """Rejected Service submission never reaches Pod POST or claims an unchanged transaction."""
    fake = CreateClient()
    fake.services.commit = False
    fake.services.create_error = client.ApiException(status=status)
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    assert fake.networking.policy is None and not fake.create_calls
    assert not any(method == "delete" for method, _, _ in fake.services.calls)


@pytest.mark.anyio
@pytest.mark.parametrize("remaining", ["service", "policy", "pod-service", "pod-policy", "none"])
async def test_partial_resources_delete_without_recreation(remaining, creation_clock):
    """Owned orphan cleanup supports partial layouts without ever rebuilding missing resources."""
    fake = owned_triple()
    if "pod" not in remaining:
        fake.pod = None
    if "service" not in remaining:
        fake.services.service = None
    if "policy" not in remaining:
        fake.networking.policy = None
    await runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert fake.pod is fake.services.service is fake.networking.policy is None
    await runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert not fake.create_calls


@pytest.mark.anyio
async def test_service_delete_uses_uid_and_resource_version(creation_clock):
    """Core receives normal deletion with precise name, namespace and both identity preconditions."""
    from control_plane.runtime.kubernetes.agent_service import agent_service_name
    fake = owned_triple()
    await runtime(fake).delete(SID, expected_transaction_id=TXN)
    calls = [call for call in fake.services.calls if call[0] == "delete"]
    assert len(calls) == 1
    _, args, kwargs = calls[0]
    assert args == (agent_service_name(SID), NAMESPACE)
    assert kwargs["body"].preconditions.uid == SERVICE_UID
    assert kwargs["body"].preconditions.resource_version == "300"
    assert kwargs["body"].grace_period_seconds is None


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["pod", "service"])
async def test_unconfirmed_cleanup_retains_supporting_resources(stage, creation_clock):
    """A retained Pod or Service prevents removal of the final default-deny protection."""
    fake = CreateClient()
    fake.startup = "failed"
    if stage == "pod":
        fake.remove_on_delete = False
    else:
        fake.services.remove_on_delete = False
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert fake.services.service is not None and fake.networking.policy is not None
    assert not any(method == "delete" for method, _, _ in fake.networking.calls)
    if stage == "pod":
        assert not any(method == "delete" for method, _, _ in fake.services.calls)


@pytest.mark.anyio
async def test_replaced_service_uid_is_not_adopted_during_rollback(creation_clock):
    """A replacement after Pod cleanup is not deleted and its policy remains in place."""
    fake = CreateClient()
    fake.startup = "failed"
    original = fake.delete_namespaced_pod

    def replace_service(*args, **kwargs):
        """Replace only Service UID after original Pod removal."""
        result = original(*args, **kwargs)
        fake.services.service.metadata.uid = "d5667ab6-3f02-40ba-9b29-b1967503a3f0"
        return result

    fake.delete_namespaced_pod = replace_service
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(sandbox_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert fake.pod is None and fake.networking.policy is not None
    assert not any(method == "delete" for method, _, _ in fake.services.calls)


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["create-service", "delete-service", "close-success"])
async def test_repeated_cancel_settles_all_resources(stage, creation_clock):
    """Repeated cancellation cannot abandon Service acceptance, cleanup or late successful creation."""
    fake = CreateClient()
    entered, release = threading.Event(), threading.Event()
    if stage == "create-service":
        fake.services.create_entered, fake.services.create_release = entered, release
    elif stage == "delete-service":
        fake.startup = "failed"
        fake.services.delete_entered, fake.services.delete_release = entered, release
    else:
        fake.close_entered, fake.close_release = entered, release
    operation = asyncio.create_task(runtime(fake).create(sandbox_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        operation.cancel()
        await asyncio.sleep(0)
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done() and fake.networking.policy is not None
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert fake.pod is fake.services.service is fake.networking.policy is None
    assert fake.closed.is_set()


@pytest.mark.anyio
async def test_service_is_not_an_external_endpoint():
    """An internal Service never enables delivery of a bare Agent address."""
    fake = owned_triple()
    with pytest.raises(SandboxEndpointUnavailable):
        await runtime(fake).get_endpoint(SID)
    assert fake.calls == []


@pytest.mark.anyio
async def test_get_rejects_layout_downgrade_between_discovery_and_exact_read():
    """A new-layout candidate cannot lose both Service annotations and bypass required verification."""
    fake = owned_triple()
    original = fake.read_namespaced_pod

    def downgrade(*args, **kwargs):
        """Return the same Pod identity with a contradictory historical layout on its fresh read."""
        result = original(*args, **kwargs)
        result.metadata.annotations.pop("io.agent-sandbox.agent-service-version")
        result.metadata.annotations.pop("io.agent-sandbox.agent-service-uid")
        return result

    fake.read_namespaced_pod = downgrade
    fake.services.service = None
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake).get(SID)


@pytest.mark.anyio
async def test_get_rejects_modified_internal_traffic_policy():
    """Node-local-only routing is a configuration change rather than the internal Cluster default."""
    fake = owned_triple()
    fake.services.service.spec.internal_traffic_policy = "Local"
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake).get(SID)


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["uid", "transaction", "version"])
async def test_public_delete_preflights_service_before_removing_pod(fault):
    """A mismatched Service prevents destructive Pod work instead of being adopted during cleanup."""
    fake = owned_triple()
    if fault == "uid":
        fake.services.service.metadata.uid = "d5667ab6-3f02-40ba-9b29-b1967503a3f0"
    elif fault == "transaction":
        fake.services.service.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    else:
        fake.services.service.metadata.annotations["io.agent-sandbox.agent-service-version"] = "2"
    with pytest.raises(SandboxDeleteConflict):
        await runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert not fake.delete_calls
    assert fake.pod is not None and fake.networking.policy is not None
    assert not any(method == "delete" for method, _, _ in fake.services.calls)


@pytest.mark.anyio
async def test_cancel_with_unconfirmed_service_cleanup_keeps_domain_evidence(creation_clock):
    """Repeated cancellation cannot hide a retained Service behind ordinary CancelledError or close failure."""
    fake = CreateClient()
    fake.services.remove_on_delete = False
    fake.close_error = RuntimeError("private close failure")
    entered, release = threading.Event(), threading.Event()
    fake.services.create_entered, fake.services.create_release = entered, release
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
    with pytest.raises(SandboxCleanupFailed) as captured:
        await operation
    assert captured.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert fake.pod is None and fake.services.service is not None and fake.networking.policy is not None
    assert fake.closed.is_set() and "private close failure" not in str(captured.value)
