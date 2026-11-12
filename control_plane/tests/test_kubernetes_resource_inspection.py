"""Read-only integrity classification independent of runtime readiness and current creation configuration."""

import asyncio
import copy
import json
import threading

import pytest
from kubernetes import client

from control_plane.core.errors import KubernetesOperationUnsupported, SandboxDeleteConflict, SandboxNotFound
from kubernetes_proxy_fakes import FakeProxyClient, PREFIX, ROLE
from test_kubernetes_proxy_lifecycle import runtime, authenticated_spec, proxy_clock
from test_kubernetes_persistent_workspace import persistence_clock
from test_kubernetes_query import SID, TXN

ROLES = ("container", "network-policy", "agent-service", "proxy-access", "proxy",
         "agent-proxy-ingress", "proxy-isolation", "proxy-service", "private-workspace")
VERSION = PREFIX + "kubernetes-proxy-version"
RECORD = PREFIX + "proxy-resource-uids"


async def inspect(api, **changes):
    """Observe through a fresh unconfigured Runtime, never reuse its creation settings as layout evidence."""
    return await runtime(api, proxy_image=None).inspect_resources(SID, expected_transaction_id=TXN, **changes)


async def prepared(version="2"):
    """Create an offline historical or current inventory once, then erase its write ledger."""
    api = FakeProxyClient()
    options = {"workspace_storage_class": "agent-sandbox-workspace"} if version == "3" else {}
    await runtime(api, **options).create(authenticated_spec())
    if version == "1":
        api.proxy_service = None
        for resource in api.objects():
            resource.metadata.annotations[VERSION] = "1"
            receipt = json.loads(resource.metadata.annotations[RECORD])
            receipt.pop("proxy_service")
            resource.metadata.annotations[RECORD] = json.dumps(receipt)
    api.trace.clear()
    api.requests.clear()
    api.closed.clear()
    return api


@pytest.mark.anyio
@pytest.mark.parametrize("version,count", [("1", 7), ("2", 8), ("3", 9)])
async def test_complete_versioned_inventory_is_read_only(version, count, persistence_clock):
    """All declared roles and original bindings are inspected without any mutation or HTTP request."""
    api = await prepared(version)
    result = await inspect(api)
    assert (result.classification, result.reason) == ("all_present", "kubernetes_resources_complete")
    expected = ROLES[:count] if count != 7 else tuple(role for role in ROLES[:8] if role != "proxy-service")
    assert set(result.found_resources) == set(expected) and result.missing_resources == ()
    assert all(verb in {"read", "list"} for verb, role in api.trace)
    assert api.closed.is_set()
    assert "Q" * 43 not in repr(result) and "sha256" not in repr(result)


@pytest.mark.anyio
@pytest.mark.parametrize("role", ROLES)
async def test_one_missing_resource_is_partial(role, persistence_clock):
    """Every nine-resource role has a distinct missing name, even when the primary Pod is absent."""
    api = await prepared("3")
    resource = api.by_role(role)
    kind = next(kind for kind in ("pod", "service", "secret", "network_policy", "persistent_volume_claim")
                if resource in api.objects(kind))
    api._store(kind, None, role=role)
    result = await inspect(api)
    assert (result.classification, result.reason) == ("partial", "kubernetes_resources_partial")
    assert result.missing_resources == (role,)
    assert set(result.found_resources) == set(ROLES) - {role}


@pytest.mark.anyio
async def test_no_objects_requires_namespace_and_does_not_invent_layout():
    """Absence cannot borrow a default topology from current Runtime creation settings."""
    api = FakeProxyClient()
    result = await inspect(api)
    assert (result.classification, result.reason) == ("not_found", "kubernetes_resources_not_found")
    assert result.found_resources == result.missing_resources == ()


@pytest.mark.anyio
async def test_retained_pvc_is_explicit_partial(persistence_clock):
    """Default deletion intentionally leaves data, which is not a complete recoverable sandbox."""
    api = await prepared("3")
    await runtime(api, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    api.trace.clear()
    result = await inspect(api)
    assert (result.classification, result.reason) == ("partial", "kubernetes_private_workspace_retained")
    assert result.found_resources == ("private-workspace",)
    assert set(result.missing_resources) == set(ROLES[:-1])
    assert all(verb in {"read", "list"} for verb, role in api.trace)


@pytest.mark.anyio
async def test_pending_pvc_and_unready_pods_still_have_complete_resources(persistence_clock):
    """Integrity must not require runtime startup, Agent readiness or storage binding to have finished."""
    api = await prepared("3")
    api.startup = {"container": "pending", "proxy": "not-ready"}
    api.workspace.status = client.V1PersistentVolumeClaimStatus(phase="Pending")
    api.workspace.spec.volume_name = None
    assert (await inspect(api)).classification == "all_present"


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["transaction", "uid", "unknown-role", "duplicate-role", "selector",
                                   "policy", "secret-type", "mount", "receipt", "storage-class"])
async def test_static_positive_conflicts_are_not_missing_or_unavailable(change, persistence_clock):
    """Owned names alone never authorize incompatible identities, routes, mounts or policy rules."""
    api = await prepared("3")
    if change == "transaction":
        api.proxy_pod.metadata.labels[PREFIX + "transaction-id"] = "txn_" + "a" * 32
    elif change == "uid":
        api.workspace.metadata.uid = "00000000-0000-4000-8000-000000000001"
    elif change == "unknown-role":
        api.proxy_service.metadata.labels[ROLE] = "unknown"
    elif change == "duplicate-role":
        api.inventories["service"] = [api.services.service, copy.deepcopy(api.services.service)]
    elif change == "selector":
        api.services.service.spec.selector[PREFIX + "sandbox-id"] = "sbx_" + "f" * 32
    elif change == "policy":
        api.networking.policy.spec.egress = [client.V1NetworkPolicyEgressRule()]
    elif change == "secret-type":
        api.secret.type = "kubernetes.io/service-account-token"
    elif change == "mount":
        api.pod.spec.volumes[0].persistent_volume_claim.claim_name = "other"
    elif change == "receipt":
        api.secret.metadata.annotations[RECORD] = '{"unknown":"00000000-0000-4000-8000-000000000001"}'
    else:
        api.workspace.spec.storage_class_name = "changed"
    result = await inspect(api)
    assert (result.classification, result.reason) == ("conflict", "kubernetes_resource_conflict")


@pytest.mark.anyio
@pytest.mark.parametrize("fact", [RECORD, PREFIX + "proxy-token-sha256", PREFIX + "proxy-image"])
async def test_incomplete_publication_is_partial_without_repair(fact, persistence_clock):
    """A missing promised receipt or credential fact cannot become a successful inferred identity."""
    api = await prepared("2")
    for resource in api.objects():
        resource.metadata.annotations.pop(fact, None)
    result = await inspect(api)
    assert (result.classification, result.reason) == ("partial", "kubernetes_identity_incomplete")
    assert all(verb in {"read", "list"} for verb, role in api.trace)


@pytest.mark.anyio
@pytest.mark.parametrize("status", [403, 404, 504, 500])
async def test_unreliable_collection_observation_is_unavailable(status, persistence_clock):
    """Namespace/permission/transport errors never masquerade as missing objects."""
    api = await prepared("2")
    api.faults[("list", "service")] = client.ApiException(status=status, reason="private response")
    result = await inspect(api)
    assert (result.classification, result.reason) == ("unavailable", "kubernetes_inspection_unavailable")
    assert "private response" not in repr(result)


@pytest.mark.anyio
async def test_truncated_list_is_not_an_empty_or_complete_success(persistence_clock):
    """An unread continuation leaves the role inventory unknown instead of confirming missing roles."""
    api = await prepared("3")
    api.continuations["pod"] = "unread"
    assert (await inspect(api)).classification == "unavailable"


@pytest.mark.anyio
async def test_replacement_between_list_and_read_is_changed(persistence_clock):
    """Positive evidence of an original UID followed by a replacement is distinguished from static conflict."""
    api = await prepared("3")
    original_read = api._read
    changed = False

    def read(kind, name, namespace, kwargs):
        """Act as an external replacement just after the first ownership listing."""
        nonlocal changed
        if kind == "persistent_volume_claim" and not changed:
            api.workspace.metadata.uid = "00000000-0000-4000-8000-000000000001"
            changed = True
        return original_read(kind, name, namespace, kwargs)
    api._read = read
    result = await inspect(api)
    assert (result.classification, result.reason) == ("changed", "kubernetes_resources_changed")


@pytest.mark.anyio
async def test_standalone_close_failure_changes_success_to_unavailable(persistence_clock):
    """Client closure is part of successful observation and must not silently fail."""
    api = await prepared("2")
    api.close_error = RuntimeError("private close diagnostic")
    assert (await inspect(api)).classification == "unavailable"


@pytest.mark.anyio
@pytest.mark.parametrize("sandbox_id,transaction", [("invalid", TXN), (SID, "invalid")])
async def test_invalid_identity_is_rejected_before_client_creation(sandbox_id, transaction):
    """Malformed identities cannot trigger configuration loading or API access."""
    def forbidden(**kwargs):
        """Fail any attempted transport acquisition for invalid arguments."""
        pytest.fail("invalid identity created a client")
    with pytest.raises((SandboxNotFound, SandboxDeleteConflict)):
        await runtime(None, client_factory=forbidden).inspect_resources(sandbox_id, expected_transaction_id=transaction)


@pytest.mark.anyio
async def test_independent_workspace_remains_explicitly_unsupported():
    """A private PVC cannot pretend to verify the separate persistent occupancy contract."""
    with pytest.raises(KubernetesOperationUnsupported):
        await inspect(None, expected_workspace=object())


@pytest.mark.anyio
@pytest.mark.parametrize("count", [1, 2, 3])
async def test_early_layouts_use_their_own_persisted_declarations(count, proxy_clock):
    """Single Pod, Pod+policy and three-resource history are not upgraded to a proxy topology."""
    api = FakeProxyClient()
    await runtime(api, proxy_image=None).create(authenticated_spec())
    if count < 3:
        api.services.service = None
        api.pod.metadata.annotations.pop(PREFIX + "agent-service-version")
        api.pod.metadata.annotations.pop(PREFIX + "agent-service-uid")
    if count == 1:
        api.networking.policy = None
        api.pod.metadata.labels.pop(PREFIX + "kubernetes-network-version")
        api.pod.metadata.annotations.pop(PREFIX + "network-policy-uid")
    result = await inspect(api)
    assert result.classification == "all_present" and set(result.found_resources) == set(ROLES[:count])


@pytest.mark.anyio
@pytest.mark.parametrize("role", ["container", "proxy", "private-workspace"])
async def test_already_deleting_objects_are_partial_not_identity_conflicts(role, persistence_clock):
    """A legal in-progress deletion is not normal completeness, but its marker alone proves no ownership conflict."""
    api = await prepared("3")
    from test_kubernetes_query import CREATED
    api.by_role(role).metadata.deletion_timestamp = CREATED
    result = await inspect(api)
    assert (result.classification, result.reason) == ("partial", "kubernetes_resources_deleting")


@pytest.mark.anyio
async def test_empty_receipts_preserve_incomplete_identity(persistence_clock):
    """An empty bounded receipt has no contradictory UID and cannot authorize all_present."""
    api = await prepared("2")
    for resource in api.objects():
        resource.metadata.annotations[RECORD] = "{}"
    result = await inspect(api)
    assert (result.classification, result.reason) == ("partial", "kubernetes_identity_incomplete")


@pytest.mark.anyio
async def test_damaged_receipt_does_not_become_incomplete_after_version_removal(persistence_clock):
    """Missing layout publication cannot hide a positively malformed UID receipt."""
    api = await prepared("2")
    for resource in api.objects():
        resource.metadata.annotations.pop(VERSION)
    api.secret.metadata.annotations[RECORD] = "damaged"
    assert (await inspect(api)).classification == "conflict"


@pytest.mark.anyio
async def test_conflict_is_not_overwritten_by_close_failure(persistence_clock):
    """A cleanup failure does not erase already established positive conflicting ownership."""
    api = await prepared("2")
    api.secret.metadata.labels[PREFIX + "transaction-id"] = "txn_" + "f" * 32
    api.close_error = RuntimeError("private close")
    assert (await inspect(api)).classification == "conflict"


@pytest.mark.anyio
async def test_missing_namespace_and_transport_timeout_are_unavailable():
    """Neither namespace 404 nor a client timeout may be interpreted as complete absence."""
    for error in (client.ApiException(status=404), TimeoutError()):
        api = FakeProxyClient()

        def namespace(*args, **kwargs):
            """Raise a fixed controlled failure instead of claiming namespace existence."""
            raise error
        api.read_namespace = namespace
        assert (await inspect(api)).classification == "unavailable"


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["ingress", "isolation", "upstream"])
async def test_partial_layout_does_not_hide_known_link_conflicts(change, persistence_clock):
    """An unrelated missing resource does not erase positive evidence of cross-instance routing or loosened policy."""
    api = await prepared("3")
    api.secret = None
    if change == "ingress":
        api.by_role("agent-proxy-ingress").spec.ingress = [client.V1NetworkPolicyIngressRule()]
    elif change == "isolation":
        api.by_role("proxy-isolation").spec.egress = [client.V1NetworkPolicyEgressRule()]
    else:
        upstream = next(env for env in api.proxy_pod.spec.containers[0].env if env.name == "SANDBOX_AGENT_UPSTREAM_IPV4")
        upstream.value = "10.96.0.99"
    assert (await inspect(api)).classification == "conflict"


@pytest.mark.anyio
@pytest.mark.parametrize("condition", ["Lost", "Resizing"])
async def test_storage_health_does_not_redefine_resource_presence(condition, persistence_clock):
    """An unchanged PVC can need health remediation while every declared resource remains present and owned."""
    api = await prepared("3")
    if condition == "Lost":
        api.workspace.status.phase = "Lost"
    else:
        api.workspace.status.conditions = [client.V1PersistentVolumeClaimCondition(type="Resizing", status="True")]
    assert (await inspect(api)).classification == "all_present"
    from control_plane.core.errors import SandboxQueryFailed
    with pytest.raises(SandboxQueryFailed):
        await runtime(api, proxy_image=None).get(SID)


@pytest.mark.anyio
@pytest.mark.parametrize("mutation", ["host-network", "host-pid", "host-ipc", "privileged", "root", "writable-root", "escalation",
                                      "container-root", "container-seccomp"])
async def test_fixed_agent_security_contradictions_are_conflicts(mutation, persistence_clock):
    """Managed identities do not make explicitly loosened primary container isolation valid configuration."""
    api = await prepared("3")
    spec = api.pod.spec
    if mutation == "host-network":
        spec.host_network = True
    elif mutation == "host-pid":
        spec.host_pid = True
    elif mutation == "host-ipc":
        spec.host_ipc = True
    elif mutation == "root":
        spec.security_context.run_as_user = 0
    else:
        security = spec.containers[0].security_context
        if mutation == "privileged":
            security.privileged = True
        elif mutation == "writable-root":
            security.read_only_root_filesystem = False
        elif mutation == "container-root":
            security.run_as_user = 0
        elif mutation == "container-seccomp":
            security.seccomp_profile = client.V1SeccompProfile(type="Unconfined")
        else:
            security.allow_privilege_escalation = True
    assert (await inspect(api)).classification == "conflict"


@pytest.mark.anyio
async def test_missing_pvc_business_timestamp_is_incomplete_not_invented(persistence_clock):
    """A missing original field remains partial without synthesizing a timestamp for strict builders."""
    api = await prepared("3")
    api.workspace.metadata.annotations.pop(PREFIX + "created-at")
    result = await inspect(api)
    assert (result.classification, result.reason) == ("partial", "kubernetes_identity_incomplete")


@pytest.mark.anyio
async def test_role_changes_between_rounds_have_positive_changed_evidence(persistence_clock):
    """A role changed after a valid first scan is distinguished from a static unknown-role conflict."""
    api = await prepared("2")
    original = api._list
    rounds = 0

    def listing(kind, namespace, kwargs):
        """Simulate an external metadata change exactly at the second bounded observation."""
        nonlocal rounds
        if kind == "pod":
            rounds += 1
            if rounds == 2:
                api.proxy_pod.metadata.labels[ROLE] = "new-unknown-role"
        return original(kind, namespace, kwargs)
    api._list = listing
    assert (await inspect(api)).classification == "changed"
