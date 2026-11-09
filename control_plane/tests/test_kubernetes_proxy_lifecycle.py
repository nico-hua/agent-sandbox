"""Offline optional proxy lifecycle, aggregate observation and original-UID cleanup contracts."""

import asyncio
import base64
import copy
import hashlib
import json
import threading
from types import SimpleNamespace

import pytest
from kubernetes import client

from control_plane.core.errors import (
    CreationResourceState, KubernetesConfigurationError, SandboxCleanupFailed,
    SandboxCreateFailed, SandboxDeleteConflict, SandboxDeleteFailed, SandboxQueryFailed,
)
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from kubernetes_proxy_fakes import FakeProxyClient, PREFIX, ROLE, ROLE_UIDS
from test_kubernetes_pod_spec import sandbox_spec
from test_kubernetes_query import NAMESPACE, SID, TXN

PROXY_IMAGE = "agent-sandbox-proxy:dev"
VERSION = PREFIX + "kubernetes-proxy-version"
DIGEST = PREFIX + "proxy-token-sha256"
RECORD = PREFIX + "proxy-resource-uids"
RECORD_ROLES = {"agent": "container", "policy": "network-policy", "service": "agent-service",
                "secret": "proxy-access", "proxy": "proxy", "ingress": "agent-proxy-ingress",
                "isolation": "proxy-isolation"}
CREATE_ROLES = ("network-policy", "agent-service", "container", "proxy-access",
                "proxy-isolation", "agent-proxy-ingress", "proxy")


def runtime(fake, *, proxy_image=PROXY_IMAGE, **changes):
    """Build an inert explicitly scoped Runtime with one entirely offline client factory."""
    options = dict(kubeconfig_path="/never-read", context="explicit-test", namespace=NAMESPACE,
                   client_factory=lambda **_: fake, proxy_image=proxy_image)
    options.update(changes)
    return KubernetesSandboxRuntime(**options)


def authenticated_spec(**changes):
    """Supply a synthetically fixed token without retaining it in assertion parameters or diagnostics."""
    return sandbox_spec(access_token="Q" * 43, **changes)


def mutations(fake, verb):
    """Extract ordered role identities without serializing SDK resource content."""
    return [role for method, role in fake.trace if method == verb]


@pytest.fixture
def proxy_clock(monkeypatch):
    """Share short worker-only budgets across create, rollback, companion operations and deletion."""
    from control_plane.runtime.kubernetes import agent_service, network_policy, pod_create, pod_delete
    from control_plane.runtime.kubernetes import proxy_resources
    clock = SimpleNamespace(now=0.0, waits=[])

    def now():
        """Read deterministic worker time without changing asyncio timing."""
        return clock.now

    def wait(seconds):
        """Advance only synchronous bounded polling and retain its requested interval."""
        clock.waits.append(seconds)
        clock.now += seconds

    for module in (pod_create, pod_delete, network_policy, agent_service, proxy_resources):
        if hasattr(module, "monotonic"):
            monkeypatch.setattr(module, "monotonic", now)
        if hasattr(module, "sleep"):
            monkeypatch.setattr(module, "sleep", wait)
        for key in vars(module):
            if key.endswith("TIMEOUT_SECONDS") and isinstance(getattr(module, key), (int, float)):
                monkeypatch.setattr(module, key, 0.4)
    return clock


@pytest.mark.anyio
async def test_optional_proxy_creates_complete_chain_with_original_uid_bindings(proxy_clock):
    """The explicit configuration creates seven unique resources and records only exact original UID bindings."""
    fake = FakeProxyClient()
    assert await runtime(fake).create(authenticated_spec()) is None
    assert len(fake.objects()) == 7
    posted = mutations(fake, "create")
    assert posted[:4] == list(CREATE_ROLES[:4])
    assert set(posted[4:6]) == set(CREATE_ROLES[4:6]) and posted[6:] == ["proxy"]
    assert len({item.metadata.uid for item in fake.objects()}) == 7
    expected = {key: ROLE_UIDS[role] for key, role in RECORD_ROLES.items()}
    for resource in fake.objects():
        assert resource.metadata.annotations[VERSION] == "1"
        assert json.loads(resource.metadata.annotations[RECORD]) == expected
    assert fake.pod.metadata.annotations[DIGEST] == hashlib.sha256(b"Q" * 43).hexdigest()
    assert fake.pod.spec.containers[0].env in (None, [])
    assert fake.proxy_pod.spec.containers[0].image == PROXY_IMAGE
    assert fake.secret.immutable is True
    assert "Q" * 43 not in repr(fake.requests)
    assert base64.b64encode(b"Q" * 43).decode() not in repr(fake.requests)
    assert fake.closed.is_set() and len({thread for _, thread in fake.events}) == 1


@pytest.mark.anyio
async def test_default_runtime_retains_three_resource_layout_and_ignores_token(proxy_clock):
    """Omitting trusted proxy configuration preserves the existing three-resource behavior."""
    fake = FakeProxyClient()
    await runtime(fake, proxy_image=None).create(authenticated_spec())
    assert mutations(fake, "create") == list(CREATE_ROLES[:3])
    assert len(fake.objects()) == 3 and fake.secret is None and fake.proxy_pod is None
    assert VERSION not in fake.pod.metadata.annotations
    assert RECORD not in fake.pod.metadata.annotations


@pytest.mark.anyio
@pytest.mark.parametrize("token", [None, "", "short", True])
async def test_configured_proxy_rejects_missing_or_invalid_token_before_factory(token):
    """A configured proxy cannot acquire a transport before validating its credential contract."""
    def forbidden(**_):
        """Detect any forbidden client acquisition on a pure validation failure."""
        pytest.fail("invalid proxy credential acquired a client")

    with pytest.raises((ValueError, KubernetesConfigurationError, SandboxCreateFailed)) as captured:
        await runtime(None, client_factory=forbidden).create(sandbox_spec(access_token=token))
    if not isinstance(captured.value, ValueError):
        evidence = captured.value.creation_failure
        assert evidence is not None and evidence.resource_state is CreationResourceState.UNCHANGED


@pytest.mark.parametrize("image", ["", True, " bad", "bad\nimage"])
def test_proxy_image_is_validated_without_factory_access(image):
    """Trusted proxy image validation stays inert and accepts no malformed image setting."""
    with pytest.raises(KubernetesConfigurationError):
        runtime(None, proxy_image=image)


@pytest.mark.anyio
@pytest.mark.parametrize("agent_ready,proxy_ready", [(True, True), (True, False), (False, True), (False, False)])
async def test_get_aggregates_both_pods_without_requiring_readiness_during_creation(
    agent_ready, proxy_ready, proxy_clock,
):
    """Creation accepts running Pods, while public readiness requires both independent checks."""
    fake = FakeProxyClient()
    fake.startup["container"] = "running" if agent_ready else "not-ready"
    fake.startup["proxy"] = "running" if proxy_ready else "not-ready"
    await runtime(fake).create(authenticated_spec())
    result = await runtime(fake, proxy_image=None).get(SID)
    assert result.status == ("ready" if agent_ready and proxy_ready else "running")
    assert result.transaction_id == TXN
    assert not mutations(fake, "delete")


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["secret-content", "secret-mutable", "proxy-command", "upstream", "ingress", "isolation", "uid-map"])
async def test_query_rejects_tampered_proxy_identity_configuration_or_rules(fault, proxy_clock):
    """Ownership labels do not replace credential, upstream, workload or policy integrity checks."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    if fault == "secret-content":
        fake.secret.data["access-token"] = base64.b64encode(b"R" * 43).decode()
    elif fault == "secret-mutable":
        fake.secret.immutable = False
    elif fault == "proxy-command":
        fake.proxy_pod.spec.containers[0].command = ["/unexpected"]
    elif fault == "upstream":
        fake.proxy_pod.spec.containers[0].env[0].value = "10.96.0.99"
    elif fault == "ingress":
        fake.by_role("agent-proxy-ingress").spec.ingress[0]._from = []
    elif fault == "isolation":
        fake.by_role("proxy-isolation").spec.egress = [client.V1NetworkPolicyEgressRule()]
    else:
        record = json.loads(fake.pod.metadata.annotations[RECORD])
        record["proxy"] = ROLE_UIDS["proxy-access"]
        fake.pod.metadata.annotations[RECORD] = json.dumps(record)
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake, proxy_image=None).get(SID)
    assert not mutations(fake, "delete")


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["pod", "network_policy", "secret", "service"])
@pytest.mark.parametrize("fault", ["duplicate", "unknown-role"])
async def test_unknown_or_duplicate_same_sandbox_roles_are_never_hidden(kind, fault, proxy_clock):
    """Inventories include every same-sandbox role and fail closed on ambiguity before deletion."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    inventory = copy.deepcopy(fake.objects(kind))
    extra = copy.deepcopy(inventory[0])
    if fault == "unknown-role":
        extra.metadata.name += "-unknown"
        extra.metadata.labels[ROLE] = "unknown"
    inventory.append(extra)
    fake.inventories[kind] = inventory
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake, proxy_image=None).get(SID)
    with pytest.raises(SandboxDeleteConflict):
        await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert not mutations(fake, "delete")


@pytest.mark.anyio
@pytest.mark.parametrize("role", CREATE_ROLES)
@pytest.mark.parametrize("committed", [False, True])
async def test_each_post_failure_or_lost_response_rolls_back_original_resources(role, committed, proxy_clock):
    """Every POST stage compensates its accepted original resources without retrying creation."""
    fake = FakeProxyClient()
    if not committed:
        fake.no_commit.add(role)
    fake.faults[("create", role)] = TimeoutError("private transport diagnostics") if committed else client.ApiException(status=422)
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(authenticated_spec())
    expected = CreationResourceState.UNCHANGED if role == "network-policy" and not committed else CreationResourceState.ROLLED_BACK
    assert captured.value.creation_failure.resource_state is expected
    assert not fake.objects()
    assert mutations(fake, "create").count(role) == 1
    assert len(mutations(fake, "create")) == len(set(mutations(fake, "create")))
    assert "private" not in repr(captured.value)
    assert "Q" * 43 not in repr(captured.value)
    assert fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("role", CREATE_ROLES)
async def test_repeated_cancellation_at_each_post_waits_for_original_uid_cleanup(role, proxy_clock):
    """Cancellation settles its blocked worker and reverse cleanup before releasing the caller."""
    fake = FakeProxyClient()
    entered, release = threading.Event(), threading.Event()
    fake.barriers[("create", role)] = (entered, release)
    operation = asyncio.create_task(runtime(fake).create(authenticated_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        operation.cancel()
        await asyncio.sleep(0)
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done() and not fake.closed.is_set()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert not fake.objects() and fake.closed.is_set()
    assert mutations(fake, "create").count(role) == 1


@pytest.mark.anyio
@pytest.mark.parametrize("startup", ["failed", "pending", "image"])
async def test_proxy_startup_failure_compensates_both_pods_and_companions(startup, proxy_clock):
    """A failed or bounded-unusable proxy never leaves a successful Agent chain behind."""
    fake = FakeProxyClient()
    fake.startup["proxy"] = startup
    with pytest.raises(SandboxCreateFailed) as captured:
        await runtime(fake).create(authenticated_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    assert not fake.objects()
    deleted = mutations(fake, "delete")
    assert set(deleted[:2]) == {"container", "proxy"}
    assert all(role not in {"container", "proxy"} for role in deleted[2:])


@pytest.mark.anyio
@pytest.mark.parametrize("role", ["container", "proxy"])
async def test_retained_pod_keeps_all_remaining_dependencies_for_later_cleanup(role, proxy_clock):
    """Failed disappearance confirmation preserves Secret, Service and policy isolation resources."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    fake.stuck.add(role)
    with pytest.raises(SandboxDeleteFailed):
        await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert fake.by_role(role) is not None
    assert fake.secret is not None and fake.services.service is not None
    assert len(fake.objects("network_policy")) == 3
    assert set(mutations(fake, "delete")) <= {"container", "proxy"}
    fake.stuck.clear()
    await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert not fake.objects()


@pytest.mark.anyio
@pytest.mark.parametrize("missing", ["container", "proxy", "proxy-access", "agent-service", "agent-proxy-ingress"])
async def test_fresh_unconfigured_runtime_deletes_partial_recorded_chain_idempotently(missing, proxy_clock):
    """Persisted layout/UID annotations permit restart cleanup after any one resource already disappeared."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    resource = fake.by_role(missing)
    kind = next(kind for kind in ("pod", "secret", "service", "network_policy") if resource in fake.objects(kind))
    fake._store(kind, None, role=missing)
    await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert not fake.objects()
    count = len(mutations(fake, "delete"))
    await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert len(mutations(fake, "delete")) == count


@pytest.mark.anyio
async def test_replacement_uid_prevents_adoption_and_retains_dependencies(proxy_clock):
    """A same-name proxy replacement is never deleted by the original transaction's confirmation loop."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    replacement = copy.deepcopy(fake.proxy_pod)
    replacement.metadata.uid = "25667ab6-3f02-40ba-9b29-b1967503a3f0"
    fake.replacement["proxy"] = replacement
    with pytest.raises(SandboxDeleteConflict):
        await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert fake.proxy_pod.metadata.uid == replacement.metadata.uid
    assert mutations(fake, "delete").count("proxy") == 1
    assert fake.secret is not None and len(fake.objects("network_policy")) == 3


@pytest.mark.anyio
@pytest.mark.parametrize("role", CREATE_ROLES)
async def test_metadata_patch_failure_never_reports_success(role, proxy_clock):
    """Failure to record original UID bindings is a transactional creation failure requiring cleanup."""
    fake = FakeProxyClient()
    fake.faults[("patch", role)] = client.ApiException(status=409)
    with pytest.raises((SandboxCreateFailed, SandboxCleanupFailed)) as captured:
        await runtime(fake).create(authenticated_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    assert not fake.objects()


@pytest.mark.anyio
async def test_recorded_proxy_layout_survives_runtime_restart_and_deletes_all_seven(proxy_clock):
    """A Runtime assembled without proxy creation configuration uses only recorded original identities for cleanup."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    assert (await runtime(fake, proxy_image=None).get(SID)).status == "ready"
    await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    deleted = mutations(fake, "delete")
    assert len(deleted) == 7 and set(deleted) == set(CREATE_ROLES)
    assert set(deleted[:2]) == {"container", "proxy"}
    assert not fake.objects()
    assert all(kwargs["body"].preconditions.uid in ROLE_UIDS.values()
               for method, _, _, kwargs in fake.requests if method == "delete")


@pytest.mark.anyio
@pytest.mark.parametrize("role", ["container", "proxy"])
async def test_rollback_with_retained_pod_preserves_all_supporting_resources(role, proxy_clock):
    """Creation rollback cannot remove credential or isolation dependencies while either original Pod remains."""
    fake = FakeProxyClient()
    fake.startup["proxy"] = "failed"
    fake.stuck.add(role)
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(authenticated_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert fake.by_role(role) is not None
    assert fake.secret is not None and fake.services.service is not None
    assert len(fake.objects("network_policy")) == 3
    assert set(mutations(fake, "delete")) <= {"proxy", "container"}


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["bad-json", "unknown-key", "duplicate-key", "noncanonical-uid", "self-binding", "version"])
async def test_surviving_receipt_tampering_blocks_query_and_deletion_before_mutation(fault, proxy_clock):
    """A fresh unconfigured Runtime treats every surviving receipt as an integrity boundary."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    annotations = fake.secret.metadata.annotations
    record = json.loads(annotations[RECORD])
    if fault == "bad-json":
        annotations[RECORD] = "{"
    elif fault == "unknown-key":
        record["other"] = ROLE_UIDS["proxy"]
        annotations[RECORD] = json.dumps(record)
    elif fault == "duplicate-key":
        annotations[RECORD] = annotations[RECORD][:-1] + ',"secret":"' + ROLE_UIDS["proxy-access"] + '"}'
    elif fault == "noncanonical-uid":
        record["secret"] = ROLE_UIDS["proxy-access"].upper()
        annotations[RECORD] = json.dumps(record)
    elif fault == "self-binding":
        record["secret"] = ROLE_UIDS["proxy"]
        annotations[RECORD] = json.dumps(record)
    else:
        annotations[VERSION] = "unknown"
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake, proxy_image=None).get(SID)
    with pytest.raises(SandboxDeleteConflict):
        await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert not mutations(fake, "delete")


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ["pod", "network_policy", "secret", "service"])
async def test_proxy_inventory_pagination_never_authorizes_deletion(kind, proxy_clock):
    """Every resource kind keeps the bounded single-page discovery rule for query and cleanup."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    fake.continuations[kind] = "another-page"
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake, proxy_image=None).get(SID)
    with pytest.raises(SandboxDeleteConflict):
        await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert not mutations(fake, "delete")


@pytest.mark.anyio
@pytest.mark.parametrize("state,expected", [("pending", "starting"), ("failed", "failed")])
async def test_query_uses_current_proxy_startup_and_failure_state(state, expected, proxy_clock):
    """A healthy Agent does not conceal a proxy that later becomes pending or fails."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    fake.startup["proxy"] = state
    assert (await runtime(fake, proxy_image=None).get(SID)).status == expected
    assert not mutations(fake, "delete")


def test_fake_exact_name_selectors_and_preconditions_are_enforced():
    """The transport fake cannot conceal roles or accept stale identity guards that a real API would reject."""
    from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
    fake = FakeProxyClient()
    pod = build_pod_spec(authenticated_spec(), namespace=NAMESPACE, agent_image="agent-sandbox-basic:dev")
    fake.create_namespaced_pod(NAMESPACE, body=pod, _request_timeout=(0.5, 0.5))
    selector = PREFIX + "sandbox-id=" + SID
    page = fake.list_namespaced_pod(NAMESPACE, label_selector=selector,
                                    field_selector="metadata.name=" + pod.metadata.name, limit=3, watch=False)
    assert len(page.items) == 1
    assert not fake.list_namespaced_pod(NAMESPACE, label_selector=selector,
                                        field_selector="metadata.name=other", limit=3, watch=False).items
    with pytest.raises(client.ApiException) as missing:
        fake.read_namespaced_pod("other", NAMESPACE)
    assert missing.value.status == 404
    with pytest.raises(client.ApiException) as stale_delete:
        fake.delete_namespaced_pod(pod.metadata.name, NAMESPACE, body=client.V1DeleteOptions(
            preconditions=client.V1Preconditions(uid=ROLE_UIDS["container"], resource_version="stale")))
    assert stale_delete.value.status == 409 and fake.pod is not None
    with pytest.raises(client.ApiException) as stale_patch:
        fake.patch_namespaced_pod(pod.metadata.name, NAMESPACE, body=[
            {"op": "test", "path": "/metadata/uid", "value": ROLE_UIDS["container"]},
            {"op": "test", "path": "/metadata/resourceVersion", "value": "stale"},
            {"op": "add", "path": "/metadata/annotations/test", "value": "unexpected"},
        ])
    assert stale_patch.value.status == 409 and "test" not in fake.pod.metadata.annotations
