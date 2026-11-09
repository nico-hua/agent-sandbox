"""Offline Runtime v2 Service identity, historical layout and partial cleanup contracts."""

import copy
import json

import pytest
from kubernetes import client

from control_plane.core.errors import (
    CreationResourceState, SandboxCleanupFailed, SandboxDeleteConflict,
    SandboxQueryFailed,
)
from control_plane.runtime.kubernetes.agent_service import AgentServiceResource
from control_plane.runtime.kubernetes.proxy_resources import _record
from kubernetes_proxy_fakes import FakeProxyClient, ROLE, ROLE_UIDS
from test_kubernetes_proxy_lifecycle import (
    RECORD, VERSION, authenticated_spec, mutations, proxy_clock, runtime,
)
from test_kubernetes_query import NAMESPACE, SID, TXN


def historical_seven(fake):
    """Represent an actual v1 persisted layout without inventing a production downgrade path."""
    fake.proxy_service = None
    for item in fake.objects():
        annotations = item.metadata.annotations
        annotations[VERSION] = "1"
        record = json.loads(annotations[RECORD])
        record.pop("proxy_service")
        annotations[RECORD] = json.dumps(record, separators=(",", ":"), sort_keys=True)


@pytest.mark.anyio
async def test_historical_seven_resource_layout_remains_queryable_and_deletable(proxy_clock):
    """An unconfigured Runtime observes and deletes v1 without building a missing eighth resource."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    historical_seven(fake)
    fake.trace.clear()
    assert (await runtime(fake, proxy_image=None).get(SID)).status == "ready"
    assert not mutations(fake, "create") and fake.proxy_service is None
    await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert not fake.objects()
    assert len(mutations(fake, "delete")) == 7
    await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert len(mutations(fake, "delete")) == 7


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["missing", "uid", "selector", "port", "unready", "external", "time"])
async def test_proxy_service_integrity_failure_prevents_ready(fault, proxy_clock):
    """A running pair of Pods cannot conceal missing, replaced or misrouted original Service state."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    service = fake.proxy_service
    if fault == "missing":
        fake.proxy_service = None
    elif fault == "uid":
        service.metadata.uid = ROLE_UIDS["proxy"]
    elif fault == "selector":
        service.spec.selector[ROLE] = "container"
    elif fault == "port":
        service.spec.ports[0].target_port = 8081
    elif fault == "unready":
        service.spec.publish_not_ready_addresses = True
    elif fault == "external":
        service.spec.external_ips = ["192.0.2.1"]
    else:
        service.metadata.annotations["io.agent-sandbox.expires-at"] = "2035-01-01T00:00:00Z"
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake, proxy_image=None).get(SID)
    if fault == "uid":
        with pytest.raises(SandboxDeleteConflict):
            await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
        assert not mutations(fake, "delete")
    else:
        # Cleanup ownership deliberately does not require healthy routing.
        await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
        assert not fake.objects()


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["missing-key", "mixed-version", "old-with-new-key", "missing-version"])
async def test_layout_records_cannot_silently_downgrade(fault, proxy_clock):
    """The declared version and every receipt remain mutually consistent across both Service roles."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    if fault == "missing-key":
        for item in fake.objects():
            record = json.loads(item.metadata.annotations[RECORD])
            record.pop("proxy_service")
            item.metadata.annotations[RECORD] = json.dumps(record)
    elif fault == "mixed-version":
        fake.proxy_service.metadata.annotations[VERSION] = "1"
    elif fault == "old-with-new-key":
        for item in fake.objects():
            item.metadata.annotations[VERSION] = "1"
    else:
        fake.proxy_service.metadata.annotations.pop(VERSION)
    with pytest.raises(SandboxQueryFailed):
        await runtime(fake, proxy_image=None).get(SID)
    with pytest.raises(SandboxDeleteConflict):
        await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert not mutations(fake, "delete")


@pytest.mark.anyio
async def test_proxy_service_only_survivor_carries_complete_cleanup_authority(proxy_clock):
    """The original Service receipt suffices after both Pods and all other owned resources disappeared."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    survivor = fake.proxy_service
    for kind in ("pod", "network_policy", "secret", "service"):
        for item in list(fake.objects(kind)):
            if item is not survivor:
                fake._store(kind, None, role=item.metadata.labels[ROLE])
    fake.trace.clear()
    await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert mutations(fake, "delete") == ["proxy-service"] and not fake.objects()
    await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert mutations(fake, "delete") == ["proxy-service"]


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["duplicate", "unknown", "transaction"])
async def test_agent_service_discovery_checks_closed_peer_role_set(fault, proxy_clock):
    """Shared Service discovery validates both named roles instead of filtering conflicting peers away."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    if fault == "transaction":
        fake.proxy_service.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    else:
        extra = copy.deepcopy(fake.proxy_service)
        if fault == "unknown":
            extra.metadata.name += "-unknown"
            extra.metadata.labels[ROLE] = "unknown"
        fake.inventories["service"] = [*fake.objects("service"), extra]
    with pytest.raises(SandboxDeleteConflict):
        AgentServiceResource(fake, SID, NAMESPACE, TXN).lookup()
    assert not mutations(fake, "delete")


@pytest.mark.anyio
async def test_service_record_publication_failure_retains_anchor_for_retry(proxy_clock):
    """Another persisted receipt protects the original Service when its PATCH and Pod cleanup fail."""
    fake = FakeProxyClient()
    fake.faults[("patch", "proxy-service")] = client.ApiException(status=409)
    fake.stuck.add("container")
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(authenticated_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert fake.proxy_service is not None and fake.proxy_pod is None
    assert RECORD not in fake.proxy_service.metadata.annotations
    assert json.loads(fake.pod.metadata.annotations[RECORD])["proxy_service"] == fake.proxy_service.metadata.uid
    fake.faults.clear()
    fake.stuck.clear()
    await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert not fake.objects()


@pytest.mark.anyio
async def test_service_post_unconfirmed_absence_preserves_unknown_evidence(proxy_clock):
    """A lost response and repeated absence cannot prove that the Service POST never committed."""
    fake = FakeProxyClient()
    fake.no_commit.add("proxy-service")
    fake.faults[("create", "proxy-service")] = TimeoutError("private transport detail")
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(authenticated_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert mutations(fake, "create").count("proxy-service") == 1
    assert "proxy" not in mutations(fake, "create")
    assert "private" not in str(captured.value)


@pytest.mark.anyio
async def test_service_cleanup_timeout_preserves_remaining_evidence(proxy_clock):
    """A retained original eighth resource prevents a false complete rollback result."""
    fake = FakeProxyClient()
    fake.startup["proxy"] = "failed"
    fake.stuck.add("proxy-service")
    with pytest.raises(SandboxCleanupFailed) as captured:
        await runtime(fake).create(authenticated_spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert fake.proxy_service is not None and not fake.objects("pod")
    deleted = mutations(fake, "delete")
    assert deleted.index("proxy-service") > max(deleted.index("container"), deleted.index("proxy"))
    fake.stuck.clear()
    await runtime(fake, proxy_image=None).delete(SID, expected_transaction_id=TXN)
    assert not fake.objects()


@pytest.mark.anyio
async def test_receipt_bounds_and_keys_follow_declared_version(proxy_clock):
    """Versioned receipt parsers permit eight canonical keys only in v2 and keep bounded input lengths."""
    fake = FakeProxyClient()
    await runtime(fake).create(authenticated_spec())
    metadata = copy.deepcopy(fake.proxy_service.metadata)
    assert len(_record(metadata)) == 8
    raw = metadata.annotations[RECORD]
    metadata.annotations[RECORD] = raw + " " * (769 - len(raw))
    with pytest.raises(SandboxDeleteConflict):
        _record(metadata)
    metadata.annotations[RECORD] = raw
    metadata.annotations[VERSION] = "1"
    with pytest.raises(SandboxDeleteConflict):
        _record(metadata)
    historical_seven(fake)
    assert len(_record(fake.pod.metadata)) == 7
    raw = fake.pod.metadata.annotations[RECORD]
    fake.pod.metadata.annotations[RECORD] = raw + " " * (513 - len(raw))
    with pytest.raises(SandboxDeleteConflict):
        _record(fake.pod.metadata)


@pytest.mark.parametrize("response", [{"status": "Success"}, client.V1Status(status="Success")])
def test_integration_recorder_accepts_secret_delete_status_response(response):
    """Secret DELETE may return a mapping rather than a SecretList; diagnostic redaction must accept both."""
    from types import SimpleNamespace
    from test_kubernetes_proxy_lifecycle_integration import _RecordingFacade

    def delete(*args, **kwargs):
        """Return the official client's alternate DELETE response shape without any live transport."""
        return response

    facade = _RecordingFacade(SimpleNamespace(delete_namespaced_secret=delete), None, None, networking=True)
    assert facade.delete_namespaced_secret("owned", NAMESPACE) is response
