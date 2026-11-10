"""Existing-session endpoint facts, credential delivery and matching transaction deletion."""

import asyncio
import hashlib
from dataclasses import replace

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.errors import (
    SandboxDeleteConflict, SandboxDeleteFailed, SandboxEndpointAuthUnavailable,
    SandboxEndpointUnavailable, SandboxMetadataConflict, SandboxNotFound, KubernetesOperationUnsupported,
)
from control_plane.core.models import SandboxAccessCredential, SandboxLifecycleRecord
from control_plane.core.service import SandboxService
from kubernetes_proxy_fakes import FakeProxyClient
from lifecycle_fakes import FakeLifecycleStore
from test_kubernetes_local_entry_manager import manager_harness
from test_kubernetes_proxy_lifecycle import authenticated_spec, proxy_clock, runtime
from test_kubernetes_query import CREATED, EXPIRES, SID, TXN


@pytest.fixture
def endpoint_harness(manager_harness):
    """Attach a real manager with controlled sessions to a real Kubernetes Runtime using only fake SDK I/O."""
    h = manager_harness
    h.api = FakeProxyClient()
    h.backend = runtime(h.api)
    h.manager = h.make()
    return h


@pytest.mark.anyio
async def test_runtime_returns_existing_protected_origin_and_nonsecret_facts(endpoint_harness, proxy_clock):
    """No plaintext credential or unsupported application-service capability leaves the Runtime."""
    h = endpoint_harness
    await h.backend.create(authenticated_spec())
    h.backend.bind_local_entry_manager(h.manager)
    await h.manager.open(SID)
    endpoint = await h.backend.get_endpoint(SID)
    assert endpoint.url == "http://127.0.0.1:43123"
    assert endpoint.auth_required is True and endpoint.auth_version == "1"
    assert endpoint.transaction_id == TXN and endpoint.created_at == CREATED
    assert endpoint.credential_digest == hashlib.sha256(b"Q" * 43).hexdigest()
    assert endpoint.access_token is None and endpoint.services_version is None
    assert len(h.sessions) == 1
    await h.manager.close_all()


@pytest.mark.anyio
async def test_missing_or_unopened_manager_never_creates_client_or_session(endpoint_harness):
    """GET cannot turn an unconfigured or unopened local entry into an implicit forward."""
    h = endpoint_harness
    with pytest.raises(SandboxEndpointUnavailable):
        await h.backend.get_endpoint(SID)
    h.backend.bind_local_entry_manager(h.manager)
    with pytest.raises(SandboxEndpointUnavailable):
        await h.backend.get_endpoint(SID)
    assert not h.sessions and not h.resolves and not h.api.events


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["transaction", "digest", "uid", "dead", "closing"])
async def test_endpoint_rechecks_current_target_and_session_before_delivery(fault, endpoint_harness):
    """Neither a cached process nor the periodic monitor can replace per-request identity verification."""
    h = endpoint_harness
    h.backend.bind_local_entry_manager(h.manager)
    await h.manager.open(SID)
    if fault == "transaction":
        h.targets[SID] = replace(h.targets[SID], transaction_id="txn_" + "f" * 32)
    elif fault == "digest":
        h.targets[SID] = replace(h.targets[SID], credential_digest="0" * 64)
    elif fault == "uid":
        records = dict(h.targets[SID].resource_uids)
        records["proxy"] = "a5667ab6-3f02-40ba-9b29-b1967503a3f0"
        h.targets[SID] = replace(h.targets[SID], proxy_pod_uid=records["proxy"], resource_uids=tuple(sorted(records.items())))
    elif fault == "dead":
        h.sessions[0].is_active = False
    else:
        h.close_gate.clear()
        closing = asyncio.create_task(h.manager.close(SID))
        await h.closing.wait()
    try:
        with pytest.raises(SandboxEndpointUnavailable):
            await h.backend.get_endpoint(SID)
        assert len(h.sessions) == 1
    finally:
        h.close_gate.set()
        if fault == "closing":
            await closing
        await h.manager.close_all()


@pytest.mark.anyio
async def test_service_adds_only_matching_persisted_credential(endpoint_harness, proxy_clock):
    """The existing Service remains the only layer returning the stored plaintext Access Token."""
    h = endpoint_harness
    await h.backend.create(authenticated_spec())
    h.backend.bind_local_entry_manager(h.manager)
    await h.manager.open(SID)
    store = FakeLifecycleStore()
    await store.create(SandboxLifecycleRecord(SID, TXN, CREATED, EXPIRES, management_state="active"),
                       credential=SandboxAccessCredential(SID, TXN, "Q" * 43))
    service = SandboxService(h.backend, store)
    endpoint = await service.get_endpoint(SID)
    assert endpoint.access_token == "Q" * 43 and "Q" * 43 not in repr(endpoint)
    store.credentials[SID] = SandboxAccessCredential(SID, TXN, "R" * 43)
    with pytest.raises(SandboxEndpointAuthUnavailable):
        await service.get_endpoint(SID)
    store.records[SID] = replace(store.records[SID], transaction_id="txn_" + "f" * 32)
    with pytest.raises(SandboxMetadataConflict):
        await service.get_endpoint(SID)
    await h.manager.close_all()


@pytest.mark.anyio
async def test_http_auth_precedes_lookup_and_protected_delivery_keeps_no_store(endpoint_harness, proxy_clock):
    """Unauthenticated endpoint access cannot resolve targets, consume slots or obtain instance credentials."""
    h = endpoint_harness
    await h.backend.create(authenticated_spec())
    store = FakeLifecycleStore()
    await store.create(SandboxLifecycleRecord(SID, TXN, CREATED, EXPIRES, management_state="active"),
                       credential=SandboxAccessCredential(SID, TXN, "Q" * 43))
    application = create_app(h.backend, store=store, api_key="synthetic-admin-key", reaper_enabled=False,
                            kubernetes_entry_manager=h.manager, kubernetes_initial_sandbox_ids=(SID,))
    async with application.router.lifespan_context(application):
        before = len(h.resolves)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://control") as http:
            for headers in ({}, {"Authorization": "Bearer wrong-admin"}):
                assert (await http.get(f"/v1/sandboxes/{SID}/endpoint", headers=headers)).status_code == 401
            assert len(h.resolves) == before
            response = await http.get(f"/v1/sandboxes/{SID}/endpoint", headers={"Authorization": "Bearer synthetic-admin-key"})
            assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
            assert response.json()["access_token"] == "Q" * 43
            assert "synthetic-admin-key" not in response.text
    assert h.manager.is_closed


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["id", "transaction", "workspace", "wrong-transaction"])
async def test_invalid_delete_cannot_close_another_transaction(fault, endpoint_harness):
    """Deletion argument and transaction rejection must precede any local process shutdown."""
    h = endpoint_harness
    h.backend.bind_local_entry_manager(h.manager)
    await h.manager.open(SID)
    kwargs = {"expected_transaction_id": TXN}
    sandbox_id = SID
    if fault == "id":
        sandbox_id = "../pod"
    elif fault == "transaction":
        kwargs["expected_transaction_id"] = "bad"
    elif fault == "workspace":
        kwargs["expected_workspace"] = object()
    else:
        kwargs["expected_transaction_id"] = "txn_" + "f" * 32
    expected = SandboxNotFound if fault == "id" else KubernetesOperationUnsupported if fault == "workspace" else SandboxDeleteConflict
    with pytest.raises(expected):
        await h.backend.delete(sandbox_id, **kwargs)
    assert not h.sessions[0].close_calls and h.sessions[0].is_active
    assert not h.api.delete_calls
    await h.manager.close_all()


@pytest.mark.anyio
async def test_matching_delete_closes_forward_before_resource_mutation(endpoint_harness, proxy_clock):
    """Both session close and the complete SDK delete remain inside one per-instance exclusion period."""
    h = endpoint_harness
    await h.backend.create(authenticated_spec())
    h.backend.bind_local_entry_manager(h.manager)
    await h.manager.open(SID)
    h.close_gate.clear()
    deletion = asyncio.create_task(h.backend.delete(SID, expected_transaction_id=TXN))
    await h.closing.wait()
    assert not h.api.delete_calls
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    with pytest.raises(LocalEntryError):
        await h.manager.open(SID)
    h.close_gate.set()
    await deletion
    assert h.sessions[0].closed.is_set() and not h.api.objects()
    assert h.manager.session_count == 0
    await h.manager.close_all()


@pytest.mark.anyio
async def test_cleanup_failure_prevents_resource_delete(endpoint_harness, proxy_clock):
    """A retained forwarding process prevents a false clean delete and keeps its slot and handle owned."""
    h = endpoint_harness
    await h.backend.create(authenticated_spec())
    h.backend.bind_local_entry_manager(h.manager)
    await h.manager.open(SID)
    h.close_error = True
    with pytest.raises(SandboxDeleteFailed):
        await h.backend.delete(SID, expected_transaction_id=TXN)
    assert h.manager.session_count == 1 and not h.api.delete_calls
    h.close_error = False
    await h.manager.close_all()


@pytest.mark.anyio
async def test_kubernetes_failure_during_service_recheck_is_fixed_endpoint_error(endpoint_harness, proxy_clock):
    """Backend failure after session lookup must not escape endpoint HTTP handling as an unknown exception."""
    from kubernetes import client
    h = endpoint_harness
    await h.backend.create(authenticated_spec())
    store = FakeLifecycleStore()
    await store.create(SandboxLifecycleRecord(SID, TXN, CREATED, EXPIRES, management_state="active"),
                       credential=SandboxAccessCredential(SID, TXN, "Q" * 43))
    app = create_app(h.backend, store=store, api_key="synthetic-admin-key", reaper_enabled=False,
                     kubernetes_entry_manager=h.manager, kubernetes_initial_sandbox_ids=(SID,))
    async with app.router.lifespan_context(app):
        h.api.faults[("list", "pod")] = client.ApiException(status=403, reason="private backend diagnostic")
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as http:
            response = await http.get(f"/v1/sandboxes/{SID}/endpoint", headers={"Authorization": "Bearer synthetic-admin-key"})
        assert response.status_code == 503 and response.headers["cache-control"] == "no-store"
        assert response.json() == {"error": {"code": "sandbox_endpoint_unavailable", "message": "sandbox endpoint is unavailable"}}
        assert "private" not in response.text


@pytest.mark.anyio
async def test_cancelled_delete_keeps_guard_until_sdk_thread_and_cleanup_finish(endpoint_harness, proxy_clock):
    """HTTP cancellation cannot admit a new forward while the original resource deletion is in flight."""
    import threading
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    h = endpoint_harness
    await h.backend.create(authenticated_spec())
    h.backend.bind_local_entry_manager(h.manager)
    await h.manager.open(SID)
    entered, release = threading.Event(), threading.Event()
    h.api.barriers[("delete", "proxy")] = (entered, release)
    deleting = asyncio.create_task(h.backend.delete(SID, expected_transaction_id=TXN))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        assert h.sessions[0].closed.is_set()
        deleting.cancel()
        await asyncio.sleep(0)
        deleting.cancel()
        await asyncio.sleep(0)
        with pytest.raises(LocalEntryError):
            await h.manager.open(SID)
        with pytest.raises(SandboxEndpointUnavailable):
            await h.backend.get_endpoint(SID)
        assert not deleting.done() and len(h.sessions) == 1
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await deleting
    assert not h.api.objects() and h.manager.session_count == 0
    await h.manager.close_all()
