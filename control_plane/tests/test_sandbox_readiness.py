"""Read-only instance readiness orchestration and HTTP deadline tests."""

import asyncio
from dataclasses import replace
from unittest.mock import AsyncMock

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.errors import (
    DockerUnavailable, SandboxAgentUnavailable, SandboxEndpointUnavailable,
    SandboxMetadataConflict, SandboxMetadataUnavailable, SandboxNotFound,
    SandboxNotReady, SandboxQueryFailed,
    SandboxReadinessTimeout,
)
from control_plane.core.models import SandboxEndpoint, SandboxLifecycleRecord
from control_plane.core.service import SandboxService
from control_plane.runtime.agent_http_probe import HTTPAgentHealthProbe
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_sandbox_service import CREATED_AT, EXPIRES_AT, FIXED_SANDBOX_ID as SID
from test_sandbox_service import FIXED_TRANSACTION_ID as TX, FakeRuntime
from test_agent_http_probe import ProbeStream, ProbeTransport


@pytest.fixture
async def setup_readiness(tmp_path):
    """Supply a real temporary lifecycle row with matching ready runtime observations."""
    store = SQLiteLifecycleStore(tmp_path / "readiness.sqlite3")
    await store.initialize()
    record = SandboxLifecycleRecord(SID, TX, CREATED_AT, EXPIRES_AT)
    await store.create(record)
    record = await store.activate(SID, TX, expected_version=0)
    runtime = FakeRuntime()
    runtime.query_result = replace(
        runtime.query_result, status="ready", transaction_id=TX,
        created_at=CREATED_AT, expires_at=EXPIRES_AT,
    )
    runtime.get_endpoint = AsyncMock(return_value=SandboxEndpoint(SID, "http://127.0.0.1:49153"))
    probe = AsyncMock()
    service = SandboxService(runtime, store, agent_probe=probe)
    return store, record, runtime, probe, service


@pytest.mark.anyio
async def test_ready_check_preserves_record_and_uses_verified_entry(setup_readiness):
    """A successful check confirms the link without changing persistent or runtime data."""
    store, record, runtime, probe, service = setup_readiness
    before = runtime.query_result
    result = await service.check_ready(SID)
    assert (result.sandbox_id, result.status) == (SID, "ready")
    probe.check.assert_awaited_once_with("http://127.0.0.1:49153")
    runtime.get_endpoint.assert_awaited_once_with(SID)
    assert await store.get(SID) == record
    assert runtime.query_result == before
    assert runtime.specs == runtime.deletions == []


@pytest.mark.anyio
@pytest.mark.parametrize("status", ["starting", "running", "stopped", "failed"])
async def test_unready_does_not_query_endpoint_or_agent(setup_readiness, status):
    """Container health gates both entry lookup and HTTP probing."""
    store, record, runtime, probe, service = setup_readiness
    runtime.query_result = replace(runtime.query_result, status=status)
    with pytest.raises(SandboxNotReady):
        await service.check_ready(SID)
    runtime.get_endpoint.assert_not_awaited()
    probe.check.assert_not_awaited()
    assert await store.get(SID) == record


@pytest.mark.anyio
@pytest.mark.parametrize("phase,error,expected", [
    ("get", SandboxNotFound(), SandboxNotFound),
    ("get", DockerUnavailable(), DockerUnavailable),
    ("get", SandboxQueryFailed(), SandboxQueryFailed),
    ("endpoint", SandboxNotFound(), SandboxNotFound),
    ("endpoint", DockerUnavailable(), SandboxEndpointUnavailable),
    ("endpoint", SandboxQueryFailed(), SandboxEndpointUnavailable),
    ("endpoint", SandboxEndpointUnavailable(), SandboxEndpointUnavailable),
])
async def test_query_failures_prevent_probe(setup_readiness, phase, error, expected):
    """Runtime errors preserve their phase's public policy and never expose an unchecked target."""
    store, record, runtime, probe, service = setup_readiness
    if phase == "get":
        runtime.get = AsyncMock(side_effect=error)
    else:
        runtime.get_endpoint.side_effect = error
    with pytest.raises(expected):
        await service.check_ready(SID)
    probe.check.assert_not_awaited()
    assert await store.get(SID) == record


@pytest.mark.anyio
@pytest.mark.parametrize("mismatch", ["transaction", "created_at", "endpoint_id", "observed_id"])
async def test_identity_mismatches_prevent_http(setup_readiness, mismatch):
    """Every verified identity must match before any HTTP request is allowed."""
    store, record, runtime, probe, service = setup_readiness
    expected = SandboxMetadataConflict
    if mismatch == "transaction":
        runtime.query_result = replace(runtime.query_result, transaction_id="different")
    elif mismatch == "created_at":
        runtime.query_result = replace(runtime.query_result, created_at=EXPIRES_AT)
    elif mismatch == "observed_id":
        runtime.query_result = replace(runtime.query_result, sandbox_id="different")
    else:
        runtime.get_endpoint.return_value = SandboxEndpoint("different", "http://127.0.0.1:49153")
        expected = SandboxEndpointUnavailable
    with pytest.raises(expected):
        await service.check_ready(SID)
    probe.check.assert_not_awaited()
    assert await store.get(SID) == record


@pytest.mark.anyio
async def test_store_failure_prevents_probe(setup_readiness):
    """Unavailable metadata cannot silently fall back to an active HTTP check."""
    store, _, runtime, probe, service = setup_readiness
    store.get = AsyncMock(side_effect=RuntimeError("private SQL /path"))
    with pytest.raises(SandboxMetadataUnavailable):
        await service.check_ready(SID)
    runtime.get_endpoint.assert_not_awaited()
    probe.check.assert_not_awaited()


@pytest.mark.anyio
async def test_legacy_ready_instance_is_not_adopted(setup_readiness):
    """An unmanaged ready instance can be probed without inserting lifecycle metadata."""
    store, record, _, _, service = setup_readiness
    await store.delete(SID, TX, expected_version=record.version)
    assert (await service.check_ready(SID)).status == "ready"
    assert await store.get(SID) is None


@pytest.mark.anyio
async def test_invalid_id_does_not_touch_dependencies(setup_readiness):
    """Invalid sandbox identifiers are rejected before runtime, metadata or HTTP lookup."""
    store, _, runtime, probe, service = setup_readiness
    store.get = AsyncMock()
    with pytest.raises(SandboxNotFound):
        await service.check_ready("invalid")
    assert runtime.queries == []
    store.get.assert_not_awaited()
    probe.check.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("error,status,code", [
    (SandboxNotReady("secret"), 503, "sandbox_not_ready"),
    (SandboxAgentUnavailable("secret"), 503, "sandbox_agent_unavailable"),
    (SandboxReadinessTimeout("secret"), 504, "sandbox_readiness_timeout"),
    (SandboxNotFound("secret"), 404, "sandbox_not_found"),
    (SandboxMetadataConflict(SID), 409, "sandbox_metadata_conflict"),
    (SandboxMetadataUnavailable(SID), 503, "sandbox_metadata_unavailable"),
    (DockerUnavailable("secret"), 503, "docker_unavailable"),
    (SandboxQueryFailed("secret"), 500, "sandbox_query_failed"),
    (SandboxEndpointUnavailable("secret"), 503, "sandbox_endpoint_unavailable"),
])
async def test_ready_api_uses_centralized_sanitized_errors(setup_readiness, error, status, code):
    """The new route uses fixed domain mappings without exposing exception messages."""
    store, _, runtime, probe, _ = setup_readiness
    app = create_app(runtime, store=store, agent_probe=probe, reaper_enabled=False)
    app.state.service.check_ready = AsyncMock(side_effect=error)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/v1/sandboxes/{SID}/readyz")
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert "secret" not in response.text


@pytest.mark.anyio
async def test_ready_api_success_and_root_probe_independence(setup_readiness):
    """Instance readiness returns the small contract while root checks remain Docker-only."""
    store, record, runtime, probe, _ = setup_readiness
    app = create_app(runtime, store=store, agent_probe=probe, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/healthz")).status_code == 200
        assert (await client.get("/readyz")).status_code == 200
        probe.check.assert_not_awaited()
        response = await client.get(f"/v1/sandboxes/{SID}/readyz?url=http://evil.example")
    assert response.status_code == 200
    assert response.json() == {"sandbox_id": SID, "status": "ready"}
    probe.check.assert_awaited_once_with("http://127.0.0.1:49153")
    assert await store.get(SID) == record


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["get", "endpoint", "probe"])
async def test_whole_ready_deadline_cancels_current_read(setup_readiness, phase):
    """The API deadline bounds runtime queries and HTTP work together, releasing active work."""
    store, _, runtime, probe, _ = setup_readiness
    entered, cancelled = asyncio.Event(), asyncio.Event()

    async def block(*args):
        """Keep one read in flight until the overall deadline cancels it."""
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    if phase == "get":
        runtime.get = block
    elif phase == "endpoint":
        runtime.get_endpoint = block
    else:
        probe.check.side_effect = block
    app = create_app(runtime, store=store, agent_probe=probe, sandbox_query_timeout=0.05, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/v1/sandboxes/{SID}/readyz")
    assert entered.is_set() and cancelled.is_set()
    assert response.status_code == 504
    assert response.json()["error"]["code"] == "sandbox_readiness_timeout"


@pytest.mark.anyio
@pytest.mark.parametrize("api_timeout,probe_timeout,status,code", [
    (1, 0.02, 503, "sandbox_agent_unavailable"),
    (0.05, 1, 504, "sandbox_readiness_timeout"),
])
async def test_network_and_api_deadlines_close_http_resources(
    setup_readiness, api_timeout, probe_timeout, status, code,
):
    """Probe and API timeouts have distinct responses and both cancel and close the HTTP stream."""
    store, record, runtime, _, _ = setup_readiness
    stream = ProbeStream(block=True)
    transport = ProbeTransport(stream)
    probe = HTTPAgentHealthProbe(timeout=probe_timeout, transport=transport)
    app = create_app(runtime, store=store, agent_probe=probe, sandbox_query_timeout=api_timeout, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/v1/sandboxes/{SID}/readyz")
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert stream.closed and stream.cancelled and transport.closed
    assert len(transport.requests) == 1
    assert await store.get(SID) == record


@pytest.mark.anyio
async def test_request_cancellation_settles_http_shutdown(setup_readiness):
    """Repeated caller cancellation reaches HTTP reads and leaves no unfinished probe task."""
    store, record, runtime, _, _ = setup_readiness
    stream = ProbeStream(block=True, gate_close=True)
    transport = ProbeTransport(stream)
    app = create_app(
        runtime, store=store, reaper_enabled=False,
        agent_probe=HTTPAgentHealthProbe(timeout=5, transport=transport),
        sandbox_query_timeout=10,
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        before = asyncio.all_tasks()
        request = asyncio.create_task(client.get(f"/v1/sandboxes/{SID}/readyz"))
        try:
            await asyncio.wait_for(stream.entered.wait(), 2)
            request.cancel()
            await asyncio.wait_for(stream.closing.wait(), 2)
            request.cancel()
            await asyncio.sleep(0)
            assert not request.done() and stream.cancelled
            stream.release_close.set()
            with pytest.raises(asyncio.CancelledError):
                await request
            assert transport.closed and stream.closed
            assert asyncio.all_tasks() == before
        finally:
            stream.release_close.set()
            await asyncio.gather(request, return_exceptions=True)
    assert await store.get(SID) == record


def test_assembly_does_not_open_http_clients(tmp_path, monkeypatch):
    """Constructing a probe or application keeps the default HTTP dependency lazy."""
    def forbidden(*args, **kwargs):
        """Reject client construction during app assembly rather than a request."""
        pytest.fail("HTTP client opened during assembly")

    monkeypatch.setattr(httpx, "AsyncClient", forbidden)
    HTTPAgentHealthProbe()
    create_app(FakeRuntime(), database_path=tmp_path / "untouched" / "db.sqlite3")
    assert not (tmp_path / "untouched").exists()


@pytest.mark.anyio
async def test_expired_api_deadline_wins_over_http_close_failure(setup_readiness):
    """Cleanup failure after API cancellation still returns the complete-check timeout policy."""
    store, _, runtime, _, _ = setup_readiness
    stream = ProbeStream(block=True)
    stream.aclose = AsyncMock(side_effect=RuntimeError("private socket detail"))
    transport = ProbeTransport(stream)
    app = create_app(
        runtime, store=store, reaper_enabled=False, sandbox_query_timeout=0.05,
        agent_probe=HTTPAgentHealthProbe(timeout=1, transport=transport),
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get(f"/v1/sandboxes/{SID}/readyz")
    assert response.status_code == 504
    assert response.json()["error"]["code"] == "sandbox_readiness_timeout"
    assert "private" not in response.text
    assert stream.cancelled and transport.closed
