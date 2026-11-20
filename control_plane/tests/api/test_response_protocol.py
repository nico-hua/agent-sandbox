"""Verify the unified JSON protocol at FastAPI's outer HTTP boundary."""

import httpx
import pytest

from control_plane.app import create_app
from control_plane.domain.errors import SandboxNotFound
from control_plane.tests.fakes import APIRuntime, FakeLifecycleStore


@pytest.mark.anyio
async def test_framework_errors_and_health_share_envelope():
    """Cover health, routing, validation and method errors through the real app."""
    app = create_app(APIRuntime(), store=FakeLifecycleStore())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        cases = (
            ("GET", "/healthz", 200, "ok", {"status": "ok"}),
            ("GET", "/healthz/", 404, "route not found", None),
            ("GET", "/unknown", 404, "route not found", None),
            ("GET", "/v1/sandboxes/invalid", 400, "invalid sandbox ID", None),
            ("GET", "/v1/workspaces/invalid", 400, "invalid workspace ID", None),
            ("POST", "/healthz", 405, "method not allowed", None),
            ("DELETE", "/v1/sandboxes/sbx_123?delete_workspace=bad", 400, "invalid request", None),
        )
        for method, path, status, message, data in cases:
            response = await client.request(method, path)
            assert response.status_code == status
            payload = response.json()
            assert type(payload["code"]) is int
            assert payload == {"code": status, "message": message, "data": data}
            if status == 405:
                assert response.headers["Allow"] == "GET"
        for path in ("/v1/sandboxes/invalid/endpoint", "/v1/sandboxes/invalid/services/3000/endpoint"):
            response = await client.get(path)
            assert response.status_code == 400
            assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.anyio
@pytest.mark.parametrize(("path", "allowed", "cache_control"), [
    ("/v1/sandboxes", "GET, POST", None),
    ("/v1/sandboxes/sbx_" + "a" * 32, "DELETE, GET", None),
    ("/v1/workspaces/wsp_" + "a" * 32, "DELETE, GET", None),
    ("/healthz", "GET", None),
    ("/v1/sandboxes/sbx_" + "a" * 32 + "/endpoint", "GET", "no-store"),
    ("/v1/sandboxes/sbx_" + "a" * 32 + "/services/3000/endpoint", "GET", "no-store"),
])
async def test_method_not_allowed_lists_every_registered_method(path, allowed, cache_control):
    """Check the union of matched methods and preserve credential response headers."""
    app = create_app(APIRuntime(), store=FakeLifecycleStore(), auth_enabled=True, api_key="test-key")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.put(path, headers={"Authorization": "Bearer test-key"})

    assert response.status_code == 405
    assert response.json() == {"code": 405, "message": "method not allowed", "data": None}
    assert response.headers["Allow"] == allowed
    assert response.headers.get("Cache-Control") == cache_control


@pytest.mark.anyio
async def test_unexpected_error_has_safe_500_envelope(monkeypatch):
    """Unexpected service exceptions keep their diagnostics out of the wire response."""
    app = create_app(APIRuntime(), store=FakeLifecycleStore())

    async def fail(_sandbox_id):
        """Raise a representative internal exception containing private data."""
        raise RuntimeError("private credential and stack context")

    monkeypatch.setattr(app.state.service, "get_endpoint", fail)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/v1/sandboxes/sbx_" + "a" * 32 + "/endpoint")
    assert response.status_code == 500
    assert response.json() == {"code": 500, "message": "internal server error", "data": None}
    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.anyio
async def test_canonical_unknown_resources_return_404(tmp_path):
    """Canonical IDs go to the Runtime or Store and report a missing resource."""
    app = create_app(
        APIRuntime(SandboxNotFound()), database_path=tmp_path / "protocol.sqlite3",
        reaper_enabled=False,
    )
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            missing_sandbox = await client.get("/v1/sandboxes/sbx_" + "a" * 32 + "/services/3000/endpoint")
            missing_workspace = await client.get("/v1/workspaces/wsp_" + "a" * 32)
    assert missing_sandbox.status_code == missing_workspace.status_code == 404
    assert missing_sandbox.json() == {"code": 404, "message": "sandbox not found", "data": None}
    assert missing_workspace.json() == {"code": 404, "message": "workspace not found", "data": None}
