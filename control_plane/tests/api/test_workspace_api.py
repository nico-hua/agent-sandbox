"""Workspace HTTP contracts through actual SQLite and Service, without Docker."""

from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from control_plane.tests.fakes import create_test_app
from control_plane.tests.http_assertions import assert_failure, success_data
from control_plane.tests.fakes import APIRuntime as FakeRuntime
from control_plane.tests.fakes import WorkspaceAPIRuntime as WorkspaceRuntime


@pytest.mark.anyio
async def test_workspace_round_trip_and_sequential_reuse(tmp_path: Path) -> None:
    """HTTP binding is exclusive, reusable after deletion and separately removable."""
    runtime = WorkspaceRuntime()
    app = create_test_app(runtime, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            created = await client.post("/v1/workspaces", json={})
            assert created.status_code == 200, created.text
            workspace = success_data(created)
            workspace_id = workspace["workspace_id"]
            assert set(workspace) == {"workspace_id", "status", "sandbox_id", "created_at"}
            assert workspace["status"] == "available" and workspace["sandbox_id"] is None
            first = await client.post("/v1/sandboxes", json={"workspace_id": workspace_id})
            assert first.status_code == 200, first.text
            first_id = success_data(first)["sandbox_id"]
            assert success_data(first)["workspace_id"] == workspace_id
            assert success_data(await client.get(f"/v1/sandboxes/{first_id}"))["workspace_id"] == workspace_id
            occupied = success_data(await client.get(f"/v1/workspaces/{workspace_id}"))
            assert occupied["status"] == "occupied" and occupied["sandbox_id"] == first_id
            assert (await client.post("/v1/sandboxes", json={"workspace_id": workspace_id})).status_code == 409
            assert (await client.delete(f"/v1/workspaces/{workspace_id}")).status_code == 409
            protected = await client.delete(f"/v1/sandboxes/{first_id}?delete_workspace=true")
            assert protected.status_code == 409
            assert_failure(protected, "workspace must be deleted separately", {"workspace_id": workspace_id})
            assert (await client.delete(f"/v1/sandboxes/{first_id}")).status_code == 200
            assert success_data(await client.get(f"/v1/workspaces/{workspace_id}"))["status"] == "available"
            second = await client.post("/v1/sandboxes", json={"workspace_id": workspace_id})
            assert second.status_code == 200, second.text
            assert success_data(second)["sandbox_id"] != first_id
            assert (await client.delete(f"/v1/sandboxes/{success_data(second)['sandbox_id']}")).status_code == 200
            assert success_data(await client.delete(f"/v1/workspaces/{workspace_id}")) is None
            missing = await client.get(f"/v1/workspaces/{workspace_id}")
            assert missing.status_code == 404
            assert_failure(missing, "workspace not found")


@pytest.mark.anyio
@pytest.mark.parametrize("body", [
    b"null", b"[]", b'{"environment":null}', b'{"environment":true}',
    b'{"environment":"basic","environment":"python"}', b'{"environment":NaN}',
    b'{"workspace_id":"wsp_x"}', b'{"environment":"../basic"}', b'{"environment":"missing"}',
])
async def test_invalid_workspace_creation_precedes_runtime(tmp_path: Path, body: bytes) -> None:
    """Invalid JSON, environment shape and registration fail before runtime work."""
    runtime = WorkspaceRuntime()
    app = create_test_app(runtime, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            response = await client.post("/v1/workspaces", content=body)
    assert response.status_code == 400, response.text
    assert_failure(response, "invalid workspace creation options")
    assert runtime.workspace_calls == []


@pytest.mark.anyio
@pytest.mark.parametrize("value", [None, True, 1, [], {}, "", "wsp_" + "A" * 32, "../workspace"])
async def test_invalid_explicit_binding_is_rejected(tmp_path: Path, value) -> None:
    """An explicit null or invalid ID must not silently choose a private workspace."""
    runtime = WorkspaceRuntime()
    app = create_test_app(runtime, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            response = await client.post("/v1/sandboxes", json={"workspace_id": value})
    assert response.status_code == 400, response.text
    assert_failure(response, "invalid sandbox creation options")
    assert runtime.calls == 0 and runtime.workspace_calls == []


@pytest.mark.anyio
async def test_workspace_body_limit_and_private_compatibility(tmp_path: Path) -> None:
    """Actual bytes are bounded and private sandbox responses omit workspace_id."""
    runtime = WorkspaceRuntime()
    app = create_test_app(runtime, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            oversized = await client.post("/v1/workspaces", content=b" " * 4097)
            assert oversized.status_code == 413
            assert runtime.workspace_calls == []
            created = await client.post("/v1/sandboxes")
            assert created.status_code == 200, created.text
            assert "workspace_id" not in success_data(created)
            queried = await client.get(f"/v1/sandboxes/{success_data(created)['sandbox_id']}")
            assert queried.status_code == 200
            assert "workspace_id" not in success_data(queried)


@pytest.mark.anyio
@pytest.mark.parametrize("error_name,status,message", [
    ("WorkspaceNotFound", 404, "workspace not found"),
    ("WorkspaceConflict", 409, "workspace conflict"),
    ("WorkspaceUnavailable", 503, "workspace is unavailable"),
    ("WorkspaceOperationFailed", 500, "workspace operation failed"),
    ("WorkspaceDeleteProtected", 409, "workspace must be deleted separately"),
])
async def test_workspace_errors_are_fixed_and_traceable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_name: str, status: int, message: str,
) -> None:
    """Domain failures expose fixed wording and an authorized diagnostic workspace ID."""
    from control_plane.domain import errors
    from control_plane.services.sandbox_service import SandboxService

    workspace_id = "wsp_" + "a" * 32

    async def fail(self, workspace_id):
        """Raise a sensitive-looking domain exception through the shared Service route."""
        error = getattr(errors, error_name)(workspace_id)
        error.args = ("secret Docker path",)
        raise error

    monkeypatch.setattr(SandboxService, "get_workspace", fail)
    app = create_test_app(WorkspaceRuntime(), database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            response = await client.get(f"/v1/workspaces/{workspace_id}")
    assert response.status_code == status
    assert_failure(response, message, None if status == 404 else {"workspace_id": workspace_id})
    assert "secret" not in response.text


@pytest.mark.anyio
async def test_workspace_occupancy_survives_fresh_application(tmp_path: Path) -> None:
    """A second application recovers SQLite occupancy rather than trusting process memory."""
    runtime = WorkspaceRuntime()
    database = tmp_path / "lifecycle.sqlite3"
    app = create_test_app(runtime, database_path=database, reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            workspace_id = success_data(await client.post("/v1/workspaces"))["workspace_id"]
            created = await client.post("/v1/sandboxes", json={"workspace_id": workspace_id})
            assert created.status_code == 200, created.text
            sandbox_id = success_data(created)["sandbox_id"]
    restarted = create_test_app(runtime, database_path=database, reaper_enabled=False)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted), base_url="http://restarted") as client:
            observed = await client.get(f"/v1/workspaces/{workspace_id}")
            assert observed.status_code == 200, observed.text
            assert success_data(observed)["sandbox_id"] == sandbox_id
            assert success_data(observed)["status"] == "occupied"
            assert (await client.post("/v1/sandboxes", json={"workspace_id": workspace_id})).status_code == 409
            assert (await client.delete(f"/v1/sandboxes/{sandbox_id}")).status_code == 200
            assert success_data(await client.get(f"/v1/workspaces/{workspace_id}"))["status"] == "available"


@pytest.mark.anyio
async def test_workspace_query_deadline_cancels_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The API read deadline cancels lookup and returns a stable timeout response."""
    import asyncio
    from control_plane.services.sandbox_service import SandboxService

    cancelled = asyncio.Event()

    async def blocked(self, workspace_id):
        """Wait for API cancellation without creating or deleting resources."""
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(SandboxService, "get_workspace", blocked)
    app = create_test_app(WorkspaceRuntime(), database_path=tmp_path / "lifecycle.sqlite3", sandbox_query_timeout=0.01, reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            response = await client.get("/v1/workspaces/wsp_" + "a" * 32)
    assert response.status_code == 504
    assert_failure(response, "workspace query timed out")
    assert cancelled.is_set()


@pytest.mark.anyio
async def test_workspace_bound_uses_actual_streamed_bytes(tmp_path: Path) -> None:
    """A misleading short Content-Length cannot bypass the 4096-byte streaming limit."""
    runtime = WorkspaceRuntime()

    async def oversized_body():
        """Send the body in separate chunks to exercise incremental limit accounting."""
        yield b" " * 4096
        yield b"{}"

    app = create_test_app(runtime, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            response = await client.post("/v1/workspaces", headers={"Content-Length": "2"}, content=oversized_body())
    assert response.status_code == 413
    assert_failure(response, "creation request body must not exceed 4096 bytes")
    assert runtime.workspace_calls == []
