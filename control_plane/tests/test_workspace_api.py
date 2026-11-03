"""Workspace HTTP contracts through actual SQLite and Service, without Docker."""

from dataclasses import replace
from pathlib import Path

import httpx
import pytest

from control_plane.app import create_app
from test_app import FakeRuntime


class WorkspaceRuntime(FakeRuntime):
    """Keep independent workspace and instance facts outside SQLite."""

    def __init__(self) -> None:
        """Initialize per-test independent resources and operation records."""
        super().__init__()
        self.workspaces = {}
        self.instances = {}
        self.workspace_calls = []

    async def resolve_environment_image(self, environment: str) -> str:
        """Resolve a stable image identity without contacting Docker."""
        self.workspace_calls.append(("image", environment))
        return "sha256:" + "1" * 64

    async def create_workspace(self, record) -> None:
        """Store the immutable workspace identity once."""
        self.workspace_calls.append(("create", record.workspace_id))
        self.workspaces[record.workspace_id] = record

    async def inspect_workspace(self, record) -> None:
        """Require persisted workspace identity to match runtime facts."""
        self.workspace_calls.append(("inspect", record.workspace_id))
        assert self.workspaces[record.workspace_id].creation_token == record.creation_token

    async def delete_workspace(self, record) -> None:
        """Remove only the requested independent workspace."""
        self.workspace_calls.append(("delete", record.workspace_id))
        self.workspaces.pop(record.workspace_id, None)

    async def create(self, spec) -> None:
        """Record a distinct instance with its immutable workspace binding."""
        await super().create(spec)
        self.result = replace(
            self.result, workspace_id=spec.workspace.workspace_id if spec.workspace else None,
            workspace=spec.workspace,
        )
        self.instances[spec.sandbox_id] = self.result

    async def get(self, sandbox_id):
        """Return the requested instance rather than the latest-created instance."""
        return self.instances[sandbox_id]

    async def delete(self, sandbox_id, *, expected_workspace=None, **kwargs) -> None:
        """Delete an instance while preserving independent workspace facts."""
        await super().delete(sandbox_id, **kwargs)
        self.instances.pop(sandbox_id, None)


@pytest.mark.anyio
async def test_workspace_round_trip_and_sequential_reuse(tmp_path: Path) -> None:
    """HTTP binding is exclusive, reusable after deletion and separately removable."""
    runtime = WorkspaceRuntime()
    app = create_app(runtime, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            created = await client.post("/v1/workspaces", json={})
            assert created.status_code == 201, created.text
            workspace = created.json()
            workspace_id = workspace["workspace_id"]
            assert set(workspace) == {"workspace_id", "status", "sandbox_id", "created_at"}
            assert workspace["status"] == "available" and workspace["sandbox_id"] is None
            first = await client.post("/v1/sandboxes", json={"workspace_id": workspace_id})
            assert first.status_code == 201, first.text
            first_id = first.json()["sandbox_id"]
            assert first.json()["workspace_id"] == workspace_id
            assert (await client.get(f"/v1/sandboxes/{first_id}")).json()["workspace_id"] == workspace_id
            occupied = (await client.get(f"/v1/workspaces/{workspace_id}")).json()
            assert occupied["status"] == "occupied" and occupied["sandbox_id"] == first_id
            assert (await client.post("/v1/sandboxes", json={"workspace_id": workspace_id})).status_code == 409
            assert (await client.delete(f"/v1/workspaces/{workspace_id}")).status_code == 409
            protected = await client.delete(f"/v1/sandboxes/{first_id}?delete_workspace=true")
            assert protected.status_code == 409
            assert protected.json()["error"]["code"] == "workspace_delete_protected"
            assert (await client.delete(f"/v1/sandboxes/{first_id}")).status_code == 204
            assert (await client.get(f"/v1/workspaces/{workspace_id}")).json()["status"] == "available"
            second = await client.post("/v1/sandboxes", json={"workspace_id": workspace_id})
            assert second.status_code == 201, second.text
            assert second.json()["sandbox_id"] != first_id
            assert (await client.delete(f"/v1/sandboxes/{second.json()['sandbox_id']}")).status_code == 204
            assert (await client.delete(f"/v1/workspaces/{workspace_id}")).status_code == 204
            assert (await client.get(f"/v1/workspaces/{workspace_id}")).status_code == 404


@pytest.mark.anyio
@pytest.mark.parametrize("body", [
    b"null", b"[]", b'{"environment":null}', b'{"environment":true}',
    b'{"environment":"basic","environment":"python"}', b'{"environment":NaN}',
    b'{"workspace_id":"wsp_x"}', b'{"environment":"../basic"}', b'{"environment":"missing"}',
])
async def test_invalid_workspace_creation_precedes_runtime(tmp_path: Path, body: bytes) -> None:
    """Invalid JSON, environment shape and registration fail before runtime work."""
    runtime = WorkspaceRuntime()
    app = create_app(runtime, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            response = await client.post("/v1/workspaces", content=body)
    assert response.status_code == 400, response.text
    assert response.json()["error"]["code"] == "invalid_request"
    assert runtime.workspace_calls == []


@pytest.mark.anyio
@pytest.mark.parametrize("value", [None, True, 1, [], {}, "", "wsp_" + "A" * 32, "../workspace"])
async def test_invalid_explicit_binding_is_rejected(tmp_path: Path, value) -> None:
    """An explicit null or invalid ID must not silently choose a private workspace."""
    runtime = WorkspaceRuntime()
    app = create_app(runtime, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            response = await client.post("/v1/sandboxes", json={"workspace_id": value})
    assert response.status_code == 400, response.text
    assert response.json()["error"]["code"] == "invalid_request"
    assert runtime.calls == 0 and runtime.workspace_calls == []


@pytest.mark.anyio
async def test_workspace_body_limit_and_private_compatibility(tmp_path: Path) -> None:
    """Actual bytes are bounded and private sandbox responses omit workspace_id."""
    runtime = WorkspaceRuntime()
    app = create_app(runtime, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            oversized = await client.post("/v1/workspaces", content=b" " * 4097)
            assert oversized.status_code == 413
            assert runtime.workspace_calls == []
            created = await client.post("/v1/sandboxes")
            assert created.status_code == 201, created.text
            assert "workspace_id" not in created.json()
            queried = await client.get(f"/v1/sandboxes/{created.json()['sandbox_id']}")
            assert queried.status_code == 200
            assert "workspace_id" not in queried.json()


@pytest.mark.anyio
@pytest.mark.parametrize("error_name,status,code", [
    ("WorkspaceNotFound", 404, "workspace_not_found"),
    ("WorkspaceConflict", 409, "workspace_conflict"),
    ("WorkspaceUnavailable", 503, "workspace_unavailable"),
    ("WorkspaceOperationFailed", 500, "workspace_operation_failed"),
    ("WorkspaceDeleteProtected", 409, "workspace_delete_protected"),
])
async def test_workspace_errors_are_fixed_and_traceable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error_name: str, status: int, code: str,
) -> None:
    """Domain failures expose fixed wording and an authorized diagnostic workspace ID."""
    from control_plane.core import errors
    from control_plane.core.service import SandboxService

    workspace_id = "wsp_" + "a" * 32

    async def fail(self, workspace_id):
        """Raise a sensitive-looking domain exception through the shared Service route."""
        error = getattr(errors, error_name)(workspace_id)
        error.args = ("secret Docker path",)
        raise error

    monkeypatch.setattr(SandboxService, "get_workspace", fail)
    app = create_app(WorkspaceRuntime(), database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            response = await client.get(f"/v1/workspaces/{workspace_id}")
    assert response.status_code == status
    assert response.json()["error"]["code"] == code
    assert response.json()["error"]["workspace_id"] == workspace_id
    assert "secret" not in response.text


@pytest.mark.anyio
async def test_workspace_occupancy_survives_fresh_application(tmp_path: Path) -> None:
    """A second application recovers SQLite occupancy rather than trusting process memory."""
    runtime = WorkspaceRuntime()
    database = tmp_path / "lifecycle.sqlite3"
    app = create_app(runtime, database_path=database, reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            workspace_id = (await client.post("/v1/workspaces")).json()["workspace_id"]
            created = await client.post("/v1/sandboxes", json={"workspace_id": workspace_id})
            assert created.status_code == 201, created.text
            sandbox_id = created.json()["sandbox_id"]
    restarted = create_app(runtime, database_path=database, reaper_enabled=False)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted), base_url="http://restarted") as client:
            observed = await client.get(f"/v1/workspaces/{workspace_id}")
            assert observed.status_code == 200, observed.text
            assert observed.json()["sandbox_id"] == sandbox_id
            assert observed.json()["status"] == "occupied"
            assert (await client.post("/v1/sandboxes", json={"workspace_id": workspace_id})).status_code == 409
            assert (await client.delete(f"/v1/sandboxes/{sandbox_id}")).status_code == 204
            assert (await client.get(f"/v1/workspaces/{workspace_id}")).json()["status"] == "available"


@pytest.mark.anyio
async def test_workspace_query_deadline_cancels_read(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The API read deadline cancels lookup and returns a stable timeout response."""
    import asyncio
    from control_plane.core.service import SandboxService

    cancelled = asyncio.Event()

    async def blocked(self, workspace_id):
        """Wait for API cancellation without creating or deleting resources."""
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    monkeypatch.setattr(SandboxService, "get_workspace", blocked)
    app = create_app(WorkspaceRuntime(), database_path=tmp_path / "lifecycle.sqlite3", sandbox_query_timeout=0.01, reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            response = await client.get("/v1/workspaces/wsp_" + "a" * 32)
    assert response.status_code == 504
    assert response.json()["error"]["code"] == "workspace_query_timeout"
    assert cancelled.is_set()


@pytest.mark.anyio
async def test_workspace_bound_uses_actual_streamed_bytes(tmp_path: Path) -> None:
    """A misleading short Content-Length cannot bypass the 4096-byte streaming limit."""
    runtime = WorkspaceRuntime()

    async def oversized_body():
        """Send the body in separate chunks to exercise incremental limit accounting."""
        yield b" " * 4096
        yield b"{}"

    app = create_app(runtime, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            response = await client.post("/v1/workspaces", headers={"Content-Length": "2"}, content=oversized_body())
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "invalid_request"
    assert runtime.workspace_calls == []
