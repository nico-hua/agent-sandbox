"""Explicit, isolated Docker verification of extended background command lifetimes."""

import asyncio
import os
import secrets
from contextlib import asynccontextmanager
from pathlib import Path
from time import monotonic

import docker
import httpx
import pytest

from control_plane.app import create_app
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from test_sandbox_docker_integration import RecordingDockerClient, _remove_owned_resources

pytestmark = [
    pytest.mark.docker_integration,
    pytest.mark.anyio,
    pytest.mark.skipif(
        os.getenv("RUN_DOCKER_INTEGRATION") != "1",
        reason="explicit Docker opt-in required; independent long-task image must exist",
    ),
    pytest.mark.timeout(120),
]

_IMAGE = "agent-sandbox-python-long-task:dev"
_HTTP_SERVICE = r'''
import http.server, json, secrets, time
identity, started = secrets.token_hex(16), time.monotonic()
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        """Expose a stable process identity and monotonic lifetime without request diagnostics."""
        body = json.dumps({'identity': identity, 'uptime': time.monotonic() - started}).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        """Avoid storing request headers or an unbounded stream of access messages."""
        pass
http.server.HTTPServer(('0.0.0.0', 3000), Handler).serve_forever()
'''


@asynccontextmanager
async def _long_task_instance(tmp_path: Path):
    """Register only the independent image and own a temporary authenticated control-plane lifespan."""
    client = docker.from_env(timeout=5)
    created = []
    try:
        # This explicit test never builds, pulls, replaces tags or opens a development database.
        image_id = client.images.get(_IMAGE).id
        runtime = DockerSandboxRuntime(
            lambda: RecordingDockerClient(client, created),
            additional_environments={"long-task": _IMAGE},
        )
        # The test key is held in the test client only, never in an Agent command or log.
        admin = secrets.token_urlsafe(32)
        app = create_app(
            runtime, api_key=admin, database_path=tmp_path / "long-task.sqlite3",
            reaper_enabled=False, sandbox_query_timeout=5,
        )
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://control",
                headers={"Authorization": "Bearer " + admin},
            ) as control:
                response = await control.post("/v1/sandboxes", json={
                    "environment": "long-task", "ttl_seconds": 180,
                })
                assert response.status_code == 201
                sandbox_id = response.json()["sandbox_id"]
                containers = [entry for entry in created if entry.kind == "container"]
                assert len(containers) == 1
                container = client.containers.get(containers[0].resource_id)
                assert container.image.id == image_id
                async with asyncio.timeout(35):
                    while True:
                        if (await control.get(f"/v1/sandboxes/{sandbox_id}/readyz")).status_code == 200:
                            break
                        await asyncio.sleep(0.1)
                endpoint = await app.state.service.get_endpoint(sandbox_id)
                service_response = await control.get(f"/v1/sandboxes/{sandbox_id}/services/3000/endpoint")
                assert service_response.status_code == 200
                async with httpx.AsyncClient(
                    base_url=endpoint.url, headers={"Authorization": "Bearer " + endpoint.access_token},
                    trust_env=False, follow_redirects=False, timeout=5,
                ) as agent:
                    yield control, agent, service_response.json()["url"], sandbox_id
                assert (await control.delete(f"/v1/sandboxes/{sandbox_id}")).status_code == 204
                # Normal deletion keeps the private workspace; final cleanup verifies its original labels.
                for entry in created:
                    if entry.kind == "workspace":
                        client.volumes.get(entry.resource_id)
    finally:
        try:
            await asyncio.to_thread(_remove_owned_resources, client, list(reversed(created)))
        finally:
            client.close()


async def _wait_service(agent: httpx.AsyncClient, url: str, expected: int = 200) -> httpx.Response:
    """Observe listening or termination with a short bounded GET loop, never retrying a write."""
    async with asyncio.timeout(8):
        while True:
            response = await agent.get(url)
            if response.status_code == expected:
                return response
            assert response.status_code in {200, 502, 503, 504}
            await asyncio.sleep(0.05)


async def _wait_terminal(agent: httpx.AsyncClient, command_id: str) -> dict:
    """Wait for Runner cleanup and immutable terminal output without treating cancel acceptance as completion."""
    async with asyncio.timeout(8):
        while True:
            response = await agent.get(f"/v1/commands/{command_id}")
            assert response.status_code == 200
            result = response.json()
            if result["status"] != "running":
                return result
            await asyncio.sleep(0.05)


async def test_long_http_service_survives_old_limit_and_stops_on_cancel(tmp_path: Path) -> None:
    """A real service exceeds 30 seconds, retains its slot, then stops and releases it after cancellation."""
    async with _long_task_instance(tmp_path) as (_, agent, service_url, _):
        # Closing the actual submission connection must not cancel the accepted task.
        async with httpx.AsyncClient(
            base_url=agent.base_url, trust_env=False, follow_redirects=False, timeout=5,
        ) as submitter:
            accepted = await submitter.post("/v1/commands", headers={
                "Authorization": agent.headers["Authorization"],
            }, json={
                "argv": ["python3", "-u", "-c", _HTTP_SERVICE],
                "timeout_ms": 600000, "max_output_bytes_per_stream": 1024,
            })
            assert accepted.status_code == 202
            command_id = accepted.json()["command_id"]
        first = (await _wait_service(agent, service_url)).json()
        observed_at = monotonic()
        # Real wall-clock time is intentional here: mocks cannot prove the old 30-second boundary.
        await asyncio.sleep(max(0, 31.2 - (monotonic() - observed_at)))
        after = (await agent.get(service_url)).json()
        assert after["identity"] == first["identity"] and after["uptime"] >= 31
        running = (await agent.get(f"/v1/commands/{command_id}")).json()
        assert running["status"] == "running" and running["timeout_ms"] == 600000

        other_ids = []
        for _ in range(3):
            accepted = await agent.post("/v1/commands", json={
                "argv": ["python3", "-c", "import threading; threading.Event().wait()"],
                "timeout_ms": 600000,
            })
            assert accepted.status_code == 202
            other_ids.append(accepted.json()["command_id"])
        for path in ("/v1/commands", "/v1/commands:run", "/v1/commands:stream"):
            full = await agent.post(path, json={"argv": ["true"]})
            assert full.status_code == 429 and full.json()["error"]["code"] == "command_capacity_exceeded"
        assert (await agent.get(f"/v1/commands/{command_id}/output")).status_code == 200
        cancel = await agent.post(f"/v1/commands/{command_id}/cancel")
        assert cancel.status_code == 202 and cancel.json()["timeout_ms"] == 600000
        terminal = await _wait_terminal(agent, command_id)
        assert terminal["status"] == "canceled" and terminal["timeout_ms"] == 600000
        assert terminal["error"]["code"] == "command_canceled"
        assert (await agent.post(f"/v1/commands/{command_id}/cancel")).status_code == 200
        await _wait_service(agent, service_url, expected=502)
        reusable = await agent.post("/v1/commands:run", json={"argv": ["true"]})
        assert reusable.status_code == 200 and reusable.json()["exit_code"] == 0
        for other_id in other_ids:
            assert (await agent.post(f"/v1/commands/{other_id}/cancel")).status_code == 202
            assert (await _wait_terminal(agent, other_id))["status"] == "canceled"


async def test_paused_background_deadline_does_not_restart_on_resume(tmp_path: Path) -> None:
    """A finite background deadline elapses during freezing and ends the original task after thawing."""
    async with _long_task_instance(tmp_path) as (control, agent, service_url, sandbox_id):
        response = await agent.post("/v1/commands", json={
            "argv": ["python3", "-u", "-c", _HTTP_SERVICE], "timeout_ms": 3000,
        })
        assert response.status_code == 202
        command_id = response.json()["command_id"]
        await _wait_service(agent, service_url)
        assert (await control.post(f"/v1/sandboxes/{sandbox_id}/pause")).status_code == 204
        await asyncio.sleep(3.1)
        assert (await control.post(f"/v1/sandboxes/{sandbox_id}/resume")).status_code == 204
        result = await _wait_terminal(agent, command_id)
        assert result["status"] == "failed" and result["timeout_ms"] == 3000
        assert result["error"]["code"] == "command_timeout"
        await _wait_service(agent, service_url, expected=502)
