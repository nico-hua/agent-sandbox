"""Opt-in real Nginx service routing with isolated control plane and owned Docker resources."""

import asyncio
import base64
import os
import secrets
from contextlib import asynccontextmanager

import docker
import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.service import SandboxService
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_sandbox_docker_integration import RecordingDockerClient, _remove_owned_resources

pytestmark = [pytest.mark.docker_integration, pytest.mark.anyio,
              pytest.mark.skipif(os.getenv("RUN_DOCKER_INTEGRATION") != "1", reason="explicit Docker opt-in required"),
              pytest.mark.timeout(150)]

SOURCE = r'''
import base64, http.server, json, sys, threading
identity = sys.argv[1]
release = threading.Event()
lock = threading.Lock()
effects = gates = 0
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        """Echo only safe routing observations and optionally await a controlled release."""
        self.respond()
    def do_POST(self):
        """Count a visible application side effect and echo arbitrary binary bytes."""
        global effects
        with lock:
            effects += 1
        self.respond()
    def respond(self):
        """Provide deterministic gateway, credential-stripping and concurrency observations."""
        global gates
        if self.path.startswith('/release'):
            release.set()
        if self.path.startswith('/gate'):
            with lock:
                gates += 1
                with open('/workspace/service-gates', 'w') as file:
                    file.write(str(gates))
            release.wait(12)
        names = ['Authorization', 'Proxy-Authorization', 'X-API-Key', 'X-Access-Token', 'X-Sandbox-Token', 'X-Sandbox-Access-Token', 'X-Auth-Token']
        forwarding = ['Forwarded', 'X-Forwarded-For', 'X-Forwarded-Host', 'X-Forwarded-Proto', 'X-Forwarded-Port', 'X-Original-URL', 'X-Rewrite-URL']
        data = self.rfile.read(int(self.headers.get('Content-Length', '0')))
        body = json.dumps({'identity': identity, 'port': self.server.server_port, 'method': self.command,
            'path': self.path, 'body': base64.b64encode(data).decode(), 'effects': effects,
            'credential_present': any(self.headers.get(name) for name in names),
            'forwarding_present': any(self.headers.get(name) for name in forwarding),
            'host': self.headers.get('Host')}).encode()
        self.send_response(418 if self.path.startswith('/status') else 302 if self.path.startswith('/redirect') else 200)
        if self.path.startswith('/redirect'):
            self.send_header('Location', 'http://not-followed.invalid/elsewhere')
        if self.path.startswith('/accel'):
            self.send_header('X-Accel-Redirect', '/v1/files?path=secret')
            self.send_header('X-Accel-Buffering', 'yes')
            self.send_header('X-Accel-Limit-Rate', '1')
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(body)))
        self.send_header('X-App', identity)
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        """Suppress untrusted request diagnostics and credentials."""
        pass
for port in (3000, 3001):
    server = http.server.ThreadingHTTPServer(('0.0.0.0', port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
threading.Event().wait()
'''


@asynccontextmanager
async def isolated_control(tmp_path):
    """Hold a normal temporary lifespan and record exact resource identities for cleanup."""
    docker_client = docker.from_env(timeout=5)
    resources = []
    runtime = DockerSandboxRuntime(lambda: RecordingDockerClient(docker_client, resources))
    store = SQLiteLifecycleStore(tmp_path / "services.sqlite3")
    admin = secrets.token_urlsafe(32)
    app = create_app(runtime, store=store, api_key=admin, reaper_enabled=False)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control", headers={"Authorization": "Bearer " + admin}) as control:
                yield control, app, docker_client, resources, store
    finally:
        await asyncio.to_thread(_remove_owned_resources, docker_client, list(reversed(resources)))
        docker_client.close()


async def create_ready(control, app):
    """Create through the authenticated API and wait only for existing local readiness."""
    response = await control.post("/v1/sandboxes", json={"environment": "python", "ttl_seconds": 120})
    assert response.status_code == 201
    sandbox_id = response.json()["sandbox_id"]
    async with asyncio.timeout(35):
        while True:
            ready = await control.get(f"/v1/sandboxes/{sandbox_id}/readyz")
            if ready.status_code == 200:
                break
            await asyncio.sleep(0.1)
    endpoint = await app.state.service.get_endpoint(sandbox_id)
    return sandbox_id, endpoint


async def start_app(agent, identity):
    """Use the existing bounded background command protocol without long-lived task extensions."""
    response = await agent.post("/v1/commands", json={"argv": ["python3", "-u", "-c", SOURCE, identity], "timeout_ms": 30000})
    assert response.status_code == 202
    return response.json()["command_id"]


async def wait_app(http, url, *, expected=200):
    """Bound startup/termination observation without claiming endpoint retrieval probes a service."""
    async with asyncio.timeout(8):
        while True:
            response = await http.get(url)
            if response.status_code == expected:
                return response
            await asyncio.sleep(0.05)


async def raw_request(endpoint, target, *, method=b"GET", extra=b"", body=b""):
    """Send unnormalized request-target bytes without recording private Authorization values."""
    address = httpx.URL(endpoint.url)
    reader, writer = await asyncio.open_connection(address.host, address.port)
    try:
        wire = method + b" " + target + b" HTTP/1.1\r\nHost: attacker.invalid\r\nAuthorization: Bearer " + endpoint.access_token.encode() + b"\r\nConnection: close\r\n" + extra + b"\r\n"
        writer.write(wire + body)
        await writer.drain()
        async with asyncio.timeout(5):
            response = await reader.read(65536)
        return int(response.split(b" ", 2)[1])
    finally:
        writer.close()
        await writer.wait_closed()


async def test_real_service_routing_authentication_and_untrusted_responses(tmp_path):
    """Real HTTP services retain bytes and status without exposing credentials or changing targets."""
    async with isolated_control(tmp_path) as (control, app, docker_client, resources, store):
        a_id, a = await create_ready(control, app)
        b_id, b = await create_ready(control, app)
        async with httpx.AsyncClient(base_url=a.url, headers={"Authorization": "Bearer " + a.access_token}, timeout=15, trust_env=False, follow_redirects=False) as agent, httpx.AsyncClient(base_url=b.url, headers={"Authorization": "Bearer " + b.access_token}, timeout=15, trust_env=False) as other:
            a_result = await control.get(f"/v1/sandboxes/{a_id}/services/3000/endpoint")
            assert a_result.status_code == 200 and a_result.headers["cache-control"] == "no-store"
            url = a_result.json()["url"]
            assert (await agent.get(url)).status_code == 502
            record = await store.get(a_id)
            await start_app(agent, "instance-a")
            await start_app(other, "instance-b")
            await wait_app(agent, url)
            for target, identity, port in [(a.url + "/services/3000/", "instance-a", 3000), (a.url + "/services/3001/", "instance-a", 3001)]:
                response = await wait_app(agent, target)
                assert response.status_code == 200
                assert (response.json()["identity"], response.json()["port"]) == (identity, port)
            assert (await wait_app(other, b.url + "/services/3000/")).json()["identity"] == "instance-b"
            for token in (None, "bad", b.access_token, a.access_token.swapcase()):
                headers = {} if token is None else {"Authorization": "Bearer " + token}
                async with httpx.AsyncClient(trust_env=False, headers=headers, timeout=5) as unauthorized:
                    assert (await unauthorized.post(url + "effect", content=b"denied")).status_code == 401
                    assert (await unauthorized.get(url + "healthz")).status_code == 401
            assert (await agent.get(url + "stats")).json()["effects"] == 0
            query = "q=one%2Ftwo&x=a+b&x=second&percent=%252e&url=http://127.0.0.1:8080"
            headers = {name: "untrusted" for name in ["Proxy-Authorization", "X-API-Key", "X-Access-Token", "X-Sandbox-Token", "X-Sandbox-Access-Token", "X-Auth-Token", "Forwarded", "X-Forwarded-Host", "X-Forwarded-For", "X-Forwarded-Proto", "X-Forwarded-Port", "X-Original-URL", "X-Rewrite-URL", "Host"]}
            response = await agent.post(url + "echo?" + query, headers=headers, content=b"\x00\xffbinary\x80")
            result = response.json()
            assert response.status_code == 200 and result["path"] == "/echo?" + query
            assert result["method"] == "POST" and base64.b64decode(result["body"]) == b"\x00\xffbinary\x80"
            assert result["host"] == "sandbox:3000" and not result["credential_present"] and not result["forwarding_present"]
            assert (await agent.get(url + "hello%20world")).json()["path"] == "/hello%20world"
            assert (await agent.get(url + "status")).status_code == 418
            redirect = await agent.get(url + "redirect")
            assert redirect.status_code == 302 and redirect.headers["location"] == "http://not-followed.invalid/elsewhere"
            accel = await agent.get(url + "accel")
            assert accel.status_code == 200 and accel.json()["path"] == "/accel"
            assert not any(name.startswith("x-accel-") for name in accel.headers)
            for target in (b"/./services/3000/../../v1/commands:run", b"/ser%76ices/3000/../../v1/commands:run"):
                body = b'{"argv":["true"]}'
                status = await raw_request(a, target, method=b"POST", body=body,
                                           extra=f"Content-Length: {len(body)}\r\nContent-Type: application/json\r\n".encode())
                assert status == 400, "normalized service namespace escaped into Agent routing"
            for target in [b"/services/80/", b"/services/1023/", b"/services/8080/", b"/services/65536/", b"/services/03000/", b"/services/3000@egress/", b"/services/3000/../../v1/files", b"/services/3000/%2e%2e/v1/files", b"/services%2f3000/", b"/%73ervices/3000/", b"/services/3000/a%2fb", b"/services/3000/a%5cb", b"/services/3000/a\\..\\b", b"//services/3000/", b"http://other.invalid/services/3000/"]:
                assert await raw_request(a, target) == 400, "ambiguous service route was not rejected"
            # Accepted boundary ports reach the fixed main container, never Agent/egress.
            for port in (1024, 65535):
                assert await raw_request(a, f"/services/{port}/".encode()) == 502
            restarted = SandboxService(
                DockerSandboxRuntime(lambda: RecordingDockerClient(docker_client, resources)),
                SQLiteLifecycleStore(store.database_path),
            )
            restored = await restarted.get_service_endpoint(a_id, 3000)
            original_secret = restored.access_token == a.access_token
            assert restored.url == url and original_secret
            assert await store.get(a_id) == record
            assert (await control.post(f"/v1/sandboxes/{a_id}/pause")).status_code == 204
            assert (await control.get(f"/v1/sandboxes/{a_id}")).json()["status"] == "paused"
            assert (await control.post(f"/v1/sandboxes/{a_id}/resume")).status_code == 204
            assert (await wait_app(agent, url)).json()["identity"] == "instance-a"
            assert (await agent.get("/healthz", headers={"Authorization": ""})).status_code == 200
            assert (await agent.post("/v1/commands:run", json={"argv": ["true"]})).json()["exit_code"] == 0
            assert (await agent.post("/v1/files?path=service-regression", content=b"file")).status_code == 201
            assert (await agent.get("/v1/files?path=service-regression")).content == b"file"
            assert "event: complete" in (await agent.post("/v1/commands:stream", json={"argv": ["printf", "ok"]})).text
        for resource in resources:
            if resource.kind == "container":
                container = docker_client.containers.get(resource.resource_id)
                assert not any(value for value in container.attrs["NetworkSettings"]["Ports"].values())
            if resource.kind == "proxy":
                proxy = docker_client.containers.get(resource.resource_id)
                safe_logs = a.access_token.encode() not in proxy.logs() and b.access_token.encode() not in proxy.logs()
                assert safe_logs


async def test_real_service_capacity_limits_and_stop_fail_without_agent_fallback(tmp_path):
    """Eight active app requests are bounded independently of Agent health and task cleanup."""
    async with isolated_control(tmp_path) as (control, app, docker_client, resources, store):
        sandbox_id, endpoint = await create_ready(control, app)
        async with httpx.AsyncClient(base_url=endpoint.url, headers={"Authorization": "Bearer " + endpoint.access_token}, timeout=20, trust_env=False) as agent:
            task_id = await start_app(agent, "capacity")
            url = endpoint.url + "/services/3000/"
            await wait_app(agent, url)
            assert await raw_request(endpoint, b"/services/3000/", extra=b"Upgrade: websocket\r\n") == 400
            assert await raw_request(endpoint, b"/services/3000/", method=b"POST", extra=b"Content-Length: 10485761\r\n") == 413
            gates = [asyncio.create_task(agent.get(url + "gate")) for _ in range(8)]
            try:
                async with asyncio.timeout(5):
                    while True:
                        response = await agent.get("/v1/files", params={"path": "service-gates"})
                        if response.status_code == 200 and response.content == b"8":
                            break
                        await asyncio.sleep(0.05)
                ninth = await agent.get(url)
                assert ninth.status_code == 429 and ninth.json()["error"]["code"] == "service_capacity_exceeded"
                assert (await agent.get("/healthz")).status_code == 200
                release = await agent.post("/v1/commands:run", json={"argv": ["python3", "-c", "import urllib.request; urllib.request.build_opener(urllib.request.ProxyHandler({})).open('http://127.0.0.1:3000/release').read()"]})
                assert release.json()["exit_code"] == 0
                assert all(response.status_code == 200 for response in await asyncio.gather(*gates))
            finally:
                for gate in gates:
                    if not gate.done():
                        gate.cancel()
                await asyncio.gather(*gates, return_exceptions=True)
            assert (await agent.get(url)).status_code == 200
            assert (await agent.post(f"/v1/commands/{task_id}/cancel")).status_code in {200, 202}
            await wait_app(agent, url, expected=502)
            connection = await control.get(f"/v1/sandboxes/{sandbox_id}/services/3000/endpoint")
            assert connection.status_code == 200 and connection.json()["url"] == url
