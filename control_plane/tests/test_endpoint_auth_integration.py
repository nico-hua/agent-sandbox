"""Opt-in real Nginx endpoint authentication and credential isolation checks."""

import asyncio
import os
import secrets
from pathlib import Path
from typing import Any

import docker
import httpx
import pytest

from control_plane.core.service import SandboxService
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_sandbox_docker_integration import CreatedResource, RecordingDockerClient, _remove_owned_resources

pytestmark = [
    pytest.mark.docker_integration,
    pytest.mark.skipif(os.getenv("RUN_DOCKER_INTEGRATION") != "1", reason="explicit Docker opt-in required"),
    pytest.mark.anyio,
    pytest.mark.timeout(90),
]


async def _ready_endpoint(service: SandboxService, sandbox_id: str) -> Any:
    """Wait for the real proxy and Agent without exposing credentials in diagnostics."""
    async with asyncio.timeout(35):
        while True:
            try:
                endpoint = await service.get_endpoint(sandbox_id)
                async with httpx.AsyncClient(trust_env=False, timeout=2) as client:
                    if (await client.get(endpoint.url + "/healthz")).status_code == 200:
                        return endpoint
            except Exception:
                pass
            await asyncio.sleep(0.2)


async def test_real_proxy_rejects_other_credentials_before_agent_side_effects(tmp_path: Path) -> None:
    """Two instances accept only their own token across commands, files, streams and task routes."""
    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    store = SQLiteLifecycleStore(tmp_path / "auth.sqlite3")
    await store.initialize()
    service = SandboxService(DockerSandboxRuntime(lambda: RecordingDockerClient(client, created)), store)
    try:
        first, second = await service.create(), await service.create()
        a = await _ready_endpoint(service, first.sandbox_id)
        b = await _ready_endpoint(service, second.sandbox_id)
        admin = secrets.token_urlsafe(32)
        independent_credentials = a.access_token != b.access_token
        assert independent_credentials, "instance credentials must be independent"
        async with httpx.AsyncClient(base_url=a.url, timeout=10, trust_env=False) as http:
            auth = {"Authorization": "Bearer " + a.access_token}
            cases = [
                ("POST", "/v1/commands:run", {"json": {"argv": ["sh", "-c", "touch /workspace/denied-command"]}}),
                ("POST", "/v1/commands:stream", {"json": {"argv": ["sh", "-c", "touch /workspace/denied-stream"]}}),
                ("POST", "/v1/commands", {"json": {"argv": ["sh", "-c", "touch /workspace/denied-task"]}}),
                ("POST", "/v1/files?path=denied-upload", {"content": b"denied"}),
                ("GET", "/v1/files?path=denied-upload", {}),
                ("GET", "/v1/commands/cmd_" + "a" * 32, {}),
                ("GET", "/v1/commands/cmd_" + "a" * 32 + "/output", {}),
                ("POST", "/v1/commands/cmd_" + "a" * 32 + "/cancel", {}),
                ("GET", "/unknown", {}),
            ]
            for credential in [None, b.access_token, admin, a.access_token.swapcase()]:
                headers = {} if credential is None else {"Authorization": "Bearer " + credential}
                for method, path, kwargs in cases:
                    response = await http.request(method, path, headers=headers, **kwargs)
                    assert response.status_code == 401
                    assert response.headers["www-authenticate"] == "Bearer"
                    response_hides_credential = a.access_token not in response.text
                    assert response_hides_credential, "response exposed a credential"
            duplicate = await http.get("/unknown", headers=[("Authorization", "Bearer " + a.access_token), ("Authorization", "Bearer " + a.access_token)])
            assert duplicate.status_code in {400, 401}
            for method, path in [("GET", "/healthz?x=1"), ("GET", "//healthz"), ("GET", "/%68ealthz"), ("HEAD", "/healthz"), ("POST", "/healthz"), ("GET", "/healthz/"), ("GET", "/HEALTHZ"), ("GET", "/Healthz")]:
                assert (await http.request(method, path)).status_code == 401
            assert (await http.get("/healthz")).status_code == 200
            address = httpx.URL(a.url)
            reader, writer = await asyncio.open_connection(address.host, address.port)
            try:
                writer.write(b"get /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                await writer.drain()
                status = (await reader.readline()).split()[1]
                assert status in {b"400", b"401", b"405"}
            finally:
                writer.close()
                await writer.wait_closed()
            check = await http.post("/v1/commands:run", headers=auth, json={"argv": ["sh", "-c", "test ! -e denied-command && test ! -e denied-stream && test ! -e denied-task && test ! -e denied-upload"]})
            assert check.status_code == 200 and check.json()["exit_code"] == 0
            environment = await http.post("/v1/commands:run", headers=auth, json={"argv": ["sh", "-c", 'test -z "${SANDBOX_ACCESS_TOKEN:-}" && test -z "${AGENT_SANDBOX_API_KEY:-}"']})
            assert environment.status_code == 200 and environment.json()["exit_code"] == 0
            assert (await http.post("/v1/files?path=allowed", headers=auth, content=b"secret-free-content")).status_code == 201
            assert (await http.get("/v1/files?path=allowed", headers=auth)).content == b"secret-free-content"
            stream = await http.post("/v1/commands:stream", headers=auth, json={"argv": ["printf", "stream-ok"]})
            assert stream.status_code == 200 and "event: complete" in stream.text
            task = await http.post("/v1/commands", headers=auth, json={"argv": ["sh", "-c", "sleep 5"]})
            assert task.status_code == 202
            task_id = task.json()["command_id"]
            assert (await http.get("/v1/commands/" + task_id, headers=auth)).status_code == 200
            assert (await http.get("/v1/commands/" + task_id + "/output", headers=auth)).status_code == 200
            assert (await http.post("/v1/commands/" + task_id + "/cancel", headers=auth)).status_code in {200, 202}
        async with httpx.AsyncClient(base_url=b.url, timeout=10, trust_env=False) as http:
            assert (await http.post("/v1/commands:run", headers={"Authorization": "Bearer " + b.access_token}, json={"argv": ["true"]})).status_code == 200
        for record in created:
            if record.kind == "proxy":
                logs = await asyncio.to_thread(client.containers.get(record.resource_id).logs)
                logs_hide_credentials = a.access_token.encode() not in logs and b.access_token.encode() not in logs
                assert logs_hide_credentials, "proxy logs exposed a credential"
    finally:
        await asyncio.to_thread(_remove_owned_resources, client, list(reversed(created)))
        client.close()


def _test_labels(role: str, sandbox_id: str, transaction_id: str) -> dict[str, str]:
    """Give each private proxy experiment a complete independent ownership identity."""
    return {
        "io.agent-sandbox.managed": "true", "io.agent-sandbox.project": "agent-sandbox",
        "io.agent-sandbox.resource": role, "io.agent-sandbox.sandbox-id": sandbox_id,
        "io.agent-sandbox.transaction-id": transaction_id,
    }


async def test_real_proxy_strips_authentication_headers_from_every_upstream_location() -> None:
    """A trusted private echo upstream reports only credential presence, never credential values."""
    from control_plane.runtime.docker.transaction import PROXY_IMAGE

    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    recording = RecordingDockerClient(client, created)
    sandbox_id, transaction_id = "sbx_" + secrets.token_hex(16), "txn_" + secrets.token_hex(16)
    token = secrets.token_urlsafe(32)
    source = '''import http.server, json
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        """Return credential presence facts without returning header values."""
        names = ['Authorization', 'Proxy-Authorization', 'X-API-Key', 'X-Access-Token', 'X-Sandbox-Token', 'X-Sandbox-Access-Token', 'X-Auth-Token']
        body = json.dumps({'credential_present': any(self.headers.get(name) for name in names), 'ordinary_present': self.headers.get('X-Ordinary') == 'kept'}).encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        """Keep request diagnostics free of submitted credentials."""
        pass
http.server.HTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
'''
    try:
        network = recording.api.create_network("auth-headers-" + sandbox_id, driver="bridge", internal=False, labels=_test_labels("network", sandbox_id, transaction_id))
        upstream = recording.api.create_container(
            image=client.images.get("agent-sandbox-python:dev").id,
            entrypoint=["python3", "-c", source], user="sandbox",
            labels=_test_labels("container", sandbox_id, transaction_id),
            host_config=recording.api.create_host_config(network_mode=network["Id"], read_only=True, cap_drop=["ALL"], security_opt=["no-new-privileges:true"]),
            networking_config={"EndpointsConfig": {network["Id"]: {"Aliases": ["sandbox"]}}},
        )
        client.containers.get(upstream["Id"]).start()
        proxy = recording.api.create_container(
            image=client.images.get(PROXY_IMAGE).id,
            environment={"SANDBOX_ACCESS_TOKEN": token, "SANDBOX_PROXY_AUTH_VERSION": "1"},
            labels=_test_labels("proxy", sandbox_id, transaction_id), user="nginx", ports=[8080],
            host_config=recording.api.create_host_config(network_mode=network["Id"], read_only=True, cap_drop=["ALL"], security_opt=["no-new-privileges:true"], tmpfs={"/tmp": "rw,nosuid,nodev,size=16m,mode=1777"}, port_bindings={8080: ("127.0.0.1", None)}),
            networking_config={"EndpointsConfig": {network["Id"]: {}}},
        )
        container = client.containers.get(proxy["Id"])
        container.start()
        container.reload()
        port = container.attrs["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostPort"]
        async with httpx.AsyncClient(base_url="http://127.0.0.1:" + port, timeout=3, trust_env=False) as http:
            async with asyncio.timeout(15):
                while True:
                    try:
                        if (await http.get("/healthz")).status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1)
            headers = {name: "synthetic" for name in ["Proxy-Authorization", "X-API-Key", "X-Access-Token", "X-Sandbox-Token", "X-Sandbox-Access-Token", "X-Auth-Token"]}
            headers.update(Authorization="Bearer " + token, **{"X-Ordinary": "kept"})
            for path in ["/healthz", "/v1/files", "/v1/commands:stream", "/unknown"]:
                response = await http.get(path, headers=headers)
                assert response.status_code == 200
                assert response.json() == {"credential_present": False, "ordinary_present": True}
        modes = container.exec_run(["sh", "-c", "stat -c %a /tmp/proxy-auth.* /tmp/proxy-auth.*/nginx.conf"])
        assert modes.exit_code == 0 and modes.output.splitlines() == [b"700", b"600"]
        logs_hide_credential = token.encode() not in container.logs()
        assert logs_hide_credential, "proxy logs exposed a credential"
    finally:
        await asyncio.to_thread(_remove_owned_resources, client, list(reversed(created)))
        client.close()


@pytest.mark.parametrize("environment", [
    {}, {"SANDBOX_ACCESS_TOKEN": "bad", "SANDBOX_PROXY_AUTH_VERSION": "1"},
    {"SANDBOX_ACCESS_TOKEN": "a" * 43, "SANDBOX_PROXY_AUTH_VERSION": "2"},
])
async def test_real_proxy_startup_fails_closed_without_valid_configuration(environment: dict[str, str]) -> None:
    """Missing or malformed startup configuration never starts Nginx or prints the supplied token."""
    from control_plane.runtime.docker.transaction import PROXY_IMAGE

    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    recording = RecordingDockerClient(client, created)
    try:
        labels = _test_labels("proxy", "sbx_" + secrets.token_hex(16), "txn_" + secrets.token_hex(16))
        response = recording.api.create_container(
            image=client.images.get(PROXY_IMAGE).id, environment=environment, labels=labels,
            host_config=recording.api.create_host_config(network_mode="none", read_only=True, tmpfs={"/tmp": "rw,nosuid,nodev,size=16m,mode=1777"}),
        )
        container = client.containers.get(response["Id"])
        container.start()
        result = await asyncio.to_thread(container.wait, timeout=5)
        assert result["StatusCode"] == 1
        assert container.logs() == b"proxy authentication configuration failed\n"
    finally:
        await asyncio.to_thread(_remove_owned_resources, client, list(reversed(created)))
        client.close()
