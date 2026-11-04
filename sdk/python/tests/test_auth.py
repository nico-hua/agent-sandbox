"""Keep the explicit admin credential on control-plane requests only."""

import json

import httpx
import pytest

from agent_sandbox import APIError, ClientClosedError, NetworkError, Sandbox, SandboxClient, Workspace
from test_client import CONTROL, SBX, WSP, Recorder, sandbox_payload
from test_wait_ready import Clock
import agent_sandbox.client as client_module

KEY = "sdk-test_admin-key"


@pytest.mark.parametrize("operation,method,path", [
    ("create_sandbox", "POST", "/v1/sandboxes"),
    ("get_sandbox", "GET", f"/v1/sandboxes/{SBX}"),
    ("refresh_sandbox", "GET", f"/v1/sandboxes/{SBX}"),
    ("endpoint", "GET", f"/v1/sandboxes/{SBX}/endpoint"),
    ("ready", "GET", f"/v1/sandboxes/{SBX}/readyz"),
    ("renew", "POST", f"/v1/sandboxes/{SBX}/renew"),
    ("pause", "POST", f"/v1/sandboxes/{SBX}/pause"),
    ("resume", "POST", f"/v1/sandboxes/{SBX}/resume"),
    ("delete_sandbox", "DELETE", f"/v1/sandboxes/{SBX}"),
    ("create_workspace", "POST", "/v1/workspaces"),
    ("get_workspace", "GET", f"/v1/workspaces/{WSP}"),
    ("refresh_workspace", "GET", f"/v1/workspaces/{WSP}"),
    ("delete_workspace", "DELETE", f"/v1/workspaces/{WSP}"),
])
def test_all_management_routes_send_one_explicit_bearer_and_fail_401(operation, method, path):
    """Each lifecycle route authenticates once and preserves unauthorized failures."""
    recorder = Recorder(httpx.Response(401, json={"error": {
        "code": "unauthorized", "message": "authentication required"}}))
    with recorder.client(api_key=KEY) as client:
        sandbox, workspace = Sandbox(client, SBX), Workspace(client, WSP)
        operations = {
            "create_sandbox": client.create_sandbox,
            "get_sandbox": lambda: client.get_sandbox(SBX),
            "refresh_sandbox": sandbox.refresh,
            "endpoint": sandbox.get_endpoint,
            "ready": sandbox.wait_ready,
            "renew": lambda: sandbox.renew(60),
            "pause": sandbox.pause,
            "resume": sandbox.resume,
            "delete_sandbox": sandbox.delete,
            "create_workspace": client.create_workspace,
            "get_workspace": lambda: client.get_workspace(WSP),
            "refresh_workspace": workspace.refresh,
            "delete_workspace": workspace.delete,
        }
        with pytest.raises(APIError) as caught:
            operations[operation]()
        assert (caught.value.status_code, caught.value.code) == (401, "unauthorized")
        assert KEY not in str(caught.value) + repr(caught.value)
        assert "authorization" not in client._http.headers
        assert client._http.auth is None
    assert [(request.method, request.url.path) for request in recorder.requests] == [(method, path)]
    assert recorder.requests[0].headers.get_list("authorization") == [f"Bearer {KEY}"]
    if operation not in {"create_sandbox", "create_workspace"}:
        identity = caught.value.workspace_id if "workspace" in operation else caught.value.sandbox_id
        assert identity == (WSP if "workspace" in operation else SBX)


@pytest.mark.parametrize("endpoint", [CONTROL, "http://127.0.0.1:19001"])
def test_agent_command_and_files_never_receive_admin_key_even_at_control_origin(endpoint):
    """Explicit data-plane classification prevents same-origin credential leakage."""
    endpoint_payload = {"sandbox_id": SBX, "url": endpoint}
    recorder = Recorder(
        httpx.Response(200, json=endpoint_payload),
        httpx.Response(200, json={"exit_code": 0, "stdout": "ok", "stderr": ""}),
        httpx.Response(200, json=endpoint_payload), httpx.Response(201),
        httpx.Response(200, json=endpoint_payload), httpx.Response(200, content=b"file"),
    )
    with recorder.client(api_key=KEY) as client:
        sandbox = Sandbox(client, SBX)
        assert sandbox.run(["true"], env={"TASK_VALUE": "visible"}).stdout == "ok"
        sandbox.write_file("/workspace/a", b"file")
        assert sandbox.read_file("/workspace/a") == b"file"
    assert [(request.method, request.url.path) for request in recorder.requests] == [
        ("GET", f"/v1/sandboxes/{SBX}/endpoint"), ("POST", "/v1/commands:run"),
        ("GET", f"/v1/sandboxes/{SBX}/endpoint"), ("POST", "/v1/files"),
        ("GET", f"/v1/sandboxes/{SBX}/endpoint"), ("GET", "/v1/files"),
    ]
    for index, request in enumerate(recorder.requests):
        assert request.headers.get_list("authorization") == ([f"Bearer {KEY}"] if index % 2 == 0 else [])
        assert KEY.encode() not in request.content
    assert json.loads(recorder.requests[1].content)["env"] == {"TASK_VALUE": "visible"}


@pytest.mark.parametrize("base_url,target", [
    ("http://example.test", "http://example.test:81/v1/sandboxes"),
    ("http://example.test", "http://example.test:0/v1/sandboxes"),
    ("https://example.test", "https://example.test:0/v1/sandboxes"),
    ("http://example.test", "https://example.test/v1/sandboxes"),
    ("http://example.test", "http://example.test.evil/v1/sandboxes"),
    ("http://example.test", "http://example.test@evil.test/v1/sandboxes"),
    ("http://example.test", "http://example.test:800/v1/sandboxes"),
    (CONTROL, "http://127.0.0.1:180830/v1/sandboxes"),
])
def test_internal_management_routing_rejects_different_origin_before_http(base_url, target):
    """A caller mistake cannot send credentials to prefix lookalikes or other ports."""
    recorder = Recorder()
    with SandboxClient(base_url, api_key=KEY, transport=httpx.MockTransport(recorder)) as client:
        with pytest.raises(ValueError) as caught:
            client._request("GET", target, expected=200, control_plane=True)
    assert not recorder.requests
    assert KEY not in str(caught.value) + repr(caught.value)


@pytest.mark.parametrize("base_url,target", [
    ("http://EXAMPLE.test:80", "http://example.test/v1/sandboxes"),
    ("https://example.test", "https://example.test:443/v1/sandboxes"),
    ("http://[::1]", "http://[::1]:80/v1/sandboxes"),
])
def test_management_origin_normalizes_host_and_effective_default_port(base_url, target):
    """Origin equality accepts equivalent default ports and canonical host names."""
    recorder = Recorder(httpx.Response(200, json={}))
    with SandboxClient(base_url, api_key=KEY, transport=httpx.MockTransport(recorder)) as client:
        assert client._request("GET", target, expected=200, control_plane=True) == {}
    assert recorder.requests[0].headers["authorization"] == f"Bearer {KEY}"


@pytest.mark.parametrize("api_key", [None, KEY])
def test_environment_key_is_ignored_and_only_explicit_key_is_used(monkeypatch, api_key):
    """Ambient credentials never silently select SDK authentication."""
    monkeypatch.setenv("AGENT_SANDBOX_API_KEY", "ambient-secret-key")
    monkeypatch.setenv("SDK_TEST_API_KEY", "integration-secret-key")
    recorder = Recorder(httpx.Response(201, json=sandbox_payload()))
    with recorder.client(api_key=api_key) as client:
        assert client.create_sandbox().sandbox_id == SBX
    assert recorder.requests[0].headers.get_list("authorization") == (
        [] if api_key is None else [f"Bearer {KEY}"])


def test_omitted_key_does_not_consume_environment(monkeypatch):
    """The default constructor leaves authentication absent despite an ambient key."""
    monkeypatch.setenv("AGENT_SANDBOX_API_KEY", KEY)
    recorder = Recorder(httpx.Response(201, json=sandbox_payload()))
    with recorder.client() as client:
        client.create_sandbox()
    assert "authorization" not in recorder.requests[0].headers


@pytest.mark.parametrize("api_key", [
    "", "a" * 129, " key", "key ", "key\n", "key\r", "key\t", "key+", "key/", "key=",
    "key,other", "key.secret", "é", "Ａ", "secret\x00", b"secret", 1, True, [], {},
])
def test_invalid_api_key_is_rejected_locally_without_echoing_input(api_key):
    """Only bounded ASCII URL-safe strings are accepted without trimming or coercion."""
    recorder = Recorder()
    with pytest.raises(ValueError) as caught:
        recorder.client(api_key=api_key)
    assert str(caught.value) == "invalid API key"
    assert not recorder.requests


@pytest.mark.parametrize("api_key", ["a", "a" * 128, "AZaz09_-"])
def test_valid_api_key_boundaries_are_sent_unchanged(api_key):
    """Valid short and maximum-length credentials retain their precise bytes."""
    recorder = Recorder(httpx.Response(201, json=sandbox_payload()))
    with recorder.client(api_key=api_key) as client:
        client.create_sandbox()
    assert recorder.requests[0].headers["authorization"] == f"Bearer {api_key}"


@pytest.mark.parametrize("location", [f"{CONTROL}/redirected", "http://evil.test/collect"])
def test_authenticated_redirects_are_never_followed(location):
    """No management redirect can replay the authenticated operation elsewhere."""
    recorder = Recorder(httpx.Response(307, headers={"location": location}))
    with recorder.client(api_key=KEY) as client, pytest.raises(APIError) as caught:
        client.create_sandbox()
    assert caught.value.status_code == 307
    assert len(recorder.requests) == 1


def test_unauthorized_readiness_fails_without_sleep_or_retries(monkeypatch):
    """Authentication failures escape polling immediately with the known sandbox ID."""
    clock = Clock()
    monkeypatch.setattr(client_module, "monotonic", clock.monotonic)
    monkeypatch.setattr(client_module, "sleep", clock.sleep)
    recorder = Recorder(httpx.Response(401, json={"error": {
        "code": "unauthorized", "message": "authentication required"}}))
    with recorder.client(api_key=KEY) as client, pytest.raises(APIError) as caught:
        Sandbox(client, SBX).wait_ready(timeout=10)
    assert (caught.value.status_code, caught.value.code, caught.value.sandbox_id) == (401, "unauthorized", SBX)
    assert len(recorder.requests) == 1 and clock.sleeps == []


def test_authenticated_client_close_prevents_all_new_requests():
    """Key configuration never bypasses the existing closed-client guard."""
    recorder = Recorder()
    client = recorder.client(api_key=KEY)
    client.close()
    for operation in (client.create_sandbox, Sandbox(client, SBX).refresh, Workspace(client, WSP).refresh):
        with pytest.raises(ClientClosedError):
            operation()
    assert not recorder.requests


def test_transport_failure_cannot_print_admin_key_or_retry():
    """Authentication leaves safe transport diagnostics and one-attempt semantics intact."""
    recorder = Recorder(httpx.ConnectError(KEY))
    with recorder.client(api_key=KEY) as client, pytest.raises(NetworkError) as caught:
        assert KEY not in repr(client)
        Sandbox(client, SBX).refresh()
    assert caught.value.sandbox_id == SBX
    assert KEY not in str(caught.value) + repr(caught.value)
    assert len(recorder.requests) == 1
