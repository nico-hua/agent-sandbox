"""Exercise the public synchronous SDK through an in-memory HTTP transport."""

import json
from datetime import datetime, timezone

import httpx
import pytest

from agent_sandbox import (
    APIError, ClientClosedError, NetworkError, ProtocolError,
    SDKTimeoutError, SandboxClient,
)

SBX = "sbx_" + "1" * 32
OTHER_SBX = "sbx_" + "2" * 32
WSP = "wsp_" + "3" * 32
STAMP = "2026-11-03T01:02:03.123456Z"
CONTROL = "http://127.0.0.1:18083"


def sandbox_payload(**updates):
    """Return a complete sandbox response while allowing focused malformed variants."""
    return {"sandbox_id": SBX, "status": "running", "created_at": STAMP,
            "expires_at": STAMP, **updates}


def workspace_payload(**updates):
    """Return a complete independent workspace response."""
    return {"workspace_id": WSP, "status": "available", "created_at": STAMP,
            "sandbox_id": None, **updates}


class Recorder:
    """Serve queued HTTP responses and retain observable outgoing requests."""

    def __init__(self, *responses):
        """Initialize an ordered response script without opening sockets."""
        self.responses = list(responses)
        self.requests = []

    def __call__(self, request):
        """Record one request and return or raise its scripted outcome."""
        self.requests.append(request)
        assert self.responses, f"unexpected request: {request.method} {request.url}"
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def client(self, **kwargs):
        """Build the actual SDK client over this in-memory HTTP boundary."""
        return SandboxClient(CONTROL, transport=httpx.MockTransport(self), **kwargs)


def test_create_omits_unspecified_options_and_does_not_wait():
    """Creation sends the empty object and returns without readiness side effects."""
    recorder = Recorder(httpx.Response(201, json=sandbox_payload(status="starting")))
    with recorder.client() as client:
        sandbox = client.create_sandbox()
    assert sandbox.sandbox_id == SBX
    assert sandbox.info.status == "starting"
    assert [(r.method, str(r.url)) for r in recorder.requests] == [
        ("POST", f"{CONTROL}/v1/sandboxes")]
    assert json.loads(recorder.requests[0].content) == {}


def test_create_transmits_custom_environment_zero_resources_network_and_workspace():
    """Serialization preserves explicit zero and delegates environment/network policy."""
    options = {"environment": "org-custom", "ttl_seconds": 0,
               "resources": {"cpu_millis": 0, "memory_mib": 64},
               "network": {"allow_domains": ["example.org"]}, "workspace_id": WSP}
    recorder = Recorder(httpx.Response(201, json=sandbox_payload(workspace_id=WSP)))
    with recorder.client() as client:
        sandbox = client.create_sandbox(**options)
    assert json.loads(recorder.requests[0].content) == options
    assert sandbox.info.workspace_id == WSP


@pytest.mark.parametrize("options", [
    {"environment": None}, {"environment": 3}, {"ttl_seconds": None},
    {"ttl_seconds": True}, {"ttl_seconds": 1.2}, {"resources": None},
    {"resources": []}, {"resources": {"cpu_millis": True}},
    {"resources": {"memory_mib": "128"}}, {"network": None},
    {"network": {"allow_domains": "example.org"}},
    {"network": {"allow_domains": [None]}}, {"workspace_id": None},
    {"workspace_id": WSP + "/../"},
])
def test_create_rejects_invalid_input_before_http(options):
    """Wrong local input types and injected identities never reach the control plane."""
    recorder = Recorder()
    with recorder.client() as client, pytest.raises((ValueError, TypeError)):
        client.create_sandbox(**options)
    assert recorder.requests == []


@pytest.mark.parametrize("kind,bad", [
    ("sandbox", "sbx_" + "A" * 32), ("sandbox", "sbx_" + "1" * 31),
    ("sandbox", SBX + "/endpoint"), ("sandbox", SBX + "?x=y"),
    ("sandbox", "../" + SBX), ("sandbox", WSP), ("sandbox", None),
    ("workspace", WSP + "#fragment"), ("workspace", SBX),
    ("workspace", "wsp_" + "g" * 32), ("workspace", 1),
])
def test_opaque_ids_are_validated_before_path_construction(kind, bad):
    """Opaque IDs cannot smuggle paths, query strings, other types or uppercase hex."""
    recorder = Recorder()
    with recorder.client() as client, pytest.raises((ValueError, TypeError)):
        getattr(client, f"get_{kind}")(bad)
    assert recorder.requests == []


def test_workspace_create_refresh_delete_and_custom_environment_routes():
    """Workspace handles use only lifecycle endpoints and refresh their cached info."""
    recorder = Recorder(
        httpx.Response(201, json=workspace_payload()),
        httpx.Response(200, json=workspace_payload(status="occupied", sandbox_id=SBX)),
        httpx.Response(204),
        httpx.Response(201, json=workspace_payload()),
    )
    with recorder.client() as client:
        workspace = client.create_workspace(environment="custom-image")
        assert workspace.workspace_id == WSP
        info = workspace.refresh()
        assert workspace.info is info
        assert info.sandbox_id == SBX
        assert workspace.delete() is None
        client.create_workspace()
    assert [(r.method, r.url.path) for r in recorder.requests] == [
        ("POST", "/v1/workspaces"), ("GET", f"/v1/workspaces/{WSP}"),
        ("DELETE", f"/v1/workspaces/{WSP}"), ("POST", "/v1/workspaces")]
    assert json.loads(recorder.requests[0].content) == {"environment": "custom-image"}
    assert json.loads(recorder.requests[3].content) == {}


@pytest.mark.parametrize("environment", [None, 1, True])
def test_workspace_create_invalid_environment_is_local(environment):
    """Workspace creation rejects non-string environment values before HTTP."""
    recorder = Recorder()
    with recorder.client() as client, pytest.raises((ValueError, TypeError)):
        client.create_workspace(environment=environment)
    assert not recorder.requests


def test_sandbox_lifecycle_routes_update_only_observed_metadata():
    """Lifecycle methods use their exact routes without hidden refresh or deletion."""
    later = "2026-11-04T01:02:03Z"
    recorder = Recorder(
        httpx.Response(200, json=sandbox_payload()),
        httpx.Response(200, json=sandbox_payload(status="paused")),
        httpx.Response(200, json={"sandbox_id": SBX, "expires_at": later}),
        httpx.Response(204), httpx.Response(204), httpx.Response(204),
    )
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        info = sandbox.refresh()
        assert sandbox.info is info and info.status == "paused"
        renewal = sandbox.renew(0)
        assert renewal.expires_at == datetime(2026, 11, 4, 1, 2, 3, tzinfo=timezone.utc)
        assert sandbox.info.expires_at == renewal.expires_at
        assert sandbox.pause() is None
        assert sandbox.resume() is None
        assert sandbox.delete(delete_workspace=True) is None
    assert [(r.method, r.url.path) for r in recorder.requests] == [
        ("GET", f"/v1/sandboxes/{SBX}"), ("GET", f"/v1/sandboxes/{SBX}"),
        ("POST", f"/v1/sandboxes/{SBX}/renew"),
        ("POST", f"/v1/sandboxes/{SBX}/pause"),
        ("POST", f"/v1/sandboxes/{SBX}/resume"),
        ("DELETE", f"/v1/sandboxes/{SBX}")]
    assert json.loads(recorder.requests[2].content) == {"ttl_seconds": 0}
    assert recorder.requests[-1].url.params["delete_workspace"] == "true"


def test_delete_preserves_workspace_by_default():
    """Default sandbox deletion explicitly requests preservation of workspace data."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()), httpx.Response(204))
    with recorder.client() as client:
        client.get_sandbox(SBX).delete()
    assert recorder.requests[-1].url.params["delete_workspace"] == "false"


@pytest.mark.parametrize("method,kwargs", [
    ("renew", {"ttl_seconds": True}), ("renew", {"ttl_seconds": None}),
    ("renew", {"ttl_seconds": 1.5}), ("delete", {"delete_workspace": "true"}),
    ("delete", {"delete_workspace": 1}),
])
def test_lifecycle_invalid_types_do_not_mutate(method, kwargs):
    """Ambiguous booleans and numeric coercions cannot issue lifecycle mutations."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()))
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        with pytest.raises((ValueError, TypeError)):
            getattr(sandbox, method)(**kwargs)
    assert len(recorder.requests) == 1


def test_run_refreshes_endpoint_each_time_and_preserves_direct_argv_and_nonzero():
    """Commands use a fresh backend address with literal argv and plain output strings."""
    recorder = Recorder(
        httpx.Response(200, json=sandbox_payload()),
        httpx.Response(200, json={"sandbox_id": SBX, "auth_required": True, "access_token": "a" * 43, "url": "http://127.0.0.1:19001"}),
        httpx.Response(200, json={"exit_code": 7, "stdout": "hello\n", "stderr": "err"}),
        httpx.Response(200, json={"sandbox_id": SBX, "auth_required": True, "access_token": "a" * 43, "url": "http://127.0.0.1:19002/"}),
        httpx.Response(200, json={"exit_code": 0, "stdout": "", "stderr": ""}),
    )
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        result = sandbox.run(["echo", "$(literal); x"], cwd="/workspace", env={"A": "b"},
                             stdin="input", timeout_ms=30000, max_output_bytes_per_stream=0)
        assert (result.exit_code, result.stdout, result.stderr) == (7, "hello\n", "err")
        sandbox.run(("true",))
    commands = [recorder.requests[2], recorder.requests[4]]
    assert [str(r.url) for r in commands] == [
        "http://127.0.0.1:19001/v1/commands:run", "http://127.0.0.1:19002/v1/commands:run"]
    assert json.loads(commands[0].content) == {
        "argv": ["echo", "$(literal); x"], "cwd": "/workspace", "env": {"A": "b"},
        "stdin": "input", "timeout_ms": 30000, "max_output_bytes_per_stream": 0}
    assert json.loads(commands[1].content) == {"argv": ["true"]}
    assert commands[0].extensions["timeout"]["read"] >= 35.0
    assert commands[1].extensions["timeout"]["read"] >= 10.0
    assert all(0 < value < float("inf") for value in commands[0].extensions["timeout"].values())


@pytest.mark.parametrize("argv,kwargs", [
    ("echo hi", {}), ([], {}), ([1], {}), (["echo"], {"cwd": None}),
    (["echo"], {"env": {"X": 1}}), (["echo"], {"stdin": b"x"}),
    (["echo"], {"timeout_ms": True}), (["echo"], {"timeout_ms": 0.5}),
    (["echo"], {"max_output_bytes_per_stream": False}),
])
def test_run_rejects_bad_types_before_endpoint_lookup(argv, kwargs):
    """Malformed commands fail locally without contacting the endpoint service."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()))
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        with pytest.raises((ValueError, TypeError)):
            sandbox.run(argv, **kwargs)
    assert len(recorder.requests) == 1


@pytest.mark.parametrize("url", [
    "https://127.0.0.1:1234", "http://localhost:1234", "http://127.0.0.2:1234",
    "http://127.0.0.1:0", "http://127.0.0.1:65536", "http://127.0.0.1:0123",
    "http://127.0.0.1:1234/path", "http://127.0.0.1:1234?query=1",
    "http://127.0.0.1:1234#frag", "http://user@127.0.0.1:1234",
    "http://127.0.0.1", " http://127.0.0.1:1234", "http://2130706433:1234",
    "http://127.0.0.1:1234/../", "http://127.0.0.1:1234\\evil",
])
def test_untrusted_endpoint_cannot_redirect_agent_requests(url):
    """Only the precise literal loopback endpoint grammar may receive agent data."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()),
                        httpx.Response(200, json={"sandbox_id": SBX, "auth_required": True, "access_token": "a" * 43, "url": url}))
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        with pytest.raises(ProtocolError):
            sandbox.run(["true"])
    assert len(recorder.requests) == 2


def test_endpoint_identity_must_match_handle():
    """An endpoint response for another sandbox cannot route this handle's command."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()), httpx.Response(
        200, json={"sandbox_id": OTHER_SBX, "auth_required": True, "access_token": "a" * 43, "url": "http://127.0.0.1:19001"}))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.get_sandbox(SBX).get_endpoint()
    assert len(recorder.requests) == 2


@pytest.mark.parametrize("status", [400, 404, 409, 429, 503, 504])
def test_api_errors_preserve_status_unknown_code_and_safe_diagnostics(status):
    """HTTP failures retain structured diagnostics without exposing them in str(error)."""
    recorder = Recorder(httpx.Response(status, json={"error": {
        "code": "future_api_code", "message": "secret remote text", "sandbox_id": SBX,
        "workspace_id": WSP}}))
    with recorder.client() as client, pytest.raises(APIError) as caught:
        client.create_sandbox()
    error = caught.value
    assert (error.status_code, error.code, error.message) == (status, "future_api_code", "secret remote text")
    assert (error.sandbox_id, error.workspace_id) == (SBX, WSP)
    assert "secret" not in str(error)
    assert len(recorder.requests) == 1


@pytest.mark.parametrize("status,content", [(400, b"<html>private traceback</html>"),
                                             (503, b'{"error": {"code": 3}}')])
def test_non_json_or_malformed_error_uses_safe_http_fallback(status, content):
    """Malformed error bodies do not hide the HTTP status or leak raw text."""
    recorder = Recorder(httpx.Response(status, content=content))
    with recorder.client() as client, pytest.raises(APIError) as caught:
        client.create_sandbox()
    assert caught.value.status_code == status
    assert caught.value.code == "http_error"
    assert caught.value.message == "HTTP request failed"
    assert "traceback" not in str(caught.value)


@pytest.mark.parametrize("status,content", [(200, b"{}"), (201, b"not json"),
                                             (201, b"[]"), (204, b"")])
def test_malformed_or_unexpected_success_is_protocol_error(status, content):
    """A successful HTTP status still requires the operation's full response contract."""
    recorder = Recorder(httpx.Response(status, content=content))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.create_sandbox()
    assert len(recorder.requests) == 1


@pytest.mark.parametrize("failure,expected", [
    (httpx.ReadTimeout("sensitive body"), SDKTimeoutError),
    (httpx.ConnectTimeout("sensitive host"), SDKTimeoutError),
    (httpx.ConnectError("sensitive credentials"), NetworkError),
    (httpx.RemoteProtocolError("sensitive protocol"), NetworkError),
])
@pytest.mark.parametrize("operation", ["create", "get", "delete", "command"])
def test_network_failures_are_safe_and_never_retried(failure, expected, operation):
    """GETs and writes each make one attempt and normalize transport errors safely."""
    initial = [] if operation in {"create", "get"} else [httpx.Response(200, json=sandbox_payload())]
    if operation == "command":
        initial.append(httpx.Response(200, json={"sandbox_id": SBX, "auth_required": True, "access_token": "a" * 43, "url": "http://127.0.0.1:19001"}))
    recorder = Recorder(*initial, failure)
    with recorder.client() as client, pytest.raises(expected) as caught:
        if operation == "create":
            client.create_sandbox()
        elif operation == "get":
            client.get_sandbox(SBX)
        elif operation == "delete":
            client.get_sandbox(SBX).delete()
        else:
            client.get_sandbox(SBX).run(["true"])
    assert "sensitive" not in str(caught.value)
    assert len(recorder.requests) == len(initial) + 1


def test_redirect_is_not_followed():
    """Control-plane redirects cannot forward SDK requests to another host."""
    recorder = Recorder(httpx.Response(307, headers={"location": "http://evil.example/collect"}))
    with recorder.client() as client, pytest.raises(APIError) as caught:
        client.create_sandbox()
    assert caught.value.status_code == 307
    assert len(recorder.requests) == 1


class ClosingTransport(httpx.MockTransport):
    """Track transport lifetime independently from sandbox resource lifetime."""

    def __init__(self, handler):
        """Install a request script and start with an open transport."""
        super().__init__(handler)
        self.close_count = 0

    def close(self):
        """Observe whether the SDK closes its owned connections exactly once."""
        self.close_count += 1
        super().close()


def test_context_manager_closes_connections_without_deleting_resources():
    """Client cleanup closes its owned transport and leaves all remote resources alone."""
    recorder = Recorder(httpx.Response(201, json=sandbox_payload()),
                        httpx.Response(201, json=workspace_payload()))
    transport = ClosingTransport(recorder)
    client = SandboxClient(CONTROL, transport=transport)
    with client:
        sandbox = client.create_sandbox()
        workspace = client.create_workspace()
    client.close()
    assert transport.close_count == 1
    assert [r.method for r in recorder.requests] == ["POST", "POST"]
    for operation in (sandbox.refresh, workspace.refresh, client.create_sandbox):
        with pytest.raises(ClientClosedError):
            operation()


def test_environment_proxy_variables_cannot_reroute_requests(monkeypatch):
    """SDK requests bypass inherited proxy configuration for the local control plane."""
    monkeypatch.setenv("HTTP_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("ALL_PROXY", "http://proxy.example:8080")
    monkeypatch.setenv("NO_PROXY", "")
    recorder = Recorder(httpx.Response(201, json=sandbox_payload()))
    transport = httpx.MockTransport(recorder)
    configured_proxies = []
    original_init = httpx.HTTPTransport.__init__

    def capture_transport_options(self, *args, **kwargs):
        """Observe actual proxy transport construction while retaining real HTTPX wiring."""
        configured_proxies.append(kwargs.get("proxy"))
        original_init(self, *args, **kwargs)

    monkeypatch.setattr(httpx.HTTPTransport, "__init__", capture_transport_options)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", transport.handle_request)
    with SandboxClient(CONTROL) as client:
        client.create_sandbox()
    assert str(recorder.requests[0].url) == f"{CONTROL}/v1/sandboxes"
    assert configured_proxies and all(proxy is None for proxy in configured_proxies)


@pytest.mark.parametrize("timeout", [
    None, True, 0, -1, float("inf"), float("nan"),
    httpx.Timeout(None), httpx.Timeout(5, read=None),
])
def test_client_requires_finite_positive_timeout_phases(timeout):
    """Invalid timeout settings cannot create requests with unbounded HTTP phases."""
    recorder = Recorder()
    with pytest.raises((ValueError, TypeError)):
        recorder.client(timeout=timeout)
    assert not recorder.requests


@pytest.mark.parametrize("kind,identifier,payload", [
    ("sandbox", SBX, sandbox_payload(sandbox_id=OTHER_SBX)),
    ("workspace", WSP, workspace_payload(workspace_id="wsp_" + "4" * 32)),
])
def test_get_rejects_response_for_different_identity(kind, identifier, payload):
    """Lookup methods bind response identity to the requested opaque resource ID."""
    recorder = Recorder(httpx.Response(200, json=payload))
    with recorder.client() as client, pytest.raises(ProtocolError):
        getattr(client, f"get_{kind}")(identifier)
    assert len(recorder.requests) == 1


@pytest.mark.parametrize("operation", ["pause", "resume", "delete", "workspace_delete"])
def test_mutations_require_empty_204_success(operation):
    """Lifecycle actions reject a success body that violates their empty-response contract."""
    payload = workspace_payload() if operation == "workspace_delete" else sandbox_payload()
    recorder = Recorder(httpx.Response(200, json=payload), httpx.Response(204, content=b"unexpected"))
    with recorder.client() as client:
        handle = client.get_workspace(WSP) if operation == "workspace_delete" else client.get_sandbox(SBX)
        with pytest.raises(ProtocolError):
            getattr(handle, "delete" if operation == "workspace_delete" else operation)()
    assert len(recorder.requests) == 2


def test_default_control_url_and_public_direct_endpoint_lookup():
    """The default local control URL and explicit endpoint lookup perform one read each."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()),
                        httpx.Response(200, json={"sandbox_id": SBX, "auth_required": True, "access_token": "a" * 43, "url": "http://127.0.0.1:65535/"}))
    with SandboxClient(transport=httpx.MockTransport(recorder)) as client:
        endpoint = client.get_sandbox(SBX).get_endpoint()
    assert endpoint in {"http://127.0.0.1:65535", "http://127.0.0.1:65535/"}
    assert [str(request.url) for request in recorder.requests] == [
        f"{CONTROL}/v1/sandboxes/{SBX}", f"{CONTROL}/v1/sandboxes/{SBX}/endpoint"]


def test_command_explicit_zero_timeout_reaches_agent_validation_unchanged():
    """A caller's explicit zero is never replaced with the SDK's or agent's default."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()),
                        httpx.Response(200, json={"sandbox_id": SBX, "auth_required": True, "access_token": "a" * 43, "url": "http://127.0.0.1:19001"}),
                        httpx.Response(400, json={"error": {"code": "invalid_request", "message": "invalid timeout"}}))
    with recorder.client() as client, pytest.raises(APIError) as caught:
        client.get_sandbox(SBX).run(["true"], timeout_ms=0)
    assert caught.value.status_code == 400
    assert json.loads(recorder.requests[-1].content)["timeout_ms"] == 0
    assert len(recorder.requests) == 3


def test_context_exception_closes_transport_and_preserves_original_error():
    """An application exception still releases connections without deleting created resources."""
    recorder = Recorder(httpx.Response(201, json=sandbox_payload()))
    transport = ClosingTransport(recorder)
    client = SandboxClient(CONTROL, transport=transport)
    with pytest.raises(RuntimeError, match="caller failure"):
        with client:
            client.create_sandbox()
            raise RuntimeError("caller failure")
    assert transport.close_count == 1
    assert len(recorder.requests) == 1 and recorder.requests[0].method == "POST"


@pytest.mark.parametrize("operation", ["get_sandbox", "readyz", "renew", "command", "get_workspace"])
def test_malformed_success_preserves_known_resource_diagnostic_id(operation):
    """Model validation failures retain the requested resource identity for follow-up."""
    initial = [] if operation.startswith("get_") else [httpx.Response(200, json=sandbox_payload())]
    if operation == "command":
        initial.append(httpx.Response(200, json={"sandbox_id": SBX, "auth_required": True, "access_token": "a" * 43, "url": "http://127.0.0.1:19001"}))
    recorder = Recorder(*initial, httpx.Response(200, json={}))
    with recorder.client() as client, pytest.raises(ProtocolError) as caught:
        if operation == "get_workspace":
            client.get_workspace(WSP)
        elif operation == "get_sandbox":
            client.get_sandbox(SBX)
        else:
            sandbox = client.get_sandbox(SBX)
            if operation == "readyz":
                sandbox.wait_ready(timeout=1)
            elif operation == "renew":
                sandbox.renew(60)
            else:
                sandbox.run(["true"])
    if operation == "get_workspace":
        assert caught.value.workspace_id == WSP
    else:
        assert caught.value.sandbox_id == SBX
    assert len(recorder.requests) == len(initial) + 1


@pytest.mark.parametrize("response,expected", [
    (httpx.Response(503, content=b"<html>private failure</html>"), APIError),
    (httpx.ReadTimeout("remote creation may have happened"), SDKTimeoutError),
    (httpx.Response(201, json={}), ProtocolError),
])
def test_workspace_bound_creation_failure_retains_known_workspace_id(response, expected):
    """Uncertain creation failures retain the caller's existing workspace identity."""
    recorder = Recorder(response)
    with recorder.client() as client, pytest.raises(expected) as caught:
        client.create_sandbox(workspace_id=WSP)
    assert caught.value.workspace_id == WSP
    assert len(recorder.requests) == 1
