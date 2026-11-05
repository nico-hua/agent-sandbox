"""Validate service connection discovery without widening Agent credential routing."""

from dataclasses import FrozenInstanceError

import httpx
import pytest

import agent_sandbox
from agent_sandbox import APIError, EndpointConnection, NetworkError, ProtocolError, Sandbox
from test_auth import KEY
from test_client import CONTROL, OTHER_SBX, SBX, Recorder

TOKEN = "a" * 43
OTHER_TOKEN = "B" * 43
ENTRY = "http://127.0.0.1:19001"
SERVICE = ENTRY + "/services/3000/"


def service_payload(**updates):
    """Provide the complete public service endpoint response with explicit overrides."""
    return {"sandbox_id": SBX, "port": 3000, "url": SERVICE,
            "auth_required": True, "access_token": TOKEN, **updates}


@pytest.mark.parametrize("port", [1024, 3000, 65535])
def test_service_lookup_returns_frozen_secret_safe_connection_without_app_request(port):
    """Discovery performs one admin GET and returns a separately typed credential."""
    url = f"{ENTRY}/services/{port}/"
    recorder = Recorder(httpx.Response(200, json=service_payload(port=port, url=url)))
    with recorder.client(api_key=KEY) as client:
        connection = Sandbox(client, SBX).get_service_connection(port)
        assert isinstance(connection, agent_sandbox.ServiceConnection)
        assert not isinstance(connection, EndpointConnection)
        assert (connection.sandbox_id, connection.port, connection.url,
                connection.auth_required, connection.access_token) == (SBX, port, url, True, TOKEN)
        assert TOKEN not in repr(connection)
        with pytest.raises(FrozenInstanceError):
            connection.access_token = OTHER_TOKEN
        assert "authorization" not in client._http.headers
        assert client._http.auth is None
    assert [(request.method, str(request.url)) for request in recorder.requests] == [
        ("GET", f"{CONTROL}/v1/sandboxes/{SBX}/services/{port}/endpoint")]
    assert recorder.requests[0].headers["authorization"] == "Bearer " + KEY
    assert TOKEN.encode() not in recorder.requests[0].content


@pytest.mark.parametrize("port", [None, True, False, "3000", 3000.0, 0, -1, 1023, 8080, 65536, 10**100])
def test_invalid_local_port_never_sends_a_request(port):
    """Port type, reserved Agent port and range errors fail before discovery."""
    recorder = Recorder()
    with recorder.client(api_key=KEY) as client, pytest.raises(ValueError):
        Sandbox(client, SBX).get_service_connection(port)
    assert not recorder.requests


@pytest.mark.parametrize("url", [
    ENTRY, SERVICE.rstrip("/"), ENTRY + "/services/3001/", ENTRY + "/services/03000/",
    ENTRY + "/services/+3000/", ENTRY + "/services/3000//", ENTRY + "/services/3000/next",
    ENTRY + "/services/%33%30%30%30/", ENTRY + "/%73ervices/3000/",
    ENTRY + "/services/3000/../3000/", ENTRY + "/services/3000/%2e/",
    SERVICE + "?", SERVICE + "?token=secret", SERVICE + "#", SERVICE + "#fragment",
    "https://127.0.0.1:19001/services/3000/", "HTTP://127.0.0.1:19001/services/3000/",
    "http://localhost:19001/services/3000/", "http://127.0.0.2:19001/services/3000/",
    "http://127.1:19001/services/3000/", "http://[::1]:19001/services/3000/",
    "http://user@127.0.0.1:19001/services/3000/",
    "http://user:secret@127.0.0.1:19001/services/3000/",
    "http://127.0.0.1:0/services/3000/", "http://127.0.0.1:65536/services/3000/",
    "http://127.0.0.1:019001/services/3000/", "http://127.0.0.1/services/3000/",
    "http://127.0.0.1:19001.evil.test/services/3000/", "//127.0.0.1:19001/services/3000/",
    " " + SERVICE, SERVICE + "\n", SERVICE.replace("/services", "\\services"), None, 3,
])
def test_malicious_or_ambiguous_response_url_is_rejected(url):
    """Only the exact local HTTP origin and canonical requested service path pass."""
    recorder = Recorder(httpx.Response(200, json=service_payload(url=url)))
    with recorder.client(api_key=KEY) as client, pytest.raises(ProtocolError) as caught:
        Sandbox(client, SBX).get_service_connection(3000)
    assert caught.value.sandbox_id == SBX
    assert TOKEN not in str(caught.value) + repr(caught.value)
    assert len(recorder.requests) == 1


@pytest.mark.parametrize("updates", [
    {"sandbox_id": OTHER_SBX}, {"sandbox_id": "bad"}, {"sandbox_id": None},
    {"port": 3001}, {"port": "3000"}, {"port": 3000.0}, {"port": True},
    {"port": 8080}, {"port": 1023}, {"port": 65536}, {"port": None},
    {"auth_required": False}, {"auth_required": 1}, {"auth_required": "true"},
    {"auth_required": None}, {"access_token": None}, {"access_token": True},
    {"access_token": "a" * 42}, {"access_token": "a" * 44},
    {"access_token": "a" * 42 + "+"}, {"access_token": "a" * 42 + "="},
    {"access_token": "a" * 42 + "\n"}, {"access_token": "a" * 42 + "é"},
])
def test_response_identity_and_credentials_are_strict(updates):
    """Response mismatches cannot authorize a different sandbox, port or auth mode."""
    recorder = Recorder(httpx.Response(200, json=service_payload(**updates)))
    with recorder.client(api_key=KEY) as client, pytest.raises(ProtocolError) as caught:
        Sandbox(client, SBX).get_service_connection(3000)
    assert caught.value.sandbox_id == SBX
    assert len(recorder.requests) == 1


@pytest.mark.parametrize("field", ["sandbox_id", "port", "url", "auth_required", "access_token"])
def test_missing_service_response_fields_fail_closed(field):
    """Every public connection field is required before exposing a credential."""
    payload = service_payload()
    del payload[field]
    recorder = Recorder(httpx.Response(200, json=payload))
    with recorder.client(api_key=KEY) as client, pytest.raises(ProtocolError):
        Sandbox(client, SBX).get_service_connection(3000)
    assert len(recorder.requests) == 1


@pytest.mark.parametrize("entry_port", [1, 65535])
def test_service_connection_accepts_entry_port_boundaries(entry_port):
    """Proxy entry ports have their own 1 through 65535 range."""
    url = f"http://127.0.0.1:{entry_port}/services/3000/"
    recorder = Recorder(httpx.Response(200, json=service_payload(url=url)))
    with recorder.client() as client:
        assert Sandbox(client, SBX).get_service_connection(3000).url == url
    assert "authorization" not in recorder.requests[0].headers


def test_each_lookup_reads_current_instance_connection_without_caching():
    """Repeated and cross-sandbox lookups retain their own fresh response identity."""
    recorder = Recorder(
        httpx.Response(200, json=service_payload()),
        httpx.Response(200, json=service_payload(access_token=OTHER_TOKEN)),
        httpx.Response(200, json=service_payload(sandbox_id=OTHER_SBX, access_token="c" * 43)),
    )
    with recorder.client(api_key=KEY) as client:
        sandbox = Sandbox(client, SBX)
        assert sandbox.get_service_connection(3000).access_token == TOKEN
        assert sandbox.get_service_connection(3000).access_token == OTHER_TOKEN
        assert Sandbox(client, OTHER_SBX).get_service_connection(3000).access_token == "c" * 43
    assert len(recorder.requests) == 3
    assert all(request.headers["authorization"] == "Bearer " + KEY for request in recorder.requests)


@pytest.mark.parametrize("status", [401, 307, 409, 504])
def test_service_api_errors_and_redirects_are_not_retried(status):
    """A failed lookup preserves normal API errors without redirecting credentials."""
    recorder = Recorder(httpx.Response(status, headers={"location": "http://evil.test/collect"},
                                     json={"error": {"code": "service_error", "message": "failed"}}))
    with recorder.client(api_key=KEY) as client, pytest.raises(APIError) as caught:
        Sandbox(client, SBX).get_service_connection(3000)
    assert (caught.value.status_code, caught.value.code, caught.value.sandbox_id) == (status, "service_error", SBX)
    assert len(recorder.requests) == 1


def test_service_network_error_is_not_retried():
    """A transport failure makes no second discovery or application attempt."""
    recorder = Recorder(httpx.ConnectError("offline"))
    with recorder.client(api_key=KEY) as client, pytest.raises(NetworkError) as caught:
        Sandbox(client, SBX).get_service_connection(3000)
    assert caught.value.sandbox_id == SBX
    assert len(recorder.requests) == 1


def test_service_model_cannot_widen_agent_connections_or_data_request_authority():
    """Service credentials never become accepted Agent endpoint connections."""
    recorder = Recorder(httpx.Response(200, json=service_payload()))
    with recorder.client(api_key=KEY) as client:
        connection = Sandbox(client, SBX).get_service_connection(3000)
        with pytest.raises(ProtocolError):
            EndpointConnection(SBX, SERVICE, True, TOKEN)
        with pytest.raises(ValueError):
            client._request("GET", SERVICE + "v1/files", expected=200, control_plane=False,
                            sandbox_id=SBX, connection=connection)
    assert len(recorder.requests) == 1


@pytest.mark.parametrize("updates", [{"port": True}, {"port": 8080}, {"url": ENTRY},
                                           {"auth_required": 1}, {"access_token": b"a" * 43}])
def test_direct_service_model_construction_checks_security_fields(updates):
    """Callers cannot bypass validation by constructing the exported frozen model."""
    with pytest.raises(ProtocolError):
        agent_sandbox.ServiceConnection(**service_payload(**updates))


def test_service_lookup_same_origin_still_uses_only_admin_authority():
    """Matching proxy and control origins cannot change the selected auth channel."""
    recorder = Recorder(httpx.Response(200, json=service_payload(url=CONTROL + "/services/3000/")),
                        httpx.Response(200, json={"sandbox_id": SBX, "url": ENTRY,
                                                 "auth_required": True, "access_token": OTHER_TOKEN}),
                        httpx.Response(200, json={"exit_code": 0, "stdout": "ok", "stderr": ""}))
    with recorder.client(api_key=KEY) as client:
        sandbox = Sandbox(client, SBX)
        assert sandbox.get_service_connection(3000).access_token == TOKEN
        assert sandbox.run(["true"]).stdout == "ok"
    assert [request.headers["authorization"] for request in recorder.requests] == [
        "Bearer " + KEY, "Bearer " + KEY, "Bearer " + OTHER_TOKEN]
    assert str(recorder.requests[2].url) == ENTRY + "/v1/commands:run"
