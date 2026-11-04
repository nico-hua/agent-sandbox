"""Fail closed on endpoint credentials and separate instance and admin authority."""

from dataclasses import FrozenInstanceError

import httpx
import pytest

from agent_sandbox import APIError, EndpointConnection, ProtocolError, Sandbox
from test_auth import KEY
from test_client import OTHER_SBX, SBX, Recorder

TOKEN_A = "a" * 43
TOKEN_B = "B" * 43
ENDPOINT = "http://127.0.0.1:19001"


def endpoint_payload(**updates):
    """Return a complete protected endpoint response with focused overrides."""
    return {"sandbox_id": SBX, "url": ENDPOINT, "auth_required": True,
            "access_token": TOKEN_A, **updates}


def test_connection_is_immutable_validated_and_secret_safe():
    """Connection lookup exports a frozen credential whose repr hides its token."""
    recorder = Recorder(httpx.Response(200, json=endpoint_payload(url=ENDPOINT + "/")),
                        httpx.Response(200, json=endpoint_payload()))
    with recorder.client(api_key=KEY) as client:
        sandbox = Sandbox(client, SBX)
        connection = sandbox.get_connection()
        assert isinstance(connection, EndpointConnection)
        assert (connection.sandbox_id, connection.url, connection.auth_required) == (SBX, ENDPOINT, True)
        assert connection.access_token == TOKEN_A
        assert TOKEN_A not in repr(connection)
        assert sandbox.get_endpoint() == ENDPOINT
        with pytest.raises(FrozenInstanceError):
            connection.access_token = TOKEN_B
    assert len(recorder.requests) == 2


@pytest.mark.parametrize("updates,missing", [
    ({}, "auth_required"), ({}, "access_token"),
    ({"auth_required": False}, None), ({"auth_required": 1}, None),
    ({"auth_required": "true"}, None), ({"auth_required": None}, None),
    ({"access_token": None}, None), ({"access_token": "a" * 42}, None),
    ({"access_token": "a" * 44}, None), ({"access_token": "a" * 42 + "+"}, None),
    ({"access_token": "a" * 42 + "\n"}, None), ({"access_token": b"a" * 43}, None),
    ({"access_token": True}, None), ({"sandbox_id": OTHER_SBX}, None),
])
def test_missing_false_or_malformed_auth_never_reaches_agent(updates, missing):
    """Untrusted credentials cannot downgrade data operations to anonymous requests."""
    payload = endpoint_payload(**updates)
    if missing:
        del payload[missing]
    # Bytes are impossible in JSON; exercise model parsing directly for that type.
    if isinstance(payload.get("access_token"), bytes):
        with pytest.raises(ProtocolError):
            EndpointConnection.from_payload(payload, expected_id=SBX)
        return
    recorder = Recorder(httpx.Response(200, json=payload),
                        httpx.Response(200, json={"exit_code": 0, "stdout": "", "stderr": ""}))
    with recorder.client(api_key=KEY) as client, pytest.raises(ProtocolError) as caught:
        Sandbox(client, SBX).run(["true"])
    assert caught.value.sandbox_id == SBX
    assert TOKEN_A not in str(caught.value) + repr(caught.value)
    assert len(recorder.requests) == 1


def test_each_operation_reads_its_current_instance_credential():
    """Rotations and different handles never reuse another operation's token."""
    recorder = Recorder(
        httpx.Response(200, json=endpoint_payload()), httpx.Response(200, json={"exit_code": 0, "stdout": "", "stderr": ""}),
        httpx.Response(200, json=endpoint_payload(access_token=TOKEN_B)), httpx.Response(201),
        httpx.Response(200, json=endpoint_payload(sandbox_id=OTHER_SBX, access_token="c" * 43)), httpx.Response(200, content=b"ok"),
    )
    with recorder.client(api_key=KEY) as client:
        first, second = Sandbox(client, SBX), Sandbox(client, OTHER_SBX)
        first.run(["true"], env={"TASK_VALUE": "visible"})
        first.write_file("a", b"value")
        assert second.read_file("a") == b"ok"
        assert "authorization" not in client._http.headers
        assert client._http.auth is None
    assert [r.headers["authorization"] for r in recorder.requests] == [
        "Bearer " + KEY, "Bearer " + TOKEN_A, "Bearer " + KEY, "Bearer " + TOKEN_B,
        "Bearer " + KEY, "Bearer " + "c" * 43,
    ]
    for request in recorder.requests:
        assert TOKEN_A.encode() not in request.content and KEY.encode() not in request.content
        assert "access_token" not in request.url.params


@pytest.mark.parametrize("status", [401, 307])
def test_agent_auth_failures_and_redirects_are_not_retried(status):
    """Agent rejection preserves its API error without replaying instance credentials."""
    recorder = Recorder(httpx.Response(200, json=endpoint_payload()), httpx.Response(
        status, headers={"location": "http://evil.test/collect"},
        json={"error": {"code": "unauthorized", "message": "authentication required"}}))
    with recorder.client(api_key=KEY) as client, pytest.raises(APIError) as caught:
        Sandbox(client, SBX).run(["true"])
    assert (caught.value.status_code, caught.value.sandbox_id) == (status, SBX)
    assert TOKEN_A not in str(caught.value) + repr(caught.value)
    assert len(recorder.requests) == 2


def test_data_request_requires_an_explicit_validated_connection():
    """Internal callers cannot issue anonymous data requests or route tokens elsewhere."""
    recorder = Recorder(httpx.Response(200, json={}))
    with recorder.client(api_key=KEY) as client:
        with pytest.raises((ValueError, ProtocolError)):
            client._request("GET", ENDPOINT + "/v1/files", expected=200, control_plane=False)
    assert not recorder.requests


@pytest.mark.parametrize("target,identity", [
    ("http://127.0.0.1:19002/v1/files", SBX),
    ("http://127.0.0.1:190010/v1/files", SBX),
    (ENDPOINT + "/v1/files", OTHER_SBX),
])
def test_connection_cannot_authorize_another_origin_or_sandbox(target, identity):
    """Explicit connections constrain token delivery to their validated routing identity."""
    recorder = Recorder()
    connection = EndpointConnection(SBX, ENDPOINT, True, TOKEN_A)
    with recorder.client(api_key=KEY) as client, pytest.raises(ValueError):
        client._request("GET", target, expected=200, control_plane=False,
                        sandbox_id=identity, connection=connection)
    assert not recorder.requests


@pytest.mark.parametrize("authorization,accepted", [
    (None, False), ("Bearer " + KEY, False), ("Bearer " + TOKEN_A, True),
])
def test_real_http_audit_requires_instance_auth_without_saving_values(monkeypatch, authorization, accepted):
    """Integration instrumentation rejects absent/admin auth and records only booleans."""
    from test_integration import AgentAuthorizationAudit

    recorder = Recorder(httpx.Response(200))

    def local_transport(**kwargs):
        """Substitute only network I/O below the real authorization audit."""
        return httpx.MockTransport(recorder)

    monkeypatch.setattr(httpx, "HTTPTransport", local_transport)
    monkeypatch.setenv("SDK_TEST_API_KEY", KEY)
    audit = AgentAuthorizationAudit()
    headers = {} if authorization is None else {"authorization": authorization}
    request = httpx.Request("GET", ENDPOINT + "/v1/files", headers=headers)
    try:
        if accepted:
            audit.handle_request(request)
            assert audit.authorization_seen
            assert not audit.admin_authorization_seen
            assert len(recorder.requests) == 1
        else:
            with pytest.raises(AssertionError):
                audit.handle_request(request)
            assert not recorder.requests
        assert TOKEN_A not in repr(audit.__dict__)
        assert KEY not in repr(audit.__dict__)
    finally:
        audit.close()
