"""Check SDK error classification and safe HTTP error diagnostics."""

import pytest

from agent_sandbox.errors import (
    APIError,
    ClientClosedError,
    NetworkError,
    ProtocolError,
    ReadinessTimeoutError,
    SDKError,
    SDKTimeoutError,
    parse_api_error,
)

SBX = "sbx_" + "a" * 32
WSP = "wsp_" + "b" * 32


def test_api_error_preserves_message_and_ids_without_printing_message():
    """The status category is printable and server messages stay in diagnostics."""
    error = parse_api_error(
        409,
        {
            "code": 409,
            "message": "secret-command",
            "data": {
                "reason": "future_conflict",
                "sandbox_id": SBX,
                "workspace_id": WSP,
            }
        },
    )
    assert isinstance(error, APIError)
    assert isinstance(error, SDKError)
    assert (error.status_code, error.code, error.message) == (
        409, "conflict", "secret-command"
    )
    assert (error.sandbox_id, error.workspace_id) == (SBX, WSP)
    assert "409" in str(error) and "conflict" in str(error)
    assert "secret-command" not in str(error)
    assert "secret-command" not in repr(error)


@pytest.mark.parametrize(
    "payload",
    [
        None,
        "<html>secret</html>",
        [],
        {},
        {"error": {"code": "old_proxy_error", "message": "secret"}},
        {"code": 500, "message": "secret", "data": []},
        {"code": 500, "message": "secret", "data": True},
        {"code": 500, "message": False, "data": None},
        {"code": 500, "data": None},
        {"code": 500, "message": "secret", "data": None, "extra": True},
    ],
)
def test_malformed_error_keeps_status_and_known_ids(payload):
    """Malformed bodies still report their HTTP status and known resources."""
    error = parse_api_error(500, payload, sandbox_id=SBX, workspace_id=WSP)
    assert (error.status_code, error.code, error.message) == (
        500, "http_error", "HTTP request failed"
    )
    assert (error.sandbox_id, error.workspace_id) == (SBX, WSP)
    assert "secret" not in str(error)


def test_error_diagnostics_ignore_wrong_types_and_preserve_known_ids():
    """Invalid optional diagnostic values cannot replace request context."""
    error = parse_api_error(
        404,
        {
            "code": 404,
            "message": "gone",
            "data": {
                "sandbox_id": 9,
                "workspace_id": None,
            }
        },
        sandbox_id=SBX,
        workspace_id=WSP,
    )
    assert (error.sandbox_id, error.workspace_id) == (SBX, WSP)


@pytest.mark.parametrize("code", ["future\nsecret-command", "x" * 1024])
def test_unsafe_or_unbounded_error_codes_are_data_only(code):
    """Explicitly constructed APIError retains its public constructor behavior."""
    error = APIError(500, code, "secret")
    assert error.code == code
    assert str(error) == "HTTP 500: http_error"
    assert code not in repr(error)


@pytest.mark.parametrize("error_type", [NetworkError, SDKTimeoutError, ProtocolError])
def test_local_error_printing_is_safe_and_ids_are_available(error_type):
    """Locally classified failures never print arbitrary diagnostic text."""
    error = error_type("secret-command", sandbox_id=SBX, workspace_id=WSP)
    assert isinstance(error, SDKError)
    assert (error.sandbox_id, error.workspace_id) == (SBX, WSP)
    assert "secret-command" not in str(error)
    assert "secret-command" not in repr(error)


def test_readiness_timeout_is_distinct_and_keeps_sandbox_id():
    """A local readiness deadline can be distinguished from request timeouts."""
    error = ReadinessTimeoutError(SBX)
    assert isinstance(error, SDKError)
    assert not isinstance(error, NetworkError)
    assert error.sandbox_id == SBX
    assert isinstance(SDKTimeoutError(), NetworkError)
    assert isinstance(ClientClosedError(), SDKError)


@pytest.mark.parametrize("message", [
    "sandbox instance limit exceeded",
    "sandbox creation capacity exceeded",
])
def test_creation_capacity_errors_share_category_and_are_not_retried(message):
    """Each capacity response supplies its own message alongside the HTTP category."""
    from test_client import Recorder, failure_response

    recorder = Recorder(failure_response(429, message))
    with recorder.client() as client, pytest.raises(APIError) as captured:
        client.create_sandbox(environment="tools")
    assert (captured.value.status_code, captured.value.code, captured.value.message) == (
        429, "too_many_requests", message,
    )
    assert captured.value.sandbox_id is None
    assert len(recorder.requests) == 1
