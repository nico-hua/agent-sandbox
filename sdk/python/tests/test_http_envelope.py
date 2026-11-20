"""Verify the wire envelope through the SDK's public HTTP methods."""

import json

import httpx
import pytest

from agent_sandbox import APIError, ProtocolError
from test_client import SBX, Recorder, sandbox_payload


def test_create_unpacks_http_200_and_preserves_sandbox_fields():
    """Create returns a handle populated from the business object inside data."""
    recorder = Recorder(httpx.Response(
        200, json={"code": 200, "message": "ok", "data": sandbox_payload(status="started")},
    ))
    with recorder.client() as client:
        sandbox = client.create_sandbox()
    assert sandbox.sandbox_id == SBX and sandbox.info.status == "started"


@pytest.mark.parametrize("envelope", [
    {"message": "ok", "data": {}},
    {"code": 200, "data": {}},
    {"code": 200, "message": "ok"},
    {"code": "200", "message": "ok", "data": {}},
    {"code": 200.0, "message": "ok", "data": {}},
    {"code": True, "message": "ok", "data": {}},
    {"code": 201, "message": "ok", "data": {}},
    {"code": 200, "message": 200, "data": {}},
    {"code": 200, "message": "success", "data": {}},
    {"code": 200, "message": "ok", "data": {}, "extra": True},
])
def test_read_rejects_malformed_success_envelopes(envelope):
    """Envelope validation must run before interpreting otherwise valid business data."""
    if "data" in envelope:
        envelope = {**envelope, "data": sandbox_payload()}
    recorder = Recorder(httpx.Response(200, json=envelope))
    with recorder.client() as client, pytest.raises(ProtocolError) as captured:
        client.get_sandbox(SBX)
    assert captured.value.sandbox_id == SBX


def test_read_rejects_duplicate_envelope_fields():
    """A valid later code cannot hide a conflicting HTTP code in the same object."""
    recorder = Recorder(httpx.Response(
        200, content='{"code":201,"code":200,"message":"ok","data":' + json.dumps(sandbox_payload()) + '}',
    ))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.get_sandbox(SBX)


def test_error_envelope_exposes_category_and_diagnostic_identity():
    """A valid conflict envelope preserves HTTP status, message and diagnostic identity."""
    recorder = Recorder(httpx.Response(
        409, json={"code": 409, "message": "sandbox renewal conflict", "data": {"sandbox_id": SBX}},
    ))
    with recorder.client() as client, pytest.raises(APIError) as captured:
        client.create_sandbox()
    error = captured.value
    assert (error.status_code, error.code, error.message, error.sandbox_id) == (
        409, "conflict", "sandbox renewal conflict", SBX,
    )


@pytest.mark.parametrize("code", [504, "503", 503.0, False])
def test_bad_error_code_cannot_supply_retry_reason_or_diagnostic_data(code):
    """Malformed proxy errors retain the actual status and use only generic safe diagnostics."""
    recorder = Recorder(httpx.Response(503, json={
        "code": code, "message": "sensitive proxy error",
        "data": {"reason": "sandbox_not_ready", "sandbox_id": SBX},
    }))
    with recorder.client() as client, pytest.raises(APIError) as captured:
        client.create_sandbox()
    error = captured.value
    assert (error.status_code, error.code, error.message, error.sandbox_id) == (
        503, "http_error", "HTTP request failed", None,
    )
