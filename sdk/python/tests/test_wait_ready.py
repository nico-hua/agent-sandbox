"""Deterministic readiness polling without real time, sockets or lifecycle cleanup."""

import math

import httpx
import pytest

from agent_sandbox import APIError, NetworkError, ProtocolError, ReadinessTimeoutError
import agent_sandbox.client as client_module
from test_client import SBX, Recorder, sandbox_payload


class Clock:
    """Provide a monotonic clock whose only progression is explicit test-controlled sleep."""

    def __init__(self):
        """Initialize the deterministic budget and record all requested sleeps."""
        self.now = 0.0
        self.sleeps = []

    def monotonic(self):
        """Return the synthetic current monotonic time."""
        return self.now

    def sleep(self, duration):
        """Advance time exactly by the SDK's requested polling delay."""
        self.sleeps.append(duration)
        self.now += duration


@pytest.fixture
def clock(monkeypatch):
    """Patch only the SDK's imported clock functions, preserving real HTTP handling."""
    result = Clock()
    monkeypatch.setattr(client_module, "monotonic", result.monotonic)
    monkeypatch.setattr(client_module, "sleep", result.sleep)
    return result


def ready_response():
    """Return a complete readyz success payload for the requested sandbox."""
    return httpx.Response(200, json={"sandbox_id": SBX, "status": "ready"})


def error_response(status, code):
    """Build the exact structured HTTP failure used to test readiness polling policy."""
    return httpx.Response(status, json={"error": {"code": code, "message": "not ready"}})


@pytest.mark.parametrize("status,code", [
    (503, "sandbox_not_ready"), (503, "sandbox_endpoint_unavailable"),
    (503, "sandbox_agent_unavailable"), (504, "sandbox_readiness_timeout"),
    (503, "docker_unavailable"),
])
def test_only_exact_transient_pairs_are_polled(status, code, clock):
    """Every documented temporary pair can recover solely through readyz polling."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()),
                        error_response(status, code), ready_response())
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        result = sandbox.wait_ready(timeout=2, poll_interval=0.25)
    assert result.sandbox_id == SBX and result.status == "ready"
    assert [(request.method, request.url.path) for request in recorder.requests] == [
        ("GET", f"/v1/sandboxes/{SBX}"),
        ("GET", f"/v1/sandboxes/{SBX}/readyz"),
        ("GET", f"/v1/sandboxes/{SBX}/readyz")]
    assert clock.sleeps == [0.25]


def test_ready_success_has_no_preflight_or_post_success_refresh(clock):
    """Immediate readiness sends one readyz request and performs no hidden refresh."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()), ready_response())
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        sandbox.wait_ready()
    assert len(recorder.requests) == 2
    assert recorder.requests[-1].url.path.endswith("/readyz")
    assert not clock.sleeps


def test_zero_budget_makes_no_readiness_request_or_cleanup(clock):
    """An exhausted caller budget fails before any new HTTP request."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()))
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        with pytest.raises(ReadinessTimeoutError) as caught:
            sandbox.wait_ready(timeout=0)
    assert caught.value.sandbox_id == SBX
    assert len(recorder.requests) == 1 and not clock.sleeps


def test_poll_sleep_is_capped_by_remaining_budget(clock):
    """The last delay uses the remaining budget and never starts an expired request."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()),
                        error_response(503, "sandbox_not_ready"),
                        error_response(503, "sandbox_not_ready"))
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        with pytest.raises(ReadinessTimeoutError):
            sandbox.wait_ready(timeout=0.3, poll_interval=0.25)
    assert clock.sleeps == pytest.approx([0.25, 0.05])
    assert len(recorder.requests) == 3
    assert clock.now == pytest.approx(0.3)
    assert all(request.method == "GET" for request in recorder.requests)
    assert all(request.url.path.endswith("/readyz") for request in recorder.requests[1:])


def test_each_request_phase_timeout_is_finite_and_capped_by_remaining(clock):
    """Poll requests cap configured connect/read/write/pool timeouts by current budget."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()),
                        error_response(503, "sandbox_not_ready"), ready_response())
    configured = httpx.Timeout(connect=1.0, read=5.0, write=3.0, pool=4.0)
    with recorder.client(timeout=configured) as client:
        client.get_sandbox(SBX).wait_ready(timeout=0.4, poll_interval=0.25)
    first, second = [request.extensions["timeout"] for request in recorder.requests[1:]]
    assert first.keys() == second.keys() == {"connect", "read", "write", "pool"}
    assert all(math.isfinite(value) and 0 < value <= 0.4 for value in first.values())
    assert all(math.isfinite(value) and 0 < value <= 0.15 + 1e-12 for value in second.values())


@pytest.mark.parametrize("status,code", [
    (404, "sandbox_not_found"), (409, "sandbox_not_ready"),
    (429, "docker_unavailable"), (500, "sandbox_not_ready"),
    (504, "sandbox_not_ready"), (503, "sandbox_readiness_timeout"),
    (503, "sandbox_endpoint_timeout"), (503, "new_transient_maybe"),
    (400, "invalid_request"),
])
def test_non_transient_http_pairs_fail_immediately_without_cleanup(status, code, clock):
    """Readiness does not broaden retry policy by status, similar name or future code."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()), error_response(status, code))
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        with pytest.raises(APIError) as caught:
            sandbox.wait_ready(timeout=10)
    assert (caught.value.status_code, caught.value.code) == (status, code)
    assert len(recorder.requests) == 2 and not clock.sleeps
    assert all(request.method == "GET" for request in recorder.requests)


@pytest.mark.parametrize("response,expected", [
    (httpx.Response(200, content=b"bad JSON"), ProtocolError),
    (httpx.Response(200, json={"sandbox_id": SBX, "status": "starting"}), ProtocolError),
    (httpx.Response(200, json={"sandbox_id": "sbx_" + "9" * 32, "status": "ready"}), ProtocolError),
    (httpx.Response(503, content=b"not JSON"), APIError),
    (httpx.ReadTimeout("private details"), NetworkError),
    (httpx.ConnectError("private details"), NetworkError),
])
def test_protocol_and_network_failures_are_not_polled(response, expected, clock):
    """Malformed readiness and transport failures escape immediately without a retry."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()), response)
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        with pytest.raises(expected):
            sandbox.wait_ready(timeout=10)
    assert len(recorder.requests) == 2 and not clock.sleeps
    assert all(request.method == "GET" for request in recorder.requests)


@pytest.mark.parametrize("kwargs", [
    {"timeout": -1}, {"timeout": float("inf")}, {"timeout": float("nan")},
    {"timeout": None}, {"timeout": True}, {"poll_interval": 0},
    {"poll_interval": -1}, {"poll_interval": float("inf")},
    {"poll_interval": float("nan")}, {"poll_interval": None},
    {"poll_interval": False},
])
def test_invalid_poll_budget_is_local(kwargs, clock):
    """Nonfinite, ambiguous or invalid polling values fail before readyz HTTP."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()))
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        with pytest.raises((ValueError, TypeError)):
            sandbox.wait_ready(**kwargs)
    assert len(recorder.requests) == 1 and not clock.sleeps
