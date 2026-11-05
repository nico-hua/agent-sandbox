"""Independent managed-list models, single-page requests and admin credential routing."""

import base64
import json
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import httpx
import pytest

from agent_sandbox import APIError, ClientClosedError, ProtocolError
from test_client import Recorder, SBX, OTHER_SBX, WSP, STAMP, CONTROL


def cursor_for(sandbox_id=SBX, micros=None):
    """Encode a synthetic cursor independently of server implementation imports."""
    delta = datetime.fromisoformat(STAMP) - datetime(1970, 1, 1, tzinfo=UTC)
    if micros is None:
        micros = (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds
    return "v1." + base64.urlsafe_b64encode(json.dumps([micros, sandbox_id], separators=(",", ":")).encode()).decode().rstrip("=")


def summary(**updates):
    """Build a complete management summary rather than runtime status metadata."""
    return {"sandbox_id": SBX, "management_state": "pending", "created_at": STAMP,
            "expires_at": STAMP, "workspace_id": None, **updates}


def payload(**updates):
    """Build one complete response with nullable continuation and adjustable counts."""
    return {"items": [summary()], "next_cursor": None, "managed_count": 1, "max_instances": 10, **updates}


def test_authenticated_list_is_one_page_with_immutable_public_models():
    """The SDK returns immutable management facts and never automatically follows cursors."""
    recorder = Recorder(httpx.Response(200, json=payload(next_cursor=cursor_for(), managed_count=2)))
    with recorder.client(api_key="test-admin") as client:
        page = client.list_sandboxes(limit=1)
        assert type(page).__name__ == "SandboxListPage"
        assert type(page.items[0]).__name__ == "SandboxManagementSummary"
        assert page.items[0].created_at == datetime.fromisoformat(STAMP)
        assert page.items[0].management_state == "pending" and page.items[0].workspace_id is None
        assert page.next_cursor == cursor_for() and isinstance(page.items, tuple)
        with pytest.raises(FrozenInstanceError):
            page.managed_count = 0
        with pytest.raises(FrozenInstanceError):
            page.items[0].management_state = "ready"
    assert len(recorder.requests) == 1
    request = recorder.requests[0]
    assert request.method == "GET" and str(request.url).split("?")[0] == CONTROL + "/v1/sandboxes"
    assert dict(request.url.params) == {"limit": "1"}
    assert request.headers["authorization"] == "Bearer test-admin"


@pytest.mark.parametrize("limit", [1, 20, 100])
def test_parameters_and_managed_count_above_capacity(limit):
    """An existing excess count is valid, and each supplied cursor is query encoded."""
    recorder = Recorder(httpx.Response(200, json=payload(items=[], managed_count=30, max_instances=1)))
    with recorder.client() as client:
        page = client.list_sandboxes(limit=limit, cursor=cursor_for())
    assert page.managed_count == 30 and not page.items
    assert dict(recorder.requests[0].url.params) == {"limit": str(limit), "cursor": cursor_for()}
    assert "authorization" not in recorder.requests[0].headers


def test_default_parameters_do_not_send_null_cursor():
    """No cursor denotes the first page without a stringified null parameter."""
    recorder = Recorder(httpx.Response(200, json=payload()))
    with recorder.client() as client:
        client.list_sandboxes()
    assert dict(recorder.requests[0].url.params) == {"limit": "20"}


@pytest.mark.parametrize("state", ["pending", "active", "deleting"])
def test_states_workspace_and_extra_fields(state):
    """Every management state is accepted, with optional binding and forward-compatible fields."""
    recorder = Recorder(httpx.Response(200, json=payload(items=[summary(management_state=state, workspace_id=WSP, extra="future")], extra="future")))
    with recorder.client() as client:
        page = client.list_sandboxes()
    assert page.items[0].workspace_id == WSP and page.items[0].management_state == state


@pytest.mark.parametrize("options", [
    {"limit": True}, {"limit": 0}, {"limit": -1}, {"limit": 101}, {"limit": "20"},
    {"limit": 1.0}, {"limit": None}, {"cursor": 1}, {"cursor": ""},
    {"cursor": "v2.abcd"}, {"cursor": "x" * 257}, {"cursor": "v1.@@"},
])
def test_invalid_local_parameters_do_not_send_requests(options):
    """Strict local input checks prevent malformed pagination without HTTP side effects."""
    recorder = Recorder()
    with recorder.client() as client, pytest.raises(ValueError):
        client.list_sandboxes(**options)
    assert not recorder.requests


@pytest.mark.parametrize("updates", [
    {"sandbox_id": "../escape"}, {"sandbox_id": None}, {"management_state": "ready"},
    {"management_state": None}, {"created_at": None}, {"created_at": "not-time"},
    {"created_at": "2026-11-03T01:02:03.1234567Z"},
    {"created_at": "2026-11-03T01:02:03+08:00"}, {"expires_at": "2020-01-01T00:00:00Z"},
    {"workspace_id": "volume-name"},
])
def test_invalid_summary_is_protocol_error(updates):
    """IDs, management state and microsecond UTC time pairs are checked strictly."""
    recorder = Recorder(httpx.Response(200, json=payload(items=[summary(**updates)])))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.list_sandboxes()


@pytest.mark.parametrize("field", ["sandbox_id", "management_state", "created_at", "expires_at", "workspace_id"])
def test_missing_summary_field_is_protocol_error(field):
    """Nullable fields still must be explicitly present in a management response."""
    row = summary()
    del row[field]
    recorder = Recorder(httpx.Response(200, json=payload(items=[row])))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.list_sandboxes()


@pytest.mark.parametrize("updates", [
    {"items": None}, {"items": {}}, {"items": [None]}, {"managed_count": True},
    {"managed_count": -1}, {"managed_count": 0}, {"managed_count": "1"},
    {"max_instances": True}, {"max_instances": 0}, {"max_instances": 1025},
    {"max_instances": 1.0}, {"next_cursor": ""}, {"next_cursor": "v2.abcd"},
    {"next_cursor": "x" * 257}, {"next_cursor": cursor_for(OTHER_SBX)},
    {"items": [summary(), summary()]},
    {"items": [summary(sandbox_id=OTHER_SBX), summary()]},
])
def test_invalid_page_is_protocol_error(updates):
    """Bad counts, ordering, entries or continuation never become a successful SDK page."""
    recorder = Recorder(httpx.Response(200, json=payload(**updates)))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.list_sandboxes()


@pytest.mark.parametrize("field", ["items", "managed_count", "max_instances", "next_cursor"])
def test_missing_page_field_is_protocol_error(field):
    """Pagination metadata is not guessed when a required field is missing."""
    body = payload()
    del body[field]
    recorder = Recorder(httpx.Response(200, json=body))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.list_sandboxes()


def test_response_cannot_exceed_limit_or_replay_before_cursor():
    """Page contents honor both the requested size and exclusive starting position."""
    recorder = Recorder(httpx.Response(200, json=payload(items=[summary(), summary(sandbox_id=OTHER_SBX)], managed_count=2)), httpx.Response(200, json=payload()))
    with recorder.client() as client:
        with pytest.raises(ProtocolError):
            client.list_sandboxes(limit=1)
        with pytest.raises(ProtocolError):
            client.list_sandboxes(cursor=cursor_for(micros=2000000000000000))


@pytest.mark.parametrize("status", [400, 401, 429, 503, 504, 307])
def test_api_errors_and_redirects_are_not_retried(status):
    """API errors retain stable diagnostics and redirects never replay credentials."""
    recorder = Recorder(httpx.Response(status, json={"error": {"code": "future_error", "message": "safe"}}, headers={"location": "http://evil.test"}))
    with recorder.client(api_key="admin") as client, pytest.raises(APIError) as caught:
        client.list_sandboxes()
    assert caught.value.status_code == status and caught.value.code == "future_error"
    assert len(recorder.requests) == 1


def test_closed_client_does_not_list_or_delete_resources():
    """Connection shutdown has no remote effects and blocks subsequent listing."""
    recorder = Recorder()
    client = recorder.client()
    client.close()
    with pytest.raises(ClientClosedError):
        client.list_sandboxes()
    assert not recorder.requests


@pytest.mark.parametrize("updates,limit", [
    ({"items": [], "managed_count": 1}, 20),
    ({"managed_count": 2}, 1),
    ({"managed_count": 2, "next_cursor": cursor_for()}, 20),
])
def test_first_page_cannot_hide_records_or_end_prematurely(updates, limit):
    """A first page's count and continuation must describe the same complete snapshot."""
    recorder = Recorder(httpx.Response(200, json=payload(**updates)))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.list_sandboxes(limit=limit)
