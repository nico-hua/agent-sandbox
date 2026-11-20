"""HTTP renewal contracts using a temporary lifecycle database and fake runtime."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

import control_plane.services.sandbox_service as service_module
from control_plane.app import create_app
from control_plane.domain.errors import DockerUnavailable, SandboxNotFound, SandboxQueryFailed
from control_plane.domain.models import SandboxLifecycleRecord, format_utc_datetime
from control_plane.storage.base import LifecycleStoreError
from control_plane.storage.sqlite_store import SQLiteLifecycleStore
from control_plane.tests.http_assertions import assert_failure, success_data
from control_plane.tests.fakes import APIRuntime as FakeRuntime


NOW = datetime(2026, 10, 19, 8, 0, 0, 123456, tzinfo=UTC)
SANDBOX_ID = "sbx_" + "1" * 32
TRANSACTION_ID = "txn_" + "2" * 32
INVALID_REQUEST = {
    "code": 400,
    "message": "request body must contain only ttl_seconds as an integer from 1 to 86400",
    "data": None,
}


@pytest.fixture
async def renewal_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Provide real temporary persistence and a fixed clock without automatic reaping."""
    monkeypatch.setattr(service_module, "_utc_now", lambda: NOW)
    runtime = FakeRuntime()
    application = create_app(
        runtime, allow_unauthenticated=True, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False,
    )
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as client:
            yield application, client, runtime


async def seed_record(application, runtime, *, state="active", expires_at=None):
    """Set matching runtime facts and persist a chosen management-state fixture."""
    record = SandboxLifecycleRecord(
        sandbox_id=SANDBOX_ID, transaction_id=TRANSACTION_ID,
        created_at=NOW - timedelta(seconds=1800),
        expires_at=expires_at if expires_at is not None else NOW + timedelta(seconds=1),
    )
    store = application.state.lifecycle_store
    await store.create(record)
    if state != "pending":
        record = await store.activate(
            SANDBOX_ID, TRANSACTION_ID, expected_version=record.version,
        )
    if state == "deleting":
        record = await store.mark_deleting(
            SANDBOX_ID, TRANSACTION_ID, expected_version=record.version,
        )
    runtime.result = replace(
        runtime.result, sandbox_id=record.sandbox_id,
        transaction_id=record.transaction_id,
        created_at=record.created_at, expires_at=record.expires_at,
    )
    return record


@pytest.mark.anyio
async def test_renew_returns_saved_utc_expiration_and_query_uses_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Renewal persists UTC microseconds while leaving initial runtime facts unchanged."""
    monkeypatch.setattr(service_module, "_utc_now", lambda: NOW)
    runtime = FakeRuntime()
    application = create_app(
        runtime, allow_unauthenticated=True, database_path=tmp_path / "lifecycle.sqlite3", reaper_enabled=False,
    )
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as client:
            created = await client.post("/v1/sandboxes")
            assert created.status_code == 200
            sandbox_id = success_data(created)["sandbox_id"]
            renewed = await client.post(
                f"/v1/sandboxes/{sandbox_id}/renew", json={"ttl_seconds": 3600},
            )
            assert renewed.status_code == 200, renewed.text
            assert success_data(renewed) == {
                "sandbox_id": sandbox_id,
                "expires_at": "2026-10-19T09:00:00.123456Z",
            }
            queried = await client.get(f"/v1/sandboxes/{sandbox_id}")
        record = await application.state.lifecycle_store.get(sandbox_id)
        assert record.expires_at == NOW + timedelta(seconds=3600)
    assert success_data(queried)["expires_at"] == success_data(renewed)["expires_at"]
    assert success_data(queried)["status"] == runtime.result.status
    assert runtime.result.expires_at == NOW + timedelta(seconds=1800)


@pytest.mark.anyio
@pytest.mark.parametrize("ttl_seconds", [1, 86400])
async def test_renew_accepts_integer_boundaries_without_shortening(
    renewal_app, ttl_seconds: int,
) -> None:
    """Both integer boundaries are valid and a shorter target preserves the deadline."""
    application, client, runtime = renewal_app
    record = await seed_record(application, runtime, expires_at=NOW + timedelta(seconds=1800))

    response = await client.post(
        f"/v1/sandboxes/{SANDBOX_ID}/renew", json={"ttl_seconds": ttl_seconds},
    )

    expected = max(record.expires_at, NOW + timedelta(seconds=ttl_seconds))
    assert response.status_code == 200, response.text
    assert success_data(response) == {"sandbox_id": SANDBOX_ID, "expires_at": format_utc_datetime(expected)}
    assert (await application.state.lifecycle_store.get(SANDBOX_ID)).expires_at == expected
    assert runtime.result.expires_at == record.expires_at


@pytest.mark.anyio
async def test_renew_accepts_valid_body_at_4096_byte_limit(renewal_app) -> None:
    """Whitespace within the byte limit does not change a valid renewal request."""
    application, client, runtime = renewal_app
    await seed_record(application, runtime)
    body = b'{"ttl_seconds":1800}'.ljust(4096)

    response = await client.post(f"/v1/sandboxes/{SANDBOX_ID}/renew", content=body)

    assert response.status_code == 200
    assert success_data(response) == {
        "sandbox_id": SANDBOX_ID,
        "expires_at": format_utc_datetime(NOW + timedelta(seconds=1800)),
    }


@pytest.mark.anyio
async def test_renew_stops_reading_chunked_body_after_4096_bytes(
    renewal_app, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first excessive chunk terminates the read before renewal is attempted."""
    application, client, runtime = renewal_app

    async def forbidden(*args, **kwargs):
        """Assert the oversized request never reaches the Service."""
        pytest.fail("oversized renewal reached business logic")

    async def body():
        """Cross the limit with whitespace before the producer finishes sending."""
        yield b'{"ttl_seconds":1800}'.ljust(4096)
        yield b" "
        pytest.fail("renewal continued to read after exceeding the limit")

    monkeypatch.setattr(application.state.service, "renew", forbidden)
    request = client.build_request("POST", f"/v1/sandboxes/{SANDBOX_ID}/renew", content=body())
    assert "content-length" not in request.headers
    assert request.headers["transfer-encoding"] == "chunked"

    response = await client.send(request)

    assert response.status_code == 413
    assert_failure(response, "renewal request body must not exceed 4096 bytes")
    assert runtime.calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize("body", [
    b"", b"{", b"{\"ttl_seconds\":}", b"\xff", b"null", b"[]", b"1", b"true",
    b"\"ttl_seconds\"", b"{}", b'{"ttl_seconds":null}', b'{"ttl_seconds":true}',
    b'{"ttl_seconds":false}', b'{"ttl_seconds":0}', b'{"ttl_seconds":-1}',
    b'{"ttl_seconds":86401}', b'{"ttl_seconds":1.0}', b'{"ttl_seconds":1e3}',
    b'{"ttl_seconds":"1800"}', b'{"ttl_seconds":[]}', b'{"ttl_seconds":{}}',
    b'{"ttl_seconds":NaN}', b'{"ttl_seconds":Infinity}',
    b'{"ttl_seconds":1800,"unknown":true}', b'{"unknown":1800}',
    pytest.param(b"[" * 1500 + b"0" + b"]" * 1500, id="parser_depth_limit"),
    pytest.param(b'{"ttl_seconds":1800,"ttl_seconds":3600}', id="duplicate_ttl"),
    pytest.param(b'{"ttl_seconds":NaN,"ttl_seconds":1}', id="masked_nan"),
    pytest.param(b'{"ttl_seconds":Infinity,"ttl_seconds":1}', id="masked_infinity"),
    pytest.param(b'{"ttl_seconds":-Infinity,"ttl_seconds":1}', id="masked_negative_infinity"),
])
async def test_invalid_renewal_request_is_stable_400_without_business_calls(
    renewal_app, monkeypatch: pytest.MonkeyPatch, body: bytes,
) -> None:
    """Invalid JSON shapes and types fail before Service, SQLite or runtime access."""
    application, client, runtime = renewal_app

    async def forbidden(*args, **kwargs):
        """Fail if invalid request data reaches business or storage operations."""
        pytest.fail("invalid renewal request reached business logic")

    monkeypatch.setattr(application.state.service, "renew", forbidden, raising=False)
    monkeypatch.setattr(application.state.lifecycle_store, "get", forbidden)
    monkeypatch.setattr(application.state.lifecycle_store, "update_expiration", forbidden)
    response = await client.post(
        f"/v1/sandboxes/{SANDBOX_ID}/renew", content=body,
        headers={"content-type": "application/json"},
    )

    assert response.status_code == 400
    assert response.json() == INVALID_REQUEST
    assert runtime.calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize("sandbox_id", ["invalid", "sbx_" + "A" * 32, "sbx_1234"])
async def test_renew_invalid_id_returns_400_without_runtime_or_store(
    renewal_app, monkeypatch: pytest.MonkeyPatch, sandbox_id: str,
) -> None:
    """Malformed public identities return 400 before any backend I/O."""
    application, client, runtime = renewal_app

    async def forbidden(*args, **kwargs):
        """Reject metadata reads for a syntactically invalid sandbox identity."""
        pytest.fail("invalid sandbox ID reached storage")

    monkeypatch.setattr(application.state.lifecycle_store, "get", forbidden)
    response = await client.post(
        f"/v1/sandboxes/{sandbox_id}/renew", json={"ttl_seconds": 1800},
    )

    assert response.status_code == 400
    assert_failure(response, "invalid sandbox ID")
    assert runtime.calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["pending", "deleting", "expired", "exactly_due", "unmanaged"])
async def test_renew_rejects_unmanaged_inactive_and_expired_instances(
    renewal_app, state: str,
) -> None:
    """Only a managed active sandbox strictly before expiration may renew."""
    application, client, runtime = renewal_app
    if state == "unmanaged":
        record = None
        runtime.result = replace(runtime.result, sandbox_id=SANDBOX_ID)
    else:
        deadline = {
            "expired": NOW - timedelta(microseconds=1), "exactly_due": NOW,
        }.get(state, NOW + timedelta(seconds=1800))
        record = await seed_record(
            application, runtime,
            state=state if state in ("pending", "deleting") else "active",
            expires_at=deadline,
        )

    response = await client.post(
        f"/v1/sandboxes/{SANDBOX_ID}/renew", json={"ttl_seconds": 1800},
    )

    assert response.status_code == 409
    assert_failure(response, "sandbox renewal conflict", {"sandbox_id": SANDBOX_ID})
    assert await application.state.lifecycle_store.get(SANDBOX_ID) == record
    assert runtime.deletions == []


@pytest.mark.anyio
@pytest.mark.parametrize("field", ["sandbox_id", "transaction_id", "created_at"])
async def test_renew_identity_conflict_is_distinct_from_renewal_conflict(
    renewal_app, field: str,
) -> None:
    """Runtime identity mismatches use the existing metadata conflict response."""
    application, client, runtime = renewal_app
    record = await seed_record(application, runtime)
    values = {
        "sandbox_id": "sbx_" + "3" * 32,
        "transaction_id": "txn_" + "4" * 32,
        "created_at": record.created_at + timedelta(microseconds=1),
    }
    runtime.result = replace(runtime.result, **{field: values[field]})

    response = await client.post(
        f"/v1/sandboxes/{SANDBOX_ID}/renew", json={"ttl_seconds": 1800},
    )

    assert response.status_code == 409
    assert_failure(response, "sandbox lifecycle metadata conflict", {"sandbox_id": SANDBOX_ID})
    assert await application.state.lifecycle_store.get(SANDBOX_ID) == record


@pytest.mark.anyio
@pytest.mark.parametrize("failure,status,message", [
    (SandboxNotFound("secret Docker response"), 404, "sandbox not found"),
    (DockerUnavailable("secret Docker response"), 503, "Docker runtime is unavailable"),
    (SandboxQueryFailed("secret Docker response"), 500, "sandbox state could not be determined"),
])
async def test_renew_runtime_errors_use_existing_sanitized_contract(
    renewal_app, failure: Exception, status: int, message: str,
) -> None:
    """Read-only runtime verification preserves stable errors without leaking causes."""
    application, client, runtime = renewal_app
    record = await seed_record(application, runtime)
    runtime.error = failure

    response = await client.post(
        f"/v1/sandboxes/{SANDBOX_ID}/renew", json={"ttl_seconds": 1800},
    )

    assert response.status_code == status
    assert_failure(response, message)
    assert "secret" not in response.text
    assert await application.state.lifecycle_store.get(SANDBOX_ID) == record


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["get", "update_expiration"])
async def test_renew_storage_failures_are_sanitized_and_traceable(
    renewal_app, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    """Store failures during lookup or guarded update return a stable traced 503."""
    application, client, runtime = renewal_app
    record = await seed_record(application, runtime)
    store = application.state.lifecycle_store
    original_get = store.get

    async def unavailable(*args, **kwargs):
        """Attach private-looking causes that must not cross the HTTP boundary."""
        raise LifecycleStoreError() from RuntimeError("secret SQL /private/database.sqlite3")

    monkeypatch.setattr(store, operation, unavailable)
    response = await client.post(
        f"/v1/sandboxes/{SANDBOX_ID}/renew", json={"ttl_seconds": 1800},
    )

    assert response.status_code == 503
    assert_failure(response, "sandbox lifecycle metadata is unavailable", {"sandbox_id": SANDBOX_ID})
    assert "secret" not in response.text
    assert "/private" not in response.text
    assert await original_get(SANDBOX_ID) == record


@pytest.mark.anyio
async def test_renew_does_not_apply_readonly_query_timeout(
    renewal_app, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A renewal must bypass asyncio.wait_for because its write may commit late."""
    application, client, runtime = renewal_app
    await seed_record(application, runtime)

    def forbidden_timeout(*args, **kwargs):
        """Reject use of a read-only request deadline around a renewal write."""
        pytest.fail("renewal used asyncio.wait_for")

    monkeypatch.setattr(asyncio, "wait_for", forbidden_timeout)
    response = await client.post(
        f"/v1/sandboxes/{SANDBOX_ID}/renew", json={"ttl_seconds": 1800},
    )

    assert response.status_code == 200, response.text


@pytest.mark.anyio
async def test_renew_persists_after_app_restart_with_initial_runtime_times_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fresh application uses the saved renewal without changing runtime metadata."""
    monkeypatch.setattr(service_module, "_utc_now", lambda: NOW)
    path = tmp_path / "lifecycle.sqlite3"
    runtime = FakeRuntime()
    application = create_app(runtime, allow_unauthenticated=True, database_path=path, reaper_enabled=False)
    async with application.router.lifespan_context(application):
        record = await seed_record(application, runtime)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as client:
            renewed = await client.post(
                f"/v1/sandboxes/{SANDBOX_ID}/renew", json={"ttl_seconds": 3600},
            )
            assert renewed.status_code == 200

    restarted = create_app(runtime, allow_unauthenticated=True, store=SQLiteLifecycleStore(path), reaper_enabled=False)
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://test",
        ) as client:
            queried = await client.get(f"/v1/sandboxes/{SANDBOX_ID}")

    assert queried.status_code == 200
    assert success_data(queried)["expires_at"] == success_data(renewed)["expires_at"]
    assert runtime.result.expires_at == record.expires_at
