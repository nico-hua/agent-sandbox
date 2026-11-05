"""Read-only managed pagination, SQLite snapshot and authenticated HTTP contracts."""

import asyncio
import base64
import json
import sqlite3
import threading
from contextlib import closing, contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.models import SandboxLifecycleRecord
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore, LifecycleStoreError
from lifecycle_fakes import FakeLifecycleStore
from test_app import FakeRuntime

NOW = datetime(2026, 11, 5, 1, 2, 3, 123456, tzinfo=UTC)
KEY = "list-test-admin"


def item(index, *, created_at=NOW):
    """Build independent pending identities with exact microseconds and no credentials."""
    return SandboxLifecycleRecord(f"sbx_{index:032x}", f"txn_{index:032x}", created_at, created_at + timedelta(seconds=1))


def cursor_for(position):
    """Encode arbitrary cursor payloads without importing production validation."""
    return "v1." + base64.urlsafe_b64encode(json.dumps(position, separators=(",", ":")).encode()).decode().rstrip("=")


async def seeded(tmp_path, count=0, cls=SQLiteLifecycleStore):
    """Initialize a new temporary database and seed valid original lifecycle records."""
    store = cls(tmp_path / "list.sqlite3")
    await store.initialize()
    for index in reversed(range(count)):
        await store.create(item(index), max_instances=1024)
    return store


@pytest.mark.anyio
async def test_store_pagination_is_stable_precise_and_continues_after_deleted_cursor(tmp_path):
    """Tied creation times use IDs; deleted anchors and microsecond boundaries remain valid."""
    store = await seeded(tmp_path, 4)
    await store.create(item(9, created_at=NOW + timedelta(microseconds=1)))
    await store.activate(item(1).sandbox_id, item(1).transaction_id, expected_version=0)
    await store.mark_deleting(item(2).sandbox_id, item(2).transaction_id, expected_version=0)
    page = await store.list_sandboxes(limit=2)
    assert [row.sandbox_id for row in page.items] == [item(0).sandbox_id, item(1).sandbox_id]
    assert [row.management_state for row in page.items] == ["pending", "active"]
    assert page.managed_count == 5 and page.next_cursor is not None
    await store.delete(item(1).sandbox_id, item(1).transaction_id, expected_version=1)
    next_page = await store.list_sandboxes(limit=2, cursor=page.next_cursor)
    assert [row.sandbox_id for row in next_page.items] == [item(2).sandbox_id, item(3).sandbox_id]
    assert next_page.items[0].management_state == "deleting" and next_page.managed_count == 4
    last = await store.list_sandboxes(limit=2, cursor=next_page.next_cursor)
    assert last.items[0].created_at == NOW + timedelta(microseconds=1)
    assert len(last.items) == 1 and last.next_cursor is None


@pytest.mark.anyio
@pytest.mark.parametrize("limit", [1, 20, 100])
async def test_store_static_pages_have_no_duplicates_or_omissions(tmp_path, limit):
    """SQL-bounded pages enumerate every state and expired row exactly once."""
    store = await seeded(tmp_path, 103)
    seen, cursor = [], None
    while True:
        page = await store.list_sandboxes(limit=limit, cursor=cursor)
        assert len(page.items) <= limit and page.managed_count == 103
        seen.extend(row.sandbox_id for row in page.items)
        cursor = page.next_cursor
        if cursor is None:
            break
    assert seen == [item(index).sandbox_id for index in range(103)]


@pytest.mark.anyio
async def test_default_empty_page_and_service_capacity_without_runtime(tmp_path):
    """Listing needs only storage, returns current configuration and never checks Docker."""
    store = await seeded(tmp_path)
    runtime = Mock()
    service = SandboxService(runtime, store, max_instances=1)
    empty = await service.list_sandboxes()
    assert empty.items == () and empty.next_cursor is None and empty.managed_count == 0 and empty.max_instances == 1
    for index in range(21):
        await store.create(item(index), max_instances=1024)
    before = [await store.get(item(index).sandbox_id) for index in range(21)]
    result = await service.list_sandboxes()
    assert len(result.items) == 20 and result.managed_count == 21 and result.max_instances == 1
    assert [await store.get(row.sandbox_id) for row in before] == before
    assert runtime.mock_calls == []


class ObservedStore(SQLiteLifecycleStore):
    """Capture SQL and optionally coordinate a concurrent writer after snapshot counting."""

    def __init__(self, path):
        """Keep thread events and query observations isolated to this temporary store."""
        super().__init__(path)
        self.queries = []
        self.pause = False
        self.counted = threading.Event()
        self.written = threading.Event()

    @contextmanager
    def _connection(self, **kwargs):
        """Install a trace on each real independent SQLite connection."""
        with super()._connection(**kwargs) as connection:
            connection.set_trace_callback(self._trace)
            yield connection

    def _trace(self, sql):
        """Hold page selection while another connection commits into a WAL database."""
        self.queries.append(sql)
        if self.pause and sql.startswith("SELECT sandbox_id, management_state"):
            self.counted.set()
            assert self.written.wait(5)


@pytest.mark.anyio
async def test_count_and_page_share_real_read_snapshot_and_do_not_read_credentials(tmp_path):
    """A concurrent WAL commit cannot mix old counts with newly inserted page entries."""
    store = await seeded(tmp_path, 1, ObservedStore)
    with closing(sqlite3.connect(store.database_path)) as connection:
        connection.execute("PRAGMA journal_mode=WAL")
    store.queries.clear()
    store.pause = True
    task = asyncio.create_task(store.list_sandboxes(limit=100))
    try:
        assert await asyncio.to_thread(store.counted.wait, 5)
        await SQLiteLifecycleStore(store.database_path).create(item(1))
    finally:
        store.written.set()
    page = await task
    assert page.managed_count == 1 and len(page.items) == 1
    selects = [sql for sql in store.queries if sql.startswith("SELECT") and "FROM lifecycle_records" in sql]
    assert len(selects) == 2 and any("LIMIT 101" in sql for sql in selects)
    assert all("transaction_id" not in sql and "access_token" not in sql for sql in selects)
    assert not any("FROM endpoint_credentials" in sql for sql in store.queries)
    assert "BEGIN" in store.queries and "COMMIT" in store.queries


BAD_CURSORS = ["", "x" * 257, "v2.abc", "v1.@@", "v1.e30", "v1.W10", "v1.====",
               cursor_for([True, item(0).sandbox_id]), cursor_for([1.0, item(0).sandbox_id]),
               cursor_for([10**100, item(0).sandbox_id]), cursor_for([0, "../bad"]),
               cursor_for([0, item(0).sandbox_id, "extra"]), cursor_for([-62135596800000001, item(0).sandbox_id])]


@pytest.mark.anyio
@pytest.mark.parametrize("cursor", BAD_CURSORS)
async def test_invalid_cursor_rejected_before_store_or_runtime(cursor):
    """Only a bounded canonical versioned paging position can reach persistence."""
    store, runtime = Mock(), Mock()
    with pytest.raises(ValueError):
        await SandboxService(runtime, store).list_sandboxes(cursor=cursor)
    assert not store.mock_calls and not runtime.mock_calls


@pytest.mark.anyio
@pytest.mark.parametrize("limit", [0, -1, 101, True, 1.0, "20", None])
async def test_direct_service_and_store_reject_invalid_limit_without_io(tmp_path, limit):
    """Non-HTTP callers cannot bypass strict pagination bounds."""
    store = SQLiteLifecycleStore(tmp_path / "absent.sqlite3")
    with pytest.raises(ValueError):
        await store.list_sandboxes(limit=limit)
    with pytest.raises(ValueError):
        await SandboxService(Mock(), store).list_sandboxes(limit=limit)
    assert not store.database_path.exists()


@pytest.mark.anyio
@pytest.mark.parametrize("query", ["limit=", "limit=0", "limit=-1", "limit=101", "limit=true", "limit=1.0", "limit=+1", "limit=1&limit=2", "cursor=a&cursor=b", "other=x", "cursor="])
async def test_api_strict_query_and_authentication_priority(query):
    """Authentication precedes even invalid query parsing, which never reaches Store."""
    store, runtime = Mock(), FakeRuntime()
    app = create_app(runtime, store=store, reaper_enabled=False, api_key=KEY)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
        for headers, expected in [({}, 401), ({"Authorization": "Bearer wrong"}, 401), ({"Authorization": "Bearer " + KEY}, 400)]:
            result = await client.get("/v1/sandboxes?" + query, headers=headers)
            assert result.status_code == expected
            assert result.json()["error"]["code"] == ("unauthorized" if expected == 401 else "invalid_request")
    assert not store.mock_calls and runtime.calls == 0


@pytest.mark.anyio
async def test_api_public_summary_and_no_store_cache(tmp_path):
    """Authenticated listings expose exactly the public fields, independent of runtime health."""
    store = await seeded(tmp_path, 2)
    runtime = FakeRuntime(error=RuntimeError("DOCKER_SECRET"))
    app = create_app(runtime, store=store, reaper_enabled=False, api_key=KEY, max_instances=1)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control", headers={"Authorization": "Bearer " + KEY}) as client:
        response = await client.get("/v1/sandboxes?limit=1")
        assert response.status_code == 200 and response.headers["Cache-Control"] == "no-store"
        data = response.json()
        assert data["managed_count"] == 2 and data["max_instances"] == 1
        assert set(data["items"][0]) == {"sandbox_id", "management_state", "created_at", "expires_at", "workspace_id"}
        assert data["items"][0] == {"sandbox_id": item(0).sandbox_id, "management_state": "pending", "created_at": "2026-11-05T01:02:03.123456Z", "expires_at": "2026-11-05T01:02:04.123456Z", "workspace_id": None}
        assert (await client.get("/v1/sandboxes", params={"cursor": data["next_cursor"]})).json()["next_cursor"] is None
    assert runtime.calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [LifecycleStoreError(), RuntimeError("SECRET_SQL_PATH"), "timeout"])
async def test_api_storage_failures_and_timeout_are_fixed_not_empty_success(failure):
    """Backend errors and deadline exhaustion produce sanitized non-success responses."""
    store = FakeLifecycleStore()

    async def fail(**kwargs):
        """Deterministically fail or wait for the API deadline without a real database."""
        if failure == "timeout":
            await asyncio.Event().wait()
        raise failure

    store.list_sandboxes = fail
    app = create_app(FakeRuntime(), store=store, reaper_enabled=False, api_key=KEY, sandbox_query_timeout=0.01)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control", headers={"Authorization": "Bearer " + KEY}) as client:
        response = await client.get("/v1/sandboxes")
    assert response.status_code == (504 if failure == "timeout" else 503)
    assert response.json() == {"error": {"code": "sandbox_list_timeout" if failure == "timeout" else "sandbox_list_unavailable", "message": "sandbox list query timed out" if failure == "timeout" else "sandbox list is unavailable"}}
    assert response.headers["Cache-Control"] == "no-store"


@pytest.mark.anyio
async def test_explicit_workspace_summary_and_default_api_page(tmp_path):
    """List bindings through public IDs without reading workspace secrets or versions."""
    from test_workspace_store import workspace, binding, lifecycle
    store = await seeded(tmp_path, 21)
    work = workspace()
    await store.create_workspace(work)
    await store.confirm_workspace(work.workspace_id, work.creation_token, expected_version=0)
    bound = lifecycle()
    await store.create_with_workspace(bound, binding(bound), expected_workspace_version=1, max_instances=1024)
    before = await store.get_workspace(work.workspace_id)
    app = create_app(FakeRuntime(), store=store, reaper_enabled=False, api_key=KEY)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control", headers={"Authorization": "Bearer " + KEY}) as client:
        page = (await client.get("/v1/sandboxes")).json()
    assert len(page["items"]) == 20 and page["managed_count"] == 22
    assert page["items"][0]["workspace_id"] == work.workspace_id
    assert await store.get_workspace(work.workspace_id) == before
    assert work.creation_token not in json.dumps(page) and work.volume_name not in json.dumps(page)


@pytest.mark.anyio
@pytest.mark.parametrize("created", [datetime.min.replace(tzinfo=UTC), datetime.max.replace(tzinfo=UTC)])
async def test_cursor_preserves_full_datetime_range_without_floats(tmp_path, created):
    """Even pre-epoch and maximum datetime positions retain every integer microsecond."""
    store = await seeded(tmp_path)
    for index in range(2):
        row = SandboxLifecycleRecord(item(index).sandbox_id, item(index).transaction_id, created, created)
        await store.create(row)
    first = await store.list_sandboxes(limit=1)
    second = await store.list_sandboxes(limit=1, cursor=first.next_cursor)
    assert second.items[0].sandbox_id == item(1).sandbox_id
    assert second.items[0].created_at == created and second.next_cursor is None


@pytest.mark.anyio
@pytest.mark.parametrize("condition", ["missing", "corrupt"])
async def test_real_storage_errors_do_not_initialize_or_return_empty(tmp_path, condition):
    """Missing and corrupt files stay unchanged while listing reports a domain failure."""
    from control_plane.core.errors import SandboxListUnavailable
    path = tmp_path / "unavailable.sqlite3"
    if condition == "corrupt":
        path.write_bytes(b"not a sqlite database")
    store = SQLiteLifecycleStore(path)
    with pytest.raises(SandboxListUnavailable):
        await SandboxService(Mock(), store).list_sandboxes()
    assert (path.read_bytes() == b"not a sqlite database") if condition == "corrupt" else not path.exists()
