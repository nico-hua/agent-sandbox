"""Application assembly and startup coverage for persistent lifecycle metadata."""

import subprocess
import sqlite3
import sys
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest

import control_plane.app as app_module
import control_plane.core.service as service_module
from control_plane.app import create_app
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore
from test_app import FakeRuntime


class InitializationStore(SQLiteLifecycleStore):
    """Count explicit initialization calls and optionally fail before creating files."""

    def __init__(self, path: Path, *, fail: bool = False) -> None:
        """Record initialization observations without performing filesystem writes."""
        super().__init__(path)
        self.initializations = 0
        self.fail = fail

    async def initialize(self) -> None:
        """Initialize the actual temporary file or emit a stable storage error."""
        self.initializations += 1
        if self.fail:
            raise LifecycleStoreError()
        await super().initialize()


def test_default_database_path_is_relative_to_application_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Changing the working directory must not change the production database path."""
    monkeypatch.chdir(tmp_path)

    application = create_app(FakeRuntime())

    expected = Path(app_module.__file__).resolve().parent / ".local" / "lifecycle.sqlite3"
    assert app_module.DEFAULT_DATABASE_PATH == expected
    assert application.state.lifecycle_store._path == expected
    assert not (tmp_path / ".local").exists()


def test_database_path_injection_remains_lazy(tmp_path: Path) -> None:
    """Application construction must retain an injected path without opening it."""
    database_path = tmp_path / "not-created" / "metadata.sqlite3"

    application = create_app(FakeRuntime(), database_path=database_path)

    assert application.state.lifecycle_store._path == database_path
    assert not database_path.parent.exists()


def test_import_store_construction_and_app_assembly_have_no_database_or_docker_io() -> None:
    """A fresh process must assemble its application without connecting or creating files."""
    script = '''
import docker
import sqlite3
from pathlib import Path
from fastapi import FastAPI

def forbidden(*args, **kwargs):
    """Reject database, directory and Docker creation during assembly."""
    raise AssertionError("unexpected external side effect")

docker.from_env = forbidden
sqlite3.connect = forbidden
Path.mkdir = forbidden
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
store = SQLiteLifecycleStore("/tmp/lifecycle-assembly-must-remain-lazy.sqlite3")
from control_plane.app import app, create_app
assert isinstance(app, FastAPI)
assert isinstance(create_app(), FastAPI)
assert isinstance(create_app(store=store), FastAPI)
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True, text=True, timeout=10,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.anyio
async def test_lifespan_initializes_injected_store_once_and_preserves_dependency(
    tmp_path: Path,
) -> None:
    """Startup must explicitly initialize the same Store used by the shared Service."""
    path = tmp_path / "metadata" / "lifecycle.sqlite3"
    store = InitializationStore(path)
    runtime = FakeRuntime()
    application = create_app(runtime, store=store)
    assert store.initializations == 0
    assert not path.exists()

    async with application.router.lifespan_context(application):
        assert store.initializations == 1
        assert application.state.lifecycle_store is store
        assert application.state.runtime is runtime
        assert path.is_file()
        assert await store.get("missing") is None

    assert store.initializations == 1


@pytest.mark.anyio
async def test_lifespan_initializes_real_injected_database_path(tmp_path: Path) -> None:
    """The default Store implementation must initialize only when lifespan starts."""
    path = tmp_path / "metadata" / "lifecycle.sqlite3"
    application = create_app(FakeRuntime(), database_path=path)
    assert not path.exists()

    async with application.router.lifespan_context(application):
        assert path.is_file()
        assert await SQLiteLifecycleStore(path).get("missing") is None


@pytest.mark.anyio
async def test_lifespan_initialization_failure_aborts_startup_without_fallback(
    tmp_path: Path,
) -> None:
    """A persistence failure must escape startup and retain the configured Store."""
    path = tmp_path / "not-created" / "lifecycle.sqlite3"
    store = InitializationStore(path, fail=True)
    runtime = FakeRuntime()
    application = create_app(runtime, store=store)

    with pytest.raises(LifecycleStoreError):
        async with application.router.lifespan_context(application):
            pytest.fail("startup must fail before serving requests")

    assert store.initializations == 1
    assert application.state.lifecycle_store is store
    assert runtime.calls == 0
    assert not path.parent.exists()


@pytest.mark.anyio
async def test_api_uses_updated_database_expiration_after_application_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Fresh app and Store instances merge current SQLite expiration with runtime facts."""
    now = datetime(2026, 10, 19, 8, 0, 0, 123456, tzinfo=UTC)
    monkeypatch.setattr(service_module, "_utc_now", lambda: now)
    database_path = tmp_path / "lifecycle.sqlite3"
    runtime = FakeRuntime()
    application = create_app(runtime, database_path=database_path)
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as client:
            response = await client.post("/v1/sandboxes")
            assert response.status_code == 201, response.text
            created = response.json()
        record = await application.state.lifecycle_store.get(created["sandbox_id"])
        assert record is not None
        assert record.created_at == now
        assert record.expires_at == now + timedelta(seconds=1800)
        assert record.management_state == "active" and record.version == 1
        assert "management_state" not in created

    expires_at = now + timedelta(hours=2, microseconds=1)
    store = SQLiteLifecycleStore(database_path)
    updated = await store.update_expiration(
        record.sandbox_id, record.transaction_id,
        expected_version=record.version, expires_at=expires_at,
    )
    assert updated.version == 2
    restarted = create_app(runtime, store=SQLiteLifecycleStore(database_path))
    async with restarted.router.lifespan_context(restarted):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://test",
        ) as client:
            response = await client.get(f"/v1/sandboxes/{record.sandbox_id}")

    assert response.status_code == 200, response.text
    assert response.json()["created_at"] == created["created_at"]
    assert response.json()["expires_at"] == "2026-10-19T10:00:00.123457Z"
    assert "transaction_id" not in response.json()
    assert "management_state" not in response.json()
    assert runtime.result.status == response.json()["status"]
    assert runtime.result.expires_at == record.expires_at


@pytest.mark.anyio
@pytest.mark.parametrize("method", ["POST", "GET", "DELETE"])
async def test_api_maps_unavailable_metadata_without_database_details(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, method: str,
) -> None:
    """Corrupt persisted metadata must produce stable traced errors on business routes."""
    sandbox_id = "sbx_" + "1" * 32
    monkeypatch.setattr(service_module, "_new_sandbox_id", lambda: sandbox_id)
    path = tmp_path / "secret-lifecycle.sqlite3"
    runtime = FakeRuntime()
    application = create_app(runtime, database_path=path)
    async with application.router.lifespan_context(application):
        path.write_bytes(b"corrupt database containing secret SQL and /private/path")
        url = "/v1/sandboxes" if method == "POST" else f"/v1/sandboxes/{sandbox_id}"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as client:
            response = await client.request(method, url)

    assert response.status_code == 503
    assert response.json() == {"error": {
        "code": "sandbox_metadata_unavailable",
        "message": "sandbox lifecycle metadata is unavailable",
        "sandbox_id": sandbox_id,
    }}
    assert "secret" not in response.text
    assert "SQL" not in response.text
    assert str(path) not in response.text
    assert runtime.calls == (1 if method == "GET" else 0)


@pytest.mark.anyio
async def test_api_rejects_duplicate_metadata_before_runtime_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second creation of an occupied ID must preserve the original durable record."""
    sandbox_id = "sbx_" + "1" * 32
    monkeypatch.setattr(service_module, "_new_sandbox_id", lambda: sandbox_id)
    runtime = FakeRuntime()
    application = create_app(runtime, database_path=tmp_path / "lifecycle.sqlite3")
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as client:
            first = await client.post("/v1/sandboxes")
            original = await application.state.lifecycle_store.get(sandbox_id)
            second = await client.post("/v1/sandboxes")
        assert await application.state.lifecycle_store.get(sandbox_id) == original

    assert first.status_code == 201
    assert second.status_code == 409
    assert second.json() == {"error": {
        "code": "sandbox_metadata_conflict",
        "message": "sandbox lifecycle metadata conflict",
        "sandbox_id": sandbox_id,
    }}
    assert runtime.calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("field", ["transaction_id", "created_at"])
async def test_api_rejects_runtime_identity_mismatch_without_overwriting_metadata(
    tmp_path: Path, field: str,
) -> None:
    """A query must not merge expiration from a different transaction or creation time."""
    runtime = FakeRuntime()
    application = create_app(runtime, database_path=tmp_path / "lifecycle.sqlite3")
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as client:
            created = await client.post("/v1/sandboxes")
            assert created.status_code == 201
            sandbox_id = created.json()["sandbox_id"]
            original = await application.state.lifecycle_store.get(sandbox_id)
            value = "txn_" + "2" * 32 if field == "transaction_id" else original.created_at + timedelta(microseconds=1)
            runtime.result = replace(runtime.result, **{field: value})
            response = await client.get(f"/v1/sandboxes/{sandbox_id}")
        assert await application.state.lifecycle_store.get(sandbox_id) == original

    assert response.status_code == 409
    assert response.json() == {"error": {
        "code": "sandbox_metadata_conflict",
        "message": "sandbox lifecycle metadata conflict",
        "sandbox_id": sandbox_id,
    }}


@pytest.mark.anyio
async def test_api_keeps_retry_record_when_metadata_cleanup_is_locked(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime deletion followed by locked SQL cleanup must expose a traceable retry."""
    path = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(path, lock_timeout=0)
    runtime = FakeRuntime()
    application = create_app(runtime, store=store)
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as client:
            created = await client.post("/v1/sandboxes")
            assert created.status_code == 201
            sandbox_id = created.json()["sandbox_id"]
            original = await store.get(sandbox_id)
            with closing(sqlite3.connect(path)) as locking_connection:
                original_delete = runtime.delete
                lock_after_delete = True

                async def delete_then_lock(
                    instance_id: str, *, delete_workspace: bool = False,
                    expected_transaction_id: str | None = None,
                ) -> None:
                    """Lock SQLite only after the runtime completes its first deletion."""
                    nonlocal lock_after_delete
                    await original_delete(
                        instance_id, delete_workspace=delete_workspace,
                        expected_transaction_id=expected_transaction_id,
                    )
                    if lock_after_delete:
                        locking_connection.execute("BEGIN IMMEDIATE")
                        lock_after_delete = False

                monkeypatch.setattr(runtime, "delete", delete_then_lock)
                try:
                    failed = await client.delete(f"/v1/sandboxes/{sandbox_id}")
                    retained = await store.get(sandbox_id)
                    assert retained.management_state == "deleting"
                    assert retained.version == original.version + 1
                finally:
                    locking_connection.rollback()
            retried = await client.delete(f"/v1/sandboxes/{sandbox_id}")
        assert await store.get(sandbox_id) is None

    assert failed.status_code == 500
    assert failed.json() == {"error": {
        "code": "sandbox_metadata_cleanup_failed",
        "message": "sandbox lifecycle metadata cleanup failed",
        "sandbox_id": sandbox_id,
    }}
    assert str(path) not in failed.text
    assert retried.status_code == 204
    assert runtime.deletions == [(sandbox_id, False), (sandbox_id, False)]
    assert runtime.deletion_transactions == [original.transaction_id] * 2
