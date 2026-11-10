"""Double-opt-in Service creation transactions with real SQLite and eight owned Kubernetes resources."""

import asyncio
import re
import secrets
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.errors import SandboxNotFound
from control_plane.core.tasks import settle_task
from control_plane.runtime.kubernetes.local_entry_manager import KubernetesLocalEntryManager
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_endpoint_integration import _delivered_endpoint
from test_kubernetes_local_entry_integration import _assert_port_closed, _protocol
from test_kubernetes_proxy_integration import _PROXY_IMAGE, _RedactedText
from test_kubernetes_proxy_lifecycle_integration import owned_proxy_lifecycle, pytestmark


class _CommittedActivationStore(SQLiteLifecycleStore):
    """Delay activation's return after the actual SQLite transaction has committed."""

    def __init__(self, path):
        """Keep real storage and expose only event-loop gates around its committed result."""
        super().__init__(path)
        self.activation_entered = asyncio.Event()
        self.activation_release = asyncio.Event()

    async def activate(self, sandbox_id, transaction_id, *, expected_version):
        """Commit normally, then retain the returned version until cancellation has been exercised."""
        active = await super().activate(
            sandbox_id, transaction_id, expected_version=expected_version,
        )
        self.activation_entered.set()
        await self.activation_release.wait()
        return active


def _observe_creation(chain, backend, store, monkeypatch):
    """Observe Service's own specification and durable prewrite before delegating unchanged to Runtime."""
    original = backend.create
    fixture_spec = chain["spec"]
    observed = []

    async def create(spec):
        """Require real pending metadata and its original credential before any managed SDK POST."""
        observed.append(spec)
        chain["spec"] = spec
        assert not chain["managed"].entries
        assert spec.sandbox_id != fixture_spec.sandbox_id
        assert spec.transaction_id != fixture_spec.transaction_id
        assert re.fullmatch(r"sbx_[0-9a-f]{32}", spec.sandbox_id)
        assert re.fullmatch(r"txn_[0-9a-f]{32}", spec.transaction_id)
        record = await store.get(spec.sandbox_id)
        assert record is not None
        assert (record.transaction_id, record.created_at, record.expires_at) == (
            spec.transaction_id, spec.created_at, spec.expires_at,
        )
        assert record.management_state == "pending" and record.version == 0
        credential = await store.get_credential(spec.sandbox_id, spec.transaction_id)
        assert credential is not None
        same_token = credential.access_token == spec.access_token
        assert same_token, "pending creation did not persist the Service-generated credential"
        assert (await store.list_sandboxes()).managed_count == 1
        return await original(spec)

    monkeypatch.setattr(backend, "create", create)
    return observed


def _application(backend, manager, store):
    """Assemble normal Service factories and an authenticated lifespan without preselected sandbox IDs."""
    admin = _RedactedText(secrets.token_urlsafe(32))
    app = create_app(
        runtime=backend, store=store, api_key=str(admin), reaper_enabled=False,
        pending_recovery_enabled=False, sandbox_query_timeout=10,
        kubernetes_entry_manager=manager,
    )
    return app, admin


async def _ready(service, sandbox_id):
    """Wait for Service's identity-checked runtime query without implicitly opening a local entry."""
    deadline = time.monotonic() + 45
    while True:
        observed = await service.get(sandbox_id)
        if observed.status == "ready":
            return observed
        assert observed.status in {"starting", "running"}
        assert time.monotonic() < deadline, "Service-created proxy chain did not become ready"
        await asyncio.sleep(0.1)


def _metadata_counts(path):
    """Count actual temporary database rows without exposing any credential values."""
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as connection:
        return (
            connection.execute("SELECT COUNT(*) FROM lifecycle_records").fetchone()[0],
            connection.execute("SELECT COUNT(*) FROM endpoint_credentials").fetchone()[0],
        )


async def _assert_removed(chain, service, store, spec):
    """Independently require resource absence, credential cascade and fully released admission capacity."""
    await asyncio.to_thread(chain["managed"].assert_absent)
    assert await store.get(spec.sandbox_id) is None
    assert await store.get_credential(spec.sandbox_id, spec.transaction_id) is None
    assert await asyncio.to_thread(_metadata_counts, store.database_path) == (0, 0)
    assert (await store.list_sandboxes()).managed_count == 0
    assert service._creates_in_progress == 0
    with pytest.raises(SandboxNotFound):
        await service.get(spec.sandbox_id)


async def _finish_tasks(tasks, manager):
    """Settle all test-owned tasks before closing forwarding sessions and yielding to UID-guarded cleanup."""
    try:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*(settle_task(task) for task in tasks), return_exceptions=True)
    finally:
        await manager.close_all()


@asynccontextmanager
async def _running_application(app, tasks, manager, *, release=None):
    """Settle Service work before lifespan releases its SQLite lock, including any failed assertion."""
    try:
        async with app.router.lifespan_context(app):
            try:
                yield
            finally:
                if release is not None:
                    release.set()
                await _finish_tasks(tasks, manager)
    finally:
        await manager.close_all()


@pytest.mark.parametrize("delete_workspace", [False, True], ids=["default-delete", "delete-emptydir"])
def test_service_create_endpoint_and_delete_owns_complete_transaction(
    owned_proxy_lifecycle, tmp_path, monkeypatch, delete_workspace,
):
    """Service creation must prewrite, activate, deliver its credential and delete the complete owned chain."""
    chain = owned_proxy_lifecycle

    async def verify():
        """Keep SQLite ownership, actual Service creation and the local process on one event loop."""
        backend = chain["make_runtime"](proxy_image=_PROXY_IMAGE)
        store = SQLiteLifecycleStore(tmp_path / "service-create.sqlite3")
        manager = KubernetesLocalEntryManager(backend)
        observed = _observe_creation(chain, backend, store, monkeypatch)
        app, admin = _application(backend, manager, store)
        tasks = []
        async with _running_application(app, tasks, manager):
            service = app.state.service
            assert manager.session_count == 0
            before = datetime.now(UTC)
            creation = asyncio.create_task(service.create())
            tasks.append(creation)
            result = await creation
            after = datetime.now(UTC)
            assert len(observed) == 1
            spec = observed[0]
            assert result.sandbox_id == spec.sandbox_id and result.status == "started"
            assert before <= result.created_at <= after
            assert result.expires_at - result.created_at == timedelta(seconds=1800)
            assert (result.created_at, result.expires_at) == (spec.created_at, spec.expires_at)
            chain["managed"].assert_complete()
            active = await store.get(spec.sandbox_id)
            assert active.management_state == "active" and active.version == 1
            assert (await store.list_sandboxes()).managed_count == 1
            assert await asyncio.to_thread(_metadata_counts, store.database_path) == (1, 1)
            assert service._creates_in_progress == 0
            ready = await _ready(service, spec.sandbox_id)
            assert ready.transaction_id == spec.transaction_id
            assert manager.session_count == 0
            connection = await manager.open(spec.sandbox_id)
            assert manager.session_count == 1
            assert connection.target.transaction_id == spec.transaction_id
            assert connection.target.proxy_pod_uid == chain["managed"].role("proxy")["uid"]
            assert connection.target.proxy_service_uid == chain["managed"].role("proxy-service")["uid"]
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://isolated-control", trust_env=False,
            ) as control:
                entry, delivered = await _delivered_endpoint(
                    control, f"/v1/sandboxes/{spec.sandbox_id}/endpoint", admin,
                    _RedactedText(spec.access_token),
                )
                assert entry.url == connection.url
                await _protocol(entry, delivered)
            if delete_workspace:
                deletion = asyncio.create_task(service.delete(spec.sandbox_id, delete_workspace=True))
            else:
                deletion = asyncio.create_task(service.delete(spec.sandbox_id))
            tasks.append(deletion)
            await deletion
            assert manager.session_count == 0
            await _assert_port_closed(entry.url)
            await _assert_removed(chain, service, store, spec)
        assert manager.is_closed

    asyncio.run(verify())


def test_service_cancel_after_activation_commit_compensates_all_resources(
    owned_proxy_lifecycle, tmp_path, monkeypatch,
):
    """Repeated cancellation after real activation commit must compensate resources, metadata and credential."""
    chain = owned_proxy_lifecycle

    async def verify():
        """Hold the real committed result while cancelling its Service waiter, then await full compensation."""
        backend = chain["make_runtime"](proxy_image=_PROXY_IMAGE)
        store = _CommittedActivationStore(tmp_path / "cancelled-create.sqlite3")
        manager = KubernetesLocalEntryManager(backend)
        observed = _observe_creation(chain, backend, store, monkeypatch)
        app, _ = _application(backend, manager, store)
        tasks = []
        async with _running_application(app, tasks, manager, release=store.activation_release):
            service = app.state.service
            creation = asyncio.create_task(service.create())
            entered = asyncio.create_task(store.activation_entered.wait())
            tasks.extend((creation, entered))
            done, _ = await asyncio.wait({creation, entered}, return_when=asyncio.FIRST_COMPLETED)
            if creation in done:
                await creation
                pytest.fail("Service creation returned before the committed activation gate was released")
            assert entered in done and len(observed) == 1
            spec = observed[0]
            chain["managed"].assert_complete()
            active = await store.get(spec.sandbox_id)
            assert active.management_state == "active" and active.version == 1
            assert await asyncio.to_thread(_metadata_counts, store.database_path) == (1, 1)
            assert service._creates_in_progress == 1
            assert manager.session_count == 0
            creation.cancel()
            await asyncio.sleep(0)
            creation.cancel()
            await asyncio.sleep(0)
            assert not creation.done(), "cancelled creation returned before settling the committed activation"
            assert (await store.get(spec.sandbox_id)).management_state == "active"
            store.activation_release.set()
            with pytest.raises(asyncio.CancelledError):
                await creation
            await _assert_removed(chain, service, store, spec)
            assert manager.session_count == 0
        assert manager.is_closed

    asyncio.run(verify())
