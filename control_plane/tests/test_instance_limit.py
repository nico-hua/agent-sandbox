"""Persistent instance admission across transactions, lifecycle cleanup and cancellation."""

import asyncio
import sqlite3
import threading
from contextlib import closing
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core import errors
from control_plane.core.models import SandboxAccessCredential, SandboxCreateOptions, SandboxLifecycleRecord, WorkspaceBinding
from control_plane.core.service import SandboxService
from control_plane.storage import lifecycle_store as storage
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_workspace_reuse import WorkspaceRuntime, GatedWorkspaceStore
from test_service_lifecycle import ControlledRuntime, FaultStore
from test_sandbox_service import FIXED_SANDBOX_ID, FIXED_TRANSACTION_ID

NOW = datetime(2026, 11, 5, tzinfo=UTC)


def record(index=0):
    """Construct an independent original pending identity with deterministic UTC metadata."""
    return SandboxLifecycleRecord(f"sbx_{index:032x}", f"txn_{index:032x}", NOW, NOW + timedelta(hours=1))


def credential(item):
    """Build a synthetic secret bound only to the supplied test transaction."""
    return SandboxAccessCredential(item.sandbox_id, item.transaction_id, "x" * 43)


def snapshot(path):
    """Read all admission-related rows without exposing secret values in diagnostics."""
    with closing(sqlite3.connect(path)) as connection:
        return tuple(tuple(connection.execute(f"SELECT * FROM {table} ORDER BY 1")) for table in (
            "lifecycle_records", "endpoint_credentials", "workspaces", "workspace_bindings",
        ))


async def initialized(tmp_path, cls=SQLiteLifecycleStore):
    """Initialize only a newly owned temporary file database."""
    store = cls(tmp_path / "instances.sqlite3")
    await store.initialize()
    return store


@pytest.mark.parametrize("value", [True, False, 0, -1, 1025, 1.0, "10", None, 10**100])
@pytest.mark.parametrize("factory", ["app", "service"])
def test_invalid_configuration_fails_without_storage_or_runtime(tmp_path, value, factory):
    """Malformed trusted limits cannot silently disable persistent admission."""
    store, runtime = SQLiteLifecycleStore(tmp_path / "absent.sqlite3"), WorkspaceRuntime()
    with pytest.raises(ValueError, match="max_instances"):
        if factory == "app":
            create_app(runtime, store=store, max_instances=value, allow_unauthenticated=True)
        else:
            SandboxService(runtime, store, max_instances=value)
    assert not store.database_path.exists() and not runtime.specs


@pytest.mark.anyio
@pytest.mark.parametrize("limit", [None, 1, 1024])
async def test_configured_limit_is_enforced_through_application(tmp_path, limit):
    """The default admits ten; both allowed boundaries reach the Store and reject extras."""
    store = await initialized(tmp_path)
    maximum = 10 if limit is None else limit
    for index in range(maximum - 1):
        await store.create(record(index), max_instances=1024)
    runtime = WorkspaceRuntime()
    app = create_app(runtime, store=store, reaper_enabled=False, api_key="test-admin", **({} if limit is None else {"max_instances": limit}))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control", headers={"Authorization": "Bearer test-admin"}) as client:
        assert (await client.post("/v1/sandboxes")).status_code == 201
        before = snapshot(store.database_path)
        denied = await client.post("/v1/sandboxes")
        assert denied.status_code == 429
        assert denied.json() == {"error": {"code": "sandbox_instance_limit_exceeded", "message": "sandbox instance limit exceeded"}}
        assert "retry-after" not in denied.headers
        unchanged = snapshot(store.database_path) == before
        assert unchanged and len(runtime.specs) == 1
        assert (await client.post("/v1/sandboxes", json={"max_instances": 1024})).status_code == 400


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["pending", "active", "deleting"])
@pytest.mark.parametrize("expired", [False, True])
async def test_all_rows_including_legacy_and_due_records_hold_capacity(tmp_path, state, expired):
    """No management state, missing token or elapsed TTL implicitly frees a live record."""
    store = await initialized(tmp_path)
    old = record()
    if expired:
        old = replace(old, expires_at=NOW)
    await store.create(old)
    if state == "active":
        await store.activate(old.sandbox_id, old.transaction_id, expected_version=0)
    elif state == "deleting":
        await store.mark_deleting(old.sandbox_id, old.transaction_id, expected_version=0)
    runtime = WorkspaceRuntime()
    with pytest.raises(errors.SandboxInstanceLimitExceeded):
        await SandboxService(runtime, store, clock=lambda: NOW, max_instances=1).create()
    assert runtime.specs == []
    assert await store.get_credential(old.sandbox_id, old.transaction_id) is None


@pytest.mark.anyio
@pytest.mark.parametrize("bound", [False, True])
async def test_last_slot_is_atomic_for_independent_connections(tmp_path, bound):
    """Concurrent ordinary and optional workspace inserts never both acquire the final slot."""
    store = await initialized(tmp_path)
    runtime = WorkspaceRuntime()
    service = SandboxService(runtime, store)
    workspace = await service.create_workspace()
    ws = await store.get_workspace(workspace.workspace_id)
    binding = WorkspaceBinding(ws.workspace_id, ws.creation_token, ws.volume_name, ws.image_id, ws.version + 1, record(2).sandbox_id, record(2).transaction_id)
    first, second = record(1), replace(record(2), workspace_id=workspace.workspace_id if bound else None)
    start = threading.Barrier(2)

    def insert(item, workspace_binding=None):
        """Enter two real SQLite transactions simultaneously through distinct Store connections."""
        peer = SQLiteLifecycleStore(store.database_path)
        start.wait(timeout=5)
        try:
            if workspace_binding is None:
                peer._create_sync(item, credential(item), 1)
            else:
                peer._create_with_workspace_sync(item, workspace_binding, ws.version, credential(item), 1)
            return True
        except storage.LifecycleInstanceLimitExceeded:
            return False

    results = await asyncio.gather(asyncio.to_thread(insert, first), asyncio.to_thread(insert, second, binding if bound else None))
    assert sorted(results) == [False, True]
    with closing(sqlite3.connect(store.database_path)) as connection:
        assert connection.execute("SELECT COUNT(*) FROM lifecycle_records").fetchone() == (1,)
        assert connection.execute("SELECT COUNT(*) FROM endpoint_credentials").fetchone() == (1,)
    if not results[1]:
        assert await store.get_workspace(workspace.workspace_id) == ws
        assert await store.get_workspace_binding(second.sandbox_id, second.transaction_id) is None


@pytest.mark.anyio
async def test_rejected_workspace_transaction_changes_nothing_and_is_reusable(tmp_path):
    """Quota rejection cannot acquire a workspace, increment it or leave a credential behind."""
    store = await initialized(tmp_path)
    runtime = WorkspaceRuntime()
    service = SandboxService(runtime, store, clock=lambda: NOW, max_instances=1)
    workspace = await service.create_workspace()
    first = await service.create()
    before = snapshot(store.database_path)
    with pytest.raises(errors.SandboxInstanceLimitExceeded):
        await service.create(SandboxCreateOptions(workspace_id=workspace.workspace_id))
    unchanged = snapshot(store.database_path) == before
    assert unchanged and len(runtime.specs) == 1
    await service.delete(first.sandbox_id)
    second = await service.create(SandboxCreateOptions(workspace_id=workspace.workspace_id))
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id == second.sandbox_id


@pytest.mark.anyio
@pytest.mark.parametrize("outcome", ["rollback", "unknown", "cleanup"])
async def test_creation_failure_releases_only_confirmed_cleanup(tmp_path, outcome):
    """Uncertain creation and incomplete rollback retain both the record and its capacity."""
    store = await initialized(tmp_path)
    runtime = WorkspaceRuntime()
    service = SandboxService(runtime, store, max_instances=1)
    runtime.create_error = {"rollback": errors.SandboxCreateFailed(), "unknown": RuntimeError("private"), "cleanup": errors.SandboxCleanupFailed(FIXED_SANDBOX_ID)}[outcome]
    expected = {"rollback": errors.SandboxCreateFailed, "unknown": errors.SandboxCreateUncertain, "cleanup": errors.SandboxCleanupFailed}[outcome]
    with pytest.raises(expected):
        await service.create()
    runtime.create_error = None
    if outcome == "rollback":
        await service.create()
    else:
        with pytest.raises(errors.SandboxInstanceLimitExceeded):
            await service.create()
        assert len(runtime.specs) == 1


@pytest.mark.anyio
async def test_restart_and_lower_limit_do_not_evict_and_deletion_restores_capacity(tmp_path):
    """A new Service uses all persisted rows, allows management and waits for guarded removal."""
    store = await initialized(tmp_path)
    runtime = WorkspaceRuntime()
    original = SandboxService(runtime, store, max_instances=2, clock=lambda: NOW)
    first, second = await original.create(), await original.create()
    restarted = SandboxService(runtime, SQLiteLifecycleStore(store.database_path), max_instances=1, clock=lambda: NOW)
    with pytest.raises(errors.SandboxInstanceLimitExceeded):
        await restarted.create()
    assert (await restarted.get(first.sandbox_id)).sandbox_id == first.sandbox_id
    await restarted.pause(first.sandbox_id)
    await restarted.renew(first.sandbox_id, 3600)
    await restarted.resume(first.sandbox_id)
    await restarted.delete(first.sandbox_id)
    with pytest.raises(errors.SandboxInstanceLimitExceeded):
        await restarted.create()
    await restarted.delete(second.sandbox_id)
    await restarted.create()


@pytest.mark.anyio
@pytest.mark.parametrize("runtime_status", ["paused", "stopped"])
async def test_runtime_status_never_releases_persisted_capacity(tmp_path, runtime_status):
    """Frozen or stopped resources remain charged until metadata is actually removed."""
    store = await initialized(tmp_path)
    runtime = WorkspaceRuntime()
    service = SandboxService(runtime, store, max_instances=1)
    first = await service.create()
    runtime.statuses[first.sandbox_id] = runtime_status
    assert (await service.get(first.sandbox_id)).status == runtime_status
    with pytest.raises(errors.SandboxInstanceLimitExceeded):
        await service.create()


@pytest.mark.anyio
@pytest.mark.parametrize("failure_stage", ["runtime", "metadata"])
async def test_reaper_failure_retains_slot_then_success_releases_it(tmp_path, failure_stage):
    """Deletion retries retain capacity even after expiry until guarded metadata cleanup succeeds."""
    store = await initialized(tmp_path, FaultStore)
    runtime = WorkspaceRuntime()
    now = NOW
    service = SandboxService(runtime, store, max_instances=1, clock=lambda: now)
    first = await service.create(SandboxCreateOptions(ttl_seconds=1))
    now = NOW + timedelta(seconds=1)
    runtime.delete_error = errors.SandboxDeleteFailed(first.sandbox_id) if failure_stage == "runtime" else None
    store.fail_delete = failure_stage == "metadata"
    assert (await service.reap_expired_once()).failed == 1
    with pytest.raises(errors.SandboxInstanceLimitExceeded):
        await service.create()
    runtime.delete_error = None
    store.fail_delete = False
    assert (await service.reap_expired_once()).succeeded == 1
    await service.create()
    assert await store.get(first.sandbox_id) is None


@pytest.mark.anyio
@pytest.mark.parametrize("late_failure", [False, True])
async def test_repeated_cancelled_sqlite_write_holds_slot_until_outcome_is_known(tmp_path, late_failure):
    """Late commit failures retain capacity; confirmed cancelled prewrites clean it only after settling."""
    store = await initialized(tmp_path, FaultStore)
    runtime = ControlledRuntime(store)
    store.block_write, store.fail_after_commit = True, late_failure
    service = SandboxService(runtime, store, id_factory=lambda: FIXED_SANDBOX_ID, transaction_id_factory=lambda: FIXED_TRANSACTION_ID, max_instances=1)
    task = asyncio.create_task(service.create())
    try:
        assert await asyncio.to_thread(store.write_entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        assert not task.done()
        store.write_release.set()
        with pytest.raises(errors.SandboxMetadataUnavailable if late_failure else asyncio.CancelledError):
            await task
        assert runtime.specs == []
        peer = SandboxService(WorkspaceRuntime(), SQLiteLifecycleStore(store.database_path), max_instances=1)
        if late_failure:
            with pytest.raises(errors.SandboxInstanceLimitExceeded):
                await peer.create()
        else:
            await peer.create()
    finally:
        store.write_release.set()
        if not task.done():
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["locked", "corrupt", "missing"])
async def test_storage_failures_are_never_reported_as_full_capacity(tmp_path, failure):
    """SQLite write-lock and file failures win over any apparent full-count snapshot."""
    store = await initialized(tmp_path)
    await store.create(record())
    lock = None
    if failure == "locked":
        lock = sqlite3.connect(store.database_path)
        lock.execute("BEGIN IMMEDIATE")
        store = SQLiteLifecycleStore(store.database_path, lock_timeout=0)
    elif failure == "corrupt":
        store.database_path.write_bytes(b"not a SQLite database")
    else:
        store = SQLiteLifecycleStore(tmp_path / "missing.sqlite3")
    runtime = WorkspaceRuntime()
    try:
        with pytest.raises(errors.SandboxMetadataUnavailable):
            await SandboxService(runtime, store, max_instances=1).create()
        assert runtime.specs == []
    finally:
        if lock is not None:
            lock.rollback()
            lock.close()


@pytest.mark.anyio
@pytest.mark.parametrize("incomplete", [False, True])
async def test_runtime_cancellation_holds_slot_until_rollback_finishes(tmp_path, incomplete):
    """Neither first nor repeated cancellation can reuse a slot before Runtime cleanup."""
    store = await initialized(tmp_path, FaultStore)
    runtime = ControlledRuntime(store)
    runtime.block_create = runtime.block_rollback = True
    if incomplete:
        runtime.rollback_error = errors.SandboxCleanupFailed(FIXED_SANDBOX_ID)
    service = SandboxService(runtime, store, max_instances=1)
    peer = SandboxService(WorkspaceRuntime(), SQLiteLifecycleStore(store.database_path), max_instances=1)
    task = asyncio.create_task(service.create())
    try:
        await asyncio.wait_for(runtime.create_entered.wait(), 3)
        task.cancel()
        await asyncio.wait_for(runtime.rollback_entered.wait(), 3)
        task.cancel()
        assert not task.done()
        with pytest.raises(errors.SandboxInstanceLimitExceeded):
            await peer.create()
        runtime.rollback_release.set()
        with pytest.raises(errors.SandboxCleanupFailed if incomplete else asyncio.CancelledError):
            await task
        if incomplete:
            with pytest.raises(errors.SandboxInstanceLimitExceeded):
                await peer.create()
        else:
            await peer.create()
    finally:
        runtime.create_release.set()
        runtime.rollback_release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("incomplete", [False, True])
async def test_late_runtime_success_holds_slot_through_compensation(tmp_path, incomplete):
    """A late-created instance consumes its slot until actual deletion and metadata commit."""
    store = await initialized(tmp_path, FaultStore)
    runtime = ControlledRuntime(store)
    runtime.block_create = runtime.ignore_create_cancel = runtime.block_delete = True
    if incomplete:
        runtime.delete_error = errors.SandboxDeleteFailed(FIXED_SANDBOX_ID)
    service = SandboxService(runtime, store, max_instances=1)
    peer = SandboxService(WorkspaceRuntime(), SQLiteLifecycleStore(store.database_path), max_instances=1)
    task = asyncio.create_task(service.create())
    try:
        await asyncio.wait_for(runtime.create_entered.wait(), 3)
        task.cancel()
        await asyncio.wait_for(runtime.rollback_entered.wait(), 3)
        runtime.create_release.set()
        await asyncio.wait_for(runtime.delete_entered.wait(), 3)
        task.cancel()
        with pytest.raises(errors.SandboxInstanceLimitExceeded):
            await peer.create()
        runtime.delete_release.set()
        with pytest.raises(errors.SandboxCleanupFailed if incomplete else asyncio.CancelledError):
            await task
        if incomplete:
            with pytest.raises(errors.SandboxInstanceLimitExceeded):
                await peer.create()
        else:
            await peer.create()
    finally:
        runtime.create_release.set()
        runtime.delete_release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_cancel_after_workspace_commit_waits_before_releasing_occupancy(tmp_path):
    """A committed pending insert remains charged until cancellation settles its acknowledgement."""
    store = await initialized(tmp_path, GatedWorkspaceStore)
    runtime = WorkspaceRuntime()
    service = SandboxService(runtime, store, max_instances=1)
    workspace = await service.create_workspace()
    store.block_prewrite_ack = True
    task = asyncio.create_task(service.create(SandboxCreateOptions(workspace_id=workspace.workspace_id)))
    peer = SandboxService(runtime, SQLiteLifecycleStore(store.database_path), max_instances=1)
    try:
        await asyncio.wait_for(store.prewrite_committed.wait(), 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        assert not task.done()
        with pytest.raises(errors.SandboxInstanceLimitExceeded):
            await peer.create()
        assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is not None
        store.prewrite_ack_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert runtime.specs == []
        created = await peer.create(SandboxCreateOptions(workspace_id=workspace.workspace_id))
        assert created.workspace_id == workspace.workspace_id
    finally:
        store.prewrite_ack_release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_new_application_uses_existing_count_without_evicting(tmp_path):
    """Reassembly with a lower bound rejects creation but leaves both existing instances manageable."""
    store = await initialized(tmp_path)
    runtime = WorkspaceRuntime()
    original = SandboxService(runtime, store, max_instances=2)
    first, second = await original.create(), await original.create()
    app = create_app(runtime, database_path=store.database_path, max_instances=1, api_key="instance-admin", reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control", headers={"Authorization": "Bearer instance-admin"}) as client:
            assert (await client.post("/v1/sandboxes")).status_code == 429
            assert (await client.get(f"/v1/sandboxes/{first.sandbox_id}")).status_code == 200
            assert (await client.delete(f"/v1/sandboxes/{first.sandbox_id}")).status_code == 204
            assert (await client.post("/v1/sandboxes")).status_code == 429
            assert (await client.delete(f"/v1/sandboxes/{second.sandbox_id}")).status_code == 204
            assert (await client.post("/v1/sandboxes")).status_code == 201


@pytest.mark.anyio
async def test_authentication_and_input_validation_precede_instance_admission(tmp_path):
    """Unauthorized and malformed requests win over full capacity without allocating identities."""
    from unittest.mock import Mock

    store = await initialized(tmp_path)
    await store.create(record(), max_instances=1)
    runtime = WorkspaceRuntime()
    app = create_app(runtime, store=store, api_key="instance-admin", max_instances=1, reaper_enabled=False)
    identity = Mock(return_value=record(1).sandbox_id)
    app.state.service._id_factory = identity
    before = snapshot(store.database_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
        assert (await client.post("/v1/sandboxes", content=b"invalid")).status_code == 401
        assert (await client.post("/v1/sandboxes", headers={"Authorization": "Bearer instance-admin"}, json={"ttl_seconds": 0})).status_code == 400
        assert identity.call_count == 0
        assert (await client.post("/v1/sandboxes", headers={"Authorization": "Bearer instance-admin"})).status_code == 429
    unchanged = snapshot(store.database_path) == before
    assert unchanged and not runtime.specs


@pytest.mark.anyio
async def test_cancelled_prewrite_loses_last_slot_without_metadata_error(tmp_path):
    """A worker rejected after cancellation must neither mask cancellation nor remove the winner."""
    store = await initialized(tmp_path, FaultStore)
    runtime = ControlledRuntime(store)
    store.block_write = True
    first = SandboxService(runtime, store, id_factory=lambda: FIXED_SANDBOX_ID, max_instances=1)
    task = asyncio.create_task(first.create())
    try:
        assert await asyncio.to_thread(store.write_entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        peer = SandboxService(WorkspaceRuntime(), SQLiteLifecycleStore(store.database_path), max_instances=1)
        winner = await peer.create()
        before = snapshot(store.database_path)
        store.write_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        unchanged = snapshot(store.database_path) == before
        assert unchanged and not runtime.specs
        assert await store.get(FIXED_SANDBOX_ID) is None
        assert await store.get(winner.sandbox_id) is not None
    finally:
        store.write_release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("committed", [False, True])
async def test_activation_failure_retains_slot_even_when_commit_acknowledgement_is_lost(tmp_path, committed):
    """A created Runtime with pending or uncertain-active metadata remains charged after failure."""
    store = await initialized(tmp_path, FaultStore)
    store.fail_activation, store.fail_after_activation = not committed, committed
    runtime = ControlledRuntime(store)
    service = SandboxService(runtime, store, max_instances=1)
    with pytest.raises(errors.SandboxMetadataUnavailable):
        await service.create()
    with pytest.raises(errors.SandboxInstanceLimitExceeded):
        await service.create()
    assert len(runtime.specs) == 1
    observed = await store.get(runtime.specs[0].sandbox_id)
    assert observed.management_state == ("active" if committed else "pending")


@pytest.mark.anyio
async def test_stale_metadata_delete_cannot_free_instance_capacity(tmp_path):
    """Only a successful guarded deletion releases the row used for admission."""
    store = await initialized(tmp_path)
    item = record()
    await store.create(item, credential=credential(item), max_instances=1)
    active = await store.activate(item.sandbox_id, item.transaction_id, expected_version=0)
    for transaction, version in ((item.transaction_id, 0), ("other-transaction", active.version)):
        with pytest.raises(storage.LifecycleStoreConflict):
            await store.delete(item.sandbox_id, transaction, expected_version=version)
        with pytest.raises(storage.LifecycleInstanceLimitExceeded):
            await store.create(record(1), max_instances=1)
    await store.delete(item.sandbox_id, item.transaction_id, expected_version=active.version)
    await store.create(record(1), max_instances=1)
