"""Read-only pending inspection over real Kubernetes orchestration and temporary SQLite."""

import asyncio
import sqlite3
import threading
from contextlib import closing
from dataclasses import replace
from datetime import timedelta

import httpx
import pytest
from kubernetes import client

from control_plane.core.models import SandboxAccessCredential, SandboxLifecycleRecord
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from kubernetes_proxy_fakes import FakeProxyClient, PREFIX
from test_kubernetes_persistent_workspace import persistence_clock
from test_kubernetes_proxy_lifecycle import authenticated_spec, proxy_clock, runtime
from test_kubernetes_query import CREATED, EXPIRES, SID, TXN

ROLES = {"container", "network-policy", "agent-service", "proxy-access", "proxy",
         "agent-proxy-ingress", "proxy-isolation", "proxy-service", "private-workspace"}


async def pending_backend(tmp_path, monkeypatch):
    """Create real version-three resources and pending metadata, then prohibit all writes."""
    fake = FakeProxyClient()
    backend = runtime(fake, workspace_storage_class="agent-sandbox-workspace")
    spec = authenticated_spec()
    await backend.create(spec)
    store = SQLiteLifecycleStore(tmp_path / "inspection.sqlite3")
    await store.initialize()
    record = SandboxLifecycleRecord(SID, TXN, CREATED, EXPIRES)
    credential = SandboxAccessCredential(SID, TXN, spec.access_token)
    await store.create(record, credential=credential)

    def forbidden(*args, **kwargs):
        """Reject runtime mutations, metadata repairs and execution HTTP access during inspection."""
        raise AssertionError("read-only pending inspection attempted a write or HTTP request")

    for name in ("_create", "_patch", "_delete"):
        monkeypatch.setattr(fake, name, forbidden)
    for name in ("create", "delete"):
        monkeypatch.setattr(backend, name, forbidden)
    for name in ("create", "activate", "mark_deleting", "delete", "update_expiration"):
        monkeypatch.setattr(store, name, forbidden)
    monkeypatch.setattr(httpx.AsyncClient, "request", forbidden)
    monkeypatch.setattr(httpx.Client, "request", forbidden)
    fake.trace.clear()
    fake.events.clear()
    fake.closed.clear()
    return backend, fake, store, record, credential


def inspection_service(backend, store):
    """Build a real Service whose observational path cannot use lifecycle time."""
    def forbidden_clock():
        """Fail on age-based recovery or renewed lifecycle timestamps."""
        raise AssertionError("pending inspection must not sample lifecycle time")

    return SandboxService(backend, store, clock=forbidden_clock)


async def assert_unchanged(fake, store, record, credential):
    """Verify immutable pending facts, private credential and closed read-only worker ownership."""
    assert await store.get(SID) == record
    assert await store.get_credential(SID, TXN) == credential
    assert all(verb in {"list", "read"} for verb, _ in fake.trace)
    assert fake.closed.is_set()


def gate_inventory(fake, monkeypatch):
    """Pause the first Pod inventory without changing shared fake API event semantics."""
    entered, release = threading.Event(), threading.Event()
    original = fake._list

    def gated(kind, namespace, kwargs):
        """Retain the worker handle while a competing persistent change or cancellation occurs."""
        if kind == "pod" and not entered.is_set():
            entered.set()
            if not release.wait(3):
                raise AssertionError("inspection inventory worker was not released")
        return original(kind, namespace, kwargs)

    monkeypatch.setattr(fake, "_list", gated)
    return entered, release


@pytest.mark.anyio
@pytest.mark.parametrize("observation", ["complete", "not-ready", "partial", "conflict", "unavailable"])
async def test_pending_inspection_reports_backend_facts_without_recovery(
    tmp_path, monkeypatch, persistence_clock, observation,
):
    """Pending PVC and unready Pods remain present; missing, foreign and unavailable stay distinct."""
    backend, fake, store, record, credential = await pending_backend(tmp_path, monkeypatch)
    expected = ("all_present", "kubernetes_resources_complete")
    found, missing = ROLES, set()
    if observation == "not-ready":
        fake.startup = {"container": "not-ready", "proxy": "pending"}
        fake.pod.status.conditions[0].status = "False"
        fake.pod.status.container_statuses[0].ready = False
        fake.proxy_pod.status = client.V1PodStatus(phase="Pending")
        fake.workspace.status = client.V1PersistentVolumeClaimStatus(phase="Pending")
    elif observation == "partial":
        fake.proxy_pod = None
        expected = ("partial", "kubernetes_resources_partial")
        found, missing = ROLES - {"proxy"}, {"proxy"}
    elif observation == "conflict":
        fake.pod.metadata.labels[PREFIX + "transaction-id"] = "txn_" + "f" * 32
        expected = ("conflict", "kubernetes_resource_conflict")
        found = set()
    elif observation == "unavailable":
        fake.faults[("list", "pod")] = client.ApiException(status=503, reason="private-backend-diagnostic")
        expected = ("unavailable", "kubernetes_inspection_unavailable")
        found = set()

    result = await inspection_service(backend, store).inspect_pending_once()

    assert result.error is None and len(result.items) == 1
    item = result.items[0]
    assert (item.classification, item.reason) == expected
    assert set(item.found_resources) == found and set(item.missing_resources) == missing
    assert "private-backend-diagnostic" not in repr(result)
    await assert_unchanged(fake, store, record, credential)


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["active", "deleting", "deleted", "transaction", "version", "created_at"])
async def test_real_runtime_observation_is_discarded_when_pending_record_changes(
    tmp_path, monkeypatch, persistence_clock, change,
):
    """Competing SQLite state, identity, time and version changes override even a complete inventory."""
    backend, fake, store, record, credential = await pending_backend(tmp_path, monkeypatch)
    entered, release = gate_inventory(fake, monkeypatch)
    task = asyncio.create_task(inspection_service(backend, store).inspect_pending_once())
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        other = SQLiteLifecycleStore(tmp_path / "inspection.sqlite3")
        if change == "active":
            current = await other.activate(SID, TXN, expected_version=0)
        elif change == "deleting":
            current = await other.mark_deleting(SID, TXN, expected_version=0)
        elif change == "version":
            def increment_version():
                """Commit a version-only concurrent update through an independent SQLite connection."""
                with closing(sqlite3.connect(tmp_path / "inspection.sqlite3")) as connection, connection:
                    connection.execute("UPDATE lifecycle_records SET version = version + 1 WHERE sandbox_id = ?", (SID,))

            await asyncio.to_thread(increment_version)
            current = replace(record, version=1)
        else:
            await other.delete(SID, TXN, expected_version=0)
            current = None
            if change == "transaction":
                current = replace(record, transaction_id="txn_" + "f" * 32)
            elif change == "created_at":
                current = replace(record, created_at=CREATED + timedelta(microseconds=1))
            if current is not None:
                await other.create(current)
    finally:
        release.set()
    result = await task

    assert result.error is None and len(result.items) == 1
    assert (result.items[0].classification, result.items[0].reason) == ("changed", "pending_record_changed")
    assert await store.get(SID) == current
    assert all(verb in {"list", "read"} for verb, _ in fake.trace)
    assert fake.closed.is_set()
    if change in {"active", "deleting", "version"}:
        assert await store.get_credential(SID, TXN) == credential


@pytest.mark.anyio
@pytest.mark.parametrize("late_failure", [False, True])
async def test_repeated_pending_inspection_cancellation_settles_real_kubernetes_worker(
    tmp_path, monkeypatch, persistence_clock, late_failure,
):
    """Repeated cancellation waits for the SDK worker and close without activation or recovery."""
    backend, fake, store, record, credential = await pending_backend(tmp_path, monkeypatch)
    entered, release = gate_inventory(fake, monkeypatch)
    if late_failure:
        fake.faults[("list", "pod")] = client.ApiException(status=503)
    task = asyncio.create_task(inspection_service(backend, store).inspect_pending_once())
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not fake.closed.is_set()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await assert_unchanged(fake, store, record, credential)
    assert len({thread for _, thread in fake.events}) == 1
