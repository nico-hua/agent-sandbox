"""Service identity and current-expiration merges over real temporary SQLite and fake Kubernetes reads."""

from dataclasses import replace
from datetime import timedelta

import pytest

from control_plane.core.errors import SandboxMetadataConflict, SandboxNotFound
from control_plane.core.models import SandboxLifecycleRecord
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_query import CREATED, EXPIRES, SID, TXN, QueryClient, query_runtime


@pytest.mark.anyio
@pytest.mark.parametrize("present", [True, False])
async def test_service_merges_only_matching_current_metadata_without_modifying_it(tmp_path, present):
    """A matching database overrides expiration; no record leaves initial annotations untouched without adoption."""
    store = SQLiteLifecycleStore(tmp_path / "query.sqlite3")
    await store.initialize()
    record = SandboxLifecycleRecord(SID, TXN, CREATED, EXPIRES)
    if present:
        await store.create(record)
        record = await store.activate(SID, TXN, expected_version=0)
        record = await store.update_expiration(
            SID, TXN, expires_at=EXPIRES + timedelta(hours=2), expected_version=record.version,
        )
    fake = QueryClient()
    result = await SandboxService(query_runtime(fake), store).get(SID)
    assert result.status == "ready" and result.created_at == CREATED and result.transaction_id == TXN
    assert result.expires_at == (record.expires_at if present else EXPIRES)
    assert await store.get(SID) == (record if present else None)
    assert fake.pod.metadata.annotations["io.agent-sandbox.expires-at"] == "2026-10-19T08:30:00.123456Z"
    assert fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("changed", ["transaction", "creation"])
async def test_service_rejects_mismatched_metadata_without_overwriting_records(tmp_path, changed):
    """A different transaction or business creation time cannot be merged with a Pod."""
    store = SQLiteLifecycleStore(tmp_path / "conflict.sqlite3")
    await store.initialize()
    record = SandboxLifecycleRecord(SID, TXN, CREATED, EXPIRES)
    if changed == "transaction":
        record = replace(record, transaction_id="txn_" + "f" * 32)
    elif changed == "creation":
        record = replace(record, created_at=CREATED + timedelta(microseconds=1))
    await store.create(record)
    record = await store.activate(SID, record.transaction_id, expected_version=0)
    with pytest.raises(SandboxMetadataConflict):
        await SandboxService(query_runtime(QueryClient()), store).get(SID)
    assert await store.get(SID) == record


@pytest.mark.anyio
async def test_database_record_cannot_make_an_absent_pod_exist(tmp_path):
    """Runtime absence remains 404 and does not delete or otherwise repair persistent metadata."""
    store = SQLiteLifecycleStore(tmp_path / "missing.sqlite3")
    await store.initialize()
    record = SandboxLifecycleRecord(SID, TXN, CREATED, EXPIRES)
    await store.create(record)
    fake = QueryClient()
    fake.items = []
    with pytest.raises(SandboxNotFound):
        await SandboxService(query_runtime(fake), store).get(SID)
    assert await store.get(SID) == record
