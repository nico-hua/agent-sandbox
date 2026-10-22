"""Service transactions across real temporary SQLite and an isolated fake runtime."""

import asyncio
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from control_plane.core.service import SandboxService
from control_plane.core.errors import (
    DockerUnavailable, SandboxCleanupFailed, SandboxCreateFailed,
    SandboxCreateUncertain, SandboxDeleteFailed, SandboxImageUnavailable,
    SandboxMetadataCleanupFailed, SandboxMetadataConflict,
    SandboxMetadataUnavailable, SandboxNotFound, SandboxResourceConflict,
)
from control_plane.core.models import SandboxCreateOptions, SandboxResources, SandboxSpec
from control_plane.storage.lifecycle_store import (
    SQLiteLifecycleStore, LifecycleStoreConflict, LifecycleStoreError,
)
from test_sandbox_service import FakeRuntime, FIXED_SANDBOX_ID, FIXED_TRANSACTION_ID


class FaultStore(SQLiteLifecycleStore):
    """Gate actual SQLite workers and inject failures without replacing persistence."""

    def __init__(self, database: Path) -> None:
        """Initialize a real file store with deterministic worker coordination."""
        super().__init__(database)
        self.block_write = False
        self.block_delete = False
        self.fail_write = False
        self.fail_after_commit = False
        self.fail_read = False
        self.fail_delete = False
        self.block_activation = False
        self.fail_activation = False
        self.fail_after_activation = False
        self.write_entered = threading.Event()
        self.write_release = threading.Event()
        self.delete_entered = threading.Event()
        self.delete_release = threading.Event()
        self.activation_entered = threading.Event()
        self.activation_release = threading.Event()

    def _create_sync(self, record) -> None:
        """Delay a real commit until released, then optionally fail before or after it."""
        if self.block_write:
            self.write_entered.set()
            assert self.write_release.wait(timeout=3), "write gate was not released"
        if self.fail_write:
            raise LifecycleStoreError()
        super()._create_sync(record)
        if self.fail_after_commit:
            raise LifecycleStoreError()

    async def get(self, sandbox_id: str):
        """Read real persisted records unless the read failure is explicitly enabled."""
        if self.fail_read:
            raise LifecycleStoreError()
        return await super().get(sandbox_id)

    def _activate_sync(self, sandbox_id: str, transaction_id: str, expected_version: int):
        """Gate the real activation worker to expose late commits and stable failures."""
        if self.block_activation:
            self.activation_entered.set()
            assert self.activation_release.wait(timeout=3), "activation gate was not released"
        if self.fail_activation:
            raise LifecycleStoreError()
        result = super()._activate_sync(sandbox_id, transaction_id, expected_version)
        if self.fail_after_activation:
            raise LifecycleStoreError()
        return result

    def _delete_sync(self, sandbox_id: str, transaction_id: str, expected_version: int) -> None:
        """Gate guarded real deletion and preserve the row when failure is enabled."""
        if self.block_delete:
            self.delete_entered.set()
            assert self.delete_release.wait(timeout=3), "delete gate was not released"
        if self.fail_delete:
            raise LifecycleStoreError()
        super()._delete_sync(sandbox_id, transaction_id, expected_version)


class ControlledRuntime(FakeRuntime):
    """Expose creation, rollback and deletion boundaries independently of SQLite."""

    def __init__(self, store: FaultStore) -> None:
        """Set independent runtime failure and cancellation controls."""
        super().__init__()
        self.store = store
        self.create_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.rollback_error: Exception | None = None
        self.block_create = False
        self.block_rollback = False
        self.block_delete = False
        self.ignore_create_cancel = False
        self.create_entered = asyncio.Event()
        self.create_release = asyncio.Event()
        self.rollback_entered = asyncio.Event()
        self.rollback_release = asyncio.Event()
        self.delete_entered = asyncio.Event()
        self.delete_release = asyncio.Event()
        self.expected_deletions: list[tuple[str, bool, str | None]] = []
        self.live_resources: set[tuple[str, str]] = set()

    async def create(self, spec: SandboxSpec) -> None:
        """Assert durable prewrite and emulate confirmed rollback or a late commit."""
        self.specs.append(spec)
        record = await self.store.get(spec.sandbox_id)
        assert record is not None and record.transaction_id == spec.transaction_id
        assert record.management_state == "pending" and record.version == 0
        self.create_entered.set()
        if self.block_create:
            try:
                await self.create_release.wait()
            except asyncio.CancelledError:
                self.rollback_entered.set()
                if self.ignore_create_cancel:
                    await self.create_release.wait()
                else:
                    if self.block_rollback:
                        await self.rollback_release.wait()
                    if self.rollback_error is not None:
                        raise self.rollback_error
                    raise
        if self.create_error is not None:
            raise self.create_error
        self.query_result = replace(
            self.query_result, sandbox_id=spec.sandbox_id,
            transaction_id=spec.transaction_id, created_at=spec.created_at,
            expires_at=spec.expires_at,
        )
        self.live_resources.add((spec.sandbox_id, spec.transaction_id))

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Record identity protection and complete deletion only after the test releases it."""
        self.expected_deletions.append((sandbox_id, delete_workspace, expected_transaction_id))
        self.delete_entered.set()
        if self.block_delete:
            await self.delete_release.wait()
        if self.delete_error is not None:
            raise self.delete_error
        self.live_resources.difference_update({
            identity for identity in self.live_resources
            if identity[0] == sandbox_id
            and (expected_transaction_id is None or identity[1] == expected_transaction_id)
        })


@pytest.fixture
async def store(tmp_path: Path) -> FaultStore:
    """Initialize a real per-test file database without any Docker resources."""
    result = FaultStore(tmp_path / "lifecycle.sqlite3")
    await result.initialize()
    return result


def service_for(runtime: FakeRuntime, store: SQLiteLifecycleStore) -> SandboxService:
    """Inject fixed creation identity and time into the real service."""
    return SandboxService(
        runtime, store,
        id_factory=lambda: FIXED_SANDBOX_ID,
        transaction_id_factory=lambda: FIXED_TRANSACTION_ID,
        clock=lambda: datetime(2026, 10, 19, 8, tzinfo=UTC),
    )


@pytest.mark.anyio
async def test_creation_persists_metadata_before_return_and_survives_new_store(
    tmp_path: Path,
) -> None:
    """A successful creation must have the same durable identity and initial times."""
    database = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database)
    await store.initialize()
    runtime = FakeRuntime()
    service = SandboxService(
        runtime, store,
        id_factory=lambda: FIXED_SANDBOX_ID,
        transaction_id_factory=lambda: FIXED_TRANSACTION_ID,
        clock=lambda: datetime(2026, 10, 19, 8, tzinfo=UTC),
    )

    result = await service.create()

    record = await SQLiteLifecycleStore(database).get(result.sandbox_id)
    assert record is not None
    assert record.transaction_id == FIXED_TRANSACTION_ID
    assert record.created_at == result.created_at
    assert record.expires_at == result.expires_at
    assert record.version == 1
    assert record.management_state == "active"
    assert runtime.specs[0].created_at == record.created_at


@pytest.mark.anyio
@pytest.mark.parametrize("committed", [False, True])
async def test_activation_failure_keeps_traceable_record_without_create_success(
    store: FaultStore, committed: bool,
) -> None:
    """An activation failure retains either pending or late-active metadata for diagnosis."""
    runtime = ControlledRuntime(store)
    store.fail_activation = not committed
    store.fail_after_activation = committed

    with pytest.raises(SandboxMetadataUnavailable) as failure:
        await service_for(runtime, store).create()

    assert failure.value.sandbox_id == FIXED_SANDBOX_ID
    record = await store.get(FIXED_SANDBOX_ID)
    assert record is not None
    assert record.management_state == ("active" if committed else "pending")
    assert record.version == int(committed)
    assert len(runtime.specs) == 1
    assert runtime.expected_deletions == []


@pytest.mark.anyio
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_cancelled_activation_waits_for_commit_and_uses_confirmed_version(
    store: FaultStore, cleanup_fails: bool,
) -> None:
    """Repeated cancellation waits for activation and compensates its actual version."""
    runtime = ControlledRuntime(store)
    store.block_activation = True
    if cleanup_fails:
        runtime.delete_error = SandboxDeleteFailed(FIXED_SANDBOX_ID)
    task = asyncio.create_task(service_for(runtime, store).create())
    try:
        assert await asyncio.to_thread(store.activation_entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        record = await store.get(FIXED_SANDBOX_ID)
        assert record.management_state == "pending"
        store.activation_release.set()
        expected = SandboxCleanupFailed if cleanup_fails else asyncio.CancelledError
        with pytest.raises(expected):
            await task
        assert runtime.expected_deletions == [(FIXED_SANDBOX_ID, True, FIXED_TRANSACTION_ID)]
        record = await store.get(FIXED_SANDBOX_ID)
        if cleanup_fails:
            assert record is not None and record.version == 2
            assert record.management_state == "deleting"
            runtime.delete_error = None
            retried = await service_for(runtime, store).reap_expired_once()
            assert retried.succeeded == 1
            assert await store.get(FIXED_SANDBOX_ID) is None
        else:
            assert record is None
    finally:
        store.activation_release.set()


@pytest.mark.anyio
async def test_cancelled_activation_with_unknown_commit_keeps_retry_identity(
    store: FaultStore,
) -> None:
    """A committed activation whose acknowledgement fails must retain its identity."""
    runtime = ControlledRuntime(store)
    store.block_activation = True
    store.fail_after_activation = True
    task = asyncio.create_task(service_for(runtime, store).create())
    try:
        assert await asyncio.to_thread(store.activation_entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        store.activation_release.set()
        with pytest.raises(SandboxMetadataUnavailable):
            await task
        record = await store.get(FIXED_SANDBOX_ID)
        assert record is not None and record.management_state == "active"
        assert record.version == 1
        assert runtime.expected_deletions == []
    finally:
        store.activation_release.set()


@pytest.mark.anyio
async def test_failed_insert_and_duplicate_never_start_runtime(store: FaultStore) -> None:
    """A store failure or occupied ID must stop creation before backend resource calls."""
    runtime = ControlledRuntime(store)
    service = service_for(runtime, store)
    store.fail_write = True
    with pytest.raises(SandboxMetadataUnavailable) as failure:
        await service.create()
    assert failure.value.sandbox_id == FIXED_SANDBOX_ID
    assert runtime.specs == []
    store.fail_write = False
    await service.create()
    original = await store.get(FIXED_SANDBOX_ID)
    with pytest.raises(SandboxMetadataConflict):
        await service.create()
    assert len(runtime.specs) == 1
    assert await store.get(FIXED_SANDBOX_ID) == original


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [SandboxCreateFailed(), SandboxImageUnavailable(), SandboxResourceConflict()])
async def test_confirmed_rollback_removes_only_prewrite(store: FaultStore, failure: Exception) -> None:
    """A confirmed backend rollback permits exact version-zero metadata compensation."""
    runtime = ControlledRuntime(store)
    runtime.create_error = failure
    with pytest.raises(type(failure)):
        await service_for(runtime, store).create()
    assert await store.get(FIXED_SANDBOX_ID) is None


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [DockerUnavailable("secret"), RuntimeError("secret"), SandboxCleanupFailed(FIXED_SANDBOX_ID)])
async def test_unknown_or_incomplete_creation_keeps_durable_record(
    store: FaultStore, failure: Exception,
) -> None:
    """An uncertain runtime outcome must retain metadata and return a diagnostic ID."""
    runtime = ControlledRuntime(store)
    runtime.create_error = failure
    expected = SandboxCleanupFailed if isinstance(failure, SandboxCleanupFailed) else SandboxCreateUncertain
    with pytest.raises(expected) as captured:
        await service_for(runtime, store).create()
    assert captured.value.sandbox_id == FIXED_SANDBOX_ID
    assert "secret" not in str(captured.value)
    assert await store.get(FIXED_SANDBOX_ID) is not None


@pytest.mark.anyio
async def test_compensation_failure_keeps_record_and_diagnostic_id(store: FaultStore) -> None:
    """Metadata cleanup failure must remain visible after confirmed runtime rollback."""
    runtime = ControlledRuntime(store)
    runtime.create_error = SandboxCreateFailed()
    store.fail_delete = True
    with pytest.raises(SandboxMetadataCleanupFailed) as failure:
        await service_for(runtime, store).create()
    assert failure.value.sandbox_id == FIXED_SANDBOX_ID
    assert await store.get(FIXED_SANDBOX_ID) is not None


@pytest.mark.anyio
@pytest.mark.parametrize("late_error", [False, True])
async def test_cancelled_sqlite_write_waits_for_commit_before_compensation(
    store: FaultStore, late_error: bool,
) -> None:
    """Repeated cancellation cannot hide a late commit or start runtime creation."""
    runtime = ControlledRuntime(store)
    store.block_write = True
    store.fail_after_commit = late_error
    task = asyncio.create_task(service_for(runtime, store).create())
    try:
        assert await asyncio.to_thread(store.write_entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        store.write_release.set()
        expected = SandboxMetadataUnavailable if late_error else asyncio.CancelledError
        with pytest.raises(expected):
            await task
        assert runtime.specs == []
        assert (await store.get(FIXED_SANDBOX_ID) is not None) is late_error
    finally:
        store.write_release.set()


@pytest.mark.anyio
@pytest.mark.parametrize("rollback_fails", [False, True])
@pytest.mark.parametrize("options", [None, SandboxCreateOptions(123, SandboxResources(750, 384))])
async def test_cancelled_runtime_waits_for_rollback_and_preserves_uncertain_records(
    store: FaultStore, rollback_fails: bool, options: SandboxCreateOptions | None,
) -> None:
    """Metadata survives repeated cancellation until runtime rollback is confirmed."""
    runtime = ControlledRuntime(store)
    runtime.block_create = True
    runtime.block_rollback = True
    if rollback_fails:
        runtime.rollback_error = SandboxCleanupFailed(FIXED_SANDBOX_ID)
    task = asyncio.create_task(service_for(runtime, store).create(options))
    try:
        await runtime.create_entered.wait()
        task.cancel()
        await runtime.rollback_entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert await store.get(FIXED_SANDBOX_ID) is not None
        runtime.rollback_release.set()
        expected = SandboxCleanupFailed if rollback_fails else asyncio.CancelledError
        with pytest.raises(expected):
            await task
        assert (await store.get(FIXED_SANDBOX_ID) is not None) is rollback_fails
    finally:
        runtime.create_release.set()
        runtime.rollback_release.set()


@pytest.mark.anyio
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_late_runtime_commit_is_deleted_by_exact_transaction_on_cancel(
    store: FaultStore, cleanup_fails: bool,
) -> None:
    """A creation that finishes despite cancellation needs guarded runtime cleanup."""
    runtime = ControlledRuntime(store)
    runtime.block_create = True
    runtime.ignore_create_cancel = True
    if cleanup_fails:
        runtime.delete_error = SandboxDeleteFailed(FIXED_SANDBOX_ID)
    task = asyncio.create_task(service_for(runtime, store).create())
    try:
        await runtime.create_entered.wait()
        task.cancel()
        await runtime.rollback_entered.wait()
        task.cancel()
        runtime.create_release.set()
        expected = SandboxCleanupFailed if cleanup_fails else asyncio.CancelledError
        with pytest.raises(expected):
            await task
        assert runtime.expected_deletions == [(FIXED_SANDBOX_ID, True, FIXED_TRANSACTION_ID)]
        record = await store.get(FIXED_SANDBOX_ID)
        assert (record is not None) is cleanup_fails
        if cleanup_fails:
            assert record.management_state == "deleting" and record.version == 1
            runtime.delete_error = None
            retried = await service_for(runtime, store).reap_expired_once()
            assert retried.succeeded == 1
            assert await store.get(FIXED_SANDBOX_ID) is None
    finally:
        runtime.create_release.set()


@pytest.mark.anyio
async def test_manual_delete_during_creation_compensates_late_runtime_success(
    store: FaultStore,
) -> None:
    """A pending row removed during creation cannot leave untracked late runtime resources."""
    runtime = ControlledRuntime(store)
    runtime.block_create = True
    service = service_for(runtime, store)
    creation = asyncio.create_task(service.create())
    try:
        await runtime.create_entered.wait()
        pending = await store.get(FIXED_SANDBOX_ID)
        assert pending is not None and pending.management_state == "pending"
        await service.delete(FIXED_SANDBOX_ID)
        assert await store.get(FIXED_SANDBOX_ID) is None
        runtime.create_release.set()
        with pytest.raises(SandboxMetadataConflict):
            await creation
        assert runtime.expected_deletions == [
            (FIXED_SANDBOX_ID, False, FIXED_TRANSACTION_ID),
            (FIXED_SANDBOX_ID, True, FIXED_TRANSACTION_ID),
        ]
        assert runtime.live_resources == set()
        assert await store.get(FIXED_SANDBOX_ID) is None
    finally:
        runtime.create_release.set()


@pytest.mark.anyio
@pytest.mark.parametrize("cancel_creation", [False, True])
async def test_late_creation_waits_for_manual_deletion_then_cleans_exact_resources(
    store: FaultStore, cancel_creation: bool,
) -> None:
    """Late resource compensation waits for claimed deletion and still cleans only its transaction."""
    runtime = ControlledRuntime(store)
    runtime.block_create = True
    runtime.ignore_create_cancel = cancel_creation
    late_created = asyncio.Event()
    create_runtime = runtime.create

    async def create_and_notify(spec: SandboxSpec) -> None:
        """Expose the late resource commit before its coordinated compensation begins."""
        await create_runtime(spec)
        late_created.set()

    runtime.create = create_and_notify
    service = service_for(runtime, store)
    creation = asyncio.create_task(service.create())
    deletion = None
    try:
        await runtime.create_entered.wait()
        store.block_delete = True
        deletion = asyncio.create_task(service.delete(FIXED_SANDBOX_ID))
        assert await asyncio.to_thread(store.delete_entered.wait, 3)
        deleting = await store.get(FIXED_SANDBOX_ID)
        assert deleting.management_state == "deleting" and deleting.version == 1
        if cancel_creation:
            creation.cancel()
            await runtime.rollback_entered.wait()
            creation.cancel()
        runtime.create_release.set()
        await asyncio.wait_for(late_created.wait(), 2)
        # Manual deletion owns the instance until metadata cleanup finishes;
        # compensation cannot race it even after the resource commit arrives.
        assert not creation.done()
        assert runtime.expected_deletions == [(FIXED_SANDBOX_ID, False, FIXED_TRANSACTION_ID)]
        assert runtime.live_resources == {(FIXED_SANDBOX_ID, FIXED_TRANSACTION_ID)}
        assert await store.get(FIXED_SANDBOX_ID) == deleting
        store.delete_release.set()
        await deletion
        expected = asyncio.CancelledError if cancel_creation else SandboxMetadataConflict
        with pytest.raises(expected):
            await creation
        assert runtime.expected_deletions == [
            (FIXED_SANDBOX_ID, False, FIXED_TRANSACTION_ID),
            (FIXED_SANDBOX_ID, True, FIXED_TRANSACTION_ID),
        ]
        assert runtime.live_resources == set()
        assert await store.get(FIXED_SANDBOX_ID) is None
    finally:
        runtime.create_release.set()
        store.delete_release.set()
        tasks = [creation] if deletion is None else [creation, deletion]
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("cancel_creation", [False, True])
async def test_late_creation_cleanup_preserves_replacement_transaction_metadata(
    store: FaultStore, cancel_creation: bool,
) -> None:
    """Confirmed original resources may be cleaned without adopting replacement metadata."""
    runtime = ControlledRuntime(store)
    runtime.block_create = True
    runtime.ignore_create_cancel = cancel_creation
    service = service_for(runtime, store)
    creation = asyncio.create_task(service.create())
    try:
        await runtime.create_entered.wait()
        original = await store.get(FIXED_SANDBOX_ID)
        await store.delete(
            original.sandbox_id, original.transaction_id, expected_version=original.version,
        )
        replacement = replace(
            original, transaction_id="txn_ffffffffffffffffffffffffffffffff",
        )
        await store.create(replacement)
        if cancel_creation:
            creation.cancel()
            await runtime.rollback_entered.wait()
            creation.cancel()
        runtime.create_release.set()
        with pytest.raises(SandboxMetadataConflict):
            await creation
        assert runtime.expected_deletions == [
            (FIXED_SANDBOX_ID, True, FIXED_TRANSACTION_ID),
        ]
        assert runtime.live_resources == set()
        assert await store.get(FIXED_SANDBOX_ID) == replacement
    finally:
        runtime.create_release.set()
        await asyncio.gather(creation, return_exceptions=True)


@pytest.mark.anyio
async def test_database_expiration_overrides_only_expiration_after_restart(store: FaultStore) -> None:
    """Fresh service/store objects merge current metadata while preserving runtime facts."""
    runtime = ControlledRuntime(store)
    created = await service_for(runtime, store).create()
    initial = runtime.query_result
    new_expiration = created.expires_at + timedelta(seconds=600, microseconds=1)
    await store.update_expiration(
        FIXED_SANDBOX_ID, FIXED_TRANSACTION_ID, expected_version=1, expires_at=new_expiration,
    )
    fresh_store = SQLiteLifecycleStore(store._path)
    result = await service_for(runtime, fresh_store).get(FIXED_SANDBOX_ID)
    assert result == replace(initial, expires_at=new_expiration)
    assert runtime.query_result == initial
    assert runtime.specs[0].expires_at == created.expires_at


@pytest.mark.anyio
async def test_runtime_not_found_wins_over_database_record_or_failure(store: FaultStore) -> None:
    """A database row never turns a missing actual instance into a successful query."""
    runtime = ControlledRuntime(store)
    service = service_for(runtime, store)
    await service.create()
    runtime.error = SandboxNotFound()
    store.fail_read = True
    with pytest.raises(SandboxNotFound):
        await service.get(FIXED_SANDBOX_ID)


@pytest.mark.anyio
@pytest.mark.parametrize("legacy_times", [False, True])
async def test_queries_without_record_do_not_adopt_instances(
    store: FaultStore, legacy_times: bool,
) -> None:
    """Legacy nulls and initial labels remain readable without creating metadata."""
    runtime = FakeRuntime()
    if legacy_times:
        runtime.query_result = replace(
            runtime.query_result, transaction_id=FIXED_TRANSACTION_ID,
            created_at=datetime(2026, 10, 19, 8, tzinfo=UTC),
            expires_at=datetime(2026, 10, 19, 8, 30, tzinfo=UTC),
        )
    result = await service_for(runtime, store).get(FIXED_SANDBOX_ID)
    assert result == runtime.query_result
    assert await store.get(FIXED_SANDBOX_ID) is None


@pytest.mark.anyio
@pytest.mark.parametrize("field", ["sandbox_id", "transaction_id", "created_at"])
async def test_query_refuses_identity_mismatch(store: FaultStore, field: str) -> None:
    """Only the same instance, transaction and creation time may use current metadata."""
    runtime = ControlledRuntime(store)
    service = service_for(runtime, store)
    await service.create()
    replacement = {
        "sandbox_id": "sbx_ffffffffffffffffffffffffffffffff",
        "transaction_id": "txn_ffffffffffffffffffffffffffffffff",
        "created_at": runtime.query_result.created_at + timedelta(microseconds=1),
    }[field]
    runtime.query_result = replace(runtime.query_result, **{field: replacement})
    with pytest.raises(SandboxMetadataConflict):
        await service.get(FIXED_SANDBOX_ID)


@pytest.mark.anyio
@pytest.mark.parametrize("delete_workspace", [False, True])
async def test_delete_uses_transaction_without_requiring_runtime_query(
    store: FaultStore, delete_workspace: bool,
) -> None:
    """Deleting partial resources can finish and remove metadata even with retained volume."""
    runtime = ControlledRuntime(store)
    service = service_for(runtime, store)
    await service.create()
    runtime.error = SandboxNotFound()
    await service.delete(FIXED_SANDBOX_ID, delete_workspace=delete_workspace)
    assert runtime.queries == []
    assert runtime.expected_deletions == [(FIXED_SANDBOX_ID, delete_workspace, FIXED_TRANSACTION_ID)]
    assert await store.get(FIXED_SANDBOX_ID) is None
    await service.delete(FIXED_SANDBOX_ID, delete_workspace=True)
    assert runtime.expected_deletions[-1] == (FIXED_SANDBOX_ID, True, None)


@pytest.mark.anyio
@pytest.mark.parametrize("failure_stage", ["runtime", "metadata"])
async def test_partial_deletion_preserves_retry_record(store: FaultStore, failure_stage: str) -> None:
    """Runtime failure and metadata cleanup failure remain traceable and retryable."""
    runtime = ControlledRuntime(store)
    service = service_for(runtime, store)
    await service.create()
    if failure_stage == "runtime":
        runtime.delete_error = SandboxDeleteFailed(FIXED_SANDBOX_ID)
        expected = SandboxDeleteFailed
    else:
        store.fail_delete = True
        expected = SandboxMetadataCleanupFailed
    with pytest.raises(expected) as failure:
        await service.delete(FIXED_SANDBOX_ID)
    assert failure.value.sandbox_id == FIXED_SANDBOX_ID
    assert await store.get(FIXED_SANDBOX_ID) is not None
    runtime.delete_error = None
    store.fail_delete = False
    await service.delete(FIXED_SANDBOX_ID)
    assert await store.get(FIXED_SANDBOX_ID) is None


@pytest.mark.anyio
async def test_delete_does_not_refresh_a_replaced_transaction(store: FaultStore) -> None:
    """A new transaction during runtime deletion must never be adopted for cleanup."""
    runtime = ControlledRuntime(store)
    service = service_for(runtime, store)
    await service.create()
    runtime.block_delete = True
    task = asyncio.create_task(service.delete(FIXED_SANDBOX_ID))
    try:
        await runtime.delete_entered.wait()
        initial = await store.get(FIXED_SANDBOX_ID)
        assert initial.management_state == "deleting" and initial.version == 2
        with pytest.raises(LifecycleStoreConflict):
            await store.update_expiration(
                FIXED_SANDBOX_ID, FIXED_TRANSACTION_ID, expected_version=initial.version,
                expires_at=initial.expires_at + timedelta(seconds=1),
            )
        await store.delete(
            FIXED_SANDBOX_ID, FIXED_TRANSACTION_ID, expected_version=initial.version,
        )
        changed = replace(
            initial, transaction_id="txn_ffffffffffffffffffffffffffffffff",
            version=0, management_state="pending",
        )
        await store.create(changed)
        runtime.delete_release.set()
        with pytest.raises(SandboxMetadataCleanupFailed):
            await task
        assert await store.get(FIXED_SANDBOX_ID) == changed
    finally:
        runtime.delete_release.set()


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["runtime_delete", "metadata_delete", "create_compensation"])
async def test_repeated_cancellation_waits_for_all_critical_cleanup(
    store: FaultStore, phase: str,
) -> None:
    """First and repeated cancellation wait for guarded SQLite and runtime completion."""
    runtime = ControlledRuntime(store)
    service = service_for(runtime, store)
    if phase == "create_compensation":
        store.block_delete = True
        runtime.create_error = SandboxCreateFailed()
        task = asyncio.create_task(service.create())
    else:
        await service.create()
        runtime.block_delete = phase == "runtime_delete"
        store.block_delete = phase == "metadata_delete"
        task = asyncio.create_task(service.delete(FIXED_SANDBOX_ID))
    try:
        if phase == "runtime_delete":
            await runtime.delete_entered.wait()
        else:
            assert await asyncio.to_thread(store.delete_entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert await store.get(FIXED_SANDBOX_ID) is not None
        runtime.delete_release.set()
        store.delete_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await store.get(FIXED_SANDBOX_ID) is None
    finally:
        runtime.delete_release.set()
        store.delete_release.set()
