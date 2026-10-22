"""Pause/resume contracts, lifecycle races and cancellation without a Docker daemon."""

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest

import control_plane.core.errors as errors
import control_plane.core.service as service_module
from control_plane.app import create_app
from control_plane.core.models import SandboxLifecycleRecord, SandboxQueryResult
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import LifecycleStoreError, SQLiteLifecycleStore
from test_sandbox_service import FakeRuntime, FIXED_SANDBOX_ID as SID
from test_sandbox_service import FIXED_TRANSACTION_ID as TX


NOW = datetime(2026, 10, 22, 8, 0, 0, 123456, tzinfo=UTC)
OTHER_ID = "sbx_" + "f" * 32


class TransitionRuntime(FakeRuntime):
    """Represent observable runtime state and explicitly gated mutation completion."""

    def __init__(self) -> None:
        """Initialize independent instance facts, mutation gates and failure injection."""
        super().__init__()
        self.observations: dict[str, SandboxQueryResult] = {}
        self.transitions: list[tuple[str, str, str]] = []
        self.gates: dict[tuple[str, str], tuple[asyncio.Event, asyncio.Event]] = {}
        self.query_error: Exception | None = None
        self.mutation_error: Exception | None = None
        self.partial_failure = False
        self.cancelled = asyncio.Event()
        self.deleted_states: list[str] = []
        self.deletion_entered = asyncio.Event()
        self.deletion_release = asyncio.Event()
        self.deletion_release.set()

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read actual per-instance facts and preserve simulated runtime disappearance."""
        self.queries.append(sandbox_id)
        if self.query_error is not None:
            raise self.query_error
        if sandbox_id not in self.observations:
            raise errors.SandboxNotFound()
        return self.observations[sandbox_id]

    def block(self, operation: str, sandbox_id: str = SID) -> tuple[asyncio.Event, asyncio.Event]:
        """Install a gate that identifies Runtime entry separately from final mutation."""
        events = (asyncio.Event(), asyncio.Event())
        self.gates[operation, sandbox_id] = events
        return events

    async def pause(self, sandbox_id: str, *, expected_transaction_id: str) -> None:
        """Pause only through the shared transition implementation and supplied identity."""
        await self._transition("pause", sandbox_id, expected_transaction_id)

    async def resume(self, sandbox_id: str, *, expected_transaction_id: str) -> None:
        """Resume only through the shared transition implementation and supplied identity."""
        await self._transition("resume", sandbox_id, expected_transaction_id)

    async def _transition(self, operation: str, sandbox_id: str, transaction_id: str) -> None:
        """Settle a dispatched mutation after cancellation and expose its real final state."""
        self.transitions.append((operation, sandbox_id, transaction_id))
        cancellation = None
        if (operation, sandbox_id) in self.gates:
            entered, release = self.gates[operation, sandbox_id]
            entered.set()
            try:
                await release.wait()
            except asyncio.CancelledError as error:
                cancellation = error
                self.cancelled.set()
                while not release.is_set():
                    try:
                        await asyncio.shield(release.wait())
                    except asyncio.CancelledError:
                        continue
        if self.mutation_error is None or self.partial_failure:
            self.observations[sandbox_id] = replace(
                self.observations[sandbox_id],
                status="paused" if operation == "pause" else "running",
            )
        if self.mutation_error is not None:
            raise self.mutation_error
        if cancellation is not None:
            raise cancellation

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Hold cleanup after its metadata claim and then remove this exact observation."""
        self.deletions.append((sandbox_id, delete_workspace))
        self.deleted_states.append(self.observations[sandbox_id].status)
        assert expected_transaction_id == self.observations[sandbox_id].transaction_id
        self.deletion_entered.set()
        await self.deletion_release.wait()
        del self.observations[sandbox_id]


async def seed(
    store: SQLiteLifecycleStore, runtime: TransitionRuntime, *, sandbox_id: str = SID,
    state: str = "active", expires_at: datetime | None = None, status: str = "running",
) -> SandboxLifecycleRecord:
    """Persist real metadata and publish matching initial runtime identity facts."""
    record = SandboxLifecycleRecord(
        sandbox_id, TX, NOW - timedelta(minutes=30),
        expires_at if expires_at is not None else NOW + timedelta(minutes=30),
    )
    await store.create(record)
    if state != "pending":
        record = await store.activate(sandbox_id, TX, expected_version=record.version)
    if state == "deleting":
        record = await store.mark_deleting(sandbox_id, TX, expected_version=record.version)
    runtime.observations[sandbox_id] = replace(
        runtime.query_result, sandbox_id=sandbox_id, transaction_id=record.transaction_id,
        created_at=record.created_at, expires_at=record.expires_at, status=status,
    )
    return record


@pytest.fixture
async def setup_operations(tmp_path: Path):
    """Provide a real initialized temporary database and one fixed-clock Service."""
    store = SQLiteLifecycleStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    runtime = TransitionRuntime()
    service = SandboxService(runtime, store, clock=lambda: NOW)
    return store, runtime, service


@pytest.fixture
async def operation_app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Run HTTP requests through real Service wiring while disabling background reaping."""
    monkeypatch.setattr(service_module, "_utc_now", lambda: NOW)
    runtime = TransitionRuntime()
    application = create_app(
        runtime, database_path=tmp_path / "http.sqlite3", reaper_enabled=False,
    )
    async with application.router.lifespan_context(application):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://test",
        ) as client:
            yield application, client, runtime


@pytest.mark.anyio
@pytest.mark.parametrize("operation,initial,expected", [
    ("pause", "running", "paused"), ("pause", "paused", "paused"),
    ("resume", "paused", "running"), ("resume", "running", "running"),
])
async def test_operation_preserves_metadata_and_uses_stored_identity(
    setup_operations, operation: str, initial: str, expected: str,
) -> None:
    """Successful and idempotent operations keep TTL/version unchanged and pass transaction identity."""
    store, runtime, service = setup_operations
    record = await seed(store, runtime, status=initial)
    # A previously renewed authoritative deadline may differ from immutable initial labels.
    record = await store.update_expiration(
        SID, TX, expected_version=record.version, expires_at=NOW + timedelta(hours=1),
    )

    assert await getattr(service, operation)(SID) is None

    assert runtime.transitions == [(operation, SID, TX)]
    assert runtime.observations[SID].status == expected
    assert await store.get(SID) == record
    assert runtime.deletions == runtime.specs == []


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
@pytest.mark.parametrize("condition", ["pending", "deleting", "expired", "equal", "legacy"])
async def test_ineligible_metadata_never_reaches_mutation(
    setup_operations, operation: str, condition: str,
) -> None:
    """Only active managed instances with a strictly future deadline authorize transitions."""
    store, runtime, service = setup_operations
    delta = timedelta(microseconds=-1) if condition == "expired" else timedelta(0)
    record = await seed(
        store, runtime,
        state=condition if condition in ("pending", "deleting") else "active",
        expires_at=NOW + delta if condition in ("expired", "equal") else None,
    )
    if condition == "legacy":
        await store.delete(SID, TX, expected_version=record.version)
        record = None
    conflict = getattr(errors, f"Sandbox{operation.title()}Conflict")

    with pytest.raises(conflict):
        await getattr(service, operation)(SID)

    assert runtime.transitions == []
    assert await store.get(SID) == record
    assert service._instance_operations == {}


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
@pytest.mark.parametrize("field", ["sandbox_id", "transaction_id", "created_at", "legacy"])
async def test_identity_mismatch_never_reaches_mutation(
    setup_operations, operation: str, field: str,
) -> None:
    """Runtime identity must match sandbox, transaction and creation time before mutation."""
    store, runtime, service = setup_operations
    record = await seed(store, runtime)
    changes = {
        "sandbox_id": {"sandbox_id": OTHER_ID},
        "transaction_id": {"transaction_id": "txn_" + "f" * 32},
        "created_at": {"created_at": record.created_at - timedelta(microseconds=1)},
        "legacy": {"transaction_id": None, "created_at": None, "expires_at": None},
    }
    runtime.observations[SID] = replace(runtime.observations[SID], **changes[field])

    with pytest.raises(errors.SandboxMetadataConflict):
        await getattr(service, operation)(SID)

    assert runtime.transitions == []
    assert await store.get(SID) == record


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
async def test_missing_runtime_wins_over_unavailable_metadata(
    setup_operations, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    """An actually absent main container stays 404 without consulting unavailable metadata."""
    store, runtime, service = setup_operations
    await seed(store, runtime)
    runtime.observations.clear()
    unavailable = AsyncMock(side_effect=LifecycleStoreError())
    monkeypatch.setattr(store, "get", unavailable)

    with pytest.raises(errors.SandboxNotFound):
        await getattr(service, operation)(SID)

    unavailable.assert_not_awaited()
    assert runtime.transitions == []


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
async def test_store_failure_prevents_mutation(
    setup_operations, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    """Metadata read failures map to the stable 503 domain error without touching Runtime state."""
    store, runtime, service = setup_operations
    await seed(store, runtime)
    monkeypatch.setattr(store, "get", AsyncMock(side_effect=LifecycleStoreError()))

    with pytest.raises(errors.SandboxMetadataUnavailable):
        await getattr(service, operation)(SID)

    assert runtime.transitions == []
    assert service._instance_operations == {}


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
async def test_wrong_store_identity_is_rejected(
    setup_operations, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    """A broken store adapter returning a different sandbox cannot authorize any mutation."""
    store, runtime, service = setup_operations
    record = await seed(store, runtime)
    monkeypatch.setattr(store, "get", AsyncMock(return_value=replace(record, sandbox_id=OTHER_ID)))

    with pytest.raises(errors.SandboxMetadataConflict):
        await getattr(service, operation)(SID)

    assert runtime.transitions == []


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
async def test_api_success_is_empty_204(operation_app, operation: str) -> None:
    """POST action routes return no representation and retain authoritative lifecycle metadata."""
    application, client, runtime = operation_app
    store = application.state.lifecycle_store
    record = await seed(store, runtime, status="paused" if operation == "resume" else "running")

    response = await client.post(f"/v1/sandboxes/{SID}/{operation}")

    assert response.status_code == 204, response.text
    assert response.content == b""
    assert runtime.transitions == [(operation, SID, TX)]
    assert await store.get(SID) == record


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
@pytest.mark.parametrize("sandbox_id", ["invalid", "sbx_" + "A" * 32, "sbx_1234"])
async def test_api_invalid_id_is_404_without_dependencies(
    operation_app, monkeypatch: pytest.MonkeyPatch, operation: str, sandbox_id: str,
) -> None:
    """Malformed identifiers must fail before Runtime, metadata reads or coordination allocation."""
    application, client, runtime = operation_app
    reader = AsyncMock(side_effect=AssertionError("invalid ID reached storage"))
    monkeypatch.setattr(application.state.lifecycle_store, "get", reader)

    response = await client.post(f"/v1/sandboxes/{sandbox_id}/{operation}")

    assert response.status_code == 404
    assert response.json() == {"error": {"code": "sandbox_not_found", "message": "sandbox not found"}}
    reader.assert_not_awaited()
    assert runtime.queries == runtime.transitions == []
    assert application.state.service._instance_operations == {}


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
@pytest.mark.parametrize("kind", ["conflict", "failed", "unknown", "docker", "missing", "identity", "metadata"])
async def test_api_errors_have_fixed_status_code_and_sanitized_message(
    operation_app, monkeypatch: pytest.MonkeyPatch, operation: str, kind: str,
) -> None:
    """Operation and dependency failures expose stable public messages without exception details."""
    application, client, runtime = operation_app
    store = application.state.lifecycle_store
    await seed(store, runtime)
    status, code, message = {
        "conflict": (409, f"sandbox_{operation}_conflict", f"sandbox {operation} conflict"),
        "failed": (500, f"sandbox_{operation}_failed", f"sandbox {operation} failed"),
        "unknown": (500, f"sandbox_{operation}_failed", f"sandbox {operation} failed"),
        "docker": (503, "docker_unavailable", "Docker runtime is unavailable"),
        "missing": (404, "sandbox_not_found", "sandbox not found"),
        "identity": (409, "sandbox_metadata_conflict", "sandbox lifecycle metadata conflict"),
        "metadata": (503, "sandbox_metadata_unavailable", "sandbox lifecycle metadata is unavailable"),
    }[kind]
    if kind in ("conflict", "failed"):
        error = getattr(errors, f"Sandbox{operation.title()}{kind.title()}")()
        error.args = ("secret Docker socket credentials",)
        runtime.mutation_error = error
    elif kind == "unknown":
        runtime.mutation_error = RuntimeError("secret Docker socket credentials")
    elif kind == "docker":
        runtime.query_error = errors.DockerUnavailable()
    elif kind == "missing":
        runtime.observations.clear()
    elif kind == "identity":
        runtime.observations[SID] = replace(runtime.observations[SID], transaction_id="other")
    else:
        monkeypatch.setattr(store, "get", AsyncMock(side_effect=LifecycleStoreError()))

    response = await client.post(f"/v1/sandboxes/{SID}/{operation}")

    assert response.status_code == status, response.text
    assert response.json()["error"]["code"] == code
    assert response.json()["error"]["message"] == message
    assert "secret" not in response.text
    assert application.state.service._instance_operations == {}


@pytest.mark.anyio
async def test_paused_readiness_rejects_before_endpoint_and_probe(
    operation_app, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Paused query state rejects readyz before endpoint lookup or Agent HTTP traffic."""
    application, client, runtime = operation_app
    record = await seed(application.state.lifecycle_store, runtime, status="paused")
    endpoint = AsyncMock()
    probe = AsyncMock()
    monkeypatch.setattr(runtime, "get_endpoint", endpoint)
    application.state.service._agent_probe = probe

    queried = await client.get(f"/v1/sandboxes/{SID}")
    readiness = await client.get(f"/v1/sandboxes/{SID}/readyz")

    assert queried.status_code == 200
    assert queried.json()["status"] == "paused"
    assert readiness.status_code == 503
    assert readiness.json()["error"]["code"] == "sandbox_not_ready"
    endpoint.assert_not_awaited()
    probe.check.assert_not_awaited()
    assert await application.state.lifecycle_store.get(SID) == record


@pytest.mark.anyio
async def test_paused_instance_can_renew_without_resuming(operation_app) -> None:
    """Renew changes only authoritative expiration while paused runtime state stays observable."""
    application, client, runtime = operation_app
    record = await seed(application.state.lifecycle_store, runtime, status="paused")

    renewed = await client.post(f"/v1/sandboxes/{SID}/renew", json={"ttl_seconds": 3600})

    assert renewed.status_code == 200, renewed.text
    assert await application.state.lifecycle_store.get(SID) == replace(
        record, expires_at=NOW + timedelta(hours=1), version=record.version + 1,
    )
    assert runtime.observations[SID].status == "paused"
    assert runtime.transitions == []


@pytest.mark.anyio
@pytest.mark.parametrize("cleanup", ["manual", "reap"])
@pytest.mark.parametrize("operation", ["pause", "resume"])
async def test_cleanup_claim_blocks_waiting_transition(
    setup_operations, operation: str, cleanup: str,
) -> None:
    """Deletion owns each instance from claim to final cleanup so no waiting transition can revive it."""
    store, runtime, service = setup_operations
    await seed(store, runtime, expires_at=NOW if cleanup == "reap" else None, status="paused")
    runtime.deletion_release.clear()
    deletion = asyncio.create_task(service.delete(SID) if cleanup == "manual" else service.reap_expired_once())
    transition = None
    try:
        await asyncio.wait_for(runtime.deletion_entered.wait(), 2)
        assert (await store.get(SID)).management_state == "deleting"
        transition = asyncio.create_task(getattr(service, operation)(SID))
        await asyncio.sleep(0)
        assert runtime.transitions == []
        assert not transition.done()
        runtime.deletion_release.set()
        await deletion
        with pytest.raises(errors.SandboxNotFound):
            await transition
        assert runtime.transitions == []
        assert await store.get(SID) is None
        assert service._instance_operations == {}
    finally:
        runtime.deletion_release.set()
        await asyncio.gather(*([deletion, transition] if transition is not None else [deletion]), return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("cleanup", ["manual", "reap"])
async def test_transition_finishes_before_cleanup_claim(setup_operations, cleanup: str) -> None:
    """An in-flight Runtime mutation retains ownership before manual deletion or an expired claim."""
    store, runtime, service = setup_operations
    record = await seed(store, runtime)
    entered, release = runtime.block("pause")
    pause = asyncio.create_task(service.pause(SID))
    deletion = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        # Advance the reaper's clock after pause eligibility was checked.
        if cleanup == "reap":
            service._clock = lambda: record.expires_at
        deletion = asyncio.create_task(service.delete(SID) if cleanup == "manual" else service.reap_expired_once())
        await asyncio.sleep(0)
        assert (await store.get(SID)).management_state == "active"
        assert runtime.deletions == []
        release.set()
        await pause
        await deletion
        assert await store.get(SID) is None
        assert runtime.transitions == [("pause", SID, TX)]
        assert runtime.deletions == [(SID, False)]
        assert service._instance_operations == {}
    finally:
        release.set()
        await asyncio.gather(*([pause, deletion] if deletion is not None else [pause]), return_exceptions=True)


@pytest.mark.anyio
async def test_same_instance_serializes_but_other_instances_proceed(setup_operations) -> None:
    """Pause and resume serialize for one ID while another ID completes and lock entries are reclaimed."""
    store, runtime, service = setup_operations
    await seed(store, runtime)
    await seed(store, runtime, sandbox_id=OTHER_ID)
    entered, release = runtime.block("pause")
    first = asyncio.create_task(service.pause(SID))
    second = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        second = asyncio.create_task(service.resume(SID))
        await asyncio.sleep(0)
        assert not second.done()
        await asyncio.wait_for(service.pause(OTHER_ID), 2)
        assert runtime.observations[OTHER_ID].status == "paused"
        assert runtime.transitions == [("pause", SID, TX), ("pause", OTHER_ID, TX)]
        release.set()
        await asyncio.gather(first, second)
        assert runtime.observations[SID].status == "running"
        assert service._instance_operations == {}
    finally:
        release.set()
        await asyncio.gather(*([first, second] if second is not None else [first]), return_exceptions=True)


@pytest.mark.anyio
async def test_cancelled_waiter_does_not_cancel_current_mutation(setup_operations) -> None:
    """Cancelling a queued resume must leave the current pause untouched and reclaim only its reference."""
    store, runtime, service = setup_operations
    await seed(store, runtime)
    entered, release = runtime.block("pause")
    pause = asyncio.create_task(service.pause(SID))
    waiter = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        waiter = asyncio.create_task(service.resume(SID))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert not pause.done()
        assert not runtime.cancelled.is_set()
        assert runtime.transitions == [("pause", SID, TX)]
        release.set()
        await pause
        assert runtime.observations[SID].status == "paused"
        assert service._instance_operations == {}
    finally:
        release.set()
        await asyncio.gather(*([pause, waiter] if waiter is not None else [pause]), return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
@pytest.mark.parametrize("failure", [False, True])
async def test_repeated_cancellation_keeps_lock_until_runtime_settles(
    setup_operations, operation: str, failure: bool,
) -> None:
    """Caller cancellation reaches Runtime but repeated cancellation cannot expose unsettled resources."""
    store, runtime, service = setup_operations
    record = await seed(store, runtime, status="paused" if operation == "resume" else "running")
    entered, release = runtime.block(operation)
    if failure:
        runtime.mutation_error = getattr(errors, f"Sandbox{operation.title()}Failed")()
        runtime.partial_failure = True
    mutation = asyncio.create_task(getattr(service, operation)(SID))
    deletion = None
    try:
        await asyncio.wait_for(entered.wait(), 2)
        mutation.cancel()
        await asyncio.wait_for(runtime.cancelled.wait(), 2)
        mutation.cancel()
        await asyncio.sleep(0)
        assert not mutation.done()
        deletion = asyncio.create_task(service.delete(SID))
        await asyncio.sleep(0)
        assert runtime.deletions == []
        assert (await store.get(SID)) == record
        release.set()
        outcome = (await asyncio.gather(mutation, return_exceptions=True))[0]
        expected = getattr(errors, f"Sandbox{operation.title()}Failed") if failure else asyncio.CancelledError
        assert isinstance(outcome, expected)
        await deletion
        assert runtime.transitions == [(operation, SID, TX)]
        assert runtime.deletions == [(SID, False)]
        assert runtime.deleted_states == ["paused" if operation == "pause" else "running"]
        assert await store.get(SID) is None
        assert service._instance_operations == {}
    finally:
        release.set()
        await asyncio.gather(*([mutation, deletion] if deletion is not None else [mutation]), return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
async def test_partial_runtime_failure_preserves_actual_state_and_metadata(
    setup_operations, operation: str,
) -> None:
    """A partially applied failure must not compensate runtime state or rewrite TTL metadata."""
    store, runtime, service = setup_operations
    record = await seed(store, runtime, status="paused" if operation == "resume" else "running")
    failure = getattr(errors, f"Sandbox{operation.title()}Failed")
    runtime.mutation_error = failure()
    runtime.partial_failure = True

    with pytest.raises(failure):
        await getattr(service, operation)(SID)

    assert runtime.observations[SID].status == ("paused" if operation == "pause" else "running")
    assert runtime.transitions == [(operation, SID, TX)]
    assert runtime.deletions == []
    assert await store.get(SID) == record
    assert service._instance_operations == {}


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
async def test_cancelled_late_success_keeps_actual_state_without_compensation(
    setup_operations, operation: str,
) -> None:
    """A mutation completed after caller cancellation remains visible with the original TTL and version."""
    store, runtime, service = setup_operations
    record = await seed(store, runtime, status="paused" if operation == "resume" else "running")
    entered, release = runtime.block(operation)
    mutation = asyncio.create_task(getattr(service, operation)(SID))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        mutation.cancel()
        await asyncio.wait_for(runtime.cancelled.wait(), 2)
        mutation.cancel()
        await asyncio.sleep(0)
        assert not mutation.done()
        assert SID in service._instance_operations
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await mutation
        assert runtime.observations[SID].status == ("paused" if operation == "pause" else "running")
        assert runtime.transitions == [(operation, SID, TX)]
        assert runtime.deletions == []
        assert await store.get(SID) == record
        assert service._instance_operations == {}
    finally:
        release.set()
        await asyncio.gather(mutation, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
@pytest.mark.parametrize("phase", ["runtime", "metadata"])
async def test_cancelled_read_settles_before_releasing_instance_lock(
    setup_operations, monkeypatch: pytest.MonkeyPatch, operation: str, phase: str,
) -> None:
    """Cancelling before mutation waits for an already-started read and prevents later mutation."""
    store, runtime, service = setup_operations
    record = await seed(store, runtime)
    entered, release = asyncio.Event(), asyncio.Event()
    read_cancelled = asyncio.Event()

    async def blocked_read(sandbox_id: str):
        """Keep the selected read open and report any attempt to abandon its worker."""
        assert sandbox_id == SID
        entered.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            read_cancelled.set()
            raise
        return runtime.observations[SID] if phase == "runtime" else record

    monkeypatch.setattr(runtime if phase == "runtime" else store, "get", blocked_read)
    mutation = asyncio.create_task(getattr(service, operation)(SID))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        mutation.cancel()
        await asyncio.sleep(0)
        mutation.cancel()
        await asyncio.sleep(0)
        assert not mutation.done()
        assert SID in service._instance_operations
        assert not read_cancelled.is_set()
        assert runtime.transitions == []
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await mutation
        assert runtime.transitions == []
        assert runtime.deletions == []
        assert service._instance_operations == {}
        assert await SQLiteLifecycleStore(store.database_path).get(SID) == record
    finally:
        release.set()
        await asyncio.gather(mutation, return_exceptions=True)


@pytest.mark.anyio
async def test_instance_lock_records_do_not_accumulate_with_distinct_ids(setup_operations) -> None:
    """Completed operations on many unique instances must leave no historical lock records."""
    store, runtime, service = setup_operations
    for value in range(8):
        sandbox_id = f"sbx_{value:032x}"
        await seed(store, runtime, sandbox_id=sandbox_id)
        await service.pause(sandbox_id)
        await service.resume(sandbox_id)
        assert service._instance_operations == {}
    assert len(runtime.transitions) == 16


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
async def test_creation_rollback_waits_for_active_instance_mutation(
    setup_operations, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    """Creation cancellation compensation must acquire the instance lock before claiming deletion."""
    store, runtime, service = setup_operations
    record = await seed(store, runtime, status="paused" if operation == "resume" else "running")
    mutation_entered, mutation_release = runtime.block(operation)
    rollback_entered = asyncio.Event()
    original_finish = service._finish_creation_rollback
    mark_deleting = AsyncMock(wraps=store.mark_deleting)

    async def observed_rollback(candidate: SandboxLifecycleRecord) -> None:
        """Identify rollback dispatch before entering its actual coordination and cleanup logic."""
        rollback_entered.set()
        await original_finish(candidate)

    monkeypatch.setattr(service, "_finish_creation_rollback", observed_rollback)
    monkeypatch.setattr(store, "mark_deleting", mark_deleting)
    runtime.deletion_release.clear()
    mutation = asyncio.create_task(getattr(service, operation)(SID))
    rollback = None
    try:
        await asyncio.wait_for(mutation_entered.wait(), 2)
        rollback = asyncio.create_task(service._rollback_created(record))
        await asyncio.wait_for(rollback_entered.wait(), 2)
        mark_deleting.assert_not_awaited()
        assert await store.get(SID) == record
        assert not runtime.deletion_entered.is_set()
        assert runtime.deletions == []
        mutation_release.set()
        await mutation
        await asyncio.wait_for(runtime.deletion_entered.wait(), 2)
        mark_deleting.assert_awaited_once_with(SID, TX, expected_version=record.version)
        assert (await store.get(SID)).management_state == "deleting"
        runtime.deletion_release.set()
        await rollback
        assert runtime.deleted_states == ["paused" if operation == "pause" else "running"]
        assert runtime.deletions == [(SID, True)]
        assert await store.get(SID) is None
        assert service._instance_operations == {}
    finally:
        mutation_release.set()
        runtime.deletion_release.set()
        await asyncio.gather(
            *([mutation, rollback] if rollback is not None else [mutation]), return_exceptions=True,
        )


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
async def test_runtime_only_creation_compensation_waits_for_instance_mutation(
    setup_operations, monkeypatch: pytest.MonkeyPatch, operation: str,
) -> None:
    """Resource-only compensation shares freeze/thaw coordination while retaining concurrent metadata."""
    store, runtime, service = setup_operations
    record = await seed(store, runtime, status="paused" if operation == "resume" else "running")
    mutation_entered, mutation_release = runtime.block(operation)
    cleanup_entered = asyncio.Event()
    original_cleanup = service._cleanup_created_runtime

    async def observed_cleanup(candidate: SandboxLifecycleRecord) -> None:
        """Signal compensation dispatch before entering its resource-only cleanup implementation."""
        cleanup_entered.set()
        await original_cleanup(candidate)

    monkeypatch.setattr(service, "_cleanup_created_runtime", observed_cleanup)
    runtime.deletion_release.clear()
    mutation = asyncio.create_task(getattr(service, operation)(SID))
    cleanup = None
    try:
        await asyncio.wait_for(mutation_entered.wait(), 2)
        cleanup = asyncio.create_task(service._cleanup_created_runtime(record))
        await asyncio.wait_for(cleanup_entered.wait(), 2)
        await asyncio.sleep(0)
        assert not runtime.deletion_entered.is_set()
        assert runtime.deletions == []
        assert await store.get(SID) == record
        mutation_release.set()
        await mutation
        await asyncio.wait_for(runtime.deletion_entered.wait(), 2)
        runtime.deletion_release.set()
        await cleanup
        assert runtime.deleted_states == ["paused" if operation == "pause" else "running"]
        assert runtime.deletions == [(SID, True)]
        assert await store.get(SID) == record
        assert service._instance_operations == {}
    finally:
        mutation_release.set()
        runtime.deletion_release.set()
        await asyncio.gather(
            *([mutation, cleanup] if cleanup is not None else [mutation]), return_exceptions=True,
        )
