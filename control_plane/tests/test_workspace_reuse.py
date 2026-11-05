"""Persistent exclusive workspace reuse and Service compensation without Docker."""

from dataclasses import FrozenInstanceError

import pytest

from control_plane.core import models


@pytest.mark.parametrize('value', ['wsp_' + 'a' * 32, None])
def test_creation_options_support_an_explicit_workspace_identity(value):
    """Domain options preserve a validated optional workspace identity without Docker names."""
    options = models.SandboxCreateOptions(workspace_id=value)
    assert options.workspace_id == value
    with pytest.raises(FrozenInstanceError):
        options.workspace_id = 'wsp_' + 'b' * 32


@pytest.mark.parametrize('value', ['', 'volume-name', 'wsp_' + 'A' * 32, 1, True, [], 'wsp_' + 'a' * 33])
def test_creation_options_reject_invalid_workspace_identities(value):
    """Non-HTTP callers cannot bypass the bounded opaque workspace identifier format."""
    with pytest.raises(ValueError):
        models.SandboxCreateOptions(workspace_id=value)

import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from control_plane.core.errors import (
    SandboxCleanupFailed, SandboxCreateFailed, SandboxCreateUncertain,
    SandboxDeleteFailed, SandboxMetadataCleanupFailed, SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    WorkspaceConflict, WorkspaceDeleteProtected,
    WorkspaceNotFound, WorkspaceOperationFailed,
)
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore, LifecycleStoreError
from test_sandbox_service import FakeRuntime

NOW = datetime(2026, 11, 3, 8, tzinfo=UTC)
IMAGE = 'sha256:' + 'a' * 64


class WorkspaceRuntime(FakeRuntime):
    """Keep independent volume data separate from fake sandbox resources and controlled waits."""

    def __init__(self):
        """Set isolated resource maps and deterministic cancellation/deletion gates."""
        super().__init__()
        self.image_id = IMAGE
        self.workspaces = {}
        self.contents = {}
        self.instances = {}
        self.create_error = None
        self.delete_error = None
        self.block_create = False
        self.late_success = False
        self.block_delete = False
        self.create_entered = asyncio.Event()
        self.create_release = asyncio.Event()
        self.cleanup_entered = asyncio.Event()
        self.cleanup_release = asyncio.Event()
        self.delete_entered = asyncio.Event()
        self.delete_release = asyncio.Event()
        self.workspace_calls = []
        self.statuses = {}
        self.query_overrides = {}
        self.inspections = []

    async def resolve_environment_image(self, environment):
        """Resolve a fixed test image without any network or Docker access."""
        self.workspace_calls.append(('resolve', environment))
        return self.image_id

    async def create_workspace(self, record):
        """Create an independent data object once, without any sandbox resources."""
        self.workspaces[record.workspace_id] = record
        self.contents[record.workspace_id] = {}

    async def inspect_workspace(self, record):
        """Check independent identity and every existing fake container reference."""
        assert self.workspaces[record.workspace_id].creation_token == record.creation_token
        for spec in self.instances.values():
            if spec.workspace and spec.workspace.workspace_id == record.workspace_id:
                if spec.sandbox_id != record.sandbox_id or spec.transaction_id != record.transaction_id:
                    raise WorkspaceConflict(record.workspace_id)

    async def delete_workspace(self, record):
        """Reject live references and remove only the independent workspace object."""
        await self.inspect_workspace(record)
        if any(spec.workspace and spec.workspace.workspace_id == record.workspace_id for spec in self.instances.values()):
            raise WorkspaceConflict(record.workspace_id)
        self.workspaces.pop(record.workspace_id, None)
        self.contents.pop(record.workspace_id, None)

    async def create(self, spec):
        """Expose a deterministic pending interval and model confirmed versus late cancellation."""
        self.specs.append(spec)
        self.create_entered.set()
        if self.block_create:
            try:
                await self.create_release.wait()
            except asyncio.CancelledError:
                if self.late_success:
                    await self.create_release.wait()
                else:
                    self.cleanup_entered.set()
                    await self.cleanup_release.wait()
                    raise
        if self.create_error:
            raise self.create_error
        self.instances[spec.sandbox_id] = spec

    async def get(self, sandbox_id):
        """Return original identity and workspace while preserving original TTL."""
        from control_plane.core.models import SandboxQueryResult
        from control_plane.core.errors import SandboxNotFound
        if sandbox_id not in self.instances:
            raise SandboxNotFound()
        spec = self.instances[sandbox_id]
        result = SandboxQueryResult(
            sandbox_id, self.statuses.get(sandbox_id, 'ready'), 'health_check_passed', 'Agent is healthy',
            spec.created_at, spec.expires_at, spec.transaction_id,
            workspace_id=spec.workspace.workspace_id if spec.workspace else None,
            workspace=spec.workspace,
        )
        return replace(result, **self.query_overrides)

    async def inspect_resources(self, sandbox_id, *, expected_transaction_id, expected_workspace=None):
        """Verify the supplied occupancy before reporting complete fake resources."""
        from control_plane.core.models import SandboxResourceInspection
        self.inspections.append((sandbox_id, expected_transaction_id, expected_workspace))
        spec = self.instances.get(sandbox_id)
        if spec and spec.workspace != expected_workspace:
            return SandboxResourceInspection(sandbox_id, 'conflict', 'workspace_binding_conflict')
        return SandboxResourceInspection(sandbox_id, 'all_present' if spec else 'not_found', 'checked')

    async def delete(self, sandbox_id, *, delete_workspace=False, expected_transaction_id=None, expected_workspace=None):
        """Settle deletion without removing independent workspace data or losing identity guards."""
        self.deletions.append((sandbox_id, delete_workspace))
        if expected_workspace is not None:
            assert not delete_workspace
            assert expected_workspace.sandbox_id == sandbox_id
            assert expected_workspace.transaction_id == expected_transaction_id
        self.delete_entered.set()
        if self.block_delete:
            await self.delete_release.wait()
        if self.delete_error:
            raise self.delete_error
        self.instances.pop(sandbox_id, None)

    async def pause(self, sandbox_id, *, expected_transaction_id, expected_workspace=None):
        """Record a freeze without changing the stored identity, data or occupancy."""
        self.transitions.append(('pause', sandbox_id, expected_transaction_id))
        self.statuses[sandbox_id] = 'paused'

    async def resume(self, sandbox_id, *, expected_transaction_id, expected_workspace=None):
        """Record thawing the original identity without changing workspace contents."""
        self.transitions.append(('resume', sandbox_id, expected_transaction_id))
        self.statuses[sandbox_id] = 'running'


@pytest.fixture
async def workspace_service(tmp_path):
    """Use a real temporary file database and an event-controlled backend for each test."""
    store = SQLiteLifecycleStore(tmp_path / 'lifecycle.sqlite3')
    await store.initialize()
    runtime = WorkspaceRuntime()
    service = SandboxService(runtime, store, clock=lambda: NOW)
    return service, store, runtime


@pytest.mark.anyio
async def test_workspace_data_survives_sequential_instances_and_service_restart(workspace_service):
    """A and B use the same independent data object after durable, guarded ownership release."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    first = await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    assert first.workspace_id == workspace.workspace_id
    runtime.contents[workspace.workspace_id]['original'] = b'first\x00\xff'
    with pytest.raises(WorkspaceConflict):
        await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    assert len(runtime.specs) == 1
    await service.delete(first.sandbox_id)
    assert runtime.contents[workspace.workspace_id]['original'] == b'first\x00\xff'
    reopened = SQLiteLifecycleStore(store.database_path)
    restarted = SandboxService(runtime, reopened, clock=lambda: NOW)
    second = await restarted.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    assert second.sandbox_id != first.sandbox_id
    runtime.contents[workspace.workspace_id]['original'] += b'-second'
    observed = await restarted.get(second.sandbox_id)
    assert observed.workspace_id == workspace.workspace_id
    assert (await reopened.get_workspace(workspace.workspace_id)).sandbox_id == second.sandbox_id
    with pytest.raises(WorkspaceConflict):
        await restarted.delete_workspace(workspace.workspace_id)
    await restarted.delete(second.sandbox_id)
    await restarted.delete_workspace(workspace.workspace_id)
    await restarted.delete_workspace(workspace.workspace_id)
    assert await reopened.get_workspace(workspace.workspace_id) is None


@pytest.mark.anyio
async def test_workspace_delete_option_rejects_before_claim_or_runtime(workspace_service):
    """delete_workspace=true cannot destroy an externally owned volume or change instance state."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    created = await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    before = await store.get(created.sandbox_id)
    with pytest.raises(WorkspaceDeleteProtected):
        await service.delete(created.sandbox_id, delete_workspace=True)
    assert await store.get(created.sandbox_id) == before
    assert not runtime.deletions


@pytest.mark.anyio
async def test_workspace_image_change_and_unknown_workspace_reject_before_prewrite(workspace_service):
    """Changing a selected image tag cannot reuse a volume pinned to the old image content."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    runtime.image_id = 'sha256:' + 'b' * 64
    with pytest.raises(WorkspaceConflict):
        await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    with pytest.raises(WorkspaceNotFound):
        await service.create(models.SandboxCreateOptions(workspace_id='wsp_' + 'f' * 32))
    assert not runtime.specs and not await store.list_pending()


@pytest.mark.anyio
@pytest.mark.parametrize('uncertain', [False, True])
async def test_creation_failure_preserves_data_and_releases_only_confirmed_cleanup(workspace_service, uncertain):
    """Only a confirmed absence/rollback can release occupancy; ambiguous outcomes retain it."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    runtime.contents[workspace.workspace_id]['kept'] = b'unchanged'
    runtime.create_error = SandboxCleanupFailed('sbx_' + '1' * 32) if uncertain else SandboxCreateFailed()
    with pytest.raises(SandboxCleanupFailed if uncertain else SandboxCreateFailed):
        await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    persisted = await store.get_workspace(workspace.workspace_id)
    assert (persisted.sandbox_id is not None) == uncertain
    assert runtime.contents[workspace.workspace_id] == {'kept': b'unchanged'}
    assert len(await store.list_pending()) == int(uncertain)


@pytest.mark.anyio
@pytest.mark.parametrize('late_success', [False, True])
async def test_cancel_keeps_occupancy_until_runtime_and_compensation_finish(workspace_service, late_success):
    """Repeated caller cancellation cannot make the workspace available during cleanup."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    runtime.block_create = True
    runtime.late_success = late_success
    runtime.block_delete = late_success
    task = asyncio.create_task(service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id)))
    await asyncio.wait_for(runtime.create_entered.wait(), 3)
    task.cancel()
    if late_success:
        runtime.create_release.set()
        await asyncio.wait_for(runtime.delete_entered.wait(), 3)
    else:
        await asyncio.wait_for(runtime.cleanup_entered.wait(), 3)
    task.cancel()
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is not None
    with pytest.raises(WorkspaceConflict):
        await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    runtime.delete_release.set()
    runtime.cleanup_release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 3)
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is None
    assert workspace.workspace_id in runtime.workspaces


@pytest.mark.anyio
async def test_manual_delete_waits_for_inflight_create_before_releasing_occupancy(workspace_service):
    """A known pending ID cannot be deleted into a free workspace while create may still mount it."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    runtime.block_create = True
    task = asyncio.create_task(service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id)))
    await asyncio.wait_for(runtime.create_entered.wait(), 3)
    pending = (await store.list_pending())[0]
    deleting = asyncio.create_task(service.delete(pending.sandbox_id))
    await asyncio.sleep(0)
    assert not runtime.delete_entered.is_set()
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id == pending.sandbox_id
    runtime.create_release.set()
    await asyncio.wait_for(task, 3)
    await asyncio.wait_for(deleting, 3)
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is None


class GatedWorkspaceStore(SQLiteLifecycleStore):
    """Expose acknowledgement boundaries around real SQLite commits without worker sleeps."""

    def __init__(self, database_path):
        """Initialize independent prewrite, activation and metadata-cleanup failure controls."""
        super().__init__(database_path)
        self.block_prewrite = False
        self.block_prewrite_ack = False
        self.block_activation = False
        self.block_activation_ack = False
        self.fail_activation_ack = False
        self.fail_delete = False
        self.prewrite_entered = asyncio.Event()
        self.prewrite_release = asyncio.Event()
        self.prewrite_committed = asyncio.Event()
        self.prewrite_ack_release = asyncio.Event()
        self.activation_entered = asyncio.Event()
        self.activation_release = asyncio.Event()
        self.activation_committed = asyncio.Event()
        self.activation_ack_release = asyncio.Event()

    async def create_with_workspace(self, record, binding, *, expected_workspace_version, credential=None, max_instances=10):
        """Hold before and after the actual atomic occupancy/pending transaction commits."""
        self.prewrite_entered.set()
        if self.block_prewrite:
            await self.prewrite_release.wait()
        await super().create_with_workspace(
            record, binding, expected_workspace_version=expected_workspace_version, credential=credential, max_instances=max_instances,
        )
        self.prewrite_committed.set()
        if self.block_prewrite_ack:
            await self.prewrite_ack_release.wait()

    async def activate(self, sandbox_id, transaction_id, *, expected_version):
        """Commit real activation before optionally withholding or failing its acknowledgement."""
        self.activation_entered.set()
        if self.block_activation:
            await self.activation_release.wait()
        result = await super().activate(sandbox_id, transaction_id, expected_version=expected_version)
        self.activation_committed.set()
        if self.block_activation_ack:
            await self.activation_ack_release.wait()
        if self.fail_activation_ack:
            raise LifecycleStoreError()
        return result

    async def delete(self, sandbox_id, transaction_id, *, expected_version):
        """Fail before guarded metadata removal to leave a durable deletion retry."""
        if self.fail_delete:
            raise LifecycleStoreError()
        await super().delete(sandbox_id, transaction_id, expected_version=expected_version)


@pytest.fixture
async def gated_workspace_service(tmp_path):
    """Provide real temporary SQLite commits with asynchronous boundary gates."""
    store = GatedWorkspaceStore(tmp_path / 'gated-lifecycle.sqlite3')
    await store.initialize()
    runtime = WorkspaceRuntime()
    service = SandboxService(runtime, store, clock=lambda: NOW)
    return service, store, runtime


@pytest.mark.anyio
async def test_cancelled_prewrite_settles_atomic_occupancy_commit_before_release(gated_workspace_service):
    """Cancellation before a late commit cannot abandon either its pending row or occupancy."""
    service, store, runtime = gated_workspace_service
    workspace = await service.create_workspace()
    runtime.contents[workspace.workspace_id]['kept'] = b'prewrite-data'
    store.block_prewrite = store.block_prewrite_ack = True
    task = asyncio.create_task(service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id)))
    try:
        await asyncio.wait_for(store.prewrite_entered.wait(), 3)
        task.cancel()
        assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is None
        assert not await store.list_pending()
        store.prewrite_release.set()
        await asyncio.wait_for(store.prewrite_committed.wait(), 3)
        pending = (await store.list_pending())[0]
        occupied = await store.get_workspace(workspace.workspace_id)
        assert occupied.sandbox_id == pending.sandbox_id
        assert occupied.transaction_id == pending.transaction_id
        task.cancel()
        with pytest.raises(WorkspaceConflict):
            await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
        assert not task.done() and not runtime.specs
    finally:
        store.prewrite_release.set()
        store.prewrite_ack_release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 3)
    assert not await store.list_pending()
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is None
    assert runtime.contents[workspace.workspace_id] == {'kept': b'prewrite-data'}
    assert not runtime.specs and not runtime.deletions


@pytest.mark.anyio
async def test_cancelled_activation_waits_for_late_commit_and_runtime_cleanup(gated_workspace_service):
    """A late activation commit retains occupancy through repeated cancellation and compensation."""
    service, store, runtime = gated_workspace_service
    workspace = await service.create_workspace()
    runtime.contents[workspace.workspace_id]['kept'] = b'activation-data'
    store.block_activation = store.block_activation_ack = True
    runtime.block_delete = True
    task = asyncio.create_task(service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id)))
    try:
        await asyncio.wait_for(store.activation_entered.wait(), 3)
        pending = (await store.list_pending())[0]
        task.cancel()
        store.activation_release.set()
        await asyncio.wait_for(store.activation_committed.wait(), 3)
        assert (await store.get(pending.sandbox_id)).management_state == 'active'
        task.cancel()
        assert (await store.get_workspace(workspace.workspace_id)).sandbox_id == pending.sandbox_id
        store.activation_ack_release.set()
        await asyncio.wait_for(runtime.delete_entered.wait(), 3)
        task.cancel()
        with pytest.raises(WorkspaceConflict):
            await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
        assert not task.done()
        assert pending.sandbox_id in runtime.instances
    finally:
        store.activation_release.set()
        store.activation_ack_release.set()
        runtime.delete_release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, 3)
    assert await store.get(pending.sandbox_id) is None
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is None
    assert pending.sandbox_id not in runtime.instances
    assert runtime.contents[workspace.workspace_id] == {'kept': b'activation-data'}


@pytest.mark.anyio
@pytest.mark.parametrize('cancel_creation', [False, True])
async def test_activation_postcommit_error_preserves_durable_holder_and_data(gated_workspace_service, cancel_creation):
    """An unknown activation acknowledgement never guesses a rollback version or frees live data."""
    service, store, runtime = gated_workspace_service
    workspace = await service.create_workspace()
    runtime.contents[workspace.workspace_id]['kept'] = b'uncertain-commit'
    store.fail_activation_ack = True
    store.block_activation_ack = True
    task = asyncio.create_task(service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id)))
    try:
        await asyncio.wait_for(store.activation_committed.wait(), 3)
        if cancel_creation:
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
    finally:
        store.activation_ack_release.set()
    with pytest.raises(SandboxMetadataUnavailable):
        await asyncio.wait_for(task, 3)
    occupied = await store.get_workspace(workspace.workspace_id)
    committed = await store.get(occupied.sandbox_id)
    assert committed.management_state == 'active' and committed.version == 1
    assert occupied.sandbox_id in runtime.instances
    assert occupied.transaction_id == committed.transaction_id
    assert not runtime.deletions
    assert runtime.contents[workspace.workspace_id] == {'kept': b'uncertain-commit'}
    with pytest.raises(WorkspaceConflict):
        await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))


@pytest.mark.anyio
@pytest.mark.parametrize('failure_stage', ['runtime', 'metadata'])
async def test_delete_failure_retains_occupancy_until_retry_finishes(gated_workspace_service, failure_stage):
    """Runtime cleanup failure and missing metadata acknowledgement both retain a guarded retry."""
    service, store, runtime = gated_workspace_service
    workspace = await service.create_workspace()
    created = await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    runtime.contents[workspace.workspace_id]['kept'] = b'retry-data'
    if failure_stage == 'runtime':
        runtime.delete_error = SandboxDeleteFailed(created.sandbox_id)
        expected_error = SandboxDeleteFailed
    else:
        store.fail_delete = True
        expected_error = SandboxMetadataCleanupFailed
    with pytest.raises(expected_error):
        await service.delete(created.sandbox_id)
    deleting = await store.get(created.sandbox_id)
    assert deleting.management_state == 'deleting'
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id == created.sandbox_id
    assert (created.sandbox_id in runtime.instances) == (failure_stage == 'runtime')
    with pytest.raises(WorkspaceConflict):
        await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    reopened = SQLiteLifecycleStore(store.database_path)
    restarted = SandboxService(runtime, reopened, clock=lambda: NOW)
    runtime.delete_error = None
    await restarted.delete(created.sandbox_id)
    assert await reopened.get(created.sandbox_id) is None
    assert (await reopened.get_workspace(workspace.workspace_id)).sandbox_id is None
    assert runtime.contents[workspace.workspace_id] == {'kept': b'retry-data'}
    next_instance = await restarted.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    assert (await reopened.get_workspace(workspace.workspace_id)).sandbox_id == next_instance.sandbox_id


@pytest.mark.anyio
@pytest.mark.parametrize('status', ['paused', 'stopped'])
async def test_renewed_paused_or_stopped_instance_reaps_without_deleting_workspace(workspace_service, status):
    """Runtime state never releases occupancy early, and TTL cleanup preserves independent data."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    created = await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id, ttl_seconds=10))
    runtime.contents[workspace.workspace_id]['kept'] = b'ttl-data'
    if status == 'paused':
        await service.pause(created.sandbox_id)
    else:
        runtime.statuses[created.sandbox_id] = status
    initial_binding = await store.get_workspace_binding(created.sandbox_id, runtime.instances[created.sandbox_id].transaction_id)
    later = SandboxService(runtime, store, clock=lambda: NOW + timedelta(seconds=5))
    renewed = await later.renew(created.sandbox_id, 30)
    assert renewed.expires_at == NOW + timedelta(seconds=35)
    assert await store.get_workspace_binding(created.sandbox_id, initial_binding.transaction_id) == initial_binding
    assert (await later.get(created.sandbox_id)).status == status
    assert (await later.get(created.sandbox_id)).expires_at == renewed.expires_at
    with pytest.raises(WorkspaceConflict):
        await later.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    before_expiry = SandboxService(runtime, store, clock=lambda: NOW + timedelta(seconds=10))
    assert not (await before_expiry.reap_expired_once()).items
    expired = SandboxService(runtime, store, clock=lambda: NOW + timedelta(seconds=35))
    reaped = await expired.reap_expired_once()
    assert [(item.sandbox_id, item.outcome) for item in reaped.items] == [(created.sandbox_id, 'succeeded')]
    assert await store.get(created.sandbox_id) is None
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is None
    assert runtime.contents[workspace.workspace_id] == {'kept': b'ttl-data'}
    assert runtime.deletions == [(created.sandbox_id, False)]


async def _persist_pending_workspace_instance(store, runtime, workspace):
    """Model a crash after resource creation using original pending metadata and occupancy."""
    record = models.SandboxLifecycleRecord(
        'sbx_' + 'c' * 32, 'txn_' + 'c' * 32, NOW, NOW + timedelta(seconds=60),
        workspace_id=workspace.workspace_id,
    )
    binding = models.WorkspaceBinding(
        workspace.workspace_id, workspace.creation_token, workspace.volume_name,
        workspace.image_id, workspace.version + 1, record.sandbox_id, record.transaction_id,
    )
    await store.create_with_workspace(record, binding, expected_workspace_version=workspace.version)
    runtime.instances[record.sandbox_id] = models.SandboxSpec(
        record.sandbox_id, f'agent-sandbox-{record.sandbox_id}',
        f'agent-sandbox-{record.sandbox_id}-internal', binding.volume_name,
        record.transaction_id, record.created_at, record.expires_at, workspace=binding,
    )
    return record, binding


@pytest.mark.anyio
async def test_pending_inspection_is_read_only_and_restart_recovers_original_binding(workspace_service):
    """Inspection leaves pending occupancy untouched; exclusive restart recovery confirms that holder."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    runtime.contents[workspace.workspace_id]['kept'] = b'recovered-data'
    record, binding = await _persist_pending_workspace_instance(store, runtime, workspace)
    occupied = await store.get_workspace(workspace.workspace_id)
    reopened = SQLiteLifecycleStore(store.database_path)
    restarted = SandboxService(runtime, reopened, clock=lambda: NOW + timedelta(seconds=5))
    inspection = await restarted.inspect_pending_once()
    assert [(item.sandbox_id, item.classification) for item in inspection.items] == [(record.sandbox_id, 'all_present')]
    assert await reopened.get(record.sandbox_id) == record
    assert await reopened.get_workspace(workspace.workspace_id) == occupied
    assert await reopened.get_workspace_binding(record.sandbox_id, record.transaction_id) == binding
    assert not runtime.specs and not runtime.deletions
    result = await restarted.recover_pending_once()
    assert [(item.sandbox_id, item.outcome, item.reason) for item in result.items] == [(record.sandbox_id, 'recovered', 'recovered')]
    confirmed = await reopened.get(record.sandbox_id)
    assert confirmed == replace(record, management_state='active', version=1)
    assert await reopened.get_workspace(workspace.workspace_id) == occupied
    assert await reopened.get_workspace_binding(record.sandbox_id, record.transaction_id) == binding
    assert runtime.inspections == [(record.sandbox_id, record.transaction_id, binding)] * 2
    assert runtime.contents[workspace.workspace_id] == {'kept': b'recovered-data'}
    assert not runtime.specs and not runtime.deletions


@pytest.mark.anyio
@pytest.mark.parametrize('mismatch', ['transaction_id', 'created_at', 'workspace_id', 'binding'])
async def test_pending_recovery_identity_mismatch_never_rebinds_or_releases_workspace(workspace_service, mismatch):
    """A ready-looking replacement cannot activate the original pending transaction or free its data."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    runtime.contents[workspace.workspace_id]['kept'] = b'original-holder'
    record, binding = await _persist_pending_workspace_instance(store, runtime, workspace)
    occupied = await store.get_workspace(workspace.workspace_id)
    if mismatch == 'binding':
        runtime.instances[record.sandbox_id] = replace(
            runtime.instances[record.sandbox_id], workspace=replace(binding, creation_token='wct_' + 'd' * 32),
        )
        expected_reason = 'resource_ownership_conflict'
    else:
        runtime.query_overrides = {
            mismatch: {
                'transaction_id': 'txn_' + 'd' * 32,
                'created_at': NOW + timedelta(seconds=1),
                'workspace_id': 'wsp_' + 'd' * 32,
            }[mismatch],
        }
        if mismatch == 'workspace_id':
            runtime.query_overrides['workspace'] = replace(
                binding, workspace_id='wsp_' + 'd' * 32,
                volume_name='agent-sandbox-wsp_' + 'd' * 32,
            )
        expected_reason = 'sandbox_identity_mismatch'
    restarted = SandboxService(runtime, SQLiteLifecycleStore(store.database_path), clock=lambda: NOW)
    result = await restarted.recover_pending_once()
    assert [(item.outcome, item.reason) for item in result.items] == [('skipped', expected_reason)]
    assert await store.get(record.sandbox_id) == record
    assert await store.get_workspace(workspace.workspace_id) == occupied
    assert await store.get_workspace_binding(record.sandbox_id, record.transaction_id) == binding
    assert runtime.contents[workspace.workspace_id] == {'kept': b'original-holder'}
    assert not runtime.specs and not runtime.deletions


@pytest.mark.anyio
async def test_separate_services_compete_for_one_atomic_workspace_occupancy(tmp_path):
    """Two Services with separate SQLite connections cannot both mount the same free workspace."""
    database_path = tmp_path / 'shared-lifecycle.sqlite3'
    first_store = GatedWorkspaceStore(database_path)
    await first_store.initialize()
    second_store = GatedWorkspaceStore(database_path)
    runtime = WorkspaceRuntime()
    first_service = SandboxService(runtime, first_store, clock=lambda: NOW)
    second_service = SandboxService(runtime, second_store, clock=lambda: NOW)
    workspace = await first_service.create_workspace()
    runtime.contents[workspace.workspace_id]['kept'] = b'one-holder'
    first_store.block_prewrite = second_store.block_prewrite = True
    options = models.SandboxCreateOptions(workspace_id=workspace.workspace_id)
    tasks = [asyncio.create_task(first_service.create(options)), asyncio.create_task(second_service.create(options))]
    try:
        await asyncio.wait_for(asyncio.gather(
            first_store.prewrite_entered.wait(), second_store.prewrite_entered.wait(),
        ), 3)
        assert not runtime.specs and not await first_store.list_pending()
    finally:
        first_store.prewrite_release.set()
        second_store.prewrite_release.set()
    outcomes = await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), 3)
    successes = [result for result in outcomes if isinstance(result, models.SandboxCreateResult)]
    conflicts = [error for error in outcomes if isinstance(error, WorkspaceConflict)]
    assert len(successes) == len(conflicts) == 1
    assert len(runtime.specs) == len(runtime.instances) == 1
    holder = await second_store.get_workspace(workspace.workspace_id)
    assert holder.sandbox_id == successes[0].sandbox_id
    assert (await first_store.get(holder.sandbox_id)).management_state == 'active'
    assert not await first_store.list_pending()
    assert runtime.contents[workspace.workspace_id] == {'kept': b'one-holder'}


@pytest.mark.anyio
async def test_missing_main_resource_delete_releases_only_original_occupancy(workspace_service):
    """Missing runtime resources still allow exact metadata cleanup while keeping independent data."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    created = await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    runtime.contents[workspace.workspace_id]['kept'] = b'main-gone'
    original = runtime.instances.pop(created.sandbox_id)
    await service.delete(created.sandbox_id)
    assert await store.get(created.sandbox_id) is None
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is None
    assert runtime.contents[workspace.workspace_id] == {'kept': b'main-gone'}
    assert original.workspace.sandbox_id == created.sandbox_id
    assert runtime.deletions == [(created.sandbox_id, False)]


@pytest.mark.anyio
@pytest.mark.parametrize('mismatched_field', ['creation_token', 'image_id', 'transaction_id'])
async def test_query_and_renew_reject_actual_workspace_identity_mismatch(workspace_service, mismatched_field):
    """The same workspace ID cannot hide a replacement token, image or sandbox transaction."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    created = await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    before = await store.get(created.sandbox_id)
    occupied = await store.get_workspace(workspace.workspace_id)
    original_binding = runtime.instances[created.sandbox_id].workspace
    replacement_values = {
        'creation_token': 'wct_' + 'e' * 32,
        'image_id': 'sha256:' + 'e' * 64,
        'transaction_id': 'txn_' + 'e' * 32,
    }
    runtime.query_overrides = {
        'workspace': replace(original_binding, **{mismatched_field: replacement_values[mismatched_field]}),
    }
    with pytest.raises(SandboxMetadataConflict):
        await service.get(created.sandbox_id)
    with pytest.raises(SandboxMetadataConflict):
        await service.renew(created.sandbox_id, 3600)
    assert await store.get(created.sandbox_id) == before
    assert await store.get_workspace(workspace.workspace_id) == occupied
    assert not runtime.deletions


@pytest.mark.anyio
async def test_cancelled_creation_cleanup_failure_preserves_holder_for_restart_retry(gated_workspace_service):
    """An unsuccessful cancellation rollback retains its claimed transaction until exact retry."""
    service, store, runtime = gated_workspace_service
    workspace = await service.create_workspace()
    runtime.contents[workspace.workspace_id]['kept'] = b'cleanup-failed'
    store.block_activation = True
    task = asyncio.create_task(service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id)))
    try:
        await asyncio.wait_for(store.activation_entered.wait(), 3)
        pending = (await store.list_pending())[0]
        runtime.delete_error = SandboxDeleteFailed(pending.sandbox_id)
        task.cancel()
    finally:
        store.activation_release.set()
    with pytest.raises(SandboxCleanupFailed):
        await asyncio.wait_for(task, 3)
    assert (await store.get(pending.sandbox_id)).management_state == 'deleting'
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id == pending.sandbox_id
    assert pending.sandbox_id in runtime.instances
    with pytest.raises(WorkspaceConflict):
        await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    restarted = SandboxService(runtime, SQLiteLifecycleStore(store.database_path), clock=lambda: NOW)
    runtime.delete_error = None
    await restarted.delete(pending.sandbox_id)
    assert await store.get(pending.sandbox_id) is None
    assert (await store.get_workspace(workspace.workspace_id)).sandbox_id is None
    assert runtime.contents[workspace.workspace_id] == {'kept': b'cleanup-failed'}


@pytest.mark.anyio
async def test_unrecorded_runtime_reference_blocks_occupancy_before_pending_write(workspace_service):
    """A free metadata row cannot override a still-mounted runtime reference to the volume."""
    service, store, runtime = workspace_service
    workspace = await service.create_workspace()
    runtime.contents[workspace.workspace_id]['kept'] = b'referenced-data'
    record, _binding = await _persist_pending_workspace_instance(store, runtime, workspace)
    # Model an external reference left after metadata loss, without invoking Runtime cleanup.
    await store.delete(record.sandbox_id, record.transaction_id, expected_version=record.version)
    free_metadata = await store.get_workspace(workspace.workspace_id)
    assert free_metadata.sandbox_id is None
    with pytest.raises(WorkspaceConflict):
        await service.create(models.SandboxCreateOptions(workspace_id=workspace.workspace_id))
    assert await store.get_workspace(workspace.workspace_id) == free_metadata
    assert not await store.list_pending()
    assert record.sandbox_id in runtime.instances
    assert runtime.contents[workspace.workspace_id] == {'kept': b'referenced-data'}
    assert not runtime.specs and not runtime.deletions
