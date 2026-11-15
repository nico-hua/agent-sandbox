"""Internal Service/SQLite/PVC receipt creation without public image resolution or cluster I/O."""

import asyncio
import threading
from dataclasses import replace
from pathlib import Path

import pytest

from kubernetes import client

from control_plane.core.errors import (
    CreationResourceState, KubernetesConnectionFailed, KubernetesOperationUnsupported,
    WorkspaceCreationFailed, WorkspaceError, WorkspaceOperationFailed, WorkspaceUnavailable, WorkspaceConflict,
)
from control_plane.core.models import WorkspaceResourceReceipt
from control_plane.core.service import SandboxService
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore, LifecycleStoreError
from test_kubernetes_workspace_resource import FakeAPI, clock
from test_kubernetes_workspace_spec import record, UID, NAMESPACE, SC


class OwnedAPI(FakeAPI):
    """Add thread ownership and deterministic close faults to the real-shaped PVC fake."""

    def __init__(self):
        """Retain call boundaries without constructing a real SDK transport."""
        super().__init__()
        self.api_client = self
        self.close_error = None
        self.threads = []
        self.before_post = None

    def create_namespaced_persistent_volume_claim(self, namespace, body, **kwargs):
        """Expose pre-commit barriers to test ordering and repeated cancellation deterministically."""
        if self.before_post:
            self.before_post()
        return super().create_namespaced_persistent_volume_claim(namespace, body, **kwargs)

    def close(self):
        """Record eventual pool cleanup on the SDK worker thread."""
        self.threads.append(threading.get_ident())
        if self.close_error:
            raise self.close_error


def runtime(api):
    """Construct an inert configured Runtime whose factory records its owning worker."""
    def factory(**kwargs):
        """Check the exact trusted target before returning the borrowed API facade."""
        assert kwargs == dict(kubeconfig_path=Path('/not-read'), context='explicit')
        api.threads.append(threading.get_ident())
        return api

    return KubernetesSandboxRuntime(kubeconfig_path='/not-read', context='explicit', namespace=NAMESPACE,
        proxy_image='agent-sandbox-proxy:dev', workspace_storage_class=SC, client_factory=factory)


@pytest.fixture
async def store(tmp_path):
    """Initialize only a temporary, current-schema SQLite database."""
    result = SQLiteLifecycleStore(tmp_path / 'workspace.sqlite3')
    await result.initialize()
    return result


@pytest.mark.anyio
async def test_internal_create_confirms_original_receipt_atomically(store):
    """Only a validated original POST response can confirm pending into durable available."""
    api = OwnedAPI()
    backend = runtime(api)
    service = SandboxService(backend, store=store)
    result = await service._create_workspace_record(record())
    assert result.management_state == 'available' and result.version == 1
    receipt = await store.get_workspace_receipt(result.workspace_id, result.creation_token, expected_version=1)
    assert receipt.pvc_uid == UID and receipt.namespace == NAMESPACE and receipt.storage_class == SC
    assert receipt.original_uid_verified is True
    assert api.threads[0] == api.threads[1] != threading.get_ident()
    assert sum(call[0] == 'post' for call in api.calls) == 1
    reopened = SQLiteLifecycleStore(store.database_path)
    assert await reopened.get_workspace_receipt(result.workspace_id, result.creation_token, expected_version=1) == receipt


@pytest.mark.anyio
@pytest.mark.parametrize('result', [None, {}, 'invalid'])
async def test_required_receipt_never_falls_back_to_ordinary_confirmation(store, result):
    """A Kubernetes backend missing its required receipt must leave the original pending record."""
    backend = runtime(OwnedAPI())

    async def broken(_record):
        """Simulate a buggy backend return without creating any real resource."""
        return result

    backend.create_workspace = broken
    with pytest.raises(WorkspaceOperationFailed):
        await SandboxService(backend, store=store)._create_workspace_record(record())
    saved = await store.get_workspace(record().workspace_id)
    assert saved.management_state == 'pending' and saved.version == 0


def original_receipt(**changes):
    """Supply only typed original-create evidence, with independently specified immutable identity."""
    value = record()
    receipt = WorkspaceResourceReceipt(value.workspace_id, value.creation_token, value.volume_name, value.image_id,
        value.created_at, 'kubernetes', NAMESPACE, 'agent-sandbox-wsp-' + value.workspace_id[4:], UID, SC, True)
    return replace(receipt, **changes)


@pytest.mark.anyio
async def test_pending_precedes_post_and_receipt_confirmation_is_one_operation(store, monkeypatch):
    """The resource POST sees committed pending; Service never saves first or ordinarily confirms."""
    api = OwnedAPI()
    before = store._get_workspace_sync

    def inspect_pending():
        """Read from an independent worker connection before the fake API commits the PVC."""
        assert before(record().workspace_id) == record()

    api.before_post = inspect_pending
    original_confirm = store.confirm_workspace_with_receipt
    confirms = []

    async def confirm(receipt, **kwargs):
        """Verify no receipt or state was prematurely published before atomic confirmation."""
        assert await store.get_workspace(record().workspace_id) == record()
        assert await store.get_workspace_receipt(record().workspace_id, record().creation_token, expected_version=0) is None
        confirms.append(receipt)
        return await original_confirm(receipt, **kwargs)

    async def forbidden(*args, **kwargs):
        """Reject the two-step confirmation path that could publish inconsistent metadata."""
        pytest.fail('ordinary confirmation or separate receipt save')

    monkeypatch.setattr(store, 'confirm_workspace_with_receipt', confirm)
    monkeypatch.setattr(store, 'confirm_workspace', forbidden)
    monkeypatch.setattr(store, 'save_workspace_receipt', forbidden)
    await SandboxService(runtime(api), store=store)._create_workspace_record(record())
    assert confirms == [original_receipt()]


@pytest.mark.anyio
@pytest.mark.parametrize('change', [dict(creation_token='wct_' + 'c' * 32), dict(image_id='sha256:' + 'e' * 64)])
async def test_receipt_for_another_immutable_identity_is_not_saved(store, change):
    """A valid receipt for a different creation may never be confirmed against the pending original."""
    backend = runtime(OwnedAPI())

    async def broken(_record):
        """Return structurally valid but mismatched original creation evidence."""
        return original_receipt(**change)

    backend.create_workspace = broken
    with pytest.raises(WorkspaceOperationFailed):
        await SandboxService(backend, store=store)._create_workspace_record(record())
    assert await store.get_workspace(record().workspace_id) == record()
    assert await store.get_workspace_receipt(record().workspace_id, record().creation_token, expected_version=0) is None


@pytest.mark.anyio
async def test_close_failure_saves_original_receipt_but_keeps_pending(store):
    """Late client failure preserves original UID for recovery without advertising availability."""
    api = OwnedAPI()
    api.close_error = RuntimeError('private transport diagnostic')
    with pytest.raises(WorkspaceCreationFailed) as failed:
        await SandboxService(runtime(api), store=store)._create_workspace_record(record())
    assert failed.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert 'private' not in str(failed.value) and UID not in repr(failed.value)
    assert await store.get_workspace(record().workspace_id) == record()
    assert await store.get_workspace_receipt(record().workspace_id, record().creation_token, expected_version=0) == original_receipt()
    assert api.pvc is not None and not any(call[0] == 'delete' for call in api.calls)


@pytest.mark.anyio
@pytest.mark.parametrize('mode,state', [('reject', 'unchanged'), ('lost', 'unknown'), ('malformed', 'unknown')])
async def test_post_failure_or_candidate_never_becomes_original_receipt(store, mode, state, clock):
    """Denied, ambiguous or invalid responses retain distinct facts and never adopt subsequent candidates."""
    api = OwnedAPI()
    if mode == 'reject':
        api.reject = client.ApiException(status=403, reason='private diagnostic')
    elif mode == 'lost':
        api.post_error = TimeoutError('private diagnostic')
    else:
        api.post_response = client.V1PersistentVolumeClaim(metadata=client.V1ObjectMeta())
    api.close_error = RuntimeError('secondary close failure')
    with pytest.raises(WorkspaceError) as failed:
        await SandboxService(runtime(api), store=store)._create_workspace_record(record())
    assert failed.value.creation_failure.resource_state.value == state
    assert await store.get_workspace(record().workspace_id) == record()
    assert await store.get_workspace_receipt(record().workspace_id, record().creation_token, expected_version=0) is None
    assert sum(call[0] == 'post' for call in api.calls) == 1
    assert len(api.threads) == 2


@pytest.mark.anyio
async def test_client_acquisition_failure_has_no_submission(store):
    """Client acquisition failure is unchanged, while Service still preserves the diagnostic pending identity."""
    def factory(**kwargs):
        """Fail before acquiring a pool or submitting a request."""
        raise TimeoutError('private configuration')

    backend = KubernetesSandboxRuntime(kubeconfig_path='/not-read', context='explicit', namespace=NAMESPACE,
        proxy_image='proxy:dev', workspace_storage_class=SC, client_factory=factory)
    with pytest.raises(WorkspaceOperationFailed) as failed:
        await SandboxService(backend, store=store)._create_workspace_record(record())
    assert failed.value.creation_failure.resource_state is CreationResourceState.UNCHANGED
    assert await store.get_workspace(record().workspace_id) == record()


@pytest.mark.anyio
@pytest.mark.parametrize('after_commit', [False, True])
async def test_confirmation_failure_never_retries_or_deletes_resources(store, monkeypatch, after_commit):
    """Both rejected writes and lost acknowledgements preserve actual database facts without compensation."""
    api = OwnedAPI()
    original = store.confirm_workspace_with_receipt
    calls = []

    async def fail(receipt, **kwargs):
        """Optionally commit before losing the sole confirmation acknowledgement."""
        calls.append(receipt)
        if after_commit:
            await original(receipt, **kwargs)
        raise LifecycleStoreError('private database diagnostic')

    monkeypatch.setattr(store, 'confirm_workspace_with_receipt', fail)
    with pytest.raises(WorkspaceUnavailable) as failed:
        await SandboxService(runtime(api), store=store)._create_workspace_record(record())
    assert failed.value.receipt == original_receipt()
    assert failed.value.creation_failure.resource_state is CreationResourceState.REMAINING
    saved = await store.get_workspace(record().workspace_id)
    assert saved.management_state == ('available' if after_commit else 'pending')
    receipt = await store.get_workspace_receipt(saved.workspace_id, saved.creation_token, expected_version=saved.version)
    assert receipt == (original_receipt() if after_commit else None)
    assert len(calls) == 1 and api.pvc is not None
    assert not any(call[0] == 'delete' for call in api.calls)


@pytest.mark.anyio
@pytest.mark.parametrize('after_commit', [False, True])
async def test_failed_receipt_save_keeps_resource_and_original_state(store, monkeypatch, after_commit):
    """A close failure plus failed receipt persistence cannot justify deleting data or refreshing CAS."""
    api = OwnedAPI()
    api.close_error = RuntimeError('close failed')
    original = store.save_workspace_receipt
    calls = []

    async def fail(receipt, **kwargs):
        """Expose both precommit failure and committed-but-unacknowledged receipt persistence."""
        calls.append(receipt)
        if after_commit:
            await original(receipt, **kwargs)
        raise LifecycleStoreError('write failed')

    monkeypatch.setattr(store, 'save_workspace_receipt', fail)
    with pytest.raises(WorkspaceUnavailable) as failed:
        await SandboxService(runtime(api), store=store)._create_workspace_record(record())
    assert failed.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert failed.value.receipt == original_receipt()
    assert await store.get_workspace(record().workspace_id) == record()
    stored = await store.get_workspace_receipt(record().workspace_id, record().creation_token, expected_version=0)
    assert stored == (original_receipt() if after_commit else None)
    assert len(calls) == 1 and api.pvc is not None


@pytest.mark.anyio
@pytest.mark.parametrize('close_fails', [False, True])
async def test_repeated_cancellation_settles_late_post_and_persists_original(store, close_fails):
    """Repeated waiter cancellation cannot lose a late original UID or interrupt pool cleanup and persistence."""
    api = OwnedAPI()
    entered, release = threading.Event(), threading.Event()

    def block():
        """Hold the in-flight POST at an explicit boundary until the test releases it."""
        entered.set()
        assert release.wait(5)

    api.before_post = block
    if close_fails:
        api.close_error = RuntimeError('close failed')
    task = asyncio.create_task(SandboxService(runtime(api), store=store)._create_workspace_record(record()))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(WorkspaceCreationFailed if close_fails else asyncio.CancelledError):
        await task
    saved = await store.get_workspace(record().workspace_id)
    assert saved.management_state == ('pending' if close_fails else 'available')
    assert await store.get_workspace_receipt(saved.workspace_id, saved.creation_token, expected_version=saved.version) == original_receipt()
    assert len(api.threads) == 2 and api.pvc is not None


@pytest.mark.anyio
async def test_direct_runtime_cancellation_carries_late_original_receipt():
    """Direct Runtime users also receive original identity evidence instead of losing it to cancellation."""
    api = OwnedAPI()
    entered, release = threading.Event(), threading.Event()

    def block():
        """Wait for a repeated cancellation before returning the original successful POST."""
        entered.set()
        assert release.wait(5)

    api.before_post = block
    task = asyncio.create_task(runtime(api).create_workspace(record()))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
    finally:
        release.set()
    with pytest.raises(WorkspaceCreationFailed) as failed:
        await task
    assert failed.value.receipt == original_receipt() and len(api.threads) == 2


@pytest.mark.anyio
async def test_docker_none_confirmation_unchanged(store):
    """The legacy backend does not require a receipt and retains its ordinary confirmation path."""
    from test_workspace_api import WorkspaceRuntime

    backend = WorkspaceRuntime()
    result = await SandboxService(backend, store=store)._create_workspace_record(record())
    assert result.management_state == 'available'
    assert await store.get_workspace_receipt(result.workspace_id, result.creation_token, expected_version=1) is None


@pytest.mark.anyio
async def test_public_creation_still_stops_before_pending_or_client(store):
    """Tag-only configuration keeps unsupported public creation outside identity allocation and SDK work."""
    api = OwnedAPI()
    with pytest.raises(KubernetesOperationUnsupported):
        await SandboxService(runtime(api), store=store).create_workspace()
    assert await store.get_workspace(record().workspace_id) is None
    assert api.calls == [] and api.threads == []


@pytest.mark.anyio
async def test_confirm_cas_conflict_keeps_original_receipt_and_does_not_refresh(store, monkeypatch):
    """A concurrent management change cannot be bypassed by refreshing the expected confirmation version."""
    api = OwnedAPI()
    calls = []

    async def competing_confirmation(receipt, **kwargs):
        """Advance the real row just before the original receipt confirmation CAS."""
        calls.append(kwargs['expected_version'])
        await store.confirm_workspace(record().workspace_id, record().creation_token, expected_version=0)
        return await original(receipt, **kwargs)

    original = store.confirm_workspace_with_receipt
    monkeypatch.setattr(store, 'confirm_workspace_with_receipt', competing_confirmation)
    with pytest.raises(WorkspaceConflict) as failed:
        await SandboxService(runtime(api), store=store)._create_workspace_record(record())
    assert failed.value.receipt == original_receipt() and calls == [0]
    saved = await store.get_workspace(record().workspace_id)
    assert saved.version == 1 and saved.management_state == 'available'
    assert await store.get_workspace_receipt(saved.workspace_id, saved.creation_token, expected_version=1) is None
    assert api.pvc is not None and not any(call[0] == 'delete' for call in api.calls)


@pytest.mark.anyio
@pytest.mark.parametrize('phase', ['confirmation', 'failed_receipt_save'])
async def test_cancellation_during_persistence_waits_for_final_database_result(store, monkeypatch, phase):
    """Cancellation cannot interrupt atomic confirmation or lose a failed operation's original pending receipt."""
    api = OwnedAPI()
    entered, release = asyncio.Event(), asyncio.Event()
    method = 'confirm_workspace_with_receipt' if phase == 'confirmation' else 'save_workspace_receipt'
    original = getattr(store, method)
    if phase == 'failed_receipt_save':
        api.close_error = RuntimeError('close failed')

    async def blocked(receipt, **kwargs):
        """Hold receipt persistence at an explicit event until both cancellations have been observed."""
        entered.set()
        await release.wait()
        return await original(receipt, **kwargs)

    monkeypatch.setattr(store, method, blocked)
    task = asyncio.create_task(SandboxService(runtime(api), store=store)._create_workspace_record(record()))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError if phase == 'confirmation' else WorkspaceCreationFailed):
        await task
    saved = await store.get_workspace(record().workspace_id)
    assert saved.management_state == ('available' if phase == 'confirmation' else 'pending')
    assert await store.get_workspace_receipt(saved.workspace_id, saved.creation_token, expected_version=saved.version) == original_receipt()


@pytest.mark.anyio
async def test_lost_pending_commit_acknowledgement_never_dispatches_post(store, monkeypatch):
    """An uncertain prewrite retains its actual pending row and stops before creating a PVC."""
    api = OwnedAPI()
    original = store.create_workspace

    async def lost_ack(value):
        """Commit the pending identity, then lose the sole acknowledgement."""
        await original(value)
        raise LifecycleStoreError('lost acknowledgement')

    monkeypatch.setattr(store, 'create_workspace', lost_ack)
    with pytest.raises(WorkspaceUnavailable):
        await SandboxService(runtime(api), store=store)._create_workspace_record(record())
    assert await store.get_workspace(record().workspace_id) == record()
    assert api.calls == [] and api.threads == []


@pytest.mark.anyio
async def test_public_http_does_not_allocate_workspace_identity(store, monkeypatch):
    """Authenticated tag-only public creation fails at image resolution before identity allocation."""
    import httpx
    from control_plane.app import create_app
    from control_plane.core import service as service_module

    def forbidden(*args, **kwargs):
        """Catch premature ID generation or a pending write in the unsupported public path."""
        pytest.fail('public workspace creation allocated identity or wrote pending')

    api = OwnedAPI()
    app = create_app(runtime=runtime(api), store=store, api_key='workspace-test-key', reaper_enabled=False)
    monkeypatch.setattr(service_module.secrets, 'token_hex', forbidden)
    monkeypatch.setattr(store, 'create_workspace', forbidden)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://control') as client:
        response = await client.post('/v1/workspaces', headers={'Authorization': 'Bearer workspace-test-key'}, json={})
    assert response.status_code == 501
    assert response.json()['error']['code'] == 'runtime_operation_unsupported'
    assert api.calls == [] and api.threads == []


@pytest.mark.anyio
@pytest.mark.parametrize('through_service', [False, True])
async def test_worker_and_service_use_private_immutable_identity_snapshot(store, through_service):
    """Caller-held record mutation after dispatch cannot change the original POST receipt or confirmation identity."""
    api = OwnedAPI()
    entered, release = threading.Event(), threading.Event()
    source = record()

    def block():
        """Hold the POST after resource construction while the caller changes its own record object."""
        entered.set()
        assert release.wait(5)

    api.before_post = block
    backend = runtime(api)
    operation = SandboxService(backend, store=store)._create_workspace_record(source) if through_service else backend.create_workspace(source)
    task = asyncio.create_task(operation)
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        object.__setattr__(source, 'image_id', 'sha256:' + 'f' * 64)
    finally:
        release.set()
    result = await task
    if through_service:
        assert result.management_state == 'available' and result.image_id == record().image_id
        receipt = await store.get_workspace_receipt(result.workspace_id, result.creation_token, expected_version=1)
    else:
        receipt = result
    assert receipt == original_receipt()
