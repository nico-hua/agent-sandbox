"""Durable receipt-based workspace access through real Service/SQLite and offline Kubernetes clients."""

import asyncio
import threading
from dataclasses import replace
from datetime import timedelta

import pytest
from kubernetes import client

from control_plane.core.errors import (
    KubernetesConfigurationError, KubernetesConnectionFailed, KubernetesPermissionDenied, KubernetesTimeout,
    WorkspaceConflict, WorkspaceNotFound, WorkspaceOperationFailed, WorkspaceUnavailable,
)
from control_plane.core.service import SandboxService
from control_plane.core.models import SandboxLifecycleRecord, WorkspaceBinding
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore, LifecycleStoreError
from test_kubernetes_workspace_create import OwnedAPI, store, runtime, original_receipt
from test_kubernetes_workspace_resource import clock, consumer
from test_kubernetes_workspace_spec import record, UID, OTHER_UID, NAMESPACE, SC

pytestmark = pytest.mark.anyio


def rebuilt_runtime(api, **changes):
    """Reconstruct management without original creation StorageClass or proxy configuration."""
    def factory(**kwargs):
        """Acquire only the existing offline client with explicit trusted context."""
        api.threads.append(threading.get_ident())
        return api

    values = dict(kubeconfig_path='/not-read', context='explicit', namespace=NAMESPACE, client_factory=factory)
    values.update(changes)
    return KubernetesSandboxRuntime(**values)


async def created_workspace(store):
    """Enter the supported trusted internal creation chain without forging availability or original UID."""
    api = OwnedAPI()
    available = await SandboxService(runtime(api), store=store)._create_workspace_record(record())
    return api, available


async def test_rebuilt_runtime_and_store_inspect_original_pending_pvc(store):
    """The original receipt survives both objects being rebuilt; legal Pending is only identity-checked."""
    api, available = await created_workspace(store)
    reopened = SQLiteLifecycleStore(store.database_path)
    service = SandboxService(rebuilt_runtime(api), store=reopened)
    assert await service.get_workspace(available.workspace_id) == available
    assert api.pvc.status.phase == 'Pending'
    assert await reopened.get_workspace_receipt(available.workspace_id, available.creation_token, expected_version=1) == original_receipt()


async def test_service_delete_removes_original_then_metadata_and_receipt(store):
    """CAS deletion uses the original UID and current RV; only confirmed cleanup releases durable metadata."""
    api, available = await created_workspace(store)
    service = SandboxService(rebuilt_runtime(api), store=SQLiteLifecycleStore(store.database_path))
    await service.delete_workspace(available.workspace_id)
    assert api.pvc is None and await store.get_workspace(available.workspace_id) is None
    deletes = [call for call in api.calls if call[0] == 'delete']
    assert len(deletes) == 1 and deletes[0][3].preconditions.uid == UID
    assert deletes[0][3].preconditions.resource_version == '7'
    await service.delete_workspace(available.workspace_id)
    assert len([call for call in api.calls if call[0] == 'delete']) == 1


async def test_kubernetes_missing_receipt_fails_before_client(store):
    """A legacy metadata row cannot cause Kubernetes to rediscover or adopt a same-name PVC."""
    await store.create_workspace(record())
    await store.confirm_workspace(record().workspace_id, record().creation_token, expected_version=0)
    api = OwnedAPI()
    with pytest.raises(WorkspaceUnavailable):
        await SandboxService(rebuilt_runtime(api), store=store).get_workspace(record().workspace_id)
    assert api.calls == [] and api.threads == []


async def test_namespace_conflict_rejected_before_sdk_acquisition(store):
    """Saved targets cannot redirect a reconstructed Runtime outside its trusted namespace."""
    api, available = await created_workspace(store)
    api.calls.clear()
    api.threads.clear()
    with pytest.raises(WorkspaceConflict):
        await SandboxService(rebuilt_runtime(api, namespace='another-namespace'), store=store).get_workspace(available.workspace_id)
    assert api.calls == [] and api.threads == []


@pytest.mark.parametrize('action', ['inspect_workspace', 'delete_workspace'])
@pytest.mark.parametrize('field,value', [
    ('backend', 'docker'), ('workspace_id', 'wsp_' + 'c' * 32),
    ('creation_token', 'wct_' + 'b' * 32), ('image_id', 'sha256:' + 'd' * 64),
    ('created_at', record().created_at + timedelta(seconds=1)),
    ('namespace', 'different-namespace'), ('pvc_name', 'arbitrary-name'),
    ('pvc_uid', ''), ('original_uid_verified', False), ('storage_class', 'bad class'),
])
async def test_malformed_or_mismatched_receipt_rejected_without_client(action, field, value):
    """No credential, target or immutable identity corruption may reach the SDK client factory."""
    receipt = original_receipt()
    object.__setattr__(receipt, field, value)
    api = OwnedAPI()
    with pytest.raises(WorkspaceConflict):
        await getattr(rebuilt_runtime(api), action)(record(), receipt=receipt)
    assert api.calls == [] and api.threads == []


@pytest.mark.parametrize('action', ['inspect_workspace', 'delete_workspace'])
@pytest.mark.parametrize('value', [None, {}, False])
async def test_direct_runtime_requires_full_receipt(action, value):
    """Neither absent nor untyped evidence permits discovery and adoption by name."""
    api = OwnedAPI()
    with pytest.raises(WorkspaceUnavailable if value is None else WorkspaceConflict):
        await getattr(rebuilt_runtime(api), action)(record(), receipt=value)
    assert api.calls == [] and api.threads == []


@pytest.mark.parametrize('action', ['inspect_workspace', 'delete_workspace'])
@pytest.mark.parametrize('value', [None, {}])
async def test_invalid_record_is_fixed_conflict_before_client(action, value):
    """Direct Runtime callers cannot turn malformed records into raw exceptions or SDK operations."""
    api = OwnedAPI()
    with pytest.raises(WorkspaceConflict):
        await getattr(rebuilt_runtime(api), action)(value)
    assert api.calls == [] and api.threads == []


async def test_original_storage_class_used_even_when_new_creation_config_differs(store):
    """Management relies on persisted original storage settings, not a subsequent constructor's creation policy."""
    api, available = await created_workspace(store)
    changed = rebuilt_runtime(api, proxy_image='proxy:dev', workspace_storage_class='another-storage')
    assert await SandboxService(changed, store=store).get_workspace(available.workspace_id) == available
    await SandboxService(changed, store=store).delete_workspace(available.workspace_id)
    assert api.pvc is None


@pytest.mark.parametrize('fault,error', [
    ('namespace_missing', KubernetesConfigurationError), ('permission', KubernetesPermissionDenied),
    ('connection', KubernetesConnectionFailed), ('timeout', KubernetesTimeout),
])
async def test_backend_observation_failure_not_workspace_absence(store, fault, error):
    """Namespace absence, denied reads and transport failures remain distinct from a proven object 404."""
    api, available = await created_workspace(store)
    api.calls.clear()
    api.pvc = None
    if fault == 'namespace_missing':
        api.errors['namespace'] = client.ApiException(status=404, reason='private response')
    else:
        api.errors['list'] = {'permission': client.ApiException(status=403, reason='private response'),
            'connection': OSError('private address'), 'timeout': TimeoutError('private address')}[fault]
    with pytest.raises(error) as failed:
        await SandboxService(rebuilt_runtime(api), store=store).get_workspace(available.workspace_id)
    assert 'private' not in str(failed.value)
    assert not any(call[0] in ('post', 'delete') for call in api.calls)
    assert await store.get_workspace(available.workspace_id) == available


async def test_confirmed_pvc_absence_is_not_found_and_get_is_read_only(store):
    """Only full discovery plus exact-name absence and accessible namespace yields workspace-not-found."""
    api, available = await created_workspace(store)
    api.pvc = None
    api.calls.clear()
    with pytest.raises(WorkspaceNotFound):
        await SandboxService(rebuilt_runtime(api), store=store).get_workspace(available.workspace_id)
    assert any(call[0] == 'namespace' for call in api.calls)
    assert not any(call[0] in ('post', 'delete', 'consumers') for call in api.calls)
    assert await store.get_workspace(available.workspace_id) == available
    assert await store.get_workspace_receipt(available.workspace_id, available.creation_token, expected_version=1) == original_receipt()


@pytest.mark.parametrize('fault', ['uid', 'spec', 'metadata', 'truncated'])
async def test_identity_or_configuration_conflict_is_not_found_never(store, fault):
    """Replacements, edited declarations and incomplete inventory remain conflicts, preserving the original receipt."""
    api, available = await created_workspace(store)
    if fault == 'uid':
        api.pvc.metadata.uid = OTHER_UID
    elif fault == 'spec':
        api.pvc.spec.storage_class_name = 'different-storage'
    elif fault == 'metadata':
        api.pvc.metadata.labels['io.agent-sandbox.workspace-token'] = 'wct_' + 'f' * 32
    else:
        api.pvc_page = client.V1PersistentVolumeClaimList(api_version='v1', kind='PersistentVolumeClaimList',
            items=[], metadata=client.V1ListMeta(_continue='next'))
    api.calls.clear()
    service = SandboxService(rebuilt_runtime(api), store=store)
    with pytest.raises(WorkspaceConflict):
        await service.get_workspace(available.workspace_id)
    with pytest.raises(WorkspaceConflict):
        await service.delete_workspace(available.workspace_id)
    deleting = await store.get_workspace(available.workspace_id)
    assert deleting.management_state == 'deleting' and deleting.version == 2
    assert await store.get_workspace_receipt(deleting.workspace_id, deleting.creation_token, expected_version=2) == original_receipt()
    assert not any(call[0] in ('post', 'delete') for call in api.calls)


async def test_receipt_is_read_after_claim_with_deleting_version(store, monkeypatch):
    """Service cannot reuse an available-version receipt after claiming deletion."""
    api, available = await created_workspace(store)
    original = store.get_workspace_receipt
    seen = []

    async def guarded(wid, token, *, expected_version):
        """Require the row's committed deleting identity/version at the receipt read boundary."""
        current = await store.get_workspace(wid)
        assert current.management_state == 'deleting' and current.version == expected_version == 2
        seen.append(expected_version)
        return await original(wid, token, expected_version=expected_version)

    monkeypatch.setattr(store, 'get_workspace_receipt', guarded)
    await SandboxService(rebuilt_runtime(api), store=store).delete_workspace(available.workspace_id)
    assert seen == [2]


async def test_version_change_between_record_and_receipt_read_rejects_query(store, monkeypatch):
    """A real concurrent CAS change is rejected before any backend query, with no version refresh."""
    api, available = await created_workspace(store)
    api.calls.clear()
    api.threads.clear()
    original = store.get_workspace_receipt
    versions = []

    async def race(wid, token, *, expected_version):
        """Claim the actual row before the old-version receipt read begins."""
        versions.append(expected_version)
        await store.claim_workspace_deletion(wid, token, expected_version=expected_version)
        return await original(wid, token, expected_version=expected_version)

    monkeypatch.setattr(store, 'get_workspace_receipt', race)
    with pytest.raises(WorkspaceConflict):
        await SandboxService(rebuilt_runtime(api), store=store).get_workspace(available.workspace_id)
    assert versions == [1] and api.calls == [] and api.threads == []


@pytest.mark.parametrize('operation', ['get_workspace', 'delete_workspace'])
@pytest.mark.parametrize('fault', ['none', 'identity', 'backend', 'read_error'])
async def test_service_rejects_bad_or_unavailable_stored_receipts(store, monkeypatch, operation, fault):
    """Untrusted stored receipt results never become fallback operations, including after deletion claim."""
    api, available = await created_workspace(store)
    api.calls.clear()
    api.threads.clear()

    async def bad(*args, **kwargs):
        """Inject a failed read or corrupt returned identity at the Store contract boundary."""
        if fault == 'read_error':
            raise LifecycleStoreError('private database path')
        if fault == 'none':
            return None
        receipt = original_receipt()
        object.__setattr__(receipt, 'backend' if fault == 'backend' else 'creation_token',
                           'docker' if fault == 'backend' else 'wct_' + 'f' * 32)
        return receipt

    monkeypatch.setattr(store, 'get_workspace_receipt', bad)
    error = WorkspaceUnavailable if fault in ('none', 'read_error') else WorkspaceOperationFailed
    with pytest.raises(error):
        await getattr(SandboxService(rebuilt_runtime(api), store=store), operation)(available.workspace_id)
    saved = await store.get_workspace(available.workspace_id)
    assert saved.management_state == ('deleting' if operation == 'delete_workspace' else 'available')
    assert api.calls == [] and api.threads == []


@pytest.mark.parametrize('phase,terminating', [('Running', False), ('Pending', False), ('Succeeded', False), ('Failed', True)])
async def test_actual_consumer_blocks_deletion_and_safe_retry_preserves_receipt(store, phase, terminating):
    """Even foreign or terminal consumers block PVC deletion; deleting-version retry does not change the receipt."""
    api, available = await created_workspace(store)
    api.pods = [consumer(phase, terminating=terminating)]
    service = SandboxService(rebuilt_runtime(api), store=store)
    with pytest.raises(WorkspaceConflict):
        await service.delete_workspace(available.workspace_id)
    assert await store.get_workspace_receipt(available.workspace_id, available.creation_token, expected_version=2) == original_receipt()
    assert api.pvc is not None and not any(call[0] == 'delete' for call in api.calls)
    api.pods = []
    await service.delete_workspace(available.workspace_id)
    assert api.pvc is None and await store.get_workspace(available.workspace_id) is None


@pytest.mark.parametrize('fault', ['truncated', 'permission', 'timeout'])
async def test_incomplete_consumer_scan_does_not_allow_delete(store, fault):
    """Pagination and failed namespace-wide observation cannot be treated as absence of consumers."""
    api, available = await created_workspace(store)
    if fault == 'truncated':
        api.continuation = 'next'
    else:
        api.errors['consumers'] = client.ApiException(status=403) if fault == 'permission' else TimeoutError()
    with pytest.raises(WorkspaceConflict if fault == 'truncated' else WorkspaceOperationFailed):
        await SandboxService(rebuilt_runtime(api), store=store).delete_workspace(available.workspace_id)
    assert api.pvc is not None and not any(call[0] == 'delete' for call in api.calls)
    assert (await store.get_workspace(available.workspace_id)).management_state == 'deleting'


@pytest.mark.parametrize('fault', ['lost', 'rv_conflict', 'timeout', 'replacement'])
async def test_delete_outcomes_preserve_retry_state_and_never_remove_replacement(store, fault, clock):
    """Lost responses require absence confirmation; conflicts/timeout retain deleting and never adopt a replacement."""
    api, available = await created_workspace(store)
    if fault == 'lost':
        api.delete_error = TimeoutError('response lost after commit')
    elif fault == 'rv_conflict':
        api.keep_on_delete = True
        api.delete_error = client.ApiException(status=409)
    elif fault == 'timeout':
        api.keep_on_delete = True
    else:
        api.replacement_on_delete = True
    service = SandboxService(rebuilt_runtime(api), store=store)
    if fault == 'lost':
        await service.delete_workspace(available.workspace_id)
        assert await store.get_workspace(available.workspace_id) is None and api.pvc is None
    else:
        with pytest.raises(WorkspaceOperationFailed if fault == 'timeout' else WorkspaceConflict):
            await service.delete_workspace(available.workspace_id)
        assert (await store.get_workspace(available.workspace_id)).management_state == 'deleting'
        assert await store.get_workspace_receipt(available.workspace_id, available.creation_token, expected_version=2) == original_receipt()
    assert sum(call[0] == 'delete' for call in api.calls) == 1


@pytest.mark.parametrize('after_commit', [False, True])
async def test_metadata_cleanup_failure_or_lost_ack_is_safe_to_retry(store, monkeypatch, after_commit):
    """Only the actual original absence permits metadata deletion; a failed acknowledgement never triggers another PVC DELETE."""
    api, available = await created_workspace(store)
    original = store.delete_workspace

    async def fail(wid, token, **kwargs):
        """Fail before commit or lose the final cleanup acknowledgement after cascading receipt deletion."""
        assert api.pvc is None
        if after_commit:
            await original(wid, token, **kwargs)
        raise LifecycleStoreError('private database path')

    monkeypatch.setattr(store, 'delete_workspace', fail)
    service = SandboxService(rebuilt_runtime(api), store=store)
    with pytest.raises(WorkspaceUnavailable):
        await service.delete_workspace(available.workspace_id)
    saved = await store.get_workspace(available.workspace_id)
    assert saved is None if after_commit else saved.management_state == 'deleting'
    if not after_commit:
        assert await store.get_workspace_receipt(available.workspace_id, available.creation_token, expected_version=2) == original_receipt()
    monkeypatch.setattr(store, 'delete_workspace', original)
    api.calls.clear()
    await service.delete_workspace(available.workspace_id)
    assert await store.get_workspace(available.workspace_id) is None
    assert not any(call[0] == 'delete' for call in api.calls)
    if not after_commit:
        assert any(call[0] == 'namespace' for call in api.calls)


@pytest.mark.parametrize('action', ['inspect_workspace', 'delete_workspace'])
async def test_docker_rejects_foreign_receipts_before_client(action):
    """A wrong-backend receipt must never be silently ignored and interpreted as Docker volume authority."""
    from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime

    def forbidden():
        """Catch accidental Docker access before rejecting the Kubernetes receipt."""
        pytest.fail('foreign receipt caused Docker access')

    with pytest.raises(WorkspaceConflict):
        await getattr(DockerSandboxRuntime(client_factory=forbidden), action)(record(), receipt=original_receipt())


async def test_database_occupancy_rejects_delete_before_backend_access(store):
    """The existing durable holder protocol remains authoritative before any PVC consumer scan."""
    api, available = await created_workspace(store)
    sid, tid = 'sbx_' + 'a' * 32, 'txn_' + 'b' * 32
    holder = SandboxLifecycleRecord(sid, tid, available.created_at, available.created_at + timedelta(hours=1),
                                    workspace_id=available.workspace_id)
    binding = WorkspaceBinding(available.workspace_id, available.creation_token, available.volume_name,
        available.image_id, 2, sid, tid)
    await store.create_with_workspace(holder, binding, expected_workspace_version=1)
    api.calls.clear()
    api.threads.clear()
    with pytest.raises(WorkspaceConflict):
        await SandboxService(rebuilt_runtime(api), store=store).delete_workspace(available.workspace_id)
    occupied = await store.get_workspace(available.workspace_id)
    assert occupied.sandbox_id == sid and occupied.version == 2 and occupied.management_state == 'available'
    assert api.calls == [] and api.threads == []


async def test_pending_receipt_does_not_permit_deletion_or_implicit_confirmation(store):
    """Persisted evidence on a failed pending creation cannot bypass the existing available-only claim."""
    await store.create_workspace(record())
    await store.save_workspace_receipt(original_receipt(), expected_version=0)
    api = OwnedAPI()
    with pytest.raises(WorkspaceConflict):
        await SandboxService(rebuilt_runtime(api), store=store).delete_workspace(record().workspace_id)
    assert await store.get_workspace(record().workspace_id) == record()
    assert await store.get_workspace_receipt(record().workspace_id, record().creation_token, expected_version=0) == original_receipt()
    assert api.calls == [] and api.threads == []


@pytest.mark.parametrize('operation', ['get_workspace', 'delete_workspace'])
@pytest.mark.parametrize('close_fails', [False, True])
async def test_repeated_cancellation_waits_for_sdk_cleanup_and_metadata_result(store, monkeypatch, operation, close_fails):
    """Cancellation cannot abandon a blocked SDK worker, its connection or the Service's deletion CAS."""
    api, available = await created_workspace(store)
    api.threads.clear()
    entered, release = threading.Event(), threading.Event()
    original = api.read_namespaced_persistent_volume_claim

    def blocked(*args, **kwargs):
        """Block one exact-name SDK read until both caller cancellation signals are delivered."""
        entered.set()
        assert release.wait(5)
        return original(*args, **kwargs)

    monkeypatch.setattr(api, 'read_namespaced_persistent_volume_claim', blocked)
    if close_fails:
        api.close_error = OSError('private close error')
    service = SandboxService(rebuilt_runtime(api), store=store)
    task = asyncio.create_task(getattr(service, operation)(available.workspace_id))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    error = (WorkspaceOperationFailed if operation == 'delete_workspace' else KubernetesConnectionFailed) if close_fails else asyncio.CancelledError
    with pytest.raises(error):
        await task
    assert len(api.threads) == 2 and api.threads[0] == api.threads[1] != threading.get_ident()
    saved = await store.get_workspace(available.workspace_id)
    if operation == 'get_workspace':
        assert saved == available and api.pvc is not None
    elif close_fails:
        assert saved.management_state == 'deleting' and api.pvc is None
        api.close_error = None
        await service.delete_workspace(available.workspace_id)
        assert await store.get_workspace(available.workspace_id) is None
    else:
        assert saved is None and api.pvc is None


async def test_primary_workspace_conflict_survives_client_close_failure(store):
    """A close failure cannot disguise the original UID conflict as an ordinary backend connection failure."""
    api, available = await created_workspace(store)
    api.pvc.metadata.uid = OTHER_UID
    api.close_error = RuntimeError('private close diagnostic')
    with pytest.raises(WorkspaceConflict):
        await SandboxService(rebuilt_runtime(api), store=store).get_workspace(available.workspace_id)
    assert api.pvc is not None and await store.get_workspace(available.workspace_id) == available


@pytest.mark.parametrize('after_commit', [False, True])
async def test_repeated_cancellation_during_metadata_deletion_preserves_commit_fact(store, monkeypatch, after_commit):
    """Once PVC cleanup succeeds, repeated cancellation still settles the original metadata/receipt CAS."""
    api, available = await created_workspace(store)
    entered, release = asyncio.Event(), asyncio.Event()
    original = store.delete_workspace

    async def blocked(*args, **kwargs):
        """Expose either side of the actual metadata commit without blocking its underlying SQLite worker."""
        if after_commit:
            await original(*args, **kwargs)
        entered.set()
        await release.wait()
        if not after_commit:
            await original(*args, **kwargs)

    monkeypatch.setattr(store, 'delete_workspace', blocked)
    task = asyncio.create_task(SandboxService(rebuilt_runtime(api), store=store).delete_workspace(available.workspace_id))
    try:
        await asyncio.wait_for(entered.wait(), 5)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and api.pvc is None
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert await store.get_workspace(available.workspace_id) is None
