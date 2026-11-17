"""Read-only active entry recovery using actual SQLite, Service and managed session ownership."""

from dataclasses import replace
from datetime import timedelta

import pytest

from control_plane.app import create_app
from control_plane.runtime.configuration import load_backend_configuration
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_backend_configuration import configure_kubernetes
from test_kubernetes_pending_recovery import recovery_harness, interrupted
from test_kubernetes_local_entry_manager import manager_harness
from test_kubernetes_persistent_workspace import persistence_clock
from test_kubernetes_proxy_lifecycle import proxy_clock
from test_kubernetes_query import SID, TXN, CREATED, EXPIRES


@pytest.mark.anyio
async def test_candidates_are_bounded_active_unexpired_and_stably_ordered(tmp_path):
    """Filtering in SQL must exclude pending/deleting/expired and retain renewed database deadlines."""
    from control_plane.core.models import SandboxLifecycleRecord
    store = SQLiteLifecycleStore(tmp_path / 'selection.sqlite3')
    await store.initialize()
    for number, state, deadline in [(3,'active',EXPIRES),(1,'active',EXPIRES),
                                    (2,'pending',EXPIRES),(4,'active',CREATED),
                                    (5,'deleting',EXPIRES)]:
        record = SandboxLifecycleRecord('sbx_'+f'{number:032x}', 'txn_'+f'{number:032x}', CREATED, deadline)
        await store.create(record)
        if state != 'pending':
            await store.activate(record.sandbox_id, record.transaction_id, expected_version=0)
        if state == 'deleting':
            await store.mark_deleting(record.sandbox_id, record.transaction_id, expected_version=1)
    rows = await store.list_active_entry_candidates(CREATED, limit=1)
    assert [row.sandbox_id for row in rows] == ['sbx_'+'0'*31+'1']
    assert [row.sandbox_id for row in await store.list_active_entry_candidates(CREATED, limit=10)] == [
        'sbx_'+'0'*31+'1', 'sbx_'+'0'*31+'3']


@pytest.mark.anyio
@pytest.mark.parametrize('persistent', [False, True])
async def test_active_original_restores_without_explicit_ids_or_metadata_changes(recovery_harness, tmp_path, persistent):
    """An active eight/nine-resource original gains one session without changing state, TTL or credentials."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path, persistent=persistent)
    record = await store.activate(SID, TXN, expected_version=0)
    app = create_app(h.backend, store=store, api_key='synthetic-restore-admin', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        result = app.state.kubernetes_entry_restore_result
        assert result.restored == 1 and result.error is None
        endpoint = await app.state.service.get_endpoint(SID)
        assert endpoint.url == 'http://127.0.0.1:43123'
        assert endpoint.access_token == credential.access_token, 'original credential changed'
        assert await store.get(SID) == record
        assert await store.get_credential(SID, TXN) == credential
        assert len(h.sessions) == 1
    assert h.manager.is_closed and h.sessions[0].closed.is_set()


@pytest.mark.parametrize('value', ['', '1', 'TRUE', ' true', 'false\n', 'yes'])
def test_restore_configuration_rejects_noncanonical_boolean(monkeypatch, value):
    """An explicit malformed switch must never silently disable or enable restoration."""
    configure_kubernetes(monkeypatch)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_ENTRY_RESTORE_ENABLED', value)
    with pytest.raises(ValueError):
        load_backend_configuration()


@pytest.mark.anyio
@pytest.mark.parametrize('fault,reason', [
    ('not-ready','sandbox_not_ready_or_conflict'), ('missing','sandbox_not_ready_or_conflict'),
    ('wrong-transaction','sandbox_not_ready_or_conflict'), ('credential-missing','sandbox_credential_mismatch'),
    ('credential-wrong','sandbox_credential_mismatch'), ('changed-before-resolve','active_record_changed'),
])
async def test_rejected_original_never_starts_a_session(recovery_harness, tmp_path, fault, reason, monkeypatch):
    """Unready/conflicting resources and wrong metadata or credentials must be rejected before the process factory."""
    from control_plane.core.models import SandboxAccessCredential
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)
    record = await store.activate(SID, TXN, expected_version=0)
    if fault == 'not-ready':
        h.api.startup['proxy'] = 'not-ready'
    elif fault == 'missing':
        h.api.proxy_service = None
    elif fault == 'wrong-transaction':
        h.api.pod.metadata.labels['io.agent-sandbox.transaction-id'] = 'txn_'+'f'*32
    elif fault.startswith('credential'):
        await store.delete(SID, TXN, expected_version=1)
        credential = None if fault == 'credential-missing' else SandboxAccessCredential(SID, TXN, 'R'*43)
        await store.create(replace(record, version=0, management_state='pending'), credential=credential)
        record = await store.activate(SID, TXN, expected_version=0)
    else:
        original = store.list_active_entry_candidates

        async def changing(now, *, limit):
            """Change the selected version before the first target resolution can proceed."""
            rows = await original(now, limit=limit)
            await store.update_expiration(SID, TXN, expected_version=1, expires_at=EXPIRES+timedelta(seconds=1))
            return rows

        monkeypatch.setattr(store, 'list_active_entry_candidates', changing)
    app = create_app(h.backend, store=store, api_key='synthetic-key', reaper_enabled=False,
                     kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        result = app.state.kubernetes_entry_restore_result
        assert result.items[0].reason == reason and result.restored == 0
        assert not h.sessions


@pytest.mark.anyio
async def test_renewed_database_deadline_overrides_original_resource_expiry(recovery_harness, tmp_path, monkeypatch):
    """A legitimate renewal remains restorable after the initial Pod annotation deadline has passed."""
    import control_plane.core.service as service_module
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    record = await store.update_expiration(SID, TXN, expected_version=1, expires_at=EXPIRES+timedelta(hours=1))
    monkeypatch.setattr(service_module, '_utc_now', lambda: EXPIRES+timedelta(minutes=1))
    app = create_app(h.backend, store=store, api_key='synthetic-key', reaper_enabled=False,
                     kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        assert app.state.kubernetes_entry_restore_result.restored == 1
        assert await store.get(SID) == record


@pytest.mark.anyio
@pytest.mark.parametrize('when', ['resolve', 'opening', 'after-open'])
async def test_record_changes_retire_only_new_original_session(recovery_harness, tmp_path, monkeypatch, when):
    """Every asynchronous boundary must reject a changed record and reclaim only this round's own late session."""
    h = recovery_harness
    store, record, credential = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    resolve = h.manager._resolver
    factory = h.manager._factory
    changed = False

    async def change():
        """Advance the metadata version exactly once without replacing the original resource identity."""
        nonlocal changed
        if not changed:
            changed = True
            await store.update_expiration(SID, TXN, expected_version=1, expires_at=EXPIRES+timedelta(seconds=1))

    async def resolving(backend, sid):
        """Model renewal after the target observation but before the credential check."""
        result = await resolve(backend, sid)
        if when == 'resolve':
            await change()
        return result

    def creating(backend, sid):
        """Model changes while the fake process is starting, or just after a successful open."""
        session = factory(backend, sid)
        opened = session.open

        async def open_session():
            """Keep real manager ownership while changing only the external process boundary."""
            if when == 'opening':
                await change()
            result = await opened()
            if when == 'after-open':
                await change()
            return result

        session.open = open_session
        return session

    monkeypatch.setattr(h.manager, '_resolver', resolving)
    monkeypatch.setattr(h.manager, '_factory', creating)
    app = create_app(h.backend, store=store, api_key='synthetic-key', reaper_enabled=False,
                     kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        assert app.state.kubernetes_entry_restore_result.items[0].reason == 'active_record_changed'
        assert h.manager.session_count == 0
        assert all(session.closed.is_set() for session in h.sessions)


@pytest.mark.anyio
async def test_explicit_ids_are_prioritized_deduplicated_and_not_reopened(recovery_harness, tmp_path):
    """Repeated explicit entries use a single slot and are excluded from automatic admission."""
    h = recovery_harness
    store, record, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    app = create_app(h.backend, store=store, api_key='synthetic-key', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_initial_sandbox_ids=[SID,SID], kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        assert app.state.kubernetes_entry_restore_result.items[0].reason == 'explicit_entry'
        assert len(h.sessions) == 1 and h.manager.session_count == 1


@pytest.mark.anyio
async def test_outage_is_fixed_and_metadata_failure_blocks_startup(recovery_harness, tmp_path, monkeypatch):
    """An unavailable backend may stop the batch; unreadable metadata must fail startup without exposing details."""
    from kubernetes import client
    from control_plane.storage.lifecycle_store import LifecycleStoreError
    h = recovery_harness
    store, record, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    h.api.faults[('list','pod')] = client.ApiException(status=403, reason='sensitive-diagnostic')
    app = create_app(h.backend, store=store, api_key='synthetic-key', reaper_enabled=False,
                     kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        result = app.state.kubernetes_entry_restore_result
        assert result.error == 'kubernetes_unavailable' and 'sensitive' not in repr(result)
    from test_kubernetes_proxy_lifecycle import runtime
    h.backend = runtime(h.api)
    h.manager = h.make_manager()

    async def fail(now, *, limit):
        """Model a genuine storage failure after initialization has acquired application ownership."""
        raise RuntimeError('sensitive-database-diagnostic')

    monkeypatch.setattr(store, 'list_active_entry_candidates', fail)
    app = create_app(h.backend, store=store, api_key='synthetic-key', reaper_enabled=False,
                     kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    with pytest.raises(LifecycleStoreError):
        async with app.router.lifespan_context(app):
            pytest.fail('metadata failure must prevent startup')
    assert h.manager.is_closed and app.state.kubernetes_entry_run_lock is None


@pytest.mark.parametrize('enabled', [None,False])
def test_injected_runtime_ignores_restore_environment(monkeypatch, tmp_path, enabled):
    """Explicit injection never rereads backend settings, even when their switch is malformed."""
    from test_kubernetes_proxy_lifecycle import runtime
    monkeypatch.setenv('AGENT_SANDBOX_K8S_ENTRY_RESTORE_ENABLED', 'invalid')
    app = create_app(runtime(None), database_path=tmp_path/'inert.sqlite3', api_key='test',
        reaper_enabled=False, kubernetes_entry_restore_enabled=enabled)
    assert app.state.kubernetes_entry_manager is None
    assert not (tmp_path/'inert.sqlite3').exists()


@pytest.mark.parametrize('value', [1,0,'true',[],{}])
def test_trusted_restore_argument_requires_boolean(value):
    """Truthy values cannot accidentally enable cluster/session operations."""
    from test_kubernetes_proxy_lifecycle import runtime
    with pytest.raises(ValueError):
        create_app(runtime(None), kubernetes_entry_restore_enabled=value)


@pytest.mark.parametrize('kind', ['docker','non-sqlite'])
def test_enabled_restore_requires_kubernetes_sqlite_ownership(tmp_path, kind):
    """Restoration cannot run without the application's exclusive SQLite ownership."""
    from lifecycle_fakes import FakeLifecycleStore
    from test_app import FakeRuntime
    from test_kubernetes_proxy_lifecycle import runtime
    with pytest.raises(ValueError):
        create_app(FakeRuntime() if kind=='docker' else runtime(None),
            store=SQLiteLifecycleStore(tmp_path/'x.sqlite3') if kind=='docker' else FakeLifecycleStore(),
            kubernetes_entry_restore_enabled=True)


@pytest.mark.anyio
async def test_budget_stops_new_candidates_without_opening(recovery_harness, tmp_path, monkeypatch):
    """A selection that consumes the budget must not admit even the first eligible process."""
    from control_plane.runtime.kubernetes import entry_restore
    h = recovery_harness
    store, record, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    ticks = iter([0.0, 31.0])
    monkeypatch.setattr(entry_restore, 'monotonic', lambda: next(ticks))
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        result = app.state.kubernetes_entry_restore_result
        assert result.selected == 1 and result.stopped == 'budget_exhausted' and result.items == ()
        assert not h.sessions


@pytest.mark.anyio
async def test_cancelled_startup_waits_for_late_session_and_lock_cleanup(recovery_harness, tmp_path):
    """Repeated cancellation never abandons a late process or releases RunLock while its close is pending."""
    import asyncio
    from control_plane.storage.run_lock import SQLiteRunLock, LifecycleRunLockBusy
    h = recovery_harness
    store, record, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    h.open_gate.clear()
    h.close_gate.clear()
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    context = app.router.lifespan_context(app)
    starting = asyncio.create_task(context.__aenter__())
    try:
        while not h.sessions:
            await asyncio.sleep(0)
        await h.sessions[0].started.wait()
        starting.cancel()
        starting.cancel()
        h.open_gate.set()
        await h.closing.wait()
        with pytest.raises(LifecycleRunLockBusy):
            SQLiteRunLock(store.database_path).acquire()
        assert not starting.done() and not h.sessions[0].closed.is_set()
        starting.cancel()
        h.close_gate.set()
        with pytest.raises(asyncio.CancelledError):
            await starting
        assert h.manager.is_closed and app.state.kubernetes_entry_run_lock is None
        lock = SQLiteRunLock(store.database_path)
        lock.acquire()
        lock.release()
    finally:
        h.open_gate.set()
        h.close_gate.set()
        if not starting.done():
            starting.cancel()
            await asyncio.gather(starting, return_exceptions=True)
        await h.manager.close_all()


@pytest.mark.anyio
async def test_default_disabled_never_selects_or_opens(recovery_harness, tmp_path, monkeypatch):
    """Existing startup keeps active resources unmanaged locally unless explicitly opted in."""
    h = recovery_harness
    store, _, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)

    async def forbidden(*args, **kwargs):
        """Detect any hidden selection while the feature is disabled."""
        pytest.fail('disabled restore selected candidates')

    monkeypatch.setattr(store, 'list_active_entry_candidates', forbidden)
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
                     kubernetes_entry_manager=h.manager)
    async with app.router.lifespan_context(app):
        assert app.state.kubernetes_entry_restore_result is None and not h.sessions


@pytest.mark.anyio
async def test_metadata_failure_retains_fixed_summary(recovery_harness, tmp_path, monkeypatch):
    """An aborted startup still exposes a bounded fixed summary for local diagnosis."""
    from control_plane.storage.lifecycle_store import LifecycleStoreError
    h = recovery_harness
    store, _, _ = await interrupted(h, tmp_path)

    async def fail(now, *, limit):
        """Fail metadata observation without exposing raw SQLite text."""
        raise RuntimeError('private-database-text')

    monkeypatch.setattr(store, 'list_active_entry_candidates', fail)
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    with pytest.raises(LifecycleStoreError):
        async with app.router.lifespan_context(app):
            pytest.fail('metadata failure did not stop startup')
    result = app.state.kubernetes_entry_restore_result
    assert result.error == 'sandbox_metadata_unavailable' and result.items == ()
    assert h.manager.is_closed


def test_trusted_argument_overrides_formal_environment_without_io(monkeypatch, tmp_path):
    """A trusted false override wins over environment enablement while assembly remains entirely inert."""
    configure_kubernetes(monkeypatch)
    monkeypatch.setenv('AGENT_SANDBOX_K8S_ENTRY_RESTORE_ENABLED', 'true')
    app = create_app(database_path=tmp_path/'formal.sqlite3', api_key='test',
                     kubernetes_entry_restore_enabled=False)
    assert not (tmp_path/'formal.sqlite3').exists()
    assert app.state.kubernetes_entry_restore_result is None
    configuration = load_backend_configuration()
    monkeypatch.setenv('AGENT_SANDBOX_K8S_ENTRY_RESTORE_ENABLED', 'false')
    assert configuration.entry_restore_enabled is True


@pytest.mark.anyio
async def test_capacity_preserves_existing_reservation_without_eviction(recovery_harness, tmp_path):
    """Automatic recovery shares atomic manager capacity with all existing owned startup reservations."""
    from test_kubernetes_local_entry import target_snapshot
    h = recovery_harness
    store, _, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    h.manager = h.make_manager(max_sessions=1)
    other = 'sbx_'+'f'*32
    target = target_snapshot(sandbox_id=other, proxy_pod_name='agent-sandbox-sbx-'+'f'*32+'-proxy',
                             proxy_service_name='agent-sandbox-sbx-'+'f'*32+'-proxy')
    h.targets[other] = target
    resolver = h.manager._resolver

    async def resolve(backend, sid):
        """Model an already owned unrelated entry without changing the selected sandbox's real verification."""
        return target if sid == other else await resolver(backend, sid)

    h.manager._resolver = resolve
    await h.manager.open(other)
    original_session = h.sessions[0]
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        result = app.state.kubernetes_entry_restore_result
        assert result.stopped == 'session_capacity_exceeded'
        assert h.manager.session_count == 1 and len(h.sessions) == 1
        assert original_session.is_active and not original_session.closed.is_set()


@pytest.mark.anyio
async def test_one_missing_item_does_not_prevent_next_original(recovery_harness, tmp_path):
    """One invalid candidate is isolated within the bounded original selection, rather than failing the round."""
    from control_plane.core.models import SandboxLifecycleRecord
    h = recovery_harness
    store, _, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    missing = 'sbx_'+'0'*32
    await store.create(SandboxLifecycleRecord(missing,'txn_'+'0'*32,CREATED,EXPIRES))
    await store.activate(missing,'txn_'+'0'*32,expected_version=0)
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        result = app.state.kubernetes_entry_restore_result
        assert result.selected == 2 and result.restored == 1 and result.skipped == 1
        assert [item.sandbox_id for item in result.items] == [missing,SID]
        assert len(h.sessions) == 1


@pytest.mark.anyio
async def test_batch_limit_does_not_scan_to_fill_slots(recovery_harness, tmp_path):
    """The single SQL limit bounds work even if the first candidate cannot restore a session."""
    from control_plane.core.models import SandboxLifecycleRecord
    h = recovery_harness
    store, _, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    missing = 'sbx_'+'0'*32
    await store.create(SandboxLifecycleRecord(missing,'txn_'+'0'*32,CREATED,EXPIRES))
    await store.activate(missing,'txn_'+'0'*32,expected_version=0)
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True, kubernetes_entry_restore_limit=1)
    async with app.router.lifespan_context(app):
        result = app.state.kubernetes_entry_restore_result
        assert result.selected == 1 and result.restored == 0 and not h.sessions
        assert len(result.items) == 1


@pytest.mark.anyio
async def test_uid_replaced_during_process_start_is_not_delivered(recovery_harness, tmp_path, monkeypatch):
    """The manager's fresh lookup rejects replacement of any original component and closes the new owned process."""
    from uuid import uuid4
    h = recovery_harness
    store, record, _ = await interrupted(h, tmp_path)
    record = await store.activate(SID, TXN, expected_version=0)
    factory = h.manager._factory

    def changed_factory(backend, sid):
        """Replace the original UID only after process creation has been admitted."""
        session = factory(backend, sid)
        original = session.open

        async def opening():
            """Finish the process but model a replaced Kubernetes proxy before lookup returns."""
            await original()
            h.api.proxy.metadata.uid = str(uuid4())
            return session

        session.open = opening
        return session

    monkeypatch.setattr(h.manager, '_factory', changed_factory)
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        result = app.state.kubernetes_entry_restore_result
        assert result.restored == 0 and h.manager.session_count == 0
        assert h.sessions[0].closed.is_set() and await store.get(SID) == record


@pytest.mark.anyio
async def test_failed_close_keeps_handle_and_runlock(recovery_harness, tmp_path, monkeypatch):
    """A failed recovery cleanup must not lose its original process reservation or unlock the database."""
    from control_plane.runtime.kubernetes.local_entry import LocalEntryError
    from control_plane.storage.run_lock import SQLiteRunLock, LifecycleRunLockBusy
    h = recovery_harness
    store, _, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    factory = h.manager._factory

    def changing_factory(backend, sid):
        """Make the metadata ineligible at the startup boundary, then fail exact session closure."""
        session = factory(backend, sid)
        original = session.open

        async def opening():
            """Renew the record while its original local process is being established."""
            await original()
            await store.update_expiration(SID,TXN,expected_version=1,expires_at=EXPIRES+timedelta(seconds=1))
            h.close_error = True
            return session

        session.open = opening
        return session

    monkeypatch.setattr(h.manager, '_factory', changing_factory)
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    try:
        with pytest.raises(LocalEntryError):
            async with app.router.lifespan_context(app):
                pytest.fail('unconfirmed close must prevent startup')
        assert h.manager.session_count == 1 and not h.manager.is_closed
        with pytest.raises(LifecycleRunLockBusy):
            SQLiteRunLock(store.database_path).acquire()
        assert app.state.kubernetes_entry_run_lock is not None
    finally:
        h.close_error = False
        await h.manager.close_all()
        app.state.kubernetes_entry_run_lock.release()


@pytest.mark.anyio
async def test_changed_record_does_not_close_preexisting_session(recovery_harness, tmp_path, monkeypatch):
    """Recovery cannot retire another owner's session, even for an identical original target."""
    h = recovery_harness
    store, _, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    await h.manager.open(SID)
    original_session = h.sessions[0]
    opened = h.manager.open

    async def changing(*args, **kwargs):
        """Model a database change while automatic recovery reuses an already held session."""
        result = await opened(*args, **kwargs)
        await store.update_expiration(SID,TXN,expected_version=1,expires_at=EXPIRES+timedelta(seconds=1))
        return result

    monkeypatch.setattr(h.manager, 'open', changing)
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        assert app.state.kubernetes_entry_restore_result.restored == 0
        assert original_session.is_active and not original_session.closed.is_set()
        assert h.manager.session_count == 1 and len(h.sessions) == 1


@pytest.mark.anyio
async def test_independent_pvc_receipt_is_compared_with_database(recovery_harness, tmp_path, monkeypatch):
    """Valid cluster binding cannot authorize an entry when its original PVC UID disagrees with SQLite evidence."""
    from uuid import uuid4
    from control_plane.core.service import SandboxService
    from control_plane.core.models import SandboxCreateOptions
    from test_kubernetes_workspace_binding import BoundClient, bound_runtime
    h = recovery_harness
    h.api = BoundClient(existing=False)
    h.backend = bound_runtime(h.api)
    store = SQLiteLifecycleStore(tmp_path/'bound.sqlite3')
    await store.initialize()
    service = SandboxService(h.backend, store, clock=lambda: CREATED)
    workspace = await service.create_workspace()
    made = await service.create(SandboxCreateOptions(workspace_id=workspace.workspace_id))
    get_receipt = store.get_workspace_receipt

    async def altered(*args, **kwargs):
        """Model a contradictory trusted record observation without modifying immutable SQLite rows."""
        receipt = await get_receipt(*args, **kwargs)
        return replace(receipt, pvc_uid=str(uuid4()))

    monkeypatch.setattr(store, 'get_workspace_receipt', altered)
    h.manager = h.make_manager()
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=False,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True)
    async with app.router.lifespan_context(app):
        result = app.state.kubernetes_entry_restore_result
        assert result.items[0].reason == 'workspace_identity_mismatch'
        assert not h.sessions and await store.get(made.sandbox_id) is not None


@pytest.mark.anyio
async def test_entry_restore_precedes_pending_round_and_reaper(recovery_harness, tmp_path, monkeypatch):
    """The optional existing pending recovery and request-serving phase observe completed active preparation."""
    import asyncio
    import control_plane.app as app_module
    from control_plane.core.service import SandboxService
    h = recovery_harness
    store, _, _ = await interrupted(h, tmp_path)
    await store.activate(SID, TXN, expected_version=0)
    events = []
    pending = SandboxService.recover_pending_once

    async def observe_pending(service, *, limit):
        """Execute the actual empty pending selection only after automatic entry preparation completed."""
        assert h.manager.session_count == 1
        events.append('pending')
        return await pending(service, limit=limit)

    async def reaper(service, **kwargs):
        """Keep only the unrelated periodic scheduler inert while checking startup sequencing."""
        assert events == ['pending'] and h.manager.session_count == 1
        events.append('reaper')
        await asyncio.Event().wait()

    monkeypatch.setattr(SandboxService, 'recover_pending_once', observe_pending)
    monkeypatch.setattr(app_module, 'run_reaper', reaper)
    app = create_app(h.backend, store=store, api_key='test', reaper_enabled=True,
        kubernetes_entry_manager=h.manager, kubernetes_entry_restore_enabled=True, pending_recovery_enabled=True)
    async with app.router.lifespan_context(app):
        assert events == ['pending']
        assert app.state.kubernetes_entry_restore_result.restored == 1
        await asyncio.sleep(0)
        assert events == ['pending','reaper']


@pytest.mark.parametrize('limit', [True,0,-1,1001,1.5,'1'])
def test_candidate_limit_validation_does_not_create_storage(tmp_path, limit):
    """Invalid startup batches fail during inert assembly instead of reaching SQL or the backend."""
    from test_kubernetes_proxy_lifecycle import runtime
    with pytest.raises(ValueError):
        create_app(runtime(None), database_path=tmp_path/'invalid.sqlite3', kubernetes_entry_restore_limit=limit)
    assert not (tmp_path/'invalid.sqlite3').exists()


@pytest.mark.parametrize('budget', [True,0,-1,301,float('inf'),float('nan'),'30'])
def test_budget_validation_does_not_create_storage(tmp_path, budget):
    """Nonfinite, unbounded or nonnumeric budgets cannot admit startup work."""
    from test_kubernetes_proxy_lifecycle import runtime
    with pytest.raises(ValueError):
        create_app(runtime(None), database_path=tmp_path/'invalid.sqlite3', kubernetes_entry_restore_budget_seconds=budget)
    assert not (tmp_path/'invalid.sqlite3').exists()
