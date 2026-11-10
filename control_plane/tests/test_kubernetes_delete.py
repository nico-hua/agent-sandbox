"""Offline exact-identity Kubernetes deletion and bounded worker cleanup tests."""

import asyncio
import copy
import threading
from types import SimpleNamespace

import pytest
from kubernetes import client

from control_plane.core.errors import (
    KubernetesConfigurationError, KubernetesConnectionFailed, KubernetesOperationUnsupported,
    KubernetesPermissionDenied, KubernetesTimeout, SandboxDeleteConflict, SandboxDeleteFailed,
    SandboxNotFound,
)
from control_plane.core.models import SandboxAccessCredential, SandboxLifecycleRecord
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_query import CREATED, EXPIRES, LABELS, NAMESPACE, POD_NAME, SID, TXN, UID, primary_pod, query_runtime
from test_kubernetes_runtime import FakeClient


class DeleteClient(FakeClient):
    """Model API object persistence separately from the DELETE response and discovery snapshots."""

    def __init__(self):
        """Initialize an owned Pod and controllable transport, race and deletion faults."""
        super().__init__()
        self.pod = primary_pod()
        self.inventory = None
        self.continuation = None
        self.list_error = self.read_error = self.delete_error = self.namespace_error = None
        self.delete_calls, self.read_calls, self.namespace_calls = [], [], []
        self.remove_on_delete = True
        self.replace_after_delete = None
        self.remaining_reads = 0
        self.delete_entered = self.delete_release = None

    def list_namespaced_pod(self, namespace, **kwargs):
        """Expose all same-sandbox roles unless a scripted inventory overrides object presence."""
        self.calls.append((namespace, kwargs))
        self.events.append(("list", threading.get_ident()))
        if self.list_error is not None:
            raise self.list_error
        items = self.inventory if self.inventory is not None else ([] if self.pod is None else [self.pod])
        return client.V1PodList(items=copy.deepcopy(items), metadata=client.V1ListMeta(_continue=self.continuation))

    def read_namespaced_pod(self, name, namespace, **kwargs):
        """Return exact-name facts, including eventual disappearance after successful DELETE."""
        self.read_calls.append((name, namespace, kwargs))
        self.events.append(("read", threading.get_ident()))
        if self.read_error is not None:
            raise self.read_error
        if self.delete_calls and self.remaining_reads:
            self.remaining_reads -= 1
            result = copy.deepcopy(self.pod)
            if not self.remaining_reads:
                self.pod = None
            return result
        if self.pod is None or name != self.pod.metadata.name:
            raise client.ApiException(status=404, reason="private API response")
        return copy.deepcopy(self.pod)

    def delete_namespaced_pod(self, name, namespace, **kwargs):
        """Record preconditions and optionally commit deletion before losing the response."""
        self.delete_calls.append((name, namespace, kwargs))
        self.events.append(("delete", threading.get_ident()))
        if self.delete_entered is not None:
            self.delete_entered.set()
        if self.delete_release is not None and not self.delete_release.wait(3):
            raise AssertionError("test did not release DELETE")
        if self.remove_on_delete and not self.remaining_reads:
            self.pod = None
        if self.replace_after_delete is not None:
            self.pod = copy.deepcopy(self.replace_after_delete)
        if self.delete_error is not None:
            raise self.delete_error
        return client.V1Status(status="Success")

    def read_namespace(self, name, **kwargs):
        """Prove namespace existence so its absence never counts as successful Pod deletion."""
        self.namespace_calls.append((name, kwargs))
        self.events.append(("namespace", threading.get_ident()))
        if self.namespace_error is not None:
            raise self.namespace_error
        return client.V1Namespace(metadata=client.V1ObjectMeta(name=name))


@pytest.fixture
def deletion_clock(monkeypatch):
    """Advance only worker polling time without waiting through the production deletion budget."""
    from control_plane.runtime.kubernetes import pod_delete
    clock = SimpleNamespace(now=0.0, waits=[])

    def now():
        """Return deterministic monotonic time without altering the event loop's clock."""
        return clock.now

    def wait(seconds):
        """Record bounded intervals and advance the worker's monotonic budget."""
        clock.waits.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(pod_delete, "monotonic", now)
    monkeypatch.setattr(pod_delete, "sleep", wait)
    monkeypatch.setattr(pod_delete, "DELETE_TIMEOUT_SECONDS", 0.3)
    return clock


@pytest.mark.anyio
async def test_delete_uses_uid_and_resource_version_then_confirms_absence():
    """DELETE is followed by fresh absence and namespace verification before client close."""
    fake = DeleteClient()
    await query_runtime(fake).delete(SID, expected_transaction_id=TXN)
    name, namespace, kwargs = fake.delete_calls[0]
    assert (name, namespace) == (POD_NAME, NAMESPACE)
    options = kwargs["body"]
    assert options.preconditions.uid == UID and options.preconditions.resource_version == "100"
    assert options.grace_period_seconds is None and options.propagation_policy is None
    assert len(fake.delete_calls) == 1 and len(fake.read_calls) >= 2 and fake.namespace_calls
    assert fake.closed.is_set() and len({thread for _, thread in fake.events}) == 1
    assert all(call[1]["limit"] in (2, 3) and call[1]["watch"] is False for call in fake.calls)
    selector = fake.calls[0][1]["label_selector"]
    assert "resource=" not in selector and "transaction-id=" not in selector
    assert all(f"io.agent-sandbox.{key}={LABELS['io.agent-sandbox.' + key]}" in selector
               for key in ("managed", "project", "sandbox-id"))


@pytest.mark.anyio
@pytest.mark.parametrize("kwargs,expected", [
    ({"sandbox_id": "sbx_bad"}, SandboxNotFound),
    ({"expected_transaction_id": ""}, SandboxDeleteConflict),
    ({"expected_transaction_id": "txn_BAD"}, SandboxDeleteConflict),
    ({"expected_transaction_id": 1}, SandboxDeleteConflict),
    ({"delete_workspace": 1}, ValueError),
    ({"delete_workspace": None}, ValueError),
    ({"expected_workspace": object()}, KubernetesOperationUnsupported),
])
async def test_invalid_and_unsupported_inputs_never_acquire_client(kwargs, expected):
    """Reject malformed identities or unsupported storage operations before cluster access."""
    def forbidden(**_):
        """Fail if invalid input reaches client assembly."""
        pytest.fail("client acquired before validation")
    with pytest.raises(expected):
        await query_runtime(None, factory=forbidden).delete(**{"sandbox_id": SID, **kwargs})


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["uid", "namespace", "name", "transaction", "owner", "resource-version", "blank-version", "binding", "unknown-marker", "extra-container", "volume"])
async def test_conflicting_identity_or_layout_never_deletes(fault):
    """Ownership, exact identity, resource version and supported layout precede mutation."""
    fake = DeleteClient()
    metadata = fake.pod.metadata
    if fault == "uid":
        metadata.uid = "invalid"
    elif fault == "namespace":
        metadata.namespace = "other"
    elif fault == "name":
        metadata.name = "other"
    elif fault == "transaction":
        metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    elif fault == "owner":
        metadata.labels["io.agent-sandbox.project"] = "other"
    elif fault == "resource-version":
        metadata.resource_version = None
    elif fault == "blank-version":
        metadata.resource_version = " "
    elif fault == "binding":
        metadata.annotations["io.agent-sandbox.workspace-id"] = "wsp_" + "1" * 32
    elif fault == "unknown-marker":
        metadata.annotations["io.agent-sandbox.network-version"] = "unsupported"
    elif fault == "extra-container":
        fake.pod.spec.containers.append(client.V1Container(name="other", image="unused"))
    else:
        fake.pod.spec.volumes = [client.V1Volume(name="data", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(claim_name="data"))]
    with pytest.raises(SandboxDeleteConflict):
        await query_runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert not fake.delete_calls and fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["duplicate", "role", "paged", "foreign-name", "read-replaced"])
async def test_discovery_and_exact_read_cannot_hide_ambiguous_resources(fault):
    """Extra roles, paginated inventories and foreign/replaced exact-name targets prevent deletion."""
    fake = DeleteClient()
    if fault in {"duplicate", "role"}:
        extra = copy.deepcopy(fake.pod)
        if fault == "role":
            extra.metadata.labels["io.agent-sandbox.resource"] = "egress"
        fake.inventory = [fake.pod, extra]
    elif fault == "paged":
        fake.continuation = "another-page"
    elif fault == "foreign-name":
        fake.inventory = []
        fake.pod.metadata.labels["io.agent-sandbox.managed"] = "false"
    else:
        fake.inventory = [copy.deepcopy(fake.pod)]
        fake.pod.metadata.uid = "ce667ab6-3f02-40ba-9b29-b1967503a3f0"
    with pytest.raises(SandboxDeleteConflict):
        await query_runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert not fake.delete_calls


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["Pending", "Failed", "Succeeded", "bad-readiness", "bad-business-time"])
async def test_delete_does_not_depend_on_health_or_business_time(state):
    """Cleanup remains possible when state mapping or original times cannot produce a query result."""
    fake = DeleteClient()
    if state == "bad-readiness":
        fake.pod.spec.containers[0].readiness_probe = None
        fake.pod.status.conditions = None
    elif state == "bad-business-time":
        fake.pod.metadata.annotations = {}
    else:
        fake.pod.status.phase = state
        fake.pod.status.container_statuses = None
    await query_runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert len(fake.delete_calls) == 1 and fake.pod is None


@pytest.mark.anyio
async def test_original_absence_and_repeated_delete_are_idempotent():
    """Repeated calls verify exact name and all same-sandbox roles without another DELETE."""
    fake = DeleteClient()
    runtime = query_runtime(fake)
    await runtime.delete(SID)
    await runtime.delete(SID)
    assert len(fake.delete_calls) == 1 and len(fake.namespace_calls) >= 2


@pytest.mark.anyio
@pytest.mark.parametrize("already_deleting", [False, True])
async def test_delete_waits_for_disappearance_without_repeating_delete(deletion_clock, already_deleting):
    """A DELETE response or deletion timestamp cannot replace API object disappearance."""
    fake = DeleteClient()
    fake.remove_on_delete = False
    if already_deleting:
        fake.pod.metadata.deletion_timestamp = CREATED
    with pytest.raises(SandboxDeleteFailed):
        await query_runtime(fake).delete(SID, expected_transaction_id=TXN)
    assert len(fake.delete_calls) == (0 if already_deleting else 1)
    assert deletion_clock.waits and 0.3 <= deletion_clock.now < 0.5 and fake.closed.is_set()


@pytest.mark.anyio
async def test_successful_response_does_not_skip_eventual_deletion_polling(deletion_clock):
    """The worker waits between exact reads until eventual disappearance is confirmed."""
    fake = DeleteClient()
    fake.remaining_reads = 2
    await query_runtime(fake).delete(SID)
    assert len(fake.delete_calls) == 1 and deletion_clock.waits and fake.namespace_calls


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["uid", "transaction", "owner", "binding"])
async def test_wait_does_not_adopt_replacement_or_modified_identity(fault):
    """A changed target stops confirmation without deleting the new observation."""
    fake = DeleteClient()
    replacement = copy.deepcopy(fake.pod)
    if fault == "uid":
        replacement.metadata.uid = "ce667ab6-3f02-40ba-9b29-b1967503a3f0"
    elif fault == "transaction":
        replacement.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    elif fault == "owner":
        replacement.metadata.labels["io.agent-sandbox.managed"] = "false"
    else:
        replacement.metadata.annotations["io.agent-sandbox.workspace-id"] = "unsupported"
    fake.replace_after_delete = replacement
    with pytest.raises(SandboxDeleteConflict):
        await query_runtime(fake).delete(SID)
    assert len(fake.delete_calls) == 1 and fake.pod.metadata.uid == replacement.metadata.uid


@pytest.mark.anyio
@pytest.mark.parametrize("committed", [False, True])
@pytest.mark.parametrize("error", [TimeoutError("private deadline"), ConnectionError("private API")])
async def test_uncertain_delete_is_confirmed_without_retrying(deletion_clock, committed, error):
    """A lost DELETE response succeeds only after disappearance, otherwise bounded failure retains evidence."""
    fake = DeleteClient()
    fake.remove_on_delete, fake.delete_error = committed, error
    if committed:
        await query_runtime(fake).delete(SID)
    else:
        with pytest.raises(SandboxDeleteFailed) as captured:
            await query_runtime(fake).delete(SID)
        assert "private" not in str(captured.value)
    assert len(fake.delete_calls) == 1 and fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("stage,status,expected", [
    ("list", 403, KubernetesPermissionDenied), ("read", 403, KubernetesPermissionDenied),
    ("delete", 403, KubernetesPermissionDenied), ("namespace", 403, KubernetesPermissionDenied),
    ("list", 404, KubernetesConfigurationError), ("namespace", 404, KubernetesConfigurationError),
    ("delete", 409, SandboxDeleteConflict),
])
async def test_api_errors_are_not_success_or_reclassified_as_transport(stage, status, expected):
    """Permissions, namespace absence and DELETE precondition conflicts retain stable classifications."""
    fake = DeleteClient()
    setattr(fake, stage + "_error", client.ApiException(status=status, reason="private API credentials"))
    if stage == "namespace":
        fake.pod = None
    if stage == "delete":
        fake.remove_on_delete = False
    with pytest.raises(expected) as captured:
        await query_runtime(fake).delete(SID)
    assert "private" not in str(captured.value) and fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("error,expected", [(TimeoutError("private"), KubernetesTimeout), (ConnectionError("private"), KubernetesConnectionFailed)])
async def test_before_mutation_transport_errors_remain_typed(error, expected):
    """Discovery transport failures cannot be mistaken for deleted objects."""
    fake = DeleteClient()
    fake.list_error, fake.close_error = error, RuntimeError("private cleanup")
    with pytest.raises(expected) as captured:
        await query_runtime(fake).delete(SID)
    assert "private" not in str(captured.value) and not fake.delete_calls and fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("worker_failure", [False, True])
async def test_repeated_cancellation_waits_for_worker_and_close(worker_failure):
    """Cancellation cannot release a running worker or lose its eventual failure."""
    fake = DeleteClient()
    fake.delete_entered, fake.delete_release = threading.Event(), threading.Event()
    if worker_failure:
        fake.delete_error = client.ApiException(status=409, reason="private precondition")
        fake.remove_on_delete = False
        fake.close_error = RuntimeError("private close")
    operation = asyncio.create_task(query_runtime(fake).delete(SID))
    try:
        assert await asyncio.to_thread(fake.delete_entered.wait, 1)
        operation.cancel()
        await asyncio.sleep(0)
        operation.cancel()
        await asyncio.sleep(0)
        assert not operation.done() and not fake.closed.is_set()
    finally:
        fake.delete_release.set()
    with pytest.raises(SandboxDeleteConflict if worker_failure else asyncio.CancelledError):
        await operation
    assert fake.closed.is_set() and len({thread for _, thread in fake.events}) == 1


@pytest.mark.anyio
async def test_close_failure_after_confirmation_remains_failure():
    """Confirmed deletion does not hide a failed client resource close."""
    fake = DeleteClient()
    fake.close_error = RuntimeError("private close")
    with pytest.raises(KubernetesConnectionFailed):
        await query_runtime(fake).delete(SID)
    assert fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("status", [404, 500, 504])
async def test_uncertain_api_delete_response_requires_fresh_confirmation(status):
    """An API error response is neither retried nor trusted as proof that deletion did not commit."""
    fake = DeleteClient()
    fake.delete_error = client.ApiException(status=status, reason="private response")
    await query_runtime(fake).delete(SID)
    assert len(fake.delete_calls) == 1 and fake.namespace_calls and fake.closed.is_set()


@pytest.mark.anyio
async def test_fresh_resource_version_is_used_without_refreshing_on_conflict():
    """Status changes after LIST use the exact GET version; DELETE 409 does not acquire a newer version."""
    fake = DeleteClient()
    fake.inventory = [copy.deepcopy(fake.pod)]
    fake.pod.metadata.resource_version = "101"
    fake.remove_on_delete = False
    fake.delete_error = client.ApiException(status=409)
    with pytest.raises(SandboxDeleteConflict):
        await query_runtime(fake).delete(SID)
    assert len(fake.delete_calls) == 1 and len(fake.read_calls) >= 1
    assert fake.delete_calls[0][2]["body"].preconditions.resource_version == "101"


@pytest.mark.anyio
async def test_confirmation_transport_failure_never_cleans_metadata(deletion_clock):
    """After submitting DELETE, unavailable observations exhaust the same bounded budget with no new mutation."""
    fake = DeleteClient()
    original = fake.delete_namespaced_pod

    def delete_then_lose_reads(*args, **kwargs):
        """Make subsequent GET results unavailable even if the API accepted deletion."""
        result = original(*args, **kwargs)
        fake.read_error = TimeoutError("private confirmation response")
        return result

    fake.delete_namespaced_pod = delete_then_lose_reads
    with pytest.raises(SandboxDeleteFailed):
        await query_runtime(fake).delete(SID)
    assert len(fake.delete_calls) == 1 and deletion_clock.waits and fake.closed.is_set()


@pytest.mark.anyio
async def test_delete_failure_preserves_lifecycle_record(tmp_path, deletion_clock):
    """Service removes no lifecycle row when Runtime cannot confirm original Pod removal."""
    store = SQLiteLifecycleStore(tmp_path / "delete.sqlite3")
    await store.initialize()
    credential = SandboxAccessCredential(SID, TXN, "A" * 43)
    await store.create(SandboxLifecycleRecord(SID, TXN, CREATED, EXPIRES), credential=credential)
    fake = DeleteClient()
    fake.remove_on_delete = False
    with pytest.raises(SandboxDeleteFailed):
        await SandboxService(query_runtime(fake), store).delete(SID)
    remaining = await store.get(SID)
    assert remaining.transaction_id == TXN and remaining.management_state == "deleting"
    assert await store.get_credential(SID, TXN) == credential
    fake.remove_on_delete = True
    await SandboxService(query_runtime(fake), store).delete(SID)
    assert await store.get(SID) is None and await store.get_credential(SID, TXN) is None
