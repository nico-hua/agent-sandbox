"""Offline one-shot PVC operations, original UID evidence and consumer-safe cleanup."""

import copy
from dataclasses import replace
from types import SimpleNamespace

import pytest
from kubernetes import client

from control_plane.core.errors import (
    CreationResourceState, KubernetesConfigurationError, KubernetesConnectionFailed,
    KubernetesPermissionDenied, KubernetesTimeout, WorkspaceConflict, WorkspaceOperationFailed,
)
from test_kubernetes_workspace_spec import module, record, live_pvc, UID, OTHER_UID, NAMESPACE, SC, WID


class FakeAPI:
    """Model real one-shot commits and lossy responses without owning a client or hiding consumer roles."""

    def __init__(self):
        """Keep independent resource state, fault points and request evidence for each test."""
        self.pvc = None
        self.calls = []
        self.pods = []
        self.errors = {}
        self.post_error = None
        self.reject = None
        self.post_response = None
        self.keep_on_delete = False
        self.delete_error = None
        self.pvc_page = None
        self.pod_page = None
        self.extra_candidates = []
        self.read_sequence = []
        self.continuation = None
        self.remaining_count = None
        self.replacement_on_delete = False

    def _error(self, key):
        """Inject API-shaped failures only at the requested external boundary."""
        if key in self.errors:
            raise self.errors[key]

    def read_namespace(self, name, **kwargs):
        """Confirm the exact namespace or reject without conflating it with PVC absence."""
        self.calls.append(("namespace", name, kwargs))
        self._error("namespace")
        return client.V1Namespace(metadata=client.V1ObjectMeta(name=name))

    def list_namespaced_persistent_volume_claim(self, namespace, **kwargs):
        """Expose a complete page or explicit conflicting/truncated inventory."""
        self.calls.append(("list", namespace, kwargs))
        self._error("list")
        if self.pvc_page is not None:
            return self.pvc_page
        items = self.extra_candidates + ([] if self.pvc is None else [self.pvc])
        return client.V1PersistentVolumeClaimList(api_version="v1", kind="PersistentVolumeClaimList", items=copy.deepcopy(items), metadata=client.V1ListMeta())

    def read_namespaced_persistent_volume_claim(self, name, namespace, **kwargs):
        """Use exact-name observations, optionally hiding a just-submitted commit for a bounded interval."""
        self.calls.append(("read", name, namespace, kwargs))
        self._error("read")
        result = self.read_sequence.pop(0) if self.read_sequence else self.pvc
        if isinstance(result, Exception):
            raise result
        if result is None:
            raise client.ApiException(status=404)
        return copy.deepcopy(result)

    def create_namespaced_persistent_volume_claim(self, namespace, body, **kwargs):
        """Accept exactly one PVC before optionally losing its response or returning a corrupted observation."""
        self.calls.append(("post", namespace, copy.deepcopy(body), kwargs))
        if self.reject is not None:
            raise self.reject
        self.pvc = copy.deepcopy(body)
        self.pvc.metadata.uid, self.pvc.metadata.resource_version = UID, "7"
        self.pvc.status = client.V1PersistentVolumeClaimStatus(phase="Pending")
        if self.post_error is not None:
            raise self.post_error
        return copy.deepcopy(self.pvc if self.post_response is None else self.post_response)

    def list_namespaced_pod(self, namespace, **kwargs):
        """Include foreign and terminal consumers in a bounded namespace-wide page."""
        self.calls.append(("consumers", namespace, kwargs))
        self._error("consumers")
        return self.pod_page or client.V1PodList(items=copy.deepcopy(self.pods), metadata=client.V1ListMeta(
            _continue=self.continuation, remaining_item_count=self.remaining_count))

    def delete_namespaced_persistent_volume_claim(self, name, namespace, body, **kwargs):
        """Apply UID/RV-protected deletion before a possible lost response, never deleting replacements."""
        self.calls.append(("delete", name, namespace, body, kwargs))
        if self.pvc is not None:
            assert body.preconditions.uid == self.pvc.metadata.uid
            assert body.preconditions.resource_version == self.pvc.metadata.resource_version
        if self.replacement_on_delete:
            self.pvc.metadata.uid = OTHER_UID
        elif not self.keep_on_delete:
            self.pvc = None
        if self.delete_error is not None:
            raise self.delete_error
        return client.V1Status(status="Success")


@pytest.fixture
def clock(monkeypatch):
    """Advance only module polling deadlines, making timeout tests finite without real sleeps."""
    subject = module()
    clock = SimpleNamespace(now=0.0, waits=[])

    def now():
        """Read deterministic monotonic worker time."""
        return clock.now

    def wait(seconds):
        """Advance time by the requested bounded polling interval."""
        clock.waits.append(seconds)
        clock.now += seconds

    monkeypatch.setattr(subject, "monotonic", now, raising=False)
    monkeypatch.setattr(subject, "sleep", wait, raising=False)
    for key in ("OPERATION_TIMEOUT_SECONDS", "RECONCILE_TIMEOUT_SECONDS", "DELETE_TIMEOUT_SECONDS"):
        monkeypatch.setattr(subject, key, 0.4, raising=False)
    return clock


def resource(api, **changes):
    """Use the actual resource helper with a borrowed offline transport and existing workspace identity."""
    return module().WorkspaceResource(api, record(**changes), namespace=NAMESPACE, storage_class=SC)


def mutation_count(api, verb):
    """Count only actual external write calls, independently of internal object fields."""
    return sum(call[0] == verb for call in api.calls)


def consumer(phase="Running", *, terminating=False, claim=None):
    """Build a non-platform Pod referencing the workspace without relying on platform labels or health."""
    return client.V1Pod(metadata=client.V1ObjectMeta(name="foreign-consumer", namespace=NAMESPACE,
        deletion_timestamp="2026-11-15T00:00:00Z" if terminating else None),
        spec=client.V1PodSpec(containers=[client.V1Container(name="task")], volumes=[client.V1Volume(
            name="data", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=claim or "agent-sandbox-wsp-0123456789abcdef0123456789abcdef"))]),
        status=client.V1PodStatus(phase=phase))


def test_pending_creation_fixes_uid_without_waiting_or_resubmitting(clock):
    """A valid Pending result pins original UID; repeat create cannot write or erase receipts."""
    api = FakeAPI()
    item = resource(api)
    assert not api.calls and item.uid is None and not item.submitted
    observed = item.create()
    assert observed.uid == UID and observed.original_uid_verified is True and item.uid == UID
    assert item.submitted and api.pvc.status.phase == "Pending"
    assert item.creation_failure.resource_state == CreationResourceState.REMAINING
    with pytest.raises(WorkspaceOperationFailed) as caught:
        item.create()
    assert caught.value.creation_failure.resource_state == CreationResourceState.REMAINING
    assert mutation_count(api, "post") == 1 and item.uid == UID and item.submitted
    inventory = next(call for call in api.calls if call[0] == "list")
    assert inventory[1] == NAMESPACE and inventory[2]["limit"] == 2 and inventory[2]["watch"] is False
    selector = inventory[2]["label_selector"]
    assert WID in selector and "resource=" not in selector and "workspace-token=" not in selector
    for call in api.calls:
        timeout = call[-1]["_request_timeout"]
        assert 0 < timeout[0] <= 0.5 and 0 < timeout[1] <= 0.5


@pytest.mark.parametrize("fault", ["existing", "foreign-name", "list-only", "duplicate", "truncated", "remaining", "role"])
def test_preflight_never_adopts_or_overwrites_existing_or_ambiguous_resources(clock, fault):
    """Any exact-name collision or inventory mismatch blocks POST, regardless of matching ownership labels."""
    api = FakeAPI()
    existing = live_pvc()
    if fault == "list-only":
        api.extra_candidates = [existing]
    else:
        api.pvc = existing
    if fault == "foreign-name":
        api.pvc.metadata.labels = {}
        api.pvc_page = client.V1PersistentVolumeClaimList(api_version="v1", kind="PersistentVolumeClaimList", items=[], metadata=client.V1ListMeta())
    elif fault == "duplicate":
        api.extra_candidates = [existing]
    elif fault == "role":
        api.pvc.metadata.labels["io.agent-sandbox.resource"] = "private-workspace"
    elif fault in {"truncated", "remaining"}:
        api.pvc_page = client.V1PersistentVolumeClaimList(api_version="v1", kind="PersistentVolumeClaimList", items=[], metadata=client.V1ListMeta(
            _continue="more" if fault == "truncated" else None, remaining_item_count=1 if fault == "remaining" else None))
    item = resource(api)
    with pytest.raises(WorkspaceConflict) as caught:
        item.create()
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED
    assert mutation_count(api, "post") == mutation_count(api, "delete") == 0
    assert item.uid is None and not item.submitted


@pytest.mark.parametrize("status,error", [(400, WorkspaceOperationFailed), (401, KubernetesPermissionDenied),
    (403, KubernetesPermissionDenied), (404, KubernetesConfigurationError), (409, WorkspaceConflict),
    (422, WorkspaceOperationFailed)])
def test_definitive_post_rejection_preserves_unchanged_evidence(clock, status, error):
    """A rejected POST is classified by fixed category and is not retried or disguised as uncertain success."""
    api = FakeAPI()
    api.reject = client.ApiException(status=status, reason="private response content")
    item = resource(api)
    with pytest.raises(error) as caught:
        item.create()
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED
    assert item.uid is None and not item.submitted and mutation_count(api, "post") == 1
    assert "private response" not in str(caught.value)


@pytest.mark.parametrize("fault", ["response-lost", "no-commit", "read-fails", "invalid-response", "server-error"])
def test_ambiguous_post_never_promotes_later_candidate_or_claims_absence(clock, fault):
    """Lost response and temporary absence retain UNKNOWN and cannot authorize deleting a later matching object."""
    api = FakeAPI()
    api.post_error = TimeoutError("private transport output")
    if fault == "no-commit":
        api.reject = TimeoutError("not known to have committed")
    elif fault == "read-fails":
        api.reject = TimeoutError("not known to have committed")
    elif fault == "invalid-response":
        api.post_error = None
        api.post_response = live_pvc()
        api.post_response.spec.resources.requests = {"storage": "2Gi"}
    elif fault == "server-error":
        api.post_error = client.ApiException(status=500, body="private response body")
    item = resource(api)
    if fault == "read-fails":
        original = api.create_namespaced_persistent_volume_claim

        def lost(*args, **kwargs):
            """Lose the POST response and make subsequent observations unavailable."""
            api.errors["read"] = ConnectionError("private API address")
            return original(*args, **kwargs)
        api.create_namespaced_persistent_volume_claim = lost
    with pytest.raises((KubernetesTimeout, KubernetesConnectionFailed, WorkspaceConflict)) as caught:
        item.create()
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNKNOWN
    assert item.uid is None and item.submitted and mutation_count(api, "post") == 1
    assert item.creation_failure.resource_state == CreationResourceState.UNKNOWN
    if api.pvc is not None and fault != "read-fails":
        assert item.candidate.uid == UID and item.candidate.original_uid_verified is False
    with pytest.raises(WorkspaceOperationFailed):
        item.create()
    with pytest.raises(WorkspaceConflict):
        item.delete(expected_uid=UID)
    assert mutation_count(api, "post") == 1 and mutation_count(api, "delete") == 0


@pytest.mark.parametrize("fault,error", [("namespace-missing", KubernetesConfigurationError),
    ("namespace-denied", KubernetesPermissionDenied), ("collection-missing", KubernetesConfigurationError),
    ("denied", KubernetesPermissionDenied), ("connection", KubernetesConnectionFailed), ("timeout", KubernetesTimeout)])
def test_missing_objects_require_reliable_namespace_and_api_observation(clock, fault, error):
    """Namespace, RBAC and transport failures can never turn inspection or deletion into absent success."""
    api = FakeAPI()
    if fault.startswith("namespace"):
        api.errors["namespace"] = client.ApiException(status=404 if fault.endswith("missing") else 403)
    elif fault == "collection-missing":
        api.errors["list"] = client.ApiException(status=404)
    else:
        api.errors["list"] = {"denied": client.ApiException(status=403),
            "connection": ConnectionError("private endpoint"), "timeout": TimeoutError("private timeout")}[fault]
    item = resource(api)
    for operation in (lambda: item.inspect(expected_uid=UID), lambda: item.delete(expected_uid=UID)):
        with pytest.raises(error):
            operation()
    assert not mutation_count(api, "delete")


@pytest.mark.parametrize("change", ["uid", "token", "spec", "rv-race"])
def test_inspection_requires_original_uid_and_consistent_observations(clock, change):
    """Read-only inspection cannot follow replacements, modified specs, creation identity or list/read changes."""
    api = FakeAPI()
    api.pvc = live_pvc()
    if change == "uid":
        api.pvc.metadata.uid = OTHER_UID
    elif change == "token":
        api.pvc.metadata.labels["io.agent-sandbox.workspace-token"] = "wct_" + "f" * 32
    elif change == "spec":
        api.pvc.spec.storage_class_name = "other"
    else:
        listed = copy.deepcopy(api.pvc)
        listed.metadata.resource_version = "6"
        api.pvc_page = client.V1PersistentVolumeClaimList(api_version="v1", kind="PersistentVolumeClaimList", items=[listed], metadata=client.V1ListMeta())
    with pytest.raises(WorkspaceConflict):
        resource(api).inspect(expected_uid=UID)
    assert not mutation_count(api, "post") and not mutation_count(api, "delete")


def test_inspection_missing_original_and_pending_are_distinct(clock):
    """A confirmed absent object returns None; Pending/Lost health alone cannot invalidate original identity."""
    api = FakeAPI()
    item = resource(api)
    assert item.inspect(expected_uid=UID) is None
    assert any(call[0] == "namespace" for call in api.calls)
    api.pvc = live_pvc()
    api.pvc.status = client.V1PersistentVolumeClaimStatus(phase="Lost")
    assert item.inspect(expected_uid=UID).original_uid_verified is True
    with pytest.raises(ValueError):
        item.inspect(expected_uid=None)
    with pytest.raises(ValueError):
        item.delete(expected_uid="")


@pytest.mark.parametrize("phase,terminating", [("Running", False), ("Pending", False),
    ("Succeeded", False), ("Failed", False), ("Running", True)])
def test_any_consumer_object_blocks_deletion(clock, phase, terminating):
    """Even unlabelled, stopped or terminating consumers prevent PVC deletion."""
    api = FakeAPI()
    api.pvc = live_pvc()
    api.pods = [consumer(phase, terminating=terminating)]
    with pytest.raises(WorkspaceConflict):
        resource(api).delete(expected_uid=UID)
    assert not mutation_count(api, "delete")
    scan = next(call for call in api.calls if call[0] == "consumers")
    assert "label_selector" not in scan[2] and scan[2]["limit"] == 101 and scan[2]["watch"] is False


def test_occupied_record_blocks_before_api_access(clock):
    """A recorded holder is sufficient to deny cleanup even if the backend cannot find the PVC."""
    api = FakeAPI()
    item = resource(api, sandbox_id="sbx_" + "a" * 32, transaction_id="txn_" + "b" * 32)
    with pytest.raises(WorkspaceConflict):
        item.delete(expected_uid=UID)
    assert not api.calls


@pytest.mark.parametrize("fault", ["pagination", "remaining", "too-many", "malformed", "denied", "connection"])
def test_incomplete_consumer_checks_never_allow_delete(clock, fault):
    """An incomplete or unavailable namespace-wide consumer scan is not evidence of an unused volume."""
    api = FakeAPI()
    api.pvc = live_pvc()
    if fault == "pagination":
        api.continuation = "more"
    elif fault == "remaining":
        api.remaining_count = 1
    elif fault == "too-many":
        api.pods = [consumer(claim="other") for _ in range(101)]
    elif fault == "malformed":
        api.pods = [SimpleNamespace(spec=None)]
    else:
        api.errors["consumers"] = client.ApiException(status=403) if fault == "denied" else ConnectionError("private")
    with pytest.raises((WorkspaceConflict, KubernetesPermissionDenied, KubernetesConnectionFailed)):
        resource(api).delete(expected_uid=UID)
    assert not mutation_count(api, "delete")


@pytest.mark.parametrize("lost", [False, True])
def test_exact_delete_waits_for_absence_and_preserves_original_receipt(clock, lost):
    """Only confirmed disappearance completes cleanup, including a DELETE whose response was lost after commit."""
    api = FakeAPI()
    item = resource(api)
    item.create()
    if lost:
        api.delete_error = TimeoutError("private delete output")
    item.delete(expected_uid=UID)
    assert item.uid == UID and item.submitted
    assert item.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
    deletion = next(call for call in api.calls if call[0] == "delete")
    assert deletion[1:3] == ("agent-sandbox-wsp-0123456789abcdef0123456789abcdef", NAMESPACE)
    assert deletion[3].preconditions.uid == UID and deletion[3].preconditions.resource_version == "7"
    item.delete(expected_uid=UID)
    assert mutation_count(api, "delete") == 1 and item.uid == UID
    with pytest.raises(WorkspaceOperationFailed) as caught:
        item.create()
    assert caught.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
    assert mutation_count(api, "post") == 1


@pytest.mark.parametrize("fault", ["stuck", "replacement", "conflict", "already-deleting"])
def test_delete_does_not_succeed_early_or_follow_replacements(clock, fault):
    """Acknowledged DELETE, conflicts and terminating objects need bounded original-identity confirmation."""
    api = FakeAPI()
    item = resource(api)
    item.create()
    api.keep_on_delete = True
    if fault == "replacement":
        api.replacement_on_delete = True
    elif fault == "conflict":
        api.delete_error = client.ApiException(status=409, body="private conflict")
    elif fault == "already-deleting":
        api.pvc.metadata.deletion_timestamp = "2026-11-15T00:00:00Z"
    with pytest.raises((WorkspaceConflict, WorkspaceOperationFailed)) as caught:
        item.delete(expected_uid=UID)
    assert item.uid == UID and item.submitted
    assert caught.value.creation_failure.resource_state in {CreationResourceState.REMAINING, CreationResourceState.UNKNOWN}
    assert mutation_count(api, "delete") == (0 if fault == "already-deleting" else 1)
    if fault in {"stuck", "already-deleting"}:
        assert clock.waits and clock.now <= 0.4


def test_borrowed_client_is_never_acquired_closed_or_used_for_other_resource_kinds(clock, monkeypatch):
    """Only permitted namespace/PVC/Pod API methods exist on this transport; no ownership escapes to the helper."""
    api = FakeAPI()

    def forbidden(*args, **kwargs):
        """Fail if a resource operation tries to acquire another SDK transport."""
        pytest.fail("borrowed-client ownership violated")

    monkeypatch.setattr(client, "ApiClient", forbidden)
    item = resource(api)
    item.create()
    assert item.inspect(expected_uid=UID).uid == UID
    item.delete(expected_uid=UID)
    assert api.pvc is None


def test_budget_expiry_before_post_is_proven_unchanged(clock):
    """Running out of preflight budget cannot mark a POST as possibly dispatched when no write was called."""
    api = FakeAPI()
    original = api.read_namespace

    def expires(*args, **kwargs):
        """Consume all remaining budget at the last read before the write boundary."""
        response = original(*args, **kwargs)
        clock.now = 0.4
        return response

    api.read_namespace = expires
    item = resource(api)
    with pytest.raises(KubernetesTimeout) as caught:
        item.create()
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED
    assert not item.submitted and mutation_count(api, "post") == 0


def test_late_candidate_after_temporary_absence_is_not_an_original_receipt(clock):
    """A Pending PVC appearing after a lost POST still cannot authorize automatic rollback."""
    api = FakeAPI()
    original = api.list_namespaced_persistent_volume_claim
    observations = []
    api.reject = TimeoutError("response not observed")

    def delayed(namespace, **kwargs):
        """Expose the candidate only after a full absent reconciliation observation has completed."""
        observations.append(True)
        if len(observations) == 3:
            api.pvc = live_pvc()
        return original(namespace, **kwargs)

    api.list_namespaced_persistent_volume_claim = delayed
    item = resource(api)
    with pytest.raises(KubernetesTimeout):
        item.create()
    assert clock.waits and item.uid is None and item.candidate.uid == UID
    assert item.creation_failure.resource_state == CreationResourceState.UNKNOWN
    with pytest.raises(WorkspaceConflict):
        item.delete(expected_uid=item.candidate.uid)
    assert not mutation_count(api, "delete")


def test_non_consumer_pods_do_not_block_original_pvc_cleanup(clock):
    """The scan checks actual claimName rather than denying all Pods or only checking platform labels."""
    api = FakeAPI()
    api.pvc = live_pvc()
    api.pods = [consumer(claim="some-other-pvc")]
    resource(api).delete(expected_uid=UID)
    assert api.pvc is None and mutation_count(api, "delete") == 1


def test_delete_retry_after_unconfirmed_cleanup_keeps_original_receipts(clock):
    """A caller may retry deletion once the original resource can be confirmed gone; create remains one-shot."""
    api = FakeAPI()
    item = resource(api)
    item.create()
    api.keep_on_delete = True
    api.delete_error = TimeoutError("response lost")
    with pytest.raises(WorkspaceOperationFailed) as caught:
        item.delete(expected_uid=UID)
    assert caught.value.creation_failure.resource_state == CreationResourceState.REMAINING
    api.pvc = None
    item.delete(expected_uid=UID)
    assert item.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
    assert item.uid == UID and item.submitted and mutation_count(api, "delete") == 1


@pytest.mark.parametrize("status,error", [(401, KubernetesPermissionDenied), (403, KubernetesPermissionDenied),
    (409, WorkspaceConflict)])
def test_delete_rejection_is_fixed_and_keeps_original_identity(clock, status, error):
    """Permission/precondition failures neither clear the UID nor retry the destructive operation."""
    api = FakeAPI()
    item = resource(api)
    item.create()
    api.keep_on_delete = True
    api.delete_error = client.ApiException(status=status, body="private response")
    with pytest.raises(error) as caught:
        item.delete(expected_uid=UID)
    assert item.uid == UID and item.submitted and mutation_count(api, "delete") == 1
    assert caught.value.creation_failure.resource_state == CreationResourceState.REMAINING
    assert "private response" not in str(caught.value)


def test_replacement_seen_after_lost_delete_is_unknown_not_rolled_back(clock):
    """A replacement after an ambiguous deletion prevents positive cleanup classification and is never followed."""
    api = FakeAPI()
    item = resource(api)
    item.create()
    api.replacement_on_delete = True
    api.delete_error = TimeoutError("response lost")
    with pytest.raises(WorkspaceConflict) as caught:
        item.delete(expected_uid=UID)
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNKNOWN
    assert api.pvc.metadata.uid == OTHER_UID and item.uid == UID
    assert mutation_count(api, "delete") == 1


def test_wrong_caller_uid_preserves_creation_evidence_without_api_access(clock):
    """Rejecting an incorrect original UID must not erase a previously proven creation receipt."""
    api = FakeAPI()
    item = resource(api)
    item.create()
    count = len(api.calls)
    for operation in (item.inspect, item.delete):
        with pytest.raises(WorkspaceConflict) as caught:
            operation(expected_uid=OTHER_UID)
        assert caught.value.creation_failure.resource_state == CreationResourceState.REMAINING
    assert len(api.calls) == count and item.uid == UID and item.submitted


def test_finished_delete_cannot_be_followed_by_creation_on_same_object(clock):
    """A completed deletion is terminal for this helper and cannot make a new PVC inherit old cleanup evidence."""
    api = FakeAPI()
    item = resource(api)
    item.delete(expected_uid=UID)
    with pytest.raises(WorkspaceOperationFailed):
        item.create()
    assert not item.submitted and item.uid is None and not mutation_count(api, "post")


def test_list_item_type_metadata_is_supplied_only_from_verified_v1_pvc_envelope(clock):
    """Real Kubernetes list entries omit kind/apiVersion; exact identity still comes from UID and workspace metadata."""
    api = FakeAPI()
    api.pvc = live_pvc()
    listed = copy.deepcopy(api.pvc)
    listed.api_version = listed.kind = None
    api.pvc_page = client.V1PersistentVolumeClaimList(api_version="v1", kind="PersistentVolumeClaimList",
        items=[listed], metadata=client.V1ListMeta())
    assert resource(api).inspect(expected_uid=UID).uid == UID
    assert listed.api_version is None and listed.kind is None


@pytest.mark.parametrize("kind,version", [("PodList", "v1"), ("PersistentVolumeClaimList", "v2"),
    (None, None)])
def test_unknown_list_envelope_cannot_supply_missing_item_types(clock, kind, version):
    """Missing item type fields do not authorize guessing a collection's resource kind or API version."""
    api = FakeAPI()
    api.pvc = live_pvc()
    listed = copy.deepcopy(api.pvc)
    listed.api_version = listed.kind = None
    api.pvc_page = client.V1PersistentVolumeClaimList(api_version=version, kind=kind,
        items=[listed], metadata=client.V1ListMeta())
    with pytest.raises(WorkspaceConflict):
        resource(api).inspect(expected_uid=UID)
    assert not mutation_count(api, "delete")
