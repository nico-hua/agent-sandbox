"""Offline original-UID lifecycle for standalone Egress components, never Runtime integration."""

import copy
import importlib
import threading
from dataclasses import FrozenInstanceError, replace
from types import SimpleNamespace
from uuid import UUID

import pytest
from kubernetes import client

from control_plane.core.errors import (
    CreationResourceState, KubernetesConfigurationError, KubernetesPermissionDenied,
    KubernetesTimeout, KubernetesConnectionFailed, SandboxNotFound,
)
from control_plane.runtime.kubernetes.egress_spec import (
    build_egress_pod, build_egress_service, build_egress_network_policies,
)
from test_kubernetes_egress_spec import spec, network_inputs, IMAGE, PREFIX, ROOT
from test_kubernetes_query import NAMESPACE, SID, TXN

KEYS = ('isolation', 'agent_policy', 'service', 'pod')
ROLES = {'isolation': 'egress-isolation', 'agent_policy': 'agent-egress', 'service': 'egress-service', 'pod': 'egress'}
KINDS = {'isolation': 'network_policy', 'agent_policy': 'network_policy', 'service': 'service', 'pod': 'pod'}
UIDS = {key: str(UUID(int=index + 30)) for index, key in enumerate(KEYS)}


def module():
    """Import the implementation only when a test exercises the desired missing/new behavior."""
    return importlib.import_module('control_plane.runtime.kubernetes.egress_resources')


class FakeEgressAPI:
    """Model actual SDK object defaults, discovery, RV guards and faults without running Docker or Kubernetes."""

    def __init__(self):
        """Keep the borrowed facade/transport visibly shared and all mutation evidence observable."""
        self.networking = self
        self.api_client = SimpleNamespace(close=self.close)
        self.objects, self.calls, self.originals = {}, [], {}
        self.before, self.after, self.responses = {}, {}, {}
        self.transform_list = None
        self.stuck, self.hidden, self.deleted = set(), set(), []
        self.cancel_after = None
        self.closed = False
        self.namespace_failure = None

    def close(self):
        """Expose any accidental attempt to close the borrowed transport."""
        self.closed = True
        pytest.fail('borrowed client was closed')

    def key(self, body):
        """Derive a test resource role without treating ordinary Agent components as Egress-owned."""
        return next((key for key, role in ROLES.items() if body.metadata.labels.get(PREFIX + 'resource') == role), 'other')

    def read_namespace(self, name, **kwargs):
        """Only an exact successful namespace observation can authorize absence conclusions."""
        self.calls.append(('namespace', name, kwargs))
        if self.namespace_failure:
            raise self.namespace_failure
        return client.V1Namespace(metadata=client.V1ObjectMeta(name=name))

    def __getattr__(self, name):
        """Implement only the resource API methods the component is authorized to call."""
        if '_namespaced_' not in name:
            raise AttributeError(name)
        verb, kind = name.split('_namespaced_', 1)
        if kind not in {'pod', 'service', 'network_policy'} or verb not in {'read', 'list', 'create', 'delete'}:
            raise AttributeError(name)

        def operation(*args, **kwargs):
            """Simulate bounded list/exact-read, single POST or UID/RV-preconditioned normal deletion."""
            timeout = kwargs['_request_timeout']
            assert len(timeout) == 2 and all(0 < n <= 0.5 for n in timeout)
            if verb == 'list':
                namespace = args[0]
                selector = dict(piece.split('=', 1) for piece in kwargs['label_selector'].split(','))
                assert PREFIX + 'transaction-id' not in selector
                assert kwargs['watch'] is False and 1 <= kwargs['limit'] <= 8
                items = [copy.deepcopy(obj) for (typ, _), obj in self.objects.items() if typ == kind
                         and obj.metadata.namespace == namespace and all(obj.metadata.labels.get(k) == v for k, v in selector.items())]
                pages = {'pod': client.V1PodList, 'service': client.V1ServiceList, 'network_policy': client.V1NetworkPolicyList}
                page = pages[kind](api_version='networking.k8s.io/v1' if kind == 'network_policy' else 'v1',
                    kind={'pod': 'PodList', 'service': 'ServiceList', 'network_policy': 'NetworkPolicyList'}[kind],
                    metadata=client.V1ListMeta(), items=items[:kwargs['limit']])
                self.calls.append((verb, kind, kwargs))
                if self.transform_list:
                    self.transform_list(kind, page)
                return page
            if verb == 'create':
                body = copy.deepcopy(kwargs['body'])
                key = self.key(body)
                namespace = args[0]
            else:
                resource_name, namespace = args[:2]
                body = self.objects.get((kind, resource_name))
                key = self.key(body) if body is not None else next((k for k in KEYS if KINDS[k] == kind and
                    resource_name == ROOT + {'pod': '-egress', 'service': '-egress', 'isolation': '-egress-deny', 'agent_policy': '-egress-out'}[k]), 'other')
            self.calls.append((verb, key, kwargs))
            failure = self.before.get((verb, key))
            if failure:
                raise failure
            if verb == 'create':
                if (kind, body.metadata.name) in self.objects:
                    raise client.ApiException(status=409)
                body.metadata.uid, body.metadata.resource_version = UIDS[key], '100'
                if kind == 'service':
                    body.spec.cluster_ip, body.spec.cluster_ips = '10.96.0.42', ['10.96.0.42']
                    body.spec.session_affinity, body.spec.internal_traffic_policy = 'None', 'Cluster'
                self.objects[(kind, body.metadata.name)] = copy.deepcopy(body)
                self.originals[key] = body.metadata.uid
                if self.cancel_after:
                    self.cancel_after(key)
                if (verb, key) in self.after:
                    raise self.after[(verb, key)]
                response = copy.deepcopy(body)
                if key in self.responses:
                    self.responses[key](response)
                return response
            if verb == 'read':
                if body is None or key in self.hidden:
                    raise client.ApiException(status=404)
                return copy.deepcopy(body)
            if body is None:
                raise client.ApiException(status=404)
            options = kwargs['body']
            assert options.preconditions.uid == body.metadata.uid
            assert options.preconditions.resource_version == body.metadata.resource_version
            assert options.grace_period_seconds is None and options.orphan_dependents is None
            self.deleted.append(key)
            if key not in self.stuck:
                del self.objects[(kind, body.metadata.name)]
            if (verb, key) in self.after:
                raise self.after[(verb, key)]
            return client.V1Status(status='Success')

        return operation


@pytest.fixture
def harness(monkeypatch):
    """Keep worker polling deterministic and finite without guessing wall-clock sleeps."""
    implementation = module()
    clock = SimpleNamespace(value=0.0)

    def now():
        """Read only the synchronous operation clock."""
        return clock.value

    def wait(duration):
        """Advance every bounded poll so timeout tests remain instant."""
        assert duration > 0
        clock.value += duration

    monkeypatch.setattr(implementation, 'monotonic', now)
    monkeypatch.setattr(implementation, 'sleep', wait)
    monkeypatch.setattr(implementation, 'CLEANUP_TIMEOUT_SECONDS', 0.5)
    monkeypatch.setattr(implementation, 'RECONCILE_TIMEOUT_SECONDS', 0.3)
    api = FakeEgressAPI()
    resource = implementation.EgressResources(api, spec(), egress_image=IMAGE, **network_inputs())
    return SimpleNamespace(api=api, resource=resource, implementation=implementation, clock=clock)


def observed(api, key):
    """Get only one known role in a test, never use it as original authorization in production."""
    return next(obj for obj in api.objects.values() if api.key(obj) == key)


def test_constructor_is_inert_freezes_mutable_inputs_and_success_snapshot_is_immutable():
    """No discovery/client creation at construction, and later caller changes must not alter submitted policy."""
    api, source, inputs = FakeEgressAPI(), spec(), network_inputs()
    resource = module().EgressResources(api, source, egress_image=IMAGE, **inputs)
    assert not api.calls and not api.closed
    inputs['dns_pod_labels'].clear()
    inputs['cluster_cidrs'].clear()
    object.__setattr__(source, 'transaction_id', 'txn_' + 'f' * 32)
    result = resource.create()
    assert result.transaction_id == TXN and result.service_ipv4 == '10.96.0.42'
    assert dict(result.resource_uids) == UIDS
    assert result.dns_pod_labels == (('k8s-app', 'kube-dns'),)
    assert result.allow_domains == ('api.example.com', 'example.com')
    with pytest.raises(FrozenInstanceError):
        result.service_ipv4 = '10.96.0.99'
    assert [key for verb, key, _ in api.calls if verb == 'create'] == list(KEYS)
    assert not api.closed


def test_complete_inspection_and_agent_environment_require_original_service(harness):
    """Running state alone grants nothing; only full original configuration can supply the proxy address."""
    h = harness
    snapshot = h.resource.create()
    assert h.resource.inspect(snapshot) == snapshot
    assert {env.name: env.value for env in h.resource.agent_proxy_env(snapshot)}['HTTP_PROXY'] == 'http://10.96.0.42:18084'
    observed(h.api, 'service').metadata.uid = str(UUID(int=999))
    with pytest.raises(h.implementation.EgressResourceConflict):
        h.resource.agent_proxy_env(snapshot)


def test_delete_is_exact_idempotent_and_does_not_close_client(harness):
    """Both policies must survive until the original Pod is confirmed gone, with UID/RV-protected normal DELETE."""
    h = harness
    result = h.resource.create()
    h.resource.delete(result)
    h.resource.delete(result)
    assert h.api.deleted == ['pod', 'service', 'agent_policy', 'isolation']
    assert not h.api.objects and not h.api.closed
    assert dict(h.resource.original_uids) == UIDS
    assert h.resource.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    with pytest.raises(h.implementation.EgressOperationFailed):
        h.resource.create()
    assert len([1 for verb, _, _ in h.api.calls if verb == 'create']) == 4


@pytest.mark.parametrize('key', KEYS)
def test_each_definite_post_rejection_rolls_back_only_confirmed_prior_objects(harness, key):
    """A definite rejection is never retried and cannot authorize deletion of an existing conflicting object."""
    h = harness
    h.api.before[('create', key)] = client.ApiException(status=422, reason='private response diagnostic')
    with pytest.raises(h.implementation.EgressOperationFailed) as failed:
        h.resource.create()
    expected = CreationResourceState.UNCHANGED if key == 'isolation' else CreationResourceState.ROLLED_BACK
    assert failed.value.creation_failure.resource_state is expected
    assert not h.api.objects and not h.api.closed
    assert 'private' not in str(failed.value)
    assert [role for verb, role, _ in h.api.calls if verb == 'create'] == list(KEYS[:KEYS.index(key) + 1])


@pytest.mark.parametrize('key', KEYS)
def test_lost_post_response_never_promotes_a_candidate_to_original_uid(harness, key):
    """A committed but unacknowledged object remains UNKNOWN, even when later exact-name reads find it."""
    h = harness
    h.api.after[('create', key)] = TimeoutError('secret raw SDK body')
    with pytest.raises(KubernetesTimeout) as failed:
        h.resource.create()
    assert failed.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert key not in dict(h.resource.original_uids)
    assert observed(h.api, key).metadata.uid == UIDS[key]
    assert key not in h.api.deleted
    if key == 'pod':
        assert set(h.api.originals) == set(KEYS) and not h.api.deleted
    before = h.resource.submitted
    with pytest.raises(h.implementation.EgressOperationFailed):
        h.resource.create()
    assert h.resource.submitted == before
    assert len([1 for verb, role, _ in h.api.calls if verb == 'create' and role == key]) == 1
    assert 'secret' not in str(failed.value)


def test_wrong_post_type_is_not_an_original_receipt(harness):
    """A response from the wrong API kind cannot authorize cleanup merely by containing plausible metadata."""
    h = harness
    h.api.responses['service'] = lambda obj: setattr(obj, 'kind', 'Alien')
    with pytest.raises(h.implementation.EgressResourceConflict) as failed:
        h.resource.create()
    assert 'service' not in dict(h.resource.original_uids)
    assert failed.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert 'service' not in h.api.deleted


@pytest.mark.parametrize('change', ['uid', 'selector', 'port', 'cli', 'dns', 'allow-all', 'mount', 'image', 'missing'])
def test_inspect_rejects_original_replacement_and_configuration_widening(harness, change):
    """A live Pod never hides changed Service routing, CLI, DNS OR semantics, privileges or missing resources."""
    h = harness
    snapshot = h.resource.create()
    if change == 'uid':
        observed(h.api, 'pod').metadata.uid = str(UUID(int=999))
    elif change == 'selector':
        observed(h.api, 'service').spec.selector[PREFIX + 'resource'] = 'container'
    elif change == 'port':
        observed(h.api, 'service').spec.ports[0].target_port = 8080
    elif change == 'cli':
        observed(h.api, 'pod').spec.containers[0].args += ['-allow-domain', 'evil.invalid']
    elif change == 'dns':
        observed(h.api, 'isolation').spec.egress[0].to[0].pod_selector = None
    elif change == 'allow-all':
        observed(h.api, 'agent_policy').spec.egress.append(client.V1NetworkPolicyEgressRule())
    elif change == 'mount':
        observed(h.api, 'pod').spec.volumes = [client.V1Volume(name='host', host_path=client.V1HostPathVolumeSource(path='/'))]
    elif change == 'image':
        observed(h.api, 'pod').spec.containers[0].image = 'external/other:dev'
    else:
        target = observed(h.api, 'service')
        del h.api.objects[('service', target.metadata.name)]
    before = len(h.api.deleted)
    with pytest.raises(h.implementation.EgressResourceIncomplete if change == 'missing' else h.implementation.EgressResourceConflict):
        h.resource.inspect(snapshot)
    assert len(h.api.deleted) == before


@pytest.mark.parametrize('change', ['old-transaction', 'duplicate', 'unknown-role', 'truncated', 'inconsistent-read', 'unlabeled-name'])
def test_preflight_refuses_ambiguity_without_post_or_deletion(harness, change):
    """Bounded lists cannot hide stale transactions, unknown roles, duplicates or unlabeled exact-name collisions."""
    h = harness
    seed = build_egress_pod(spec(), namespace=NAMESPACE, egress_image=IMAGE)
    seed.metadata.uid, seed.metadata.resource_version = UIDS['pod'], '1'
    h.api.objects[('pod', seed.metadata.name)] = seed
    if change == 'old-transaction':
        seed.metadata.labels[PREFIX + 'transaction-id'] = 'txn_' + 'f' * 32
    elif change == 'duplicate':
        duplicate = copy.deepcopy(seed)
        duplicate.metadata.name += '-duplicate'
        h.api.objects[('pod', duplicate.metadata.name)] = duplicate
    elif change == 'unknown-role':
        seed.metadata.labels[PREFIX + 'resource'] = 'unknown-egress-role'
    elif change == 'truncated':
        h.api.transform_list = lambda kind, page: setattr(page.metadata, '_continue', 'next')
    elif change == 'inconsistent-read':
        h.api.hidden.add('pod')
    else:
        seed.metadata.labels.clear()
    with pytest.raises(h.implementation.EgressResourceConflict) as failed:
        h.resource.create()
    assert failed.value.creation_failure.resource_state is CreationResourceState.UNCHANGED
    assert not h.api.deleted and not any(verb == 'create' for verb, _, _ in h.api.calls)


def test_other_valid_components_are_neither_egress_candidates_nor_cleanup_targets(harness):
    """The same sandbox Agent/entry Service and deny policy can coexist without being claimed by this component."""
    from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
    h = harness
    agent = build_pod_spec(replace(spec(), network=type(spec().network)()), namespace=NAMESPACE, agent_image='agent-sandbox-basic:dev')
    agent.metadata.uid, agent.metadata.resource_version = str(UUID(int=999)), '1'
    h.api.objects[('pod', agent.metadata.name)] = agent
    snapshot = h.resource.create()
    assert h.resource.inspect(snapshot) == snapshot
    h.resource.delete(snapshot)
    assert list(h.api.objects.values()) == [agent]
    assert 'other' not in h.api.deleted


@pytest.mark.parametrize('action,status,expected', [('namespace', 404, KubernetesConfigurationError),
    ('namespace', 403, KubernetesPermissionDenied), ('namespace', 'connection', KubernetesConnectionFailed),
    ('namespace', 'timeout', KubernetesTimeout), ('list', 403, KubernetesPermissionDenied)])
def test_unreadable_namespace_or_inventory_is_not_resource_absence(harness, action, status, expected):
    """Backend/RBAC failure must not become a successful empty component or idempotent deletion."""
    h = harness
    failure = ConnectionError('raw private data') if status == 'connection' else TimeoutError('raw private data') if status == 'timeout' else client.ApiException(status=status, reason='raw private data')
    if action == 'namespace':
        h.api.namespace_failure = failure
    else:
        def truncated(kind, page):
            """Expose a list authorization failure after a readable namespace."""
            raise failure
        h.api.transform_list = truncated
    with pytest.raises(expected) as failed:
        h.resource.create()
    assert 'raw private' not in str(failed.value) and not h.api.deleted
    assert failed.value.creation_failure.resource_state is CreationResourceState.UNCHANGED


def test_pod_cleanup_timeout_keeps_both_policies_and_can_be_retried(harness):
    """A successful DELETE response with a surviving Pod cannot revoke isolation or report full cleanup."""
    h = harness
    result = h.resource.create()
    h.api.stuck.add('pod')
    with pytest.raises(KubernetesTimeout) as failed:
        h.resource.delete(result)
    assert failed.value.creation_failure.resource_state is CreationResourceState.REMAINING
    assert len(h.api.objects) == 4 and h.api.deleted == ['pod']
    h.api.stuck.clear()
    h.resource.delete(result)
    assert not h.api.objects and h.resource.creation_failure.resource_state is CreationResourceState.ROLLED_BACK


@pytest.mark.parametrize('key', KEYS)
def test_delete_response_loss_is_confirmed_by_original_absence(harness, key):
    """An unacknowledged DELETE can complete only through bounded exact reads and a readable namespace."""
    h = harness
    result = h.resource.create()
    h.api.after[('delete', key)] = TimeoutError('private transport')
    h.resource.delete(result)
    assert not h.api.objects and h.resource.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    assert h.api.deleted.count(key) == 1


def test_delete_invalid_snapshot_is_rejected_without_observing_or_mutating_cluster(harness):
    """A wrong namespace/UID receipt must fail before attempting even evidence-refresh reads."""
    h = harness
    result = h.resource.create()
    before = len(h.api.calls)
    with pytest.raises(h.implementation.EgressResourceConflict):
        h.resource.delete(replace(result, namespace='another-namespace'))
    assert len(h.api.calls) == before and not h.api.deleted


@pytest.mark.parametrize('stage', [None, 'isolation', 'agent_policy', 'service', 'pod'])
def test_cancel_signal_rolls_back_only_acknowledged_objects_at_sdk_boundaries(harness, stage):
    """Cancellation before submit or after a late POST response cannot skip exact cleanup or trigger retries."""
    h = harness
    cancelled = threading.Event()
    if stage is None:
        cancelled.set()
    h.api.cancel_after = lambda key: cancelled.set() if key == stage else None
    resource = h.implementation.EgressResources(h.api, spec(), egress_image=IMAGE, cancelled=cancelled, **network_inputs())
    with pytest.raises(h.implementation.EgressOperationFailed) as failed:
        resource.create()
    assert not h.api.objects
    assert failed.value.creation_failure.resource_state is (CreationResourceState.UNCHANGED if stage is None else CreationResourceState.ROLLED_BACK)
    assert not h.api.closed


def test_unacknowledged_pod_with_only_404_reads_stays_unknown_and_keeps_isolation(harness):
    """No number of temporary 404s may turn a possibly committed Pod POST into rollback proof."""
    h = harness
    h.api.before[('create', 'pod')] = TimeoutError('unacknowledged dispatch')
    with pytest.raises(KubernetesTimeout) as failed:
        h.resource.create()
    assert failed.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert set(h.api.originals) == {'isolation', 'agent_policy', 'service'}
    assert not h.api.deleted
    with pytest.raises(h.implementation.EgressOperationFailed):
        h.resource.delete()
    assert not h.api.deleted and h.resource.creation_failure.resource_state is CreationResourceState.UNKNOWN


def test_backend_loss_during_pod_confirmation_keeps_original_isolation(harness):
    """Even an accepted delete cannot permit policy cleanup if the subsequent Pod observation is unavailable."""
    h = harness
    snapshot = h.resource.create()
    h.api.before[('read', 'pod')] = ConnectionError('unavailable observation')
    with pytest.raises(KubernetesConnectionFailed):
        h.resource.delete(snapshot)
    assert h.resource.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert not h.api.deleted and len(h.api.objects) == 4
    h.api.before.clear()
    h.resource.delete(snapshot)
    assert not h.api.objects


@pytest.mark.parametrize('key', KEYS)
def test_delete_precondition_conflict_never_escalates_or_drops_evidence(harness, key):
    """A stale resourceVersion rejection cannot trigger force deletion or mutation of the next dependency."""
    h = harness
    snapshot = h.resource.create()
    h.api.before[('delete', key)] = client.ApiException(status=409, reason='RV changed')
    with pytest.raises(h.implementation.EgressResourceConflict):
        h.resource.delete(snapshot)
    assert h.resource.creation_failure.resource_state is CreationResourceState.REMAINING
    assert observed(h.api, key).metadata.uid == UIDS[key]
    if key == 'pod':
        assert not h.api.deleted
    h.api.before.clear()
    h.resource.delete(snapshot)
    assert not h.api.objects


def test_valid_snapshot_survives_new_component_object_without_current_config_defaults(harness):
    """A caller can retain the original immutable snapshot and manage it with an explicitly matching new helper."""
    h = harness
    snapshot = h.resource.create()
    rebuilt = h.implementation.EgressResources(h.api, spec(), egress_image=IMAGE, **network_inputs())
    assert rebuilt.inspect(snapshot) == snapshot and not rebuilt.original_uids
    rebuilt.delete(snapshot)
    assert not h.api.objects and dict(rebuilt.original_uids) == UIDS
    with pytest.raises(SandboxNotFound):
        rebuilt.inspect(snapshot)


def test_partial_delete_retry_does_not_need_healthy_or_unchanged_configuration(harness):
    """Deletion authorization uses original identity, not a healthy Pod or an untampered allow rule."""
    h = harness
    snapshot = h.resource.create()
    observed(h.api, 'pod').status = client.V1PodStatus(phase='Failed')
    observed(h.api, 'isolation').spec.egress.append(client.V1NetworkPolicyEgressRule())
    h.api.before[('delete', 'service')] = client.ApiException(status=403)
    with pytest.raises(KubernetesPermissionDenied):
        h.resource.delete(snapshot)
    assert h.api.deleted == ['pod']
    assert h.resource.creation_failure.resource_state is CreationResourceState.REMAINING
    h.api.before.clear()
    h.resource.delete(snapshot)
    assert h.api.deleted == ['pod', 'service', 'agent_policy', 'isolation']


def test_same_name_replacement_before_delete_is_never_claimed_or_removed(harness):
    """A replacement of any original UID must stop the whole deletion before touching another component."""
    h = harness
    snapshot = h.resource.create()
    observed(h.api, 'service').metadata.uid = str(UUID(int=987))
    with pytest.raises(h.implementation.EgressResourceConflict):
        h.resource.delete(snapshot)
    assert not h.api.deleted and h.resource.creation_failure.resource_state is CreationResourceState.UNKNOWN


def test_missing_original_role_in_readable_namespace_is_incomplete_not_notfound(harness):
    """One missing original and all four missing have distinct fixed inspection outcomes."""
    h = harness
    snapshot = h.resource.create()
    target = observed(h.api, 'pod')
    del h.api.objects[('pod', target.metadata.name)]
    with pytest.raises(h.implementation.EgressResourceIncomplete):
        h.resource.inspect(snapshot)
    h.api.objects.clear()
    with pytest.raises(SandboxNotFound):
        h.resource.inspect(snapshot)


def test_list_item_type_meta_may_be_omitted_but_unknown_type_is_rejected(harness):
    """An actual v1 collection envelope supplies missing item fields, never overwrites an explicitly wrong type."""
    h = harness
    snapshot = h.resource.create()

    def omit(kind, page):
        """Mirror the API's legal omission of TypeMeta from each list item."""
        for item in page.items:
            item.api_version = item.kind = None

    h.api.transform_list = omit
    assert h.resource.inspect(snapshot) == snapshot

    def unknown(kind, page):
        """Preserve a contradictory explicit resource type rather than treating it as an omission."""
        for item in page.items:
            item.kind = 'Alien'

    h.api.transform_list = unknown
    with pytest.raises(h.implementation.EgressResourceConflict):
        h.resource.inspect(snapshot)


def test_repeat_create_during_unacknowledged_dispatch_cannot_report_unchanged(harness):
    """Even a reentrant failure must preserve that a write is in flight, rather than advertise no resource change."""
    h = harness
    outcomes = []

    def reenter(key):
        """Inspect duplicate-call evidence while the successful POST response is not yet delivered."""
        if key == 'isolation':
            with pytest.raises(h.implementation.EgressOperationFailed) as failed:
                h.resource.create()
            outcomes.append(failed.value.creation_failure.resource_state)

    h.api.cancel_after = reenter
    h.resource.create()
    assert outcomes == [CreationResourceState.UNKNOWN]
    assert len([1 for verb, _, _ in h.api.calls if verb == 'create']) == 4


def test_api_omitted_false_publish_not_ready_default_is_safe_but_true_is_rejected(harness):
    """Kubernetes omits false in real responses; this must remain internal-only without accepting true."""
    h = harness
    h.api.responses['service'] = lambda obj: setattr(obj.spec, 'publish_not_ready_addresses', None)
    snapshot = h.resource.create()
    assert h.resource.inspect(snapshot) == snapshot
    observed(h.api, 'service').spec.publish_not_ready_addresses = True
    with pytest.raises(h.implementation.EgressResourceConflict):
        h.resource.inspect(snapshot)


def test_status_only_resource_version_drift_is_not_an_identity_conflict(harness):
    """Kubelet may update Pod status between list and GET; stable UID/metadata/config must remain inspectable."""
    h = harness

    def older_list_version(kind, page):
        """Model a legitimate older collection resourceVersion without changing any immutable identity fact."""
        if kind == 'pod':
            for item in page.items:
                item.metadata.resource_version = '99'

    h.api.transform_list = older_list_version
    snapshot = h.resource.create()
    assert h.resource.inspect(snapshot) == snapshot
    h.resource.delete(snapshot)
    assert not h.api.objects


@pytest.mark.parametrize('lost_key', ['isolation', 'service'])
def test_failed_creator_cannot_import_candidate_uids_from_a_forged_snapshot(harness, lost_key):
    """An old failed attempt cannot promote an unacknowledged candidate or unsubmitted role via snapshot import."""
    h = harness
    h.api.after[('create', lost_key)] = TimeoutError('response lost')
    with pytest.raises(KubernetesTimeout):
        h.resource.create()
    original, submitted = h.resource.original_uids, h.resource.submitted
    fabricated = h.implementation.EgressSnapshot(*h.resource._config, tuple(sorted(UIDS.items())), '10.96.0.42')
    before = len(h.api.calls)
    for _ in range(2):
        with pytest.raises(h.implementation.EgressResourceConflict):
            h.resource.delete(fabricated)
    assert len(h.api.calls) == before
    assert h.resource.original_uids == original and h.resource.submitted == submitted
    assert lost_key not in h.api.deleted


def test_late_sdk_response_after_repeated_cancel_settles_original_cleanup(harness):
    """The owning worker must capture a late original response and finish rollback despite repeated cancellation."""
    from concurrent.futures import ThreadPoolExecutor
    h = harness
    entered, release, cancelled = threading.Event(), threading.Event(), threading.Event()
    resource = h.implementation.EgressResources(h.api, spec(), egress_image=IMAGE, cancelled=cancelled, **network_inputs())

    def hold_pod_response(key):
        """Hold only the in-flight SDK response, after server commitment but before helper UID acknowledgement."""
        if key == 'pod':
            entered.set()
            assert release.wait(3), 'test did not settle held SDK response'

    h.api.cancel_after = hold_pod_response
    with ThreadPoolExecutor(max_workers=1) as worker:
        pending = worker.submit(resource.create)
        try:
            assert entered.wait(3)
            cancelled.set()
            cancelled.set()
            assert resource.creation_failure.resource_state is CreationResourceState.UNKNOWN
            assert 'pod' not in dict(resource.original_uids)
            assert not h.api.deleted
        finally:
            release.set()
        with pytest.raises(h.implementation.EgressOperationFailed) as failed:
            pending.result(timeout=3)
    assert failed.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
    assert not h.api.objects and not h.api.closed


def test_uid_replacement_during_delete_confirmation_preserves_isolation(harness, monkeypatch):
    """A same-name replacement after the DELETE response cannot be followed or stripped of its remaining policies."""
    h = harness
    snapshot = h.resource.create()
    replacement = copy.deepcopy(observed(h.api, 'pod'))
    replacement.metadata.uid = str(UUID(int=989))
    original_read = h.api.read_namespaced_pod

    def read_after_replacement(*args, **kwargs):
        """Expose a new UID on the first confirmation read, after the original Pod was deleted."""
        if 'pod' in h.api.deleted:
            h.api.objects[('pod', replacement.metadata.name)] = replacement
        return original_read(*args, **kwargs)

    monkeypatch.setattr(h.api, 'read_namespaced_pod', read_after_replacement, raising=False)
    with pytest.raises(h.implementation.EgressResourceConflict):
        h.resource.delete(snapshot)
    assert h.api.deleted == ['pod'] and len(h.api.objects) == 4
    assert observed(h.api, 'pod').metadata.uid == replacement.metadata.uid
    assert h.resource.creation_failure.resource_state is CreationResourceState.UNKNOWN


def test_delayed_unacknowledged_pod_after_temporary_404_remains_candidate(harness, monkeypatch):
    """A late appearance following a lost POST response never becomes proof of original identity or rollback."""
    h = harness
    original_read = h.api.read_namespaced_pod
    reads = 0
    h.api.before[('create', 'pod')] = TimeoutError('response lost')

    def delayed_read(*args, **kwargs):
        """Return temporary object 404s before the hypothetical committed Pod becomes visible."""
        nonlocal reads
        if 'pod' in h.resource.submitted:
            reads += 1
            if reads >= 2:
                candidate = build_egress_pod(spec(), namespace=NAMESPACE, egress_image=IMAGE)
                candidate.metadata.uid, candidate.metadata.resource_version = UIDS['pod'], '200'
                h.api.objects[('pod', candidate.metadata.name)] = candidate
        return original_read(*args, **kwargs)

    monkeypatch.setattr(h.api, 'read_namespaced_pod', delayed_read, raising=False)
    with pytest.raises(KubernetesTimeout) as failed:
        h.resource.create()
    assert reads >= 2 and failed.value.creation_failure.resource_state is CreationResourceState.UNKNOWN
    assert 'pod' not in dict(h.resource.original_uids) and not h.api.deleted
