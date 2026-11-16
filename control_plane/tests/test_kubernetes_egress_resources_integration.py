"""Double-opt-in standalone Egress resources only: no Agent, public traffic or Runtime registration."""

import os
import json
import secrets
import subprocess
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from kubernetes import client

from control_plane.core.models import SandboxNetwork, SandboxSpec
from control_plane.core.errors import SandboxNotFound, CreationResourceState
from control_plane.runtime.kubernetes.egress_resources import EgressResources, EgressResourceConflict

pytestmark = [
    pytest.mark.kubernetes_integration,
    pytest.mark.kubernetes_resource_integration,
    pytest.mark.timeout(150),
    pytest.mark.skipif(
        os.getenv('RUN_KUBERNETES_INTEGRATION') != '1'
        or os.getenv('RUN_KUBERNETES_RESOURCE_INTEGRATION') != '1'
        or os.getenv('KUBERNETES_TEST_CONTEXT') != 'agent-sandbox-k8s'
        or os.getenv('KUBERNETES_TEST_NAMESPACE') != 'agent-sandbox-test',
        reason='both opt-ins and explicit existing Minikube context/namespace are required',
    ),
]
PREFIX = 'io.agent-sandbox.'
IMAGE = 'agent-sandbox-egress:dev'
KINDS = {'egress': 'pod', 'egress-service': 'service', 'agent-egress': 'network_policy', 'egress-isolation': 'network_policy'}


def _contract_differences(expected, actual, path=()):
    """Return only mismatching field paths, never SDK payloads or configuration values, for integration diagnosis."""
    from control_plane.runtime.kubernetes.proxy_resources import _spec_matches
    if _spec_matches(expected, actual, path):
        return ()
    if isinstance(expected, dict) and isinstance(actual, dict) and expected.keys() == actual.keys():
        return tuple(p for key in expected for p in _contract_differences(expected[key], actual[key], (*path, key)))
    if isinstance(expected, list) and isinstance(actual, list) and len(expected) == len(actual):
        return tuple(p for left, right in zip(expected, actual) for p in _contract_differences(left, right, path))
    return ('.'.join(path),)


def _cached_images():
    """Require existing project images on every possible node, never load, build, pull or overwrite inside the test."""
    identities = []
    for node in ('agent-sandbox-k8s', 'agent-sandbox-k8s-m02', 'agent-sandbox-k8s-m03'):
        result = subprocess.run(['minikube', '-p', 'agent-sandbox-k8s', 'ssh', '--node=' + node, '--',
            'sudo', 'crictl', 'inspecti', '--output', 'go-template', '--template', '{{.status.id}}',
            'docker.io/library/' + IMAGE], capture_output=True, text=True, timeout=20)
        assert result.returncode == 0 and result.stdout.strip().startswith('sha256:'), 'cached Egress image unavailable'
        identities.append(result.stdout.strip())
    assert len(set(identities)) == 1, 'Egress image cache differs across nodes'


def _identity(resource, entry):
    """Independently verify exact original metadata/UID before any fallback deletion, without tested helper rules."""
    metadata = resource.metadata
    assert metadata.namespace == entry['namespace'] and metadata.name == entry['name']
    assert resource.kind == entry['kind_name'] and resource.api_version == entry['api_version']
    for actual, expected in ((metadata.labels, entry['labels']), (metadata.annotations, entry['annotations'])):
        assert isinstance(actual, dict) and all(actual.get(key) == value for key, value in expected.items())
        assert not any(key.startswith(PREFIX) and key not in expected for key in actual)
    assert metadata.owner_references in (None, [])
    assert str(UUID(metadata.uid)) == metadata.uid
    if entry['uid'] is not None:
        assert metadata.uid == entry['uid'], 'original Egress resource replaced; cleanup refused'
    assert metadata.resource_version
    return metadata.uid


class RecordingAPI:
    """Retain original POST acknowledgements independently of the tested helper and share its borrowed pool."""

    def __init__(self, api, entries, receipt_path):
        """Borrow either facade and wrap networking on the same existing ApiClient."""
        self.api, self.entries, self.receipt_path = api, entries, receipt_path
        if hasattr(api, 'networking'):
            self.networking = RecordingAPI(api.networking, entries, receipt_path)

    def __getattr__(self, name):
        """Record only component POSTs; delegate reads and exact guarded deletion unchanged."""
        operation = getattr(self.api, name)
        if not name.startswith('create_namespaced_'):
            return operation

        def create(namespace, *, body, **kwargs):
            """Capture submitted metadata then save only the actual successful POST UID, never adopt later reads."""
            role = body.metadata.labels[PREFIX + 'resource']
            assert role in KINDS and name == 'create_namespaced_' + KINDS[role]
            entry = dict(kind=KINDS[role], name=body.metadata.name, namespace=namespace,
                         labels=dict(body.metadata.labels), annotations=dict(body.metadata.annotations),
                         uid=None, kind_name=body.kind, api_version=body.api_version)
            self.entries[role] = entry
            self.receipt_path.write_text(json.dumps(self.entries), encoding='utf-8')
            result = operation(namespace, body=body, **kwargs)
            entry['uid'] = _identity(result, entry)
            # Keep original POST acknowledgements available even if this test
            # process later fails during independent fallback cleanup.
            self.receipt_path.write_text(json.dumps(self.entries), encoding='utf-8')
            entry['defaulted_fields'] = tuple(key for key, value in body.spec.to_dict().items()
                                              if result.spec.to_dict().get(key) != value)
            if role == 'egress':
                entry['defaulted_fields'] = _contract_differences(body.spec.to_dict(), result.spec.to_dict())
            return result

        return create


def _read(api, entry):
    """Allow object 404 only after the independent fixture established its exact namespace."""
    facade = api.networking if entry['kind'] == 'network_policy' else api
    try:
        return getattr(facade, 'read_namespaced_' + entry['kind'])(entry['name'], entry['namespace'], _request_timeout=(0.5, 0.5))
    except client.ApiException as error:
        if error.status != 404:
            raise
        assert api.read_namespace(entry['namespace'], _request_timeout=(0.5, 0.5)).metadata.name == entry['namespace']
        return None


def _cleanup(api, entries):
    """Fallback deletes only acknowledged original UIDs, Pod first, with normal grace and current RV."""
    pod = entries.get('egress')
    if pod is not None and pod['uid'] is None:
        raise AssertionError('unacknowledged Pod cannot authorize removal of its isolation policies')
    for role in ('egress', 'egress-service', 'agent-egress', 'egress-isolation'):
        entry = entries.get(role)
        if entry is None:
            continue
        current = _read(api, entry)
        if current is not None:
            assert entry['uid'] is not None, 'unknown original UID; refusing candidate adoption'
            _identity(current, entry)
            if current.metadata.deletion_timestamp is None:
                facade = api.networking if entry['kind'] == 'network_policy' else api
                getattr(facade, 'delete_namespaced_' + entry['kind'])(entry['name'], entry['namespace'],
                    body=client.V1DeleteOptions(preconditions=client.V1Preconditions(
                        uid=entry['uid'], resource_version=current.metadata.resource_version)), _request_timeout=(0.5, 0.5))
            deadline = time.monotonic() + 40
            while time.monotonic() < deadline:
                remaining = _read(api, entry)
                if remaining is None:
                    break
                _identity(remaining, entry)
                time.sleep(0.1)
            else:
                raise AssertionError('original Egress cleanup was not confirmed; no force/finalizer removal attempted')


@pytest.fixture
def owned_egress(tmp_path):
    """Open independent cleanup ownership after gates and cached-image checks; never touch existing identities."""
    from control_plane.runtime.kubernetes.kubernetes_runtime import create_probe_client
    _cached_images()
    path = Path(os.environ.get('KUBERNETES_TEST_KUBECONFIG', str(Path.home() / '.kube' / 'config')))
    context, namespace = os.environ['KUBERNETES_TEST_CONTEXT'], os.environ['KUBERNETES_TEST_NAMESPACE']
    cleanup = create_probe_client(kubeconfig_path=path, context=context)
    entries = {}
    receipt_path = tmp_path / 'egress-original-post-receipts.json'
    receipt_path.touch(mode=0o600)
    now = datetime.now(UTC)
    sid = 'sbx_' + secrets.token_hex(16)
    source = SandboxSpec(sandbox_id=sid, transaction_id='txn_' + secrets.token_hex(16),
        container_name='unused-container', network_name='unused-network', volume_name='unused-volume',
        created_at=now, expires_at=now + timedelta(minutes=5), network=SandboxNetwork(['example.com']))
    try:
        assert cleanup.read_namespace(namespace, _request_timeout=(0.5, 0.5)).metadata.name == namespace
        dns = cleanup.list_namespaced_pod('kube-system', label_selector='k8s-app=kube-dns',
            limit=4, watch=False, _request_timeout=(0.5, 0.5))
        assert dns.items and dns.metadata._continue in (None, '')
        yield dict(source=source, namespace=namespace, entries=entries, path=path, context=context, api=cleanup,
                   receipt_path=receipt_path)
    finally:
        primary = sys.exc_info()[1]
        try:
            _cleanup(cleanup, entries)
        except BaseException:
            if primary is None:
                raise
            primary.add_note('independent Egress cleanup did not confirm all original resources absent')
        finally:
            cleanup.api_client.close()


def test_original_egress_component_creation_inspection_and_cleanup(owned_egress):
    """Verify four real originals and allocated IPv4 without Agent/public requests; close on the owning worker."""
    h = owned_egress

    def exercise():
        """Create, inspect, derive env and delete using one caller-owned worker/client and no Runtime operations."""
        from control_plane.runtime.kubernetes.kubernetes_runtime import create_probe_client
        api = create_probe_client(kubeconfig_path=h['path'], context=h['context'])
        try:
            resource = EgressResources(RecordingAPI(api, h['entries'], h['receipt_path']), h['source'], namespace=h['namespace'],
                egress_image=IMAGE, dns_namespace='kube-system', dns_pod_labels={'k8s-app': 'kube-dns'},
                cluster_cidrs=['10.244.0.0/16', '10.96.0.0/12', '192.168.49.0/24'])
            try:
                snapshot = resource.create()
            except Exception as error:
                info = resource.creation_failure
                diagnostic = tuple((role, entry.get('defaulted_fields', ())) for role, entry in h['entries'].items())
                frames = tuple((frame.name, frame.lineno) for frame in traceback.extract_tb(error.__traceback__))
                raise AssertionError((type(error).__name__, info.phase, info.resource_state.value, diagnostic, frames)) from None
            assert len(h['entries']) == 4 and len(snapshot.resource_uids) == 4
            expected_uids = {'pod': h['entries']['egress']['uid'], 'service': h['entries']['egress-service']['uid'],
                             'agent_policy': h['entries']['agent-egress']['uid'], 'isolation': h['entries']['egress-isolation']['uid']}
            assert dict(snapshot.resource_uids) == expected_uids
            assert resource.inspect(snapshot) == snapshot
            variables = {item.name: item.value for item in resource.agent_proxy_env(snapshot)}
            assert variables['HTTP_PROXY'] == 'http://' + snapshot.service_ipv4 + ':18084'
            with pytest.raises(EgressResourceConflict):
                resource.delete(replace(snapshot, transaction_id='txn_' + 'f' * 32))
            deadline = time.monotonic() + 25
            while time.monotonic() < deadline:
                pod = _read(api, h['entries']['egress'])
                _identity(pod, h['entries']['egress'])
                if pod.status.phase == 'Running' and any(c.type == 'Ready' and c.status == 'True' for c in pod.status.conditions or []):
                    break
                assert pod.status.phase not in ('Failed', 'Succeeded')
                time.sleep(0.1)
            else:
                raise AssertionError('cached Egress Pod did not become ready; no public traffic was attempted')
            resource.delete(snapshot)
            resource.delete(snapshot)
            assert resource.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
            with pytest.raises(SandboxNotFound):
                resource.inspect(snapshot)
            for entry in h['entries'].values():
                assert _read(api, entry) is None
        finally:
            api.api_client.close()

    with ThreadPoolExecutor(max_workers=1) as worker:
        worker.submit(exercise).result(timeout=100)
    slices = client.DiscoveryV1Api(h['api'].api_client)
    entry = h['entries']['egress-service']
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        page = slices.list_namespaced_endpoint_slice(h['namespace'],
            label_selector='kubernetes.io/service-name=' + entry['name'], limit=3, watch=False, _request_timeout=(0.5, 0.5))
        assert page.metadata._continue in (None, '')
        if not page.items:
            break
        for item in page.items:
            assert any(owner.uid == entry['uid'] for owner in item.metadata.owner_references or [])
        time.sleep(0.1)
    else:
        raise AssertionError('test Service EndpointSlice cleanup was not confirmed')
