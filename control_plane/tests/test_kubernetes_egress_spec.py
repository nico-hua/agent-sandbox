"""Offline contracts for future Kubernetes egress specs; no lifecycle or CNI claims."""

import builtins
import copy
import importlib
import json
import os
import secrets
import socket
import sqlite3
import subprocess
from dataclasses import replace
from ipaddress import IPv4Address, IPv4Network
from pathlib import Path

import pytest
from kubernetes import client, config

from control_plane.core.errors import KubernetesConfigurationError, KubernetesOperationUnsupported, CreationResourceState
from control_plane.core.models import SandboxNetwork
from test_kubernetes_pod_spec import sandbox_spec
from test_kubernetes_query import SID, TXN, NAMESPACE

IMAGE = 'agent-sandbox-egress:dev'
PREFIX = 'io.agent-sandbox.'
ROOT = 'agent-sandbox-sbx-0123456789abcdef0123456789abcdef'


def builders():
    """Load only the tested pure module, making missing implementation a focused test failure."""
    return importlib.import_module('control_plane.runtime.kubernetes.egress_spec')


def spec():
    """Use explicit identity and core-normalized exact domains; Docker names must not locate Kubernetes objects."""
    return sandbox_spec(network=SandboxNetwork(['Example.COM.', 'api.example.com', 'example.com']), access_token='Q' * 43)


def network_inputs(**changes):
    """Configure only precise trusted DNS selectors and explicit private/public cluster address ranges."""
    values = dict(namespace=NAMESPACE, dns_namespace='kube-system', dns_pod_labels={'k8s-app': 'kube-dns'},
                  cluster_cidrs=['10.244.0.0/16', '10.96.0.0/12', '192.168.49.0/24', '11.24.0.0/16'])
    values.update(changes)
    return values


def test_egress_pod_reuses_identity_normalized_domains_and_direct_cli():
    """Wrong roles/times, shell startup or unnormalized wildcard-like flags would change the actual Pod contract."""
    pod = builders().build_egress_pod(spec(), namespace=NAMESPACE, egress_image=IMAGE)
    assert type(pod) is client.V1Pod and pod.api_version == 'v1' and pod.kind == 'Pod'
    assert pod.metadata.name == ROOT + '-egress' and pod.metadata.namespace == NAMESPACE
    assert pod.metadata.uid is None and pod.metadata.resource_version is None
    assert pod.metadata.labels == {
        PREFIX + 'managed': 'true', PREFIX + 'project': 'agent-sandbox',
        PREFIX + 'sandbox-id': SID, PREFIX + 'transaction-id': TXN,
        PREFIX + 'resource': 'egress', PREFIX + 'kubernetes-network-version': '1',
    }
    assert pod.metadata.annotations == {
        PREFIX + 'created-at': '2026-10-19T08:00:00.123456Z',
        PREFIX + 'expires-at': '2026-10-19T08:30:00.123456Z', PREFIX + 'egress-spec-version': '1',
    }
    assert pod.metadata.owner_references in (None, [])
    container, = pod.spec.containers
    assert container.name == 'egress' and container.image == IMAGE and container.image_pull_policy == 'Never'
    assert container.command == ['/usr/local/bin/egress-proxy']
    assert container.args == ['-listen', '0.0.0.0:18084', '-allow-domain', 'api.example.com', '-allow-domain', 'example.com']
    port, = container.ports
    assert (port.container_port, port.protocol, port.host_port, port.host_ip) == (18084, 'TCP', None, None)
    probe = container.readiness_probe
    assert probe.http_get.path == '/healthz' and probe.http_get.port == 18084
    assert probe.http_get.scheme == 'HTTP' and probe.http_get.host in (None, '')
    assert probe.http_get.http_headers in (None, []) and probe._exec is None and probe.tcp_socket is None
    assert probe.timeout_seconds == 1 and container.liveness_probe is None


def test_egress_pod_enforces_non_root_limits_and_contains_no_host_credentials_or_mounts(monkeypatch):
    """Any privilege, credential propagation or workspace/host mount would violate the trusted egress boundary."""
    monkeypatch.setenv('AGENT_SANDBOX_API_KEY', 'synthetic-host-key-do-not-inherit')
    monkeypatch.setenv('KUBECONFIG', '/synthetic/host-only')
    pod = builders().build_egress_pod(spec(), namespace=NAMESPACE, egress_image=IMAGE)
    container, = pod.spec.containers
    security = pod.spec.security_context
    assert security.run_as_non_root is True and security.run_as_user == security.run_as_group == 10001
    assert security.seccomp_profile.type == 'RuntimeDefault'
    assert container.security_context.allow_privilege_escalation is False
    assert container.security_context.privileged is False and container.security_context.read_only_root_filesystem is True
    assert container.security_context.capabilities.drop == ['ALL']
    assert not container.security_context.capabilities.add
    assert container.resources.requests == container.resources.limits == {'cpu': '500m', 'memory': '128Mi'}
    assert container.resources.requests is not container.resources.limits
    assert container.env in (None, []) and container.env_from in (None, [])
    assert container.volume_mounts in (None, []) and pod.spec.volumes in (None, [])
    assert pod.spec.init_containers in (None, []) and pod.spec.ephemeral_containers in (None, [])
    assert pod.spec.restart_policy == 'Never' and pod.spec.host_network is pod.spec.host_pid is pod.spec.host_ipc is False
    assert pod.spec.automount_service_account_token is False and pod.spec.enable_service_links is False
    assert pod.spec.dns_policy == 'ClusterFirst'
    encoded = json.dumps(pod.to_dict())
    assert all(value not in encoded for value in ('Q' * 43, 'synthetic-host-key-do-not-inherit', '/synthetic/host-only',
                                                  'docker_container_only', 'docker_network_only', 'docker_volume_only'))


def test_service_targets_only_matching_egress_without_exposure_or_unready_backends():
    """ClusterIP routing must select Egress rather than Agent/proxy and cannot expose a host or unready backend."""
    module = builders()
    source = spec()
    service = module.build_egress_service(source, namespace=NAMESPACE)
    assert service.kind == 'Service' and service.api_version == 'v1'
    assert service.metadata.name == ROOT + '-egress' and len(service.metadata.name) <= 63
    assert service.metadata.labels[PREFIX + 'resource'] == 'egress-service'
    assert service.metadata.labels[PREFIX + 'transaction-id'] == TXN
    assert service.metadata.annotations[PREFIX + 'created-at'] == '2026-10-19T08:00:00.123456Z'
    assert service.spec.type == 'ClusterIP' and service.spec.cluster_ip is None and not service.spec.cluster_ips
    assert service.spec.publish_not_ready_addresses is False
    assert service.spec.external_ips in (None, []) and service.spec.external_name is None
    assert service.spec.load_balancer_ip is None
    port, = service.spec.ports
    assert (port.port, port.target_port, port.protocol, port.node_port) == (18084, 18084, 'TCP', None)
    pod = module.build_egress_pod(source, namespace=NAMESPACE, egress_image=IMAGE)
    assert service.spec.selector == pod.metadata.labels
    for changed in (replace(source, sandbox_id='sbx_' + 'f' * 32), replace(source, transaction_id='txn_' + 'f' * 32)):
        other = module.build_egress_pod(changed, namespace=NAMESPACE, egress_image=IMAGE)
        assert not all(other.metadata.labels.get(k) == v for k, v in service.spec.selector.items())
    assert service.spec.selector[PREFIX + 'resource'] != 'container'


def test_policies_allow_both_sides_of_same_instance_flow_without_agent_dns_or_public_access():
    """Allowing only one direction or a wider peer/port would either block the proxy or leak direct Agent access."""
    agent, egress = builders().build_egress_network_policies(spec(), **network_inputs())
    assert agent.spec.policy_types == ['Egress'] and agent.spec.ingress in (None, [])
    assert agent.spec.pod_selector.match_labels[PREFIX + 'resource'] == 'container'
    assert agent.spec.pod_selector.match_labels[PREFIX + 'transaction-id'] == TXN
    assert agent.spec.pod_selector.match_expressions in (None, [])
    rule, = agent.spec.egress
    peer, = rule.to
    assert peer.pod_selector.match_labels[PREFIX + 'resource'] == 'egress'
    assert peer.pod_selector.match_labels[PREFIX + 'sandbox-id'] == SID
    assert peer.pod_selector.match_labels[PREFIX + 'transaction-id'] == TXN
    assert peer.namespace_selector is None and peer.ip_block is None
    port, = rule.ports
    assert (port.protocol, port.port, port.end_port) == ('TCP', 18084, None)
    assert egress.spec.policy_types == ['Ingress', 'Egress']
    assert egress.spec.pod_selector.match_labels == peer.pod_selector.match_labels
    incoming, = egress.spec.ingress
    sender, = incoming._from
    assert sender.pod_selector.match_labels == agent.spec.pod_selector.match_labels
    assert sender.namespace_selector is None and sender.ip_block is None
    port, = incoming.ports
    assert (port.protocol, port.port, port.end_port) == ('TCP', 18084, None)
    assert len(egress.spec.egress) == 2
    assert all(p.metadata.namespace == NAMESPACE and p.metadata.annotations[PREFIX + 'egress-spec-version'] == '1'
               for p in (agent, egress))


def test_dns_uses_intersection_of_exact_namespace_and_pod_labels_on_only_udp_tcp_53():
    """Splitting DNS selectors into separate peers would produce an unintended OR allowing the whole namespace."""
    _, egress = builders().build_egress_network_policies(spec(), **network_inputs())
    dns = egress.spec.egress[0]
    peer, = dns.to
    assert peer.namespace_selector.match_labels == {'kubernetes.io/metadata.name': 'kube-system'}
    assert peer.pod_selector.match_labels == {'k8s-app': 'kube-dns'}
    assert peer.namespace_selector.match_expressions in (None, []) and peer.pod_selector.match_expressions in (None, [])
    assert peer.ip_block is None
    assert {(p.protocol, p.port, p.end_port) for p in dns.ports} == {('UDP', 53, None), ('TCP', 53, None)}


@pytest.mark.parametrize('address', ['0.1.2.3', '10.1.2.3', '100.64.0.1', '127.0.0.1', '169.254.169.254',
    '172.16.4.1', '192.0.0.9', '192.0.2.1', '192.88.99.1', '192.168.49.1', '198.18.0.1',
    '198.51.100.1', '203.0.113.1', '224.0.0.1', '255.255.255.255', '11.24.1.2'])
def test_public_rule_excludes_private_special_and_explicit_public_cluster_networks(address):
    """A special address or public-numbered cluster range must not match Egress's ordinary web peer."""
    _, policy = builders().build_egress_network_policies(spec(), **network_inputs())
    web = policy.spec.egress[1]
    peer, = web.to
    assert peer.pod_selector is None and peer.namespace_selector is None
    assert peer.ip_block.cidr == '0.0.0.0/0'
    assert any(IPv4Address(address) in IPv4Network(prefix) for prefix in peer.ip_block._except)
    assert {(p.protocol, p.port, p.end_port) for p in web.ports} == {('TCP', 80, None), ('TCP', 443, None)}
    assert all(IPv4Network(prefix).prefixlen > 0 for prefix in peer.ip_block._except)


@pytest.mark.parametrize('address', ['1.1.1.1', '8.8.8.8', '93.184.216.34'])
def test_ordinary_public_ipv4_is_not_accidentally_denied_at_web_layer(address):
    """The address filter is a web-only layer; actual domain policy remains the Go proxy's job."""
    _, policy = builders().build_egress_network_policies(spec(), **network_inputs())
    peer = policy.spec.egress[1].to[0]
    assert not any(IPv4Address(address) in IPv4Network(prefix) for prefix in peer.ip_block._except)


@pytest.mark.parametrize('value', ['10.96.0.42', '172.20.0.9', '11.25.1.2'])
def test_agent_proxy_environment_uses_only_canonical_ipv4_and_loopback_bypass(value):
    """No Agent DNS lookup, broad bypass or URL-supplied upstream can replace the trusted Service IPv4."""
    variables = builders().build_agent_proxy_env(value)
    assert len(variables) == 6 and len({v.name for v in variables}) == 6
    assert {v.name: v.value for v in variables} == {
        'HTTP_PROXY': 'http://' + value + ':18084', 'http_proxy': 'http://' + value + ':18084',
        'HTTPS_PROXY': 'http://' + value + ':18084', 'https_proxy': 'http://' + value + ':18084',
        'NO_PROXY': 'localhost,127.0.0.1,::1', 'no_proxy': 'localhost,127.0.0.1,::1',
    }
    assert all(v.value_from is None for v in variables)


@pytest.mark.parametrize('value', [None, True, 123, '', '10.096.0.1', '10.96.0.1 ', '10.96.0.1\n',
    'http://10.96.0.1', '10.96.0.1:18084', 'egress', 'fd00::1', '0.0.0.0', '0.1.2.3',
    '127.0.0.1', '169.254.1.1', '224.0.0.1', '255.255.255.255', '10.1.1.1; injected'])
def test_proxy_environment_rejects_missing_non_unicast_or_injected_target(value):
    """Invalid address text is rejected wholly instead of becoming a proxy URL or bypass entry."""
    with pytest.raises(KubernetesConfigurationError):
        builders().build_agent_proxy_env(value)


@pytest.mark.parametrize('field,value', [
    ('dns_namespace', None), ('dns_namespace', ''), ('dns_namespace', 'kube-system '), ('dns_namespace', 'A'),
    ('dns_pod_labels', {}), ('dns_pod_labels', []), ('dns_pod_labels', {'k8s-app': ''}),
    ('dns_pod_labels', {'k8s-app': '*'}), ('dns_pod_labels', {'/bad': 'dns'}),
    ('dns_pod_labels', {'bad..prefix/name': 'dns'}), ('dns_pod_labels', {'a/b/c': 'dns'}),
    ('dns_pod_labels', {'k8s-app': True}), ('dns_pod_labels', {'a' * 64: 'dns'}),
    ('dns_pod_labels', {str(n): 'dns' for n in range(17)}),
    ('cluster_cidrs', None), ('cluster_cidrs', []), ('cluster_cidrs', '10.0.0.0/8'),
    ('cluster_cidrs', ['10.244.0.1/16']), ('cluster_cidrs', ['10.0.0.0/08']),
    ('cluster_cidrs', ['fd00::/64']), ('cluster_cidrs', ['0.0.0.0/0']),
    ('cluster_cidrs', ['10.0.0.0']), ('cluster_cidrs', [False]), ('cluster_cidrs', ['10.0.0.0/8'] * 33),
    ('cluster_cidrs', ['10.0.0.0/8', 'invalid']), ('cluster_cidrs', ['10.0.0.0/8\n']),
])
def test_network_configuration_is_bounded_explicit_and_rejected_as_a_whole(field, value):
    """Malformed/wide DNS and unknown cluster exclusion input cannot leave partially permissive output."""
    with pytest.raises(KubernetesConfigurationError):
        builders().build_egress_network_policies(spec(), **network_inputs(**{field: value}))


@pytest.mark.parametrize('field,value', [('sandbox_id', 'sbx_bad'), ('transaction_id', 'txn_bad'),
    ('created_at', None), ('expires_at', None), ('network', SandboxNetwork()), ('network', object())])
def test_incomplete_or_unsupported_source_identity_cannot_build_any_resource(field, value):
    """No builder can create canonical-looking companion objects from missing identity/time or a disabled policy."""
    source = spec()
    object.__setattr__(source, field, value)
    module = builders()
    for operation in (
        lambda: module.build_egress_pod(source, namespace=NAMESPACE, egress_image=IMAGE),
        lambda: module.build_egress_service(source, namespace=NAMESPACE),
        lambda: module.build_egress_network_policies(source, **network_inputs()),
    ):
        with pytest.raises(KubernetesConfigurationError):
            operation()


@pytest.mark.parametrize('domains', [('example.com', '*.invalid'), ('K.com',), ('127.0.0.1',),
    ('example.com',) * 33, ['example.com'], ('EXAMPLE.COM.',)])
def test_forged_noncanonical_domain_policy_cannot_be_silently_filtered_or_reinterpreted(domains):
    """A forged frozen input is revalidated, including normalization and the supplied-entry bound."""
    source = spec()
    object.__setattr__(source.network, 'allow_domains', domains)
    with pytest.raises(KubernetesConfigurationError):
        builders().build_egress_pod(source, namespace=NAMESPACE, egress_image=IMAGE)


@pytest.mark.parametrize('namespace,image', [('wrong namespace', IMAGE), ('', IMAGE), (NAMESPACE, ''),
    (NAMESPACE, 'https://registry/egress'), (NAMESPACE, 'registry/EGRESS:dev'), (NAMESPACE, 'egress@sha256:bad'),
    (NAMESPACE, 'egress:dev\n'), (NAMESPACE, 'repo;inject')])
def test_trusted_namespace_and_image_are_still_strict_configuration_inputs(namespace, image):
    """An image is trusted for content, not exempt from bounded reference or namespace syntax validation."""
    with pytest.raises(KubernetesConfigurationError):
        builders().build_egress_pod(spec(), namespace=namespace, egress_image=image)


def test_builders_copy_all_mutable_sources_and_never_perform_external_io(monkeypatch):
    """A build must not create clients or inspect host state, and no produced nested object aliases its inputs/siblings."""
    module, source, trusted = builders(), spec(), network_inputs()
    original = copy.deepcopy(source)

    def forbidden(*args, **kwargs):
        """Stop accidental resource/configuration lookup, identity allocation or host side effects."""
        pytest.fail('pure egress specification attempted external I/O or generated identity')

    with monkeypatch.context() as guard:
        for obj, name in ((client, 'ApiClient'), (config, 'load_kube_config'), (builtins, 'open'),
            (Path, 'open'), (Path, 'read_text'), (os, 'getenv'), (socket, 'socket'), (sqlite3, 'connect'),
            (subprocess, 'Popen'), (secrets, 'token_hex'), (secrets, 'token_urlsafe')):
            guard.setattr(obj, name, forbidden)
        a = module.build_egress_pod(source, namespace=NAMESPACE, egress_image=IMAGE)
        b = module.build_egress_pod(source, namespace=NAMESPACE, egress_image=IMAGE)
        service = module.build_egress_service(source, namespace=NAMESPACE)
        service2 = module.build_egress_service(source, namespace=NAMESPACE)
        agent, egress = module.build_egress_network_policies(source, **trusted)
        agent2, egress2 = module.build_egress_network_policies(source, **trusted)
        env = module.build_agent_proxy_env('10.96.0.42')
        env2 = module.build_agent_proxy_env('10.96.0.42')
    a.metadata.labels.clear()
    a.spec.containers[0].args.clear()
    service.spec.selector.clear()
    service.spec.ports[0].port = 1
    egress.spec.egress[0].to[0].pod_selector.match_labels.clear()
    agent.spec.egress[0].to[0].pod_selector.match_labels.clear()
    env[0].value = 'mutated'
    trusted['dns_pod_labels']['k8s-app'] = 'mutated'
    trusted['cluster_cidrs'].clear()
    assert source == original and b.spec.containers[0].args
    assert service2.spec.selector and service2.spec.ports[0].port == 18084
    assert service2.metadata.labels and b.metadata.labels
    assert egress2.spec.egress[0].to[0].pod_selector.match_labels == {'k8s-app': 'kube-dns'}
    assert agent2.spec.egress[0].to[0].pod_selector.match_labels[PREFIX + 'resource'] == 'egress'
    assert env2[0].value == 'http://10.96.0.42:18084'
    assert agent.spec.pod_selector.match_labels and egress.spec.pod_selector.match_labels


@pytest.mark.anyio
async def test_runtime_still_refuses_allow_domains_before_acquiring_client():
    """Pure specs cannot accidentally enable unversioned egress lifecycle through public Runtime creation."""
    from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime

    def forbidden(**kwargs):
        """Fail if an unsupported request ever reaches a backend client factory."""
        pytest.fail('Runtime acquired SDK before rejecting controlled egress')

    backend = KubernetesSandboxRuntime(kubeconfig_path='/never-read', context='explicit', namespace=NAMESPACE,
                                       client_factory=forbidden)
    with pytest.raises(KubernetesOperationUnsupported) as captured:
        await backend.create(spec())
    assert captured.value.creation_failure.resource_state is CreationResourceState.UNCHANGED


def test_combined_cluster_exclusions_cannot_collapse_to_invalid_default_route():
    """Several individually valid exclusions must not combine into an invalid ipBlock except equal to its base."""
    with pytest.raises(KubernetesConfigurationError):
        builders().build_egress_network_policies(spec(), **network_inputs(cluster_cidrs=['0.0.0.0/1', '128.0.0.0/1']))


def test_dns_qualified_label_and_maximum_cidr_bound_produce_fresh_limited_rules():
    """Accept bounded exact qualified Kubernetes selectors and all 32 entries without broadening DNS peers."""
    labels = {'example.io/dns-role': 'Recursive_DNS-1', 'k8s-app': 'kube-dns'}
    _, policy = builders().build_egress_network_policies(spec(), **network_inputs(
        dns_pod_labels=labels, cluster_cidrs=['10.0.0.0/8'] * 32))
    peer, = policy.spec.egress[0].to
    assert peer.pod_selector.match_labels == labels
    labels.clear()
    assert peer.pod_selector.match_labels == {'example.io/dns-role': 'Recursive_DNS-1', 'k8s-app': 'kube-dns'}
    assert len(policy.spec.egress) == 2 and len(policy.spec.ingress) == 1


def test_service_explicitly_requests_ipv4_for_the_tcp4_proxy_contract():
    """An IPv6-primary dual-stack cluster must not silently allocate an unusable default-family Service."""
    service = builders().build_egress_service(spec(), namespace=NAMESPACE)
    assert service.spec.ip_families == ['IPv4'] and service.spec.ip_family_policy == 'SingleStack'
