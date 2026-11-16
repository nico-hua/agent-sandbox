"""Pure future egress specifications; no client, lifecycle registration or network-policy activation."""

import re
from collections.abc import Mapping
from dataclasses import replace
from ipaddress import IPv4Address, IPv4Network, collapse_addresses

from kubernetes import client

from control_plane.core.errors import KubernetesConfigurationError, SandboxNotFound
from control_plane.core.models import SandboxSpec, format_utc_datetime
from control_plane.runtime.kubernetes.pod_query import LABEL_PREFIX, primary_pod_labels, sandbox_pod_name
from control_plane.runtime.kubernetes.pod_spec import _validate_backend_configuration, validate_agent_image

EGRESS_PORT = 18084
EGRESS_SPEC_VERSION = "1"
EGRESS_SPEC_ANNOTATION = LABEL_PREFIX + "egress-spec-version"
# Mirror egress_proxy/network/policy.go's deliberately conservative special-use
# ranges. NetworkPolicy is additive and is not the Go proxy's domain/IP checker.
_DENIED_IPV4 = (
    "0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16",
    "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16",
    "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
)
_DNS_LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
_LABEL_PART = r"[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?"


def egress_pod_name(sandbox_id: str) -> str:
    """Derive the dedicated egress Pod's DNS-compatible name from the public identity."""
    return sandbox_pod_name(sandbox_id) + "-egress"


def egress_service_name(sandbox_id: str) -> str:
    """Use the deterministic egress name in the separate Service kind, staying within 63 characters."""
    return egress_pod_name(sandbox_id)


def _source(spec: SandboxSpec, namespace: str) -> SandboxSpec:
    """Revalidate explicit identity, times and canonical nonempty policy without allocating or sampling anything."""
    try:
        if type(spec) is not SandboxSpec:
            raise ValueError()
        spec = replace(spec)
        _validate_backend_configuration(namespace, "egress")
        sandbox_pod_name(spec.sandbox_id)
        if (type(spec.transaction_id) is not str or re.fullmatch(r"txn_[0-9a-f]{32}", spec.transaction_id) is None
            or spec.created_at is None or spec.expires_at is None or not spec.network.allow_domains):
            raise ValueError()
        return spec
    except (ValueError, TypeError, AttributeError, SandboxNotFound):
        raise KubernetesConfigurationError() from None


def _metadata(spec: SandboxSpec, namespace: str, *, name: str, role: str) -> client.V1ObjectMeta:
    """Copy only canonical ownership, original business times and the isolated specification version."""
    return client.V1ObjectMeta(
        name=name, namespace=namespace,
        labels={**primary_pod_labels(spec.sandbox_id, spec.transaction_id), LABEL_PREFIX + "resource": role},
        annotations={LABEL_PREFIX + "created-at": format_utc_datetime(spec.created_at),
                     LABEL_PREFIX + "expires-at": format_utc_datetime(spec.expires_at),
                     EGRESS_SPEC_ANNOTATION: EGRESS_SPEC_VERSION},
    )


def build_egress_pod(spec: SandboxSpec, *, namespace: str, egress_image: str) -> client.V1Pod:
    """Build one trusted non-root Go proxy with exact normalized argv, no credentials, mounts or external I/O."""
    spec = _source(spec, namespace)
    _validate_backend_configuration(namespace, egress_image)
    validate_agent_image(egress_image)
    container = client.V1Container(
        name="egress", image=egress_image, image_pull_policy="Never",
        command=["/usr/local/bin/egress-proxy"],
        args=["-listen", "0.0.0.0:18084", *[argument for domain in spec.network.allow_domains
                                         for argument in ("-allow-domain", domain)]],
        ports=[client.V1ContainerPort(name="egress-http", container_port=EGRESS_PORT, protocol="TCP")],
        # Kubelet requests the local health path directly; it does not traverse
        # an outbound HTTP proxy or depend on DNS/public egress being available.
        readiness_probe=client.V1Probe(
            http_get=client.V1HTTPGetAction(path="/healthz", port=EGRESS_PORT, scheme="HTTP"),
            period_seconds=1, timeout_seconds=1, failure_threshold=3,
        ),
        resources=client.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "128Mi"}, limits={"cpu": "500m", "memory": "128Mi"},
        ),
        security_context=client.V1SecurityContext(
            privileged=False, allow_privilege_escalation=False, read_only_root_filesystem=True,
            capabilities=client.V1Capabilities(drop=["ALL"]),
        ),
    )
    return client.V1Pod(
        api_version="v1", kind="Pod",
        metadata=_metadata(spec, namespace, name=egress_pod_name(spec.sandbox_id), role="egress"),
        spec=client.V1PodSpec(
            containers=[container], restart_policy="Never", host_network=False, host_pid=False, host_ipc=False,
            automount_service_account_token=False, enable_service_links=False, dns_policy="ClusterFirst",
            security_context=client.V1PodSecurityContext(
                run_as_non_root=True, run_as_user=10001, run_as_group=10001,
                seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault"),
            ),
        ),
    )


def build_egress_service(spec: SandboxSpec, *, namespace: str) -> client.V1Service:
    """Build only an unallocated internal TCP Service selecting this transaction's Egress Pod."""
    spec = _source(spec, namespace)
    return client.V1Service(
        api_version="v1", kind="Service",
        metadata=_metadata(spec, namespace, name=egress_service_name(spec.sandbox_id), role="egress-service"),
        spec=client.V1ServiceSpec(
            type="ClusterIP", publish_not_ready_addresses=False,
            ip_families=["IPv4"], ip_family_policy="SingleStack",
            selector={**primary_pod_labels(spec.sandbox_id, spec.transaction_id), LABEL_PREFIX + "resource": "egress"},
            ports=[client.V1ServicePort(name="egress-http", protocol="TCP", port=EGRESS_PORT, target_port=EGRESS_PORT)],
        ),
    )


def _dns_selector(namespace: str, labels: Mapping[str, str]) -> dict[str, str]:
    """Validate bounded exact DNS namespace/Pod selection, rejecting empty or wildcard-like selectors."""
    if (type(namespace) is not str or re.fullmatch(_DNS_LABEL, namespace) is None
        or not isinstance(labels, Mapping) or not 1 <= len(labels) <= 16):
        raise KubernetesConfigurationError()
    copied = dict(labels)
    for key, value in copied.items():
        if type(key) is not str or len(key) > 317 or type(value) is not str:
            raise KubernetesConfigurationError()
        prefix, slash, name = key.rpartition("/")
        if not slash:
            name = key
        if (re.fullmatch(_LABEL_PART, name) is None or re.fullmatch(_LABEL_PART, value) is None
            or (slash and (not prefix or len(prefix) > 253
                          or any(re.fullmatch(_DNS_LABEL, part) is None for part in prefix.split("."))))):
            raise KubernetesConfigurationError()
    return copied


def _public_exclusions(cluster_cidrs: list[str] | tuple[str, ...]) -> list[str]:
    """Merge explicit cluster networks with fixed special-use exclusions without accepting malformed entries."""
    try:
        if type(cluster_cidrs) not in (list, tuple) or not 1 <= len(cluster_cidrs) <= 32:
            raise ValueError()
        networks = [IPv4Network(value) for value in _DENIED_IPV4]
        for value in cluster_cidrs:
            if type(value) is not str or not 1 <= len(value) <= 18:
                raise ValueError()
            network = IPv4Network(value, strict=True)
            if str(network) != value or network.prefixlen == 0:
                raise ValueError()
            networks.append(network)
        collapsed = list(collapse_addresses(networks))
        # Kubernetes requires except prefixes to be strictly smaller than the
        # enclosing block; two accepted halves must not turn into except /0.
        if any(network.prefixlen == 0 for network in collapsed):
            raise ValueError()
        return [str(network) for network in collapsed]
    except (ValueError, TypeError):
        raise KubernetesConfigurationError() from None


def build_egress_network_policies(
    spec: SandboxSpec, *, namespace: str, dns_namespace: str, dns_pod_labels: Mapping[str, str],
    cluster_cidrs: list[str] | tuple[str, ...],
) -> tuple[client.V1NetworkPolicy, client.V1NetworkPolicy]:
    """Build both sides of Agent→Egress plus Egress-only exact DNS and IPv4 web access; never activate rules."""
    spec = _source(spec, namespace)
    dns_labels = _dns_selector(dns_namespace, dns_pod_labels)
    exclusions = _public_exclusions(cluster_cidrs)
    agent_labels = primary_pod_labels(spec.sandbox_id, spec.transaction_id)
    egress_labels = {**agent_labels, LABEL_PREFIX + "resource": "egress"}
    agent = client.V1NetworkPolicy(
        api_version="networking.k8s.io/v1", kind="NetworkPolicy",
        metadata=_metadata(spec, namespace, name=sandbox_pod_name(spec.sandbox_id) + "-egress-out", role="agent-egress"),
        spec=client.V1NetworkPolicySpec(
            pod_selector=client.V1LabelSelector(match_labels=dict(agent_labels)), policy_types=["Egress"],
            egress=[client.V1NetworkPolicyEgressRule(
                to=[client.V1NetworkPolicyPeer(pod_selector=client.V1LabelSelector(match_labels=dict(egress_labels)))],
                ports=[client.V1NetworkPolicyPort(protocol="TCP", port=EGRESS_PORT)],
            )],
        ),
    )
    isolated = client.V1NetworkPolicy(
        api_version="networking.k8s.io/v1", kind="NetworkPolicy",
        metadata=_metadata(spec, namespace, name=sandbox_pod_name(spec.sandbox_id) + "-egress-deny", role="egress-isolation"),
        spec=client.V1NetworkPolicySpec(
            pod_selector=client.V1LabelSelector(match_labels=dict(egress_labels)), policy_types=["Ingress", "Egress"],
            ingress=[client.V1NetworkPolicyIngressRule(
                _from=[client.V1NetworkPolicyPeer(pod_selector=client.V1LabelSelector(match_labels=dict(agent_labels)))],
                ports=[client.V1NetworkPolicyPort(protocol="TCP", port=EGRESS_PORT)],
            )],
            egress=[
                client.V1NetworkPolicyEgressRule(
                    # These selectors must share ONE peer (intersection). Two
                    # peers would OR them and allow the entire DNS namespace.
                    to=[client.V1NetworkPolicyPeer(
                        namespace_selector=client.V1LabelSelector(match_labels={"kubernetes.io/metadata.name": dns_namespace}),
                        pod_selector=client.V1LabelSelector(match_labels=dict(dns_labels)),
                    )],
                    ports=[client.V1NetworkPolicyPort(protocol="UDP", port=53),
                           client.V1NetworkPolicyPort(protocol="TCP", port=53)],
                ),
                client.V1NetworkPolicyEgressRule(
                    to=[client.V1NetworkPolicyPeer(ip_block=client.V1IPBlock(cidr="0.0.0.0/0", _except=exclusions))],
                    ports=[client.V1NetworkPolicyPort(protocol="TCP", port=80),
                           client.V1NetworkPolicyPort(protocol="TCP", port=443)],
                ),
            ],
        ),
    )
    return agent, isolated


def build_agent_proxy_env(service_ipv4: str) -> list[client.V1EnvVar]:
    """Build proxy URLs from an already identity-verified Service IPv4, not an arbitrary URL or inferred Service."""
    try:
        if type(service_ipv4) is not str or not 7 <= len(service_ipv4) <= 15:
            raise ValueError()
        address = IPv4Address(service_ipv4)
        if (str(address) != service_ipv4 or address.is_unspecified or address.is_loopback
            or address.is_multicast or address.is_link_local or address.is_reserved
            or address in IPv4Network("0.0.0.0/8")):
            raise ValueError()
    except (ValueError, TypeError):
        raise KubernetesConfigurationError() from None
    endpoint = f"http://{service_ipv4}:{EGRESS_PORT}"
    return [client.V1EnvVar(name=name, value=endpoint) for name in
            ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy")] + [
        client.V1EnvVar(name=name, value="localhost,127.0.0.1,::1") for name in ("NO_PROXY", "no_proxy")
    ]
