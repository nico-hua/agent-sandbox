"""Versioned Egress facts joining the existing component to authenticated Runtime layouts."""

import json
from dataclasses import dataclass

from control_plane.core.errors import KubernetesConfigurationError, SandboxDeleteConflict, SandboxQueryFailed
from control_plane.core.models import SandboxNetwork, SandboxSpec
from control_plane.runtime.kubernetes.egress_spec import _dns_selector, _public_exclusions, build_agent_proxy_env
from control_plane.runtime.kubernetes.pod_spec import validate_agent_image
from control_plane.runtime.kubernetes.pod_query import LABEL_PREFIX, lifecycle_annotations

EGRESS_LAYOUT = LABEL_PREFIX + 'kubernetes-egress-version'
EGRESS_FACTS = LABEL_PREFIX + 'egress-runtime-facts'
EGRESS_KEYS = frozenset({'egress_pod', 'egress_service', 'egress_agent_policy', 'egress_isolation'})
COMPONENT_KEYS = {'pod': 'egress_pod', 'service': 'egress_service',
                  'agent_policy': 'egress_agent_policy', 'isolation': 'egress_isolation'}
FACT_LIMIT = 20000


def _unique(pairs):
    """Reject duplicate JSON keys at every level rather than trusting the last conflicting value."""
    result = dict(pairs)
    if len(result) != len(pairs):
        raise ValueError()
    return result


def _nonfinite(value):
    """Reject JavaScript numeric extensions rather than permitting nonstandard JSON configuration."""
    raise ValueError()


def strict_json(raw, maximum=FACT_LIMIT):
    """Decode bounded JSON without duplicate keys, nonfinite constants or diagnostic disclosure."""
    try:
        if type(raw) is not str or not 2 <= len(raw) <= maximum:
            raise ValueError()
        return json.loads(raw, object_pairs_hook=_unique, parse_constant=_nonfinite)
    except (ValueError, TypeError, RecursionError):
        raise KubernetesConfigurationError() from None


@dataclass(frozen=True)
class EgressConfiguration:
    """Copy validated trusted inputs into tuples without acquiring clients or reading configuration."""

    image: str
    dns_namespace: str
    dns_pod_labels: tuple[tuple[str, str], ...]
    cluster_cidrs: tuple[str, ...]

    @classmethod
    def from_values(cls, image, dns_namespace, dns_pod_labels, cluster_cidrs):
        """Validate the complete group with the same pure policy/image rules used by component builders."""
        validate_agent_image(image)
        labels = _dns_selector(dns_namespace, dns_pod_labels)
        _public_exclusions(cluster_cidrs)
        if len(set(cluster_cidrs)) != len(cluster_cidrs):
            raise KubernetesConfigurationError()
        return cls(image, dns_namespace, tuple(sorted(labels.items())), tuple(cluster_cidrs))

    def arguments(self):
        """Produce fresh builder arguments from this immutable snapshot, never caller-owned dictionaries."""
        return dict(egress_image=self.image, dns_namespace=self.dns_namespace,
                    dns_pod_labels=dict(self.dns_pod_labels), cluster_cidrs=self.cluster_cidrs)


def encode_facts(snapshot):
    """Persist confirmed configuration/address only; original UIDs remain in the existing guarded receipt."""
    return json.dumps(dict(version='1', image=snapshot.egress_image, domains=list(snapshot.allow_domains),
        dns_namespace=snapshot.dns_namespace, dns_labels=dict(snapshot.dns_pod_labels),
        cluster_cidrs=list(snapshot.cluster_cidrs), service_ipv4=snapshot.service_ipv4),
        separators=(',', ':'), sort_keys=True)


def decode_facts(raw):
    """Validate a closed canonical configuration payload; decoding alone grants no object/delete authority."""
    try:
        facts = strict_json(raw)
        if (type(facts) is not dict or set(facts) != {'version','image','domains','dns_namespace','dns_labels','cluster_cidrs','service_ipv4'}
            or facts['version'] != '1' or type(facts['domains']) is not list or type(facts['cluster_cidrs']) is not list):
            raise ValueError()
        configuration = EgressConfiguration.from_values(facts['image'], facts['dns_namespace'], facts['dns_labels'], facts['cluster_cidrs'])
        domains = SandboxNetwork(facts['domains']).allow_domains
        if not domains or list(domains) != facts['domains']:
            raise ValueError()
        build_agent_proxy_env(facts['service_ipv4'])
        return configuration, domains, facts['service_ipv4']
    except (ValueError, TypeError, KeyError, KubernetesConfigurationError):
        raise SandboxQueryFailed() from None


def egress_layout(metadata):
    """Recognize only the explicit extension; missing facts remain incomplete, never downgrade an old layout."""
    annotations, labels = metadata.annotations or {}, getattr(metadata, "labels", None) or {}
    if EGRESS_LAYOUT in labels or EGRESS_FACTS in labels:
        raise SandboxQueryFailed()
    enabled = EGRESS_LAYOUT in annotations
    if enabled:
        if annotations[EGRESS_LAYOUT] != '1' or annotations.get(LABEL_PREFIX+'kubernetes-proxy-version') not in {'2','3','4'}:
            raise SandboxQueryFailed()
        if EGRESS_FACTS in annotations:
            decode_facts(annotations[EGRESS_FACTS])
    elif EGRESS_FACTS in annotations:
        raise SandboxQueryFailed()
    return enabled


def component_from_facts(api, metadata, bindings):
    """Rebuild expected specifications only after checking original published UID anchors and fixed business identity."""
    from control_plane.runtime.kubernetes.egress_resources import EgressResources, EgressSnapshot
    if not egress_layout(metadata) or not EGRESS_KEYS <= set(bindings):
        raise SandboxDeleteConflict()
    configuration, domains, address = decode_facts((metadata.annotations or {}).get(EGRESS_FACTS))
    created, expires = lifecycle_annotations(metadata.annotations)
    sid, txn = (metadata.labels.get(LABEL_PREFIX+key) for key in ('sandbox-id','transaction-id'))
    source = SandboxSpec(sandbox_id=sid, transaction_id=txn, created_at=created, expires_at=expires,
        container_name='unused', network_name='unused', volume_name='unused', network=SandboxNetwork(domains))
    component = EgressResources(api, source, namespace=metadata.namespace, **configuration.arguments(), runtime_publication=True)
    snapshot = EgressSnapshot(sid, txn, created, expires, metadata.namespace, configuration.image, domains,
        configuration.dns_namespace, configuration.dns_pod_labels, configuration.cluster_cidrs,
        tuple(sorted((key, bindings[value]) for key,value in COMPONENT_KEYS.items())), address)
    return component, snapshot


def verify_publication(metadata, expected_config):
    """Admit only the explicit Runtime extension on component objects and bind its facts to original blueprints."""
    from control_plane.runtime.kubernetes.proxy_resources import _MARKERS, _record
    annotations = metadata.annotations or {}
    if (not egress_layout(metadata) or EGRESS_FACTS not in annotations
        or any(key.startswith(LABEL_PREFIX) and key[len(LABEL_PREFIX):] not in _MARKERS
               for key in (*(metadata.labels or {}), *annotations))):
        raise SandboxQueryFailed()
    configuration, domains, _ = decode_facts(annotations[EGRESS_FACTS])
    if (configuration.image, domains, configuration.dns_namespace, configuration.dns_pod_labels,
        configuration.cluster_cidrs) != expected_config[5:]:
        raise SandboxQueryFailed()
    # Receipt decoding remains closed and canonical; the outer manager checks
    # every actual UID against agreed survivor receipts before using a snapshot.
    _record(metadata)


def egress_state(pod):
    """Interpret fixed Egress health evidence independently from integrity and public-network reachability."""
    from control_plane.runtime.kubernetes.pod_query import _WAITING_STATES, _valid_observation_time
    try:
        if pod.metadata.deletion_timestamp is not None:
            return 'failed'
        status = pod.status
        if status is None or status.phase in (None,'Pending') and not status.container_statuses:
            return 'starting'
        if status.phase in {'Failed','Succeeded'}:
            return 'failed'
        if status.phase not in {'Pending','Running'}:
            raise ValueError()
        if not status.container_statuses or len(status.container_statuses) != 1:
            raise ValueError()
        observed = status.container_statuses[0]
        state = observed.state
        if observed.name != 'egress' or state is None or sum(x is not None for x in (state.running,state.waiting,state.terminated)) != 1:
            raise ValueError()
        if state.terminated is not None:
            return 'failed'
        if state.waiting is not None:
            if state.waiting.reason not in _WAITING_STATES:
                raise ValueError()
            return _WAITING_STATES[state.waiting.reason][0]
        if status.phase != 'Running' or not _valid_observation_time(state.running.started_at):
            raise ValueError()
        ready = [c.status for c in status.conditions or [] if c.type=='Ready']
        if len(ready) != 1 or ready[0] not in {'True','False','Unknown'} or type(observed.ready) is not bool:
            raise ValueError()
        return 'ready' if observed.ready and ready == ['True'] else 'running'
    except (AttributeError,TypeError,ValueError):
        raise SandboxQueryFailed() from None
