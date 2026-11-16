"""Borrowed-client standalone Egress lifecycle with original POST receipts, never Runtime registration."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from threading import Event
from time import monotonic, sleep
from uuid import UUID

from kubernetes import client

from control_plane.core.errors import (
    CleanupIssue, CreationFailureInfo, CreationResourceState, KubernetesError,
    KubernetesConfigurationError, KubernetesTimeout, SandboxNotFound,
)
from control_plane.runtime.kubernetes.egress_spec import (
    build_egress_pod, build_egress_service, build_egress_network_policies, build_agent_proxy_env,
)
from control_plane.runtime.kubernetes.failures import _probe_failure
from control_plane.runtime.kubernetes.pod_query import LABEL_PREFIX, sandbox_pod_name, sandbox_pod_selector
from control_plane.runtime.kubernetes.proxy_resources import _spec_matches

OPERATION_TIMEOUT_SECONDS = 5.0
CLEANUP_TIMEOUT_SECONDS = 40.0
RECONCILE_TIMEOUT_SECONDS = 1.0
POLL_SECONDS = 0.1
_ORDER = ("isolation", "agent_policy", "service", "pod")
_KINDS = {"isolation": "network_policy", "agent_policy": "network_policy", "service": "service", "pod": "pod"}
_ROLES = {"isolation": "egress-isolation", "agent_policy": "agent-egress", "service": "egress-service", "pod": "egress"}
_OTHER = {
    "pod": {"container": "", "proxy": "-proxy"},
    "service": {"agent-service": "-agent", "proxy-service": "-proxy"},
    "network_policy": {"network-policy": "-deny", "agent-proxy-ingress": "-proxy-in", "proxy-isolation": "-proxy-deny"},
}
_TYPE_META = {"pod": ("v1", "Pod"), "service": ("v1", "Service"), "network_policy": ("networking.k8s.io/v1", "NetworkPolicy")}
_REJECTED = frozenset({400, 401, 403, 404, 405, 409, 413, 415, 422, 429})


class EgressResourceConflict(KubernetesError):
    """Reject ambiguous discovery, replacement identity or a changed component contract."""

    message = "kubernetes egress resource conflict"


class EgressResourceIncomplete(KubernetesError):
    """Report a readable original component with missing resources, without pretending it is healthy."""

    message = "kubernetes egress resources are incomplete"


class EgressOperationFailed(KubernetesError):
    """Report a failed or cancelled component operation with separately retained resource evidence."""

    message = "kubernetes egress operation failed"


@dataclass(frozen=True)
class EgressSnapshot:
    """Keep immutable original creation/configuration facts; this is not a durable metadata protocol."""

    sandbox_id: str
    transaction_id: str
    created_at: datetime
    expires_at: datetime
    namespace: str
    egress_image: str
    allow_domains: tuple[str, ...]
    dns_namespace: str
    dns_pod_labels: tuple[tuple[str, str], ...]
    cluster_cidrs: tuple[str, ...]
    resource_uids: tuple[tuple[str, str], ...]
    service_ipv4: str


def _uid(value):
    """Accept only nonempty canonical API UIDs, never an arbitrary name or later candidate."""
    try:
        if type(value) is not str or str(UUID(value)) != value:
            raise ValueError()
    except (ValueError, AttributeError, TypeError):
        raise EgressResourceConflict() from None
    return value


class EgressResources:
    """Own only in-memory four-resource evidence; the caller owns SDK retries, threads and transport closure."""

    def __init__(self, api, spec, *, namespace, egress_image, dns_namespace, dns_pod_labels, cluster_cidrs,
                 cancelled: Event | None = None, runtime_publication: bool = False):
        """Copy and validate all trusted configuration through pure builders, without performing I/O."""
        source = deepcopy(spec)
        dns_labels, cidrs = deepcopy(dns_pod_labels), deepcopy(cluster_cidrs)
        pod = build_egress_pod(source, namespace=namespace, egress_image=egress_image)
        service = build_egress_service(source, namespace=namespace)
        agent, isolation = build_egress_network_policies(source, namespace=namespace, dns_namespace=dns_namespace,
            dns_pod_labels=dns_labels, cluster_cidrs=cidrs)
        if cancelled is not None and not isinstance(cancelled, Event):
            raise KubernetesConfigurationError()
        self.api, self.networking = api, api.networking
        self._blueprints = {"isolation": isolation, "agent_policy": agent, "service": service, "pod": pod}
        self._config = (source.sandbox_id, source.transaction_id, source.created_at, source.expires_at, namespace,
                        egress_image, source.network.allow_domains, dns_namespace, tuple(sorted(dns_labels.items())), tuple(cidrs))
        self._namespace, self._sandbox_id = namespace, source.sandbox_id
        self._runtime_publication = runtime_publication
        self._cancelled = cancelled
        self._uids, self._submitted, self._rejected, self._removed = {}, set(), set(), set()
        self._uncertain_posts, self._present = set(), set()
        self._create_called = self._cleanup_uncertain = False
        self._issues = ()
        self._deadline, self._phase = 0.0, "egress_create"
        self._service_ipv4 = None

    @property
    def original_uids(self):
        """Expose a copied immutable tuple; candidates and unacknowledged submissions never enter it."""
        return tuple(sorted(self._uids.items()))

    @property
    def submitted(self):
        """Retain every dispatched POST even if its response later proves a definite rejection."""
        return tuple(key for key in _ORDER if key in self._submitted)

    @property
    def creation_failure(self):
        """Aggregate all four resources without discarding uncertain writes or earlier cleanup evidence."""
        unacknowledged = self._submitted - self._rejected - set(self._uids)
        if self._uncertain_posts or unacknowledged or self._cleanup_uncertain:
            state = CreationResourceState.UNKNOWN
        elif self._uids and set(self._uids) <= self._removed:
            state = CreationResourceState.ROLLED_BACK
        elif self._uids:
            state = CreationResourceState.REMAINING
        else:
            state = CreationResourceState.UNCHANGED
        return CreationFailureInfo(self._phase, state, self._issues)

    def _call(self, operation, *args, _post_key=None, **kwargs):
        """Bound both SDK phases by the remaining monotonic operation budget; never retry a write."""
        remaining = self._deadline - monotonic()
        if remaining <= 0:
            raise KubernetesTimeout()
        phase = min(0.5, remaining / 2)
        if _post_key is not None:
            self._submitted.add(_post_key)
        return operation(*args, _request_timeout=(phase, phase), **kwargs)

    def _error(self, error):
        """Use only stable domain categories, preserving evidence without copying SDK response text."""
        if isinstance(error, KubernetesError):
            fixed = error
        elif isinstance(error, SandboxNotFound):
            fixed = SandboxNotFound("kubernetes egress resources not found")
        elif isinstance(error, client.ApiException) and error.status == 409:
            fixed = EgressResourceConflict()
        elif isinstance(error, client.ApiException) and error.status == 404:
            fixed = KubernetesConfigurationError()
        elif isinstance(error, client.ApiException) and error.status in {400, 405, 413, 415, 422, 429}:
            fixed = EgressOperationFailed()
        else:
            fixed = _probe_failure(error)
        fixed.creation_failure = self.creation_failure
        return fixed

    def _namespace_exists(self):
        """Require a positive namespace observation before interpreting any object 404 as absence."""
        namespace = self._call(self.api.read_namespace, self._namespace)
        if namespace.metadata is None or namespace.metadata.name != self._namespace:
            raise KubernetesConfigurationError()

    def _facade(self, key):
        """Borrow exactly the caller's Core/Networking facade, with no new pool or ownership transfer."""
        return self.networking if _KINDS[key] == "network_policy" else self.api

    def _identity(self, key, resource, *, role=None, name=None):
        """Verify original platform identity independently of readiness and mutable routing/security configuration."""
        try:
            expected = self._blueprints[key].metadata
            api_version, kind = _TYPE_META[_KINDS[key]]
            metadata = resource.metadata
            labels, annotations = metadata.labels, metadata.annotations
            wanted_labels = dict(expected.labels)
            if role is not None:
                wanted_labels[LABEL_PREFIX + "resource"] = role
            wanted_annotations = expected.annotations if role is None else {
                k: v for k, v in expected.annotations.items() if k != LABEL_PREFIX + "egress-spec-version"}
            if (resource.api_version != api_version or resource.kind != kind
                or metadata.name != (name or expected.name) or metadata.namespace != self._namespace
                or not isinstance(labels, dict) or not isinstance(annotations, dict)
                or any(labels.get(k) != v for k, v in wanted_labels.items())
                or any(annotations.get(k) != v for k, v in wanted_annotations.items())
                or metadata.owner_references not in (None, [])):
                raise ValueError()
            extension = self._runtime_publication and 'io.agent-sandbox.kubernetes-egress-version' in annotations
            if role is None and extension:
                from control_plane.runtime.kubernetes.egress_runtime import verify_publication
                verify_publication(metadata, self._config)
            if role is None and not extension and any(k.startswith(LABEL_PREFIX) and k not in wanted
                    for values, wanted in ((labels, wanted_labels), (annotations, wanted_annotations)) for k in values):
                raise ValueError()
            version = metadata.resource_version
            if (type(version) is not str or not 1 <= len(version) <= 256 or not version.isascii()
                or any(ord(c) <= 32 or ord(c) == 127 for c in version)):
                raise ValueError()
            return _uid(metadata.uid), version
        except (ValueError, TypeError, AttributeError, KeyError):
            raise EgressResourceConflict() from None

    def _read(self, key):
        """Read the deterministic name and distinguish object absence from namespace/backend failure."""
        try:
            return self._call(getattr(self._facade(key), "read_namespaced_" + _KINDS[key]),
                              self._blueprints[key].metadata.name, self._namespace)
        except client.ApiException as error:
            if error.status != 404:
                raise
            self._namespace_exists()
            return None

    def _inventory(self):
        """Cross-check bounded same-sandbox role discovery with exact reads, without hiding old transactions."""
        self._namespace_exists()
        listed = {}
        for kind, others in _OTHER.items():
            facade = self.networking if kind == "network_policy" else self.api
            relevant = {role: key for key, role in _ROLES.items() if _KINDS[key] == kind}
            page = self._call(getattr(facade, "list_namespaced_" + kind), self._namespace,
                label_selector=sandbox_pod_selector(self._sandbox_id), limit=len(others) + len(relevant) + 1, watch=False)
            version, type_name = _TYPE_META[kind]
            try:
                if (page.api_version != version or page.kind != type_name + "List" or page.metadata is None
                    or not isinstance(page.items, list) or len(page.items) > len(others) + len(relevant)
                    or page.metadata._continue not in (None, "") or page.metadata.remaining_item_count not in (None, 0)):
                    raise ValueError()
                seen = set()
                for item in page.items:
                    # Collection items can omit TypeMeta; only supply missing
                    # fields from the already verified collection envelope.
                    item = deepcopy(item)
                    if item.api_version is None:
                        item.api_version = version
                    if item.kind is None:
                        item.kind = type_name
                    role = item.metadata.labels[LABEL_PREFIX + "resource"]
                    if role in seen or role not in relevant and role not in others:
                        raise ValueError()
                    seen.add(role)
                    if role in relevant:
                        key = relevant[role]
                        listed[key] = self._identity(key, item)
                    else:
                        representative = next(k for k in _ORDER if _KINDS[k] == kind)
                        self._identity(representative, item, role=role,
                                       name=sandbox_pod_name(self._sandbox_id) + others[role])
            except (AttributeError, TypeError, KeyError, ValueError):
                raise EgressResourceConflict() from None
        found = {}
        for key in _ORDER:
            item = self._read(key)
            if item is None:
                if key in listed:
                    raise EgressResourceConflict()
                continue
            # Both observations validate canonical metadata. RV may advance on
            # status-only updates; continuity is the UID, not a stale list RV.
            # Configuration is verified on this exact read, and DELETE obtains
            # another current RV for its UID/RV precondition.
            if key not in listed or self._identity(key, item)[0] != listed[key][0]:
                raise EgressResourceConflict()
            found[key] = item
        return found

    def _configuration(self, key, resource):
        """Compare the original pure blueprint with only known API allocation/default fields admitted."""
        try:
            version, kind = _TYPE_META[_KINDS[key]]
            if resource.api_version != version or resource.kind != kind or resource.metadata.deletion_timestamp is not None:
                raise ValueError()
            expected, actual = self._blueprints[key].spec.to_dict(), resource.spec.to_dict()
            if key == "service":
                address = resource.spec.cluster_ip
                build_agent_proxy_env(address)
                if resource.spec.cluster_ips not in (None, [address]):
                    raise ValueError()
                expected["cluster_ip"], expected["cluster_ips"] = address, resource.spec.cluster_ips
                if actual["publish_not_ready_addresses"] not in (None, False):
                    raise ValueError()
                expected["publish_not_ready_addresses"] = actual["publish_not_ready_addresses"]
                for field, default in (("session_affinity", "None"), ("internal_traffic_policy", "Cluster")):
                    if actual[field] not in (None, default):
                        raise ValueError()
                    expected[field] = actual[field]
            if not _spec_matches(expected, actual):
                raise ValueError()
            return resource.spec.cluster_ip if key == "service" else None
        except (ValueError, TypeError, AttributeError, KubernetesConfigurationError):
            raise EgressResourceConflict() from None

    def _validate_snapshot(self, snapshot):
        """Accept only a complete original snapshot matching this frozen target and immutable configuration."""
        try:
            if type(snapshot) is not EgressSnapshot or tuple(getattr(snapshot, name) for name in (
                "sandbox_id", "transaction_id", "created_at", "expires_at", "namespace", "egress_image", "allow_domains",
                "dns_namespace", "dns_pod_labels", "cluster_cidrs")) != self._config:
                raise ValueError()
            entries = snapshot.resource_uids
            if type(entries) is not tuple or len(entries) != 4 or any(type(p) is not tuple or len(p) != 2 for p in entries):
                raise ValueError()
            uids = dict(entries)
            if set(uids) != set(_ORDER) or len(set(uids.values())) != 4:
                raise ValueError()
            # A failed attempt cannot use a caller-supplied snapshot to promote
            # candidates/unsubmitted roles. Only a fresh helper may borrow a
            # complete trusted snapshot; existing attempts retain exact proof.
            if self._create_called and uids != self._uids:
                raise ValueError()
            for key, uid in uids.items():
                _uid(uid)
                if key in self._uids and self._uids[key] != uid:
                    raise ValueError()
            build_agent_proxy_env(snapshot.service_ipv4)
            return uids
        except (ValueError, TypeError, AttributeError, KubernetesConfigurationError):
            raise EgressResourceConflict() from None

    def _inspect(self, snapshot):
        """Observe all identities/configuration without interpreting a Pending Pod as a missing component."""
        uids = self._validate_snapshot(snapshot)
        found = self._inventory()
        if not found:
            raise SandboxNotFound()
        for key, resource in found.items():
            if self._identity(key, resource)[0] != uids[key]:
                raise EgressResourceConflict()
            address = self._configuration(key, resource)
            if key == "service" and address != snapshot.service_ipv4:
                raise EgressResourceConflict()
        if set(found) != set(_ORDER):
            raise EgressResourceIncomplete()
        return snapshot

    def inspect(self, snapshot: EgressSnapshot) -> EgressSnapshot:
        """Read only the supplied original snapshot; no health requests, mutations or UID adoption occur."""
        self._deadline = monotonic() + OPERATION_TIMEOUT_SECONDS
        try:
            return self._inspect(snapshot)
        except Exception as error:
            raise self._error(error) from None

    def agent_proxy_env(self, snapshot: EgressSnapshot):
        """Reverify the full component and original Service before deriving Agent proxy configuration."""
        return build_agent_proxy_env(self.inspect(snapshot).service_ipv4)

    def _check_cancelled(self):
        """Stop only at safe SDK boundaries; cleanup intentionally ignores repeat cancellation signals."""
        if self._cancelled is not None and self._cancelled.is_set():
            raise EgressOperationFailed()

    def create(self) -> EgressSnapshot:
        """Submit each resource once, preserving original acknowledgements and compensating only proven originals."""
        if self._create_called or self._submitted or self._uids:
            raise self._error(EgressOperationFailed()) from None
        self._create_called = True
        self._deadline, self._phase = monotonic() + OPERATION_TIMEOUT_SECONDS, "egress_preflight"
        try:
            self._check_cancelled()
            if self._inventory():
                raise EgressResourceConflict()
            for key in _ORDER:
                self._check_cancelled()
                self._phase = "egress_create_" + key
                # Check the budget before recording dispatch; a local timeout
                # must not manufacture an unacknowledged write.
                if monotonic() >= self._deadline:
                    raise KubernetesTimeout()
                try:
                    resource = self._call(getattr(self._facade(key), "create_namespaced_" + _KINDS[key]),
                        self._namespace, body=deepcopy(self._blueprints[key]), _post_key=key)
                except client.ApiException as error:
                    if error.status in _REJECTED:
                        self._rejected.add(key)
                    raise
                # Original identity comes only from this POST response. Even a
                # later exact same-name/label read is never promoted to a UID.
                self._uids[key] = self._identity(key, resource)[0]
                self._present.add(key)
                address = self._configuration(key, resource)
                if key == "service":
                    self._service_ipv4 = address
                self._check_cancelled()
            result = EgressSnapshot(*self._config, self.original_uids, self._service_ipv4)
            self._phase = "egress_verify"
            self._inspect(result)
            self._check_cancelled()
            return result
        except Exception as primary:
            self._uncertain_posts.update(self._submitted - self._rejected - set(self._uids))
            self._observe_unknown()
            if self._uids or self._uncertain_posts:
                try:
                    self._clean_known()
                except Exception as error:
                    fixed = self._error(error)
                    self._issues += (CleanupIssue("egress", "rollback", type(fixed).__name__),)
                    self._refresh_evidence()
            raise self._error(primary) from None

    def _observe_unknown(self):
        """Bound delayed-response observations but retain UNKNOWN regardless of candidates or temporary 404s."""
        if not self._uncertain_posts:
            return
        self._deadline = monotonic() + RECONCILE_TIMEOUT_SECONDS
        while monotonic() < self._deadline:
            try:
                if all(self._read(key) is not None for key in self._uncertain_posts):
                    return
            except Exception:
                pass
            sleep(min(POLL_SECONDS, max(0, self._deadline - monotonic())))

    def _delete_one(self, key):
        """Delete the precise original with current RV, then confirm absence without removing finalizers or forcing."""
        resource = self._read(key)
        if resource is not None:
            if self._identity(key, resource)[0] != self._uids[key]:
                raise EgressResourceConflict()
            if resource.metadata.deletion_timestamp is None:
                try:
                    self._call(getattr(self._facade(key), "delete_namespaced_" + _KINDS[key]),
                        self._blueprints[key].metadata.name, self._namespace,
                        body=client.V1DeleteOptions(preconditions=client.V1Preconditions(
                            uid=self._uids[key], resource_version=resource.metadata.resource_version)))
                except client.ApiException as error:
                    if error.status in {401, 403, 409}:
                        raise
                except Exception:
                    # A transport failure may occur after the DELETE committed.
                    # Only bounded original-object observations can confirm it.
                    pass
        while True:
            current = self._read(key)
            if current is None:
                self._removed.add(key)
                self._present.discard(key)
                return
            if self._identity(key, current)[0] != self._uids[key]:
                raise EgressResourceConflict()
            remaining = self._deadline - monotonic()
            if remaining <= 0:
                raise KubernetesTimeout()
            sleep(min(POLL_SECONDS, remaining))

    def _runtime_consumers_absent(self):
        """Refuse dependency teardown on any observed deterministic Runtime consumer, including a replacement UID."""
        if not self._runtime_publication:
            return
        root = sandbox_pod_name(self._sandbox_id)
        for name in (root, root + '-proxy'):
            try:
                self._call(self.api.read_namespaced_pod, name, self._namespace)
            except client.ApiException as error:
                if error.status != 404:
                    raise
                self._namespace_exists()
            else:
                raise EgressResourceConflict()

    def _clean_known(self):
        """Keep all isolation until Pod absence is proven; unacknowledged Pod submission cannot be cleared by reads."""
        self._deadline = monotonic() + CLEANUP_TIMEOUT_SECONDS
        self._cleanup_uncertain = False
        found = self._inventory()
        self._runtime_consumers_absent()
        for key, resource in found.items():
            if key in self._uids and self._identity(key, resource)[0] != self._uids[key]:
                raise EgressResourceConflict()
        if "pod" in self._uncertain_posts or ("pod" in found and "pod" not in self._uids):
            raise EgressOperationFailed()
        for key in ("pod", "service", "agent_policy", "isolation"):
            if key in self._uids:
                self._runtime_consumers_absent()
                self._delete_one(key)
        if self._uncertain_posts:
            raise EgressOperationFailed()
        if self._inventory():
            raise EgressResourceConflict()

    def _refresh_evidence(self):
        """Classify positive known leftovers separately from backend uncertainty without overriding the primary error."""
        self._deadline = monotonic() + 1.0
        self._cleanup_uncertain = False
        for key, uid in self._uids.items():
            if key in self._removed:
                continue
            try:
                resource = self._read(key)
                if resource is None:
                    self._removed.add(key)
                elif self._identity(key, resource)[0] == uid:
                    self._present.add(key)
                else:
                    self._cleanup_uncertain = True
            except Exception:
                self._cleanup_uncertain = True

    def delete(self, snapshot: EgressSnapshot | None = None) -> None:
        """Clean only trusted snapshot/original POST UIDs, preserving partial state for explicit bounded retry."""
        if snapshot is not None:
            try:
                uids = self._validate_snapshot(snapshot)
            except Exception as error:
                raise self._error(error) from None
            self._uids.update(uids)
            self._submitted.update(uids)
        try:
            self._phase = "egress_delete"
            self._clean_known()
        except Exception as error:
            self._refresh_evidence()
            raise self._error(error) from None
