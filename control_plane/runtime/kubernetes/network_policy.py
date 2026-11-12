"""Dedicated deny-all NetworkPolicy specification, observation and UID-protected deletion."""

from time import monotonic, sleep

from kubernetes import client

from control_plane.core.errors import (
    KubernetesConfigurationError, KubernetesPermissionDenied, SandboxDeleteConflict,
    SandboxDeleteFailed, SandboxQueryFailed,
)
from control_plane.runtime.kubernetes.pod_query import (
    LABEL_PREFIX, NETWORK_LAYOUT_LABEL, NETWORK_LAYOUT_VERSION, NETWORK_POLICY_UID_ANNOTATION, lifecycle_annotations,
    network_layout, resource_ownership, sandbox_pod_name, sandbox_pod_selector, primary_pod_labels,
)

POLICY_TIMEOUT_SECONDS = 5.0
POLICY_ROLE = "network-policy"


def network_policy_name(sandbox_id: str) -> str:
    """Derive one exact policy name without accepting caller-provided Kubernetes names."""
    return sandbox_pod_name(sandbox_id) + "-deny"


def build_network_policy(pod: client.V1Pod) -> client.V1NetworkPolicy:
    """Build independent deny-all SDK objects from the already validated primary Pod blueprint."""
    metadata = pod.metadata
    sandbox_id = metadata.labels[LABEL_PREFIX + "sandbox-id"]
    if not network_layout(metadata):
        raise SandboxQueryFailed()
    lifecycle_annotations(metadata.annotations)
    return client.V1NetworkPolicy(
        api_version="networking.k8s.io/v1", kind="NetworkPolicy",
        metadata=client.V1ObjectMeta(
            name=network_policy_name(sandbox_id), namespace=metadata.namespace,
            labels={**metadata.labels, LABEL_PREFIX + "resource": POLICY_ROLE},
            annotations={LABEL_PREFIX + key: metadata.annotations[LABEL_PREFIX + key]
                         for key in ("created-at", "expires-at")},
        ),
        spec=client.V1NetworkPolicySpec(
            pod_selector=client.V1LabelSelector(match_labels=dict(metadata.labels)),
            policy_types=["Ingress", "Egress"], ingress=[], egress=[],
        ),
    )


def policy_identity(policy, sandbox_id: str, namespace: str, transaction_id: str | None = None):
    """Validate cleanup ownership and original times independently of policy enforcement rules."""
    try:
        metadata = policy.metadata
        uid, transaction = resource_ownership(metadata, sandbox_id, namespace,
            name=network_policy_name(sandbox_id), role=POLICY_ROLE)
        if not network_layout(metadata) or (transaction_id is not None and transaction != transaction_id):
            raise SandboxDeleteConflict()
        version = metadata.resource_version
        if type(version) is not str or not version or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in version):
            raise SandboxDeleteConflict()
        known = {"private-workspace-version", "private-workspace-storage-class", "private-workspace-uid", "managed", "project", "sandbox-id", "transaction-id", "resource",
                 "created-at", "expires-at", "kubernetes-proxy-version", "proxy-resource-uids", "proxy-token-sha256", "proxy-image", "kubernetes-network-version"}
        if any(key.startswith(LABEL_PREFIX) and key[len(LABEL_PREFIX):] not in known
               for key in (*metadata.labels, *(metadata.annotations or {}))):
            raise SandboxDeleteConflict()
        return uid, transaction, *lifecycle_annotations(metadata.annotations)
    except (AttributeError, TypeError, ValueError, SandboxQueryFailed):
        raise SandboxDeleteConflict() from None


def verify_deny_policy(policy, pod, *, expected_uid: str | None = None) -> None:
    """Require exact transaction selection and zero rules; ownership alone never proves deny-all."""
    try:
        sandbox_id = pod.metadata.labels[LABEL_PREFIX + "sandbox-id"]
        transaction = pod.metadata.labels[LABEL_PREFIX + "transaction-id"]
        uid, _, created, expires = policy_identity(policy, sandbox_id, pod.metadata.namespace, transaction)
        expected_labels = primary_pod_labels(sandbox_id, transaction)
        spec = policy.spec
        bound_uid = pod.metadata.annotations.get(NETWORK_POLICY_UID_ANNOTATION)
        if (
            not network_layout(pod.metadata) or policy.metadata.deletion_timestamp is not None
            or (expected_uid is not None and uid != expected_uid)
            or bound_uid != uid
            or (created, expires) != lifecycle_annotations(pod.metadata.annotations)
            or spec.pod_selector.match_labels != expected_labels
            or spec.pod_selector.match_expressions not in (None, [])
            or not isinstance(spec.policy_types, list) or len(spec.policy_types) != 2
            or set(spec.policy_types) != {"Ingress", "Egress"}
            # Kubernetes omits empty rule arrays in API responses. Empty rules
            # and omitted arrays both deny; a single {} rule would allow all.
            or spec.ingress not in (None, []) or spec.egress not in (None, [])
        ):
            raise SandboxQueryFailed()
    except (AttributeError, KeyError, TypeError, ValueError, SandboxDeleteConflict):
        raise SandboxQueryFailed() from None


class NetworkPolicyResource:
    """Operate on one exact policy with the caller's shared, worker-owned ApiClient."""

    def __init__(self, api, sandbox_id: str, namespace: str, transaction_id: str | None = None):
        """Retain Core and Networking facades without creating or taking ownership of a transport."""
        self.core, self.api = api, api.networking
        self.sandbox_id, self.namespace, self.transaction_id = sandbox_id, namespace, transaction_id
        self.name = network_policy_name(sandbox_id)
        self.deadline = monotonic() + POLICY_TIMEOUT_SECONDS

    def _timeout(self):
        """Bound each request phase and the complete policy operation with monotonic time."""
        remaining = self.deadline - monotonic()
        if remaining <= 0:
            raise SandboxDeleteFailed(self.sandbox_id)
        phase = min(0.5, remaining / 2)
        return phase, phase

    def _inventory(self):
        """Read one limited inventory without hiding other transactions or resource roles."""
        try:
            page = self.api.list_namespaced_network_policy(
                self.namespace, label_selector=sandbox_pod_selector(self.sandbox_id),
                limit=2, watch=False, _request_timeout=self._timeout(),
            )
        except client.ApiException as error:
            if error.status == 404:
                raise KubernetesConfigurationError() from None
            raise
        try:
            if (
                not isinstance(page.items, list) or page.metadata is None or len(page.items) > 1
                or page.metadata._continue not in (None, "")
                or getattr(page.metadata, "remaining_item_count", None) not in (None, 0)
            ):
                raise SandboxDeleteConflict()
        except (AttributeError, TypeError):
            raise SandboxDeleteConflict() from None
        return page.items

    def _read(self):
        """Read the deterministic name even when label discovery returned no candidate."""
        try:
            return self.api.read_namespaced_network_policy(self.name, self.namespace, _request_timeout=self._timeout())
        except client.ApiException as error:
            if error.status == 404:
                return None
            raise

    def ensure_absent(self) -> None:
        """Refuse any existing candidate or same-name object before the first mutation."""
        if self._inventory() or self._read() is not None:
            raise SandboxDeleteConflict()

    def lookup(self):
        """Observe a unique owned policy, comparing discovery and exact-read identity."""
        candidates, policy = self._inventory(), self._read()
        if policy is None:
            if candidates:
                raise SandboxDeleteConflict()
            return None
        identity = policy_identity(policy, self.sandbox_id, self.namespace, self.transaction_id)
        if len(candidates) != 1 or policy_identity(
            candidates[0], self.sandbox_id, self.namespace, self.transaction_id,
        ) != identity:
            raise SandboxDeleteConflict()
        return policy

    def verify_for(self, pod, *, expected_uid: str | None = None) -> None:
        """Require a matching intact policy for the new Pod layout, mapping only integrity errors to query failure."""
        try:
            policy = self.lookup()
            if policy is None:
                raise SandboxQueryFailed()
            verify_deny_policy(policy, pod, expected_uid=expected_uid)
        except SandboxDeleteConflict:
            raise SandboxQueryFailed() from None

    def delete(self, expected_uid: str) -> None:
        """Delete once with exact UID/version, then confirm disappearance without adopting replacements."""
        policy = self.lookup()
        if policy is not None:
            if policy.metadata.uid != expected_uid:
                raise SandboxDeleteConflict()
            if policy.metadata.deletion_timestamp is None:
                try:
                    self.api.delete_namespaced_network_policy(
                        self.name, self.namespace,
                        body=client.V1DeleteOptions(preconditions=client.V1Preconditions(
                            uid=expected_uid, resource_version=policy.metadata.resource_version,
                        )), _request_timeout=self._timeout(),
                    )
                except client.ApiException as error:
                    if error.status == 409:
                        raise SandboxDeleteConflict() from None
                    if error.status in {401, 403}:
                        raise KubernetesPermissionDenied() from None
                except SandboxDeleteFailed:
                    raise
                except Exception:
                    pass
        while True:
            try:
                current = self.lookup()
                if current is None:
                    namespace = self.core.read_namespace(self.namespace, _request_timeout=self._timeout())
                    if namespace.metadata.name != self.namespace:
                        raise KubernetesConfigurationError()
                    return
                if current.metadata.uid != expected_uid:
                    raise SandboxDeleteConflict()
            except (SandboxDeleteConflict, SandboxDeleteFailed, KubernetesConfigurationError):
                raise
            except client.ApiException as error:
                if error.status in {401, 403}:
                    raise KubernetesPermissionDenied() from None
                if error.status == 404:
                    raise KubernetesConfigurationError() from None
            except Exception:
                pass
            remaining = self.deadline - monotonic()
            if remaining <= 0:
                raise SandboxDeleteFailed(self.sandbox_id)
            sleep(min(0.1, remaining))
