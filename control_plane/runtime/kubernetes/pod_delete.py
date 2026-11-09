"""Exact Pod-first cleanup of supported layouts with bounded confirmation and original UIDs."""

from time import monotonic, sleep

from kubernetes import client

from control_plane.core.errors import (
    KubernetesConfigurationError, KubernetesPermissionDenied, SandboxDeleteConflict,
    SandboxDeleteFailed, SandboxQueryFailed,
)
from control_plane.runtime.kubernetes.pod_query import (
    AGENT_CONTAINER_NAME, LABEL_PREFIX, NETWORK_POLICY_UID_ANNOTATION, network_layout,
    pod_ownership, sandbox_pod_selector, sandbox_pod_name,
    AGENT_SERVICE_UID_ANNOTATION, agent_service_layout,
)

# Allow the normal default Pod grace period (30s) plus API observation time.
# SDK connect/read bounds remain phase timeouts, not a hard wall-clock deadline.
DELETE_TIMEOUT_SECONDS = 45.0
_POLL_INTERVAL_SECONDS = 0.1
_KNOWN_MARKERS = frozenset({
    "managed", "project", "sandbox-id", "resource", "transaction-id", "created-at", "expires-at",
    "kubernetes-network-version", "network-policy-uid", "agent-service-version", "agent-service-uid",
})


def _deletion_identity(pod, sandbox_id: str, namespace: str, expected_transaction_id: str | None) -> tuple[str, str]:
    """Require original ownership and a supported layout without interpreting health or time annotations."""
    try:
        uid, transaction_id = pod_ownership(pod, sandbox_id, namespace)
        if expected_transaction_id is not None and transaction_id != expected_transaction_id:
            raise SandboxDeleteConflict()
        metadata, spec = pod.metadata, pod.spec
        version = metadata.resource_version
        if (
            type(version) is not str or not version
            or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in version)
        ):
            raise SandboxDeleteConflict()
        # New binding/version markers cannot silently become a legacy one-Pod
        # layout. Non-platform labels/annotations do not confer ownership.
        markers = (*metadata.labels, *(metadata.annotations or {}))
        if any(key.startswith(LABEL_PREFIX) and key[len(LABEL_PREFIX):] not in _KNOWN_MARKERS for key in markers):
            raise SandboxDeleteConflict()
        if (
            not isinstance(spec.containers, list) or len(spec.containers) != 1
            or spec.containers[0].name != AGENT_CONTAINER_NAME
            or spec.init_containers not in (None, []) or spec.ephemeral_containers not in (None, [])
            or any(volume.empty_dir is None for volume in (spec.volumes or []))
        ):
            raise SandboxDeleteConflict()
        return uid, transaction_id
    except (AttributeError, KeyError, TypeError, ValueError, SandboxQueryFailed):
        raise SandboxDeleteConflict() from None


class PodDeletion:
    """Own one worker's non-retrying DELETE and monotonic, bounded result confirmation."""

    def __init__(self, api, sandbox_id: str, namespace: str, transaction_id: str | None, *, expected_uid: str | None = None, delete_policy: bool = True, delete_service: bool = True):
        """Bind the exact target, expected transaction and one shared operation budget."""
        self.api, self.sandbox_id, self.namespace = api, sandbox_id, namespace
        self.transaction_id = transaction_id
        self.expected_uid = expected_uid
        self.delete_policy, self.delete_service = delete_policy, delete_service
        self.name = sandbox_pod_name(sandbox_id)
        self.deadline = monotonic() + DELETE_TIMEOUT_SECONDS

    def _timeout(self) -> tuple[float, float]:
        """Limit each SDK phase to the smaller of its normal bound and remaining shared budget."""
        remaining = self.deadline - monotonic()
        if remaining <= 0:
            raise SandboxDeleteFailed(self.sandbox_id)
        phase = min(0.5, remaining / 2)
        return phase, phase

    def _inventory(self):
        """Read all same-sandbox Pod roles in one bounded page, never hiding other transactions."""
        try:
            page = self.api.list_namespaced_pod(
                self.namespace, label_selector=sandbox_pod_selector(self.sandbox_id),
                limit=2, watch=False, _request_timeout=self._timeout(),
            )
        except client.ApiException as error:
            if error.status == 404:
                raise KubernetesConfigurationError() from None
            raise
        try:
            if (
                not isinstance(page.items, list) or page.metadata is None
                or page.metadata._continue not in (None, "")
                or getattr(page.metadata, "remaining_item_count", None) not in (None, 0)
                or len(page.items) > 1
            ):
                raise SandboxDeleteConflict()
            for pod in page.items:
                _deletion_identity(pod, self.sandbox_id, self.namespace, self.transaction_id)
            return page.items
        except (AttributeError, TypeError, ValueError):
            raise SandboxDeleteConflict() from None

    def _read(self):
        """Read the deterministic name even when discovery found no matching labels."""
        try:
            return self.api.read_namespaced_pod(self.name, self.namespace, _request_timeout=self._timeout())
        except client.ApiException as error:
            if error.status == 404:
                return None
            raise

    def _confirm_absence(self) -> None:
        """Require empty ownership discovery and a present namespace before declaring cleanup complete."""
        remaining = self._inventory()
        if remaining:
            # Discovery may observe an object created after the exact-name 404.
            # It must never be adopted by this deletion attempt.
            raise SandboxDeleteConflict()
        try:
            namespace = self.api.read_namespace(self.namespace, _request_timeout=self._timeout())
        except client.ApiException as error:
            if error.status == 404:
                raise KubernetesConfigurationError() from None
            raise
        if getattr(getattr(namespace, "metadata", None), "name", None) != self.namespace:
            raise KubernetesConfigurationError()

    def run(self) -> None:
        """Preflight all original identities, then confirm Pod, Service and policy removal in order."""
        from control_plane.runtime.kubernetes.network_policy import NetworkPolicyResource
        from control_plane.runtime.kubernetes.agent_service import AgentServiceResource
        policy = (NetworkPolicyResource(self.api, self.sandbox_id, self.namespace, self.transaction_id).lookup()
                  if self.delete_policy else None)
        service = (AgentServiceResource(self.api, self.sandbox_id, self.namespace, self.transaction_id).lookup()
                   if self.delete_service else None)
        if policy is not None and service is not None and (
            policy.metadata.labels[LABEL_PREFIX + "transaction-id"] != service.metadata.labels[LABEL_PREFIX + "transaction-id"]
        ):
            raise SandboxDeleteConflict()
        candidates, pod = self._inventory(), self._read()
        if pod is None:
            self._confirm_absence()
        else:
            identity = _deletion_identity(pod, self.sandbox_id, self.namespace, self.transaction_id)
            if self.expected_uid is not None and identity[0] != self.expected_uid:
                raise SandboxDeleteConflict()
            if policy is not None and (
                not network_layout(pod.metadata)
                or policy.metadata.labels[LABEL_PREFIX + "transaction-id"] != identity[1]
                or pod.metadata.annotations.get(NETWORK_POLICY_UID_ANNOTATION) != policy.metadata.uid
            ):
                raise SandboxDeleteConflict()
            if service is not None and (
                not agent_service_layout(pod.metadata)
                or service.metadata.labels[LABEL_PREFIX + "transaction-id"] != identity[1]
                or pod.metadata.annotations.get(AGENT_SERVICE_UID_ANNOTATION) != service.metadata.uid
            ):
                raise SandboxDeleteConflict()
            if not candidates or _deletion_identity(candidates[0], self.sandbox_id, self.namespace, self.transaction_id) != identity:
                raise SandboxDeleteConflict()
            if pod.metadata.deletion_timestamp is None:
                self._request_delete(pod)
            self._wait_for_absence(identity)
        # Start each resource's bounded confirmation budget after its predecessor
        # is gone. Never acquire authority to delete a replacement after mutation.
        if self.delete_service:
            current = AgentServiceResource(self.api, self.sandbox_id, self.namespace, self.transaction_id)
            if service is not None:
                current.delete(service.metadata.uid)
            elif current.lookup() is not None:
                raise SandboxDeleteConflict()
        if self.delete_policy:
            current = NetworkPolicyResource(self.api, self.sandbox_id, self.namespace, self.transaction_id)
            if policy is not None:
                current.delete(policy.metadata.uid)
            elif current.lookup() is not None:
                raise SandboxDeleteConflict()

    def confirm_absent(self) -> None:
        """Confirm a non-submitted Pod is absent without acquiring authority to delete a racing object."""
        if self._read() is not None:
            raise SandboxDeleteConflict()
        self._confirm_absence()

    def _request_delete(self, pod) -> None:
        """Submit normal graceful deletion with immutable UID and fresh resourceVersion preconditions."""
        options = client.V1DeleteOptions(preconditions=client.V1Preconditions(
            uid=pod.metadata.uid, resource_version=pod.metadata.resource_version,
        ))
        try:
            self.api.delete_namespaced_pod(
                self.name, self.namespace, body=options, _request_timeout=self._timeout(),
            )
        except client.ApiException as error:
            if error.status == 409:
                raise SandboxDeleteConflict() from None
            if error.status in {401, 403}:
                raise KubernetesPermissionDenied() from None
            # A 404 or lost/server-error response cannot prove that the original
            # target was removed. Confirm it without ever sending DELETE again.
        except SandboxDeleteFailed:
            raise
        except Exception:
            # Transport failures may happen after commit. Only subsequent API
            # observations can establish disappearance of this original target.
            pass

    def _wait_for_absence(self, identity: tuple[str, str]) -> None:
        """Observe only the original target, waiting between attempts under the shared budget."""
        while True:
            try:
                pod = self._read()
                if pod is None:
                    self._confirm_absence()
                    return
                if _deletion_identity(pod, self.sandbox_id, self.namespace, self.transaction_id) != identity:
                    raise SandboxDeleteConflict()
            except (SandboxDeleteConflict, SandboxDeleteFailed, KubernetesConfigurationError):
                raise
            except client.ApiException as error:
                if error.status in {401, 403}:
                    raise KubernetesPermissionDenied() from None
                # GET retries confirm deletion, never resubmit a mutation.
            except Exception:
                # Unavailable observations cannot imply successful deletion;
                # retain a bounded opportunity to confirm a late API commit.
                pass
            remaining = self.deadline - monotonic()
            if remaining <= 0:
                raise SandboxDeleteFailed(self.sandbox_id)
            sleep(min(_POLL_INTERVAL_SECONDS, remaining))
