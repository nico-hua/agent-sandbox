"""Private PVC specifications and bounded original-identity storage operations.

Operations are synchronous and borrow the caller's worker-owned CoreV1Api. The
caller owns thread settlement and client closure, including after cancellation.
This module neither creates a client nor deletes a PV or a consumer Pod.
"""

import re
from dataclasses import dataclass, replace
from time import monotonic, sleep
from uuid import UUID

from kubernetes import client
from kubernetes.utils.quantity import parse_quantity

from control_plane.core.errors import (
    CreationFailureInfo, CreationResourceState, KubernetesConfigurationError, KubernetesConnectionFailed,
    KubernetesOperationUnsupported, KubernetesTimeout, SandboxCreateFailed,
    SandboxDeleteConflict, SandboxDeleteFailed, SandboxQueryFailed,
)
from control_plane.core.models import SandboxSpec, format_utc_datetime
from control_plane.runtime.kubernetes.failures import _probe_failure
from control_plane.runtime.kubernetes.pod_query import (
    LABEL_PREFIX, lifecycle_annotations, resource_ownership, sandbox_pod_name, sandbox_pod_selector,
    PRIVATE_STORAGE_CLASS, PRIVATE_WORKSPACE_UID, PROXY_LAYOUT_ANNOTATION, PROXY_PERSISTENT_LAYOUT_VERSION,
    PROXY_IMAGE_ANNOTATION, PROXY_RECORD_ANNOTATION, PROXY_DIGEST_ANNOTATION, persistent_workspace_layout,
)

WORKSPACE_ROLE = "private-workspace"
VERSION_ANNOTATION = LABEL_PREFIX + "private-workspace-version"
OPERATION_TIMEOUT_SECONDS = 5.0
RECONCILE_TIMEOUT_SECONDS = 5.0
DELETE_TIMEOUT_SECONDS = 30.0
POLL_SECONDS = 0.1
MAX_CONSUMER_PODS = 100
_DNS_LABEL = r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"


def _dns_name(value) -> bool:
    """Accept bounded ASCII DNS subdomains without allowing implicit empty defaults."""
    return (type(value) is str and 1 <= len(value) <= 253
            and all(re.fullmatch(_DNS_LABEL, label) is not None for label in value.split(".")))


def private_workspace_name(sandbox_id: str) -> str:
    """Derive the private PVC name from the existing public sandbox identity."""
    return sandbox_pod_name(sandbox_id) + "-workspace"


def build_private_workspace(spec: SandboxSpec, *, namespace: str, storage_class: str) -> client.V1PersistentVolumeClaim:
    """Build a fresh fixed-size private PVC using explicit identity and trusted storage configuration."""
    if (type(namespace) is not str or re.fullmatch(_DNS_LABEL, namespace) is None
            or not _dns_name(storage_class)):
        raise KubernetesConfigurationError()
    if type(spec) is not SandboxSpec:
        raise ValueError("spec must be SandboxSpec")
    spec = replace(spec)
    name = private_workspace_name(spec.sandbox_id)
    if (type(spec.transaction_id) is not str
            or re.fullmatch(r"txn_[0-9a-f]{32}", spec.transaction_id) is None
            or spec.created_at is None or spec.expires_at is None):
        raise ValueError("private workspace requires complete business identity")
    if spec.workspace is not None:
        raise KubernetesOperationUnsupported()
    return client.V1PersistentVolumeClaim(
        api_version="v1", kind="PersistentVolumeClaim",
        metadata=client.V1ObjectMeta(
            name=name, namespace=namespace,
            labels={**spec.labels_for("workspace"), LABEL_PREFIX + "resource": WORKSPACE_ROLE},
            annotations={VERSION_ANNOTATION: "1",
                         LABEL_PREFIX + "created-at": format_utc_datetime(spec.created_at),
                         LABEL_PREFIX + "expires-at": format_utc_datetime(spec.expires_at)},
        ),
        spec=client.V1PersistentVolumeClaimSpec(
            storage_class_name=storage_class, volume_mode="Filesystem", access_modes=["ReadWriteOnce"],
            resources=client.V1VolumeResourceRequirements(requests={"storage": "1Gi"}),
        ),
    )


@dataclass(frozen=True)
class PrivateWorkspaceObservation:
    """Expose original PVC identity and binding facts without implying application-level exclusivity."""

    uid: str
    phase: str
    volume_name: str | None


def _uid(value: str) -> None:
    """Reject invalid original UID evidence before accessing the API."""
    if type(value) is not str:
        raise ValueError("invalid PVC UID")
    try:
        if str(UUID(value)) != value:
            raise ValueError()
    except ValueError:
        raise ValueError("invalid PVC UID") from None


def private_workspace_from_metadata(api, metadata, *, storage_class=None):
    """Reconstruct original storage identity from durable metadata, never the Runtime's current configuration."""
    try:
        created, expires = lifecycle_annotations(metadata.annotations)
        if storage_class is None:
            if not persistent_workspace_layout(metadata):
                raise SandboxDeleteConflict()
            storage_class = metadata.annotations[PRIVATE_STORAGE_CLASS]
        spec = SandboxSpec(
            sandbox_id=metadata.labels[LABEL_PREFIX + "sandbox-id"], container_name="unused",
            network_name="unused", volume_name="unused",
            transaction_id=metadata.labels[LABEL_PREFIX + "transaction-id"], created_at=created, expires_at=expires,
        )
        return PrivateWorkspaceResource(api, spec, namespace=metadata.namespace, storage_class=storage_class)
    except (KeyError, AttributeError, TypeError, ValueError, SandboxQueryFailed, KubernetesConfigurationError):
        raise SandboxDeleteConflict() from None


def verify_private_workspace_configuration(pvc, storage_class: str) -> None:
    """Check fixed storage requests and controller binding fields without requiring lifecycle publication or health."""
    try:
        if not _dns_name(storage_class):
            raise SandboxQueryFailed()
        spec = pvc.spec
        resources = spec.resources
        if (spec.storage_class_name != storage_class
            or spec.volume_mode != "Filesystem" or spec.access_modes != ["ReadWriteOnce"]
            or resources.requests is None or set(resources.requests) != {"storage"}
            or parse_quantity(resources.requests["storage"]) != parse_quantity("1Gi")
            or resources.limits not in (None, {})
            or spec.selector is not None or spec.data_source is not None or spec.data_source_ref is not None
            or spec.volume_attributes_class_name is not None
            or (spec.volume_name is not None and not _dns_name(spec.volume_name))):
            raise SandboxQueryFailed()
    except (AttributeError, TypeError, ValueError, ArithmeticError):
        raise SandboxQueryFailed() from None


class PrivateWorkspaceResource:
    """Bounded operations on one transaction's PVC using a borrowed, non-retrying SDK client.

    submitted is conservative after an ambiguous POST; uid is retained only
    after positive identity confirmation. Neither an unknown outcome nor a read
    failure authorizes a later transaction to adopt this PVC.
    """

    def __init__(self, api, spec: SandboxSpec, *, namespace: str, storage_class: str):
        """Validate and copy construction inputs without reading files or accessing a transport."""
        self._blueprint = build_private_workspace(spec, namespace=namespace, storage_class=storage_class)
        self.api, self.namespace, self.name = api, namespace, self._blueprint.metadata.name
        self.sandbox_id, self.transaction_id = spec.sandbox_id, spec.transaction_id
        self.uid = None
        self.submitted = False
        self._attempted = False
        self._removed = False
        self._remaining_confirmed = False
        self._deadline = 0.0

    def _creation_state(self) -> CreationResourceState:
        """Keep original submission evidence, upgrading it only after positive cleanup confirmation."""
        if not self.submitted:
            return CreationResourceState.UNCHANGED
        if self._removed and self.uid is not None:
            return CreationResourceState.ROLLED_BACK
        return CreationResourceState.REMAINING if self._remaining_confirmed else CreationResourceState.UNKNOWN

    def _request(self, method, *args, _post=False, **kwargs):
        """Use finite phase timeouts and convert SDK exceptions without copying their diagnostics."""
        remaining = self._deadline - monotonic()
        if remaining <= 0:
            raise KubernetesTimeout()
        phase = min(0.5, remaining / 2)
        try:
            return method(*args, _request_timeout=(phase, phase), **kwargs)
        except client.ApiException as error:
            if _post or error.status in {404, 409}:
                raise
            raise _probe_failure(error) from None
        except Exception as error:
            raise _probe_failure(error) from None

    def _namespace_exists(self) -> None:
        """Distinguish an absent namespace from an absent claim before claiming idempotent success."""
        try:
            namespace = self._request(self.api.read_namespace, self.namespace)
            if namespace.metadata.name != self.namespace:
                raise KubernetesConfigurationError()
        except client.ApiException:
            raise KubernetesConfigurationError() from None
        except (AttributeError, TypeError):
            raise KubernetesConfigurationError() from None

    def _identity(self, pvc) -> str:
        """Require original transaction, business times, closed platform metadata and a usable resourceVersion."""
        try:
            metadata = pvc.metadata
            uid, transaction = resource_ownership(metadata, self.sandbox_id, self.namespace,
                name=self.name, role=WORKSPACE_ROLE, check_proxy_layout=False)
            labels, annotations = metadata.labels, metadata.annotations or {}
            expected = self._blueprint.metadata
            known_labels = set(expected.labels)
            known_annotations = set(expected.annotations)
            runtime_version = annotations.get(PROXY_LAYOUT_ANNOTATION)
            if runtime_version is not None:
                if (runtime_version != PROXY_PERSISTENT_LAYOUT_VERSION or not persistent_workspace_layout(metadata)
                    or annotations.get(PRIVATE_STORAGE_CLASS) != self._blueprint.spec.storage_class_name
                    or annotations.get(PRIVATE_WORKSPACE_UID) != uid):
                    raise SandboxDeleteConflict()
                known_annotations.update({PRIVATE_STORAGE_CLASS, PRIVATE_WORKSPACE_UID, PROXY_LAYOUT_ANNOTATION,
                    PROXY_IMAGE_ANNOTATION, PROXY_RECORD_ANNOTATION, PROXY_DIGEST_ANNOTATION})
            version = metadata.resource_version
            if (transaction != self.transaction_id
                or any(labels.get(key) != value for key, value in expected.labels.items())
                or any(annotations.get(key) != value for key, value in expected.annotations.items())
                or any(key.startswith(LABEL_PREFIX) and key not in known_labels for key in labels)
                or any(key.startswith(LABEL_PREFIX) and key not in known_annotations for key in annotations)
                or type(version) is not str or not 1 <= len(version) <= 256
                or not version.isascii() or any(char.isspace() or ord(char) < 33 or ord(char) == 127 for char in version)
                or metadata.owner_references not in (None, [])):
                raise SandboxDeleteConflict()
            lifecycle_annotations(annotations)
            return uid
        except (AttributeError, TypeError, ValueError, SandboxQueryFailed):
            raise SandboxDeleteConflict() from None

    def _configuration(self, pvc) -> None:
        """Verify requested storage against this transaction's original explicit StorageClass."""
        verify_private_workspace_configuration(pvc, self._blueprint.spec.storage_class_name)

    def _observation(self, pvc, expected_uid: str | None = None) -> PrivateWorkspaceObservation:
        """Separate WFFC Pending from a verified Bound volume or contradictory/abnormal observations."""
        uid = self._identity(pvc)
        if (expected_uid is not None and uid != expected_uid) or pvc.metadata.deletion_timestamp is not None:
            raise SandboxQueryFailed()
        self._configuration(pvc)
        try:
            status = pvc.status
            phase = "Pending" if status is None or status.phase is None else status.phase
            if phase not in {"Pending", "Bound"}:
                raise SandboxQueryFailed()
            for condition in ([] if status is None else status.conditions or []):
                # v1.37 adds idle tracking. Unused=True describes an idle
                # claim, not a storage failure or authority to release data.
                if (condition.status not in {"True", "False", "Unknown"}
                    or (condition.type != "Unused" and condition.status != "False")
                    or condition.status == "Unknown"):
                    raise SandboxQueryFailed()
            if phase == "Bound" and (
                not _dns_name(pvc.spec.volume_name) or status.access_modes != ["ReadWriteOnce"]
                or not isinstance(status.capacity, dict)
                or parse_quantity(status.capacity.get("storage", "0")) < parse_quantity("1Gi")
            ):
                raise SandboxQueryFailed()
            return PrivateWorkspaceObservation(uid, phase, pvc.spec.volume_name)
        except (AttributeError, TypeError, ValueError, ArithmeticError):
            raise SandboxQueryFailed() from None

    def _lookup(self, expected_uid: str | None = None):
        """Compare bounded ownership discovery against an exact-name read, never hiding other roles."""
        try:
            page = self._request(self.api.list_namespaced_persistent_volume_claim, self.namespace,
                label_selector=sandbox_pod_selector(self.sandbox_id), limit=2, watch=False)
        except client.ApiException:
            raise KubernetesConfigurationError() from None
        try:
            if (not isinstance(page.items, list) or len(page.items) > 1 or page.metadata is None
                or page.metadata._continue not in (None, "")
                or page.metadata.remaining_item_count not in (None, 0)):
                raise SandboxDeleteConflict()
            identities = [self._identity(item) for item in page.items]
        except (AttributeError, TypeError):
            raise SandboxDeleteConflict() from None
        try:
            pvc = self._request(self.api.read_namespaced_persistent_volume_claim, self.name, self.namespace)
        except client.ApiException as error:
            if error.status != 404:
                raise SandboxDeleteConflict() from None
            if identities:
                raise SandboxDeleteConflict() from None
            self._namespace_exists()
            return None
        uid = self._identity(pvc)
        if identities != [uid] or (expected_uid is not None and uid != expected_uid):
            raise SandboxDeleteConflict()
        return pvc

    def inspect(self, expected_uid: str) -> PrivateWorkspaceObservation | None:
        """Observe the caller's original PVC without modifying it or waiting for first-consumer binding."""
        _uid(expected_uid)
        self._deadline = monotonic() + OPERATION_TIMEOUT_SECONDS
        pvc = self._lookup(expected_uid)
        return None if pvc is None else self._observation(pvc, expected_uid)

    def ensure_absent(self) -> None:
        """Check bounded candidates and the exact name before another resource is mutated."""
        self._deadline = monotonic() + OPERATION_TIMEOUT_SECONDS
        if self._lookup() is not None:
            raise SandboxDeleteConflict()

    def confirm_submission(self) -> bool:
        """Retry only positive read confirmation after an uncertain POST, never the POST itself."""
        self._reconcile_submission()
        return self._remaining_confirmed

    def create(self) -> PrivateWorkspaceObservation:
        """Submit once, retaining positive or uncertain creation evidence for a future transaction owner."""
        if self._attempted:
            error = SandboxCreateFailed("private workspace submission already attempted")
            error.creation_failure = CreationFailureInfo("private_workspace", self._creation_state())
            raise error
        self._deadline = monotonic() + OPERATION_TIMEOUT_SECONDS
        try:
            if self._lookup() is not None:
                raise SandboxCreateFailed("private workspace already exists")
            self._removed = False
            self._attempted = True
            self.submitted = True
            pvc = self._request(self.api.create_namespaced_persistent_volume_claim,
                                self.namespace, self._blueprint, _post=True)
            self.uid = self._identity(pvc)
            observation = self._observation(pvc, self.uid)
            self._remaining_confirmed = True
            return observation
        except Exception as primary:
            # Definitive API rejections did not accept our POST. Transport
            # failure is ambiguous, including a response lost after commit.
            if isinstance(primary, client.ApiException):
                if primary.status in {400, 401, 403, 404, 409, 422}:
                    self.submitted = False
                primary = (SandboxCreateFailed("private workspace creation conflict")
                           if primary.status == 409 else _probe_failure(primary))
            elif isinstance(primary, SandboxDeleteConflict):
                primary = SandboxCreateFailed("private workspace creation conflict")
            if self.submitted:
                self._reconcile_submission()
            primary.creation_failure = CreationFailureInfo("private_workspace", self._creation_state())
            raise primary from None

    def _reconcile_submission(self) -> None:
        """Seek positive original-identity evidence; repeated 404 is never proof of non-submission."""
        self._remaining_confirmed = False
        self._deadline = monotonic() + RECONCILE_TIMEOUT_SECONDS
        while monotonic() < self._deadline:
            try:
                pvc = self._lookup(self.uid)
                if pvc is not None:
                    self.uid = self._identity(pvc)
                    self._remaining_confirmed = True
                    return
            except Exception:
                # Keep the primary classification and uncertain evidence. No
                # raw SDK diagnostics, client repr or PVC content is retained.
                pass
            sleep(min(POLL_SECONDS, max(0, self._deadline - monotonic())))

    def _no_consumers(self) -> None:
        """Require a complete bounded namespace-wide Pod scan, including non-platform consumers."""
        try:
            page = self._request(self.api.list_namespaced_pod, self.namespace,
                                 limit=MAX_CONSUMER_PODS + 1, watch=False)
        except client.ApiException:
            raise KubernetesConfigurationError() from None
        try:
            if (not isinstance(page.items, list) or len(page.items) > MAX_CONSUMER_PODS
                or page.metadata is None or page.metadata._continue not in (None, "")
                or page.metadata.remaining_item_count not in (None, 0)):
                raise SandboxDeleteConflict()
            for pod in page.items:
                if pod.spec is None:
                    raise SandboxDeleteConflict()
                for volume in pod.spec.volumes or []:
                    source = volume.persistent_volume_claim
                    if source is not None and (not _dns_name(source.claim_name) or source.claim_name == self.name):
                        # Even terminal/terminating Pod objects may retain PVC
                        # protection. Caller must remove consumers first.
                        raise SandboxDeleteConflict()
        except (AttributeError, TypeError):
            raise SandboxDeleteConflict() from None

    def delete(self, expected_uid: str) -> None:
        """Delete only the original unconsumed PVC, then confirm namespace-qualified absence."""
        _uid(expected_uid)
        if self.uid is not None and self.uid != expected_uid:
            raise SandboxDeleteConflict()
        self._deadline = monotonic() + DELETE_TIMEOUT_SECONDS
        pvc = self._lookup(expected_uid)
        if pvc is None:
            self._removed = True
            return
        # Cleanup is independent of Pending/Bound/Lost status; ownership and
        # storage configuration still have to match the private PVC contract.
        self._configuration(pvc)
        self._no_consumers()
        if pvc.metadata.deletion_timestamp is None:
            try:
                self._request(self.api.delete_namespaced_persistent_volume_claim, self.name, self.namespace,
                    body=client.V1DeleteOptions(preconditions=client.V1Preconditions(
                        uid=expected_uid, resource_version=pvc.metadata.resource_version)))
            except client.ApiException as error:
                if error.status == 409:
                    raise SandboxDeleteConflict() from None
                if error.status != 404:
                    raise _probe_failure(error) from None
            except (KubernetesTimeout, KubernetesConnectionFailed):
                # It may have committed: a read-only confirmation is safe,
                # whereas automatically retrying DELETE is not.
                pass
        while monotonic() < self._deadline:
            try:
                if self._lookup(expected_uid) is None:
                    self._removed = True
                    return
            except KubernetesTimeout:
                break
            sleep(min(POLL_SECONDS, max(0, self._deadline - monotonic())))
        raise SandboxDeleteFailed(self.sandbox_id)
