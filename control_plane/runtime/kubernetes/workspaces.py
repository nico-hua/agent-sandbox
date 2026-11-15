"""Independent-workspace PVC specifications and bounded borrowed-client resource operations.

A WorkspaceRecord's volume_name remains its existing metadata identifier. Only
workspace_pvc_name derives Kubernetes names. Current UID observation alone does
not prove continuity with an earlier PVC, binding health or exclusive use.
"""

import copy
import re
from dataclasses import dataclass, replace
from time import monotonic, sleep

from kubernetes import client

from control_plane.core.errors import (
    CreationFailureInfo, CreationResourceState, KubernetesConfigurationError, KubernetesError,
    KubernetesConnectionFailed, KubernetesTimeout, SandboxQueryFailed,
    WorkspaceConflict, WorkspaceError, WorkspaceOperationFailed,
)
from control_plane.runtime.kubernetes.failures import _probe_failure
from control_plane.core.models import WorkspaceRecord, _validate_workspace_id, format_utc_datetime
from control_plane.runtime.kubernetes.pod_query import LABEL_PREFIX
from control_plane.runtime.kubernetes.private_workspace import (
    _DNS_LABEL, _dns_name, _uid, verify_private_workspace_configuration,
)

WORKSPACE_ROLE = "workspace"
VERSION_ANNOTATION = LABEL_PREFIX + "workspace-version"
WORKSPACE_VERSION = "1"


def workspace_pvc_name(workspace_id: str) -> str:
    """Map an exact public workspace ID injectively to a legal, deterministic PVC name."""
    _validate_workspace_id(workspace_id)
    return "agent-sandbox-wsp-" + workspace_id[len("wsp_"):]


def _validated_record(record: WorkspaceRecord, namespace: str, storage_class: str) -> WorkspaceRecord:
    """Recheck trusted storage configuration and the existing independent record without repairing identity."""
    if (type(namespace) is not str or re.fullmatch(_DNS_LABEL, namespace) is None
            or not _dns_name(storage_class)):
        raise KubernetesConfigurationError()
    if type(record) is not WorkspaceRecord:
        raise ValueError("record must be WorkspaceRecord")
    # Revalidation also enforces the legacy volume_name/ID relation. Never
    # convert that arbitrary field into the name used to address Kubernetes.
    return replace(record)


def _workspace_metadata(record: WorkspaceRecord, namespace: str) -> client.V1ObjectMeta:
    """Describe only independent ownership; occupancy and sandbox lifetimes are not PVC identity."""
    return client.V1ObjectMeta(
        name=workspace_pvc_name(record.workspace_id), namespace=namespace,
        labels={
            LABEL_PREFIX + "managed": "true", LABEL_PREFIX + "project": "agent-sandbox",
            LABEL_PREFIX + "resource": WORKSPACE_ROLE, LABEL_PREFIX + "workspace-id": record.workspace_id,
            LABEL_PREFIX + "workspace-token": record.creation_token,
        },
        annotations={
            VERSION_ANNOTATION: WORKSPACE_VERSION, LABEL_PREFIX + "workspace-image-id": record.image_id,
            LABEL_PREFIX + "created-at": format_utc_datetime(record.created_at),
        },
    )


def build_workspace_pvc(record: WorkspaceRecord, *, namespace: str,
                        storage_class: str) -> client.V1PersistentVolumeClaim:
    """Build fresh fixed-storage SDK objects from existing identity and explicit trusted StorageClass only."""
    record = _validated_record(record, namespace, storage_class)
    return client.V1PersistentVolumeClaim(
        api_version="v1", kind="PersistentVolumeClaim", metadata=_workspace_metadata(record, namespace),
        spec=client.V1PersistentVolumeClaimSpec(
            storage_class_name=storage_class, volume_mode="Filesystem", access_modes=["ReadWriteOnce"],
            resources=client.V1VolumeResourceRequirements(requests={"storage": "1Gi"}),
        ),
    )


@dataclass(frozen=True)
class WorkspacePVCObservation:
    """Keep current API identity facts; original_uid_verified is false when no original UID was supplied.

    These facts do not assert Bound/healthy, PV identity, consumer exclusivity,
    successful creation or durable storage of the observed UID.
    """

    uid: str
    resource_version: str
    original_uid_verified: bool


def _matches_metadata_map(actual, expected: dict[str, str]) -> bool:
    """Require complete known platform markers, permitting only well-formed non-platform extension metadata."""
    return (
        isinstance(actual, dict)
        and all(type(key) is str and type(value) is str for key, value in actual.items())
        and all(actual.get(key) == value for key, value in expected.items())
        and not any(key.startswith(LABEL_PREFIX) and key not in expected for key in actual)
    )


def verify_workspace_pvc(pvc: client.V1PersistentVolumeClaim, record: WorkspaceRecord, *,
                         namespace: str, storage_class: str,
                         expected_uid: str | None = None) -> WorkspacePVCObservation:
    """Check independent identity/specification only, optionally tying observation to a caller-held original UID."""
    record = _validated_record(record, namespace, storage_class)
    if expected_uid is not None:
        _uid(expected_uid)
    expected = _workspace_metadata(record, namespace)
    try:
        metadata = pvc.metadata
        _uid(metadata.uid)
        version = metadata.resource_version
        if (pvc.api_version != "v1" or pvc.kind != "PersistentVolumeClaim"
            or metadata.name != expected.name or metadata.namespace != namespace
            or not _matches_metadata_map(metadata.labels, expected.labels)
            or not _matches_metadata_map(metadata.annotations, expected.annotations)
            or metadata.owner_references not in (None, [])
            or type(version) is not str or not 1 <= len(version) <= 256
            or not version.isascii() or any(ord(char) <= 32 or ord(char) == 127 for char in version)
            or (expected_uid is not None and metadata.uid != expected_uid)):
            raise WorkspaceConflict(record.workspace_id)
        # Reuse only the shared fixed PVC configuration checks, never private
        # sandbox identity or its Bound/health observation. A controller-provided
        # volumeName is allowed structurally; this does not verify a backing PV.
        verify_private_workspace_configuration(pvc, storage_class)
        return WorkspacePVCObservation(metadata.uid, version, expected_uid is not None)
    except (AttributeError, TypeError, ValueError, SandboxQueryFailed):
        raise WorkspaceConflict(record.workspace_id) from None


OPERATION_TIMEOUT_SECONDS = 5.0
RECONCILE_TIMEOUT_SECONDS = 5.0
DELETE_TIMEOUT_SECONDS = 30.0
POLL_SECONDS = 0.1
MAX_CONSUMER_PODS = 100


class WorkspaceResource:
    """Operate on one independent PVC using a borrowed, non-retrying worker-owned CoreV1Api.

    Only a validated POST response fixes uid. A matching later object after a
    lost response is merely candidate evidence, never automatic delete authority.
    This object is synchronous and not a cross-thread/database occupancy lock.
    """

    def __init__(self, api, record: WorkspaceRecord, *, namespace: str, storage_class: str):
        """Copy inert trusted inputs without acquiring or closing a client or performing I/O."""
        self._blueprint = build_workspace_pvc(record, namespace=namespace, storage_class=storage_class)
        self._record = replace(record)
        self.api, self.namespace, self.name = api, namespace, workspace_pvc_name(record.workspace_id)
        self._storage_class = storage_class
        self._original_uid = None
        self._submitted = False
        self._create_called = False
        self._removed = False
        self._remaining_confirmed = False
        self._candidate = None
        self._deadline = 0.0

    @property
    def uid(self) -> str | None:
        """Expose the original UID fixed from this object's successful POST response only."""
        return self._original_uid

    @property
    def submitted(self) -> bool:
        """Keep possible POST commitment distinct from a validated original UID."""
        return self._submitted

    @property
    def candidate(self) -> WorkspacePVCObservation | None:
        """Expose a later untrusted-for-continuity observation without promoting it to an original receipt."""
        return self._candidate

    @property
    def creation_failure(self) -> CreationFailureInfo:
        """Describe this object's creation evidence, preserving original receipts across repeated calls."""
        if not self.submitted:
            state = CreationResourceState.UNCHANGED
        elif self._removed and self.uid is not None:
            state = CreationResourceState.ROLLED_BACK
        elif self.uid is not None and self._remaining_confirmed:
            state = CreationResourceState.REMAINING
        else:
            state = CreationResourceState.UNKNOWN
        return CreationFailureInfo("workspace_create", state)

    def _request(self, method, *args, _post=False, **kwargs):
        """Bound each SDK phase and mark possible commitment only when actually dispatching a POST."""
        remaining = self._deadline - monotonic()
        if remaining <= 0:
            raise KubernetesTimeout()
        phase = min(0.5, remaining / 2)
        if _post:
            self._submitted = True
        try:
            return method(*args, _request_timeout=(phase, phase), **kwargs)
        except client.ApiException as error:
            if _post or error.status in {404, 409}:
                raise
            raise _probe_failure(error) from None
        except (WorkspaceError, KubernetesError):
            raise
        except Exception as error:
            raise _probe_failure(error) from None

    def _failure(self, error: Exception, phase: str):
        """Attach current creation evidence to fixed domain errors without copying SDK messages or bodies."""
        if not isinstance(error, (WorkspaceError, KubernetesError)):
            error = WorkspaceConflict(self._record.workspace_id) if (
                isinstance(error, client.ApiException) and error.status == 409
            ) else _probe_failure(error)
        error.creation_failure = CreationFailureInfo(phase, self.creation_failure.resource_state)
        return error

    def _namespace_exists(self) -> None:
        """Require exact namespace access before interpreting a resource-level 404 as absence."""
        try:
            namespace = self._request(self.api.read_namespace, self.namespace)
            if namespace.metadata.name != self.namespace:
                raise KubernetesConfigurationError()
        except client.ApiException:
            raise KubernetesConfigurationError() from None
        except (AttributeError, TypeError):
            raise KubernetesConfigurationError() from None

    def _verify(self, pvc, expected_uid=None) -> WorkspacePVCObservation:
        """Reuse the pure independent identity and specification contract for every observed object."""
        return verify_workspace_pvc(pvc, self._record, namespace=self.namespace,
                                    storage_class=self._storage_class, expected_uid=expected_uid)

    def _lookup(self, expected_uid=None):
        """Cross-check bounded unfiltered-role discovery with the deterministic exact-name observation."""
        selector = ",".join((LABEL_PREFIX + "managed=true", LABEL_PREFIX + "project=agent-sandbox",
                             LABEL_PREFIX + "workspace-id=" + self._record.workspace_id))
        try:
            page = self._request(self.api.list_namespaced_persistent_volume_claim, self.namespace,
                                 label_selector=selector, limit=2, watch=False)
        except client.ApiException:
            raise KubernetesConfigurationError() from None
        try:
            if (page.api_version != "v1" or page.kind != "PersistentVolumeClaimList"
                or not isinstance(page.items, list) or len(page.items) > 1 or page.metadata is None
                or page.metadata._continue not in (None, "")
                or page.metadata.remaining_item_count not in (None, 0)):
                raise WorkspaceConflict(self._record.workspace_id)
            observations = []
            for listed in page.items:
                if not isinstance(listed, client.V1PersistentVolumeClaim):
                    raise WorkspaceConflict(self._record.workspace_id)
                # A v1 PVC list omits item TypeMeta. Supply only absent fields
                # from the verified collection envelope, never overwrite an
                # explicit unknown type or mutate the borrowed SDK response.
                normalized = copy.deepcopy(listed)
                if normalized.api_version is None:
                    normalized.api_version = "v1"
                if normalized.kind is None:
                    normalized.kind = "PersistentVolumeClaim"
                observations.append(self._verify(normalized, expected_uid))
        except (AttributeError, TypeError):
            raise WorkspaceConflict(self._record.workspace_id) from None
        try:
            pvc = self._request(self.api.read_namespaced_persistent_volume_claim, self.name, self.namespace)
        except client.ApiException as error:
            if error.status != 404 or observations:
                raise WorkspaceConflict(self._record.workspace_id) from None
            self._namespace_exists()
            return None
        observation = self._verify(pvc, expected_uid)
        if observations != [observation]:
            raise WorkspaceConflict(self._record.workspace_id)
        return pvc

    def create(self) -> WorkspacePVCObservation:
        """Preflight and submit once; Pending is valid and ambiguous responses never pin a later candidate UID."""
        if self._create_called or self._removed:
            raise self._failure(WorkspaceOperationFailed(self._record.workspace_id), "workspace_create")
        self._create_called = True
        self._deadline = monotonic() + OPERATION_TIMEOUT_SECONDS
        try:
            if self._lookup() is not None:
                raise WorkspaceConflict(self._record.workspace_id)
            pvc = self._request(self.api.create_namespaced_persistent_volume_claim, self.namespace,
                                copy.deepcopy(self._blueprint), _post=True)
            observed = self._verify(pvc)
            self._original_uid = observed.uid
            self._remaining_confirmed = True
            return replace(observed, original_uid_verified=True)
        except Exception as primary:
            if isinstance(primary, client.ApiException) and primary.status in {400, 401, 403, 404, 409, 422}:
                self._submitted = False
                if primary.status == 409:
                    primary = WorkspaceConflict(self._record.workspace_id)
                elif primary.status == 404:
                    primary = KubernetesConfigurationError()
                elif primary.status in {400, 422}:
                    primary = WorkspaceOperationFailed(self._record.workspace_id)
            if self.submitted and self.uid is None:
                self._observe_candidate()
            raise self._failure(primary, "workspace_create") from None

    def _observe_candidate(self) -> None:
        """Seek positive read evidence within a separate budget without adopting it or concluding non-submission."""
        self._deadline = monotonic() + RECONCILE_TIMEOUT_SECONDS
        while monotonic() < self._deadline:
            try:
                pvc = self._lookup()
                if pvc is not None:
                    self._candidate = self._verify(pvc)
                    return
            except WorkspaceConflict:
                return
            except Exception:
                pass
            sleep(min(POLL_SECONDS, max(0, self._deadline - monotonic())))

    def _expected_uid(self, expected_uid: str) -> None:
        """Reject malformed or contradictory caller-held originals without changing any saved receipt."""
        _uid(expected_uid)
        if self.uid is not None and expected_uid != self.uid:
            raise self._failure(WorkspaceConflict(self._record.workspace_id), "workspace_identity")

    def inspect(self, expected_uid: str) -> WorkspacePVCObservation | None:
        """Read only the caller's original PVC; absence requires complete discovery and namespace access."""
        self._expected_uid(expected_uid)
        self._deadline = monotonic() + OPERATION_TIMEOUT_SECONDS
        self._remaining_confirmed = False
        try:
            pvc = self._lookup(expected_uid)
            if pvc is None:
                return None
            observed = self._verify(pvc, expected_uid)
            self._remaining_confirmed = self.uid == expected_uid
            return observed
        except Exception as error:
            raise self._failure(error, "workspace_inspect") from None

    def _no_consumers(self) -> None:
        """Require a complete namespace-wide Pod scan including foreign, terminal and terminating consumers."""
        try:
            page = self._request(self.api.list_namespaced_pod, self.namespace, limit=MAX_CONSUMER_PODS + 1, watch=False)
        except client.ApiException:
            raise KubernetesConfigurationError() from None
        try:
            if (not isinstance(page.items, list) or len(page.items) > MAX_CONSUMER_PODS or page.metadata is None
                or page.metadata._continue not in (None, "")
                or page.metadata.remaining_item_count not in (None, 0)):
                raise WorkspaceConflict(self._record.workspace_id)
            for pod in page.items:
                if pod.spec is None:
                    raise WorkspaceConflict(self._record.workspace_id)
                for volume in pod.spec.volumes or []:
                    source = volume.persistent_volume_claim
                    if source is not None and (not _dns_name(source.claim_name) or source.claim_name == self.name):
                        raise WorkspaceConflict(self._record.workspace_id)
        except (AttributeError, TypeError):
            raise WorkspaceConflict(self._record.workspace_id) from None

    def delete(self, expected_uid: str) -> None:
        """Delete a proven original only after holder/consumer checks, then confirm absence without forcing storage."""
        self._expected_uid(expected_uid)
        if self._record.sandbox_id is not None or (self.submitted and self.uid is None):
            raise self._failure(WorkspaceConflict(self._record.workspace_id), "workspace_delete")
        self._deadline = monotonic() + DELETE_TIMEOUT_SECONDS
        self._remaining_confirmed = False
        try:
            pvc = self._lookup(expected_uid)
            if pvc is not None:
                self._remaining_confirmed = self.uid == expected_uid
                self._no_consumers()
                if pvc.metadata.deletion_timestamp is None:
                    try:
                        self._request(self.api.delete_namespaced_persistent_volume_claim, self.name, self.namespace,
                            body=client.V1DeleteOptions(preconditions=client.V1Preconditions(
                                uid=expected_uid, resource_version=pvc.metadata.resource_version)))
                    except client.ApiException as error:
                        if error.status != 404:
                            raise
                    except (KubernetesConnectionFailed, KubernetesTimeout):
                        pass  # DELETE may have committed; only read confirmation follows.
                while monotonic() < self._deadline:
                    self._remaining_confirmed = False
                    current = self._lookup(expected_uid)
                    if current is None:
                        break
                    self._remaining_confirmed = self.uid == expected_uid
                    sleep(min(POLL_SECONDS, max(0, self._deadline - monotonic())))
                else:
                    raise WorkspaceOperationFailed(self._record.workspace_id)
            self._removed = True
            self._remaining_confirmed = False
        except Exception as error:
            raise self._failure(error, "workspace_delete") from None
