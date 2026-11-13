"""Pure independent-workspace PVC construction and identity checks; no resource operations.

A WorkspaceRecord's volume_name remains its existing metadata identifier. Only
workspace_pvc_name derives Kubernetes names. Current UID observation alone does
not prove continuity with an earlier PVC, binding health or exclusive use.
"""

import re
from dataclasses import dataclass, replace

from kubernetes import client

from control_plane.core.errors import KubernetesConfigurationError, SandboxQueryFailed, WorkspaceConflict
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
