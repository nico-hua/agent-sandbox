"""Offline independent-workspace PVC identity and pure constrained specification contracts."""

import copy
import importlib
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime, timedelta, timezone

import pytest
from kubernetes import client

from control_plane.core.errors import (
    KubernetesConfigurationError, KubernetesOperationUnsupported, WorkspaceConflict, WorkspaceNotFound,
)
from control_plane.core.models import WorkspaceRecord

PREFIX = "io.agent-sandbox."
WID = "wsp_0123456789abcdef0123456789abcdef"
PVC_NAME = "agent-sandbox-wsp-0123456789abcdef0123456789abcdef"
UID = "45667ab6-3f02-40ba-9b29-b1967503a3f0"
OTHER_UID = "55667ab6-3f02-40ba-9b29-b1967503a3f0"
NAMESPACE = "test-sandboxes"
SC = "agent-sandbox-workspace"


def module():
    """Load only the pure module under test without constructing clients or resources."""
    return importlib.import_module("control_plane.runtime.kubernetes.workspaces")


def record(**changes):
    """Supply a fixed independent identity without sampling time or generating identifiers."""
    values = dict(workspace_id=WID, creation_token="wct_" + "a" * 32,
                  volume_name="agent-sandbox-" + WID, image_id="sha256:" + "b" * 64,
                  created_at=datetime(2026, 11, 13, 8, 0, 0, 123456, tzinfo=UTC))
    values.update(changes)
    return WorkspaceRecord(**values)


def live_pvc():
    """Attach only API observation fields to a freshly constructed independent PVC."""
    pvc = module().build_workspace_pvc(record(), namespace=NAMESPACE, storage_class=SC)
    pvc.metadata.uid, pvc.metadata.resource_version = UID, "7"
    return pvc


def verify(pvc, **changes):
    """Invoke real pure validation with fixed trusted inputs and optional original UID evidence."""
    options = dict(namespace=NAMESPACE, storage_class=SC)
    options.update(changes)
    return module().verify_workspace_pvc(pvc, record(), **options)


def test_workspace_name_is_deterministic_valid_and_injective():
    """Derive PVC names only from exact workspace IDs, including hex boundaries and distinct identities."""
    assert module().workspace_pvc_name(WID) == PVC_NAME
    assert module().workspace_pvc_name("wsp_" + "0" * 32) == "agent-sandbox-wsp-" + "0" * 32
    assert module().workspace_pvc_name("wsp_" + "f" * 32) == "agent-sandbox-wsp-" + "f" * 32
    assert module().workspace_pvc_name(WID) != module().workspace_pvc_name("wsp_" + "f" * 32)
    assert len(PVC_NAME) <= 63 and "_" not in PVC_NAME


@pytest.mark.parametrize("value", [None, True, 1, "", "wsp_" + "a" * 31, "wsp_" + "A" * 32,
    "wsp_" + "a" * 33, "agent-sandbox-" + WID, PVC_NAME, WID + "\n", "../" + WID])
def test_name_mapping_rejects_arbitrary_or_noncanonical_input(value):
    """Neither Docker volume names nor caller-selected PVC names can become a resource locator."""
    with pytest.raises((ValueError, WorkspaceNotFound)):
        module().workspace_pvc_name(value)


def test_build_has_independent_identity_and_fixed_storage():
    """The actual PVC contract carries workspace identity with no sandbox transaction or host storage."""
    original = record()
    pvc = module().build_workspace_pvc(original, namespace=NAMESPACE, storage_class=SC)
    assert (pvc.api_version, pvc.kind, pvc.metadata.name, pvc.metadata.namespace) == (
        "v1", "PersistentVolumeClaim", PVC_NAME, NAMESPACE)
    assert pvc.metadata.labels == {
        PREFIX + "managed": "true", PREFIX + "project": "agent-sandbox",
        PREFIX + "resource": "workspace", PREFIX + "workspace-id": WID,
        PREFIX + "workspace-token": "wct_" + "a" * 32,
    }
    assert pvc.metadata.annotations == {
        PREFIX + "workspace-version": "1", PREFIX + "workspace-image-id": "sha256:" + "b" * 64,
        PREFIX + "created-at": "2026-11-13T08:00:00.123456Z",
    }
    assert pvc.metadata.uid is None and pvc.metadata.owner_references in (None, [])
    assert pvc.spec.storage_class_name == SC and pvc.spec.volume_mode == "Filesystem"
    assert pvc.spec.access_modes == ["ReadWriteOnce"] and pvc.spec.resources.requests == {"storage": "1Gi"}
    assert pvc.spec.volume_name is None and pvc.spec.selector is None
    assert pvc.spec.data_source is None and pvc.spec.data_source_ref is None
    assert original.volume_name == "agent-sandbox-" + WID


def test_build_is_deterministic_and_outputs_do_not_share_mutability():
    """A caller changing one SDK result cannot rewrite another output or the input record."""
    original = record()
    before = copy.deepcopy(original)
    first = module().build_workspace_pvc(original, namespace=NAMESPACE, storage_class=SC)
    second = module().build_workspace_pvc(original, namespace=NAMESPACE, storage_class=SC)
    assert first.to_dict() == second.to_dict()
    first.metadata.labels.clear()
    first.metadata.annotations.clear()
    first.spec.access_modes.clear()
    first.spec.resources.requests.clear()
    assert second.metadata.labels[PREFIX + "workspace-id"] == WID
    assert second.metadata.annotations[PREFIX + "workspace-version"] == "1"
    assert second.spec.access_modes == ["ReadWriteOnce"] and second.spec.resources.requests == {"storage": "1Gi"}
    assert original == before


@pytest.mark.parametrize("field,value", [("volume_name", "arbitrary-volume"), ("volume_name", PVC_NAME),
    ("workspace_id", "sbx_" + "a" * 32), ("creation_token", "txn_" + "a" * 32),
    ("image_id", "agent-sandbox-basic:dev"), ("image_id", "sha256:" + "B" * 64),
    ("created_at", None), ("created_at", datetime(2026, 11, 13)),
    ("created_at", datetime(2026, 11, 13, tzinfo=timezone(timedelta(hours=1))))])
def test_forged_record_cannot_bypass_existing_identity_validation(field, value):
    """Revalidate existing frozen-model fields rather than silently repairing IDs, names, digest or time."""
    forged = record()
    object.__setattr__(forged, field, value)
    with pytest.raises((ValueError, WorkspaceNotFound)):
        module().build_workspace_pvc(forged, namespace=NAMESPACE, storage_class=SC)
    with pytest.raises((ValueError, WorkspaceNotFound)):
        module().verify_workspace_pvc(live_pvc(), forged, namespace=NAMESPACE, storage_class=SC)


@pytest.mark.parametrize("field", ["namespace", "storage_class"])
@pytest.mark.parametrize("value", [None, True, 1, "", " bad", "bad ", "bad\n", "UPPER", "a/b", "a..b", "a" * 64])
def test_explicit_storage_and_namespace_are_validated_by_both_functions(field, value):
    """Missing defaults or unsafe trusted names must reject consistently without any external lookup."""
    options = dict(namespace=NAMESPACE, storage_class=SC)
    options[field] = value
    with pytest.raises(KubernetesConfigurationError):
        module().build_workspace_pvc(record(), **options)
    with pytest.raises(KubernetesConfigurationError):
        module().verify_workspace_pvc(live_pvc(), record(), **options)


def test_uid_observation_distinguishes_initial_observation_from_original_match():
    """Returning a current UID without an expectation must never imply that replacement was ruled out."""
    pvc = live_pvc()
    observed = verify(pvc)
    assert observed.uid == UID and observed.resource_version == "7"
    assert observed.original_uid_verified is False
    original = verify(pvc, expected_uid=UID)
    assert original.uid == UID and original.original_uid_verified is True
    with pytest.raises(FrozenInstanceError):
        original.uid = OTHER_UID
    with pytest.raises(WorkspaceConflict):
        verify(pvc, expected_uid=OTHER_UID)


@pytest.mark.parametrize("value", ["", "bad", True, 1, "45667AB6-3F02-40BA-9B29-B1967503A3F0"])
def test_invalid_expected_uid_is_rejected(value):
    """Malformed caller-held original identities cannot downgrade to a first-observation comparison."""
    with pytest.raises(ValueError):
        verify(live_pvc(), expected_uid=value)


@pytest.mark.parametrize("field,value", [("name", "other"), ("namespace", "other"),
    ("uid", None), ("uid", ""), ("uid", "invalid"),
    ("resource_version", None), ("resource_version", ""), ("resource_version", "7\n")])
def test_live_metadata_must_be_complete_and_exact(field, value):
    """Identity checks reject malformed API observations and never accept a different namespace or name."""
    pvc = live_pvc()
    setattr(pvc.metadata, field, value)
    with pytest.raises(WorkspaceConflict):
        verify(pvc)


@pytest.mark.parametrize("key,value", [("managed", "false"), ("project", "other"),
    ("resource", "private-workspace"), ("resource", "external"), ("workspace-id", "wsp_" + "f" * 32),
    ("workspace-token", "wct_" + "f" * 32)])
def test_workspace_labels_cannot_be_replaced_or_removed(key, value):
    """Full independent ownership, including creation token, is required for both observed and original UID cases."""
    pvc = live_pvc()
    pvc.metadata.labels[PREFIX + key] = value
    with pytest.raises(WorkspaceConflict):
        verify(pvc, expected_uid=UID)
    del pvc.metadata.labels[PREFIX + key]
    with pytest.raises(WorkspaceConflict):
        verify(pvc)


@pytest.mark.parametrize("key,value", [("workspace-version", "2"), ("workspace-version", ""),
    ("workspace-image-id", "sha256:" + "f" * 64), ("workspace-image-id", "basic:dev"),
    ("created-at", "2026-11-13T08:00:00.123457Z"), ("created-at", "2026-11-13T08:00:00.123456+00:00"),
    ("created-at", "2026-11-13T08:00:00.123456"), ("created-at", "invalid")])
def test_workspace_annotations_require_original_image_time_and_known_version(key, value):
    """Business metadata cannot be inferred from a Pod, current time, equivalent spelling or image tag."""
    pvc = live_pvc()
    pvc.metadata.annotations[PREFIX + key] = value
    with pytest.raises(WorkspaceConflict):
        verify(pvc)
    del pvc.metadata.annotations[PREFIX + key]
    with pytest.raises(WorkspaceConflict):
        verify(pvc)


@pytest.mark.parametrize("location", ["labels", "annotations"])
@pytest.mark.parametrize("marker", ["sandbox-id", "transaction-id", "expires-at", "private-workspace-version",
    "kubernetes-proxy-version", "workspace-unknown"])
def test_instance_or_unknown_platform_markers_do_not_become_independent_workspaces(location, marker):
    """Closed platform metadata prevents private PVC import, hidden bindings or unknown-version fallback."""
    pvc = live_pvc()
    getattr(pvc.metadata, location)[PREFIX + marker] = "foreign"
    with pytest.raises(WorkspaceConflict):
        verify(pvc)


def test_sandbox_owner_references_and_foreign_pvc_are_rejected():
    """An independently owned PVC cannot be imported from a Pod-owned or unlabelled external resource."""
    pvc = live_pvc()
    pvc.metadata.owner_references = [client.V1OwnerReference(api_version="v1", kind="Pod", name="agent", uid=UID)]
    with pytest.raises(WorkspaceConflict):
        verify(pvc)
    pvc.metadata.owner_references = None
    pvc.metadata.labels = {}
    with pytest.raises(WorkspaceConflict):
        verify(pvc)


@pytest.mark.parametrize("field,value", [("storage_class_name", "other"), ("storage_class_name", None),
    ("access_modes", ["ReadWriteMany"]), ("access_modes", ["ReadWriteOnce", "ReadOnlyMany"]),
    ("volume_mode", "Block"), ("volume_mode", None),
    ("selector", client.V1LabelSelector(match_labels={"external": "volume"})),
    ("data_source", client.V1TypedLocalObjectReference(kind="PersistentVolumeClaim", name="external")),
    ("data_source_ref", client.V1TypedObjectReference(kind="VolumeSnapshot", name="snapshot")),
    ("volume_attributes_class_name", "other"), ("volume_name", "../pv")])
def test_storage_routing_and_external_data_sources_cannot_change(field, value):
    """Reject broader storage permissions, unsafe volume references, snapshots and alternate data sources."""
    pvc = live_pvc()
    setattr(pvc.spec, field, value)
    with pytest.raises(WorkspaceConflict):
        verify(pvc)


@pytest.mark.parametrize("requests,limits", [({"storage": "2Gi"}, None), ({"storage": "1G"}, None),
    ({}, None), ({"storage": "1Gi", "cpu": "1"}, None), ({"storage": "1Gi"}, {"storage": "1Gi"}),
    ({"storage": "NaN"}, None)])
def test_capacity_requests_remain_fixed_and_bounded(requests, limits):
    """Fixed one-GiB requests cannot be silently enlarged, shrunk or supplemented with unsupported limits."""
    pvc = live_pvc()
    pvc.spec.resources.requests, pvc.spec.resources.limits = requests, limits
    with pytest.raises(WorkspaceConflict):
        verify(pvc)


@pytest.mark.parametrize("status", [None, client.V1PersistentVolumeClaimStatus(phase="Pending"),
    client.V1PersistentVolumeClaimStatus(phase="Bound"), client.V1PersistentVolumeClaimStatus(phase="Lost"),
    client.V1PersistentVolumeClaimStatus(phase="Unknown", conditions=[
        client.V1PersistentVolumeClaimCondition(type="Resizing", status="True")])])
def test_health_and_binding_status_are_separate_from_identity(status):
    """Pure identity/spec validation neither waits for binding nor makes a health claim from controller status."""
    pvc = live_pvc()
    pvc.status = status
    assert verify(pvc, expected_uid=UID).original_uid_verified is True


def test_controller_binding_and_nonplatform_metadata_are_not_ownership_evidence():
    """Allow structurally valid controller fields without verifying a PV or changing the workspace identity."""
    pvc = live_pvc()
    pvc.spec.volume_name = "pvc-" + UID
    pvc.metadata.annotations["pv.kubernetes.io/bind-completed"] = "yes"
    pvc.metadata.labels["example.org/trace"] = "optional"
    assert verify(pvc).uid == UID


def test_holder_and_management_changes_do_not_rewrite_independent_pvc_identity():
    """Database occupancy, management state and CAS versions are not sandbox ownership metadata on the PVC."""
    held = replace(record(), version=8, management_state="available", sandbox_id="sbx_" + "c" * 32,
                   transaction_id="txn_" + "d" * 32)
    pvc = module().build_workspace_pvc(held, namespace=NAMESPACE, storage_class=SC)
    assert pvc.to_dict() == module().build_workspace_pvc(record(), namespace=NAMESPACE, storage_class=SC).to_dict()
    pvc.metadata.uid, pvc.metadata.resource_version = UID, "7"
    assert module().verify_workspace_pvc(pvc, held, namespace=NAMESPACE, storage_class=SC).uid == UID


@pytest.mark.parametrize("field,value", [("api_version", "v2"), ("kind", "Pod"), ("spec", None), ("metadata", None)])
def test_wrong_kind_or_malformed_objects_fail_with_fixed_domain_error(field, value):
    """A different object shape cannot escape as an AttributeError or be accepted by matching labels alone."""
    pvc = live_pvc()
    setattr(pvc, field, value)
    with pytest.raises(WorkspaceConflict) as caught:
        verify(pvc)
    assert str(caught.value) == "workspace operation failed"


@pytest.mark.parametrize("location,value", [("labels", None), ("annotations", None), ("labels", {1: "bad"}),
    ("annotations", {PREFIX + "created-at": []})])
def test_malformed_metadata_maps_cannot_escape_fixed_errors(location, value):
    """Invalid map keys or values are observations of conflict, never arbitrary runtime exceptions."""
    pvc = live_pvc()
    # The SDK normally rejects these at assignment. Bypass only that outer
    # guard to exercise our handling of a malformed transport/test object.
    object.__setattr__(pvc.metadata, location, value)
    with pytest.raises(WorkspaceConflict):
        verify(pvc)


def test_private_pvc_cannot_be_imported_by_independent_verification():
    """A genuine private-layout PVC remains private even if its name is forcibly changed to the expected one."""
    from control_plane.runtime.kubernetes.private_workspace import build_private_workspace
    from test_kubernetes_pod_spec import sandbox_spec
    pvc = build_private_workspace(sandbox_spec(), namespace=NAMESPACE, storage_class=SC)
    pvc.metadata.name, pvc.metadata.uid, pvc.metadata.resource_version = PVC_NAME, UID, "7"
    with pytest.raises(WorkspaceConflict):
        verify(pvc)


def test_import_build_and_verify_have_no_external_side_effects(monkeypatch):
    """Guard real external boundaries while executing import, construction and verification, not a mocked builder."""
    import os
    import secrets
    import socket
    import sqlite3
    import subprocess
    from pathlib import Path

    def forbidden(*args, **kwargs):
        """Reject any unexpected SDK acquisition, file/database/process/network I/O or identity generation."""
        pytest.fail("pure workspace code attempted an external operation")

    with monkeypatch.context() as patch:
        for owner, key in ((client, "ApiClient"), (client, "CoreV1Api"), (Path, "open"),
            (__import__("builtins"), "open"), (sqlite3, "connect"), (subprocess, "Popen"),
            (socket, "socket"), (socket, "create_connection"), (secrets, "token_hex"),
            (secrets, "token_urlsafe"), (os, "getenv")):
            patch.setattr(owner, key, forbidden)
        subject = importlib.reload(module())
        pvc = subject.build_workspace_pvc(record(), namespace=NAMESPACE, storage_class=SC)
        pvc.metadata.uid, pvc.metadata.resource_version = UID, "7"
        observed = subject.verify_workspace_pvc(pvc, record(), namespace=NAMESPACE, storage_class=SC, expected_uid=UID)
        assert observed.uid == UID


@pytest.mark.anyio
async def test_runtime_public_workspace_operations_remain_unsupported():
    """Pure PVC foundations must not permit partial public lifecycle flows or immutable-image substitution."""
    from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime

    def forbidden(**kwargs):
        """Reject any attempted lazy client acquisition by an unsupported public operation."""
        pytest.fail("unsupported workspace operation acquired a client")

    backend = KubernetesSandboxRuntime(kubeconfig_path="/not-read", context="explicit", namespace=NAMESPACE,
                                       client_factory=forbidden)
    for action in (backend.resolve_environment_image("basic"), backend.create_workspace(record()),
                   backend.inspect_workspace(record()), backend.delete_workspace(record())):
        with pytest.raises(KubernetesOperationUnsupported):
            await action
