"""Versioned independent PVC references, strict mounts and borrowed-client observations, never storage ownership."""

import json
from dataclasses import replace
from datetime import datetime
from kubernetes.utils.quantity import parse_quantity

from control_plane.core.errors import SandboxQueryFailed, WorkspaceConflict, WorkspaceNotFound
from control_plane.core.models import WorkspaceBinding, WorkspaceResourceReceipt, WorkspaceRecord, format_utc_datetime
from control_plane.runtime.kubernetes.pod_query import (
    LABEL_PREFIX, PROXY_LAYOUT_ANNOTATION, PROXY_BOUND_LAYOUT_VERSION, WORKSPACE_BINDING_ANNOTATION,
)
from control_plane.runtime.kubernetes.workspaces import workspace_pvc_name, WorkspaceResource

_FIELDS = frozenset({'version', 'workspace_id', 'creation_token', 'volume_name', 'image_id', 'created_at',
                     'namespace', 'pvc_name', 'pvc_uid', 'storage_class'})


def validate_binding(binding, receipt, *, sandbox_id, transaction_id, namespace, image=None):
    """Copy only a complete original matching the current holder and trusted namespace/image contract."""
    try:
        if type(binding) is not WorkspaceBinding or type(receipt) is not WorkspaceResourceReceipt:
            raise ValueError()
        binding, receipt = replace(binding), replace(receipt)
        if (binding.sandbox_id != sandbox_id or binding.transaction_id != transaction_id
            or receipt.namespace != namespace or receipt.pvc_name != workspace_pvc_name(binding.workspace_id)
            or any(getattr(binding, field) != getattr(receipt, field)
                   for field in ('workspace_id', 'creation_token', 'volume_name', 'image_id'))):
            raise ValueError()
        if image is not None:
            from control_plane.runtime.kubernetes.pod_spec import validate_agent_image
            validate_agent_image(image)
            if '@' not in image or image.partition('@')[2] != receipt.image_id:
                raise ValueError()
        return binding, receipt
    except (ValueError, TypeError, AttributeError):
        raise WorkspaceConflict() from None


def binding_annotation(spec, namespace, image):
    """Persist immutable workspace facts separately from the changing SQLite occupancy version."""
    _, receipt = validate_binding(spec.workspace, spec.workspace_receipt, sandbox_id=spec.sandbox_id,
        transaction_id=spec.transaction_id, namespace=namespace, image=image)
    values = {name: getattr(receipt, name) for name in _FIELDS - {'version', 'created_at'}}
    values.update(version=1, created_at=format_utc_datetime(receipt.created_at))
    return json.dumps(values, separators=(',', ':'), sort_keys=True)


def read_binding(metadata):
    """Decode only the explicit v4 original reference, rejecting unknown keys, versions and mutable holder fields."""
    try:
        labels, annotations = metadata.labels or {}, metadata.annotations or {}
        raw = annotations.get(WORKSPACE_BINDING_ANNOTATION)
        if (annotations.get(PROXY_LAYOUT_ANNOTATION) != PROXY_BOUND_LAYOUT_VERSION
            or WORKSPACE_BINDING_ANNOTATION in labels or type(raw) is not str or not 2 <= len(raw) <= 1536):
            raise ValueError()
        pairs = json.loads(raw, object_pairs_hook=list)
        if not isinstance(pairs, list) or any(not isinstance(pair, tuple) or len(pair) != 2 for pair in pairs):
            raise ValueError()
        values = dict(pairs)
        if len(values) != len(pairs) or set(values) != _FIELDS or type(values['version']) is not int or values.pop('version') != 1:
            raise ValueError()
        raw_time = values['created_at']
        values['created_at'] = datetime.fromisoformat(raw_time)
        if format_utc_datetime(values['created_at']) != raw_time:
            raise ValueError()
        receipt = WorkspaceResourceReceipt(**values, backend='kubernetes', original_uid_verified=True)
        binding = WorkspaceBinding(receipt.workspace_id, receipt.creation_token, receipt.volume_name,
            receipt.image_id, 0, labels[LABEL_PREFIX + 'sandbox-id'], labels[LABEL_PREFIX + 'transaction-id'])
        return validate_binding(binding, receipt, sandbox_id=binding.sandbox_id,
                                transaction_id=binding.transaction_id, namespace=metadata.namespace)
    except (ValueError, TypeError, AttributeError, KeyError, RecursionError, WorkspaceConflict):
        raise SandboxQueryFailed() from None


def workspace_record(receipt):
    """Reconstruct only immutable independent identity for borrowed storage operations, never database occupancy."""
    return WorkspaceRecord(receipt.workspace_id, receipt.creation_token, receipt.volume_name,
                           receipt.image_id, receipt.created_at)


def verify_mount(pod):
    """Require exactly the original claim at /workspace and an emptyDir at /tmp, without alternate mounts."""
    binding, receipt = read_binding(pod.metadata)
    try:
        if len(pod.spec.containers) != 1 or pod.spec.containers[0].name != 'agent':
            raise ValueError()
        validate_binding(binding, receipt, sandbox_id=binding.sandbox_id, transaction_id=binding.transaction_id,
                         namespace=pod.metadata.namespace, image=pod.spec.containers[0].image)
        volumes = pod.spec.volumes
        mounts = pod.spec.containers[0].volume_mounts
        if (not isinstance(volumes, list) or len(volumes) != 2 or {v.name for v in volumes} != {'workspace', 'tmp'}
            or not isinstance(mounts, list) or len(mounts) != 2 or {m.name for m in mounts} != {'workspace', 'tmp'}):
            raise ValueError()
        for volume in volumes:
            source = 'persistent_volume_claim' if volume.name == 'workspace' else 'empty_dir'
            if getattr(volume, source) is None or any(getattr(volume, key) is not None
                    for key in volume.attribute_map if key not in {'name', source}):
                raise ValueError()
            if source == 'persistent_volume_claim' and (volume.persistent_volume_claim.claim_name != receipt.pvc_name
                or volume.persistent_volume_claim.read_only not in (None, False)):
                raise ValueError()
        for mount in mounts:
            if (mount.mount_path != '/' + mount.name or mount.read_only not in (None, False)
                or any(getattr(mount, key) is not None for key in mount.attribute_map
                       if key not in {'name', 'mount_path', 'read_only'})):
                raise ValueError()
        return receipt
    except (ValueError, TypeError, AttributeError, IndexError, WorkspaceConflict):
        raise SandboxQueryFailed() from None


def inspect_reference(api, pod, *, require_bound=False, check_consumers=False):
    """Read the external original without adopting it, waiting for Bound or granting deletion authority."""
    receipt = verify_mount(pod)
    return inspect_receipt(api, receipt, require_bound=require_bound, check_consumers=check_consumers)


def inspect_receipt(api, receipt, *, require_bound=False, check_consumers=False):
    """Check an original reference from any surviving resource, independently of whether its consumer still exists."""
    resource = WorkspaceResource(api, workspace_record(receipt), namespace=receipt.namespace, storage_class=receipt.storage_class)
    pvc = resource.inspect_for_mount(receipt.pvc_uid, check_consumers=check_consumers)
    if pvc is None:
        raise WorkspaceNotFound(receipt.workspace_id)
    if require_bound:
        try:
            status = pvc.status
            if (pvc.metadata.deletion_timestamp is not None or status is None or status.phase != 'Bound'
                or not isinstance(pvc.spec.volume_name, str) or not pvc.spec.volume_name
                or status.access_modes != ['ReadWriteOnce'] or not isinstance(status.capacity, dict)
                or parse_quantity(status.capacity.get('storage', '0')) < parse_quantity('1Gi')
                or any(c.status not in {'True', 'False'} or (c.type != 'Unused' and c.status != 'False')
                       for c in status.conditions or [])):
                raise SandboxQueryFailed()
        except (ValueError, TypeError, AttributeError, ArithmeticError):
            raise SandboxQueryFailed() from None
    elif pvc.metadata.deletion_timestamp is not None:
        raise SandboxQueryFailed()
    return receipt, pvc
