"""Protect independent local workspaces and immutable instance binding metadata."""

from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from docker.errors import DockerException, ImageNotFound, NotFound
from requests import exceptions as requests_exceptions

from control_plane.core.errors import (
    SandboxEnvironmentNotRegistered, SandboxImageUnavailable,
    WorkspaceConflict, WorkspaceError, WorkspaceNotFound,
    WorkspaceOperationFailed, WorkspaceUnavailable,
)
from control_plane.core.models import WorkspaceBinding, WorkspaceRecord

PREFIX = "io.agent-sandbox."
BINDING_KEYS = tuple(PREFIX + suffix for suffix in (
    "workspace-binding-version", "workspace-id", "workspace-token",
    "workspace-volume", "workspace-image-id",
))


def binding_labels(binding: WorkspaceBinding) -> dict[str, str]:
    """Serialize immutable ownership markers independently of database CAS versions."""
    return dict(zip(BINDING_KEYS, (
        "1", binding.workspace_id, binding.creation_token,
        binding.volume_name, binding.image_id,
    ), strict=True))


def binding_from_labels(
    labels: dict[str, str], sandbox_id: str, transaction_id: str,
) -> WorkspaceBinding | None:
    """Restore complete v1 metadata; partial or unknown markers never mean legacy."""
    if not any(key in labels for key in BINDING_KEYS):
        return None
    if labels.get(BINDING_KEYS[0]) != "1" or any(key not in labels for key in BINDING_KEYS):
        raise WorkspaceConflict()
    try:
        return WorkspaceBinding(
            workspace_id=labels[BINDING_KEYS[1]], creation_token=labels[BINDING_KEYS[2]],
            volume_name=labels[BINDING_KEYS[3]], image_id=labels[BINDING_KEYS[4]],
            version=0, sandbox_id=sandbox_id, transaction_id=transaction_id,
        )
    except (ValueError, TypeError) as error:
        raise WorkspaceConflict() from error


def workspace_labels(record: WorkspaceRecord | WorkspaceBinding) -> dict[str, str]:
    """Return complete independent ownership, without instance or lifecycle labels."""
    return {
        PREFIX + "managed": "true", PREFIX + "project": "agent-sandbox",
        PREFIX + "resource": "workspace", PREFIX + "workspace-version": "1",
        PREFIX + "workspace-id": record.workspace_id,
        PREFIX + "workspace-token": record.creation_token,
        PREFIX + "workspace-image-id": record.image_id,
    }


def workspace_record(binding: WorkspaceBinding) -> WorkspaceRecord:
    """Adapt an observed immutable binding to the shared read-only volume verifier."""
    return WorkspaceRecord(
        binding.workspace_id, binding.creation_token, binding.volume_name,
        binding.image_id, datetime(1970, 1, 1, tzinfo=UTC),
        sandbox_id=binding.sandbox_id, transaction_id=binding.transaction_id,
    )


def verified_volume(client: Any, record: WorkspaceRecord | WorkspaceBinding) -> Any:
    """Load exact existing local storage and reject foreign, substituted or unsafe volumes."""
    try:
        volume = client.volumes.get(record.volume_name)
        volume.reload()
    except NotFound as error:
        raise WorkspaceNotFound(record.workspace_id) from error
    attrs = volume.attrs
    if (
        volume.id != record.volume_name or attrs.get("Name") != record.volume_name
        or attrs.get("Driver") != "local" or attrs.get("Options") not in (None, {})
        or not isinstance(attrs.get("Labels"), dict)
        or any(attrs["Labels"].get(key) != value for key, value in workspace_labels(record).items())
        or PREFIX + "sandbox-id" in attrs["Labels"]
        or PREFIX + "transaction-id" in attrs["Labels"]
    ):
        raise WorkspaceConflict(record.workspace_id)
    return volume


def verify_workspace_mount(container: Any, binding: WorkspaceBinding) -> None:
    """Require exactly the bound writable volume at /workspace, with no additional mounts."""
    mounts = container.attrs.get("Mounts")
    if not isinstance(mounts, list) or len(mounts) != 1:
        raise WorkspaceConflict(binding.workspace_id)
    mount = mounts[0]
    if not isinstance(mount, dict) or (
        mount.get("Type") != "volume" or mount.get("Name") != binding.volume_name
        or mount.get("Destination") != "/workspace" or mount.get("RW") is not True
    ):
        raise WorkspaceConflict(binding.workspace_id)


def verify_workspace_references(
    client: Any, record: WorkspaceRecord, *, require_unused: bool = False,
) -> None:
    """Inspect every actual container reference and allow only its persisted exact holder."""
    references = client.containers.list(all=True, filters={"volume": record.volume_name})
    if references and require_unused:
        raise WorkspaceConflict(record.workspace_id)
    for candidate in references:
        container = client.containers.get(candidate.id)
        container.reload()
        labels = container.attrs.get("Config", {}).get("Labels", {})
        if (not record.sandbox_id or not record.transaction_id or container.id != candidate.id
            or labels.get(PREFIX + "managed") != "true"
            or labels.get(PREFIX + "project") != "agent-sandbox"
            or labels.get(PREFIX + "resource") != "container"
            or labels.get(PREFIX + "sandbox-id") != record.sandbox_id
            or labels.get(PREFIX + "transaction-id") != record.transaction_id):
            raise WorkspaceConflict(record.workspace_id)
        binding = binding_from_labels(labels, record.sandbox_id, record.transaction_id)
        if binding is None or workspace_labels(binding) != workspace_labels(record) or binding.volume_name != record.volume_name:
            raise WorkspaceConflict(record.workspace_id)
        verify_workspace_mount(container, binding)


def resolve_environment_image_sync(
    factory: Callable[[], Any], environment: str, images: Mapping[str, str],
) -> str:
    """Preflight only the selected local Agent image and pin its immutable ID."""
    from control_plane.runtime.docker.transaction import _validate_image_mounts

    if environment not in images:
        raise SandboxEnvironmentNotRegistered()
    client = None
    completed = False
    try:
        client = factory()
        image = client.images.get(images[environment])
        _validate_image_mounts(image)
        if not isinstance(image.id, str) or not image.id:
            raise SandboxImageUnavailable()
        completed = True
        return image.id
    except (SandboxEnvironmentNotRegistered, SandboxImageUnavailable):
        raise
    except ImageNotFound as error:
        raise SandboxImageUnavailable() from error
    except Exception as error:
        raise WorkspaceUnavailable() from error
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as error:
                if completed:
                    raise WorkspaceUnavailable() from error


def inspect_workspace_sync(factory: Callable[[], Any], record: WorkspaceRecord) -> None:
    """Verify independent storage and all live or stopped references without mutation."""
    _workspace_operation(factory, record, "inspect")


def delete_workspace_sync(factory: Callable[[], Any], record: WorkspaceRecord) -> None:
    """Delete only unused verified independent storage, confirming exact-name absence."""
    _workspace_operation(factory, record, "delete")


def create_workspace_sync(factory: Callable[[], Any], record: WorkspaceRecord) -> None:
    """Create a new local volume and initialize image contents exactly once, retaining uncertainty."""
    _workspace_operation(factory, record, "create")


def _workspace_operation(factory: Callable[[], Any], record: WorkspaceRecord, action: str) -> None:
    """Own transport closure and stable failures around a synchronous workspace operation."""
    client = None
    completed = False
    try:
        client = factory()
        if action == "create":
            _create_workspace(client, record)
        else:
            try:
                volume = verified_volume(client, record)
            except WorkspaceNotFound:
                if action == "delete":
                    verify_workspace_references(client, record, require_unused=True)
                    completed = True
                    return
                raise
            verify_workspace_references(client, record, require_unused=action == "delete")
            if action == "delete":
                # Reinspect after the reference scan and never use force for storage removal.
                volume = verified_volume(client, record)
                try:
                    volume.remove()
                except Exception:
                    try:
                        client.volumes.get(record.volume_name)
                    except NotFound:
                        completed = True
                        return
                    raise
                try:
                    client.volumes.get(record.volume_name)
                except NotFound:
                    pass
                else:
                    raise WorkspaceOperationFailed(record.workspace_id)
        completed = True
    except WorkspaceError:
        raise
    except (DockerException, requests_exceptions.RequestException, TimeoutError) as error:
        failure = WorkspaceUnavailable if action == "inspect" else WorkspaceOperationFailed
        raise failure(record.workspace_id) from error
    except Exception as error:
        raise WorkspaceOperationFailed(record.workspace_id) from error
    finally:
        if client is not None:
            try:
                client.close()
            except Exception as error:
                if completed:
                    raise WorkspaceOperationFailed(record.workspace_id) from error


def _create_workspace(client: Any, record: WorkspaceRecord) -> None:
    """Reject preexisting names and keep the fresh volume when initialization is uncertain."""
    from control_plane.runtime.docker.transaction import _validate_image_mounts

    image = client.images.get(record.image_id)
    if image.id != record.image_id:
        raise WorkspaceConflict(record.workspace_id)
    _validate_image_mounts(image)
    try:
        client.volumes.get(record.volume_name)
    except NotFound:
        pass
    else:
        raise WorkspaceConflict(record.workspace_id)
    response = client.api.create_volume(record.volume_name, driver="local", labels=workspace_labels(record))
    if response.get("Name") != record.volume_name or response.get("Labels") != workspace_labels(record):
        raise WorkspaceConflict(record.workspace_id)
    verified_volume(client, record)
    # Docker initializes the fresh volume from the pinned image before its nonroot Agent runs.
    host_config = client.api.create_host_config(
        init=True, network_mode="none", nano_cpus=500_000_000,
        mem_limit=128 * 1024 * 1024, memswap_limit=128 * 1024 * 1024,
        pids_limit=32, cap_drop=["ALL"], security_opt=["no-new-privileges:true"],
        read_only=True, tmpfs={"/tmp": "rw,nosuid,nodev,size=16m,mode=1777"},
        binds={record.volume_name: {"bind": "/workspace", "mode": "rw"}},
    )
    initializer_labels = {**workspace_labels(record), PREFIX + "resource": "workspace-initializer"}
    response = client.api.create_container(
        image=record.image_id, entrypoint=["/usr/local/bin/agent"], command=["-help"],
        user="sandbox", working_dir="/workspace", volumes=["/workspace"],
        labels=initializer_labels, host_config=host_config, network_disabled=True,
        healthcheck={"test": ["NONE"]},
    )
    identity = response.get("Id")
    if not isinstance(identity, str) or not identity:
        raise WorkspaceOperationFailed(record.workspace_id)
    try:
        container = _verified_initializer(client, identity, initializer_labels, record)
        container.start()
        result = container.wait(timeout=5)
        if not isinstance(result, dict) or result.get("StatusCode") != 0:
            raise WorkspaceOperationFailed(record.workspace_id)
    finally:
        # Initialization owns this exact container only, never the independent data volume.
        container = _verified_initializer(client, identity, initializer_labels, record)
        container.remove(force=True, v=False)
        try:
            client.containers.get(identity)
        except NotFound:
            pass
        else:
            raise WorkspaceOperationFailed(record.workspace_id)
    verified_volume(client, record)
    verify_workspace_references(client, record, require_unused=True)


def _verified_initializer(
    client: Any, identity: str, labels: dict[str, str], record: WorkspaceRecord,
) -> Any:
    """Authorize initializer cleanup from its exact returned ID and every ownership marker."""
    container = client.containers.get(identity)
    container.reload()
    observed = container.attrs.get("Config", {}).get("Labels", {})
    if container.id != identity or any(observed.get(key) != value for key, value in labels.items()):
        raise WorkspaceConflict(record.workspace_id)
    return container
