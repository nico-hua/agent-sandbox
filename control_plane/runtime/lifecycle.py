"""Converge owned Docker sandbox pause, resume and delete operations."""

import threading
from collections.abc import Callable
from typing import Any, Literal

from docker.errors import NotFound

from control_plane.domain.errors import (
    DockerUnavailable,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxError,
    SandboxNotFound,
    SandboxPauseConflict,
    SandboxPauseFailed,
    SandboxQueryFailed,
    SandboxResumeConflict,
    SandboxResumeFailed,
    WorkspaceDeleteProtected,
)
from control_plane.domain.models import SandboxNetwork, SandboxSpec, WorkspaceBinding
from control_plane.runtime.creation import CreationCancelled
from control_plane.runtime.inspection import (
    _egress_observation,
    _endpoint_auth_facts,
    _verify_entry_networks,
)
from control_plane.runtime.resources import (
    DockerClient,
    _ResourceRecord,
    _inspect_owned,
    _is_absent,
    _remove_resource,
    discover_sandbox_resources,
    inventory_network_configuration,
    network_configuration,
    resource_labels,
)
from control_plane.runtime.workspaces import binding_from_labels, verify_workspace_mount


def _execution_state(container: Any) -> Literal["paused", "running"]:
    """Accept only live or frozen inspect facts, with Docker's Paused flag taking precedence."""
    state = container.attrs.get("State", {})
    if state.get("Status") not in {"running", "paused"}:
        raise SandboxQueryFailed()
    if state.get("Paused") is True or state.get("Status") == "paused":
        return "paused"
    return "running"


def _pause_preflight(
    client: DockerClient, sandbox_id: str, records: tuple[_ResourceRecord, ...],
    configuration: SandboxNetwork,
) -> dict[str, Any]:
    """Reinspect the complete original inventory and reuse query topology checks before mutation."""
    resources: dict[str, Any] = {}
    for record in records:
        resource = _inspect_owned(client, record)
        if not record.external and network_configuration(resource_labels(resource, record.kind)) != network_configuration(record.observed_labels or {}):
            raise SandboxQueryFailed()
        resources[record.kind] = resource
    required = {"container", "proxy", "network", "ingress", "workspace"}
    if configuration.allow_domains:
        required.add("egress")
    if set(resources) != required:
        raise SandboxQueryFailed()
    main = next(record for record in records if record.kind == "container")
    spec = SandboxSpec(
        sandbox_id=sandbox_id, container_name=f"agent-sandbox-{sandbox_id}",
        network_name=f"agent-sandbox-{sandbox_id}-internal",
        volume_name=f"agent-sandbox-{sandbox_id}-workspace",
        transaction_id=main.labels["io.agent-sandbox.transaction-id"],
        network=configuration,
    )
    _endpoint_auth_facts(client, records, resources["proxy"])
    _verify_entry_networks(
        client, resources["container"], resources["proxy"], spec,
        expected_network_ids={
            record.kind: record.resource_id
            for record in records if record.kind in {"network", "ingress"}
        },
    )
    if configuration.allow_domains:
        observation = _egress_observation(
            client, sandbox_id, records, configuration, observe_state=False,
        )
        if observation is not None:
            raise SandboxQueryFailed()
    binding = binding_from_labels(
        resource_labels(resources["container"], "container"), sandbox_id, spec.transaction_id,
    )
    if binding is not None:
        verify_workspace_mount(resources["container"], binding)
    for role in ("container", "egress"):
        if role in resources:
            _execution_state(resources[role])
    return resources


def change_pause_sync(
    client_factory: Callable[[], DockerClient], sandbox_id: str, *,
    expected_transaction_id: str, paused: bool, cancelled: threading.Event,
    expected_workspace: WorkspaceBinding | None = None,
) -> None:
    """Converge a transaction's exact targets in safe order, inspecting uncertain SDK responses."""
    conflict = SandboxPauseConflict if paused else SandboxResumeConflict
    failed = SandboxPauseFailed if paused else SandboxResumeFailed
    try:
        client = client_factory()
    except Exception as error:
        raise DockerUnavailable() from error
    attempted = False
    completed = False
    try:
        try:
            records = discover_sandbox_resources(client, sandbox_id, expected_transaction_id, expected_workspace=expected_workspace)
        except SandboxDeleteConflict as error:
            raise conflict(sandbox_id) from error
        except Exception as error:
            raise DockerUnavailable() from error
        if not any(record.kind == "container" for record in records):
            raise SandboxNotFound()
        try:
            configuration = inventory_network_configuration(records)
            _pause_preflight(client, sandbox_id, records, configuration)
        except (SandboxError, NotFound, ValueError, TypeError, KeyError) as error:
            raise conflict(sandbox_id) from error
        except Exception as error:
            raise DockerUnavailable() from error
        order = ("container", "egress") if paused else ("egress", "container")
        targets = [record for role in order for record in records if record.kind == role]
        desired = "paused" if paused else "running"
        for record in targets:
            if cancelled.is_set():
                raise CreationCancelled()
            try:
                # Revalidate the original full topology before each target, then its exact identity.
                _pause_preflight(client, sandbox_id, records, configuration)
                resource = _inspect_owned(client, record)
                if _execution_state(resource) == desired:
                    continue
                if cancelled.is_set():
                    raise CreationCancelled()
                attempted = True
                try:
                    if paused:
                        resource.pause()
                    else:
                        resource.unpause()
                except Exception:
                    # A lost response may follow a committed mutation; inspect, never compensate.
                    observed = _inspect_owned(client, record)
                    if _execution_state(observed) != desired:
                        raise
                observed = _inspect_owned(client, record)
                if _execution_state(observed) != desired:
                    raise RuntimeError("requested Docker pause state was not observed")
            except CreationCancelled:
                raise
            except Exception as error:
                raise (failed(sandbox_id) if attempted else conflict(sandbox_id)) from error
        try:
            resources = _pause_preflight(client, sandbox_id, records, configuration)
            if any(_execution_state(resources[record.kind]) != desired for record in targets):
                raise RuntimeError("sandbox pause state changed before completion")
        except Exception as error:
            raise (failed(sandbox_id) if attempted else conflict(sandbox_id)) from error
        completed = True
    finally:
        try:
            client.close()
        except Exception as error:
            if completed:
                raise failed(sandbox_id) from error


def delete_sandbox_sync(
    client_factory: Callable[[], DockerClient],
    sandbox_id: str,
    *,
    delete_workspace: bool = False,
    expected_transaction_id: str | None = None,
    expected_workspace: WorkspaceBinding | None = None,
) -> None:
    """Preflight and reclaim resources, verify absence, and retain workspace by default."""
    try:
        client = client_factory()
    except Exception as error:
        raise DockerUnavailable() from error

    completed = False
    try:
        try:
            records = discover_sandbox_resources(
                client, sandbox_id, expected_transaction_id, expected_workspace=expected_workspace,
                allow_missing_workspace=True,
            )
        except SandboxDeleteConflict:
            raise
        except Exception as error:
            raise DockerUnavailable() from error

        if delete_workspace and (
            expected_workspace is not None or any(record.external for record in records)
            or any("io.agent-sandbox.workspace-binding-version" in record.labels for record in records)
        ):
            raise WorkspaceDeleteProtected()
        failed_records: list[_ResourceRecord] = []
        for record in records:
            if record.external or (record.kind == "workspace" and not delete_workspace):
                continue
            try:
                _remove_resource(client, record)
            except SandboxDeleteConflict:
                # Stop destructive work immediately if ownership changes after discovery.
                raise
            except Exception:
                # Continue verified items; retries reclaim leftovers without restoring removals.
                failed_records.append(record)

        failed = False
        for record in failed_records:
            try:
                # A peer may finish a failed item; only its exact-ID absence clears the failure.
                # A label rescan alone could hide a leftover whose ownership changed.
                if not _is_absent(client, record):
                    failed = True
            except SandboxDeleteConflict:
                raise
            except Exception:
                failed = True

        # Rescan for overlooked owned resources; a workspace retained by default is not a failure.
        try:
            remaining = discover_sandbox_resources(
                client, sandbox_id, expected_transaction_id, expected_workspace=expected_workspace,
                allow_missing_workspace=True,
            )
        except SandboxDeleteConflict:
            raise
        except Exception as error:
            raise SandboxDeleteFailed(sandbox_id) from error
        if failed or any(
            (not record.external) and (record.kind != "workspace" or delete_workspace) for record in remaining
        ):
            raise SandboxDeleteFailed(sandbox_id)
        completed = True
    finally:
        try:
            client.close()
        except Exception as error:
            if completed:
                raise SandboxDeleteFailed(sandbox_id) from error
