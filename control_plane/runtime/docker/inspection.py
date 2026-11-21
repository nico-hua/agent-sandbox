"""Orchestrate read-only Docker state, endpoint and pending resource queries."""

import logging
from collections.abc import Callable
from dataclasses import replace
from typing import Any, Literal

from docker.errors import DockerException, NotFound
from requests import exceptions as requests_exceptions

from control_plane.domain.endpoint import SandboxEndpoint
from control_plane.domain.errors import (
    DockerUnavailable,
    SandboxDeleteConflict,
    SandboxEndpointUnavailable,
    SandboxError,
    SandboxNotFound,
    SandboxQueryFailed,
    WorkspaceError,
)
from control_plane.domain.maintenance import SandboxResourceInspection
from control_plane.domain.sandbox import (
    ResourceKind,
    SandboxNetwork,
    SandboxQueryResult,
    SandboxSpec,
)
from control_plane.domain.workspace import WorkspaceBinding
from control_plane.runtime.docker.client import DockerClient
from control_plane.runtime.docker.egress import _egress_observation
from control_plane.runtime.docker.endpoint import (
    _endpoint_auth_facts,
    _endpoint_services_version,
    _owned_container,
    _published_loopback_url,
    _verify_entry_networks,
)
from control_plane.runtime.docker.resources import (
    _lifecycle_times,
    _ResourceRecord,
    discover_sandbox_resources,
    inventory_network_configuration,
    network_configuration,
    query_labels_match,
    resource_labels,
)
from control_plane.runtime.docker.workspaces import (
    binding_from_labels,
    verify_workspace_mount,
)

_LOGGER = logging.getLogger(__name__)


def inspect_resources_sync(
    client_factory: Callable[[], DockerClient], sandbox_id: str,
    *, expected_transaction_id: str,
    expected_workspace: WorkspaceBinding | None = None,
) -> SandboxResourceInspection:
    """Classify a verified inventory without changing resources or checking creation success."""
    client = None
    result = SandboxResourceInspection(sandbox_id, "unavailable", "docker_unavailable")
    try:
        client = client_factory()
        # Discovery checks every role independently and never filters by transaction.
        records = discover_sandbox_resources(client, sandbox_id, expected_transaction_id, expected_workspace=expected_workspace)
        for record in records:
            if record.kind == "container":
                labels = record.observed_labels or {}
                binding = binding_from_labels(labels, sandbox_id, expected_transaction_id)
                if binding is not None:
                    verify_workspace_mount(client.containers.get(record.resource_id), binding)
        proxy_record = next((record for record in records if record.kind == "proxy"), None)
        try:
            _endpoint_auth_facts(client, records, client.containers.get(proxy_record.resource_id) if proxy_record else None)
        except SandboxEndpointUnavailable:
            result = SandboxResourceInspection(sandbox_id, "conflict", "endpoint_auth_configuration_conflict")
        else:
            result = _classify_resource_inventory(sandbox_id, records)
    except (SandboxDeleteConflict, WorkspaceError):
        # An interrupted scan cannot reliably identify every missing role.
        result = SandboxResourceInspection(
            sandbox_id, "conflict", "resource_ownership_conflict",
        )
    except Exception:  # noqa: BLE001
        # Preserve uncertainty rather than treating a failed list/inspect as absence.
        result = SandboxResourceInspection(sandbox_id, "unavailable", "docker_unavailable")
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:  # noqa: BLE001
                # Keep the primary conflict; standalone close failures make the read unavailable.
                if result.classification != "conflict":
                    result = SandboxResourceInspection(
                        sandbox_id, "unavailable", "docker_unavailable",
                    )
    return result


def _classify_resource_inventory(
    sandbox_id: str, records: tuple[_ResourceRecord, ...],
) -> SandboxResourceInspection:
    """Classify expected roles from policy snapshots; backend failures are handled by the caller."""
    try:
        configuration = inventory_network_configuration(records)
    except (ValueError, TypeError):
        return SandboxResourceInspection(sandbox_id, "conflict", "network_configuration_conflict")
    roles: tuple[ResourceKind, ...] = (
        "container", "proxy", "network", "ingress", "workspace",
        *(("egress",) if configuration.allow_domains else ()),
    )
    discovered = {record.kind for record in records}
    found = tuple(role for role in roles if role in discovered)
    missing = tuple(role for role in roles if role not in discovered)
    if not missing:
        classification, reason = "all_present", "resources_present"
    elif found:
        classification, reason = "partial", "resources_partial"
    else:
        classification, reason = "not_found", "resources_not_found"
    return SandboxResourceInspection(
        sandbox_id, classification, reason, found, missing,
    )


def _close_query_client(
    client: DockerClient,
    *,
    operation: Literal["query", "endpoint"],
    read_completed: bool,
) -> None:
    """Preserve primary read errors and classify only standalone client-close failures."""
    try:
        client.close()
    except Exception as error:
        _LOGGER.warning(
            "sandbox_client_close_failed operation=%s error_type=%s",
            operation, type(error).__name__,
        )
        # A failed read owns the primary error; finally must not replace its outcome.
        if not read_completed:
            return
        if isinstance(error, (TimeoutError, requests_exceptions.Timeout)):
            subject = "endpoint" if operation == "endpoint" else "state"
            raise TimeoutError(f"sandbox {subject} query timed out") from error
        if operation == "endpoint":
            raise SandboxEndpointUnavailable() from error
        raise SandboxQueryFailed() from error


def get_endpoint_sync(
    client_factory: Callable[[], DockerClient], sandbox_id: str
) -> SandboxEndpoint:
    """Verify ownership, proxy health, topology and actual port before returning an entry."""
    client = None
    read_completed = False
    try:
        client = client_factory()
        sandbox = _owned_container(client, sandbox_id, "container")
        proxy = _owned_container(client, sandbox_id, "proxy")
        sandbox_labels = resource_labels(sandbox, "container")
        proxy_labels = resource_labels(proxy, "proxy")
        # The same sandbox ID may span attempts; never combine an old proxy with a new container.
        transaction_id = sandbox_labels["io.agent-sandbox.transaction-id"]
        if proxy_labels["io.agent-sandbox.transaction-id"] != transaction_id:
            raise SandboxEndpointUnavailable()
        # Entry availability requires proxy health; status separately reports Agent readiness.
        state = proxy.attrs["State"]
        if (
            state.get("Status") != "running"
            or state.get("Health", {}).get("Status") != "healthy"
        ):
            raise SandboxEndpointUnavailable()
        # Reconstruct expected names and labels without creating or adopting resources.
        spec = SandboxSpec(
            sandbox_id=sandbox_id,
            container_name=f"agent-sandbox-{sandbox_id}",
            network_name=f"agent-sandbox-{sandbox_id}-internal",
            volume_name=f"agent-sandbox-{sandbox_id}-workspace",
            transaction_id=transaction_id,
        )
        _, records = _query_network_inventory(client, sandbox_id, sandbox_labels, sandbox.id)
        _verify_entry_networks(client, sandbox, proxy, spec)
        version, digest = _endpoint_auth_facts(client, records, proxy)
        created_at, _ = _lifecycle_times(sandbox_labels)
        if version is not None and (created_at is None or _lifecycle_times(proxy_labels)[0] != created_at):
            raise SandboxEndpointUnavailable()
        result = SandboxEndpoint(
            sandbox_id, _published_loopback_url(proxy), auth_required=version is not None,
            transaction_id=transaction_id if version is not None else None,
            created_at=created_at if version is not None else None,
            credential_digest=digest, auth_version=version,
            services_version=_endpoint_services_version(client, records, proxy),
        )
        # Mark success so finally can distinguish read errors from close-only failures.
        read_completed = True
        return result
    except (TimeoutError, requests_exceptions.Timeout) as error:
        raise TimeoutError("sandbox endpoint query timed out") from error
    except (SandboxNotFound, SandboxEndpointUnavailable):
        raise
    except Exception as error:
        raise SandboxEndpointUnavailable() from error
    finally:
        if client is not None:
            _close_query_client(client, operation="endpoint", read_completed=read_completed)


def get_sandbox_sync(
    client_factory: Callable[[], DockerClient],
    sandbox_id: str,
) -> SandboxQueryResult:
    """Read Agent state, initial times and required egress health without probing public targets."""
    try:
        client = client_factory()
    except Exception as error:
        raise DockerUnavailable() from error

    read_completed = False
    try:
        try:
            # Include non-running containers to distinguish starting/stopped/failed from absence.
            containers = client.containers.list(
                all=True,
                filters={
                    "label": [
                        "io.agent-sandbox.managed=true",
                        "io.agent-sandbox.project=agent-sandbox",
                        f"io.agent-sandbox.sandbox-id={sandbox_id}",
                        "io.agent-sandbox.resource=container",
                    ]
                },
            )
            if not containers:
                raise SandboxNotFound()
            if len(containers) != 1:
                raise SandboxQueryFailed()

            # Re-inspect the unique candidate's exact ID and labels instead of guessing by name.
            container_id = containers[0].id
            container = client.containers.get(container_id)
            container.reload()
            if container.id != container_id:
                raise SandboxNotFound()

            labels = resource_labels(container, "container")
            if not query_labels_match(labels, sandbox_id):
                raise SandboxNotFound()

            state = container.attrs.get("State")
            if not isinstance(state, dict):
                raise SandboxQueryFailed()
            # Labels hold initial times; Service merges stored expiration after identity checks.
            created_at, expires_at = _lifecycle_times(labels)
            result = replace(
                map_container_state(sandbox_id, state),
                created_at=created_at,
                expires_at=expires_at,
                transaction_id=labels["io.agent-sandbox.transaction-id"],
                workspace_id=(binding.workspace_id if (binding := binding_from_labels(
                    labels, sandbox_id, labels["io.agent-sandbox.transaction-id"],
                )) is not None else None),
                workspace=binding,
            )
            configuration, records = _query_network_inventory(
                client, sandbox_id, labels, container_id,
            )
            if configuration.allow_domains:
                egress_status = _egress_observation(client, sandbox_id, records, configuration)
                if egress_status is not None and (
                    result.status in {"ready", "running", "starting", "paused"}
                    or egress_status[0] == "paused"
                    or egress_status[1] == "pause_incomplete"
                ):
                    result = replace(result, status=egress_status[0], reason=egress_status[1], message=egress_status[2])
            read_completed = True
            return result
        except SandboxError:
            raise
        except NotFound as error:
            raise SandboxNotFound() from error
        except (DockerException, requests_exceptions.RequestException) as error:
            raise DockerUnavailable() from error
        except Exception as error:
            raise SandboxQueryFailed() from error
    finally:
        _close_query_client(client, operation="query", read_completed=read_completed)


def _query_network_inventory(
    client: DockerClient, sandbox_id: str, main_labels: dict[str, str], main_id: str,
) -> tuple[SandboxNetwork, tuple[_ResourceRecord, ...]]:
    """Check all survivor markers against the original main snapshot without requiring ordinary completeness."""
    try:
        initial = network_configuration(main_labels)
        records = discover_sandbox_resources(
            client, sandbox_id, main_labels["io.agent-sandbox.transaction-id"],
        )
        main = next((record for record in records if record.kind == "container"), None)
        if main is None:
            raise SandboxNotFound()
        if main.resource_id != main_id or network_configuration(main.observed_labels or {}) != initial:
            raise SandboxQueryFailed()
        configuration = inventory_network_configuration(records)
        proxy_record = next((record for record in records if record.kind == "proxy"), None)
        _endpoint_auth_facts(client, records, client.containers.get(proxy_record.resource_id) if proxy_record else None)
        return configuration, records
    except (SandboxDeleteConflict, WorkspaceError) as error:
        raise SandboxQueryFailed() from error
    except (ValueError, TypeError, KeyError) as error:
        raise SandboxQueryFailed() from error


def map_container_state(
    sandbox_id: str,
    state: dict[str, Any],
) -> SandboxQueryResult:
    """Map Docker and health facts to public status independently of SQLite management state."""
    status = state.get("Status")
    if state.get("Paused") is True or status == "paused":
        return SandboxQueryResult(
            sandbox_id,
            "paused",
            "container_paused",
            "container is paused",
        )
    if status == "created":
        return SandboxQueryResult(
            sandbox_id,
            "starting",
            "container_created",
            "container has been created but is not running",
        )
    if status == "restarting":
        return SandboxQueryResult(
            sandbox_id,
            "starting",
            "container_restarting",
            "container is restarting",
        )
    if status == "running":
        return map_running_state(sandbox_id, state)
    # OOM is more specific than an exit code and must precede the ordinary exited branches.
    if status == "exited" and state.get("OOMKilled") is True:
        return SandboxQueryResult(
            sandbox_id,
            "failed",
            "container_oom_killed",
            "container was terminated after exceeding its memory limit",
        )
    if status == "exited" and state.get("ExitCode") == 0:
        return SandboxQueryResult(
            sandbox_id,
            "stopped",
            "container_exited",
            "container exited successfully",
        )
    if status == "exited":
        return SandboxQueryResult(
            sandbox_id,
            "failed",
            "container_exited_with_error",
            "container exited with a non-zero status",
        )
    if status == "dead":
        return SandboxQueryResult(
            sandbox_id,
            "failed",
            "container_dead",
            "Docker reports that the container is dead",
        )
    raise SandboxQueryFailed()


def map_running_state(
    sandbox_id: str,
    state: dict[str, Any],
) -> SandboxQueryResult:
    """Distinguish a running container from a ready Agent; only healthy maps to ready."""
    health = state.get("Health")
    if health is None:
        return SandboxQueryResult(
            sandbox_id,
            "running",
            "health_check_not_configured",
            "container is running but no health check is configured",
        )
    if not isinstance(health, dict):
        raise SandboxQueryFailed()

    health_status = health.get("Status")
    if health_status == "starting":
        return SandboxQueryResult(
            sandbox_id,
            "starting",
            "health_check_starting",
            "container is running and its health check is starting",
        )
    if health_status == "healthy":
        return SandboxQueryResult(
            sandbox_id,
            "ready",
            "health_check_passed",
            "Agent health check passed",
        )
    if health_status == "unhealthy":
        return SandboxQueryResult(
            sandbox_id,
            "failed",
            "health_check_failed",
            "Agent health check is failing",
        )
    raise SandboxQueryFailed()
