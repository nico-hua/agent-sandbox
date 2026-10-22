"""Adapt async requests to Docker transactions, read-only inspection and cancellation cleanup."""

import asyncio
import logging
import re
import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from typing import Any, Literal, Protocol

import docker
from docker.errors import DockerException, NotFound
from requests import exceptions as requests_exceptions

from control_plane.core.errors import (
    CreationFailureInfo,
    CreationResourceState,
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxDeleteConflict,
    SandboxEndpointUnavailable,
    SandboxError,
    SandboxNotFound,
    SandboxQueryFailed,
)
from control_plane.core.models import (
    ResourceKind,
    SandboxEndpoint,
    SandboxNetwork,
    SandboxQueryResult,
    SandboxResourceInspection,
    SandboxSpec,
)
from control_plane.core.tasks import settle_task, wait_for_completion, wait_for_task
from control_plane.runtime.docker.transaction import (
    CreationCancelled,
    DockerClient,
    DockerCreationTransaction,
    _ResourceRecord,
    create_runtime_client,
    delete_sandbox_sync,
    discover_sandbox_resources,
    inventory_network_configuration,
    labels_match,
    network_configuration,
    query_labels_match,
    resource_manager,
    resource_labels,
)
from control_plane.runtime.sandbox_runtime import SandboxRuntime

_LOGGER = logging.getLogger(__name__)


class ProbeDockerClient(Protocol):
    """Describe the read-only Docker client methods used by the probe."""

    def ping(self) -> object:
        """Ping the Docker daemon."""
        ...

    def close(self) -> None:
        """Close client-owned resources."""
        ...


def create_probe_client() -> ProbeDockerClient:
    """Create a Docker SDK client with a short transport timeout."""
    return docker.from_env(timeout=0.5)


class DockerSandboxRuntime(SandboxRuntime):
    """Ping, create and inspect sandboxes with a fixed Docker safety baseline."""

    def __init__(
        self,
        client_factory: Callable[[], DockerClient] | None = None,
        *,
        ping_client_factory: Callable[[], ProbeDockerClient] | None = None,
    ) -> None:
        """Store separate lazy client factories without connecting during assembly."""
        # Ping has a shorter transport deadline than lifecycle calls, so factories stay separate.
        self._client_factory = (
            client_factory if client_factory is not None else create_runtime_client
        )
        self._ping_client_factory = (
            ping_client_factory
            if ping_client_factory is not None
            else create_probe_client
        )
        self._creation = DockerCreationTransaction(self._client_factory)

    async def ping(self) -> None:
        """Run a read-only Docker ping outside the ASGI event loop."""
        await asyncio.to_thread(self._ping_and_close)

    def _ping_and_close(self) -> None:
        """Map backend failures to a domain error and close any created client."""
        try:
            client = self._ping_client_factory()
            try:
                client.ping()
            finally:
                client.close()
        except Exception as error:
            raise DockerUnavailable() from error

    async def create(self, spec: SandboxSpec) -> None:
        """Create resources in a worker; propagate cancellation only after confirmed rollback."""
        # Async cancellation cannot stop an SDK call already running in the thread.
        # The transaction checks this event between resource operations instead.
        cancelled = threading.Event()
        worker = asyncio.create_task(
            asyncio.to_thread(self._creation.create_sync, spec, cancelled)
        )
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            cancelled.set()
            try:
                # Settle first to distinguish rollback, cleanup failure and late success.
                records = await settle_task(worker)
            except CreationCancelled:
                # This internal signal confirms rollback of the recorded resources.
                raise cancellation
            except SandboxCleanupFailed:
                raise
            except DockerUnavailable as error:
                # A lost create response may be classified before cancellation is observed.
                # Preserve that uncertainty rather than claiming rollback completed.
                cleanup_error = SandboxCleanupFailed(spec.sandbox_id)
                cleanup_error.creation_failure = error.creation_failure or CreationFailureInfo(
                    "runtime_create", CreationResourceState.UNKNOWN,
                )
                raise cleanup_error from error
            except SandboxError:
                raise cancellation

            # Creation won the race; reopen a client to compensate the exact returned IDs.
            cleanup = asyncio.create_task(
                asyncio.to_thread(self._creation.rollback_committed, spec, records)
            )
            await settle_task(cleanup)
            raise cancellation

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read managed container and Agent health facts without changing resources or metadata."""
        return await asyncio.to_thread(get_sandbox_sync, self._client_factory, sandbox_id)

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Recover a verified entry from current Docker facts, independent of creation memory."""
        return await asyncio.to_thread(
            get_endpoint_sync, self._client_factory, sandbox_id
        )

    async def inspect_resources(
        self, sandbox_id: str, *, expected_transaction_id: str,
    ) -> SandboxResourceInspection:
        """Inspect every role read-only and settle transport cleanup before returning cancellation."""
        worker = asyncio.create_task(asyncio.to_thread(
            inspect_resources_sync, self._client_factory, sandbox_id,
            expected_transaction_id=expected_transaction_id,
        ))
        return await settle_task(worker, propagate_cancellation=True)

    async def delete(
        self,
        sandbox_id: str,
        *,
        delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Finish deletion and client closure; cancellation does not restore deleted resources."""
        # Discover partial inventories even if the main container or get() is unavailable.
        worker = asyncio.create_task(
            asyncio.to_thread(
                delete_sandbox_sync,
                self._client_factory,
                sandbox_id,
                delete_workspace=delete_workspace,
                expected_transaction_id=expected_transaction_id,
            )
        )
        # Service separately settles metadata; this layer preserves deletion's cancellation priority.
        cancellation = await wait_for_completion(worker)
        if cancellation is not None:
            try:
                worker.result()
            except Exception:
                # Retrieve worker failure before returning cancellation; retries discover leftovers.
                pass
            raise cancellation
        worker.result()


def inspect_resources_sync(
    client_factory: Callable[[], DockerClient], sandbox_id: str,
    *, expected_transaction_id: str,
) -> SandboxResourceInspection:
    """Classify a verified inventory without changing resources or checking creation success."""
    client = None
    result = SandboxResourceInspection(sandbox_id, "unavailable", "docker_unavailable")
    try:
        client = client_factory()
        # Discovery checks every role independently and never filters by transaction.
        records = discover_sandbox_resources(client, sandbox_id, expected_transaction_id)
        result = _classify_resource_inventory(sandbox_id, records)
    except SandboxDeleteConflict:
        # An interrupted scan cannot reliably identify every missing role.
        result = SandboxResourceInspection(
            sandbox_id, "conflict", "resource_ownership_conflict",
        )
    except Exception:
        # Preserve uncertainty rather than treating a failed list/inspect as absence.
        result = SandboxResourceInspection(sandbox_id, "unavailable", "docker_unavailable")
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:
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


def _owned_container(
    client: DockerClient, sandbox_id: str, role: ResourceKind
) -> Any:
    """Resolve one container per role, then re-inspect its ID and complete ownership."""
    candidates = client.containers.list(
        all=True,
        filters={"label": [
            "io.agent-sandbox.managed=true",
            "io.agent-sandbox.project=agent-sandbox",
            f"io.agent-sandbox.sandbox-id={sandbox_id}",
            f"io.agent-sandbox.resource={role}",
        ]},
    )
    # Missing main means not found; a missing or ambiguous proxy means entry unavailable.
    if not candidates and role == "container":
        raise SandboxNotFound()
    if len(candidates) != 1:
        raise SandboxEndpointUnavailable()
    # Re-inspect the listing snapshot to detect disappearance or ownership changes.
    resource_id = candidates[0].id
    try:
        container = client.containers.get(resource_id)
        container.reload()
    except NotFound as error:
        if role == "container":
            raise SandboxNotFound() from error
        raise SandboxEndpointUnavailable() from error
    if container.id != resource_id or not query_labels_match(
        resource_labels(container, role), sandbox_id, role
    ):
        if role == "container":
            raise SandboxNotFound()
        raise SandboxEndpointUnavailable()
    return container


def _verify_entry_networks(
    client: DockerClient, sandbox: Any, proxy: Any, spec: SandboxSpec
) -> None:
    """Verify fixed network attachments, DNS alias and bridge ownership."""
    sandbox_settings = sandbox.attrs["NetworkSettings"]
    sandbox_networks = sandbox_settings["Networks"]
    proxy_networks = proxy.attrs["NetworkSettings"]["Networks"]
    # Sandbox uses internal; proxy uses ingress plus internal, with no extra attachments.
    if set(sandbox_networks) != {spec.network_name} or set(proxy_networks) != {
        spec.network_name,
        spec.ingress_network_name,
    }:
        raise SandboxEndpointUnavailable()
    internal_connection = sandbox_networks[spec.network_name]
    if "sandbox" not in (internal_connection.get("Aliases") or []):
        raise SandboxEndpointUnavailable()
    # Publish only through the proxy; matching names do not prove a shared internal bridge.
    if any(sandbox_settings.get("Ports", {}).values()):
        raise SandboxEndpointUnavailable()
    if internal_connection["NetworkID"] != proxy_networks[spec.network_name]["NetworkID"]:
        raise SandboxEndpointUnavailable()
    # Re-inspect connected NetworkIDs instead of trusting names in container snapshots.
    for name, role, internal in [
        (spec.network_name, "network", True),
        (spec.ingress_network_name, "ingress", False),
    ]:
        network_id = proxy_networks[name]["NetworkID"]
        if not isinstance(network_id, str) or not network_id:
            raise SandboxEndpointUnavailable()
        network = client.networks.get(network_id)
        network.reload()
        attrs = network.attrs
        if (
            network.id != network_id
            or attrs.get("Name") != name
            or attrs.get("Driver") != "bridge"
            or attrs.get("Internal") is not internal
            or not labels_match(resource_labels(network, role), spec.labels_for(role))
        ):
            raise SandboxEndpointUnavailable()


def _published_loopback_url(proxy: Any) -> str:
    """Accept only a unique effective IPv4 loopback mapping for Agent HTTP."""
    # HostConfig records intent; NetworkSettings contains the actual allocated port.
    mappings = proxy.attrs["NetworkSettings"]["Ports"].get("8080/tcp")
    if not isinstance(mappings, list) or len(mappings) != 1:
        raise SandboxEndpointUnavailable()
    binding = mappings[0]
    port = binding.get("HostPort")
    if (
        binding.get("HostIp") != "127.0.0.1"
        or not isinstance(port, str)
        or re.fullmatch(r"[0-9]{1,5}", port) is None
        or not 1 <= int(port) <= 65535
    ):
        raise SandboxEndpointUnavailable()
    return f"http://127.0.0.1:{int(port)}"


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
        _query_network_inventory(client, sandbox_id, sandbox_labels, sandbox.id)
        _verify_entry_networks(client, sandbox, proxy, spec)
        result = SandboxEndpoint(sandbox_id, _published_loopback_url(proxy))
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
            )
            configuration, records = _query_network_inventory(
                client, sandbox_id, labels, container_id,
            )
            if configuration.allow_domains:
                egress_status = _egress_observation(client, sandbox_id, records, configuration)
                if egress_status is not None and result.status in {"ready", "running", "starting"}:
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
        return configuration, records
    except SandboxDeleteConflict as error:
        raise SandboxQueryFailed() from error
    except (ValueError, TypeError, KeyError) as error:
        raise SandboxQueryFailed() from error


def _egress_observation(
    client: DockerClient, sandbox_id: str, records: tuple[_ResourceRecord, ...], configuration: SandboxNetwork,
) -> tuple[Literal["failed", "starting"], str, str] | None:
    """Require owned dual-network egress and local health, without probing DNS or public targets."""
    record = next((item for item in records if item.kind == "egress"), None)
    if record is None:
        return "failed", "egress_missing", "required egress proxy is missing"
    try:
        egress = client.containers.get(record.resource_id)
        egress.reload()
    except NotFound:
        return "failed", "egress_missing", "required egress proxy is missing"
    labels = resource_labels(egress, "egress")
    if egress.id != record.resource_id or not labels_match(labels, record.labels) or network_configuration(labels) != configuration:
        raise SandboxQueryFailed()
    attrs = egress.attrs
    attached = attrs["NetworkSettings"]["Networks"]
    internal_name = f"agent-sandbox-{sandbox_id}-internal"
    ingress_name = f"agent-sandbox-{sandbox_id}-ingress"
    if set(attached) != {internal_name, ingress_name} or "egress" not in (attached[internal_name].get("Aliases") or []):
        raise SandboxQueryFailed()
    if any(attrs["NetworkSettings"].get("Ports", {}).values()) or attrs.get("HostConfig", {}).get("PortBindings") or attrs.get("Mounts"):
        raise SandboxQueryFailed()
    for name, role, internal in ((internal_name, "network", True), (ingress_name, "ingress", False)):
        target = next((item for item in records if item.kind == role), None)
        network_id = attached[name].get("NetworkID")
        if target is None or target.resource_id != network_id:
            raise SandboxQueryFailed()
        try:
            network = resource_manager(client, role).get(network_id)
            network.reload()
        except NotFound as error:
            raise SandboxQueryFailed() from error
        if (network.id != network_id or not labels_match(resource_labels(network, role), target.labels)
            or network_configuration(resource_labels(network, role)) != configuration
            or network.attrs.get("Name") != name or network.attrs.get("Driver") != "bridge"
            or network.attrs.get("Internal") is not internal):
            raise SandboxQueryFailed()
    main = next(item for item in records if item.kind == "container")
    sandbox = client.containers.get(main.resource_id)
    sandbox.reload()
    if sandbox.id != main.resource_id or not labels_match(resource_labels(sandbox, "container"), main.labels):
        raise SandboxQueryFailed()
    sandbox_networks = sandbox.attrs["NetworkSettings"]["Networks"]
    if set(sandbox_networks) != {internal_name} or sandbox_networks[internal_name].get("NetworkID") != attached[internal_name].get("NetworkID") or any(sandbox.attrs["NetworkSettings"].get("Ports", {}).values()):
        raise SandboxQueryFailed()
    state = attrs.get("State", {})
    if state.get("Status") != "running":
        return "failed", "egress_stopped", "required egress proxy is not running"
    health = state.get("Health", {}).get("Status")
    if health == "healthy":
        return None
    if health == "starting":
        return "starting", "egress_starting", "egress proxy health check is starting"
    return "failed", "egress_unhealthy", "required egress proxy health check is not passing"


def _lifecycle_times(
    labels: dict[str, str],
) -> tuple[datetime | None, datetime | None]:
    """Parse initial UTC labels; accept legacy absence without repairing partial metadata."""
    keys = ("io.agent-sandbox.created-at", "io.agent-sandbox.expires-at")
    if all(key not in labels for key in keys):
        return None, None
    values: list[datetime] = []
    for key in keys:
        value = labels.get(key)
        # Require UTC RFC 3339 before parsing: fromisoformat accepts broader formats too.
        if not isinstance(value, str) or re.fullmatch(
            r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
            r"(?:\.[0-9]{1,6})?(?:Z|\+00:00)", value,
        ) is None:
            raise SandboxQueryFailed()
        try:
            values.append(datetime.fromisoformat(value))
        except ValueError as error:
            raise SandboxQueryFailed() from error
    created_at, expires_at = values
    if expires_at < created_at:
        raise SandboxQueryFailed()
    return created_at, expires_at


def map_container_state(
    sandbox_id: str,
    state: dict[str, Any],
) -> SandboxQueryResult:
    """Map Docker and health facts to public status independently of SQLite management state."""
    status = state.get("Status")
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
    if status == "paused":
        return SandboxQueryResult(
            sandbox_id,
            "stopped",
            "container_paused",
            "container is paused",
        )
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
