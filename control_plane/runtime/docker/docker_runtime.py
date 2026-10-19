"""Async Docker operations, read-only state mapping and cancellation adaptation."""

import asyncio
import re
import threading
from collections.abc import Callable
from typing import Any, Protocol, TypeVar

import docker
from docker.errors import DockerException, NotFound
from requests import exceptions as requests_exceptions

from control_plane.core.errors import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxError,
    SandboxEndpointUnavailable,
    SandboxNotFound,
    SandboxQueryFailed,
)
from control_plane.core.models import (
    ResourceKind,
    SandboxEndpoint,
    SandboxQueryResult,
    SandboxSpec,
)
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.runtime.docker.transaction import (
    CreationCancelled,
    DockerClient,
    DockerCreationTransaction,
    create_runtime_client,
    delete_sandbox_sync,
    labels_match,
    query_labels_match,
    resource_labels,
)


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


TaskResult = TypeVar("TaskResult")


async def _await_through_cancellation(
    task: asyncio.Task[TaskResult],
) -> TaskResult:
    """Wait for cleanup work despite repeated cancellation requests."""
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.cancelled():
                raise


class DockerSandboxRuntime(SandboxRuntime):
    """Ping, create and inspect sandboxes with a fixed Docker safety baseline."""

    def __init__(
        self,
        client_factory: Callable[[], DockerClient] | None = None,
        *,
        ping_client_factory: Callable[[], ProbeDockerClient] | None = None,
    ) -> None:
        """Store a lazy Docker client factory without connecting."""
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
        """Run the transaction and synchronously clean up on cancellation."""
        cancelled = threading.Event()
        worker = asyncio.create_task(
            asyncio.to_thread(self._creation.create_sync, spec, cancelled)
        )
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancellation:
            cancelled.set()
            try:
                records = await _await_through_cancellation(worker)
            except CreationCancelled:
                raise cancellation
            except SandboxCleanupFailed:
                raise
            except SandboxError:
                raise cancellation

            cleanup = asyncio.create_task(
                asyncio.to_thread(self._creation.rollback_committed, spec, records)
            )
            await _await_through_cancellation(cleanup)
            raise cancellation

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read one owned sandbox state without mutating Docker resources."""
        return await asyncio.to_thread(get_sandbox_sync, self._client_factory, sandbox_id)

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Recover a verified entry from Docker without retaining creation state."""
        return await asyncio.to_thread(
            get_endpoint_sync, self._client_factory, sandbox_id
        )

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False
    ) -> None:
        """Finish Docker deletion and client closure before propagating cancellation."""
        worker = asyncio.create_task(
            asyncio.to_thread(
                delete_sandbox_sync,
                self._client_factory,
                sandbox_id,
                delete_workspace=delete_workspace,
            )
        )
        cancellation: asyncio.CancelledError | None = None
        while not worker.done():
            try:
                # Cancelling wait() leaves its input task running. Unlike repeatedly
                # shielding an exceptional task, this also avoids Python 3.14's
                # shield exception log after a cancelled outer future.
                await asyncio.wait({worker})
            except asyncio.CancelledError as error:
                if cancellation is None:
                    cancellation = error
        if cancellation is not None:
            try:
                worker.result()
            except Exception:
                # Consume worker failure before returning cancellation. A later
                # request rediscovers leftovers from Docker labels.
                pass
            raise cancellation
        worker.result()


def _owned_container(
    client: DockerClient, sandbox_id: str, role: ResourceKind
) -> Any:
    """Resolve and re-inspect one unique container with complete role ownership."""
    candidates = client.containers.list(
        all=True,
        filters={"label": [
            "io.agent-sandbox.managed=true",
            "io.agent-sandbox.project=agent-sandbox",
            f"io.agent-sandbox.sandbox-id={sandbox_id}",
            f"io.agent-sandbox.resource={role}",
        ]},
    )
    if not candidates and role == "container":
        raise SandboxNotFound()
    if len(candidates) != 1:
        raise SandboxEndpointUnavailable()
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
    """Verify dedicated bridges, exact connections, DNS alias and network ownership."""
    sandbox_settings = sandbox.attrs["NetworkSettings"]
    sandbox_networks = sandbox_settings["Networks"]
    proxy_networks = proxy.attrs["NetworkSettings"]["Networks"]
    if set(sandbox_networks) != {spec.network_name} or set(proxy_networks) != {
        spec.network_name,
        spec.ingress_network_name,
    }:
        raise SandboxEndpointUnavailable()
    internal_connection = sandbox_networks[spec.network_name]
    if "sandbox" not in (internal_connection.get("Aliases") or []):
        raise SandboxEndpointUnavailable()
    if any(sandbox_settings.get("Ports", {}).values()):
        raise SandboxEndpointUnavailable()
    if internal_connection["NetworkID"] != proxy_networks[spec.network_name]["NetworkID"]:
        raise SandboxEndpointUnavailable()
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


def get_endpoint_sync(
    client_factory: Callable[[], DockerClient], sandbox_id: str
) -> SandboxEndpoint:
    """Read ownership, health, topology and actual port facts with transport cleanup."""
    client = None
    try:
        client = client_factory()
        sandbox = _owned_container(client, sandbox_id, "container")
        proxy = _owned_container(client, sandbox_id, "proxy")
        sandbox_labels = resource_labels(sandbox, "container")
        proxy_labels = resource_labels(proxy, "proxy")
        transaction_id = sandbox_labels["io.agent-sandbox.transaction-id"]
        if proxy_labels["io.agent-sandbox.transaction-id"] != transaction_id:
            raise SandboxEndpointUnavailable()
        state = proxy.attrs["State"]
        if (
            state.get("Status") != "running"
            or state.get("Health", {}).get("Status") != "healthy"
        ):
            raise SandboxEndpointUnavailable()
        spec = SandboxSpec(
            sandbox_id=sandbox_id,
            container_name=f"agent-sandbox-{sandbox_id}",
            network_name=f"agent-sandbox-{sandbox_id}-internal",
            volume_name=f"agent-sandbox-{sandbox_id}-workspace",
            transaction_id=transaction_id,
        )
        _verify_entry_networks(client, sandbox, proxy, spec)
        return SandboxEndpoint(sandbox_id, _published_loopback_url(proxy))
    except (TimeoutError, requests_exceptions.Timeout) as error:
        raise TimeoutError("sandbox endpoint query timed out") from error
    except (SandboxNotFound, SandboxEndpointUnavailable):
        raise
    except Exception as error:
        raise SandboxEndpointUnavailable() from error
    finally:
        if client is not None:
            try:
                client.close()
            except (TimeoutError, requests_exceptions.Timeout) as error:
                raise TimeoutError("sandbox endpoint query timed out") from error
            except Exception as error:
                raise SandboxEndpointUnavailable() from error


def get_sandbox_sync(
    client_factory: Callable[[], DockerClient],
    sandbox_id: str,
) -> SandboxQueryResult:
    """Inspect an exactly labelled managed container and map its state."""
    try:
        client = client_factory()
    except Exception as error:
        raise DockerUnavailable() from error

    try:
        try:
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
            return map_container_state(sandbox_id, state)
        except SandboxError:
            raise
        except NotFound as error:
            raise SandboxNotFound() from error
        except (DockerException, requests_exceptions.RequestException) as error:
            raise DockerUnavailable() from error
        except Exception as error:
            raise SandboxQueryFailed() from error
    finally:
        client.close()


def map_container_state(
    sandbox_id: str,
    state: dict[str, Any],
) -> SandboxQueryResult:
    """Map Docker container and health facts to the public state model."""
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
    """Map a running container's optional Docker health information."""
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
