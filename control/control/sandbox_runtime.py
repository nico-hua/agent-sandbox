"""Docker runtime for fixed sandbox creation and state inspection."""

import asyncio
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

import docker
from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from requests import exceptions as requests_exceptions

from control.sandbox_service import (
    DockerUnavailable,
    ResourceKind,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxError,
    SandboxImageUnavailable,
    SandboxNotFound,
    SandboxQueryFailed,
    SandboxQueryResult,
    SandboxResourceConflict,
    SandboxSpec,
)


SANDBOX_IMAGE = "agent-sandbox:dev"


class DockerClient(Protocol):
    """Describe the Docker client surface used by sandbox creation."""

    images: Any
    networks: Any
    volumes: Any
    containers: Any
    api: Any

    def close(self) -> None:
        """Close client-owned transport resources."""
        ...


@dataclass(frozen=True)
class _ResourceRecord:
    """Track one resource created by the current transaction."""

    kind: ResourceKind
    resource_id: str
    labels: dict[str, str]


class _CreationCancelled(Exception):
    """Signal that a worker observed cancellation between Docker steps."""


def _create_docker_client() -> DockerClient:
    """Create a Docker SDK client with a finite request timeout."""
    return docker.from_env(timeout=5.0)


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


class DockerSandboxRuntime:
    """Create and inspect sandboxes using a fixed Docker safety baseline."""

    def __init__(
        self,
        client_factory: Callable[[], DockerClient] | None = None,
    ) -> None:
        """Store a lazy Docker client factory without connecting."""
        self._client_factory = (
            client_factory if client_factory is not None else _create_docker_client
        )

    async def create(self, spec: SandboxSpec) -> None:
        """Run the transaction and synchronously clean up on cancellation."""
        cancelled = threading.Event()
        worker = asyncio.create_task(
            asyncio.to_thread(self._create_sync, spec, cancelled)
        )
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancellation:
            cancelled.set()
            try:
                records = await _await_through_cancellation(worker)
            except _CreationCancelled:
                raise cancellation
            except SandboxCleanupFailed:
                raise
            except SandboxError:
                raise cancellation

            cleanup = asyncio.create_task(
                asyncio.to_thread(self._rollback_committed, spec, records)
            )
            await _await_through_cancellation(cleanup)
            raise cancellation

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read one owned sandbox state without mutating Docker resources."""
        return await asyncio.to_thread(self._get_sync, sandbox_id)

    def _get_sync(self, sandbox_id: str) -> SandboxQueryResult:
        """Inspect an exactly labelled managed container and map its state."""
        try:
            client = self._client_factory()
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

                labels = self._resource_labels(container, "container")
                if not self._query_labels_match(labels, sandbox_id):
                    raise SandboxNotFound()

                state = container.attrs.get("State")
                if not isinstance(state, dict):
                    raise SandboxQueryFailed()
                return self._map_container_state(sandbox_id, state)
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

    def _create_sync(
        self,
        spec: SandboxSpec,
        cancelled: threading.Event,
    ) -> tuple[_ResourceRecord, ...]:
        """Create and start fixed Docker resources in transaction order."""
        try:
            client = self._client_factory()
        except Exception as error:
            raise DockerUnavailable() from error

        records: list[_ResourceRecord] = []
        try:
            try:
                client.images.get(SANDBOX_IMAGE)
                self._raise_if_cancelled(cancelled)

                network_labels = spec.labels_for("network")
                network_response = client.api.create_network(
                    spec.network_name,
                    driver="bridge",
                    internal=True,
                    labels=network_labels,
                    check_duplicate=True,
                )
                network_id = network_response.get("Id")
                if not isinstance(network_id, str) or not network_id:
                    raise SandboxCleanupFailed(spec.sandbox_id)
                records.append(
                    _ResourceRecord("network", network_id, network_labels)
                )
                self._raise_if_cancelled(cancelled)

                volume_labels = spec.labels_for("workspace")
                volume_response = client.api.create_volume(
                    spec.volume_name,
                    labels=volume_labels,
                )
                volume_id = volume_response.get("Name")
                actual_volume_labels = volume_response.get("Labels")
                if not isinstance(volume_id, str) or not volume_id:
                    raise SandboxCleanupFailed(spec.sandbox_id)
                if not self._labels_match(actual_volume_labels, volume_labels):
                    raise SandboxResourceConflict()
                records.append(
                    _ResourceRecord("workspace", volume_id, volume_labels)
                )
                self._raise_if_cancelled(cancelled)

                container_labels = spec.labels_for("container")
                host_config = client.api.create_host_config(
                    init=True,
                    nano_cpus=1_000_000_000,
                    mem_limit="256m",
                    memswap_limit="256m",
                    pids_limit=64,
                    cap_drop=["ALL"],
                    security_opt=["no-new-privileges:true"],
                    read_only=True,
                    tmpfs={"/tmp": "rw,nosuid,nodev,size=32m,mode=1777"},
                    binds={
                        spec.volume_name: {
                            "bind": "/workspace",
                            "mode": "rw",
                        }
                    },
                    network_mode=spec.network_name,
                )
                container_response = client.api.create_container(
                    image=SANDBOX_IMAGE,
                    name=spec.container_name,
                    labels=container_labels,
                    user="sandbox",
                    working_dir="/workspace",
                    volumes=["/workspace"],
                    host_config=host_config,
                    networking_config={spec.network_name: None},
                )
                container_id = container_response.get("Id")
                if not isinstance(container_id, str) or not container_id:
                    raise SandboxCleanupFailed(spec.sandbox_id)
                records.append(
                    _ResourceRecord(
                        "container",
                        container_id,
                        container_labels,
                    )
                )
                self._raise_if_cancelled(cancelled)

                container = client.containers.get(container_id)
                container.start()
                self._raise_if_cancelled(cancelled)
                return tuple(records)
            except Exception as error:
                if isinstance(error, _CreationCancelled) or cancelled.is_set():
                    classified: Exception = _CreationCancelled()
                else:
                    classified = self._classify_error(error)
                cleanup_failed = self._rollback(client, records)
                if cleanup_failed:
                    try:
                        raise classified from error
                    except Exception as mapped:
                        raise SandboxCleanupFailed(spec.sandbox_id) from mapped
                raise classified from error
        finally:
            client.close()

    def _classify_error(self, error: Exception) -> SandboxError:
        """Map Docker failures to stable sandbox domain errors."""
        if isinstance(error, SandboxError):
            return error
        if isinstance(error, ImageNotFound):
            return SandboxImageUnavailable()
        if isinstance(error, APIError):
            if error.status_code == 409:
                return SandboxResourceConflict()
            return SandboxCreateFailed()
        if isinstance(error, (DockerException, requests_exceptions.RequestException)):
            return DockerUnavailable()
        return SandboxCreateFailed()

    def _rollback(
        self,
        client: DockerClient,
        records: list[_ResourceRecord],
    ) -> bool:
        """Remove all safely owned resources and report cleanup failures."""
        failed = False
        for record in reversed(records):
            try:
                resource = self._resource_manager(client, record.kind).get(
                    record.resource_id
                )
                resource.reload()
                if resource.id != record.resource_id:
                    failed = True
                    continue
                actual_labels = self._resource_labels(resource, record.kind)
                if not self._labels_match(actual_labels, record.labels):
                    failed = True
                    continue
                if record.kind == "container":
                    resource.remove(force=True)
                else:
                    resource.remove()
            except Exception:
                failed = True
        return failed

    def _rollback_committed(
        self,
        spec: SandboxSpec,
        records: tuple[_ResourceRecord, ...],
    ) -> None:
        """Reopen committed resources by exact ID and remove them safely."""
        try:
            client = self._client_factory()
        except Exception as error:
            raise SandboxCleanupFailed(spec.sandbox_id) from error

        try:
            failed = self._rollback(client, list(records))
        finally:
            client.close()

        if failed:
            raise SandboxCleanupFailed(spec.sandbox_id)

    def _resource_manager(self, client: DockerClient, kind: ResourceKind) -> Any:
        """Select the exact-ID Docker manager for one recorded kind."""
        if kind == "container":
            return client.containers
        if kind == "workspace":
            return client.volumes
        return client.networks

    def _raise_if_cancelled(self, cancelled: threading.Event) -> None:
        """Stop the transaction between Docker operations when cancelled."""
        if cancelled.is_set():
            raise _CreationCancelled()

    def _resource_labels(self, resource: Any, kind: ResourceKind) -> dict[str, str]:
        """Read Docker labels from the shape used by each resource type."""
        if kind == "container":
            labels = resource.attrs.get("Config", {}).get("Labels", {})
        else:
            labels = resource.attrs.get("Labels", {})
        return labels if isinstance(labels, dict) else {}

    def _labels_match(self, actual: Any, expected: dict[str, str]) -> bool:
        """Check complete expected ownership labels on a Docker response."""
        return isinstance(actual, dict) and all(
            actual.get(key) == value for key, value in expected.items()
        )

    def _query_labels_match(self, labels: dict[str, str], sandbox_id: str) -> bool:
        """Verify all ownership labels required for a readable container."""
        expected = {
            "io.agent-sandbox.managed": "true",
            "io.agent-sandbox.project": "agent-sandbox",
            "io.agent-sandbox.sandbox-id": sandbox_id,
            "io.agent-sandbox.resource": "container",
        }
        transaction_id = labels.get("io.agent-sandbox.transaction-id", "")
        return self._labels_match(labels, expected) and re.fullmatch(
            r"txn_[0-9a-f]{32}", transaction_id
        ) is not None

    def _map_container_state(
        self,
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
            return self._map_running_state(sandbox_id, state)
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

    def _map_running_state(
        self,
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
