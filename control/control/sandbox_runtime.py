"""Docker runtime for fixed-configuration sandbox creation."""

import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import docker
from docker.errors import APIError, DockerException, ImageNotFound
from requests import exceptions as requests_exceptions

from control.sandbox_service import (
    DockerUnavailable,
    ResourceKind,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxError,
    SandboxImageUnavailable,
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

    def close(self) -> None:
        """Close client-owned transport resources."""
        ...


@dataclass(frozen=True)
class _ResourceRecord:
    """Track one resource created by the current transaction."""

    kind: ResourceKind
    resource_id: str
    labels: dict[str, str]
    resource: Any


class _CreationCancelled(Exception):
    """Signal that a worker observed cancellation between Docker steps."""


def _create_docker_client() -> DockerClient:
    """Create a Docker SDK client with a finite request timeout."""
    return docker.from_env(timeout=5.0)


class DockerSandboxRuntime:
    """Create one sandbox using a fixed Docker safety baseline."""

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
                records = await asyncio.shield(worker)
            except _CreationCancelled:
                raise cancellation
            except SandboxCleanupFailed:
                raise
            except SandboxError:
                raise cancellation

            await asyncio.shield(
                asyncio.create_task(
                    asyncio.to_thread(self._rollback_committed, spec, records)
                )
            )
            raise cancellation

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
                network = client.networks.create(
                    name=spec.network_name,
                    driver="bridge",
                    internal=True,
                    labels=network_labels,
                )
                records.append(
                    _ResourceRecord("network", network.id, network_labels, network)
                )
                self._raise_if_cancelled(cancelled)

                volume_labels = spec.labels_for("workspace")
                volume = client.volumes.create(
                    name=spec.volume_name,
                    labels=volume_labels,
                )
                records.append(
                    _ResourceRecord("workspace", volume.id, volume_labels, volume)
                )
                self._raise_if_cancelled(cancelled)

                container_labels = spec.labels_for("container")
                container = client.containers.create(
                    image=SANDBOX_IMAGE,
                    name=spec.container_name,
                    labels=container_labels,
                    init=True,
                    user="sandbox",
                    working_dir="/workspace",
                    nano_cpus=1_000_000_000,
                    mem_limit="256m",
                    memswap_limit="256m",
                    pids_limit=64,
                    cap_drop=["ALL"],
                    security_opt=["no-new-privileges:true"],
                    read_only=True,
                    tmpfs={"/tmp": "rw,nosuid,nodev,size=32m,mode=1777"},
                    volumes={
                        spec.volume_name: {
                            "bind": "/workspace",
                            "mode": "rw",
                        }
                    },
                    network=spec.network_name,
                )
                records.append(
                    _ResourceRecord(
                        "container",
                        container.id,
                        container_labels,
                        container,
                    )
                )
                self._raise_if_cancelled(cancelled)

                container.start()
                self._raise_if_cancelled(cancelled)
                return tuple(records)
            except Exception as error:
                if isinstance(error, _CreationCancelled) or cancelled.is_set():
                    classified: Exception = _CreationCancelled()
                else:
                    classified = self._classify_error(error)
                cleanup_failed = self._rollback(records)
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

    def _rollback(self, records: list[_ResourceRecord]) -> bool:
        """Remove all safely owned resources and report cleanup failures."""
        failed = False
        for record in reversed(records):
            try:
                record.resource.reload()
                if record.resource.id != record.resource_id:
                    failed = True
                    continue
                actual_labels = self._resource_labels(record)
                if not all(
                    actual_labels.get(key) == value
                    for key, value in record.labels.items()
                ):
                    failed = True
                    continue
                if record.kind == "container":
                    record.resource.remove(force=True)
                else:
                    record.resource.remove()
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

        loaded: list[_ResourceRecord] = []
        failed = False
        try:
            for record in records:
                try:
                    resource = self._resource_manager(client, record.kind).get(
                        record.resource_id
                    )
                    loaded.append(
                        _ResourceRecord(
                            record.kind,
                            record.resource_id,
                            record.labels,
                            resource,
                        )
                    )
                except Exception:
                    failed = True
            failed = self._rollback(loaded) or failed
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

    def _resource_labels(self, record: _ResourceRecord) -> dict[str, str]:
        """Read Docker labels from the shape used by each resource type."""
        if record.kind == "container":
            labels = record.resource.attrs.get("Config", {}).get("Labels", {})
        else:
            labels = record.resource.attrs.get("Labels", {})
        return labels if isinstance(labels, dict) else {}
