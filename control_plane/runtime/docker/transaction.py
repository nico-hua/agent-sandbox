"""Fixed-policy Docker creation, ownership checks and reverse rollback."""

import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import docker
from docker.errors import APIError, DockerException, ImageNotFound
from requests import exceptions as requests_exceptions

from control_plane.core.errors import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxError,
    SandboxImageUnavailable,
    SandboxResourceConflict,
)
from control_plane.core.models import ResourceKind, SandboxSpec


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


def create_runtime_client() -> DockerClient:
    """Create a Docker SDK client with a finite request timeout."""
    return docker.from_env(timeout=5.0)


def resource_manager(client: DockerClient, kind: ResourceKind) -> Any:
    """Select the exact-ID Docker manager for one recorded kind."""
    if kind == "container":
        return client.containers
    if kind == "workspace":
        return client.volumes
    return client.networks


def resource_labels(resource: Any, kind: ResourceKind) -> dict[str, str]:
    """Read Docker labels from the shape used by each resource type."""
    if kind == "container":
        labels = resource.attrs.get("Config", {}).get("Labels", {})
    else:
        labels = resource.attrs.get("Labels", {})
    return labels if isinstance(labels, dict) else {}


def labels_match(actual: Any, expected: dict[str, str]) -> bool:
    """Check complete expected ownership labels on a Docker response."""
    return isinstance(actual, dict) and all(
        actual.get(key) == value for key, value in expected.items()
    )


def query_labels_match(labels: dict[str, str], sandbox_id: str) -> bool:
    """Verify all ownership labels required for a readable container."""
    expected = {
        "io.agent-sandbox.managed": "true",
        "io.agent-sandbox.project": "agent-sandbox",
        "io.agent-sandbox.sandbox-id": sandbox_id,
        "io.agent-sandbox.resource": "container",
    }
    transaction_id = labels.get("io.agent-sandbox.transaction-id", "")
    return labels_match(labels, expected) and re.fullmatch(
        r"txn_[0-9a-f]{32}", transaction_id
    ) is not None


SANDBOX_IMAGE = "agent-sandbox:dev"


@dataclass(frozen=True)
class _ResourceRecord:
    """Track one resource created by the current transaction."""

    kind: ResourceKind
    resource_id: str
    labels: dict[str, str]


class CreationCancelled(Exception):
    """Signal that a worker observed cancellation between Docker steps."""


class DockerCreationTransaction:
    """Own synchronous creation and rollback for one injected client factory."""

    def __init__(
        self,
        client_factory: Callable[[], DockerClient],
    ) -> None:
        """Store a lazy Docker client factory without connecting."""
        self._client_factory = client_factory

    def create_sync(
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
                if not labels_match(actual_volume_labels, volume_labels):
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
                if isinstance(error, CreationCancelled) or cancelled.is_set():
                    classified: Exception = CreationCancelled()
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
                resource = resource_manager(client, record.kind).get(
                    record.resource_id
                )
                resource.reload()
                if resource.id != record.resource_id:
                    failed = True
                    continue
                actual_labels = resource_labels(resource, record.kind)
                if not labels_match(actual_labels, record.labels):
                    failed = True
                    continue
                if record.kind == "container":
                    resource.remove(force=True)
                else:
                    resource.remove()
            except Exception:
                failed = True
        return failed

    def rollback_committed(
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

    def _raise_if_cancelled(self, cancelled: threading.Event) -> None:
        """Stop the transaction between Docker operations when cancelled."""
        if cancelled.is_set():
            raise CreationCancelled()
