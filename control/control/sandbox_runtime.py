"""Docker runtime for fixed-configuration sandbox creation."""

import asyncio
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import docker

from control.sandbox_service import ResourceKind, SandboxSpec


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
        """Run the synchronous sandbox transaction outside the event loop."""
        cancelled = threading.Event()
        await asyncio.to_thread(self._create_sync, spec, cancelled)

    def _create_sync(
        self,
        spec: SandboxSpec,
        cancelled: threading.Event,
    ) -> tuple[_ResourceRecord, ...]:
        """Create and start fixed Docker resources in transaction order."""
        del cancelled
        client = self._client_factory()
        records: list[_ResourceRecord] = []
        try:
            client.images.get(SANDBOX_IMAGE)

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

            volume_labels = spec.labels_for("workspace")
            volume = client.volumes.create(
                name=spec.volume_name,
                labels=volume_labels,
            )
            records.append(
                _ResourceRecord("workspace", volume.id, volume_labels, volume)
            )

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

            container.start()
            return tuple(records)
        finally:
            client.close()
