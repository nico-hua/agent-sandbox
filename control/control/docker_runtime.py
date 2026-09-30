"""Docker runtime readiness abstractions."""

import asyncio
from collections.abc import Callable
from typing import Protocol

import docker


class DockerClient(Protocol):
    """Describe the read-only Docker client methods used by the probe."""

    def ping(self) -> object:
        """Ping the Docker daemon."""
        ...

    def close(self) -> None:
        """Close client-owned resources."""
        ...


class RuntimeProbe(Protocol):
    """Describe the asynchronous Docker readiness operation."""

    async def ping(self) -> None:
        """Return after a successful Docker ping or raise on failure."""
        ...


def _create_docker_client() -> DockerClient:
    """Create a Docker SDK client with a short transport timeout."""
    return docker.from_env(timeout=0.5)


class DockerRuntimeProbe:
    """Adapt the synchronous Docker SDK ping into an async readiness probe."""

    def __init__(
        self,
        client_factory: Callable[[], DockerClient] | None = None,
    ) -> None:
        """Store a lazy Docker client factory without connecting to Docker."""
        self._client_factory = client_factory or _create_docker_client

    async def ping(self) -> None:
        """Run one Docker ping outside the ASGI event loop."""
        await asyncio.to_thread(self._ping_and_close)

    def _ping_and_close(self) -> None:
        """Create, ping, and close one Docker client synchronously."""
        client = self._client_factory()
        try:
            client.ping()
        finally:
            client.close()
