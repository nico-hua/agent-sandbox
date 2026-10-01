"""Unit tests for Docker sandbox resource creation and cleanup."""

from collections.abc import Callable
from typing import Any

import pytest

import control.sandbox_runtime as sandbox_runtime
from control.sandbox_runtime import DockerSandboxRuntime
from control.sandbox_service import SandboxSpec


SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"


def make_spec() -> SandboxSpec:
    """Build one deterministic sandbox specification for runtime tests."""
    return SandboxSpec(
        sandbox_id=SANDBOX_ID,
        container_name=f"agent-sandbox-{SANDBOX_ID}",
        network_name=f"agent-sandbox-{SANDBOX_ID}-internal",
        volume_name=f"agent-sandbox-{SANDBOX_ID}-workspace",
    )


class RecordingResource:
    """Represent one Docker resource and record its start operation."""

    def __init__(
        self,
        resource_id: str,
        labels: dict[str, str],
        events: list[str],
    ) -> None:
        """Initialize a resource with Docker-like ID and attributes."""
        self.id = resource_id
        self.attrs = {"Labels": labels, "Config": {"Labels": labels}}
        self._events = events

    def start(self) -> None:
        """Record that the container start operation was requested."""
        self._events.append("container.start")


class RecordingImages:
    """Record fixed-image lookup operations."""

    def __init__(self, events: list[str]) -> None:
        """Store the shared operation log."""
        self._events = events

    def get(self, image: str) -> object:
        """Record one image lookup and return an opaque image."""
        self._events.append(f"images.get:{image}")
        return object()


class RecordingManager:
    """Record one resource manager's create calls."""

    def __init__(
        self,
        kind: str,
        events: list[str],
        resource_factory: Callable[[dict[str, Any]], RecordingResource],
    ) -> None:
        """Store resource kind, shared events, and a fake object factory."""
        self.kind = kind
        self._events = events
        self._resource_factory = resource_factory
        self.create_calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> RecordingResource:
        """Record keyword arguments and return one Docker-like resource."""
        self._events.append(f"{self.kind}.create")
        self.create_calls.append(kwargs)
        return self._resource_factory(kwargs)


class RecordingClient:
    """Provide Docker-like managers while recording all operations."""

    def __init__(self) -> None:
        """Initialize managers, resources, and shared operation history."""
        self.events: list[str] = []
        self.close_calls = 0
        self.network = RecordingResource(
            "network-id",
            make_spec().labels_for("network"),
            self.events,
        )
        self.volume = RecordingResource(
            "volume-id",
            make_spec().labels_for("workspace"),
            self.events,
        )
        self.container = RecordingResource(
            "container-id",
            make_spec().labels_for("container"),
            self.events,
        )
        self.images = RecordingImages(self.events)
        self.networks = RecordingManager(
            "networks",
            self.events,
            lambda _kwargs: self.network,
        )
        self.volumes = RecordingManager(
            "volumes",
            self.events,
            lambda _kwargs: self.volume,
        )
        self.containers = RecordingManager(
            "containers",
            self.events,
            lambda _kwargs: self.container,
        )

    def close(self) -> None:
        """Record one Docker client close."""
        self.close_calls += 1
        self.events.append("client.close")


def test_runtime_construction_does_not_create_client() -> None:
    """Constructing the runtime must not connect to Docker."""
    calls = 0

    def client_factory() -> RecordingClient:
        """Record an unexpected eager Docker client creation."""
        nonlocal calls
        calls += 1
        return RecordingClient()

    DockerSandboxRuntime(client_factory)

    assert calls == 0


@pytest.mark.anyio
async def test_default_factory_is_lazy_and_uses_five_second_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default client must be lazy and use the fixed SDK timeout."""
    client = RecordingClient()
    calls: list[dict[str, object]] = []

    def from_env(**kwargs: object) -> RecordingClient:
        """Record default Docker client construction arguments."""
        calls.append(kwargs)
        return client

    monkeypatch.setattr(sandbox_runtime.docker, "from_env", from_env)
    runtime = DockerSandboxRuntime()
    assert calls == []

    await runtime.create(make_spec())

    assert calls == [{"timeout": 5.0}]


@pytest.mark.anyio
async def test_create_checks_image_then_creates_network_volume_container_and_starts() -> None:
    """Creation must follow the fixed transaction order before closing."""
    client = RecordingClient()
    runtime = DockerSandboxRuntime(lambda: client)

    await runtime.create(make_spec())

    assert client.events == [
        "images.get:agent-sandbox:dev",
        "networks.create",
        "volumes.create",
        "containers.create",
        "container.start",
        "client.close",
    ]


@pytest.mark.anyio
async def test_network_is_dedicated_bridge_and_internal() -> None:
    """Each sandbox network must be a labeled internal bridge."""
    client = RecordingClient()

    await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.networks.create_calls == [
        {
            "name": make_spec().network_name,
            "driver": "bridge",
            "internal": True,
            "labels": make_spec().labels_for("network"),
        }
    ]


@pytest.mark.anyio
async def test_container_uses_fixed_image_names_labels_and_security_baseline() -> None:
    """The container must use the fixed image and complete safety baseline."""
    client = RecordingClient()

    await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.volumes.create_calls == [
        {
            "name": make_spec().volume_name,
            "labels": make_spec().labels_for("workspace"),
        }
    ]
    assert client.containers.create_calls == [
        {
            "image": "agent-sandbox:dev",
            "name": make_spec().container_name,
            "labels": make_spec().labels_for("container"),
            "init": True,
            "user": "sandbox",
            "working_dir": "/workspace",
            "nano_cpus": 1_000_000_000,
            "mem_limit": "256m",
            "memswap_limit": "256m",
            "pids_limit": 64,
            "cap_drop": ["ALL"],
            "security_opt": ["no-new-privileges:true"],
            "read_only": True,
            "tmpfs": {
                "/tmp": "rw,nosuid,nodev,size=32m,mode=1777",
            },
            "volumes": {
                make_spec().volume_name: {
                    "bind": "/workspace",
                    "mode": "rw",
                }
            },
            "network": make_spec().network_name,
        }
    ]


@pytest.mark.anyio
async def test_container_has_no_host_port_binding_or_extra_network() -> None:
    """The runtime must not publish ports or attach another network."""
    client = RecordingClient()

    await DockerSandboxRuntime(lambda: client).create(make_spec())

    create_options = client.containers.create_calls[0]
    assert "ports" not in create_options
    assert "network_mode" not in create_options
    assert create_options["network"] == make_spec().network_name
    assert "command" not in create_options
    assert "entrypoint" not in create_options
    assert "environment" not in create_options


@pytest.mark.anyio
async def test_success_closes_client_once() -> None:
    """A successful transaction must close its Docker client exactly once."""
    client = RecordingClient()

    await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.close_calls == 1
