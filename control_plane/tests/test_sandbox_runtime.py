"""Unit tests for Docker sandbox resource creation and cleanup."""

import asyncio
import threading
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from requests import exceptions as requests_exceptions

from control_plane.runtime.docker import transaction
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.core.errors import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxImageUnavailable,
    SandboxResourceConflict,
)
from control_plane.core.models import SandboxSpec
from control_plane.core.service import SandboxService
from lifecycle_fakes import FakeLifecycleStore


SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"
TRANSACTION_ID = "txn_0123456789abcdef0123456789abcdef"


@pytest.mark.anyio
async def test_service_creation_persists_identical_times_on_all_five_resources() -> None:
    """Service times must be persisted unchanged in every Docker create operation."""
    client = RecordingClient()
    times = {
        "io.agent-sandbox.created-at": "2026-10-19T08:00:00.123456Z",
        "io.agent-sandbox.expires-at": "2026-10-19T08:30:00.123456Z",
    }
    # The recording volume response represents Docker returning the supplied labels.
    client.volume.attrs["Labels"].update(times)
    clock_calls = 0

    def clock() -> datetime:
        """Provide one timestamp and reject any second snapshot during creation."""
        nonlocal clock_calls
        clock_calls += 1
        assert clock_calls == 1
        return datetime(2026, 10, 19, 8, 0, 0, 123456, tzinfo=UTC)

    result = await SandboxService(
        DockerSandboxRuntime(lambda: client),
        FakeLifecycleStore(),
        id_factory=lambda: SANDBOX_ID,
        transaction_id_factory=lambda: TRANSACTION_ID,
        clock=clock,
    ).create()

    calls = [*client.networks.create_calls, *client.volumes.create_calls,
             *client.containers.create_calls]
    assert len(calls) == 5
    roles = {call["labels"]["io.agent-sandbox.resource"] for call in calls}
    assert roles == {"network", "ingress", "workspace", "container", "proxy"}
    for call in calls:
        assert call["labels"] == {
            **make_spec().labels_for(call["labels"]["io.agent-sandbox.resource"]),
            **times,
        }
    assert clock_calls == 1
    assert result.created_at == datetime(2026, 10, 19, 8, 0, 0, 123456, tzinfo=UTC)
    assert result.expires_at == datetime(2026, 10, 19, 8, 30, 0, 123456, tzinfo=UTC)


@pytest.mark.anyio
async def test_timed_creation_failure_preserves_exact_reverse_rollback() -> None:
    """Adding initial lifecycle labels must preserve ownership-checked rollback."""
    client = RecordingClient()
    spec = replace(
        make_spec(),
        created_at=datetime(2026, 10, 19, 8, tzinfo=UTC),
        expires_at=datetime(2026, 10, 19, 8, 30, tzinfo=UTC),
    )
    for resource in [client.network, client.ingress, client.volume,
                     client.container, client.proxy]:
        resource.attrs["Labels"].update({
            "io.agent-sandbox.created-at": "2026-10-19T08:00:00Z",
            "io.agent-sandbox.expires-at": "2026-10-19T08:30:00Z",
        })
    client.proxy.start_error = RuntimeError("proxy start failed")

    with pytest.raises(SandboxCreateFailed):
        await DockerSandboxRuntime(lambda: client).create(spec)

    assert [event for event in client.events if event.endswith(".remove")] == [
        "proxy.remove", "container.remove", "workspace.remove",
        "ingress.remove", "network.remove",
    ]
    assert client.close_calls == 1


def make_spec() -> SandboxSpec:
    """Build one deterministic sandbox specification for runtime tests."""
    return SandboxSpec(
        sandbox_id=SANDBOX_ID,
        container_name=f"agent-sandbox-{SANDBOX_ID}",
        network_name=f"agent-sandbox-{SANDBOX_ID}-internal",
        volume_name=f"agent-sandbox-{SANDBOX_ID}-workspace",
        transaction_id=TRANSACTION_ID,
    )


def make_api_error(status_code: int, message: str = "Docker API failed") -> APIError:
    """Build a Docker API error with a stable fake HTTP status."""
    response = SimpleNamespace(
        status_code=status_code,
        url="docker://test",
        reason="Conflict" if status_code == 409 else "Server Error",
    )
    return APIError(message, response=response)


class RecordingResource:
    """Represent one Docker resource and record lifecycle operations."""

    def __init__(
        self,
        kind: str,
        resource_id: str,
        labels: dict[str, str],
        events: list[str],
    ) -> None:
        """Initialize a resource with Docker-like ID and attributes."""
        self.kind = kind
        self.id = resource_id
        self.attrs = {"Labels": labels, "Config": {"Labels": labels}}
        self._events = events
        self.start_error: Exception | None = None
        self.reload_error: Exception | None = None
        self.remove_error: Exception | None = None
        self.start_mutation: Callable[[], None] | None = None
        self.remove_calls: list[dict[str, Any]] = []
        self.removed = threading.Event()
        self.start_entered: threading.Event | None = None
        self.start_release: threading.Event | None = None

    def start(self) -> None:
        """Record that the container start operation was requested."""
        self._events.append(f"{self.kind}.start")
        if self.start_entered is not None:
            self.start_entered.set()
        if self.start_release is not None and not self.start_release.wait(timeout=1):
            raise TimeoutError("test did not release container start")
        if self.start_mutation is not None:
            self.start_mutation()
        if self.start_error is not None:
            raise self.start_error

    def reload(self) -> None:
        """Record an ownership refresh and optionally fail it."""
        self._events.append(f"{self.kind}.reload")
        if self.reload_error is not None:
            raise self.reload_error

    def remove(self, **kwargs: Any) -> None:
        """Record resource deletion arguments and optionally fail it."""
        self._events.append(f"{self.kind}.remove")
        self.remove_calls.append(kwargs)
        if self.remove_error is not None:
            raise self.remove_error
        self.removed.set()


class RecordingImages:
    """Record fixed-image lookup operations."""

    def __init__(self, events: list[str], error: Exception | None = None) -> None:
        """Store the shared operation log and an optional lookup error."""
        self._events = events
        self.error = error

    def get(self, image: str) -> object:
        """Record one image lookup and return its resolved immutable image ID."""
        self._events.append(f"images.get:{image}")
        if self.error is not None:
            raise self.error
        return SimpleNamespace(id="sha256:" + "a" * 64)


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
        self.error: Exception | None = None
        self.get_calls: list[str] = []
        self.create_entered: threading.Event | None = None
        self.create_release: threading.Event | None = None

    def create(self, **kwargs: Any) -> RecordingResource:
        """Record keyword arguments and return one Docker-like resource."""
        self._events.append(f"{self.kind}.create")
        self.create_calls.append(kwargs)
        if self.create_entered is not None:
            self.create_entered.set()
        if self.create_release is not None and not self.create_release.wait(timeout=1):
            raise TimeoutError(f"test did not release {self.kind}.create")
        if self.error is not None:
            raise self.error
        return self._resource_factory(kwargs)

    def get(self, resource_id: str) -> RecordingResource:
        """Record an exact-ID lookup for late cancellation cleanup."""
        self.get_calls.append(resource_id)
        resource = self._resource_factory({"resource_id": resource_id})
        if resource.removed.is_set():
            raise NotFound("resource was removed")
        return resource


class RecordingClient:
    """Provide Docker-like managers while recording all operations."""

    def __init__(self) -> None:
        """Initialize managers, resources, and shared operation history."""
        self.events: list[str] = []
        self.close_calls = 0
        self.closed = threading.Event()
        self.close_entered: threading.Event | None = None
        self.close_release: threading.Event | None = None
        self.network = RecordingResource(
            "network",
            "network-id",
            make_spec().labels_for("network"),
            self.events,
        )
        self.volume = RecordingResource(
            "workspace",
            "volume-id",
            make_spec().labels_for("workspace"),
            self.events,
        )
        self.container = RecordingResource(
            "container",
            "container-id",
            make_spec().labels_for("container"),
            self.events,
        )
        self.ingress = RecordingResource(
            "ingress", "ingress-id", make_spec().labels_for("ingress"), self.events
        )
        self.proxy = RecordingResource(
            "proxy", "proxy-id", make_spec().labels_for("proxy"), self.events
        )
        self.images = RecordingImages(self.events)
        self.networks = RecordingManager(
            "networks",
            self.events,
            lambda kwargs: self.ingress if kwargs.get("resource_id") == "ingress-id" or kwargs.get("name") == make_spec().ingress_network_name else self.network,
        )
        self.volumes = RecordingManager(
            "volumes",
            self.events,
            lambda _kwargs: self.volume,
        )
        self.containers = RecordingManager(
            "containers",
            self.events,
            lambda kwargs: self.proxy if kwargs.get("resource_id") == "proxy-id" or kwargs.get("image") == "agent-sandbox-proxy:dev" else self.container,
        )
        self.api = RecordingAPI(self)

    def close(self) -> None:
        """Record one Docker client close."""
        self.close_calls += 1
        self.events.append("client.close")
        if self.close_calls == 1 and self.close_entered is not None:
            self.close_entered.set()
        if (
            self.close_calls == 1
            and self.close_release is not None
            and not self.close_release.wait(timeout=1)
        ):
            raise TimeoutError("test did not release Docker client close")
        self.closed.set()


class RecordingAPI:
    """Adapt low-level Docker create calls to the existing recording managers."""

    def __init__(self, client: RecordingClient) -> None:
        """Store the recording client and a Docker API version marker."""
        self._client = client
        self._version = "1.45"

    def create_network(self, name: str, **kwargs: Any) -> dict[str, str]:
        """Create a fake network and return its confirmed ID."""
        resource = self._client.networks.create(name=name, **kwargs)
        return {"Id": resource.id}

    def create_volume(self, name: str, **kwargs: Any) -> dict[str, Any]:
        """Create a fake volume and return its name and actual labels."""
        resource = self._client.volumes.create(name=name, **kwargs)
        return {
            "Name": resource.id,
            "Labels": resource.attrs["Labels"],
        }

    def create_host_config(self, **kwargs: Any) -> dict[str, Any]:
        """Return inspectable host configuration arguments."""
        return kwargs

    def create_container(self, **kwargs: Any) -> dict[str, str]:
        """Create a fake container and return its confirmed ID."""
        resource = self._client.containers.create(**kwargs)
        return {"Id": resource.id}


    def connect_container_to_network(self, container: str, network: str) -> None:
        """Record proxy attachment to its dedicated internal bridge."""
        assert container == "proxy-id"
        assert network == "network-id"
        self._client.events.append("proxy.connect")


class RealisticAPI:
    """Return create response IDs before model inspection like Docker's API."""

    def __init__(self, client: "RealisticClient") -> None:
        """Store the owning fake client."""
        self._client = client
        self._version = "1.45"

    def create_network(self, name: str, **kwargs: Any) -> dict[str, str]:
        """Confirm network creation with its ID."""
        self._client.events.append("api.network.create")
        self._client.network_create_calls.append({"name": name, **kwargs})
        return {"Id": self._client.ingress.id if name == make_spec().ingress_network_name else self._client.network.id}

    def create_volume(self, name: str, **kwargs: Any) -> dict[str, Any]:
        """Return Docker's idempotent volume response and existing labels."""
        self._client.events.append("api.volume.create")
        self._client.volume_create_calls.append({"name": name, **kwargs})
        return {
            "Name": self._client.volume.id,
            "Labels": self._client.volume.attrs["Labels"],
        }

    def create_host_config(self, **kwargs: Any) -> dict[str, Any]:
        """Return inspectable host configuration options."""
        return kwargs

    def create_container(self, **kwargs: Any) -> dict[str, str]:
        """Confirm container creation with its ID before model inspection."""
        self._client.events.append("api.container.create")
        self._client.container_create_calls.append(kwargs)
        return {"Id": self._client.proxy.id if kwargs["image"] == "agent-sandbox-proxy:dev" else self._client.container.id}


    def connect_container_to_network(self, container: str, network: str) -> None:
        """Record proxy attachment to its dedicated internal bridge."""
        assert container == "proxy-id"
        assert network == "network-id"
        self._client.events.append("proxy.connect")


class RealisticManager:
    """Model high-level SDK create methods that inspect after creation."""

    def __init__(self, client: "RealisticClient", kind: str) -> None:
        """Store the owning client, resource kind, and lookup failures."""
        self._client = client
        self._kind = kind
        self.get_errors: list[Exception] = []
        self.get_calls: list[str] = []

    def create(self, **kwargs: Any) -> RecordingResource:
        """Use the low-level response, then inspect network or container models."""
        if self._kind == "network":
            response = self._client.api.create_network(**kwargs)
            return self.get(response["Id"])
        if self._kind == "workspace":
            self._client.api.create_volume(**kwargs)
            return self._client.volume
        response = self._client.api.create_container(**kwargs)
        return self.get(response["Id"])

    def get(self, resource_id: str) -> RecordingResource:
        """Load one exact ID and optionally fail the next inspection."""
        self.get_calls.append(resource_id)
        if self.get_errors:
            raise self.get_errors.pop(0)
        resource = {
            "proxy-id": self._client.proxy,
            "ingress-id": self._client.ingress,
        }.get(resource_id) or {
            "network": self._client.network,
            "workspace": self._client.volume,
            "container": self._client.container,
        }[self._kind]
        if resource.removed.is_set():
            raise NotFound("resource was removed")
        return resource


class RealisticClient:
    """Expose both low-level create responses and high-level model lookup."""

    def __init__(self) -> None:
        """Initialize realistic Docker SDK surfaces and fixed resources."""
        self.events: list[str] = []
        spec = make_spec()
        self.network = RecordingResource(
            "network", "network-id", spec.labels_for("network"), self.events
        )
        self.volume = RecordingResource(
            "workspace", spec.volume_name, spec.labels_for("workspace"), self.events
        )
        self.container = RecordingResource(
            "container", "container-id", spec.labels_for("container"), self.events
        )
        self.network_create_calls: list[dict[str, Any]] = []
        self.volume_create_calls: list[dict[str, Any]] = []
        self.container_create_calls: list[dict[str, Any]] = []
        self.ingress = RecordingResource(
            "ingress", "ingress-id", make_spec().labels_for("ingress"), self.events
        )
        self.proxy = RecordingResource(
            "proxy", "proxy-id", make_spec().labels_for("proxy"), self.events
        )
        self.images = RecordingImages(self.events)
        self.api = RealisticAPI(self)
        self.networks = RealisticManager(self, "network")
        self.volumes = RealisticManager(self, "workspace")
        self.containers = RealisticManager(self, "container")
        self.close_calls = 0

    def close(self) -> None:
        """Record client closure."""
        self.close_calls += 1


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

    monkeypatch.setattr(transaction.docker, "from_env", from_env)
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
        "images.get:agent-sandbox-proxy:dev",
        "networks.create",
        "networks.create",
        "volumes.create",
        "containers.create",
        "container.start",
        "containers.create",
        "proxy.connect",
        "proxy.start",
        "client.close",
    ]


@pytest.mark.anyio
async def test_network_is_dedicated_bridge_and_internal() -> None:
    """Each sandbox network must be a labeled internal bridge."""
    client = RecordingClient()

    await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.networks.create_calls[:1] == [
        {
            "name": make_spec().network_name,
            "driver": "bridge",
            "internal": True,
            "labels": make_spec().labels_for("network"),
            "check_duplicate": True,
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
    assert client.containers.create_calls[:1] == [
        {
            "image": "sha256:" + "a" * 64,
            "name": make_spec().container_name,
            "labels": make_spec().labels_for("container"),
            "user": "sandbox",
            "working_dir": "/workspace",
            "volumes": ["/workspace"],
            "host_config": {
                "init": True,
                "nano_cpus": 1_000_000_000,
                "mem_limit": 268_435_456,
                "memswap_limit": 268_435_456,
                "pids_limit": 64,
                "cap_drop": ["ALL"],
                "security_opt": ["no-new-privileges:true"],
                "read_only": True,
                "tmpfs": {
                    "/tmp": "rw,nosuid,nodev,size=32m,mode=1777",
                },
                "binds": {
                    make_spec().volume_name: {
                        "bind": "/workspace",
                        "mode": "rw",
                    }
                },
                "network_mode": make_spec().network_name,
            },
            "networking_config": {"EndpointsConfig": {make_spec().network_name: {"Aliases": ["sandbox"]}}},
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
    assert "port_bindings" not in create_options["host_config"]
    assert create_options["host_config"]["network_mode"] == make_spec().network_name
    assert create_options["networking_config"] == {"EndpointsConfig": {make_spec().network_name: {"Aliases": ["sandbox"]}}}
    assert "command" not in create_options
    assert "entrypoint" not in create_options
    assert "environment" not in create_options


@pytest.mark.anyio
async def test_success_closes_client_once() -> None:
    """A successful transaction must close its Docker client exactly once."""
    client = RecordingClient()

    await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.close_calls == 1


@pytest.mark.anyio
async def test_missing_image_creates_nothing_and_raises_image_unavailable() -> None:
    """A missing fixed image must fail before any resource is created."""
    client = RecordingClient()
    client.images.error = ImageNotFound("missing image")

    with pytest.raises(SandboxImageUnavailable):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.networks.create_calls[:1] == []
    assert client.volumes.create_calls == []
    assert client.containers.create_calls == []
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_client_factory_failure_raises_docker_unavailable() -> None:
    """A Docker client construction failure must map to unavailable."""
    error = DockerException("unix:///secret/docker.sock unavailable")

    def failing_factory() -> RecordingClient:
        """Raise before a Docker client exists."""
        raise error

    with pytest.raises(DockerUnavailable) as captured:
        await DockerSandboxRuntime(failing_factory).create(make_spec())

    assert captured.value.__cause__ is error


@pytest.mark.anyio
async def test_conflict_does_not_adopt_or_delete_existing_resource() -> None:
    """A network-name conflict must not delete or adopt an existing object."""
    client = RecordingClient()
    client.networks.error = make_api_error(409, "network conflict")

    with pytest.raises(SandboxResourceConflict):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.network.remove_calls == []
    assert client.volume.remove_calls == []
    assert client.container.remove_calls == []


@pytest.mark.anyio
async def test_volume_conflict_rolls_back_created_network_without_touching_conflicting_volume() -> None:
    """A volume conflict must remove both bridges without deleting the foreign volume."""
    client = RecordingClient()
    client.volumes.error = make_api_error(409, "volume conflict")

    with pytest.raises(SandboxResourceConflict):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.network.remove_calls == [{}]
    assert client.volume.remove_calls == []
    assert client.container.remove_calls == []
    assert [event for event in client.events if event.endswith(".remove")] == [
        "ingress.remove", "network.remove"
    ]


@pytest.mark.anyio
async def test_existing_volume_with_foreign_labels_is_not_adopted() -> None:
    """Docker's idempotent volume create must not adopt a foreign volume."""
    client = RealisticClient()
    client.volume.attrs["Labels"] = make_spec().labels_for("workspace")
    client.volume.attrs["Labels"]["io.agent-sandbox.transaction-id"] = (
        "txn_ffffffffffffffffffffffffffffffff"
    )
    client.volume.attrs["Config"]["Labels"] = client.volume.attrs["Labels"]

    with pytest.raises(SandboxResourceConflict):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.container_create_calls == []
    assert client.volume.remove_calls == []
    assert client.network.remove_calls == [{}]


@pytest.mark.anyio
async def test_network_create_does_not_depend_on_hidden_model_inspection() -> None:
    """A confirmed network ID must not be lost to a hidden SDK inspection."""
    client = RealisticClient()
    client.networks.get_errors.append(DockerException("transient inspect failure"))

    await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert "container.start" in client.events
    assert client.network.remove_calls == []


@pytest.mark.anyio
async def test_container_inspect_failure_rolls_back_confirmed_create_id() -> None:
    """A post-create inspect failure must roll back the confirmed container ID."""
    client = RealisticClient()
    client.containers.get_errors.append(DockerException("transient inspect failure"))

    with pytest.raises(DockerUnavailable):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.container.remove_calls == [{"force": True}]
    assert client.volume.remove_calls == [{}]
    assert client.network.remove_calls == [{}]


@pytest.mark.anyio
async def test_volume_failure_removes_only_created_network() -> None:
    """A volume API failure must roll back only the prior network."""
    client = RecordingClient()
    client.volumes.error = make_api_error(500, "volume failed")

    with pytest.raises(SandboxCreateFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.network.remove_calls == [{}]
    assert client.volume.remove_calls == []
    assert client.container.remove_calls == []


@pytest.mark.anyio
async def test_container_failure_removes_volume_then_network() -> None:
    """A container create failure must roll back in reverse order."""
    client = RecordingClient()
    client.containers.error = make_api_error(500, "container failed")

    with pytest.raises(SandboxCreateFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert [event for event in client.events if event.endswith(".remove")] == [
        "workspace.remove",
        "ingress.remove",
        "network.remove",
    ]
    assert client.volume.remove_calls == [{}]
    assert client.network.remove_calls == [{}]


@pytest.mark.anyio
async def test_start_failure_removes_container_volume_network_in_reverse_order() -> None:
    """A start failure must remove every created resource in reverse order."""
    client = RecordingClient()
    client.container.start_error = make_api_error(500, "start failed")

    with pytest.raises(SandboxCreateFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert [event for event in client.events if event.endswith(".remove")] == [
        "container.remove",
        "workspace.remove",
        "ingress.remove",
        "network.remove",
    ]
    assert client.container.remove_calls == [{"force": True}]
    assert client.volume.remove_calls == [{}]
    assert client.network.remove_calls == [{}]


@pytest.mark.anyio
async def test_failure_closes_client_once() -> None:
    """A failed transaction must close its Docker client exactly once."""
    client = RecordingClient()
    client.containers.error = make_api_error(500)

    with pytest.raises(SandboxCreateFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("cancel_during_close", [False, True])
async def test_create_close_failure_reports_uncertain_cleanup_with_sandbox_id(
    cancel_during_close: bool,
) -> None:
    """A transport close failure must not imply resource rollback was confirmed."""
    client = RecordingClient()
    entered, release = threading.Event(), threading.Event()

    def fail_close() -> None:
        """Expose an optional cancellation window before failing finalization."""
        client.close_calls += 1
        entered.set()
        assert release.wait(1)
        raise RuntimeError("private transport finalization details")

    client.close = fail_close
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(make_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        if cancel_during_close:
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            await asyncio.sleep(0)
        release.set()
        with pytest.raises(SandboxCleanupFailed) as captured:
            await task
        assert captured.value.sandbox_id == SANDBOX_ID
        assert client.close_calls == 1
    finally:
        release.set()


@pytest.mark.anyio
async def test_create_close_failure_preserves_confirmed_rollback_error() -> None:
    """A later close failure must not hide a primary failure with completed rollback."""
    client = RecordingClient()
    client.container.start_error = make_api_error(500, "start failed")

    def fail_close() -> None:
        """Fail transport finalization after recording its single close attempt."""
        client.close_calls += 1
        raise RuntimeError("private transport finalization details")

    client.close = fail_close
    with pytest.raises(SandboxCreateFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.close_calls == 1
    assert client.container.remove_calls == [{"force": True}]
    assert client.volume.remove_calls == [{}]
    assert client.ingress.remove_calls == [{}]
    assert client.network.remove_calls == [{}]


@pytest.mark.anyio
async def test_rollback_refuses_resource_with_changed_id() -> None:
    """Rollback must not delete an object whose Docker ID changed."""
    client = RecordingClient()
    client.container.start_error = make_api_error(500, "start failed")
    client.container.start_mutation = lambda: setattr(
        client.container,
        "id",
        "different-container-id",
    )

    with pytest.raises(SandboxCleanupFailed) as captured:
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert captured.value.sandbox_id == SANDBOX_ID
    assert client.container.remove_calls == []
    assert client.volume.remove_calls == [{}]
    assert client.network.remove_calls == [{}]


@pytest.mark.anyio
async def test_rollback_refuses_resource_with_missing_or_changed_label() -> None:
    """Rollback must skip deletion when any ownership label is wrong."""
    client = RecordingClient()
    client.container.start_error = make_api_error(500, "start failed")

    def change_labels() -> None:
        """Replace the container's labels before rollback validation."""
        client.container.attrs["Config"]["Labels"] = {
            "io.agent-sandbox.managed": "true",
            "io.agent-sandbox.project": "another-project",
            "io.agent-sandbox.sandbox-id": SANDBOX_ID,
            "io.agent-sandbox.resource": "container",
        }

    client.container.start_mutation = change_labels

    with pytest.raises(SandboxCleanupFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.container.remove_calls == []
    assert client.volume.remove_calls == [{}]
    assert client.network.remove_calls == [{}]


@pytest.mark.anyio
async def test_cleanup_failure_reports_sandbox_id_and_keeps_original_error_chain() -> None:
    """Incomplete cleanup must expose the ID and preserve failure causes."""
    client = RecordingClient()
    start_error = make_api_error(500, "start failed")
    client.container.start_error = start_error
    client.volume.remove_error = DockerException("volume remove failed")

    with pytest.raises(SandboxCleanupFailed) as captured:
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert captured.value.sandbox_id == SANDBOX_ID
    assert isinstance(captured.value.__cause__, SandboxCreateFailed)
    assert captured.value.__cause__.__cause__ is start_error
    assert client.container.remove_calls == [{"force": True}]
    assert client.volume.remove_calls == [{}]
    assert client.network.remove_calls == [{}]


@pytest.mark.anyio
async def test_rollback_loads_only_recorded_exact_resource_ids() -> None:
    """In-transaction rollback must load exact IDs rather than resource names."""
    client = RecordingClient()
    client.container.start_error = make_api_error(500, "start failed")

    with pytest.raises(SandboxCreateFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.networks.get_calls == ["ingress-id", "ingress-id", "network-id", "network-id"]
    assert client.volumes.get_calls == ["volume-id", "volume-id"]
    assert client.containers.get_calls == ["container-id", "container-id", "container-id"]


@pytest.mark.anyio
@pytest.mark.timeout(2)
async def test_cancel_stops_before_next_create_step_and_waits_for_rollback() -> None:
    """Cancellation during create must stop later steps and await cleanup."""
    client = RecordingClient()
    entered = threading.Event()
    release = threading.Event()
    client.volumes.create_entered = entered
    client.volumes.create_release = release
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(make_spec()))

    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert client.containers.create_calls == []
        assert client.volume.remove_calls == [{}]
        assert client.network.remove_calls == [{}]
    finally:
        release.set()


@pytest.mark.anyio
@pytest.mark.timeout(2)
async def test_cancel_during_start_removes_started_or_created_resources() -> None:
    """Cancellation during start must remove the complete resource stack."""
    client = RecordingClient()
    entered = threading.Event()
    release = threading.Event()
    client.container.start_entered = entered
    client.container.start_release = release
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(make_spec()))

    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert client.container.remove_calls == [{"force": True}]
        assert client.volume.remove_calls == [{}]
        assert client.network.remove_calls == [{}]
    finally:
        release.set()


@pytest.mark.anyio
@pytest.mark.timeout(2)
async def test_cancel_after_thread_commit_rolls_back_committed_resources() -> None:
    """Cancellation after commit must reopen by exact ID and roll back."""
    client = RecordingClient()
    entered = threading.Event()
    release = threading.Event()
    client.close_entered = entered
    client.close_release = release
    factory_calls = 0

    def client_factory() -> RecordingClient:
        """Return the same inspectable fake for creation and late cleanup."""
        nonlocal factory_calls
        factory_calls += 1
        return client

    task = asyncio.create_task(DockerSandboxRuntime(client_factory).create(make_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert factory_calls == 2
        assert client.containers.get_calls == [
            "container-id", "proxy-id", "proxy-id", "proxy-id", "container-id", "container-id",
        ]
        assert client.volumes.get_calls == ["volume-id", "volume-id"]
        assert client.networks.get_calls == ["ingress-id", "ingress-id", "network-id", "network-id"]
        assert client.container.remove_calls == [{"force": True}]
        assert client.volume.remove_calls == [{}]
        assert client.network.remove_calls == [{}]
        assert client.close_calls == 2
    finally:
        release.set()


@pytest.mark.anyio
@pytest.mark.timeout(2)
@pytest.mark.parametrize("with_initial_times", [False, True])
async def test_repeated_cancellation_still_waits_for_committed_cleanup(
    with_initial_times: bool,
) -> None:
    """Repeated cancellation must await exact rollback with or without time metadata."""
    client = RecordingClient()
    spec = make_spec()
    if with_initial_times:
        spec = replace(
            spec,
            created_at=datetime(2026, 10, 19, 8, tzinfo=UTC),
            expires_at=datetime(2026, 10, 19, 8, 30, tzinfo=UTC),
        )
        for resource in [client.network, client.ingress, client.volume,
                         client.container, client.proxy]:
            resource.attrs["Labels"].update({
                "io.agent-sandbox.created-at": "2026-10-19T08:00:00Z",
                "io.agent-sandbox.expires-at": "2026-10-19T08:30:00Z",
            })
    entered = threading.Event()
    release = threading.Event()
    client.close_entered = entered
    client.close_release = release
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(spec))

    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert client.container.remove_calls == [{"force": True}]
        assert client.volume.remove_calls == [{}]
        assert client.network.remove_calls == [{}]
        assert client.proxy.remove_calls == [{"force": True}]
        assert client.ingress.remove_calls == [{}]
        assert [event for event in client.events if event.endswith(".remove")] == [
            "proxy.remove", "container.remove", "workspace.remove",
            "ingress.remove", "network.remove",
        ]
    finally:
        release.set()


@pytest.mark.anyio
@pytest.mark.timeout(2)
async def test_cancel_does_not_leave_unhandled_thread_exception() -> None:
    """A late SDK failure after cancellation must be consumed after cleanup."""
    client = RecordingClient()
    entered = threading.Event()
    release = threading.Event()
    client.volumes.create_entered = entered
    client.volumes.create_release = release
    client.volumes.error = make_api_error(500, "late volume failure")
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(make_spec()))

    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        release.set()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert await asyncio.to_thread(client.closed.wait, 1)
        await asyncio.sleep(0)
        assert client.network.remove_calls == [{}]
        assert loop_errors == []
    finally:
        release.set()
        loop.set_exception_handler(previous_handler)


@pytest.mark.anyio
@pytest.mark.timeout(2)
async def test_cancel_with_incomplete_cleanup_raises_cleanup_failed() -> None:
    """Cancellation must expose incomplete cleanup instead of hiding it."""
    client = RecordingClient()
    entered = threading.Event()
    release = threading.Event()
    client.container.start_entered = entered
    client.container.start_release = release
    client.volume.remove_error = DockerException("volume cleanup failed")
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(make_spec()))

    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        release.set()

        with pytest.raises(SandboxCleanupFailed) as captured:
            await task

        assert captured.value.sandbox_id == SANDBOX_ID
        assert client.container.remove_calls == [{"force": True}]
        assert client.volume.remove_calls == [{}]
        assert client.network.remove_calls == [{}]
    finally:
        release.set()


@pytest.mark.anyio
async def test_cancel_does_not_hide_unconfirmed_created_resource_identity() -> None:
    """Cancellation must preserve an uncertain create response for persisted recovery."""
    client = RecordingClient()
    entered, release = threading.Event(), threading.Event()
    original_create_network = client.api.create_network

    def create_without_confirmed_id(name: str, **kwargs: Any) -> dict[str, str]:
        """Model daemon creation followed by an incomplete successful response."""
        original_create_network(name, **kwargs)
        entered.set()
        assert release.wait(1)
        return {}

    client.api.create_network = create_without_confirmed_id
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(make_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(SandboxCleanupFailed) as captured:
            await task
        assert captured.value.sandbox_id == SANDBOX_ID
        assert client.network.remove_calls == []
        assert client.close_calls == 1
    finally:
        release.set()


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["create_network", "create_container"])
@pytest.mark.parametrize("transport_error", [
    DockerException("private daemon transport details"),
    requests_exceptions.ConnectionError("private connection details"),
    requests_exceptions.ReadTimeout("private timeout details"),
])
async def test_cancel_with_create_transport_failure_preserves_uncertain_cleanup(
    operation: str, transport_error: Exception,
) -> None:
    """A lost create response may hide daemon resources despite recorded-ID rollback."""
    client = RecordingClient()
    entered, release = threading.Event(), threading.Event()
    original_create = getattr(client.api, operation)

    def create_with_lost_response(*args: Any, **kwargs: Any) -> dict[str, str]:
        """Model server-side creation followed by a transport failure before its ID."""
        original_create(*args, **kwargs)
        entered.set()
        assert release.wait(1)
        raise transport_error

    setattr(client.api, operation, create_with_lost_response)
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(make_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(SandboxCleanupFailed) as captured:
            await task
        assert captured.value.sandbox_id == SANDBOX_ID
        assert client.close_calls == 1
        unknown_resource = client.network if operation == "create_network" else client.container
        assert unknown_resource.remove_calls == []
        if operation == "create_container":
            assert client.volume.remove_calls == [{}]
            assert client.ingress.remove_calls == [{}]
            assert client.network.remove_calls == [{}]
    finally:
        release.set()


@pytest.mark.anyio
async def test_cancel_after_transport_error_classification_keeps_uncertain_result() -> None:
    """A cancellation during close must not hide an already classified transport failure."""
    client = RecordingClient()
    entered, release = threading.Event(), threading.Event()
    client.networks.error = DockerException("private lost create response")
    client.close_entered, client.close_release = entered, release
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(make_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        release.set()
        with pytest.raises(SandboxCleanupFailed) as captured:
            await task
        assert captured.value.sandbox_id == SANDBOX_ID
        assert client.close_calls == 1
    finally:
        release.set()


@pytest.mark.anyio
async def test_proxy_uses_dedicated_ingress_dynamic_loopback_and_safety_limits() -> None:
    """Proxy must expose only loopback with no host/workspace mounts and finite limits."""
    client = RecordingClient()
    await DockerSandboxRuntime(lambda: client).create(make_spec())
    ingress = client.networks.create_calls[1]
    assert ingress == {
        "name": make_spec().ingress_network_name, "driver": "bridge", "internal": False,
        "labels": make_spec().labels_for("ingress"), "check_duplicate": True,
    }
    proxy = client.containers.create_calls[1]
    assert proxy["image"] == "agent-sandbox-proxy:dev"
    assert proxy["name"] == make_spec().proxy_name
    assert proxy["labels"] == make_spec().labels_for("proxy")
    assert proxy["user"] == "nginx"
    assert proxy["ports"] == [8080]
    assert proxy["networking_config"] == {"EndpointsConfig": {make_spec().ingress_network_name: {}}}
    host = proxy["host_config"]
    assert host == {
        "init": True, "nano_cpus": 500_000_000, "mem_limit": "64m", "memswap_limit": "64m",
        "pids_limit": 32, "cap_drop": ["ALL"], "security_opt": ["no-new-privileges:true"],
        "read_only": True, "tmpfs": {"/tmp": "rw,nosuid,nodev,size=16m,mode=1777"},
        "network_mode": make_spec().ingress_network_name, "port_bindings": {8080: ("127.0.0.1", None)},
    }
    assert "volumes" not in proxy
    assert "command" not in proxy
    assert client.events.index("proxy.connect") < client.events.index("proxy.start")


@pytest.mark.anyio
@pytest.mark.parametrize("step", ["proxy_create", "connect", "proxy_start"])
async def test_proxy_failures_roll_back_all_confirmed_resources(step: str) -> None:
    """Proxy create, connection and start failures must reverse the full ledger."""
    client = RecordingClient()
    if step == "proxy_create":
        original = client.api.create_container

        def fail_proxy(**kwargs: Any) -> dict[str, str]:
            """Fail proxy creation after successful sandbox creation."""
            if kwargs["image"] == "agent-sandbox-proxy:dev":
                raise make_api_error(500)
            return original(**kwargs)

        client.api.create_container = fail_proxy
    elif step == "connect":
        def fail_connect(*args: Any) -> None:
            """Fail after the proxy ID has been confirmed and recorded."""
            raise make_api_error(500)

        client.api.connect_container_to_network = fail_connect
    else:
        client.proxy.start_error = make_api_error(500)
    with pytest.raises(SandboxCreateFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())
    removed = [e for e in client.events if e.endswith(".remove")]
    expected = ["container.remove", "workspace.remove", "ingress.remove", "network.remove"]
    assert removed == (["proxy.remove"] if step != "proxy_create" else []) + expected


@pytest.mark.anyio
async def test_missing_proxy_image_creates_no_resources() -> None:
    """Both images must exist before the first resource is created."""
    client = RecordingClient()

    def image_get(image: str) -> object:
        """Fail the second fixed-image lookup only."""
        if image == "agent-sandbox-proxy:dev":
            raise ImageNotFound("missing proxy")
        return SimpleNamespace(id="sha256:" + "a" * 64)

    client.images.get = image_get
    with pytest.raises(SandboxImageUnavailable):
        await DockerSandboxRuntime(lambda: client).create(make_spec())
    assert client.networks.create_calls == []


@pytest.mark.anyio
@pytest.mark.parametrize("environment", ["basic", "python", "node"])
async def test_cancel_during_proxy_start_reverses_all_resources(environment: str) -> None:
    """Cancellation must wait for proxy start and then remove every recorded role."""
    client = RecordingClient()
    entered, release = threading.Event(), threading.Event()
    client.proxy.start_entered, client.proxy.start_release = entered, release
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(
        replace(make_spec(), environment=environment),
    ))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert [e for e in client.events if e.endswith(".remove")] == [
            "proxy.remove", "container.remove", "workspace.remove", "ingress.remove", "network.remove",
        ]
    finally:
        release.set()


@pytest.mark.anyio
@pytest.mark.parametrize("role", ["proxy", "ingress"])
@pytest.mark.parametrize("change", ["id", "label"])
async def test_rollback_refuses_changed_new_resource_ownership(role: str, change: str) -> None:
    """New resource roles must receive the same exact-ID/full-label delete guard."""
    client = RecordingClient()
    target = getattr(client, role)

    def mutate() -> None:
        """Change ownership after creation but before failed proxy start returns."""
        if change == "id":
            target.id = "foreign-id"
        else:
            labels = target.attrs["Config"]["Labels"] if role == "proxy" else target.attrs["Labels"]
            labels["io.agent-sandbox.transaction-id"] = "foreign"

    client.proxy.start_mutation = mutate
    client.proxy.start_error = make_api_error(500)
    with pytest.raises(SandboxCleanupFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())
    assert target.remove_calls == []
    assert client.container.remove_calls == [{"force": True}]
    assert client.volume.remove_calls == [{}]
    assert client.network.remove_calls == [{}]


@pytest.mark.anyio
async def test_ingress_create_failure_only_removes_internal_network() -> None:
    """A failed ingress create must not record or remove unconfirmed resources."""
    client = RecordingClient()
    original = client.api.create_network

    def fail_ingress(name: str, **kwargs: Any) -> dict[str, str]:
        """Fail the ordinary bridge create after the internal bridge exists."""
        if name == make_spec().ingress_network_name:
            raise make_api_error(500)
        return original(name, **kwargs)

    client.api.create_network = fail_ingress
    with pytest.raises(SandboxCreateFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())
    assert [e for e in client.events if e.endswith(".remove")] == ["network.remove"]
    assert client.ingress.remove_calls == []
    assert client.volumes.create_calls == []


@pytest.mark.anyio
async def test_cancel_during_proxy_connect_waits_for_complete_rollback() -> None:
    """An in-flight connect must finish before repeated cancellation cleans all IDs."""
    client = RecordingClient()
    entered, release = threading.Event(), threading.Event()

    def connect(container: str, network: str) -> None:
        """Block connection until cancellation has reached the transaction."""
        entered.set()
        assert release.wait(1)

    client.api.connect_container_to_network = connect
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create(make_spec()))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert [e for e in client.events if e.endswith(".remove")] == [
            "proxy.remove", "container.remove", "workspace.remove", "ingress.remove", "network.remove",
        ]
        assert "proxy.start" not in client.events
    finally:
        release.set()
