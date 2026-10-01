"""Unit tests for Docker sandbox resource creation and cleanup."""

import asyncio
import threading
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest
from docker.errors import APIError, DockerException, ImageNotFound

import control.sandbox_runtime as sandbox_runtime
from control.sandbox_runtime import DockerSandboxRuntime
from control.sandbox_service import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxImageUnavailable,
    SandboxResourceConflict,
    SandboxSpec,
)


SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"


def make_spec() -> SandboxSpec:
    """Build one deterministic sandbox specification for runtime tests."""
    return SandboxSpec(
        sandbox_id=SANDBOX_ID,
        container_name=f"agent-sandbox-{SANDBOX_ID}",
        network_name=f"agent-sandbox-{SANDBOX_ID}-internal",
        volume_name=f"agent-sandbox-{SANDBOX_ID}-workspace",
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
        self._events.append("container.start")
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
        """Record one image lookup and return an opaque image."""
        self._events.append(f"images.get:{image}")
        if self.error is not None:
            raise self.error
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
        return self._resource_factory({})


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
        if self.close_calls == 1 and self.close_entered is not None:
            self.close_entered.set()
        if (
            self.close_calls == 1
            and self.close_release is not None
            and not self.close_release.wait(timeout=1)
        ):
            raise TimeoutError("test did not release Docker client close")
        self.closed.set()


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


@pytest.mark.anyio
async def test_missing_image_creates_nothing_and_raises_image_unavailable() -> None:
    """A missing fixed image must fail before any resource is created."""
    client = RecordingClient()
    client.images.error = ImageNotFound("missing image")

    with pytest.raises(SandboxImageUnavailable):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.networks.create_calls == []
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
    """A volume conflict must remove only the network created earlier."""
    client = RecordingClient()
    client.volumes.error = make_api_error(409, "volume conflict")

    with pytest.raises(SandboxResourceConflict):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.network.remove_calls == [{}]
    assert client.volume.remove_calls == []
    assert client.container.remove_calls == []
    assert [event for event in client.events if event.endswith(".remove")] == [
        "network.remove"
    ]


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
async def test_rollback_never_searches_for_resources_by_name() -> None:
    """In-transaction rollback must use recorded objects rather than names."""
    client = RecordingClient()
    client.container.start_error = make_api_error(500, "start failed")

    with pytest.raises(SandboxCreateFailed):
        await DockerSandboxRuntime(lambda: client).create(make_spec())

    assert client.networks.get_calls == []
    assert client.volumes.get_calls == []
    assert client.containers.get_calls == []


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
        assert client.containers.get_calls == ["container-id"]
        assert client.volumes.get_calls == ["volume-id"]
        assert client.networks.get_calls == ["network-id"]
        assert client.container.remove_calls == [{"force": True}]
        assert client.volume.remove_calls == [{}]
        assert client.network.remove_calls == [{}]
        assert client.close_calls == 2
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
