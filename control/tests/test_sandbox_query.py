"""Unit tests for reading managed sandbox state from Docker."""

from typing import Any

import pytest
from docker.errors import DockerException, NotFound

from control.sandbox_runtime import DockerSandboxRuntime
from control.sandbox_service import (
    DockerUnavailable,
    SandboxNotFound,
    SandboxQueryFailed,
    SandboxQueryResult,
)


SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"
CONTAINER_ID = "container-id"
MANAGED_LABELS = {
    "io.agent-sandbox.managed": "true",
    "io.agent-sandbox.project": "agent-sandbox",
    "io.agent-sandbox.sandbox-id": SANDBOX_ID,
    "io.agent-sandbox.transaction-id": "txn_0123456789abcdef0123456789abcdef",
    "io.agent-sandbox.resource": "container",
}


class QueryContainer:
    """Represent one inspected Docker container without mutation methods."""

    def __init__(
        self,
        state: dict[str, Any],
        labels: dict[str, str] | None = None,
    ) -> None:
        """Initialize a complete inspect shape and reload observation."""
        self.id = CONTAINER_ID
        self.attrs = {
            "Config": {"Labels": dict(labels or MANAGED_LABELS)},
            "State": state,
        }
        self.reload_calls = 0
        self.reload_error: Exception | None = None

    def reload(self) -> None:
        """Record one fresh inspect request and optionally fail it."""
        self.reload_calls += 1
        if self.reload_error is not None:
            raise self.reload_error


class QueryContainers:
    """Implement only Docker's read-only list and exact-ID get methods."""

    def __init__(self, container: QueryContainer) -> None:
        """Initialize query results, errors, and call records."""
        self.container = container
        self.results: list[QueryContainer] = [container]
        self.list_error: Exception | None = None
        self.get_error: Exception | None = None
        self.list_calls: list[dict[str, Any]] = []
        self.get_calls: list[str] = []

    def list(self, **kwargs: Any) -> list[QueryContainer]:
        """Record one filtered all-container query."""
        self.list_calls.append(kwargs)
        if self.list_error is not None:
            raise self.list_error
        return self.results

    def get(self, container_id: str) -> QueryContainer:
        """Record one exact-ID inspect lookup."""
        self.get_calls.append(container_id)
        if self.get_error is not None:
            raise self.get_error
        return self.container


class QueryClient:
    """Expose a read-only container collection and close tracking."""

    def __init__(self, container: QueryContainer) -> None:
        """Initialize the fake Docker query client."""
        self.containers = QueryContainers(container)
        self.close_calls = 0

    def close(self) -> None:
        """Record transport cleanup."""
        self.close_calls += 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("state", "status", "reason", "message"),
    [
        (
            {"Status": "created"},
            "starting",
            "container_created",
            "container has been created but is not running",
        ),
        (
            {"Status": "restarting"},
            "starting",
            "container_restarting",
            "container is restarting",
        ),
        (
            {"Status": "running"},
            "running",
            "health_check_not_configured",
            "container is running but no health check is configured",
        ),
        (
            {"Status": "running", "Health": {"Status": "starting"}},
            "starting",
            "health_check_starting",
            "container is running and its health check is starting",
        ),
        (
            {"Status": "running", "Health": {"Status": "healthy"}},
            "ready",
            "health_check_passed",
            "Agent health check passed",
        ),
        (
            {"Status": "running", "Health": {"Status": "unhealthy"}},
            "failed",
            "health_check_failed",
            "Agent health check is failing",
        ),
        (
            {"Status": "paused"},
            "stopped",
            "container_paused",
            "container is paused",
        ),
        (
            {"Status": "exited", "ExitCode": 0},
            "stopped",
            "container_exited",
            "container exited successfully",
        ),
        (
            {"Status": "exited", "ExitCode": 7},
            "failed",
            "container_exited_with_error",
            "container exited with a non-zero status",
        ),
        (
            {"Status": "exited", "ExitCode": 137, "OOMKilled": True},
            "failed",
            "container_oom_killed",
            "container was terminated after exceeding its memory limit",
        ),
        (
            {"Status": "dead"},
            "failed",
            "container_dead",
            "Docker reports that the container is dead",
        ),
    ],
)
async def test_get_maps_docker_and_health_state(
    state: dict[str, Any],
    status: str,
    reason: str,
    message: str,
) -> None:
    """Each Docker and health fact must map to one stable public state."""
    container = QueryContainer(state)
    client = QueryClient(container)

    result = await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)

    assert result == SandboxQueryResult(
        sandbox_id=SANDBOX_ID,
        status=status,
        reason=reason,
        message=message,
    )
    assert client.containers.list_calls == [
        {
            "all": True,
            "filters": {
                "label": [
                    "io.agent-sandbox.managed=true",
                    "io.agent-sandbox.project=agent-sandbox",
                    f"io.agent-sandbox.sandbox-id={SANDBOX_ID}",
                    "io.agent-sandbox.resource=container",
                ]
            },
        }
    ]
    assert client.containers.get_calls == [CONTAINER_ID]
    assert container.reload_calls == 1
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_get_returns_not_found_for_unknown_sandbox() -> None:
    """No matching managed container must be indistinguishable from unknown."""
    client = QueryClient(QueryContainer({"Status": "running"}))
    client.containers.results = []

    with pytest.raises(SandboxNotFound):
        await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)

    assert client.containers.get_calls == []
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_get_hides_container_with_mismatched_ownership_labels() -> None:
    """A filtered candidate with incomplete ownership must still return unknown."""
    labels = dict(MANAGED_LABELS)
    labels["io.agent-sandbox.project"] = "another-project"
    client = QueryClient(QueryContainer({"Status": "running"}, labels))

    with pytest.raises(SandboxNotFound):
        await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)

    assert client.close_calls == 1


@pytest.mark.anyio
async def test_get_returns_not_found_when_container_disappears_during_inspect() -> None:
    """A container removed after listing must return the stable unknown result."""
    client = QueryClient(QueryContainer({"Status": "running"}))
    client.containers.get_error = NotFound("container disappeared")

    with pytest.raises(SandboxNotFound):
        await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)

    assert client.close_calls == 1


@pytest.mark.anyio
async def test_get_rejects_ambiguous_duplicate_managed_containers() -> None:
    """Multiple owned containers for one ID must not yield an arbitrary result."""
    container = QueryContainer({"Status": "running"})
    client = QueryClient(container)
    client.containers.results = [container, container]

    with pytest.raises(SandboxQueryFailed):
        await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)

    assert client.containers.get_calls == []
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_get_maps_docker_failure_without_exposing_details() -> None:
    """Docker transport failures must become the stable unavailable error."""
    error = DockerException("secret unix:///var/run/docker.sock")
    client = QueryClient(QueryContainer({"Status": "running"}))
    client.containers.list_error = error

    with pytest.raises(DockerUnavailable) as captured:
        await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)

    assert captured.value.__cause__ is error
    assert "secret" not in str(captured.value)
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_new_runtime_instance_queries_existing_docker_state() -> None:
    """A fresh runtime must query the same container without process memory."""
    container = QueryContainer(
        {"Status": "running", "Health": {"Status": "healthy"}}
    )
    first_client = QueryClient(container)
    second_client = QueryClient(container)

    first = await DockerSandboxRuntime(lambda: first_client).get(SANDBOX_ID)
    second = await DockerSandboxRuntime(lambda: second_client).get(SANDBOX_ID)

    assert first == second
    assert second.status == "ready"
    assert first_client.containers.list_calls
    assert second_client.containers.list_calls
