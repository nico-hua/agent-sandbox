"""Unit tests for reading managed sandbox state from Docker."""

from datetime import UTC, datetime
from typing import Any

import httpx
import pytest
from docker.errors import DockerException, NotFound

from control_plane.app import create_app
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.runtime.docker import transaction
from control_plane.core.errors import (
    DockerUnavailable,
    SandboxNotFound,
    SandboxQueryFailed,
)
from control_plane.core.models import SandboxQueryResult
from control_plane.core.service import SandboxService
from lifecycle_fakes import FakeLifecycleStore


SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"
CONTAINER_ID = "container-id"
MANAGED_LABELS = {
    "io.agent-sandbox.managed": "true",
    "io.agent-sandbox.project": "agent-sandbox",
    "io.agent-sandbox.sandbox-id": SANDBOX_ID,
    "io.agent-sandbox.transaction-id": "txn_0123456789abcdef0123456789abcdef",
    "io.agent-sandbox.resource": "container",
}
CREATED_AT_LABEL = "io.agent-sandbox.created-at"
EXPIRES_AT_LABEL = "io.agent-sandbox.expires-at"


class QueryContainer:
    """Represent one inspected Docker container without mutation methods."""

    def __init__(
        self,
        state: dict[str, Any],
        labels: dict[str, Any] | None = None,
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
        roles = [value for value in kwargs["filters"]["label"] if value.startswith("io.agent-sandbox.resource=")]
        return self.results if roles == ["io.agent-sandbox.resource=container"] else []

    def get(self, container_id: str) -> QueryContainer:
        """Record one exact-ID inspect lookup."""
        self.get_calls.append(container_id)
        if self.get_error is not None:
            raise self.get_error
        return self.container


class EmptyQueryResources:
    """Expose missing legacy networks and workspace without any daemon operations."""

    def list(self, **kwargs: Any) -> list[Any]:
        """Return no resources for each complete ownership-filtered inventory scan."""
        return []


class QueryClient:
    """Expose a read-only container collection and close tracking."""

    def __init__(self, container: QueryContainer) -> None:
        """Initialize the fake Docker query client."""
        self.containers = QueryContainers(container)
        self.networks = EmptyQueryResources()
        self.volumes = EmptyQueryResources()
        self.close_calls = 0

    def close(self) -> None:
        """Record transport cleanup."""
        self.close_calls += 1


@pytest.mark.anyio
@pytest.mark.parametrize("scenario", ["not_found", "unavailable", "malformed"])
async def test_query_close_failure_preserves_primary_error(scenario: str) -> None:
    """Transport cleanup must not replace an already classified query failure."""
    client = QueryClient(QueryContainer({"Status": "invalid"}))
    expected = SandboxQueryFailed
    if scenario == "not_found":
        client.containers.results = []
        expected = SandboxNotFound
    elif scenario == "unavailable":
        client.containers.list_error = DockerException("private daemon details")
        expected = DockerUnavailable

    def fail_close() -> None:
        """Fail finalization after counting the attempt."""
        client.close_calls += 1
        raise RuntimeError("private close details")

    client.close = fail_close
    with pytest.raises(expected):
        await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_query_close_only_failure_is_sanitized() -> None:
    """A successful read followed by failed cleanup still has a stable domain error."""
    client = QueryClient(QueryContainer({"Status": "running"}))
    failure = RuntimeError("private close details")

    def fail_close() -> None:
        """Expose one synthetic finalization failure."""
        client.close_calls += 1
        raise failure

    client.close = fail_close
    with pytest.raises(SandboxQueryFailed) as captured:
        await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)
    assert captured.value.__cause__ is failure
    assert "private" not in str(captured.value)
    assert client.close_calls == 1


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
        transaction_id=MANAGED_LABELS["io.agent-sandbox.transaction-id"],
    )
    assert client.containers.list_calls[:1] == [
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
    assert client.containers.get_calls == [CONTAINER_ID, CONTAINER_ID]
    assert container.reload_calls == 2
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


@pytest.mark.anyio
async def test_default_query_factory_uses_five_second_timeout_and_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Default queries must remain lazy, bounded and read-only with client cleanup."""
    client = QueryClient(QueryContainer({"Status": "running"}))
    calls: list[dict[str, object]] = []

    def from_env(**kwargs: object) -> QueryClient:
        """Capture SDK transport arguments without accessing Docker."""
        calls.append(kwargs)
        return client

    monkeypatch.setattr(transaction.docker, "from_env", from_env)
    runtime = DockerSandboxRuntime()
    assert calls == []

    result = await runtime.get(SANDBOX_ID)

    assert result.status == "running"
    assert calls == [{"timeout": 5.0}]
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("created_label", "expires_label", "created_json", "expires_json"),
    [
        (
            "2020-01-01T08:00:00Z",
            "2020-01-01T08:30:00Z",
            "2020-01-01T08:00:00Z",
            "2020-01-01T08:30:00Z",
        ),
        (
            "2020-01-01T08:00:00Z",
            "2020-01-01T08:00:00Z",
            "2020-01-01T08:00:00Z",
            "2020-01-01T08:00:00Z",
        ),
        (
            "2020-01-01T08:00:00.123Z",
            "2020-01-01T08:30:00.123Z",
            "2020-01-01T08:00:00.123000Z",
            "2020-01-01T08:30:00.123000Z",
        ),
        (
            "2020-01-01T08:00:00.123456Z",
            "2020-01-01T08:30:00.123456Z",
            "2020-01-01T08:00:00.123456Z",
            "2020-01-01T08:30:00.123456Z",
        ),
        (
            "2020-01-01T08:00:00+00:00",
            "2020-01-01T08:30:00+00:00",
            "2020-01-01T08:00:00Z",
            "2020-01-01T08:30:00Z",
        ),
    ],
)
async def test_get_restores_elapsed_ttl_and_serializes_utc_without_state_changes(
    created_label: str,
    expires_label: str,
    created_json: str,
    expires_json: str,
) -> None:
    """Persisted times, including elapsed TTL and precision, must survive HTTP reads."""
    labels = {
        **MANAGED_LABELS,
        CREATED_AT_LABEL: created_label,
        EXPIRES_AT_LABEL: expires_label,
    }
    container = QueryContainer(
        {"Status": "running", "Health": {"Status": "healthy"}}, labels
    )
    client = QueryClient(container)
    runtime = DockerSandboxRuntime(lambda: client)

    result = await runtime.get(SANDBOX_ID)
    transport = httpx.ASGITransport(app=create_app(runtime, store=FakeLifecycleStore(), reaper_enabled=False))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        response = await http.get(f"/v1/sandboxes/{SANDBOX_ID}")

    assert result.created_at == datetime.fromisoformat(created_label)
    assert result.expires_at == datetime.fromisoformat(expires_label)
    assert result.created_at.tzinfo == UTC
    assert result.expires_at.tzinfo == UTC
    assert result.status == "ready"
    assert response.status_code == 200
    assert response.json() == {
        "sandbox_id": SANDBOX_ID,
        "status": "ready",
        "reason": "health_check_passed",
        "message": "Agent health check passed",
        "created_at": created_json,
        "expires_at": expires_json,
    }
    assert container.attrs["Config"]["Labels"] == labels
    assert client.close_calls == 2


@pytest.mark.anyio
async def test_get_legacy_container_returns_null_times_without_label_updates() -> None:
    """An owned legacy container must remain queryable without adopting a new TTL."""
    container = QueryContainer(
        {"Status": "running", "Health": {"Status": "healthy"}}
    )
    client = QueryClient(container)
    runtime = DockerSandboxRuntime(lambda: client)

    result = await runtime.get(SANDBOX_ID)
    transport = httpx.ASGITransport(app=create_app(runtime, store=FakeLifecycleStore(), reaper_enabled=False))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        response = await http.get(f"/v1/sandboxes/{SANDBOX_ID}")

    assert result.created_at is None
    assert result.expires_at is None
    assert response.status_code == 200
    assert response.json()["created_at"] is None
    assert response.json()["expires_at"] is None
    assert response.json()["status"] == "ready"
    assert container.attrs["Config"]["Labels"] == MANAGED_LABELS
    assert client.close_calls == 2


@pytest.mark.anyio
async def test_new_service_and_runtime_restore_original_times_without_clock_reads() -> None:
    """Rebuilding both layers must restore Docker metadata without advancing TTL."""
    labels = {
        **MANAGED_LABELS,
        CREATED_AT_LABEL: "2020-01-01T08:00:00.123456Z",
        EXPIRES_AT_LABEL: "2020-01-01T08:30:00.123456Z",
    }
    container = QueryContainer(
        {"Status": "running", "Health": {"Status": "healthy"}}, labels
    )
    first_client = QueryClient(container)
    second_client = QueryClient(container)

    def forbidden_clock() -> datetime:
        """Fail if either query attempts to establish a new lifecycle clock."""
        raise AssertionError("query must not read the creation clock")

    first_service = SandboxService(
        DockerSandboxRuntime(lambda: first_client), FakeLifecycleStore(),
        clock=forbidden_clock,
    )
    first = await first_service.get(SANDBOX_ID)
    second_service = SandboxService(
        DockerSandboxRuntime(lambda: second_client), FakeLifecycleStore(),
        clock=forbidden_clock,
    )
    second = await second_service.get(SANDBOX_ID)

    assert first == second
    assert second.created_at == datetime(2020, 1, 1, 8, 0, 0, 123456, tzinfo=UTC)
    assert second.expires_at == datetime(2020, 1, 1, 8, 30, 0, 123456, tzinfo=UTC)
    assert second.status == "ready"
    assert container.attrs["Config"]["Labels"] == labels
    assert first_client.close_calls == second_client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    "time_labels",
    [
        {CREATED_AT_LABEL: "2020-01-01T08:00:00Z"},
        {EXPIRES_AT_LABEL: "2020-01-01T08:30:00Z"},
        {CREATED_AT_LABEL: "", EXPIRES_AT_LABEL: ""},
        {CREATED_AT_LABEL: None, EXPIRES_AT_LABEL: None},
        {CREATED_AT_LABEL: 123, EXPIRES_AT_LABEL: "2020-01-01T08:30:00Z"},
        {CREATED_AT_LABEL: "2020-01-01T08:00:00Z", EXPIRES_AT_LABEL: 456},
        {CREATED_AT_LABEL: "secret invalid date", EXPIRES_AT_LABEL: "2020-01-01T08:30:00Z"},
        {CREATED_AT_LABEL: "2020-01-01T08:00:00Z", EXPIRES_AT_LABEL: "secret invalid date"},
        {CREATED_AT_LABEL: "2020-02-30T08:00:00Z", EXPIRES_AT_LABEL: "2020-03-01T08:30:00Z"},
        {CREATED_AT_LABEL: "2020-01-01T08:00:00", EXPIRES_AT_LABEL: "2020-01-01T08:30:00Z"},
        {CREATED_AT_LABEL: "2020-01-01T08:00:00Z", EXPIRES_AT_LABEL: "2020-01-01T08:30:00"},
        {CREATED_AT_LABEL: "2020-01-01T08:00:00+01:00", EXPIRES_AT_LABEL: "2020-01-01T08:30:00Z"},
        {CREATED_AT_LABEL: "2020-01-01T08:00:00Z", EXPIRES_AT_LABEL: "2020-01-01T08:30:00-01:00"},
        {CREATED_AT_LABEL: "2020-01-01T08:00:00-00:00", EXPIRES_AT_LABEL: "2020-01-01T08:30:00Z"},
        {CREATED_AT_LABEL: "2020-01-01 08:00:00Z", EXPIRES_AT_LABEL: "2020-01-01T08:30:00Z"},
        {CREATED_AT_LABEL: "2020-01-01T08:00:00.1234567Z", EXPIRES_AT_LABEL: "2020-01-01T08:30:00Z"},
        {CREATED_AT_LABEL: "2020-01-01T08:30:00Z", EXPIRES_AT_LABEL: "2020-01-01T08:00:00Z"},
    ],
    ids=[
        "missing-expiry", "missing-creation", "empty", "null", "numeric-creation",
        "numeric-expiry", "malformed-creation", "malformed-expiry", "invalid-date",
        "naive-creation", "naive-expiry", "non-utc-creation", "non-utc-expiry",
        "unknown-offset", "space-separator", "excess-precision", "reversed-times",
    ],
)
async def test_get_invalid_time_labels_return_stable_sanitized_query_error(
    time_labels: dict[str, Any],
) -> None:
    """Partial or malformed lifecycle labels must fail through the real query API."""
    labels = {**MANAGED_LABELS, **time_labels}
    container = QueryContainer(
        {"Status": "running", "Health": {"Status": "healthy"}}, labels
    )
    client = QueryClient(container)
    runtime = DockerSandboxRuntime(lambda: client)

    with pytest.raises(SandboxQueryFailed):
        await runtime.get(SANDBOX_ID)
    transport = httpx.ASGITransport(app=create_app(runtime, store=FakeLifecycleStore(), reaper_enabled=False))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as http:
        response = await http.get(f"/v1/sandboxes/{SANDBOX_ID}")

    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "sandbox_query_failed",
            "message": "sandbox state could not be determined",
        }
    }
    assert container.attrs["Config"]["Labels"] == labels
    assert client.close_calls == 2
