"""HTTP contract tests for the control-plane application."""

import asyncio
import subprocess
import sys
import threading
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI

import control_plane.app as app_module
from control_plane.runtime.docker import docker_runtime
from control_plane.app import create_app
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.core.service import SandboxService
import control_plane.core.service as service_module
from control_plane.core.errors import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxImageUnavailable,
    SandboxNotFound,
    SandboxQueryFailed,
    SandboxResourceConflict,
)
from control_plane.core.models import SandboxCreateResult, SandboxQueryResult, SandboxSpec


class FakeRuntime(SandboxRuntime):
    """Record all backend calls and provide deterministic runtime observations."""

    def __init__(self, error: BaseException | None = None) -> None:
        """Initialize call records and an optional domain failure."""
        self.calls = 0
        self.error = error
        self.specs: list[SandboxSpec] = []
        self.sandbox_ids: list[str] = []
        self.result = SandboxQueryResult(
            sandbox_id="sbx_0123456789abcdef0123456789abcdef",
            status="ready",
            reason="health_check_passed",
            message="Agent health check passed",
        )

    async def ping(self) -> None:
        """Record readiness and propagate the configured backend error."""
        self.calls += 1
        if self.error is not None:
            raise self.error

    async def create(self, spec: SandboxSpec) -> None:
        """Record the service-generated specification before creation succeeds."""
        self.calls += 1
        self.specs.append(spec)
        if self.error is not None:
            raise self.error

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Record the validated public ID and return the configured state."""
        self.calls += 1
        self.sandbox_ids.append(sandbox_id)
        if self.error is not None:
            raise self.error
        return self.result


class FailingRuntime(FakeRuntime):
    """Raise a sensitive-looking runtime initialization error on ping."""

    async def ping(self) -> None:
        """Raise an error that must never reach the HTTP response."""
        self.calls += 1
        raise RuntimeError(
            "DOCKER_HOST=unix:///var/run/docker.sock secret-environment-value"
        )


class BlockingPingRuntime(FakeRuntime):
    """Block ping until the application readiness deadline cancels it."""

    def __init__(self) -> None:
        """Initialize call records and readiness cancellation state."""
        super().__init__()
        self.cancelled = False

    async def ping(self) -> None:
        """Wait indefinitely and record cancellation by the deadline."""
        self.calls += 1
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled = True


class LateFailingClient:
    """Block a Docker ping until its HTTP request has already timed out."""

    def __init__(self) -> None:
        """Initialize thread coordination and close-observation state."""
        self.started = threading.Event()
        self.release = threading.Event()
        self.closed = threading.Event()
        self.close_calls = 0

    def ping(self) -> None:
        """Wait for release, then raise a sensitive late Docker error."""
        self.started.set()
        if not self.release.wait(timeout=1):
            raise TimeoutError("test did not release the Docker ping thread")
        raise RuntimeError("late secret from /var/run/docker.sock")

    def close(self) -> None:
        """Record that the timed-out probe eventually closed its client."""
        self.close_calls += 1
        self.closed.set()


class BlockingQueryRuntime(FakeRuntime):
    """Block status lookup until the application query deadline expires."""

    def __init__(self) -> None:
        """Initialize call records and query cancellation state."""
        super().__init__()
        self.cancelled = False

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Wait indefinitely and record cancellation by the deadline."""
        self.calls += 1
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled = True


@pytest.fixture(autouse=True)
def deterministic_service_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep HTTP result assertions deterministic through real service logic."""
    monkeypatch.setattr(
        service_module,
        "_new_sandbox_id",
        lambda: "sbx_0123456789abcdef0123456789abcdef",
    )


class TwoChunkStream(httpx.AsyncByteStream):
    """Expose whether an ASGI handler consumed a second request chunk."""

    def __init__(self) -> None:
        """Initialize second-chunk observation state."""
        self.second_chunk_read = False

    async def __aiter__(self):
        """Yield one invalid byte and record if the next chunk is requested."""
        yield b"x"
        self.second_chunk_read = True
        yield b"unnecessarily consumed"


UNAVAILABLE_RESPONSE = {
    "status": "unavailable",
    "error": {
        "code": "docker_unavailable",
        "message": "Docker runtime is unavailable",
    },
}


@pytest.mark.anyio
async def test_healthz_reports_process_health_without_calling_probe() -> None:
    """The process health endpoint must not depend on Docker."""
    probe = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(probe))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert probe.calls == 0


@pytest.mark.anyio
async def test_readyz_reports_success_after_one_probe() -> None:
    """The readiness endpoint must report a successful Docker ping."""
    probe = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(probe))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert probe.calls == 1


@pytest.mark.anyio
async def test_health_routes_reject_post_without_probing() -> None:
    """Unsupported methods must return 405 without touching Docker."""
    probe = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(probe))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        health_response = await client.post("/healthz")
        ready_response = await client.post("/readyz")

    assert health_response.status_code == 405
    assert ready_response.status_code == 405
    assert probe.calls == 0


@pytest.mark.anyio
async def test_readyz_hides_probe_failure_details() -> None:
    """Readiness failures must return a stable response without host details."""
    probe = FailingRuntime()
    transport = httpx.ASGITransport(app=create_app(probe))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        health_response = await client.get("/healthz")
        ready_response = await client.get("/readyz")

    assert health_response.status_code == 200
    assert health_response.json() == {"status": "ok"}
    assert ready_response.status_code == 503
    assert ready_response.json() == UNAVAILABLE_RESPONSE
    assert probe.calls == 1
    assert "DOCKER_HOST" not in ready_response.text
    assert "/var/run/docker.sock" not in ready_response.text
    assert "secret-environment-value" not in ready_response.text


@pytest.mark.anyio
@pytest.mark.timeout(2)
async def test_readyz_times_out_and_cancels_blocking_probe() -> None:
    """An expired readiness probe must return the same stable 503 response."""
    probe = BlockingPingRuntime()
    transport = httpx.ASGITransport(app=create_app(probe, readiness_timeout=0.01))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        health_response = await client.get("/healthz")
        ready_response = await client.get("/readyz")

    assert health_response.status_code == 200
    assert ready_response.status_code == 503
    assert ready_response.json() == UNAVAILABLE_RESPONSE
    assert probe.calls == 1
    assert probe.cancelled is True


@pytest.mark.anyio
@pytest.mark.timeout(2)
async def test_readyz_timeout_contains_late_thread_failure_and_closes_client() -> None:
    """A late thread failure must stay hidden while its client still closes."""
    client = LateFailingClient()
    probe = DockerSandboxRuntime(ping_client_factory=lambda: client)
    transport = httpx.ASGITransport(app=create_app(probe, readiness_timeout=0.05))
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, object]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))

    try:
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://test",
        ) as http_client:
            request = asyncio.create_task(http_client.get("/readyz"))
            assert await asyncio.to_thread(client.started.wait, 1)
            response = await request

        assert response.status_code == 503
        assert response.json() == UNAVAILABLE_RESPONSE
        assert "late secret" not in response.text
        assert "/var/run/docker.sock" not in response.text

        client.release.set()
        assert await asyncio.to_thread(client.closed.wait, 1)
        await asyncio.sleep(0)

        assert client.close_calls == 1
        assert loop_errors == []
    finally:
        client.release.set()
        loop.set_exception_handler(previous_handler)


def test_new_entrypoint_import_and_construction_do_not_connect_to_docker() -> None:
    """The new entrypoint must import in a fresh process without Docker access."""
    script = '''
import docker
from fastapi import FastAPI

def fail_from_env(**kwargs):
    """Reject any Docker connection during import or application assembly."""
    raise AssertionError("Docker client created during import or assembly")

docker.from_env = fail_from_env
from control_plane.app import app, create_app
assert isinstance(app, FastAPI)
assert isinstance(create_app(), FastAPI)
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr


def test_default_application_construction_is_lazy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default application must not connect to Docker while importing."""
    calls = 0

    def fail_from_env(**_kwargs: object) -> None:
        """Fail if application construction eagerly creates a Docker client."""
        nonlocal calls
        calls += 1
        raise AssertionError("docker.from_env called during application construction")

    monkeypatch.setattr(docker_runtime.docker, "from_env", fail_from_env)

    application = create_app()

    assert isinstance(application, FastAPI)
    assert isinstance(app_module.app, FastAPI)
    assert calls == 0


@pytest.mark.anyio
async def test_create_sandbox_returns_201_id_and_started_status() -> None:
    """An empty creation request must return the fixed public result shape."""
    creator = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(creator))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/sandboxes")

    assert response.status_code == 201
    assert response.json() == {
        "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
        "status": "started",
    }
    assert creator.calls == 1


@pytest.mark.anyio
async def test_create_sandbox_passes_service_generated_spec_to_runtime() -> None:
    """The service must build the fixed resource names before runtime creation."""
    creator = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(creator))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/sandboxes")

    assert response.status_code == 201
    assert creator.calls == 1
    assert creator.specs[0].sandbox_id == response.json()["sandbox_id"]
    assert creator.specs[0].container_name == (
        "agent-sandbox-sbx_0123456789abcdef0123456789abcdef"
    )


@pytest.mark.anyio
@pytest.mark.parametrize("body", [b"{}", b" "])
async def test_nonempty_json_or_whitespace_body_returns_400_without_calling_creator(
    body: bytes,
) -> None:
    """Any nonempty body must be rejected before sandbox creation."""
    creator = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(creator))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", content=body)

    assert response.status_code == 400
    assert response.json() == {
        "error": {
            "code": "invalid_request",
            "message": "request body must be empty",
        }
    }
    assert creator.calls == 0


@pytest.mark.anyio
async def test_chunked_nonempty_body_stops_after_first_nonempty_chunk() -> None:
    """Body validation must reject the first byte without draining the stream."""
    creator = FakeRuntime()
    stream = TwoChunkStream()
    transport = httpx.ASGITransport(app=create_app(creator))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.send(
            client.build_request("POST", "/v1/sandboxes", content=stream)
        )

    assert response.status_code == 400
    assert stream.second_chunk_read is False
    assert creator.calls == 0


@pytest.mark.anyio
async def test_unsupported_sandbox_method_returns_405_without_calling_creator() -> None:
    """Methods other than POST must return 405 without creating a sandbox."""
    creator = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(creator))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/v1/sandboxes")

    assert response.status_code == 405
    assert creator.calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("error", "status_code", "code", "message"),
    [
        (
            SandboxImageUnavailable("secret image path"),
            503,
            "sandbox_image_unavailable",
            "sandbox image is unavailable",
        ),
        (
            DockerUnavailable("DOCKER_HOST=unix:///secret/docker.sock"),
            503,
            "docker_unavailable",
            "Docker runtime is unavailable",
        ),
        (
            SandboxResourceConflict("secret existing resource"),
            409,
            "sandbox_resource_conflict",
            "sandbox resource conflict",
        ),
        (
            SandboxCreateFailed("secret daemon response"),
            500,
            "sandbox_create_failed",
            "sandbox creation failed",
        ),
    ],
)
async def test_create_sandbox_maps_domain_errors_without_sensitive_details(
    error: Exception,
    status_code: int,
    code: str,
    message: str,
) -> None:
    """Creation failures must map to stable responses without raw details."""
    creator = FakeRuntime(error)
    transport = httpx.ASGITransport(app=create_app(creator))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/sandboxes")

    assert response.status_code == status_code
    assert response.json() == {
        "error": {
            "code": code,
            "message": message,
        }
    }
    assert "secret" not in response.text
    assert "DOCKER_HOST" not in response.text
    assert creator.calls == 1


@pytest.mark.anyio
async def test_create_sandbox_reports_cleanup_failure_with_sandbox_id() -> None:
    """Incomplete cleanup must return a stable error and the managed ID."""
    creator = FakeRuntime(
        SandboxCleanupFailed("sbx_0123456789abcdef0123456789abcdef")
    )
    transport = httpx.ASGITransport(app=create_app(creator))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/sandboxes")

    assert response.status_code == 500
    assert response.json() == {
        "error": {
            "code": "sandbox_cleanup_failed",
            "message": "sandbox creation failed and cleanup was incomplete",
            "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
        }
    }


@pytest.mark.anyio
async def test_create_sandbox_propagates_request_cancellation() -> None:
    """A normal request cancellation must not be rewritten as an HTTP error."""
    creator = FakeRuntime(asyncio.CancelledError())
    transport = httpx.ASGITransport(app=create_app(creator))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(asyncio.CancelledError):
            await client.post("/v1/sandboxes")


@pytest.mark.anyio
async def test_get_sandbox_returns_observed_status_reason_and_message() -> None:
    """A managed sandbox observation must retain its complete public meaning."""
    reader = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(reader))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/v1/sandboxes/sbx_0123456789abcdef0123456789abcdef"
        )

    assert response.status_code == 200
    assert response.json() == {
        "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
        "status": "ready",
        "reason": "health_check_passed",
        "message": "Agent health check passed",
    }
    assert reader.sandbox_ids == ["sbx_0123456789abcdef0123456789abcdef"]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("error", "status_code", "code", "message"),
    [
        (
            SandboxNotFound("secret unrelated container"),
            404,
            "sandbox_not_found",
            "sandbox not found",
        ),
        (
            DockerUnavailable("secret unix:///var/run/docker.sock"),
            503,
            "docker_unavailable",
            "Docker runtime is unavailable",
        ),
        (
            SandboxQueryFailed("secret malformed inspect data"),
            500,
            "sandbox_query_failed",
            "sandbox state could not be determined",
        ),
    ],
)
async def test_get_sandbox_maps_query_errors_without_sensitive_details(
    error: Exception,
    status_code: int,
    code: str,
    message: str,
) -> None:
    """Query failures must have stable responses without Docker details."""
    reader = FakeRuntime(error)
    transport = httpx.ASGITransport(app=create_app(reader))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/v1/sandboxes/sbx_0123456789abcdef0123456789abcdef"
        )

    assert response.status_code == status_code
    assert response.json() == {"error": {"code": code, "message": message}}
    assert "secret" not in response.text
    assert "/var/run/docker.sock" not in response.text


@pytest.mark.anyio
@pytest.mark.timeout(2)
async def test_get_sandbox_times_out_and_cancels_blocking_reader() -> None:
    """An expired Docker query must return 504 and cancel its awaitable."""
    reader = BlockingQueryRuntime()
    transport = httpx.ASGITransport(
        app=create_app(
            reader,
            sandbox_query_timeout=0.01,
        )
    )

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(
            "/v1/sandboxes/sbx_0123456789abcdef0123456789abcdef"
        )

    assert response.status_code == 504
    assert response.json() == {
        "error": {
            "code": "sandbox_query_timeout",
            "message": "sandbox state query timed out",
        }
    }
    assert reader.calls == 1
    assert reader.cancelled is True


@pytest.mark.anyio
async def test_get_sandbox_propagates_request_cancellation() -> None:
    """Request cancellation must propagate instead of becoming an API error."""
    reader = FakeRuntime(asyncio.CancelledError())
    transport = httpx.ASGITransport(app=create_app(reader))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(asyncio.CancelledError):
            await client.get(
                "/v1/sandboxes/sbx_0123456789abcdef0123456789abcdef"
            )


@pytest.mark.anyio
async def test_sandbox_create_and_query_use_same_runtime() -> None:
    """The create and query routes must address the same public sandbox ID."""
    runtime = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(runtime))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        created = await client.post("/v1/sandboxes")
        queried = await client.get(
            f"/v1/sandboxes/{created.json()['sandbox_id']}"
        )

    assert created.status_code == 201
    assert queried.status_code == 200
    assert runtime.sandbox_ids == [created.json()["sandbox_id"]]
    assert runtime.specs[0].sandbox_id == created.json()["sandbox_id"]


@pytest.mark.anyio
async def test_post_to_sandbox_item_route_returns_405_without_querying() -> None:
    """The sandbox item route must remain read-only at the HTTP boundary."""
    reader = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(reader))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/v1/sandboxes/sbx_0123456789abcdef0123456789abcdef"
        )

    assert response.status_code == 405
    assert reader.sandbox_ids == []


@pytest.mark.anyio
async def test_runtime_routes_use_the_same_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """All backend routes must enter the same real service; health must bypass it."""
    observed: list[tuple[str, SandboxService]] = []
    original_ping = SandboxService.ping
    original_create = SandboxService.create
    original_get = SandboxService.get

    async def record_ping(service: SandboxService) -> None:
        """Record service entry while keeping real readiness delegation."""
        observed.append(("ping", service))
        await original_ping(service)

    async def record_create(service: SandboxService) -> SandboxCreateResult:
        """Record service entry while keeping specification generation."""
        observed.append(("create", service))
        return await original_create(service)

    async def record_get(
        service: SandboxService, sandbox_id: str
    ) -> SandboxQueryResult:
        """Record service entry while keeping ID validation and query delegation."""
        observed.append(("get", service))
        return await original_get(service, sandbox_id)

    monkeypatch.setattr(SandboxService, "ping", record_ping)
    monkeypatch.setattr(SandboxService, "create", record_create)
    monkeypatch.setattr(SandboxService, "get", record_get)
    runtime = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(runtime))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        health = await client.get("/healthz")
        assert health.status_code == 200
        assert observed == []
        assert runtime.calls == 0
        ready = await client.get("/readyz")
        created = await client.post("/v1/sandboxes")
        queried = await client.get(f"/v1/sandboxes/{created.json()['sandbox_id']}")

    assert ready.status_code == 200
    assert created.status_code == 201
    assert queried.status_code == 200
    assert [method for method, _service in observed] == ["ping", "create", "get"]
    assert all(service is observed[0][1] for _method, service in observed)
    assert runtime.calls == 3
    assert runtime.sandbox_ids == [runtime.specs[0].sandbox_id]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "sandbox_id",
    ["not-a-sandbox-id", "sbx_0123456789abcdef", "sbx_0123456789ABCDEF0123456789ABCDEF"],
)
async def test_get_rejects_invalid_id_before_runtime_lookup(sandbox_id: str) -> None:
    """Invalid public IDs must return 404 through service validation without lookup."""
    runtime = FakeRuntime()
    transport = httpx.ASGITransport(app=create_app(runtime))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/v1/sandboxes/{sandbox_id}")

    assert response.status_code == 404
    assert response.json() == {
        "error": {"code": "sandbox_not_found", "message": "sandbox not found"}
    }
    assert runtime.calls == 0
    assert runtime.sandbox_ids == []


def test_api_and_service_import_without_loading_docker_backend() -> None:
    """The public contract, service and API must import with Docker SDK unavailable."""
    script = '''
import sys
sys.modules["docker"] = None
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.core.service import SandboxService
from control_plane.api.health import health_router
from control_plane.api.sandboxes import sandbox_router
assert "control_plane.runtime.docker" not in sys.modules
assert "control_plane.runtime.docker.docker_runtime" not in sys.modules
'''
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[2],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
