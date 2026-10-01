"""HTTP contract tests for the control-plane application."""

import asyncio
import threading

import httpx
import pytest
from fastapi import FastAPI

import control.app as app_module
from control import docker_runtime
from control.app import create_app
from control.docker_runtime import DockerRuntimeProbe
from control.sandbox_service import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxCreateResult,
    SandboxImageUnavailable,
    SandboxResourceConflict,
)


class SuccessfulProbe:
    """Record successful Docker readiness probes."""

    def __init__(self) -> None:
        """Initialize an unused probe."""
        self.calls = 0

    async def ping(self) -> None:
        """Record one successful readiness probe."""
        self.calls += 1


class FailingProbe:
    """Raise a sensitive-looking runtime initialization error."""

    def __init__(self) -> None:
        """Initialize an unused failing probe."""
        self.calls = 0

    async def ping(self) -> None:
        """Raise an error that must never reach the HTTP response."""
        self.calls += 1
        raise RuntimeError(
            "DOCKER_HOST=unix:///var/run/docker.sock secret-environment-value"
        )


class BlockingProbe:
    """Block until the application cancels an expired readiness probe."""

    def __init__(self) -> None:
        """Initialize probe state used to observe cancellation."""
        self.calls = 0
        self.cancelled = False

    async def ping(self) -> None:
        """Wait indefinitely and record cancellation by the deadline."""
        self.calls += 1
        event = asyncio.Event()
        try:
            await event.wait()
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


class RecordingSandboxCreator:
    """Record sandbox creation calls and return a fixed result."""

    def __init__(self, error: BaseException | None = None) -> None:
        """Initialize a fixed result and an optional creation error."""
        self.calls = 0
        self.error = error
        self.result = SandboxCreateResult(
            sandbox_id="sbx_0123456789abcdef0123456789abcdef",
            status="started",
        )

    async def create(self) -> SandboxCreateResult:
        """Record one call and return the configured result."""
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.result


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
    probe = SuccessfulProbe()
    transport = httpx.ASGITransport(app=create_app(probe))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
    assert probe.calls == 0


@pytest.mark.anyio
async def test_readyz_reports_success_after_one_probe() -> None:
    """The readiness endpoint must report a successful Docker ping."""
    probe = SuccessfulProbe()
    transport = httpx.ASGITransport(app=create_app(probe))

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert probe.calls == 1


@pytest.mark.anyio
async def test_health_routes_reject_post_without_probing() -> None:
    """Unsupported methods must return 405 without touching Docker."""
    probe = SuccessfulProbe()
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
    probe = FailingProbe()
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
    probe = BlockingProbe()
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
    probe = DockerRuntimeProbe(lambda: client)
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
    creator = RecordingSandboxCreator()
    transport = httpx.ASGITransport(
        app=create_app(SuccessfulProbe(), sandbox_creator=creator)
    )

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/sandboxes")

    assert response.status_code == 201
    assert response.json() == {
        "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
        "status": "started",
    }
    assert creator.calls == 1


@pytest.mark.anyio
async def test_create_sandbox_passes_no_client_configuration_to_creator() -> None:
    """The HTTP boundary must invoke a zero-argument fixed-policy creator."""
    creator = RecordingSandboxCreator()
    transport = httpx.ASGITransport(
        app=create_app(SuccessfulProbe(), sandbox_creator=creator)
    )

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post("/v1/sandboxes")

    assert response.status_code == 201
    assert creator.calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("body", [b"{}", b" "])
async def test_nonempty_json_or_whitespace_body_returns_400_without_calling_creator(
    body: bytes,
) -> None:
    """Any nonempty body must be rejected before sandbox creation."""
    creator = RecordingSandboxCreator()
    transport = httpx.ASGITransport(
        app=create_app(SuccessfulProbe(), sandbox_creator=creator)
    )

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
    creator = RecordingSandboxCreator()
    stream = TwoChunkStream()
    transport = httpx.ASGITransport(
        app=create_app(SuccessfulProbe(), sandbox_creator=creator)
    )

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
    creator = RecordingSandboxCreator()
    transport = httpx.ASGITransport(
        app=create_app(SuccessfulProbe(), sandbox_creator=creator)
    )

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
    creator = RecordingSandboxCreator(error)
    transport = httpx.ASGITransport(
        app=create_app(SuccessfulProbe(), sandbox_creator=creator)
    )

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
    creator = RecordingSandboxCreator(
        SandboxCleanupFailed("sbx_0123456789abcdef0123456789abcdef")
    )
    transport = httpx.ASGITransport(
        app=create_app(SuccessfulProbe(), sandbox_creator=creator)
    )

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
    creator = RecordingSandboxCreator(asyncio.CancelledError())
    transport = httpx.ASGITransport(
        app=create_app(SuccessfulProbe(), sandbox_creator=creator)
    )

    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        with pytest.raises(asyncio.CancelledError):
            await client.post("/v1/sandboxes")
