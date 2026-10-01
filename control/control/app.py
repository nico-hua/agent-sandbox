"""FastAPI application for the host-side control plane."""

import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from control.docker_runtime import DockerRuntimeProbe, RuntimeProbe
from control.sandbox_runtime import DockerSandboxRuntime
from control.sandbox_service import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxCreator,
    SandboxImageUnavailable,
    SandboxResourceConflict,
    SandboxService,
)


UNAVAILABLE_RESPONSE = {
    "status": "unavailable",
    "error": {
        "code": "docker_unavailable",
        "message": "Docker runtime is unavailable",
    },
}


async def _request_has_body(request: Request) -> bool:
    """Detect a nonempty request without buffering the remaining stream."""
    async for chunk in request.stream():
        if chunk:
            return True
    return False


def _sandbox_error_response(
    status_code: int,
    code: str,
    message: str,
    *,
    sandbox_id: str | None = None,
) -> JSONResponse:
    """Build a stable sandbox API error without exposing Docker details."""
    error = {"code": code, "message": message}
    if sandbox_id is not None:
        error["sandbox_id"] = sandbox_id
    return JSONResponse(status_code=status_code, content={"error": error})


def create_app(
    probe: RuntimeProbe | None = None,
    sandbox_creator: SandboxCreator | None = None,
    readiness_timeout: float = 1.0,
) -> FastAPI:
    """Create the control-plane HTTP application with an injected runtime probe."""
    application = FastAPI()
    runtime_probe = probe if probe is not None else DockerRuntimeProbe()
    creator = (
        sandbox_creator
        if sandbox_creator is not None
        else SandboxService(DockerSandboxRuntime())
    )

    @application.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Report the health of the control-plane process."""
        return {"status": "ok"}

    @application.get("/readyz")
    async def readyz() -> JSONResponse:
        """Report readiness after a successful Docker ping."""
        try:
            await asyncio.wait_for(runtime_probe.ping(), timeout=readiness_timeout)
        except Exception:
            return JSONResponse(status_code=503, content=UNAVAILABLE_RESPONSE)
        return JSONResponse(content={"status": "ready"})

    @application.post("/v1/sandboxes")
    async def create_sandbox(request: Request) -> JSONResponse:
        """Create one fixed-policy Docker sandbox from an empty request."""
        if await _request_has_body(request):
            return JSONResponse(
                status_code=400,
                content={
                    "error": {
                        "code": "invalid_request",
                        "message": "request body must be empty",
                    }
                },
            )

        try:
            result = await creator.create()
        except SandboxImageUnavailable:
            return _sandbox_error_response(
                503,
                "sandbox_image_unavailable",
                "sandbox image is unavailable",
            )
        except DockerUnavailable:
            return _sandbox_error_response(
                503,
                "docker_unavailable",
                "Docker runtime is unavailable",
            )
        except SandboxResourceConflict:
            return _sandbox_error_response(
                409,
                "sandbox_resource_conflict",
                "sandbox resource conflict",
            )
        except SandboxCleanupFailed as error:
            return _sandbox_error_response(
                500,
                "sandbox_cleanup_failed",
                "sandbox creation failed and cleanup was incomplete",
                sandbox_id=error.sandbox_id,
            )
        except SandboxCreateFailed:
            return _sandbox_error_response(
                500,
                "sandbox_create_failed",
                "sandbox creation failed",
            )
        return JSONResponse(
            status_code=201,
            content={
                "sandbox_id": result.sandbox_id,
                "status": result.status,
            },
        )

    return application


app = create_app()
