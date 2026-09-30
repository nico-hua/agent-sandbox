"""FastAPI application for the host-side control plane."""

import asyncio

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from control.docker_runtime import DockerRuntimeProbe, RuntimeProbe


UNAVAILABLE_RESPONSE = {
    "status": "unavailable",
    "error": {
        "code": "docker_unavailable",
        "message": "Docker runtime is unavailable",
    },
}


def create_app(
    probe: RuntimeProbe | None = None,
    readiness_timeout: float = 1.0,
) -> FastAPI:
    """Create the control-plane HTTP application with an injected runtime probe."""
    application = FastAPI()
    runtime_probe = probe if probe is not None else DockerRuntimeProbe()

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

    return application


app = create_app()
