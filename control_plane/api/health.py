"""Process health and bounded read-only runtime readiness HTTP routes."""

import asyncio

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from control_plane.core.service import SandboxService


UNAVAILABLE_RESPONSE = {
    "status": "unavailable",
    "error": {
        "code": "docker_unavailable",
        "message": "Docker runtime is unavailable",
    },
}


def health_router(service: SandboxService, readiness_timeout: float) -> APIRouter:
    """Register health routes with the shared service and readiness deadline."""
    router = APIRouter()

    @router.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Report the health of the control-plane process."""
        return {"status": "ok"}

    @router.get("/readyz")
    async def readyz() -> JSONResponse:
        """Report readiness after a successful Docker ping."""
        try:
            await asyncio.wait_for(service.ping(), timeout=readiness_timeout)
        except Exception:
            return JSONResponse(status_code=503, content=UNAVAILABLE_RESPONSE)
        return JSONResponse(content={"status": "ready"})

    return router
