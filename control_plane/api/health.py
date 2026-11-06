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


def health_router(
    service: SandboxService, readiness_timeout: float, *,
    unavailable_error: tuple[str, str] | None = None,
) -> APIRouter:
    """Register backend-independent routes with a fixed failure selected during application assembly."""
    router = APIRouter()
    unavailable_response = UNAVAILABLE_RESPONSE if unavailable_error is None else {
        "status": "unavailable",
        "error": {"code": unavailable_error[0], "message": unavailable_error[1]},
    }

    @router.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Report the health of the control-plane process."""
        return {"status": "ok"}

    @router.get("/readyz")
    async def readyz() -> JSONResponse:
        """Report readiness through Service; wait-for cancellation may include worker cleanup time."""
        try:
            await asyncio.wait_for(service.ping(), timeout=readiness_timeout)
        except Exception:
            return JSONResponse(status_code=503, content=unavailable_response)
        return JSONResponse(content={"status": "ready"})

    return router
