"""Process health and bounded read-only runtime readiness HTTP routes."""

import asyncio

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from control_plane.api.responses import error_response, success_response
from control_plane.domain.errors import DockerUnavailable
from control_plane.services.sandbox_service import SandboxService


def health_router(service: SandboxService, readiness_timeout: float) -> APIRouter:
    """Register process health and fixed read-only Docker readiness responses."""
    router = APIRouter()

    @router.get("/healthz")
    async def healthz() -> JSONResponse:
        """Report the health of the control-plane process."""
        return success_response({"status": "ok"})

    @router.get("/readyz")
    async def readyz() -> JSONResponse:
        """Report readiness through Service; wait-for cancellation may include worker cleanup time."""
        try:
            await asyncio.wait_for(service.ping(), timeout=readiness_timeout)
        except TimeoutError:
            return error_response(504, "Docker readiness check timed out")
        except DockerUnavailable:
            return error_response(503, "Docker runtime is unavailable")
        return success_response({"status": "ready"})

    return router
