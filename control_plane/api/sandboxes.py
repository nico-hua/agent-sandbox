"""Sandbox lifecycle/query HTTP contracts and sanitized domain error mapping."""

import asyncio
import json
from typing import NoReturn

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse, Response

from control_plane.api.errors import (
    domain_error_response,
    sandbox_error_response as _sandbox_error_response,
)
from control_plane.core.errors import SandboxError
from control_plane.core.models import format_utc_datetime
from control_plane.core.service import SandboxService


async def _request_has_body(request: Request) -> bool:
    """Detect a nonempty request without buffering the remaining stream."""
    async for chunk in request.stream():
        if chunk:
            return True
    return False


def _reject_json_constant(value: str) -> NoReturn:
    """Reject non-JSON numeric tokens even when a later duplicate field would hide them."""
    raise ValueError("nonstandard JSON constant")


def sandbox_router(
    service: SandboxService,
    sandbox_query_timeout: float,
) -> APIRouter:
    """Register lifecycle and query routes with the shared sandbox service."""
    router = APIRouter()

    @router.get("/v1/sandboxes/{sandbox_id}/endpoint")
    async def get_endpoint(sandbox_id: str) -> JSONResponse:
        """Return a verified local entry URL with a bounded read-only lookup."""
        try:
            result = await asyncio.wait_for(
                service.get_endpoint(sandbox_id), timeout=sandbox_query_timeout
            )
        except TimeoutError:
            return _sandbox_error_response(
                504, "sandbox_endpoint_timeout", "sandbox endpoint query timed out"
            )
        except SandboxError as error:
            return domain_error_response(error, endpoint=True)
        return JSONResponse(
            content={"sandbox_id": result.sandbox_id, "url": result.url}
        )

    @router.post("/v1/sandboxes")
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
            result = await service.create()
        except SandboxError as error:
            return domain_error_response(error)
        return JSONResponse(
            status_code=201,
            content={
                "sandbox_id": result.sandbox_id,
                "status": result.status,
                "created_at": format_utc_datetime(result.created_at),
                "expires_at": format_utc_datetime(result.expires_at),
            },
        )

    @router.post("/v1/sandboxes/{sandbox_id}/renew")
    async def renew_sandbox(sandbox_id: str, request: Request) -> JSONResponse:
        """Extend a managed active sandbox through one protected metadata update."""
        try:
            payload = json.loads(await request.body(), parse_constant=_reject_json_constant)
        except (ValueError, RecursionError):
            payload = None
        if (
            not isinstance(payload, dict)
            or set(payload) != {"ttl_seconds"}
            or type(payload["ttl_seconds"]) is not int
            or not 1 <= payload["ttl_seconds"] <= 86400
        ):
            return _sandbox_error_response(
                400, "invalid_request",
                "request body must contain only ttl_seconds as an integer from 1 to 86400",
            )

        try:
            # Renewal can commit after its caller is cancelled. Its Service
            # settles the write; a read-only query deadline cannot wrap it.
            result = await service.renew(sandbox_id, ttl_seconds=payload["ttl_seconds"])
        except SandboxError as error:
            return domain_error_response(error)
        return JSONResponse(content={
            "sandbox_id": result.sandbox_id,
            "expires_at": format_utc_datetime(result.expires_at),
        })

    @router.delete("/v1/sandboxes/{sandbox_id}", status_code=204)
    async def delete_sandbox(
        sandbox_id: str,
        delete_workspace: bool = Query(
            default=False,
            description=(
                "Permanently delete this sandbox's dedicated workspace and all its data. "
                "The workspace is preserved by default."
            ),
        ),
    ) -> Response:
        """Reclaim runtime resources and optionally irreversibly delete workspace data."""
        try:
            await service.delete(sandbox_id, delete_workspace=delete_workspace)
        except SandboxError as error:
            return domain_error_response(error)
        return Response(status_code=204)

    @router.get("/v1/sandboxes/{sandbox_id}")
    async def get_sandbox(sandbox_id: str) -> JSONResponse:
        """Return current Docker and Agent health facts for one sandbox."""
        try:
            result = await asyncio.wait_for(
                service.get(sandbox_id),
                timeout=sandbox_query_timeout,
            )
        except TimeoutError:
            return _sandbox_error_response(
                504,
                "sandbox_query_timeout",
                "sandbox state query timed out",
            )
        except SandboxError as error:
            return domain_error_response(error)
        return JSONResponse(
            content={
                "sandbox_id": result.sandbox_id,
                "status": result.status,
                "reason": result.reason,
                "message": result.message,
                "created_at": format_utc_datetime(result.created_at),
                "expires_at": format_utc_datetime(result.expires_at),
            }
        )

    return router
