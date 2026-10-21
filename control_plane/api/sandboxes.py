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
from control_plane.core.errors import SandboxError, SandboxReadinessTimeout
from control_plane.core.models import SandboxCreateOptions, SandboxResources, format_utc_datetime
from control_plane.core.service import SandboxService


_CREATE_BODY_LIMIT = 4096


def _creation_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate JSON keys rather than hiding an explicitly invalid earlier value."""
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate creation field")
        result[key] = value
    return result


def _creation_options(body: bytes | bytearray) -> SandboxCreateOptions:
    """Check JSON shape and permitted fields, delegating numeric policy to core models."""
    payload = json.loads(
        body, parse_constant=_reject_json_constant, object_pairs_hook=_creation_object,
    ) if body else {}
    if not isinstance(payload, dict) or set(payload) - {"ttl_seconds", "resources"}:
        raise ValueError("invalid creation object")
    resources = payload.get("resources", {})
    if not isinstance(resources, dict) or set(resources) - {"cpu_millis", "memory_mib"}:
        raise ValueError("invalid resources object")
    return SandboxCreateOptions(
        **{key: value for key, value in payload.items() if key != "resources"},
        resources=SandboxResources(**resources),
    )


def _reject_json_constant(value: str) -> NoReturn:
    """Reject non-JSON numeric tokens even when a later duplicate field would hide them."""
    raise ValueError("nonstandard JSON constant")


def sandbox_router(
    service: SandboxService,
    sandbox_query_timeout: float,
) -> APIRouter:
    """Register routes with read-only deadlines and Service-managed cancellation for writes."""
    router = APIRouter()

    @router.get("/v1/sandboxes/{sandbox_id}/readyz")
    async def check_sandbox_ready(sandbox_id: str) -> JSONResponse:
        """Bound the complete read-only state, endpoint and Agent HTTP check with one deadline."""
        deadline = asyncio.timeout(sandbox_query_timeout)
        try:
            async with deadline:
                result = await service.check_ready(sandbox_id)
        except TimeoutError:
            return domain_error_response(SandboxReadinessTimeout())
        except SandboxError as error:
            # HTTP cleanup can finish after cancellation; an elapsed API deadline
            # still owns the response even if cleanup reports a domain failure.
            if deadline.expired():
                return domain_error_response(SandboxReadinessTimeout())
            return domain_error_response(error)
        return JSONResponse(content={"sandbox_id": result.sandbox_id, "status": result.status})

    @router.get("/v1/sandboxes/{sandbox_id}/endpoint")
    async def get_endpoint(sandbox_id: str) -> JSONResponse:
        """Return a verified local entry within a read-only deadline without implying Agent readiness."""
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
        """Bound actual body bytes and validate creation options before any lifecycle work."""
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > _CREATE_BODY_LIMIT:
                return _sandbox_error_response(
                    413, "invalid_request", "creation request body must not exceed 4096 bytes",
                )
            body.extend(chunk)
        try:
            options = _creation_options(body)
        except (ValueError, RecursionError):
            return _sandbox_error_response(
                400, "invalid_request", "invalid sandbox creation options",
            )

        try:
            result = await service.create(options)
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
        """Strictly validate ttl_seconds and renew through a guarded metadata update."""
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
            # Renewal may commit after cancellation; Service settles it without a read-only deadline.
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
        """Reclaim resources, deleting workspace data only with explicit delete_workspace=true."""
        try:
            await service.delete(sandbox_id, delete_workspace=delete_workspace)
        except SandboxError as error:
            return domain_error_response(error)
        return Response(status_code=204)

    @router.get("/v1/sandboxes/{sandbox_id}")
    async def get_sandbox(sandbox_id: str) -> JSONResponse:
        """Return Docker and Agent health facts with merged lifecycle times within a read-only deadline."""
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
