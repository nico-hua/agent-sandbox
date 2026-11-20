"""Sandbox lifecycle/query HTTP contracts and sanitized domain error mapping."""

import asyncio
import json
import re
from typing import Annotated, NoReturn

from fastapi import APIRouter, Path, Query, Request
from fastapi.responses import JSONResponse

from control_plane.api.errors import domain_error_response
from control_plane.api.responses import error_response, success_response
from control_plane.domain.errors import SandboxError, SandboxReadinessTimeout, SandboxListTimeout, SandboxServiceEndpointTimeout
from control_plane.domain.models import (
    SandboxCreateOptions,
    SandboxNetwork,
    SandboxResources,
    format_utc_datetime,
)
from control_plane.services.sandbox_service import SandboxService


_CREATE_BODY_LIMIT = 4096
SandboxID = Annotated[str, Path(pattern=r"^sbx_[0-9a-f]{32}$")]


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
    if not isinstance(payload, dict) or set(payload) - {"environment", "ttl_seconds", "resources", "network", "workspace_id"}:
        raise ValueError("invalid creation object")
    if "workspace_id" in payload and payload["workspace_id"] is None:
        raise ValueError("workspace_id must be omitted or a valid workspace ID")
    resources = payload.get("resources", {})
    if not isinstance(resources, dict) or set(resources) - {"cpu_millis", "memory_mib"}:
        raise ValueError("invalid resources object")
    network = payload.get("network", {})
    if not isinstance(network, dict) or set(network) - {"allow_domains"}:
        raise ValueError("invalid network object")
    return SandboxCreateOptions(
        **{key: value for key, value in payload.items() if key not in ("resources", "network")},
        resources=SandboxResources(**resources),
        network=SandboxNetwork(**network),
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
    async def check_sandbox_ready(sandbox_id: SandboxID) -> JSONResponse:
        """Bound the complete read-only state, endpoint and Agent HTTP check with one deadline."""
        deadline = asyncio.timeout(sandbox_query_timeout)
        try:
            async with deadline:
                result = await service.check_ready(sandbox_id)
        except TimeoutError:
            return domain_error_response(SandboxReadinessTimeout(), readiness=True)
        except SandboxError as error:
            # HTTP cleanup can finish after cancellation; an elapsed API deadline
            # still owns the response even if cleanup reports a domain failure.
            if deadline.expired():
                return domain_error_response(SandboxReadinessTimeout(), readiness=True)
            return domain_error_response(error, readiness=True)
        return success_response({"sandbox_id": result.sandbox_id, "status": result.status})

    @router.get("/v1/sandboxes/{sandbox_id}/services/{port}/endpoint")
    async def get_service_endpoint(sandbox_id: SandboxID, port: str) -> JSONResponse:
        """Deliver one authenticated application route within the existing read-only deadline."""
        try:
            if re.fullmatch(r"[1-9][0-9]{3,4}", port) is None:
                raise ValueError()
            result = await asyncio.wait_for(
                service.get_service_endpoint(sandbox_id, int(port)), timeout=sandbox_query_timeout,
            )
        except ValueError:
            response = error_response(400, "invalid service port")
        except TimeoutError:
            response = domain_error_response(SandboxServiceEndpointTimeout())
        except SandboxError as error:
            response = domain_error_response(error, endpoint=True)
        else:
            response = success_response({
                "sandbox_id": result.sandbox_id, "port": result.port, "url": result.url,
                "auth_required": result.auth_required, "access_token": result.access_token,
            })
        response.headers["Cache-Control"] = "no-store"
        return response

    @router.get("/v1/sandboxes/{sandbox_id}/endpoint")
    async def get_endpoint(sandbox_id: SandboxID) -> JSONResponse:
        """Return a verified local entry within a read-only deadline without implying Agent readiness."""
        try:
            result = await asyncio.wait_for(
                service.get_endpoint(sandbox_id), timeout=sandbox_query_timeout
            )
        except TimeoutError:
            response = error_response(
                504, "sandbox endpoint query timed out"
            )
        except SandboxError as error:
            response = domain_error_response(error, endpoint=True)
        else:
            response = success_response({
                "sandbox_id": result.sandbox_id, "url": result.url,
                "auth_required": result.auth_required, "access_token": result.access_token,
            })
        response.headers["Cache-Control"] = "no-store"
        return response

    @router.get("/v1/sandboxes")
    async def list_sandboxes(request: Request) -> JSONResponse:
        """Read one authenticated, SQL-bounded management page under the query deadline."""
        try:
            pairs = list(request.query_params.multi_items())
            if any(key not in ("limit", "cursor") for key, _ in pairs) or len({key for key, _ in pairs}) != len(pairs):
                raise ValueError()
            parameters = dict(pairs)
            raw_limit = parameters.get("limit", "20")
            if len(raw_limit) > 20 or re.fullmatch(r"[0-9]+", raw_limit) is None:
                raise ValueError()
            limit = int(raw_limit)
            # Service validates the cursor and range before any storage read.
            result = await asyncio.wait_for(
                service.list_sandboxes(limit=limit, cursor=parameters.get("cursor")),
                timeout=sandbox_query_timeout,
            )
        except ValueError:
            response = error_response(400, "invalid sandbox list parameters")
        except TimeoutError:
            response = domain_error_response(SandboxListTimeout())
        except SandboxError as error:
            response = domain_error_response(error)
        else:
            response = success_response({
                "items": [{
                    "sandbox_id": item.sandbox_id, "management_state": item.management_state,
                    "created_at": format_utc_datetime(item.created_at),
                    "expires_at": format_utc_datetime(item.expires_at), "workspace_id": item.workspace_id,
                } for item in result.items],
                "next_cursor": result.next_cursor, "managed_count": result.managed_count,
                "max_instances": result.max_instances,
            })
        response.headers["Cache-Control"] = "no-store"
        return response

    @router.post("/v1/sandboxes")
    async def create_sandbox(request: Request) -> JSONResponse:
        """Bound actual body bytes and validate creation options before any lifecycle work."""
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > _CREATE_BODY_LIMIT:
                return error_response(
                    413, "creation request body must not exceed 4096 bytes",
                )
            body.extend(chunk)
        try:
            options = _creation_options(body)
        except (ValueError, RecursionError):
            return error_response(
                400, "invalid sandbox creation options",
            )

        try:
            result = await service.create(options)
        except SandboxError as error:
            return domain_error_response(error)
        return success_response(
            {
                "sandbox_id": result.sandbox_id,
                "status": result.status,
                "created_at": format_utc_datetime(result.created_at),
                "expires_at": format_utc_datetime(result.expires_at),
                **({"workspace_id": result.workspace_id} if result.workspace_id is not None else {}),
            },
        )

    @router.post("/v1/sandboxes/{sandbox_id}/pause")
    async def pause_sandbox(sandbox_id: SandboxID) -> JSONResponse:
        """Freeze an eligible managed instance, leaving mutation cancellation to Service."""
        try:
            await service.pause(sandbox_id)
        except SandboxError as error:
            return domain_error_response(error)
        return success_response()

    @router.post("/v1/sandboxes/{sandbox_id}/resume")
    async def resume_sandbox(sandbox_id: SandboxID) -> JSONResponse:
        """Thaw the same eligible execution environment without promising Agent readiness."""
        try:
            await service.resume(sandbox_id)
        except SandboxError as error:
            return domain_error_response(error)
        return success_response()

    @router.post("/v1/sandboxes/{sandbox_id}/renew")
    async def renew_sandbox(sandbox_id: SandboxID, request: Request) -> JSONResponse:
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
            return error_response(
                400, "request body must contain only ttl_seconds as an integer from 1 to 86400",
            )

        try:
            # Renewal may commit after cancellation; Service settles it without a read-only deadline.
            result = await service.renew(sandbox_id, ttl_seconds=payload["ttl_seconds"])
        except SandboxError as error:
            return domain_error_response(error)
        return success_response({
            "sandbox_id": result.sandbox_id,
            "expires_at": format_utc_datetime(result.expires_at),
        })

    @router.delete("/v1/sandboxes/{sandbox_id}")
    async def delete_sandbox(
        sandbox_id: SandboxID,
        delete_workspace: bool = Query(
            default=False,
            description=(
                "Permanently delete this sandbox's dedicated workspace and all its data. "
                "The workspace is preserved by default."
            ),
        ),
    ) -> JSONResponse:
        """Reclaim resources, deleting workspace data only with explicit delete_workspace=true."""
        try:
            await service.delete(sandbox_id, delete_workspace=delete_workspace)
        except SandboxError as error:
            return domain_error_response(error)
        return success_response()

    @router.get("/v1/sandboxes/{sandbox_id}")
    async def get_sandbox(sandbox_id: SandboxID) -> JSONResponse:
        """Return Docker and Agent health facts with merged lifecycle times within a read-only deadline."""
        try:
            result = await asyncio.wait_for(
                service.get(sandbox_id),
                timeout=sandbox_query_timeout,
            )
        except TimeoutError:
            return error_response(
                504,
                "sandbox state query timed out",
            )
        except SandboxError as error:
            return domain_error_response(error)
        return success_response(
            {
                "sandbox_id": result.sandbox_id,
                "status": result.status,
                "reason": result.reason,
                "message": result.message,
                "created_at": format_utc_datetime(result.created_at),
                "expires_at": format_utc_datetime(result.expires_at),
                **({"workspace_id": result.workspace_id} if result.workspace_id is not None else {}),
            }
        )

    return router
