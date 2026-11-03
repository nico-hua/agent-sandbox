"""Independent persistent workspace HTTP contracts over the shared Service."""

import asyncio
import json

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, Response

from control_plane.api.errors import domain_error_response, sandbox_error_response
from control_plane.api.sandboxes import _CREATE_BODY_LIMIT, _creation_object, _reject_json_constant
from control_plane.core.errors import SandboxError
from control_plane.core.models import SandboxCreateOptions, WorkspaceRecord, format_utc_datetime
from control_plane.core.service import SandboxService


def _workspace_response(record: WorkspaceRecord, *, status_code: int = 200) -> JSONResponse:
    """Expose public identity and occupancy without tokens, image IDs or Docker names."""
    status = record.management_state
    if status == "available" and record.sandbox_id is not None:
        status = "occupied"
    return JSONResponse(status_code=status_code, content={
        "workspace_id": record.workspace_id,
        "status": status,
        "sandbox_id": record.sandbox_id,
        "created_at": format_utc_datetime(record.created_at),
    })


def workspace_router(service: SandboxService, workspace_query_timeout: float) -> APIRouter:
    """Register bounded read-only lookup and Service-managed workspace mutations."""
    router = APIRouter()

    @router.post("/v1/workspaces")
    async def create_workspace(request: Request) -> JSONResponse:
        """Strictly validate the bounded environment-only body before allocating identity."""
        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > _CREATE_BODY_LIMIT:
                return sandbox_error_response(
                    413, "invalid_request", "creation request body must not exceed 4096 bytes",
                )
            body.extend(chunk)
        try:
            payload = json.loads(
                body, parse_constant=_reject_json_constant, object_pairs_hook=_creation_object,
            ) if body else {}
            if not isinstance(payload, dict) or set(payload) - {"environment"}:
                raise ValueError("invalid workspace creation object")
            environment = SandboxCreateOptions(environment=payload.get("environment", "basic")).environment
        except (ValueError, RecursionError):
            return sandbox_error_response(400, "invalid_request", "invalid workspace creation options")
        try:
            record = await service.create_workspace(environment=environment)
        except SandboxError as error:
            return domain_error_response(error)
        return _workspace_response(record, status_code=201)

    @router.get("/v1/workspaces/{workspace_id}")
    async def get_workspace(workspace_id: str) -> JSONResponse:
        """Query persisted identity and verified runtime facts within one read deadline."""
        try:
            record = await asyncio.wait_for(
                service.get_workspace(workspace_id), timeout=workspace_query_timeout,
            )
        except TimeoutError:
            return sandbox_error_response(504, "workspace_query_timeout", "workspace query timed out")
        except SandboxError as error:
            return domain_error_response(error)
        return _workspace_response(record)

    @router.delete("/v1/workspaces/{workspace_id}", status_code=204)
    async def delete_workspace(workspace_id: str) -> Response:
        """Delete an unoccupied workspace only after Service settles runtime and metadata work."""
        try:
            await service.delete_workspace(workspace_id)
        except SandboxError as error:
            return domain_error_response(error)
        return Response(status_code=204)

    return router
