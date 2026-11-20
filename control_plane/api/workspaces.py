"""Independent persistent workspace HTTP contracts over the shared Service."""

import asyncio
import json
from typing import Annotated

from fastapi import APIRouter, Path, Request
from fastapi.responses import JSONResponse

from control_plane.api.errors import domain_error_response
from control_plane.api.responses import error_response, success_response
from control_plane.api.sandboxes import _CREATE_BODY_LIMIT, _creation_object, _reject_json_constant
from control_plane.domain.errors import SandboxEnvironmentNotRegistered, SandboxError
from control_plane.domain.models import SandboxCreateOptions, WorkspaceRecord, format_utc_datetime
from control_plane.services.sandbox_service import SandboxService


WorkspaceID = Annotated[str, Path(pattern=r"^wsp_[0-9a-f]{32}$")]


def _workspace_response(record: WorkspaceRecord) -> JSONResponse:
    """Expose public identity and occupancy without tokens, image IDs or Docker names."""
    status = record.management_state
    if status == "available" and record.sandbox_id is not None:
        status = "occupied"
    return success_response({
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
                return error_response(
                    413, "creation request body must not exceed 4096 bytes",
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
            return error_response(400, "invalid workspace creation options")
        try:
            record = await service.create_workspace(environment=environment)
        except SandboxEnvironmentNotRegistered:
            return error_response(400, "invalid workspace creation options")
        except SandboxError as error:
            return domain_error_response(error)
        return _workspace_response(record)

    @router.get("/v1/workspaces/{workspace_id}")
    async def get_workspace(workspace_id: WorkspaceID) -> JSONResponse:
        """Query persisted identity and verified runtime facts within one read deadline."""
        try:
            record = await asyncio.wait_for(
                service.get_workspace(workspace_id), timeout=workspace_query_timeout,
            )
        except TimeoutError:
            return error_response(504, "workspace query timed out")
        except SandboxError as error:
            return domain_error_response(error)
        return _workspace_response(record)

    @router.delete("/v1/workspaces/{workspace_id}")
    async def delete_workspace(workspace_id: WorkspaceID) -> JSONResponse:
        """Delete an unoccupied workspace only after Service settles runtime and metadata work."""
        try:
            await service.delete_workspace(workspace_id)
        except SandboxError as error:
            return domain_error_response(error)
        return success_response()

    return router
