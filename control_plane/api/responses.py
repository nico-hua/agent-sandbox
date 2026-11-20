"""Common JSON HTTP responses for the control-plane transport boundary."""

from collections.abc import Mapping
from http import HTTPStatus

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.routing import iter_route_contexts
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.routing import Match


_CREDENTIAL_ROUTES = frozenset({
    "/v1/sandboxes/{sandbox_id}/endpoint",
    "/v1/sandboxes/{sandbox_id}/services/{port}/endpoint",
})


def success_response(data: object = None) -> JSONResponse:
    """Serialize one successful operation with HTTP 200 and its original business data."""
    return JSONResponse({"code": 200, "message": "ok", "data": data})


def error_response(
    status: int, message: str, *, data: dict[str, str] | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    """Serialize a fixed public failure and preserve relevant protocol headers."""
    return JSONResponse(
        {"code": int(status), "message": message, "data": data},
        status_code=status,
        headers=headers,
    )


def _error_headers(
    request: Request, headers: Mapping[str, str] | None = None,
) -> Mapping[str, str] | None:
    """Apply no-store to framework failures on credential-delivery routes."""
    route = request.scope.get("route")
    if getattr(route, "path", None) not in _CREDENTIAL_ROUTES:
        return headers
    result = {key: value for key, value in (headers or {}).items() if key.lower() != "cache-control"}
    result["Cache-Control"] = "no-store"
    return result


def _method_error_headers(
    request: Request, headers: Mapping[str, str] | None,
) -> Mapping[str, str] | None:
    """Collect legal methods from all matching routes, including included routers."""
    methods: set[str] = set()
    for route in iter_route_contexts(request.app.routes):
        if route.methods and route.matches(request.scope)[0] is not Match.NONE:
            methods.update(route.methods)
    if not methods:
        return headers
    result = {key: value for key, value in (headers or {}).items() if key.lower() != "allow"}
    result["Allow"] = ", ".join(sorted(methods))
    return result


async def http_exception_response(request: Request, error: HTTPException) -> JSONResponse:
    """Sanitize routing and framework failures without losing Allow or authentication headers."""
    status = 400 if error.status_code == 422 else error.status_code
    if status == 404:
        message = "route not found"
    elif status == 400:
        message = "invalid request"
    else:
        message = HTTPStatus(status).phrase.lower()
    headers = error.headers
    if status == 405:
        headers = _method_error_headers(request, headers)
    return error_response(status, message, headers=_error_headers(request, headers))


async def validation_error_response(request: Request, error: RequestValidationError) -> JSONResponse:
    """Avoid transmitting invalid request values or FastAPI's default 422 payload."""
    locations = {tuple(issue["loc"]) for issue in error.errors()}
    message = "invalid request"
    if locations == {("path", "sandbox_id")}:
        message = "invalid sandbox ID"
    elif locations == {("path", "workspace_id")}:
        message = "invalid workspace ID"
    return error_response(400, message, headers=_error_headers(request))


async def internal_error_response(request: Request, _error: Exception) -> JSONResponse:
    """Leave unexpected exception diagnostics on the server side."""
    return error_response(500, "internal server error", headers=_error_headers(request))
