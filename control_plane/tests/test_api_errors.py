"""Protect fixed HTTP error contracts, inherited failures and endpoint overrides."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from fastapi import FastAPI

from control_plane.api.sandboxes import sandbox_router
from control_plane.core import errors as domain_errors
from control_plane.core.errors import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateCapacityExceeded,
    SandboxCreateFailed,
    SandboxCreateUncertain,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxEndpointUnavailable,
    SandboxError,
    SandboxImageUnavailable,
    SandboxMetadataCleanupFailed,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    SandboxNotFound,
    SandboxQueryFailed,
    SandboxRenewConflict,
    SandboxResourceConflict,
)

SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"


@pytest.fixture
def anyio_backend() -> str:
    """Keep the HTTP coroutine tests on the application's asyncio backend."""
    return "asyncio"


async def _request_error(operation: str, error: Exception) -> httpx.Response:
    """Exercise the real router with a service failure and no backend side effects."""
    service = SimpleNamespace(**{
        name: AsyncMock(side_effect=error)
        for name in ("create", "get", "get_endpoint", "renew", "delete", "pause", "resume")
    })
    app = FastAPI()
    app.include_router(sandbox_router(service, sandbox_query_timeout=1))
    requests = {
        "create": ("POST", "/v1/sandboxes", {}),
        "get": ("GET", f"/v1/sandboxes/{SANDBOX_ID}", {}),
        "get_endpoint": ("GET", f"/v1/sandboxes/{SANDBOX_ID}/endpoint", {}),
        "renew": ("POST", f"/v1/sandboxes/{SANDBOX_ID}/renew", {"json": {"ttl_seconds": 60}}),
        "delete": ("DELETE", f"/v1/sandboxes/{SANDBOX_ID}", {}),
        "pause": ("POST", f"/v1/sandboxes/{SANDBOX_ID}/pause", {}),
        "resume": ("POST", f"/v1/sandboxes/{SANDBOX_ID}/resume", {}),
    }
    method, path, kwargs = requests[operation]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test",
    ) as client:
        return await client.request(method, path, **kwargs)


@pytest.mark.anyio
@pytest.mark.parametrize("derived", [False, True], ids=["base", "subclass"])
@pytest.mark.parametrize("operation", ["create", "get", "renew", "delete"])
@pytest.mark.parametrize(
    ("error_type", "status", "code", "message"),
    [
        (SandboxMetadataUnavailable, 503, "sandbox_metadata_unavailable", "sandbox lifecycle metadata is unavailable"),
        (SandboxMetadataConflict, 409, "sandbox_metadata_conflict", "sandbox lifecycle metadata conflict"),
        (SandboxMetadataCleanupFailed, 500, "sandbox_metadata_cleanup_failed", "sandbox lifecycle metadata cleanup failed"),
        (SandboxCreateUncertain, 500, "sandbox_create_uncertain", "sandbox creation outcome is uncertain"),
    ],
)
async def test_metadata_errors_keep_fixed_messages_and_ids(
    error_type: type[SandboxError], status: int, code: str, message: str,
    operation: str, derived: bool,
) -> None:
    """Metadata failure subclasses and malicious exception text retain public contracts."""
    actual_type = type("DerivedFailure", (error_type,), {}) if derived else error_type
    error = actual_type(SANDBOX_ID)
    error.args = ("secret /private/database.sqlite token=credential",)

    response = await _request_error(operation, error)

    assert response.status_code == status
    assert response.json() == {"error": {
        "code": code, "message": message, "sandbox_id": SANDBOX_ID,
    }}


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("operation", "error_type", "status", "code", "message", "has_id"),
    [
        ("create", SandboxCreateCapacityExceeded, 429, "sandbox_create_capacity_exceeded", "sandbox creation capacity exceeded", False),
        ("create", SandboxImageUnavailable, 503, "sandbox_image_unavailable", "sandbox image is unavailable", False),
        ("create", DockerUnavailable, 503, "docker_unavailable", "Docker runtime is unavailable", False),
        ("create", SandboxResourceConflict, 409, "sandbox_resource_conflict", "sandbox resource conflict", False),
        ("create", SandboxCleanupFailed, 500, "sandbox_cleanup_failed", "sandbox creation failed and cleanup was incomplete", True),
        ("create", SandboxCreateFailed, 500, "sandbox_create_failed", "sandbox creation failed", False),
        ("get", SandboxNotFound, 404, "sandbox_not_found", "sandbox not found", False),
        ("get", SandboxQueryFailed, 500, "sandbox_query_failed", "sandbox state could not be determined", False),
        ("renew", SandboxRenewConflict, 409, "sandbox_renew_conflict", "sandbox renewal conflict", True),
        ("delete", SandboxDeleteConflict, 409, "sandbox_delete_conflict", "sandbox resource ownership conflict", False),
        ("delete", SandboxDeleteFailed, 500, "sandbox_delete_failed", "sandbox deletion was incomplete", True),
        ("get_endpoint", SandboxEndpointUnavailable, 503, "sandbox_endpoint_unavailable", "sandbox endpoint is unavailable", False),
        ("get_endpoint", DockerUnavailable, 503, "sandbox_endpoint_unavailable", "sandbox endpoint is unavailable", False),
        ("get_endpoint", SandboxQueryFailed, 503, "sandbox_endpoint_unavailable", "sandbox endpoint is unavailable", False),
        ("get_endpoint", SandboxNotFound, 404, "sandbox_not_found", "sandbox not found", False),
    ],
)
async def test_domain_subclasses_keep_existing_operation_contracts(
    operation: str, error_type: type[SandboxError], status: int, code: str,
    message: str, has_id: bool,
) -> None:
    """Inherited known failures retain fixed text, IDs and endpoint-specific 503 merging."""
    error = type("DerivedFailure", (error_type,), {})(SANDBOX_ID if has_id else "secret")
    error.args = ("secret /var/run/docker.sock token=credential",)

    response = await _request_error(operation, error)

    expected = {"code": code, "message": message}
    if has_id:
        expected["sandbox_id"] = SANDBOX_ID
    assert response.status_code == status
    assert response.json() == {"error": expected}


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["pause", "resume"])
@pytest.mark.parametrize("suffix,status", [("Conflict", 409), ("Failed", 500)])
async def test_pause_resume_errors_have_fixed_public_contracts(operation: str, suffix: str, status: int) -> None:
    """Operation errors retain fixed text even when derived backend failures contain secrets."""
    error_type = getattr(domain_errors, f"Sandbox{operation.title()}{suffix}")
    error = type("DerivedFailure", (error_type,), {})("secret docker credentials")
    response = await _request_error(operation, error)
    name = "conflict" if suffix == "Conflict" else "failed"
    assert response.status_code == status
    assert response.json() == {"error": {
        "code": f"sandbox_{operation}_{name}", "message": f"sandbox {operation} {name}",
    }}


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ["create", "get", "get_endpoint", "renew", "delete"])
@pytest.mark.parametrize("error_type", [RuntimeError, SandboxError])
async def test_unknown_failures_are_not_misreported_as_known_domain_errors(
    operation: str, error_type: type[Exception],
) -> None:
    """Unmapped programming or domain failures propagate without invented business responses."""
    error = error_type("secret unexpected backend failure")

    with pytest.raises(error_type) as captured:
        await _request_error(operation, error)

    assert captured.value is error
