"""Fixed public error responses independent of backend exception text."""

from fastapi.responses import JSONResponse

from control_plane.core.errors import (
    DockerUnavailable,
    SandboxCleanupFailed,
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

# Each entry owns status, code, fixed public text and diagnostic-ID policy.
# Exception messages can contain backend secrets, including on derived errors.
_DOMAIN_RESPONSES = {
    SandboxImageUnavailable: (503, "sandbox_image_unavailable", "sandbox image is unavailable", False),
    DockerUnavailable: (503, "docker_unavailable", "Docker runtime is unavailable", False),
    SandboxResourceConflict: (409, "sandbox_resource_conflict", "sandbox resource conflict", False),
    SandboxCleanupFailed: (500, "sandbox_cleanup_failed", "sandbox creation failed and cleanup was incomplete", True),
    SandboxCreateFailed: (500, "sandbox_create_failed", "sandbox creation failed", False),
    SandboxNotFound: (404, "sandbox_not_found", "sandbox not found", False),
    SandboxQueryFailed: (500, "sandbox_query_failed", "sandbox state could not be determined", False),
    SandboxEndpointUnavailable: (503, "sandbox_endpoint_unavailable", "sandbox endpoint is unavailable", False),
    SandboxDeleteConflict: (409, "sandbox_delete_conflict", "sandbox resource ownership conflict", False),
    SandboxDeleteFailed: (500, "sandbox_delete_failed", "sandbox deletion was incomplete", True),
    SandboxMetadataUnavailable: (503, "sandbox_metadata_unavailable", "sandbox lifecycle metadata is unavailable", True),
    SandboxMetadataConflict: (409, "sandbox_metadata_conflict", "sandbox lifecycle metadata conflict", True),
    SandboxMetadataCleanupFailed: (500, "sandbox_metadata_cleanup_failed", "sandbox lifecycle metadata cleanup failed", True),
    SandboxCreateUncertain: (500, "sandbox_create_uncertain", "sandbox creation outcome is uncertain", True),
    SandboxRenewConflict: (409, "sandbox_renew_conflict", "sandbox renewal conflict", True),
}


def sandbox_error_response(
    status_code: int, code: str, message: str, *, sandbox_id: str | None = None,
) -> JSONResponse:
    """Build a fixed public failure body with an optional diagnostic sandbox ID."""
    error = {"code": code, "message": message}
    if sandbox_id is not None:
        error["sandbox_id"] = sandbox_id
    return JSONResponse(status_code=status_code, content={"error": error})


def domain_error_response(
    error: SandboxError, *, endpoint: bool = False,
) -> JSONResponse:
    """Map a known domain failure by inheritance while retaining endpoint policy."""
    if endpoint and isinstance(
        error, (SandboxEndpointUnavailable, DockerUnavailable, SandboxQueryFailed),
    ):
        return sandbox_error_response(
            503, "sandbox_endpoint_unavailable", "sandbox endpoint is unavailable",
        )
    for error_type in type(error).__mro__:
        if error_type in _DOMAIN_RESPONSES:
            status, code, message, include_id = _DOMAIN_RESPONSES[error_type]
            return sandbox_error_response(
                status, code, message,
                sandbox_id=error.sandbox_id if include_id else None,
            )
    # Unknown domain failures need diagnosis; an invented business response
    # would hide a missing mapping or programming defect.
    raise error
