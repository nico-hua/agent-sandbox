"""Fixed public error responses independent of backend exception text."""

from fastapi.responses import JSONResponse

from control_plane.api.response import error_response
from control_plane.domain.errors import (
    DockerUnavailable,
    SandboxAgentUnavailable,
    SandboxCleanupFailed,
    SandboxCreateCapacityExceeded,
    SandboxCreateFailed,
    SandboxCreateUncertain,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxEndpointAuthRequired,
    SandboxEndpointAuthUnavailable,
    SandboxEndpointUnavailable,
    SandboxEnvironmentNotRegistered,
    SandboxError,
    SandboxImageUnavailable,
    SandboxInstanceLimitExceeded,
    SandboxListTimeout,
    SandboxListUnavailable,
    SandboxMetadataCleanupFailed,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    SandboxNotFound,
    SandboxNotReady,
    SandboxPauseConflict,
    SandboxPauseFailed,
    SandboxQueryFailed,
    SandboxReadinessTimeout,
    SandboxRenewConflict,
    SandboxResourceConflict,
    SandboxResumeConflict,
    SandboxResumeFailed,
    SandboxServiceEndpointTimeout,
    SandboxServiceUnsupported,
    WorkspaceConflict,
    WorkspaceDeleteProtected,
    WorkspaceNotFound,
    WorkspaceOperationFailed,
    WorkspaceUnavailable,
)

# Each entry owns HTTP status, fixed public text and diagnostic-ID policy.
# Exception messages can contain backend secrets, including on derived errors.
_DOMAIN_RESPONSES = {
    SandboxServiceUnsupported: (409, "proxy does not support service forwarding; recreate sandbox", False),
    SandboxServiceEndpointTimeout: (504, "sandbox service endpoint query timed out", False),
    SandboxListUnavailable: (503, "sandbox list is unavailable", False),
    SandboxListTimeout: (504, "sandbox list query timed out", False),
    WorkspaceNotFound: (404, "workspace not found", False),
    WorkspaceConflict: (409, "workspace conflict", False),
    WorkspaceUnavailable: (503, "workspace is unavailable", False),
    WorkspaceOperationFailed: (500, "workspace operation failed", False),
    WorkspaceDeleteProtected: (409, "workspace must be deleted separately", False),
    SandboxEnvironmentNotRegistered: (400, "invalid sandbox creation options", False),
    SandboxInstanceLimitExceeded: (429, "sandbox instance limit exceeded", False),
    SandboxCreateCapacityExceeded: (429, "sandbox creation capacity exceeded", False),
    SandboxImageUnavailable: (503, "sandbox image is unavailable", False),
    DockerUnavailable: (503, "Docker runtime is unavailable", False),
    SandboxResourceConflict: (409, "sandbox resource conflict", False),
    SandboxCleanupFailed: (500, "sandbox creation failed and cleanup was incomplete", True),
    SandboxCreateFailed: (500, "sandbox creation failed", False),
    SandboxNotFound: (404, "sandbox not found", False),
    SandboxQueryFailed: (500, "sandbox state could not be determined", False),
    SandboxEndpointUnavailable: (503, "sandbox endpoint is unavailable", False),
    SandboxEndpointAuthRequired: (409, "legacy endpoint is unauthenticated; recreate sandbox", False),
    SandboxEndpointAuthUnavailable: (503, "execution credentials are unavailable", False),
    SandboxNotReady: (503, "sandbox is not ready", False),
    SandboxPauseConflict: (409, "sandbox pause conflict", False),
    SandboxPauseFailed: (500, "sandbox pause failed", False),
    SandboxResumeConflict: (409, "sandbox resume conflict", False),
    SandboxResumeFailed: (500, "sandbox resume failed", False),
    SandboxAgentUnavailable: (503, "sandbox Agent is unavailable", False),
    SandboxReadinessTimeout: (504, "sandbox readiness check timed out", False),
    SandboxDeleteConflict: (409, "sandbox resource ownership conflict", False),
    SandboxDeleteFailed: (500, "sandbox deletion was incomplete", True),
    SandboxMetadataUnavailable: (503, "sandbox lifecycle metadata is unavailable", True),
    SandboxMetadataConflict: (409, "sandbox lifecycle metadata conflict", True),
    SandboxMetadataCleanupFailed: (500, "sandbox lifecycle metadata cleanup failed", True),
    SandboxCreateUncertain: (500, "sandbox creation outcome is uncertain", True),
    SandboxRenewConflict: (409, "sandbox renewal conflict", True),
}


_READINESS_REASONS = {
    SandboxNotReady: "sandbox_not_ready",
    SandboxEndpointUnavailable: "sandbox_endpoint_unavailable",
    SandboxAgentUnavailable: "sandbox_agent_unavailable",
    DockerUnavailable: "docker_unavailable",
    SandboxReadinessTimeout: "sandbox_readiness_timeout",
}


def domain_error_response(
    error: SandboxError, *, endpoint: bool = False, readiness: bool = False,
) -> JSONResponse:
    """Map known failures by inheritance, retaining endpoint and readiness policies."""
    if endpoint and isinstance(
        error, (SandboxEndpointUnavailable, DockerUnavailable, SandboxQueryFailed),
    ):
        return error_response(503, "sandbox endpoint is unavailable")

    for error_type in type(error).__mro__:
        if error_type in _DOMAIN_RESPONSES:
            status, message, include_id = _DOMAIN_RESPONSES[error_type]
            data = {}
            if include_id:
                data["sandbox_id"] = error.sandbox_id
            workspace_id = getattr(error, "workspace_id", None)
            if status != 404 and workspace_id is not None:
                data["workspace_id"] = workspace_id
            if readiness and (reason := _READINESS_REASONS.get(error_type)):
                data["reason"] = reason
            return error_response(status, message, data=data or None)

    # Keep unknown failures available for the framework to diagnose as internal errors.
    raise error
