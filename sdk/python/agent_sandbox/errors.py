"""Stable SDK exceptions with bounded, non-payload exception messages."""

import re


class SDKError(Exception):
    """Base exception for SDK transport, protocol and API failures."""


class APIError(SDKError):
    """An unsuccessful HTTP response with inspectable API diagnostics."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        *,
        sandbox_id: str | None = None,
        workspace_id: str | None = None,
    ) -> None:
        """Retain structured diagnostics without putting messages in args."""
        self.status_code = status_code
        self.code = code
        self.message = message
        self.sandbox_id = sandbox_id
        self.workspace_id = workspace_id
        # Preserve future codes as data, but print only a bounded stable token.
        safe_code = code if re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,127}", code) else "http_error"
        super().__init__(f"HTTP {status_code}: {safe_code}")


class NetworkError(SDKError):
    """A transport failure whose remote side effects may be unknown."""

    def __init__(
        self,
        message: str = "HTTP request failed; remote outcome may be unknown",
        *,
        sandbox_id: str | None = None,
        workspace_id: str | None = None,
    ) -> None:
        """Keep known resource IDs and suppress arbitrary transport text."""
        self.message = "HTTP request failed; remote outcome may be unknown"
        self.sandbox_id = sandbox_id
        self.workspace_id = workspace_id
        super().__init__(self.message)


class SDKTimeoutError(NetworkError):
    """A finite HTTP transport timeout, distinct from readiness polling."""


class ProtocolError(SDKError):
    """A successful response that violates the SDK's required contract."""

    def __init__(
        self,
        message: str = "invalid server response",
        *,
        sandbox_id: str | None = None,
        workspace_id: str | None = None,
    ) -> None:
        """Keep request context without retaining arbitrary response text."""
        self.message = "invalid server response"
        self.sandbox_id = sandbox_id
        self.workspace_id = workspace_id
        super().__init__(self.message)


class ClientClosedError(SDKError):
    """An operation was attempted after the owned HTTP client was closed."""

    def __init__(self) -> None:
        """Describe the invalid local client lifecycle state."""
        super().__init__("SDK client is closed")


class ReadinessTimeoutError(SDKError):
    """The local readiness polling budget expired for a known sandbox."""

    def __init__(self, sandbox_id: str) -> None:
        """Retain the resource for follow-up without triggering cleanup."""
        self.sandbox_id = sandbox_id
        super().__init__("sandbox readiness wait timed out")


def parse_api_error(
    status_code: int,
    payload: object,
    *,
    sandbox_id: str | None = None,
    workspace_id: str | None = None,
) -> APIError:
    """Parse structured errors or return a status-aware generic fallback."""
    error = payload.get("error") if isinstance(payload, dict) else None
    if not (
        isinstance(error, dict)
        and isinstance(error.get("code"), str)
        and isinstance(error.get("message"), str)
    ):
        return APIError(
            status_code,
            "http_error",
            "HTTP request failed",
            sandbox_id=sandbox_id,
            workspace_id=workspace_id,
        )
    return APIError(
        status_code,
        error["code"],
        error["message"],
        sandbox_id=error["sandbox_id"] if isinstance(error.get("sandbox_id"), str) else sandbox_id,
        workspace_id=error["workspace_id"] if isinstance(error.get("workspace_id"), str) else workspace_id,
    )
