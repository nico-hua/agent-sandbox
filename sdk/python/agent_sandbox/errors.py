"""Stable SDK exceptions with bounded, non-payload exception messages."""

import re

_STATUS_CODES = {
    400: "bad_request",
    401: "unauthorized",
    403: "forbidden",
    404: "not_found",
    405: "method_not_allowed",
    409: "conflict",
    413: "payload_too_large",
    429: "too_many_requests",
    500: "internal_error",
    503: "unavailable",
    504: "gateway_timeout",
}

_TRANSIENT_READY = frozenset({
    (503, "sandbox_not_ready"),
    (503, "sandbox_endpoint_unavailable"),
    (503, "sandbox_agent_unavailable"),
    (503, "docker_unavailable"),
    (504, "sandbox_readiness_timeout"),
})


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
        # Only a bounded stable token belongs in printable diagnostics.
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


def unpack_response(status_code: int, payload: object) -> tuple[str, object]:
    """Validate the complete wire envelope before exposing message and data."""
    if not (
        isinstance(payload, dict)
        and payload.keys() == {"code", "message", "data"}
        and type(payload["code"]) is int
        and payload["code"] == status_code
        and isinstance(payload["message"], str)
    ):
        raise ProtocolError()
    return payload["message"], payload["data"]


def parse_api_error(
    status_code: int,
    payload: object,
    *,
    sandbox_id: str | None = None,
    workspace_id: str | None = None,
) -> APIError:
    """Retain known diagnostics and classify valid envelopes by HTTP status."""
    try:
        message, data = unpack_response(status_code, payload)
        if status_code not in _STATUS_CODES or (data is not None and not isinstance(data, dict)):
            raise ProtocolError()
    except ProtocolError:
        return APIError(status_code, "http_error", "HTTP request failed",
                        sandbox_id=sandbox_id, workspace_id=workspace_id)

    diagnostics = data if isinstance(data, dict) else {}
    reason = diagnostics.get("reason")
    code = _STATUS_CODES[status_code]
    if isinstance(reason, str) and (status_code, reason) in _TRANSIENT_READY:
        code = reason
    reported_sandbox = diagnostics.get("sandbox_id")
    reported_workspace = diagnostics.get("workspace_id")
    return APIError(
        status_code, code, message,
        sandbox_id=reported_sandbox if isinstance(reported_sandbox, str) else sandbox_id,
        workspace_id=reported_workspace if isinstance(reported_workspace, str) else workspace_id,
    )
