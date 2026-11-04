"""Single-admin authentication at the HTTP boundary, independent of business state."""

import hmac
import os
import re
from dataclasses import dataclass, field

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send

UNSET_API_KEY = object()
_KEY_FORMAT = re.compile(r"[A-Za-z0-9_-]{1,128}", flags=re.ASCII)
_BEARER_FORMAT = re.compile(rb"(?i:Bearer) ([A-Za-z0-9_-]{1,128})")


@dataclass(frozen=True)
class AuthSettings:
    """Hold one immutable, redacted credential snapshot for an application instance."""

    _api_key: bytes | None = field(repr=False)
    allow_unauthenticated: bool = False

    def require_configured(self) -> None:
        """Stop startup before external work when no deliberate auth mode was configured."""
        if self._api_key is None and not self.allow_unauthenticated:
            raise RuntimeError("control-plane API key is required")

    def authorized(self, headers: list[tuple[bytes, bytes]]) -> bool:
        """Accept exactly one bounded Bearer credential, using constant-time comparison."""
        if self.allow_unauthenticated:
            return True
        if self._api_key is None:
            return False
        credentials = [value for name, value in headers if name.lower() == b"authorization"]
        if len(credentials) != 1 or len(credentials[0]) > 135:
            return False
        match = _BEARER_FORMAT.fullmatch(credentials[0])
        return match is not None and hmac.compare_digest(match[1], self._api_key)


def auth_settings(
    api_key: object = UNSET_API_KEY, *, allow_unauthenticated: bool = False,
) -> AuthSettings:
    """Validate explicit configuration or capture the environment once without logging it."""
    if type(allow_unauthenticated) is not bool:
        raise ValueError("allow_unauthenticated must be a boolean")
    key = os.environ.get("AGENT_SANDBOX_API_KEY") if api_key is UNSET_API_KEY else api_key
    if key is None and api_key is UNSET_API_KEY:
        return AuthSettings(None, allow_unauthenticated)
    if type(key) is not str or _KEY_FORMAT.fullmatch(key) is None:
        raise ValueError("API key must contain 1 to 128 ASCII letters, digits, underscores or hyphens")
    if allow_unauthenticated:
        raise ValueError("API key and unauthenticated mode cannot be configured together")
    return AuthSettings(key.encode("ascii"))


class AuthenticationMiddleware:
    """Authenticate before routing or body reads, leaving only exact GET health anonymous."""

    def __init__(self, app: ASGIApp, settings: AuthSettings) -> None:
        """Bind the next ASGI application and its already validated immutable settings."""
        self.app = app
        self.settings = settings

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Deny unauthorized HTTP requests without reading their bodies or invoking routes."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        anonymous_health = (
            scope["method"] == "GET" and scope["path"] == "/healthz"
            # raw_path is optional in ASGI. When supplied, encoded aliases do
            # not gain the sole anonymous exception through URL normalization.
            and scope.get("raw_path") in (None, b"/healthz")
        )
        if anonymous_health or self.settings.authorized(scope.get("headers", [])):
            await self.app(scope, receive, send)
            return
        response = JSONResponse(
            status_code=401,
            content={"error": {"code": "unauthorized", "message": "authentication required"}},
            headers={"WWW-Authenticate": "Bearer", "Cache-Control": "no-store"},
        )
        await response(scope, receive, send)
