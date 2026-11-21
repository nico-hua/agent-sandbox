"""Verified local endpoint descriptions and per-transaction credentials."""

import re
from dataclasses import dataclass, field
from datetime import datetime

from control_plane.domain.validation import _validate_integer


def _validate_access_token(token: str) -> None:
    """Require the bounded URL-safe representation of a 32-byte random credential."""
    if type(token) is not str or re.fullmatch(r"[A-Za-z0-9_-]{43}", token) is None:
        raise ValueError("invalid sandbox access credential")


@dataclass(frozen=True)
class SandboxAccessCredential:
    """Bind a private execution credential to one immutable sandbox transaction."""

    sandbox_id: str
    transaction_id: str
    access_token: str = field(repr=False)

    def __post_init__(self) -> None:
        """Validate identity and configuration-safe token bytes without exposing secrets."""
        if type(self.sandbox_id) is not str or not self.sandbox_id:
            raise ValueError("invalid credential identity")
        if type(self.transaction_id) is not str or not self.transaction_id:
            raise ValueError("invalid credential identity")
        _validate_access_token(self.access_token)


@dataclass(frozen=True)
class SandboxEndpoint:
    """Describe the local WSL development URL of one sandbox entry proxy."""

    sandbox_id: str
    url: str
    auth_required: bool = False
    access_token: str | None = field(default=None, repr=False)
    transaction_id: str | None = None
    created_at: datetime | None = None
    credential_digest: str | None = field(default=None, repr=False)
    auth_version: str | None = None
    services_version: str | None = None


def _validate_service_port(port: int) -> None:
    """Allow canonical application ports while excluding the platform Agent listener."""
    _validate_integer(port, 1024, 65535, "port")
    if port == 8080:
        raise ValueError("port is reserved for the Agent")


@dataclass(frozen=True)
class SandboxServiceEndpoint:
    """Deliver an authenticated local service route without implying application readiness."""

    sandbox_id: str
    port: int
    url: str
    auth_required: bool
    access_token: str = field(repr=False)

    def __post_init__(self) -> None:
        """Reject ambiguous addresses or credentials before they can be delivered to a caller."""
        _validate_service_port(self.port)
        match = re.fullmatch(r"http://127\.0\.0\.1:([1-9][0-9]{0,4})/services/" + str(self.port) + "/", self.url)
        if match is None or int(match[1]) > 65535 or self.auth_required is not True:
            raise ValueError("invalid service connection")
        if type(self.access_token) is not str or re.fullmatch(r"[A-Za-z0-9_-]{43}", self.access_token) is None:
            raise ValueError("invalid service credential")
