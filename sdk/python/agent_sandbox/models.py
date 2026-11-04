"""Immutable response records and strict HTTP payload conversion."""

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .errors import ProtocolError


def _object(payload: object) -> dict[str, object]:
    """Require a JSON object while leaving future response keys compatible."""
    if not isinstance(payload, dict):
        raise ProtocolError()
    return payload


def _string(value: object, *, nonempty: bool = False) -> str:
    """Require a string, optionally excluding empty protocol states."""
    if not isinstance(value, str) or (nonempty and not value):
        raise ProtocolError()
    return value


def _optional_string(value: object) -> str | None:
    """Accept explicitly nullable response diagnostics without coercion."""
    return None if value is None else _string(value)


def _resource_id(value: object, prefix: str, expected_id: str | None = None) -> str:
    """Check an opaque resource ID and optional request-response identity."""
    resource_id = _string(value)
    if re.fullmatch(prefix + r"_[0-9a-f]{32}", resource_id) is None:
        raise ProtocolError()
    if expected_id is not None and resource_id != expected_id:
        raise ProtocolError()
    return resource_id


def _utc_datetime(value: object) -> datetime:
    """Parse explicit RFC3339 UTC timestamps with microsecond precision."""
    timestamp = _string(value)
    if re.fullmatch(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]+)?(?:Z|\+00:00)",
        timestamp,
    ) is None:
        raise ProtocolError()
    try:
        return datetime.fromisoformat(timestamp).astimezone(timezone.utc)
    except ValueError:
        raise ProtocolError() from None


@dataclass(frozen=True)
class EndpointConnection:
    """Validated local instance endpoint and its request-scoped secret credential."""

    sandbox_id: str
    url: str
    auth_required: bool
    access_token: str = field(repr=False)

    def __post_init__(self) -> None:
        """Reject anonymous or ambiguous routing even for direct construction."""
        _resource_id(self.sandbox_id, "sbx")
        match = re.fullmatch(r"http://127\.0\.0\.1:([1-9][0-9]{0,4})/?", self.url) if isinstance(self.url, str) else None
        if match is None or int(match[1]) > 65535 or self.auth_required is not True:
            raise ProtocolError()
        if not isinstance(self.access_token, str) or re.fullmatch(r"[A-Za-z0-9_-]{43}", self.access_token) is None:
            raise ProtocolError()
        object.__setattr__(self, "url", self.url.rstrip("/"))

    @classmethod
    def from_payload(cls, payload: object, *, expected_id: str | None = None) -> "EndpointConnection":
        """Require explicit authenticated metadata and the requested sandbox identity."""
        data = _object(payload)
        return cls(
            _resource_id(data.get("sandbox_id"), "sbx", expected_id),
            data.get("url"), data.get("auth_required"), data.get("access_token"),
        )


@dataclass(frozen=True)
class SandboxInfo:
    """Control-plane sandbox metadata, including nullable legacy time pairs."""

    sandbox_id: str
    status: str
    created_at: datetime | None
    expires_at: datetime | None
    reason: str | None = None
    message: str | None = None
    workspace_id: str | None = None

    @classmethod
    def from_payload(
        cls,
        payload: object,
        *,
        expected_id: str | None = None,
        creation: bool = False,
    ) -> "SandboxInfo":
        """Validate sandbox response identity, metadata and paired UTC times."""
        data = _object(payload)
        if "created_at" not in data or "expires_at" not in data:
            raise ProtocolError()
        created_at, expires_at = data["created_at"], data["expires_at"]
        if created_at is None and expires_at is None and not creation:
            created, expires = None, None
        else:
            created, expires = _utc_datetime(created_at), _utc_datetime(expires_at)
        workspace_id = data.get("workspace_id")
        return cls(
            sandbox_id=_resource_id(data.get("sandbox_id"), "sbx", expected_id),
            status=_string(data.get("status"), nonempty=True),
            created_at=created,
            expires_at=expires,
            reason=_optional_string(data.get("reason")),
            message=_optional_string(data.get("message")),
            workspace_id=None if workspace_id is None else _resource_id(workspace_id, "wsp"),
        )


@dataclass(frozen=True)
class WorkspaceInfo:
    """Workspace metadata and the sandbox currently holding it, if any."""

    workspace_id: str
    status: str
    created_at: datetime
    sandbox_id: str | None = None

    @classmethod
    def from_payload(cls, payload: object, *, expected_id: str | None = None) -> "WorkspaceInfo":
        """Require explicit workspace identity, holder and UTC creation time."""
        data = _object(payload)
        if "sandbox_id" not in data:
            raise ProtocolError()
        sandbox_id = data["sandbox_id"]
        return cls(
            workspace_id=_resource_id(data.get("workspace_id"), "wsp", expected_id),
            status=_string(data.get("status"), nonempty=True),
            created_at=_utc_datetime(data.get("created_at")),
            sandbox_id=None if sandbox_id is None else _resource_id(sandbox_id, "sbx"),
        )


@dataclass(frozen=True)
class CommandResult:
    """A completed command with its exit status and plain-text output."""

    exit_code: int
    stdout: str
    stderr: str

    @classmethod
    def from_payload(cls, payload: object, *, expected_id: str | None = None) -> "CommandResult":
        """Parse command output without treating nonzero exits as SDK errors."""
        data = _object(payload)
        exit_code = data.get("exit_code")
        if type(exit_code) is not int:
            raise ProtocolError()
        return cls(exit_code, _string(data.get("stdout")), _string(data.get("stderr")))


@dataclass(frozen=True)
class RenewalResult:
    """The persisted UTC expiry returned by a sandbox renewal."""

    sandbox_id: str
    expires_at: datetime

    @classmethod
    def from_payload(cls, payload: object, *, expected_id: str | None = None) -> "RenewalResult":
        """Validate renewal identity and its explicit UTC expiry."""
        data = _object(payload)
        return cls(
            _resource_id(data.get("sandbox_id"), "sbx", expected_id),
            _utc_datetime(data.get("expires_at")),
        )


@dataclass(frozen=True)
class ReadyResult:
    """A successful readiness probe for the requested sandbox."""

    sandbox_id: str
    status: str = "ready"

    @classmethod
    def from_payload(cls, payload: object, *, expected_id: str | None = None) -> "ReadyResult":
        """Require the explicit ready state instead of accepting other states."""
        data = _object(payload)
        if data.get("status") != "ready":
            raise ProtocolError()
        return cls(_resource_id(data.get("sandbox_id"), "sbx", expected_id))
