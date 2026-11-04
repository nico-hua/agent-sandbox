"""Synchronous HTTP handles with explicit ownership and bounded response reads."""

import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import replace
from time import monotonic, sleep
from typing import TypeVar
from urllib.parse import urlsplit

import httpx

from .errors import (
    APIError, ClientClosedError, NetworkError, ProtocolError,
    ReadinessTimeoutError, SDKTimeoutError, parse_api_error,
)
from .models import CommandResult, ReadyResult, RenewalResult, SandboxInfo, WorkspaceInfo

_UNSET = object()
_FILE_LIMIT = 10 * 1024 * 1024
_JSON_LIMIT = 16 * 1024 * 1024
_ERROR_LIMIT = 64 * 1024
_Result = TypeVar("_Result", SandboxInfo, WorkspaceInfo, CommandResult, ReadyResult, RenewalResult)
_TRANSIENT_READY = frozenset({
    (503, "sandbox_not_ready"), (503, "sandbox_endpoint_unavailable"),
    (503, "sandbox_agent_unavailable"), (504, "sandbox_readiness_timeout"),
    (503, "docker_unavailable"),
})


def _identifier(value: object, prefix: str) -> str:
    """Validate opaque IDs before using them in path segments."""
    if not isinstance(value, str) or re.fullmatch(prefix + r"_[0-9a-f]{32}", value) is None:
        raise ValueError("invalid resource ID")
    return value


def _text(value: object) -> str:
    """Require a string without silently coercing other caller types."""
    if not isinstance(value, str):
        raise ValueError("expected a string")
    return value


def _integer(value: object) -> int:
    """Preserve literal integers including zero for server business validation."""
    if type(value) is not int:
        raise ValueError("expected an integer")
    return value


def _duration(value: object, *, zero: bool = False) -> float:
    """Validate finite local budgets with an explicit policy for zero."""
    if type(value) not in (int, float):
        raise ValueError("expected a finite timeout")
    try:
        result = float(value)
    except OverflowError:
        raise ValueError("expected a finite timeout") from None
    if not math.isfinite(result) or (result < 0 if zero else result <= 0):
        raise ValueError("expected a finite timeout")
    return result


def _timeouts(value: float | httpx.Timeout) -> httpx.Timeout:
    """Copy finite positive HTTP phase budgets rather than retaining caller config."""
    if isinstance(value, httpx.Timeout):
        return httpx.Timeout(**{
            name: _duration(getattr(value, name)) for name in ("connect", "read", "write", "pool")
        })
    return httpx.Timeout(_duration(value))


def _control_url(value: str) -> str:
    """Accept a trusted HTTP base address without credentials or ambiguous routing."""
    if not isinstance(value, str) or any(c.isspace() or ord(c) < 32 for c in value):
        raise ValueError("invalid control-plane URL")
    try:
        parsed = urlsplit(value)
        if (parsed.scheme not in ("http", "https") or not parsed.hostname
                or parsed.username is not None or parsed.password is not None
                or parsed.path not in ("", "/") or parsed.query or parsed.fragment
                or "?" in value or "#" in value or parsed.port == 0):
            raise ValueError()
        normalized = httpx.URL(value)
        if not normalized.is_absolute_url:
            raise ValueError()
    except (ValueError, httpx.InvalidURL):
        raise ValueError("invalid control-plane URL") from None
    return str(normalized).rstrip("/")


def _origin(value: str) -> tuple[str, str, int]:
    """Compare parsed HTTP origins including effective default ports, never prefixes."""
    try:
        parsed = httpx.URL(value)
        if parsed.scheme not in ("http", "https") or not parsed.host:
            raise ValueError()
        port = parsed.port
        if port is None:
            port = 443 if parsed.scheme == "https" else 80
        return parsed.scheme, parsed.host, port
    except (ValueError, httpx.InvalidURL):
        raise ValueError("invalid request origin") from None


def _json(body: bytes) -> object:
    """Decode strict UTF-8 JSON, excluding nonstandard numeric constants."""
    def reject_constant(value: str) -> None:
        """Reject NaN and Infinity instead of accepting non-JSON numbers."""
        raise ValueError("invalid JSON constant")

    try:
        return json.loads(body.decode("utf-8"), parse_constant=reject_constant)
    except (ValueError, UnicodeError, RecursionError):
        raise ProtocolError() from None


def _read_bounded(response: httpx.Response, limit: int) -> bytes:
    """Read raw identity bytes with actual size and framing consistency checks."""
    if response.headers.get("content-encoding", "identity").lower() != "identity":
        raise ProtocolError()
    declared = response.headers.get("content-length")
    if declared is not None:
        if re.fullmatch(r"[0-9]+", declared) is None or len(declared) > 20:
            raise ProtocolError()
        expected = int(declared)
        if expected > limit:
            raise ProtocolError()
    else:
        expected = None
    # Custom transports can supply consumed responses; real HTTP responses stream
    # raw so automatic decompression cannot circumvent the actual-byte budget.
    if response.is_stream_consumed:
        body = response.content
        if len(body) > limit:
            raise ProtocolError()
    else:
        output = bytearray()
        # Preserve transport chunks: requesting fixed chunk sizes can read past
        # an observed limit while HTTPX waits to fill its next output chunk.
        for chunk in response.iter_raw():
            if len(chunk) > limit - len(output):
                raise ProtocolError()
            output.extend(chunk)
        body = bytes(output)
    if expected is not None and len(body) != expected:
        raise ProtocolError()
    return body


def _parse_result(model: type[_Result], payload: object, *, sandbox_id: str | None = None,
                  workspace_id: str | None = None, creation: bool = False) -> _Result:
    """Keep the requested identity when a successful response has invalid fields."""
    try:
        if creation:
            # A workspace binding is context, never the new sandbox's expected ID.
            return model.from_payload(payload, creation=True)
        return model.from_payload(payload, expected_id=sandbox_id or workspace_id)
    except ProtocolError:
        raise ProtocolError(sandbox_id=sandbox_id, workspace_id=workspace_id) from None


class SandboxClient:
    """Own HTTP connections, never remote resources; intended for local development."""

    def __init__(
        self, base_url: str = "http://127.0.0.1:18083", *,
        api_key: str | None = None,
        timeout: float | httpx.Timeout = 5.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        """Accept an explicit optional admin key without reading ambient credentials."""
        if api_key is not None and (
            not isinstance(api_key, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,128}", api_key) is None
        ):
            raise ValueError("invalid API key")
        self._base_url = _control_url(base_url)
        self._control_origin = _origin(self._base_url)
        self._api_key = api_key
        self._timeout = _timeouts(timeout)
        self._closed = False
        self._http = httpx.Client(
            timeout=self._timeout, transport=transport, trust_env=False,
            follow_redirects=False,
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
            headers={"Accept": "application/json", "Accept-Encoding": "identity"},
        )

    def __enter__(self) -> "SandboxClient":
        """Enter without creating, deleting or adopting remote resources."""
        self._ensure_open()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        """Close HTTP connections even if user code raises."""
        self.close()

    def close(self) -> None:
        """Idempotently close the owned client, including its injected transport."""
        if not self._closed:
            self._closed = True
            self._http.close()

    def _ensure_open(self) -> None:
        """Reject operations after explicit connection shutdown."""
        if self._closed:
            raise ClientClosedError()

    def _request(
        self, method: str, url: str, *, expected: int, control_plane: bool,
        payload: object = _UNSET, params: dict[str, str] | None = None,
        content: bytes | None = None, timeout: httpx.Timeout | None = None,
        binary: bool = False, empty: bool = False,
        sandbox_id: str | None = None, workspace_id: str | None = None,
    ) -> object:
        """Authenticate explicit management requests only, with one bounded HTTP attempt."""
        self._ensure_open()
        headers = {}
        if control_plane:
            if _origin(url) != self._control_origin:
                raise ValueError("control-plane request origin mismatch")
            if self._api_key is not None:
                headers["Authorization"] = "Bearer " + self._api_key
        options = {} if payload is _UNSET else {"json": payload}
        try:
            with self._http.stream(
                method, url, params=params, content=content, headers=headers,
                timeout=timeout or self._timeout, **options,
            ) as response:
                if response.status_code != expected:
                    if 200 <= response.status_code < 300:
                        raise ProtocolError()
                    try:
                        error_payload = _json(_read_bounded(response, _ERROR_LIMIT))
                    except ProtocolError:
                        error_payload = None
                    raise parse_api_error(response.status_code, error_payload,
                                          sandbox_id=sandbox_id, workspace_id=workspace_id)
                if empty:
                    _read_bounded(response, 0)
                    return None
                body = _read_bounded(response, _FILE_LIMIT if binary else _JSON_LIMIT)
                return body if binary else _json(body)
        except httpx.TimeoutException:
            raise SDKTimeoutError(sandbox_id=sandbox_id, workspace_id=workspace_id) from None
        except httpx.HTTPError:
            raise NetworkError(sandbox_id=sandbox_id, workspace_id=workspace_id) from None
        except ProtocolError as error:
            error.sandbox_id = sandbox_id
            error.workspace_id = workspace_id
            raise

    def create_sandbox(
        self, *, environment: object = _UNSET, ttl_seconds: object = _UNSET,
        resources: object = _UNSET, network: object = _UNSET, workspace_id: object = _UNSET,
    ) -> "Sandbox":
        """Submit supplied options once and return without implicitly waiting for ready."""
        self._ensure_open()
        payload: dict[str, object] = {}
        if environment is not _UNSET:
            payload["environment"] = _text(environment)
        if ttl_seconds is not _UNSET:
            payload["ttl_seconds"] = _integer(ttl_seconds)
        if resources is not _UNSET:
            if not isinstance(resources, Mapping):
                raise ValueError("resources must be an object")
            payload["resources"] = {_text(key): _integer(value) for key, value in resources.items()}
        if network is not _UNSET:
            if not isinstance(network, Mapping) or set(network) - {"allow_domains"}:
                raise ValueError("invalid network options")
            copied: dict[str, object] = {}
            if "allow_domains" in network:
                domains = network["allow_domains"]
                if not isinstance(domains, list):
                    raise ValueError("allow_domains must be a list")
                copied["allow_domains"] = [_text(domain) for domain in domains]
            payload["network"] = copied
        if workspace_id is not _UNSET:
            payload["workspace_id"] = _identifier(workspace_id, "wsp")
        known_workspace = payload.get("workspace_id")
        info = _parse_result(SandboxInfo, self._request(
            "POST", self._base_url + "/v1/sandboxes", expected=201, payload=payload,
            workspace_id=known_workspace, control_plane=True,
        ), creation=True, workspace_id=known_workspace)
        return Sandbox(self, info.sandbox_id, info)

    def get_sandbox(self, sandbox_id: str) -> "Sandbox":
        """Read an existing sandbox without creating or recovering it."""
        sandbox = Sandbox(self, sandbox_id)
        sandbox.refresh()
        return sandbox

    def create_workspace(self, *, environment: object = _UNSET) -> "Workspace":
        """Create an independent workspace with an optional registered environment."""
        self._ensure_open()
        payload = {} if environment is _UNSET else {"environment": _text(environment)}
        info = WorkspaceInfo.from_payload(self._request(
            "POST", self._base_url + "/v1/workspaces", expected=201, payload=payload,
            control_plane=True,
        ))
        return Workspace(self, info.workspace_id, info)

    def get_workspace(self, workspace_id: str) -> "Workspace":
        """Read an existing workspace without changing its occupancy."""
        workspace = Workspace(self, workspace_id)
        workspace.refresh()
        return workspace


class Sandbox:
    """A sandbox handle with cached metadata updated only by explicit observations."""

    def __init__(self, client: SandboxClient, sandbox_id: str, info: SandboxInfo | None = None) -> None:
        """Bind a validated identity without issuing an HTTP request."""
        self._client = client
        self.sandbox_id = _identifier(sandbox_id, "sbx")
        self.info = info

    @property
    def _url(self) -> str:
        """Build a control-plane resource URL from the validated opaque identity."""
        return self._client._base_url + "/v1/sandboxes/" + self.sandbox_id

    def _request(self, method: str, suffix: str = "", **kwargs: object) -> object:
        """Attach known resource identity to control-plane HTTP failures."""
        return self._client._request(
            method, self._url + suffix, sandbox_id=self.sandbox_id, control_plane=True, **kwargs,
        )

    def refresh(self) -> SandboxInfo:
        """Read current runtime and lifecycle facts and replace cached metadata."""
        self.info = _parse_result(
            SandboxInfo, self._request("GET", expected=200), sandbox_id=self.sandbox_id,
        )
        return self.info

    def get_endpoint(self) -> str:
        """Fetch and strictly validate the local endpoint before any data-plane use."""
        payload = self._request("GET", "/endpoint", expected=200)
        if not isinstance(payload, dict) or payload.get("sandbox_id") != self.sandbox_id:
            raise ProtocolError(sandbox_id=self.sandbox_id)
        url = payload.get("url")
        match = re.fullmatch(r"http://127\.0\.0\.1:([1-9][0-9]{0,4})/?", url) if isinstance(url, str) else None
        if match is None or int(match[1]) > 65535:
            raise ProtocolError(sandbox_id=self.sandbox_id)
        return url.rstrip("/")

    def wait_ready(self, *, timeout: float = 30.0, poll_interval: float = 0.25) -> ReadyResult:
        """Poll only known transient readyz errors within a monotonic waiting budget."""
        self._client._ensure_open()
        budget, interval = _duration(timeout, zero=True), _duration(poll_interval)
        deadline = monotonic() + budget
        while True:
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ReadinessTimeoutError(self.sandbox_id)
            phases = {name: min(getattr(self._client._timeout, name), remaining)
                      for name in ("connect", "read", "write", "pool")}
            try:
                return _parse_result(ReadyResult, self._request(
                    "GET", "/readyz", expected=200, timeout=httpx.Timeout(**phases),
                ), sandbox_id=self.sandbox_id)
            except APIError as error:
                if (error.status_code, error.code) not in _TRANSIENT_READY:
                    raise
            remaining = deadline - monotonic()
            if remaining > 0:
                sleep(min(interval, remaining))

    def renew(self, ttl_seconds: int) -> RenewalResult:
        """Submit one renewal without retrying or changing runtime resources."""
        result = _parse_result(RenewalResult, self._request(
            "POST", "/renew", expected=200, payload={"ttl_seconds": _integer(ttl_seconds)},
        ), sandbox_id=self.sandbox_id)
        if self.info is not None:
            self.info = replace(self.info, expires_at=result.expires_at)
        return result

    def pause(self) -> None:
        """Request process freezing without implicitly renewing or waiting."""
        self._request("POST", "/pause", expected=204, empty=True)

    def resume(self) -> None:
        """Request unfreezing; subsequent readiness checks remain explicit."""
        self._request("POST", "/resume", expected=204, empty=True)

    def delete(self, *, delete_workspace: bool = False) -> None:
        """Delete once, preserving workspace by default and honoring server conflicts."""
        if type(delete_workspace) is not bool:
            raise ValueError("delete_workspace must be a boolean")
        self._request("DELETE", expected=204, empty=True,
                      params={"delete_workspace": str(delete_workspace).lower()})

    def run(
        self, argv: Sequence[str], *, cwd: object = _UNSET, env: object = _UNSET,
        stdin: object = _UNSET, timeout_ms: object = _UNSET,
        max_output_bytes_per_stream: object = _UNSET,
    ) -> CommandResult:
        """Run literal argv once at a fresh endpoint; nonzero exits remain normal results."""
        if isinstance(argv, (str, bytes)) or not isinstance(argv, Sequence) or not argv:
            raise ValueError("argv must be a nonempty sequence of strings")
        payload: dict[str, object] = {"argv": [_text(value) for value in argv]}
        if not payload["argv"][0]:
            raise ValueError("argv executable must not be empty")
        for name, value in (("cwd", cwd), ("stdin", stdin)):
            if value is not _UNSET:
                payload[name] = _text(value)
        if env is not _UNSET:
            if not isinstance(env, Mapping):
                raise ValueError("env must be an object")
            payload["env"] = {_text(key): _text(value) for key, value in env.items()}
        for name, value in (("timeout_ms", timeout_ms), ("max_output_bytes_per_stream", max_output_bytes_per_stream)):
            if value is not _UNSET:
                payload[name] = _integer(value)
        # Invalid business ranges reach the Agent unchanged. Only its valid
        # execution budget extends the HTTP read phase, never an unbounded input.
        agent_ms = 5000 if timeout_ms is _UNSET else timeout_ms
        phases = {name: getattr(self._client._timeout, name) for name in ("connect", "read", "write", "pool")}
        if 1 <= agent_ms <= 30000:
            phases["read"] = max(phases["read"], agent_ms / 1000 + 5)
        return _parse_result(CommandResult, self._client._request(
            "POST", self.get_endpoint() + "/v1/commands:run", expected=200,
            payload=payload, timeout=httpx.Timeout(**phases), sandbox_id=self.sandbox_id,
            control_plane=False,
        ), sandbox_id=self.sandbox_id)

    def write_file(self, path: str, content: bytes) -> None:
        """Upload bounded bytes to a query-encoded workspace path without local file I/O."""
        if not _text(path) or not isinstance(content, bytes) or len(content) > _FILE_LIMIT:
            raise ValueError("expected a path and at most 10 MiB of bytes")
        self._client._request(
            "POST", self.get_endpoint() + "/v1/files", expected=201, empty=True,
            params={"path": path}, content=content, sandbox_id=self.sandbox_id,
            control_plane=False,
        )

    def read_file(self, path: str) -> bytes:
        """Read at most 10 MiB of actual bytes through a freshly queried endpoint."""
        if not _text(path):
            raise ValueError("path must not be empty")
        return self._client._request(
            "GET", self.get_endpoint() + "/v1/files", expected=200, binary=True,
            params={"path": path}, sandbox_id=self.sandbox_id,
            control_plane=False,
        )


class Workspace:
    """An explicit workspace whose lifetime is independent of client connections."""

    def __init__(self, client: SandboxClient, workspace_id: str, info: WorkspaceInfo | None = None) -> None:
        """Bind a validated workspace identity without claiming or creating it."""
        self._client = client
        self.workspace_id = _identifier(workspace_id, "wsp")
        self.info = info

    def _request(self, method: str, **kwargs: object) -> object:
        """Route workspace operations exclusively to the control plane."""
        return self._client._request(
            method, self._client._base_url + "/v1/workspaces/" + self.workspace_id,
            workspace_id=self.workspace_id, control_plane=True, **kwargs,
        )

    def refresh(self) -> WorkspaceInfo:
        """Read persisted ownership and runtime facts without acquiring occupancy."""
        self.info = _parse_result(
            WorkspaceInfo, self._request("GET", expected=200), workspace_id=self.workspace_id,
        )
        return self.info

    def delete(self) -> None:
        """Delete an idle workspace explicitly, preserving every server protection."""
        self._request("DELETE", expected=204, empty=True)
