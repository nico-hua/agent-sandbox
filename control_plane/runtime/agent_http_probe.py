"""One bounded, request-scoped HTTP health check through a verified local Agent entry."""

import asyncio
import json
import math
import re

import httpx

from control_plane.core.errors import SandboxAgentUnavailable
from control_plane.core.tasks import settle_task

_MAX_BODY_BYTES = 4096


def _health_target(endpoint: str) -> str:
    """Accept only a literal loopback HTTP base URL and append the fixed health path."""
    match = (
        re.fullmatch(r"http://127\.0\.0\.1:([1-9][0-9]{0,4})/?", endpoint)
        if isinstance(endpoint, str) else None
    )
    if match is None or int(match[1]) > 65535:
        raise SandboxAgentUnavailable()
    # Validate before URL parsing can normalize away credentials or unexpected components.
    return endpoint.rstrip("/") + "/healthz"


async def _close_probe(response: httpx.Response | None, client: httpx.AsyncClient) -> None:
    """Always close the client even if releasing its streamed response fails."""
    try:
        if response is not None:
            await response.aclose()
    finally:
        await client.aclose()


class HTTPAgentHealthProbe:
    """Probe fixed Agent health with bounded raw bytes, no redirects, proxies or retries."""

    def __init__(
        self, *, timeout: float = 0.5, transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """Keep finite network settings and an optional test transport without opening clients."""
        if type(timeout) not in (int, float):
            raise ValueError("Agent probe timeout must be positive and finite")
        try:
            valid = math.isfinite(timeout) and timeout > 0
        except OverflowError:
            valid = False
        if not valid:
            raise ValueError("Agent probe timeout must be positive and finite")
        self._timeout = timeout
        self._transport = transport

    async def check(self, endpoint: str) -> None:
        """Propagate cancellation to HTTP work and settle response/connection cleanup on all paths."""
        target = _health_target(endpoint)
        try:
            client = httpx.AsyncClient(
                timeout=self._timeout, transport=self._transport, trust_env=False,
                follow_redirects=False,
                headers={"accept": "application/json", "accept-encoding": "identity"},
            )
            response = None
            try:
                # Per-network-operation timeouts alone cannot bound a trickling body.
                # This probe cap is separate from the API's complete query/entry/HTTP deadline.
                async with asyncio.timeout(self._timeout):
                    response = await client.send(client.build_request("GET", target), stream=True)
                    if response.status_code != 200:
                        raise SandboxAgentUnavailable()
                    if response.headers.get("content-encoding", "identity").strip().lower() != "identity":
                        raise SandboxAgentUnavailable()
                    body = bytearray()
                    # The transport stream contains raw, undecoded bytes. Check
                    # each chunk before buffering, retaining at most 4 KiB.
                    # aiter_raw() also closes on EOF, which would put the first
                    # close outside our cancellation-protected cleanup below.
                    async for chunk in response.stream:
                        if len(body) + len(chunk) > _MAX_BODY_BYTES:
                            raise SandboxAgentUnavailable()
                        body.extend(chunk)
                    if json.loads(body) != {"status": "ok"}:
                        raise SandboxAgentUnavailable()
            finally:
                worker = asyncio.create_task(_close_probe(response, client))
                await settle_task(worker, propagate_cancellation=True)
        except Exception:
            # Neither transport exception text nor response bytes belong in domain failures.
            raise SandboxAgentUnavailable() from None
