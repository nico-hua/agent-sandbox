"""Bounded Agent health HTTP with strict loopback addressing and cancellation cleanup."""

import asyncio
import gzip

import httpx
import pytest

from control_plane.core.errors import SandboxAgentUnavailable
from control_plane.runtime.agent_http_probe import HTTPAgentHealthProbe


URL = "http://127.0.0.1:49153"


@pytest.mark.parametrize("timeout", [0, -1, True, None, "0.5", float("nan"), float("inf"), 10**400])
def test_probe_rejects_invalid_timeouts_without_opening_clients(timeout):
    """Invalid timeout configuration cannot silently disable the bounded HTTP probe."""
    with pytest.raises(ValueError):
        HTTPAgentHealthProbe(timeout=timeout)


class ProbeStream(httpx.AsyncByteStream):
    """Expose raw body chunks, read boundaries and controllable cancellation cleanup."""

    def __init__(self, chunks=(b'{"status":"ok"}',), *, block=False, gate_close=False):
        """Keep byte chunks independent of the probe and record read/close effects."""
        self.chunks = chunks
        self.block = block
        self.gate_close = gate_close
        self.reads = 0
        self.closed = False
        self.cancelled = False
        self.entered = asyncio.Event()
        self.closing = asyncio.Event()
        self.release_close = asyncio.Event()

    async def __aiter__(self):
        """Yield one raw chunk at a time or pause to expose network cancellation."""
        self.entered.set()
        if self.block:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        for chunk in self.chunks:
            self.reads += 1
            yield chunk

    async def aclose(self):
        """Make close observable and optionally gate the final connection release."""
        self.closing.set()
        if self.gate_close:
            await self.release_close.wait()
        self.closed = True


class ProbeTransport(httpx.AsyncBaseTransport):
    """Respond without external networking while retaining actual HTTPX stream handling."""

    def __init__(self, stream=None, *, status=200, headers=None, error=None):
        """Configure a single response and count every transport request and shutdown."""
        self.stream = stream if stream is not None else ProbeStream()
        self.status = status
        self.headers = headers or {}
        self.error = error
        self.requests = []
        self.closed = False

    async def handle_async_request(self, request):
        """Return a streamed response or a transport failure without automatic retries."""
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return httpx.Response(self.status, headers=self.headers, stream=self.stream)

    async def aclose(self):
        """Record that the request-scoped HTTP client released its transport."""
        self.closed = True


@pytest.mark.anyio
@pytest.mark.parametrize("suffix", ["", "/"])
async def test_health_probe_uses_fixed_path_and_closes_resources(suffix):
    """Only one fixed health request is issued and successful probes release all resources."""
    transport = ProbeTransport()
    await HTTPAgentHealthProbe(transport=transport).check(URL + suffix)
    assert len(transport.requests) == 1
    request = transport.requests[0]
    assert request.method == "GET" and str(request.url) == URL + "/healthz"
    assert request.headers["accept-encoding"] == "identity"
    assert transport.closed and transport.stream.closed


@pytest.mark.anyio
@pytest.mark.parametrize("url", [
    "https://127.0.0.1:49153", "http://localhost:49153", "http://127.0.0.2:49153",
    "http://[::1]:49153", "http://2130706433:49153", "http://127.1:49153",
    "http://127.0.0.1", "http://127.0.0.1:0", "http://127.0.0.1:65536",
    "http://user:secret@127.0.0.1:49153", "http://127.0.0.1:49153/healthz",
    "http://127.0.0.1:49153/../", "http://127.0.0.1:49153?target=private",
    "http://127.0.0.1:49153#secret", "http://127.0.0.1:49153/?",
    "http://127.0.0.1:49153\n", "http://127.0.0.1:49153\\private", None,
])
async def test_disallowed_endpoint_never_reaches_transport(url):
    """Unsafe host, credentials, port and URL components fail before sending any request."""
    transport = ProbeTransport()
    with pytest.raises(SandboxAgentUnavailable) as failure:
        await HTTPAgentHealthProbe(transport=transport).check(url)
    assert transport.requests == []
    assert "secret" not in str(failure.value) and "private" not in str(failure.value)


@pytest.mark.anyio
@pytest.mark.parametrize("status", [201, 204, 301, 302, 307, 404, 500])
async def test_non_200_and_redirects_are_rejected_without_following(status):
    """Even redirects to another loopback address cannot cause a second request."""
    transport = ProbeTransport(status=status, headers={"location": URL + "/private"})
    with pytest.raises(SandboxAgentUnavailable):
        await HTTPAgentHealthProbe(transport=transport).check(URL)
    assert len(transport.requests) == 1
    assert transport.closed and transport.stream.closed


@pytest.mark.anyio
@pytest.mark.parametrize("body", [
    b"", b"private invalid JSON", b"null", b"[]", b"true", b"{}",
    b'{"status":"failed"}', b'{"status":true}', b'{"health":"ok"}',
    b'{"status":NaN}', b"\xff\xfeprivate",
])
async def test_invalid_health_response_is_sanitized_and_closed(body):
    """Invalid JSON and wrong health fields are not accepted or exposed to callers."""
    transport = ProbeTransport(ProbeStream((body,)))
    with pytest.raises(SandboxAgentUnavailable) as failure:
        await HTTPAgentHealthProbe(transport=transport).check(URL)
    assert "private" not in str(failure.value)
    assert transport.closed and transport.stream.closed


@pytest.mark.anyio
async def test_body_limit_is_enforced_while_reading_not_after_full_buffering():
    """Once 4 KiB is exceeded the next unbounded body chunk must never be consumed."""
    stream = ProbeStream((b"x" * 4097, b"private remainder"))
    transport = ProbeTransport(stream)
    with pytest.raises(SandboxAgentUnavailable):
        await HTTPAgentHealthProbe(transport=transport).check(URL)
    assert stream.reads == 1 and stream.closed and transport.closed


@pytest.mark.anyio
async def test_exactly_four_kib_of_valid_json_is_accepted():
    """The response cap includes bytes and accepts the exact upper boundary."""
    body = b'{"status":"ok"}'
    transport = ProbeTransport(ProbeStream((body + b" " * (4096 - len(body)),)))
    await HTTPAgentHealthProbe(transport=transport).check(URL)
    assert transport.stream.closed and transport.closed


@pytest.mark.anyio
@pytest.mark.parametrize("encoding", ["gzip", "br", "deflate", "identity, gzip"])
async def test_compressed_responses_are_rejected_without_reading_or_expansion(encoding):
    """Unexpected encoding is rejected before touching even a compressed expansion bomb."""
    transport = ProbeTransport(
        ProbeStream((gzip.compress(b"x" * 100_000),)), headers={"content-encoding": encoding},
    )
    with pytest.raises(SandboxAgentUnavailable):
        await HTTPAgentHealthProbe(transport=transport).check(URL)
    assert transport.stream.reads == 0
    assert transport.stream.closed and transport.closed


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [httpx.ConnectError("private socket"), httpx.ReadTimeout("private timeout")])
async def test_network_failures_are_not_retried_and_client_is_closed(failure):
    """A transport failure is a sanitized Agent failure with no retry or leaked pool."""
    transport = ProbeTransport(error=failure)
    with pytest.raises(SandboxAgentUnavailable) as error:
        await HTTPAgentHealthProbe(transport=transport).check(URL)
    assert len(transport.requests) == 1 and transport.closed
    assert "private" not in str(error.value)


@pytest.mark.anyio
async def test_probe_timeout_bounds_a_stalled_stream_and_releases_connection():
    """The probe's own bound remains effective independently of the API deadline."""
    stream = ProbeStream(block=True)
    transport = ProbeTransport(stream)
    with pytest.raises(SandboxAgentUnavailable):
        await HTTPAgentHealthProbe(timeout=0.02, transport=transport).check(URL)
    assert stream.cancelled and stream.closed and transport.closed
    assert len(transport.requests) == 1


@pytest.mark.anyio
async def test_repeated_cancellation_reaches_http_and_waits_for_cleanup():
    """Caller cancellation stops the network read and settles response/client shutdown."""
    stream = ProbeStream(block=True, gate_close=True)
    transport = ProbeTransport(stream)
    before = asyncio.all_tasks()
    task = asyncio.create_task(HTTPAgentHealthProbe(transport=transport).check(URL))
    try:
        await asyncio.wait_for(stream.entered.wait(), 2)
        task.cancel()
        await asyncio.wait_for(stream.closing.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and stream.cancelled
        stream.release_close.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed and transport.closed
        assert asyncio.all_tasks() == before
    finally:
        stream.release_close.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_cancellation_during_successful_body_close_settles_response():
    """Cancellation at end-of-body cannot interrupt the response's first close halfway through."""
    stream = ProbeStream(gate_close=True)
    transport = ProbeTransport(stream)
    task = asyncio.create_task(HTTPAgentHealthProbe(timeout=5, transport=transport).check(URL))
    try:
        await asyncio.wait_for(stream.closing.wait(), 2)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        stream.release_close.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed and transport.closed
    finally:
        stream.release_close.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_environment_proxies_do_not_intercept_real_loopback_request(monkeypatch):
    """A real local health server remains reachable despite hostile environment proxy settings."""
    for name in ("HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"):
        monkeypatch.setenv(name, "http://127.0.0.1:1")
    monkeypatch.setenv("NO_PROXY", "")
    monkeypatch.setenv("no_proxy", "")
    completed = asyncio.Event()
    requests = []

    async def handle(reader, writer):
        """Serve exactly one bounded local request and close its socket on every path."""
        try:
            requests.append(await reader.readuntil(b"\r\n\r\n"))
            writer.write(b'HTTP/1.1 200 OK\r\nContent-Length: 15\r\nConnection: close\r\n\r\n{"status":"ok"}')
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
            completed.set()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, limit=8192)
    try:
        port = server.sockets[0].getsockname()[1]
        await HTTPAgentHealthProbe().check(f"http://127.0.0.1:{port}")
        await asyncio.wait_for(completed.wait(), 2)
        assert len(requests) == 1 and requests[0].startswith(b"GET /healthz HTTP/1.1\r\n")
    finally:
        server.close()
        await server.wait_closed()
