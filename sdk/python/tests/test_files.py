"""Bounded file I/O and streamed HTTP bodies at the SDK transport boundary."""

import gzip
import json

import httpx
import pytest

from agent_sandbox import APIError, NetworkError, ProtocolError, SDKTimeoutError
from test_client import CONTROL, SBX, Recorder, sandbox_payload

FILE_LIMIT = 10 * 1024 * 1024
JSON_LIMIT = 16 * 1024 * 1024


class TrackedStream(httpx.SyncByteStream):
    """Yield raw chunks while recording early stops and guaranteed response closure."""

    def __init__(self, *chunks, failure=None):
        """Prepare bytes and an optional read failure without preconsuming the response."""
        self.chunks = chunks
        self.failure = failure
        self.yielded = 0
        self.closed = False

    def __iter__(self):
        """Yield actual transport bytes and optionally fail before normal exhaustion."""
        for chunk in self.chunks:
            self.yielded += 1
            yield chunk
        if self.failure is not None:
            raise self.failure

    def close(self):
        """Record the SDK's response cleanup on success and every failure path."""
        self.closed = True


def file_script(response):
    """Return the standard control lookup followed by a single agent file response."""
    return Recorder(httpx.Response(200, json=sandbox_payload()),
                    httpx.Response(200, json={"sandbox_id": SBX, "url": "http://127.0.0.1:19001"}),
                    response)


def test_file_read_write_encode_path_as_query_and_refresh_endpoint():
    """File methods preserve binary bytes and encode reserved path characters safely."""
    path = "/workspace/a?x=1&other=# fragment/你好"
    content = b"\x00\xffbinary\r\n"
    recorder = Recorder(
        httpx.Response(200, json=sandbox_payload()),
        httpx.Response(200, json={"sandbox_id": SBX, "url": "http://127.0.0.1:19001"}),
        httpx.Response(201),
        httpx.Response(200, json={"sandbox_id": SBX, "url": "http://127.0.0.1:19002/"}),
        httpx.Response(200, content=content),
    )
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        assert sandbox.write_file(path, content) is None
        assert sandbox.read_file(path) == content
    write, read = recorder.requests[2], recorder.requests[4]
    assert (write.method, write.url.host, write.url.port, write.url.path) == (
        "POST", "127.0.0.1", 19001, "/v1/files")
    assert (read.method, read.url.port, read.url.path) == ("GET", 19002, "/v1/files")
    assert dict(write.url.params) == dict(read.url.params) == {"path": path}
    assert write.content == content
    assert not write.url.fragment


@pytest.mark.parametrize("content", ["text", bytearray(b"x"), None, b"x" * (FILE_LIMIT + 1)])
def test_invalid_file_content_fails_before_endpoint_lookup(content):
    """Writes require bytes within 10 MiB before any agent endpoint query."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()))
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        with pytest.raises((ValueError, TypeError)):
            sandbox.write_file("/workspace/file", content)
    assert len(recorder.requests) == 1


@pytest.mark.parametrize("method", ["read_file", "write_file"])
@pytest.mark.parametrize("path", ["", None, 1])
def test_invalid_file_path_fails_before_endpoint_lookup(method, path):
    """Missing and non-string paths fail locally without checking filesystem policy."""
    recorder = Recorder(httpx.Response(200, json=sandbox_payload()))
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        args = (path, b"x") if method == "write_file" else (path,)
        with pytest.raises((ValueError, TypeError)):
            getattr(sandbox, method)(*args)
    assert len(recorder.requests) == 1


def test_file_write_accepts_exact_limit():
    """An exactly 10 MiB binary request is sent intact to the existing file API."""
    recorder = file_script(httpx.Response(201))
    with recorder.client() as client:
        client.get_sandbox(SBX).write_file("/workspace/file", b"x" * FILE_LIMIT)
    assert len(recorder.requests[-1].content) == FILE_LIMIT


@pytest.mark.parametrize("headers", [{}, {"content-length": str(FILE_LIMIT)}, {"content-length": "1"}])
def test_streamed_file_limit_counts_actual_bytes_and_closes(headers):
    """Oversized streams fail even if Content-Length is absent or lies about actual bytes."""
    stream = TrackedStream(b"x" * FILE_LIMIT, b"!", b"must not consume")
    recorder = file_script(httpx.Response(200, headers=headers, stream=stream))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.get_sandbox(SBX).read_file("/workspace/file")
    assert stream.closed
    assert stream.yielded <= 2
    assert recorder.requests[-1].headers["accept-encoding"] == "identity"


def test_streamed_file_exact_limit_is_returned_and_closed():
    """The inclusive file boundary succeeds without depending on a Content-Length header."""
    stream = TrackedStream(b"x" * (FILE_LIMIT // 2), b"x" * (FILE_LIMIT // 2))
    recorder = file_script(httpx.Response(200, stream=stream))
    with recorder.client() as client:
        assert client.get_sandbox(SBX).read_file("/workspace/file") == b"x" * FILE_LIMIT
    assert stream.closed


@pytest.mark.parametrize("headers", [
    {"content-length": str(FILE_LIMIT + 1)}, {"content-length": "-1"},
    {"content-length": "invalid"}, {"content-length": "5"},
])
def test_invalid_or_mismatched_file_content_length_is_protocol_error(headers):
    """Invalid declared sizes and truncated streamed responses cannot appear complete."""
    stream = TrackedStream(b"abc")
    recorder = file_script(httpx.Response(200, headers=headers, stream=stream))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.get_sandbox(SBX).read_file("/workspace/file")
    assert stream.closed


def test_streamed_file_network_failure_closes_response_without_retry():
    """Mid-body read errors close the response and never replay a file request."""
    stream = TrackedStream(b"partial", failure=httpx.ReadError("secret details"))
    recorder = file_script(httpx.Response(200, stream=stream))
    with recorder.client() as client, pytest.raises(NetworkError) as caught:
        client.get_sandbox(SBX).read_file("/workspace/file")
    assert stream.closed and len(recorder.requests) == 3
    assert "secret" not in str(caught.value)


@pytest.mark.parametrize("encoding", ["gzip", "deflate", "br"])
def test_compressed_file_response_is_rejected_without_decompression(encoding):
    """Unexpected content encodings cannot bypass actual raw-byte response limits."""
    stream = TrackedStream(gzip.compress(b"hello"))
    recorder = file_script(httpx.Response(200, headers={"content-encoding": encoding}, stream=stream))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.get_sandbox(SBX).read_file("/workspace/file")
    assert stream.closed


def test_preconsumed_file_body_still_obeys_limit():
    """MockTransport's preconsumed content branch cannot bypass the 10 MiB check."""
    recorder = file_script(httpx.Response(200, content=b"x" * (FILE_LIMIT + 1)))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.get_sandbox(SBX).read_file("/workspace/file")


def test_large_escaped_command_output_fits_json_budget():
    """Two legal 1 MiB control-character outputs fit the 16 MiB JSON response budget."""
    output = "\x01" * (1024 * 1024)
    body = json.dumps({"exit_code": 0, "stdout": output, "stderr": output}).encode()
    assert 12 * 1024 * 1024 < len(body) < JSON_LIMIT
    stream = TrackedStream(body[:8 * 1024 * 1024], body[8 * 1024 * 1024:])
    recorder = file_script(httpx.Response(200, stream=stream))
    with recorder.client() as client:
        result = client.get_sandbox(SBX).run(["true"])
    assert result.stdout == result.stderr == output
    assert stream.closed


@pytest.mark.parametrize("headers", [{}, {"content-length": "1"}])
def test_json_limit_counts_actual_raw_bytes_and_stops_early(headers):
    """Oversized JSON cannot consume unlimited memory even with a forged small length."""
    stream = TrackedStream(b" " * JSON_LIMIT, b"!", b"must not consume")
    recorder = Recorder(httpx.Response(201, headers=headers, stream=stream))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.create_sandbox()
    assert stream.closed and stream.yielded <= 2


def test_json_exact_limit_is_accepted():
    """A valid JSON response plus whitespace at exactly 16 MiB remains legal."""
    body = json.dumps(sandbox_payload()).encode()
    stream = TrackedStream(body, b" " * (JSON_LIMIT - len(body)))
    recorder = Recorder(httpx.Response(201, stream=stream))
    with recorder.client() as client:
        assert client.create_sandbox().sandbox_id == SBX
    assert stream.closed


def test_error_body_is_bounded_to_64_kib_and_preserves_http_status():
    """Oversized failure bodies stop reading early while retaining a safe HTTP error."""
    stream = TrackedStream(b"x" * (64 * 1024), b"!", b"must not consume")
    recorder = Recorder(httpx.Response(503, stream=stream))
    with recorder.client() as client, pytest.raises(APIError) as caught:
        client.create_sandbox()
    assert caught.value.status_code == 503
    assert caught.value.code == "http_error"
    assert stream.closed and stream.yielded <= 2


@pytest.mark.parametrize("status", [201, 503])
def test_truncated_json_stream_is_closed(status):
    """Declared JSON length mismatches close streams for both successful and failed HTTP."""
    stream = TrackedStream(b"{}")
    recorder = Recorder(httpx.Response(status, headers={"content-length": "99"}, stream=stream))
    expected = ProtocolError if status == 201 else APIError
    with recorder.client() as client, pytest.raises(expected):
        client.create_sandbox()
    assert stream.closed


def test_compressed_json_is_rejected_without_decoding():
    """The JSON path enforces identity encoding as strictly as binary file reads."""
    stream = TrackedStream(gzip.compress(json.dumps(sandbox_payload()).encode()))
    recorder = Recorder(httpx.Response(201, headers={"content-encoding": "gzip"}, stream=stream))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.create_sandbox()
    assert stream.closed


@pytest.mark.parametrize("status", [200, 204])
def test_file_write_requires_exact_201_success(status):
    """File writes reject unexpected success statuses instead of silently accepting drift."""
    recorder = file_script(httpx.Response(status))
    with recorder.client() as client, pytest.raises(ProtocolError):
        client.get_sandbox(SBX).write_file("/workspace/file", b"x")


@pytest.mark.parametrize("operation", ["read_file", "write_file"])
@pytest.mark.parametrize("response,expected", [
    (httpx.Response(307, headers={"location": "http://evil.example/collect"}), APIError),
    (httpx.Response(503, json={"error": {"code": "busy", "message": "later"}}), APIError),
    (httpx.WriteError("uncertain write"), NetworkError),
])
def test_file_failure_never_follows_redirect_or_replays_io(operation, response, expected):
    """A failed file request has no hidden refresh, redirected request or write retry."""
    recorder = file_script(response)
    with recorder.client() as client:
        sandbox = client.get_sandbox(SBX)
        args = ("/workspace/file", b"x") if operation == "write_file" else ("/workspace/file",)
        with pytest.raises(expected):
            getattr(sandbox, operation)(*args)
    assert len(recorder.requests) == 3


def test_file_paths_are_server_validated_without_client_filesystem_access():
    """The SDK encodes path semantics for the agent rather than duplicating server policy."""
    recorder = file_script(httpx.Response(400, json={"error": {"code": "invalid_path", "message": "bad path"}}))
    with recorder.client() as client, pytest.raises(APIError) as caught:
        client.get_sandbox(SBX).read_file("../outside")
    assert caught.value.code == "invalid_path"
    assert recorder.requests[-1].url.params["path"] == "../outside"



def test_stream_read_timeout_closes_response_and_retains_timeout_type():
    """A mid-body timeout retains its SDK subtype while releasing the HTTP response."""
    stream = TrackedStream(b"partial", failure=httpx.ReadTimeout("secret timeout"))
    recorder = file_script(httpx.Response(200, stream=stream))
    with recorder.client() as client, pytest.raises(SDKTimeoutError):
        client.get_sandbox(SBX).read_file("/workspace/file")
    assert stream.closed and len(recorder.requests) == 3
