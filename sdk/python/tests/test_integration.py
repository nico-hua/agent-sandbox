"""Opt-in HTTP-only SDK validation against an explicitly isolated local service."""

import os
import sys
from collections.abc import Iterator

import httpx
import pytest

from agent_sandbox import APIError, Sandbox, SandboxClient, Workspace


class AgentAuthorizationAudit(httpx.BaseTransport):
    """Forward real HTTP while retaining only data-plane request counts and header presence."""

    def __init__(self) -> None:
        """Create an ordinary bounded-network transport without retaining credentials."""
        self._transport = httpx.HTTPTransport(trust_env=False, retries=0)
        self.command_requests = 0
        self.file_requests = 0
        self.authorization_seen = False
        self.admin_authorization_seen = False

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        """Check command/file requests before forwarding without saving header values."""
        if request.url.path in ("/v1/commands:run", "/v1/files"):
            if request.url.path == "/v1/commands:run":
                self.command_requests += 1
            else:
                self.file_requests += 1
            self.authorization_seen |= "authorization" in request.headers
            self.admin_authorization_seen |= (
                request.headers.get("authorization") == "Bearer " + os.environ["SDK_TEST_API_KEY"]
            )
            # Evaluate secret-bearing values only transiently; retained observations
            # and failure messages contain counts or booleans, never credentials.
            if not request.headers.get("authorization", "").startswith("Bearer "):
                raise AssertionError("Agent request lacks instance Authorization")
            if self.admin_authorization_seen:
                raise AssertionError("Agent request carried the admin credential")
        return self._transport.handle_request(request)

    def close(self) -> None:
        """Close the underlying pool owned by the SDK client."""
        self._transport.close()


@pytest.fixture
def isolated_client() -> Iterator[tuple[SandboxClient, AgentAuthorizationAudit]]:
    """Require isolation gates and an explicit key before HTTP or resource allocation."""
    control_url = os.environ.get("SDK_TEST_CONTROL_URL")
    if (
        os.environ.get("RUN_SDK_INTEGRATION") != "1"
        or not control_url
        or os.environ.get("SDK_TEST_ISOLATED") != "1"
    ):
        pytest.skip("requires RUN_SDK_INTEGRATION=1, SDK_TEST_CONTROL_URL and SDK_TEST_ISOLATED=1")
    api_key = os.environ.get("SDK_TEST_API_KEY")
    if not api_key:
        pytest.fail("opted-in integration requires SDK_TEST_API_KEY")
    audit = AgentAuthorizationAudit()
    with SandboxClient(control_url, api_key=api_key, timeout=10.0, transport=audit) as client:
        # Prove authentication before any successful creation can allocate resources.
        wrong_key = "incorrect-integration-key" if api_key != "incorrect-integration-key" else "other-key"
        for candidate in (None, wrong_key):
            with SandboxClient(control_url, api_key=candidate, timeout=10.0) as unauthorized:
                with pytest.raises(APIError) as denied:
                    unauthorized.get_workspace("wsp_" + "0" * 32)
                assert (denied.value.status_code, denied.value.code) == (401, "unauthorized")
        yield client, audit


def cleanup_created(sandboxes: list[Sandbox], workspace: Workspace | None) -> None:
    """Attempt every owned deletion and preserve any active test exception with an ID report."""
    primary = sys.exception()
    failures = []
    for sandbox in reversed(sandboxes):
        try:
            sandbox.delete()
        except Exception as error:
            failures.append(f"sandbox {sandbox.sandbox_id}: {type(error).__name__}")
    if workspace is not None:
        try:
            workspace.delete()
        except Exception as error:
            failures.append(f"workspace {workspace.workspace_id}: {type(error).__name__}")
    if failures:
        report = "SDK integration cleanup incomplete; remaining IDs: " + "; ".join(failures)
        print(report, file=sys.stderr)
        if primary is not None:
            primary.add_note(report)
        else:
            raise AssertionError(report)


@pytest.mark.sdk_integration
@pytest.mark.timeout(240)
def test_workspace_reuse_lifecycle(
    isolated_client: tuple[SandboxClient, AgentAuthorizationAudit],
) -> None:
    """Exercise binary files, command results, occupancy and lifecycle over two workspace holders."""
    client, audit = isolated_client
    sandboxes: list[Sandbox] = []
    workspace = None
    try:
        workspace = client.create_workspace()
        first = client.create_sandbox(workspace_id=workspace.workspace_id, ttl_seconds=300)
        sandboxes.append(first)
        assert first.wait_ready(timeout=60).sandbox_id == first.sandbox_id
        assert first.info.workspace_id == workspace.workspace_id
        assert client.get_sandbox(first.sandbox_id).sandbox_id == first.sandbox_id
        assert client.get_workspace(workspace.workspace_id).workspace_id == workspace.workspace_id
        assert workspace.refresh().sandbox_id == first.sandbox_id

        environment = first.run(["sh", "-c",
                                 'test "${AGENT_SANDBOX_API_KEY+x}" != x && '
                                 'test "${SANDBOX_ACCESS_TOKEN+x}" != x'])
        assert environment.exit_code == 0, "authentication variable reached the workload"
        assert environment.stdout == environment.stderr == ""

        path = "sdk-integration.bin"
        original = b"workspace SDK bytes\x00\xff\r\n"
        first.write_file(path, original)
        assert first.read_file(path) == original
        result = first.run(["sh", "-c", "printf stdout; printf stderr >&2; exit 7"])
        assert (result.exit_code, result.stdout, result.stderr) == (7, "stdout", "stderr")
        with pytest.raises(APIError) as occupied:
            unexpected = client.create_sandbox(workspace_id=workspace.workspace_id, ttl_seconds=300)
            # Retain even an unexpected successful allocation so a regression cannot leak it.
            sandboxes.append(unexpected)
        assert (occupied.value.status_code, occupied.value.code) == (409, "workspace_conflict")
        with pytest.raises(APIError) as protected:
            workspace.delete()
        assert (protected.value.status_code, protected.value.code) == (409, "workspace_conflict")

        first.pause()
        assert first.refresh().status == "paused"
        first.resume()
        assert first.wait_ready(timeout=60).status == "ready"
        renewed = first.renew(600)
        assert renewed.sandbox_id == first.sandbox_id
        assert first.info.expires_at == renewed.expires_at
        first.delete()
        sandboxes.remove(first)
        assert workspace.refresh().status == "available"

        second = client.create_sandbox(workspace_id=workspace.workspace_id, ttl_seconds=300)
        sandboxes.append(second)
        assert second.sandbox_id != first.sandbox_id
        second.wait_ready(timeout=60)
        assert second.read_file(path) == original
        updated = second.run(["sh", "-c", "printf '%s' sdk-second-holder > /workspace/sdk-integration.bin"])
        assert updated.exit_code == 0
        assert second.read_file(path) == b"sdk-second-holder"
        second.delete()
        sandboxes.remove(second)
        workspace.delete()
        workspace = None
        assert audit.command_requests >= 3
        assert audit.file_requests >= 4
        assert audit.authorization_seen
        assert not audit.admin_authorization_seen
    except APIError as error:
        diagnostic_ids = [value for value in (error.sandbox_id, error.workspace_id) if value]
        if diagnostic_ids:
            report = "API diagnostic IDs requiring outcome inspection: " + ", ".join(diagnostic_ids)
            print(report, file=sys.stderr)
            error.add_note(report)
        raise
    finally:
        cleanup_created(sandboxes, workspace)
