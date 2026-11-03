"""Opt-in HTTP-only SDK validation against an explicitly isolated local service."""

import os
import sys
from collections.abc import Iterator

import pytest

from agent_sandbox import APIError, Sandbox, SandboxClient, Workspace


@pytest.fixture
def isolated_client() -> Iterator[SandboxClient]:
    """Skip before opening HTTP unless all three dedicated-service safeguards are present."""
    control_url = os.environ.get("SDK_TEST_CONTROL_URL")
    if (
        os.environ.get("RUN_SDK_INTEGRATION") != "1"
        or not control_url
        or os.environ.get("SDK_TEST_ISOLATED") != "1"
    ):
        pytest.skip("requires RUN_SDK_INTEGRATION=1, SDK_TEST_CONTROL_URL and SDK_TEST_ISOLATED=1")
    with SandboxClient(control_url, timeout=10.0) as client:
        yield client


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
def test_workspace_reuse_lifecycle(isolated_client: SandboxClient) -> None:
    """Exercise binary files, command results, occupancy and lifecycle over two workspace holders."""
    sandboxes: list[Sandbox] = []
    workspace = None
    try:
        workspace = isolated_client.create_workspace()
        first = isolated_client.create_sandbox(workspace_id=workspace.workspace_id, ttl_seconds=300)
        sandboxes.append(first)
        assert first.wait_ready(timeout=60).sandbox_id == first.sandbox_id
        assert first.info.workspace_id == workspace.workspace_id
        assert isolated_client.get_sandbox(first.sandbox_id).sandbox_id == first.sandbox_id
        assert isolated_client.get_workspace(workspace.workspace_id).workspace_id == workspace.workspace_id
        assert workspace.refresh().sandbox_id == first.sandbox_id

        path = "sdk-integration.bin"
        original = b"workspace SDK bytes\x00\xff\r\n"
        first.write_file(path, original)
        assert first.read_file(path) == original
        result = first.run(["sh", "-c", "printf stdout; printf stderr >&2; exit 7"])
        assert (result.exit_code, result.stdout, result.stderr) == (7, "stdout", "stderr")
        with pytest.raises(APIError) as occupied:
            unexpected = isolated_client.create_sandbox(workspace_id=workspace.workspace_id, ttl_seconds=300)
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

        second = isolated_client.create_sandbox(workspace_id=workspace.workspace_id, ttl_seconds=300)
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
    except APIError as error:
        diagnostic_ids = [value for value in (error.sandbox_id, error.workspace_id) if value]
        if diagnostic_ids:
            report = "API diagnostic IDs requiring outcome inspection: " + ", ".join(diagnostic_ids)
            print(report, file=sys.stderr)
            error.add_note(report)
        raise
    finally:
        cleanup_created(sandboxes, workspace)
