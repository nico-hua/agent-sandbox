"""Reuse one explicitly owned workspace through the installed synchronous SDK."""

import argparse
import os
import sys

from agent_sandbox import APIError, Sandbox, SandboxClient, Workspace


def cleanup_owned(sandboxes: list[Sandbox], workspace: Workspace | None) -> None:
    """Delete only this run's resources, retaining the original failure and reporting IDs."""
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
        report = "Cleanup incomplete; inspect remaining resource IDs: " + "; ".join(failures)
        print(report, file=sys.stderr)
        if primary is not None:
            primary.add_note(report)
        else:
            raise RuntimeError(report)


def run_example(base_url: str) -> None:
    """Create a workspace and two sequential holders, then explicitly delete all resources."""
    sandboxes: list[Sandbox] = []
    workspace = None
    api_key = os.environ["AGENT_SANDBOX_API_KEY"]
    with SandboxClient(base_url, api_key=api_key, timeout=10.0) as client:
        try:
            workspace = client.create_workspace()
            print(f"Created workspace {workspace.workspace_id}", flush=True)
            first = client.create_sandbox(workspace_id=workspace.workspace_id, ttl_seconds=300)
            sandboxes.append(first)
            print(f"Created sandbox A {first.sandbox_id}", flush=True)
            first.wait_ready(timeout=60)
            path = "sdk-reuse.bin"
            original = b"retained bytes\x00\xff\n"
            first.write_file(path, original)
            if first.read_file(path) != original:
                raise RuntimeError("uploaded bytes did not round-trip")
            first.delete()
            sandboxes.remove(first)

            second = client.create_sandbox(workspace_id=workspace.workspace_id, ttl_seconds=300)
            sandboxes.append(second)
            print(f"Created sandbox B {second.sandbox_id}", flush=True)
            second.wait_ready(timeout=60)
            if second.read_file(path) != original:
                raise RuntimeError("workspace bytes did not survive holder deletion")
            # The upload API refuses overwrites; an explicit command changes the file.
            result = second.run(["sh", "-c", "printf '%s' sdk-updated > /workspace/sdk-reuse.bin"])
            if result.exit_code != 0 or second.read_file(path) != b"sdk-updated":
                raise RuntimeError("explicit file update failed")
            print("Workspace bytes survived reuse and were updated successfully.")
        except APIError as error:
            # A failed creation may expose an ID even though no handle was returned.
            diagnostic_ids = [value for value in (error.sandbox_id, error.workspace_id) if value]
            if diagnostic_ids:
                report = "API diagnostic resource IDs (inspect outcome): " + ", ".join(diagnostic_ids)
                print(report, file=sys.stderr)
                error.add_note(report)
            raise
        finally:
            cleanup_owned(sandboxes, workspace)


def main() -> None:
    """Parse the example's control URL and run the resource-owning demonstration."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--control-url", default="http://127.0.0.1:18083")
    args = parser.parse_args()
    run_example(args.control_url)


if __name__ == "__main__":
    main()
