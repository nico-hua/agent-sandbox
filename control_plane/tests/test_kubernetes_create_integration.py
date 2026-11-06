"""Double-opt-in real Runtime creation using cached images and independently guarded fixture cleanup."""

import asyncio
import os
import secrets
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from kubernetes import client

from control_plane.core.errors import CreationResourceState, SandboxCreateFailed, SandboxNotFound
from control_plane.core.models import SandboxSpec
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime, create_probe_client
from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
from test_kubernetes_query_integration import _delete_owned, _loaded_image_node, _verify_owned

pytestmark = [
    pytest.mark.kubernetes_integration,
    pytest.mark.kubernetes_resource_integration,
    pytest.mark.timeout(120),
    pytest.mark.skipif(
        os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
        or os.getenv("RUN_KUBERNETES_RESOURCE_INTEGRATION") != "1"
        or not os.getenv("KUBERNETES_TEST_CONTEXT")
        or not os.getenv("KUBERNETES_TEST_NAMESPACE"),
        reason="both Kubernetes opt-ins, explicit context and namespace are required for Runtime creation",
    ),
]


@pytest.fixture
def owned_runtime_creation():
    """Prepare one unique transaction and an independent client that never relies on Runtime rollback for cleanup."""
    path = Path(os.environ.get("KUBERNETES_TEST_KUBECONFIG", str(Path.home() / ".kube" / "config")))
    context, namespace = os.environ["KUBERNETES_TEST_CONTEXT"], os.environ["KUBERNETES_TEST_NAMESPACE"]
    api = create_probe_client(kubeconfig_path=path, context=context)
    created = datetime(2026, 11, 6, 8, 0, 0, 123456, tzinfo=UTC)
    spec = SandboxSpec(
        "sbx_" + secrets.token_hex(16), "unused", "unused", "unused",
        transaction_id="txn_" + secrets.token_hex(16), created_at=created,
        expires_at=created + timedelta(minutes=30), access_token="Q" * 43,
    )
    pod = build_pod_spec(spec, namespace=namespace, agent_image="agent-sandbox-basic:dev")
    expected = {"name": pod.metadata.name, "namespace": namespace,
                "labels": dict(pod.metadata.labels), "annotations": dict(pod.metadata.annotations)}
    receipt = {"uid": None, "posts": 0}

    class RecordingApi:
        """Capture original POST UID without changing requests or lifecycle behavior."""

        def __init__(self, delegate):
            """Retain one worker-owned official client without creating any extra resources."""
            self.delegate = delegate

        def __getattr__(self, name):
            """Forward bounded operations and the owned close handle to the official client."""
            return getattr(self.delegate, name)

        def create_namespaced_pod(self, *args, **kwargs):
            """Record exactly the original response UID for the independent fallback path."""
            receipt["posts"] += 1
            response = self.delegate.create_namespaced_pod(*args, **kwargs)
            _verify_owned(response, expected, None)
            receipt["uid"] = response.metadata.uid
            return response

    def factory(**kwargs):
        """Create a fresh SDK client in whichever Runtime worker owns this operation."""
        return RecordingApi(create_probe_client(**kwargs))

    def make_runtime(**changes):
        """Keep explicit target configuration fixed while permitting the test's unique missing image."""
        return KubernetesSandboxRuntime(kubeconfig_path=path, context=context, namespace=namespace,
                                        client_factory=factory, **changes)

    try:
        _loaded_image_node(api)
        with pytest.raises(client.ApiException) as before:
            api.read_namespaced_pod(expected["name"], namespace, _request_timeout=(0.5, 0.5))
        assert before.value.status == 404
        yield make_runtime, spec, api, expected, receipt, path, context
    finally:
        try:
            # Independent of Runtime.create()/delete(), with original UID when
            # available and complete unique ownership before any fallback delete.
            _delete_owned(api, expected, receipt["uid"])
        finally:
            api.api_client.close()


def test_real_runtime_create_starts_agent_and_preserves_non_root_ephemeral_security(owned_runtime_creation):
    """Create a real Pod, observe ready, verify its writable mounts and read-only root, then delete it."""
    make_runtime, spec, api, expected, receipt, path, context = owned_runtime_creation
    runtime = make_runtime()

    async def verify():
        """Keep Runtime-only creation distinct from readiness and from the test's controlled exec."""
        assert await runtime.create(spec) is None
        deadline = time.monotonic() + 45
        while True:
            result = await runtime.get(spec.sandbox_id)
            assert result.status in {"running", "ready"}
            if result.status == "ready":
                break
            assert time.monotonic() < deadline, "Agent readiness was not observed"
            await asyncio.sleep(0.1)
        assert (result.transaction_id, result.created_at, result.expires_at) == (
            spec.transaction_id, spec.created_at, spec.expires_at)
        current = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        _verify_owned(current, expected, receipt["uid"])
        assert current.spec.containers[0].image_pull_policy == "Never"
        assert current.spec.automount_service_account_token is False
        assert current.spec.containers[0].env in (None, [])
        assert "Q" * 43 not in str(current.to_dict())
        command = (
            'set -eu; test "$(id -u)" = 1000; test "$(id -g)" = 1000; '
            'printf workspace > /workspace/runtime-create-test; '
            'test "$(cat /workspace/runtime-create-test)" = workspace; '
            'printf temporary > /tmp/runtime-create-test; '
            'test "$(cat /tmp/runtime-create-test)" = temporary; '
            'awk \'$2 == "/" && $4 ~ /(^|,)ro(,|$)/ {found=1} END {exit !found}\' /proc/mounts; '
            'if (printf forbidden > /runtime-create-root-test) 2>/dev/null; then exit 7; fi; '
            'printf "non-root mounts-writable root-readonly\\n"'
        )
        completed = subprocess.run(
            ["kubectl", "--kubeconfig", str(path), "--context", context, "--namespace", expected["namespace"],
             "--request-timeout=5s", "exec", expected["name"], "-c", "agent", "--", "/bin/sh", "-c", command],
            capture_output=True, text=True, timeout=10, check=False,
        )
        assert completed.returncode == 0, "controlled security/mount exec failed"
        assert completed.stdout == "non-root mounts-writable root-readonly\n"
        after = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        _verify_owned(after, expected, receipt["uid"])
        assert receipt["posts"] == 1
        await runtime.delete(spec.sandbox_id, expected_transaction_id=spec.transaction_id)
        with pytest.raises(SandboxNotFound):
            await runtime.get(spec.sandbox_id)

    asyncio.run(verify())


def test_missing_cached_image_fails_startup_and_confirms_precise_rollback(owned_runtime_creation):
    """Never-pull missing images fail locally; startup failure removes exactly the created Pod UID."""
    make_runtime, spec, api, expected, receipt, _, _ = owned_runtime_creation
    runtime = make_runtime(agent_image="agent-sandbox-missing-" + secrets.token_hex(8) + ":never")

    async def verify():
        """Assert positive creation receipt followed by a confirmed rolled-back resource result."""
        with pytest.raises(SandboxCreateFailed) as captured:
            await runtime.create(spec)
        assert captured.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
        assert receipt["posts"] == 1 and receipt["uid"] is not None
        with pytest.raises(client.ApiException) as after:
            api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        assert after.value.status == 404

    asyncio.run(verify())
