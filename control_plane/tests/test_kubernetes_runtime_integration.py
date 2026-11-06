"""Explicit read-only Minikube probe; collection never loads kubeconfig or opens a client."""

import os
from pathlib import Path

import pytest


pytestmark = [
    pytest.mark.kubernetes_integration,
    pytest.mark.anyio,
    pytest.mark.timeout(15),
    pytest.mark.skipif(
        os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
        or not os.getenv("KUBERNETES_TEST_CONTEXT")
        or not os.getenv("KUBERNETES_TEST_NAMESPACE"),
        reason="explicit opt-in, Kubernetes context and namespace are all required",
    ),
]


async def test_explicit_namespace_ping() -> None:
    """List at most one Pod in the specified existing namespace without mutating any cluster resources."""
    # Import and construct only after the explicit skip guard, including for a missing kubeconfig.
    from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime

    runtime = KubernetesSandboxRuntime(
        kubeconfig_path=Path(os.environ.get("KUBERNETES_TEST_KUBECONFIG", str(Path.home() / ".kube" / "config"))),
        context=os.environ["KUBERNETES_TEST_CONTEXT"],
        namespace=os.environ["KUBERNETES_TEST_NAMESPACE"],
    )
    assert await runtime.ping() is None
