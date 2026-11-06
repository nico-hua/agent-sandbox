"""Explicit read-only Minikube probe; collection never loads kubeconfig or opens a client."""

import os
import secrets
from pathlib import Path

import httpx
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


async def test_authenticated_http_readiness_for_explicit_namespace() -> None:
    """Readiness reaches Minikube through HTTP and Service without touching any real metadata store."""
    from control_plane.app import create_app
    from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
    from lifecycle_fakes import FakeLifecycleStore

    runtime = KubernetesSandboxRuntime(
        kubeconfig_path=Path(os.environ.get("KUBERNETES_TEST_KUBECONFIG", str(Path.home() / ".kube" / "config"))),
        context=os.environ["KUBERNETES_TEST_CONTEXT"],
        namespace=os.environ["KUBERNETES_TEST_NAMESPACE"],
    )
    key = secrets.token_urlsafe(32)
    store = FakeLifecycleStore()
    application = create_app(
        runtime=runtime, store=store, api_key=key,
        reaper_enabled=False, pending_recovery_enabled=False,
    )
    async with application.router.lifespan_context(application):
        assert application.state.reaper_task is None
        assert application.state.pending_recovery_result is None
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://control") as http:
            assert (await http.get("/healthz")).json() == {"status": "ok"}
            for credentials in ({}, {"Authorization": "Bearer wrong-test-key"}):
                assert (await http.get("/readyz", headers=credentials)).status_code == 401
            response = await http.get("/readyz", headers={"Authorization": "Bearer " + key})
            assert response.status_code == 200 and response.json() == {"status": "ready"}
    assert store.initialize_calls == 1 and not store.records and not store.credentials
