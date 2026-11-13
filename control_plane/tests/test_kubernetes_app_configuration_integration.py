"""Explicit read-only HTTP validation of an environment-configured Kubernetes application."""

import os
import secrets
from pathlib import Path

import httpx
import pytest


pytestmark = [
    pytest.mark.kubernetes_integration,
    pytest.mark.anyio,
    pytest.mark.timeout(20),
    pytest.mark.skipif(
        os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
        or not os.getenv("KUBERNETES_TEST_CONTEXT")
        or not os.getenv("KUBERNETES_TEST_NAMESPACE"),
        reason="explicit integration opt-in, context and namespace are required before any client is loaded",
    ),
]


async def test_environment_assembled_application_probes_only_explicit_namespace(monkeypatch):
    """Selected configuration reaches real Kubernetes through authenticated HTTP without metadata or cluster writes."""
    from control_plane.app import create_app
    from control_plane.runtime.kubernetes import kubernetes_runtime
    from lifecycle_fakes import FakeLifecycleStore

    path = str(Path(os.environ.get("KUBERNETES_TEST_KUBECONFIG", str(Path.home() / ".kube" / "config"))))
    context = os.environ["KUBERNETES_TEST_CONTEXT"]
    namespace = os.environ["KUBERNETES_TEST_NAMESPACE"]
    for key, value in {
        "AGENT_SANDBOX_RUNTIME": "kubernetes", "AGENT_SANDBOX_K8S_KUBECONFIG": path,
        "AGENT_SANDBOX_K8S_CONTEXT": context, "AGENT_SANDBOX_K8S_NAMESPACE": namespace,
        "AGENT_SANDBOX_K8S_AGENT_IMAGE": "agent-sandbox-basic:dev",
        "AGENT_SANDBOX_K8S_PROXY_IMAGE": "agent-sandbox-proxy-upstream-test:dev",
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv("AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", raising=False)
    original_factory = kubernetes_runtime.create_probe_client
    attempts = []

    def counted_factory(**kwargs):
        """Count real acquisitions and require the exact trusted target configuration."""
        assert kwargs == {"kubeconfig_path": Path(path), "context": context}
        attempts.append(True)
        return original_factory(**kwargs)

    monkeypatch.setattr(kubernetes_runtime, "create_probe_client", counted_factory)
    key = secrets.token_urlsafe(32)
    store = FakeLifecycleStore()
    application = create_app(api_key=key, store=store, reaper_enabled=False, pending_recovery_enabled=False)
    assert isinstance(application.state.runtime, kubernetes_runtime.KubernetesSandboxRuntime)
    assert not attempts
    async with application.router.lifespan_context(application):
        assert application.state.kubernetes_entry_manager.session_count == 0
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://isolated-control", trust_env=False,
        ) as http:
            health = await http.get("/healthz")
            assert health.status_code == 200 and health.json() == {"status": "ok"}
            for headers in ({}, {"Authorization": "Bearer wrong-test-key"}):
                assert (await http.get("/readyz", headers=headers)).status_code == 401
            assert not attempts
            ready = await http.get("/readyz", headers={"Authorization": "Bearer " + key})
            assert ready.status_code == 200 and ready.json() == {"status": "ready"}
            assert len(attempts) == 1
    assert store.initialize_calls == 1 and not store.records and not store.credentials
