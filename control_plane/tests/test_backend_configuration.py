"""Trusted backend selection, inert application assembly and backend-specific environment admission."""

import asyncio
import os
import sqlite3
import subprocess
import sys
from dataclasses import FrozenInstanceError
from pathlib import Path

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.service import SandboxService
from control_plane.runtime.configuration import BackendConfiguration, build_runtime, load_backend_configuration
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from lifecycle_fakes import FakeLifecycleStore
from test_sandbox_service import FakeRuntime


def configure_kubernetes(monkeypatch):
    """Supply only trusted inert target values; the kubeconfig deliberately does not exist."""
    values = {
        "AGENT_SANDBOX_RUNTIME": "kubernetes",
        "AGENT_SANDBOX_K8S_KUBECONFIG": "/not-read/kubeconfig",
        "AGENT_SANDBOX_K8S_CONTEXT": "explicit-test",
        "AGENT_SANDBOX_K8S_NAMESPACE": "test-sandboxes",
        "AGENT_SANDBOX_K8S_AGENT_IMAGE": "agent-sandbox-basic:dev",
        "AGENT_SANDBOX_K8S_PROXY_IMAGE": "agent-sandbox-proxy-upstream-test:dev",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    return values


@pytest.mark.anyio
async def test_configured_kubernetes_selects_real_backend_and_rejects_unregistered_environment(monkeypatch):
    """Kubernetes selection must not accidentally register Docker's python/node presets or write pending."""
    configure_kubernetes(monkeypatch)
    store = FakeLifecycleStore()
    app = create_app(store=store, api_key="synthetic-config-key", reaper_enabled=False)
    assert isinstance(app.state.runtime, KubernetesSandboxRuntime)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as http:
        for name in ("python", "node", "tools"):
            response = await http.post("/v1/sandboxes", json={"environment": name},
                                       headers={"Authorization": "Bearer synthetic-config-key"})
            assert response.status_code == 400 and response.json()["error"]["code"] == "invalid_request"
    assert not store.records and not store.credentials


@pytest.mark.parametrize("value", ["", "unknown", "Docker", " docker", "kubernetes\n"])
def test_unknown_or_empty_backend_fails_in_assembly(monkeypatch, value):
    """An invalid trusted selection cannot silently fall back to Docker."""
    monkeypatch.setenv("AGENT_SANDBOX_RUNTIME", value)
    with pytest.raises(ValueError):
        create_app(api_key="synthetic-config-key", store=FakeLifecycleStore())


@pytest.mark.parametrize("names", [{"basic"}, {"python", "node"}, {"tools"}])
def test_service_accepts_nonempty_valid_backend_environment_subsets(names):
    """Backend environment names need not contain presets belonging to other runtimes."""
    SandboxService(FakeRuntime(), FakeLifecycleStore(), environment_names=names)


@pytest.mark.parametrize("selection", [None, "docker"])
def test_default_and_explicit_docker_ignore_unselected_kubernetes_values(monkeypatch, selection):
    """Broken or incomplete Kubernetes settings cannot affect a Docker application."""
    monkeypatch.delenv("AGENT_SANDBOX_RUNTIME", raising=False)
    if selection is not None:
        monkeypatch.setenv("AGENT_SANDBOX_RUNTIME", selection)
    monkeypatch.setenv("AGENT_SANDBOX_K8S_NAMESPACE", "not a valid namespace")
    app = create_app(api_key="synthetic-config-key", store=FakeLifecycleStore(),
                     additional_environments={"tools": "agent-sandbox-tools:dev"})
    assert isinstance(app.state.runtime, DockerSandboxRuntime)
    assert set(app.state.runtime.environment_images) == {"basic", "python", "node", "tools"}


@pytest.mark.parametrize("field", ["KUBECONFIG", "CONTEXT", "NAMESPACE", "AGENT_IMAGE", "PROXY_IMAGE"])
@pytest.mark.parametrize("fault", ["missing", "empty"])
def test_selected_kubernetes_requires_all_target_and_proxy_fields(monkeypatch, field, fault):
    """Formal Kubernetes assembly never silently chooses a context, image or unauthenticated layout."""
    configure_kubernetes(monkeypatch)
    key = "AGENT_SANDBOX_K8S_" + field
    if fault == "missing":
        monkeypatch.delenv(key)
    else:
        monkeypatch.setenv(key, "")
    with pytest.raises(ValueError):
        create_app(api_key="synthetic-config-key", store=FakeLifecycleStore())


@pytest.mark.parametrize("field,value", [
    ("KUBECONFIG", " /private/kubeconfig"), ("KUBECONFIG", "/private/config\n"),
    ("CONTEXT", "wrong context"), ("CONTEXT", "\u4e2d\u6587"), ("NAMESPACE", "Bad"),
    ("NAMESPACE", "../default"), ("AGENT_IMAGE", "bad\nimage"), ("PROXY_IMAGE", "bad image"),
    ("WORKSPACE_STORAGE_CLASS", ""), ("WORKSPACE_STORAGE_CLASS", "Bad"),
])
def test_malformed_kubernetes_settings_fail_without_exposing_the_value(monkeypatch, field, value):
    """Both parser and inert Runtime validation reject invalid configuration with fixed diagnostics."""
    configure_kubernetes(monkeypatch)
    monkeypatch.setenv("AGENT_SANDBOX_K8S_" + field, value)
    with pytest.raises(ValueError) as caught:
        create_app(api_key="synthetic-config-key", store=FakeLifecycleStore())
    assert value not in str(caught.value) or value == ""
    assert "/private" not in str(caught.value)


@pytest.mark.parametrize("storage", [False, True])
def test_selected_configuration_is_immutable_and_does_not_reread_environment(monkeypatch, storage):
    """A captured target remains unchanged when external environment values later change."""
    configure_kubernetes(monkeypatch)
    if storage:
        monkeypatch.setenv("AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", "agent-sandbox-workspace")
    else:
        monkeypatch.delenv("AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", raising=False)
    configuration = load_backend_configuration()
    monkeypatch.setenv("AGENT_SANDBOX_RUNTIME", "docker")
    monkeypatch.setenv("AGENT_SANDBOX_K8S_CONTEXT", "other-cluster")
    with pytest.raises(FrozenInstanceError):
        configuration.context = "changed-cluster"
    backend = build_runtime(configuration)
    assert isinstance(backend, KubernetesSandboxRuntime)
    assert backend._context == "explicit-test" and backend._namespace == "test-sandboxes"
    assert backend._proxy_image == "agent-sandbox-proxy-upstream-test:dev"
    assert backend._workspace_storage_class == ("agent-sandbox-workspace" if storage else None)
    assert "/not-read/kubeconfig" not in repr(configuration)


def test_factory_cannot_build_anonymous_layout_from_incomplete_snapshot():
    """Even an explicitly constructed formal configuration must provide the proxy image."""
    configuration = BackendConfiguration(
        name="kubernetes", kubeconfig_path="/not-read", context="test", namespace="test",
        agent_image="agent-sandbox-basic:dev",
    )
    with pytest.raises(ValueError):
        build_runtime(configuration)


@pytest.mark.parametrize("injected", ["docker", "kubernetes", "custom"])
def test_explicit_runtime_ignores_backend_environment_selection(monkeypatch, injected):
    """Invalid or conflicting selection cannot replace or reconfigure an injected Runtime."""
    monkeypatch.setenv("AGENT_SANDBOX_RUNTIME", "invalid")
    monkeypatch.setenv("AGENT_SANDBOX_K8S_CONTEXT", "wrong-cluster")
    if injected == "docker":
        backend = DockerSandboxRuntime()
    elif injected == "kubernetes":
        backend = KubernetesSandboxRuntime(kubeconfig_path="/not-read", context="original", namespace="original")
    else:
        backend = FakeRuntime()
    app = create_app(backend, api_key="synthetic-config-key", store=FakeLifecycleStore())
    assert app.state.runtime is backend
    if injected == "kubernetes":
        assert backend._context == "original" and backend._proxy_image is None


@pytest.mark.parametrize("additional", [{}, {"tools": "tools:dev"}, "invalid"])
def test_kubernetes_rejects_custom_environment_configuration(monkeypatch, additional):
    """Kubernetes does not silently ignore registration that only Docker can implement."""
    configure_kubernetes(monkeypatch)
    with pytest.raises(ValueError, match="custom environments"):
        create_app(api_key="synthetic-config-key", store=FakeLifecycleStore(), additional_environments=additional)


def test_kubernetes_never_uses_docker_image_registration_or_external_io(monkeypatch, tmp_path):
    """Selected Kubernetes assembly is independent of Docker and defers every backend/storage operation."""
    import control_plane.app as app_module
    import control_plane.runtime.docker.transaction as transaction
    import docker
    from kubernetes import client, config

    configure_kubernetes(monkeypatch)
    database = tmp_path / "not-created" / "kubernetes.sqlite3"

    def forbidden(*args, **kwargs):
        """Fail at any accidental external boundary without performing the guarded operation."""
        pytest.fail("Kubernetes assembly attempted I/O or Docker registration")

    for obj, attribute in (
        (app_module, "build_environment_images"), (transaction, "build_environment_images"),
        (docker, "from_env"), (sqlite3, "connect"), (client, "ApiClient"),
        (config, "load_kube_config"), (Path, "open"), (asyncio, "create_subprocess_exec"),
    ):
        monkeypatch.setattr(obj, attribute, forbidden)
    app = create_app(api_key="synthetic-config-key", database_path=database)
    assert isinstance(app.state.runtime, KubernetesSandboxRuntime)
    assert app.state.reaper_task is None and app.state.kubernetes_entry_manager is None
    assert app.state.pending_recovery_result is None and not database.parent.exists()


def test_database_defaults_are_distinct_and_trusted_path_is_snapshotted(monkeypatch, tmp_path):
    """Switching backend cannot implicitly open Docker's default database; explicit Store/path wins."""
    import control_plane.app as app_module
    monkeypatch.delenv("AGENT_SANDBOX_DATABASE_PATH", raising=False)
    monkeypatch.setenv("AGENT_SANDBOX_RUNTIME", "docker")
    docker_app = create_app(api_key="synthetic-config-key")
    configure_kubernetes(monkeypatch)
    kube_app = create_app(api_key="synthetic-config-key")
    assert docker_app.state.lifecycle_store.database_path == app_module.DEFAULT_DATABASE_PATH
    assert kube_app.state.lifecycle_store.database_path == app_module.DEFAULT_KUBERNETES_DATABASE_PATH
    configured = tmp_path / "configured.sqlite3"
    monkeypatch.setenv("AGENT_SANDBOX_DATABASE_PATH", str(configured))
    app = create_app(api_key="synthetic-config-key")
    monkeypatch.setenv("AGENT_SANDBOX_DATABASE_PATH", "invalid\npath")
    assert app.state.lifecycle_store.database_path == configured
    explicit = tmp_path / "explicit.sqlite3"
    assert create_app(api_key="synthetic-config-key", database_path=explicit).state.lifecycle_store.database_path == explicit
    fake = FakeLifecycleStore()
    assert create_app(api_key="synthetic-config-key", store=fake).state.lifecycle_store is fake
    with pytest.raises(ValueError):
        create_app(api_key="synthetic-config-key")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.anyio
async def test_http_lifecycle_errors_are_authenticated_before_backend_access(monkeypatch):
    """An actual configured Runtime reports fixed 503/501 responses while unauthenticated traffic never enters it."""
    configure_kubernetes(monkeypatch)
    app = create_app(api_key="synthetic-config-key", store=FakeLifecycleStore(), reaper_enabled=False)
    attempts = []

    def unavailable(**kwargs):
        """Represent an unreachable API Server without constructing any SDK client."""
        attempts.append(True)
        raise ConnectionError("private-api-server /private/certificate")

    app.state.runtime._client_factory = unavailable
    path = "/v1/sandboxes/sbx_0123456789abcdef0123456789abcdef"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as http:
        assert (await http.get(path)).status_code == 401
        assert not attempts
        headers = {"Authorization": "Bearer synthetic-config-key"}
        response = await http.get(path, headers=headers)
        assert response.status_code == 503 and response.json() == {"error": {
            "code": "kubernetes_unavailable", "message": "Kubernetes runtime is unavailable",
        }}
        response = await http.post("/v1/workspaces", json={}, headers=headers)
        assert response.status_code == 501 and response.json() == {"error": {
            "code": "runtime_operation_unsupported", "message": "runtime operation is not supported",
        }}
        assert len(attempts) == 1


@pytest.mark.anyio
async def test_renewal_preserves_real_kubernetes_failure_through_service(monkeypatch):
    """Service's legacy Docker-specific exception filter cannot turn a Kubernetes outage into a 500."""
    configure_kubernetes(monkeypatch)
    app = create_app(api_key="synthetic-config-key", store=FakeLifecycleStore(), reaper_enabled=False)

    def unavailable(**kwargs):
        """Fail the actual Runtime's query before metadata or renewal can be changed."""
        raise ConnectionError("private-api-server /private/certificate")

    app.state.runtime._client_factory = unavailable
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as http:
        response = await http.post(
            "/v1/sandboxes/sbx_0123456789abcdef0123456789abcdef/renew", json={"ttl_seconds": 60},
            headers={"Authorization": "Bearer synthetic-config-key"},
        )
    assert response.status_code == 503 and response.json() == {"error": {
        "code": "kubernetes_unavailable", "message": "Kubernetes runtime is unavailable",
    }}


def test_fresh_import_with_configuration_defers_all_external_state(tmp_path):
    """The public module's eagerly assembled app must remain inert even when Kubernetes is selected."""
    script = '''
from pathlib import Path
import sqlite3
import subprocess
import docker
from kubernetes import client, config
def forbidden(*args, **kwargs):
    """Reject filesystem/client/process side effects during configured module import."""
    raise AssertionError("unexpected assembly I/O")
Path.open = forbidden
sqlite3.connect = forbidden
docker.from_env = forbidden
client.ApiClient = forbidden
config.load_kube_config = forbidden
subprocess.Popen = forbidden
from control_plane.app import app
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
assert isinstance(app.state.runtime, KubernetesSandboxRuntime)
assert app.state.reaper_task is None and app.state.kubernetes_entry_manager is None
'''
    environment = dict(os.environ)
    environment.update({
        "AGENT_SANDBOX_RUNTIME": "kubernetes", "AGENT_SANDBOX_K8S_KUBECONFIG": "/not-read",
        "AGENT_SANDBOX_K8S_CONTEXT": "explicit-test", "AGENT_SANDBOX_K8S_NAMESPACE": "test-sandboxes",
        "AGENT_SANDBOX_K8S_AGENT_IMAGE": "basic:dev", "AGENT_SANDBOX_K8S_PROXY_IMAGE": "proxy:dev",
        "AGENT_SANDBOX_DATABASE_PATH": str(tmp_path / "absent" / "lifecycle.sqlite3"),
    })
    result = subprocess.run([sys.executable, "-c", script], cwd=Path(__file__).resolve().parents[2],
                            env=environment, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "absent").exists()
