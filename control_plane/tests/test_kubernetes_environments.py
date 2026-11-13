"""Trusted Kubernetes language registration and unchanged lifecycle/security contracts."""

import asyncio
from dataclasses import replace

import httpx
import pytest

from control_plane.app import create_app
from control_plane.runtime.configuration import build_runtime, load_backend_configuration
from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
from lifecycle_fakes import FakeLifecycleStore
from test_backend_configuration import configure_kubernetes
from test_kubernetes_pod_spec import sandbox_spec
from control_plane.core.errors import (
    CreationResourceState, KubernetesConfigurationError, KubernetesOperationUnsupported, SandboxCreateFailed,
)
from control_plane.core.models import SandboxCreateOptions
from test_kubernetes_auto_entry import automatic, identified_app
from test_kubernetes_local_entry_manager import manager_harness
from test_kubernetes_persistent_workspace import persistence_clock
from test_kubernetes_proxy_lifecycle import proxy_clock, runtime, authenticated_spec
from test_kubernetes_query import SID


@pytest.mark.parametrize("names", [(), ("python",), ("node",), ("python", "node")])
def test_formal_optional_images_register_only_selected_environments(monkeypatch, names):
    """A trusted optional image is the only way to expose its corresponding language name."""
    configure_kubernetes(monkeypatch)
    for name in ("python", "node"):
        key = "AGENT_SANDBOX_K8S_" + name.upper() + "_IMAGE"
        monkeypatch.delenv(key, raising=False)
        if name in names:
            monkeypatch.setenv(key, "agent-sandbox-" + name + ":dev")
    backend = build_runtime(load_backend_configuration())
    assert dict(backend.environment_images) == {
        "basic": "agent-sandbox-basic:dev", **{name: "agent-sandbox-" + name + ":dev" for name in names},
    }


@pytest.mark.anyio
async def test_formal_http_language_name_reaches_registered_runtime(monkeypatch):
    """The real Service must admit a configured language without allowing user-selected image parameters."""
    configure_kubernetes(monkeypatch)
    monkeypatch.setenv("AGENT_SANDBOX_K8S_PYTHON_IMAGE", "agent-sandbox-python:dev")
    store = FakeLifecycleStore()
    app = create_app(api_key="synthetic-language-key", store=store, reaper_enabled=False)
    calls = []

    async def unchanged(spec):
        """Reject only at the Runtime boundary so no actual client or resource can be created."""
        calls.append(spec.environment)
        raise KubernetesOperationUnsupported()

    monkeypatch.setattr(app.state.runtime, "create", unchanged)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as http:
        response = await http.post("/v1/sandboxes", json={"environment": "python"},
                                   headers={"Authorization": "Bearer synthetic-language-key"})
    assert response.status_code == 501 and calls == ["python"]
    assert not store.records and not store.credentials


@pytest.mark.parametrize("name", ["python", "node"])
def test_pod_builder_selects_trusted_language_image_without_changing_contract(name):
    """Only the container image differs across language selections; startup, safety and volumes stay fixed."""
    spec = sandbox_spec()
    basic = build_pod_spec(spec, namespace="test-sandboxes", agent_image="agent-sandbox-basic:dev")
    language = build_pod_spec(replace(spec, environment=name), namespace="test-sandboxes",
                              agent_image="agent-sandbox-" + name + ":dev")
    assert language.spec.containers[0].image == "agent-sandbox-" + name + ":dev"
    language.spec.containers[0].image = basic.spec.containers[0].image
    assert language.to_dict() == basic.to_dict()


@pytest.mark.parametrize("images", [{"basic": "custom-basic:dev"}, {"python": "python-agent:dev", "node": "node-agent:dev"}])
def test_mapping_is_copied_frozen_and_preserves_legacy_basic(images):
    """External dictionary mutation cannot change existing runtime registration or image selection."""
    backend = runtime(None, environment_images=images)
    original = dict(backend.environment_images)
    images.clear()
    assert dict(backend.environment_images) == original
    assert original["basic"] in {"custom-basic:dev", "agent-sandbox-basic:dev"}
    with pytest.raises(TypeError):
        backend.environment_images["python"] = "replacement:dev"


@pytest.mark.parametrize("legacy", ["custom-basic:dev", None])
def test_explicit_legacy_basic_must_agree_with_mapping(legacy):
    """An explicit conflicting value, including None, is rejected rather than silently overridden."""
    with pytest.raises(KubernetesConfigurationError):
        runtime(None, agent_image=legacy, environment_images={"basic": "other-basic:dev"})
    backend = runtime(None, agent_image="same:dev", environment_images={"basic": "same:dev"})
    assert backend.environment_images["basic"] == "same:dev"


@pytest.mark.parametrize("images", [{}, [], "invalid", {"tools": "tools:dev"}, {1: "image:dev"},
    {"basic": "b", "python": "p", "node": "n", "tools": "t"}])
def test_invalid_mapping_is_rejected_without_clients(images):
    """Only a bounded mapping of the three implemented names is a valid trusted registration."""
    with pytest.raises(KubernetesConfigurationError):
        runtime(None, environment_images=images)


@pytest.mark.parametrize("image", [None, True, 1, "", " image", "image ", "image\n", "a" * 513,
    "https://registry/image", "repo:", "repo/", "UPPER/repo", "host:0/repo", "a..b/repo", "a@@b",
    "repo@sha256:bad", "repo@sha256:" + "A" * 64, "registry/user:password@image", "镜像"])
def test_invalid_agent_image_is_fixed_configuration_failure(image):
    """Bad trusted references fail locally and diagnostics never echo configuration content."""
    with pytest.raises(KubernetesConfigurationError) as caught:
        runtime(None, environment_images={"python": image})
    assert str(caught.value) == "kubernetes configuration is invalid"


@pytest.mark.parametrize("image", ["agent-sandbox-python:dev", "registry.local:5000/team/python:V1",
    "localhost:5000/repo@sha256:" + "a" * 64, "team/image_tag:tag-with.dot", "image"])
def test_supported_image_reference_forms_are_pure(image):
    """Registry ports, tags and immutable digests are checked without resolving any image."""
    backend = runtime(None, environment_images={"node": image})
    assert backend.environment_images["node"] == image


@pytest.mark.parametrize("field", ["PYTHON", "NODE"])
@pytest.mark.parametrize("image", ["", "repo:", "bad image", "https://registry/image"])
def test_invalid_optional_configuration_is_not_ignored(monkeypatch, field, image):
    """Explicit invalid optional fields cannot downgrade registration to basic or an absent language."""
    configure_kubernetes(monkeypatch)
    monkeypatch.setenv("AGENT_SANDBOX_K8S_" + field + "_IMAGE", image)
    with pytest.raises(ValueError):
        build_runtime(load_backend_configuration())


def test_language_configuration_is_one_immutable_snapshot(monkeypatch):
    """Later environment changes cannot reconfigure a runtime built from an existing snapshot."""
    configure_kubernetes(monkeypatch)
    monkeypatch.setenv("AGENT_SANDBOX_K8S_PYTHON_IMAGE", "original:dev")
    snapshot = load_backend_configuration()
    monkeypatch.setenv("AGENT_SANDBOX_K8S_PYTHON_IMAGE", "changed:dev")
    monkeypatch.setenv("AGENT_SANDBOX_K8S_NODE_IMAGE", "new-node:dev")
    backend = build_runtime(snapshot)
    assert dict(backend.environment_images) == {"basic": "agent-sandbox-basic:dev", "python": "original:dev"}


@pytest.mark.anyio
async def test_unregistered_language_precedes_identity_admission_and_store(monkeypatch):
    """A real Service must reject before assigning identities, consuming capacity or calling any client."""
    configure_kubernetes(monkeypatch)
    app = create_app(api_key="synthetic-language-key", store=FakeLifecycleStore())

    def forbidden(*args, **kwargs):
        """Fail on any identity or external boundary reached by an unregistered request."""
        pytest.fail("unregistered environment reached identity or SDK boundary")

    service = app.state.service
    service._id_factory = service._transaction_id_factory = forbidden
    app.state.runtime._client_factory = forbidden
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as http:
        response = await http.post("/v1/sandboxes", json={"environment": "node"},
                                   headers={"Authorization": "Bearer synthetic-language-key"})
    assert response.status_code == 400 and service._creates_in_progress == 0
    assert not app.state.lifecycle_store.records and not app.state.lifecycle_store.credentials
    with pytest.raises(KubernetesOperationUnsupported) as caught:
        await app.state.runtime._create_resources(authenticated_spec(environment="node"))
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED


@pytest.mark.anyio
@pytest.mark.parametrize("name", ["python", "node"])
@pytest.mark.parametrize("persistent", [False, True])
async def test_language_http_creation_automatic_entry_and_historical_management(automatic, monkeypatch, name, persistent):
    """Real Service/SQLite and manager preserve transactions and old-resource management after registration removal."""
    h = automatic
    monkeypatch.setenv("AGENT_SANDBOX_K8S_" + name.upper() + "_IMAGE", "agent-sandbox-" + name + ":dev")
    if persistent:
        monkeypatch.setenv("AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", "agent-sandbox-workspace")
    app = identified_app(h)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control",
                                     headers={"Authorization": "Bearer synthetic-auto-key"}) as http:
            response = await http.post("/v1/sandboxes", json={"environment": name})
            assert response.status_code == 201
            assert h.api.pod.spec.containers[0].image == "agent-sandbox-" + name + ":dev"
            assert len(h.api.objects()) == (9 if persistent else 8) and len(h.sessions) == 1
            assert (await http.get(f"/v1/sandboxes/{SID}/endpoint")).status_code == 200
            # A restarted runtime with no language registration still recognizes
            # identities solely from original resource records, not image aliases.
            restarted = runtime(h.api)
            assert (await restarted.get(SID)).status == "ready"
            assert (await restarted.inspect_resources(SID, expected_transaction_id=h.api.pod.metadata.labels[
                "io.agent-sandbox.transaction-id"])).classification == "all_present"
            await app.state.kubernetes_entry_manager.close(SID, expected_transaction_id=h.api.pod.metadata.labels[
                "io.agent-sandbox.transaction-id"])
            await restarted.delete(SID, delete_workspace=True)
            await app.state.service.delete(SID, delete_workspace=True)
            assert not h.api.objects() and not (await app.state.lifecycle_store.list_sandboxes()).items
            assert h.sessions[0].closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("name", ["python", "node"])
@pytest.mark.parametrize("failure", ["image", "entry"])
async def test_language_failures_compensate_private_workspace_before_activation(automatic, monkeypatch, name, failure):
    """Language choice cannot change the original PVC/session rollback or metadata release rules."""
    h = automatic
    monkeypatch.setenv("AGENT_SANDBOX_K8S_" + name.upper() + "_IMAGE", "missing-test:dev")
    monkeypatch.setenv("AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", "agent-sandbox-workspace")
    if failure == "image":
        h.api.startup["container"] = "image"
    else:
        h.open_error = True
    app = identified_app(h)
    async with app.router.lifespan_context(app):
        with pytest.raises(SandboxCreateFailed) as caught:
            await app.state.service.create(SandboxCreateOptions(environment=name))
        assert caught.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
        assert not h.api.objects() and not (await app.state.lifecycle_store.list_sandboxes()).items
        assert app.state.kubernetes_entry_manager.session_count == 0


@pytest.mark.anyio
async def test_independent_workspace_image_resolution_remains_unsupported():
    """A registered tag is never misrepresented as immutable independent-workspace image identity."""
    with pytest.raises(KubernetesOperationUnsupported):
        await runtime(None, environment_images={"python": "python:dev"}).resolve_environment_image("python")


@pytest.mark.anyio
@pytest.mark.parametrize("name", ["python", "node"])
async def test_language_repeated_cancel_settles_entry_and_pvc(automatic, monkeypatch, name):
    """Repeated cancellation waits for the late owned entry, then cleans original resources and SQLite state."""
    h = automatic
    monkeypatch.setenv("AGENT_SANDBOX_K8S_" + name.upper() + "_IMAGE", "agent-sandbox-" + name + ":dev")
    monkeypatch.setenv("AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", "agent-sandbox-workspace")
    app = identified_app(h)
    h.open_gate.clear()
    h.close_gate.clear()
    async with app.router.lifespan_context(app):
        task = asyncio.create_task(app.state.service.create(SandboxCreateOptions(environment=name)))
        try:
            while not h.sessions:
                await asyncio.sleep(0)
            await h.sessions[0].started.wait()
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            h.open_gate.set()
            await asyncio.wait_for(h.closing.wait(), 2)
            assert not task.done() and h.api.objects() and not h.api.delete_calls
            h.close_gate.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not h.api.objects() and h.sessions[0].closed.is_set()
            assert (await app.state.lifecycle_store.list_sandboxes()).managed_count == 0
            assert app.state.service._creates_in_progress == 0
        finally:
            h.open_gate.set()
            h.close_gate.set()
            await asyncio.gather(task, return_exceptions=True)
