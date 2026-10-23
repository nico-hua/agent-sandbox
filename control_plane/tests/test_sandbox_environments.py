"""Preset environment validation and immutable Docker image selection without daemon access."""

from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from docker.errors import ImageNotFound

from control_plane.app import create_app
from control_plane.core.errors import SandboxEnvironmentNotRegistered, SandboxImageUnavailable
from control_plane.core.models import SandboxCreateOptions, SandboxResources
from control_plane.core.service import SandboxService
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from lifecycle_fakes import FakeLifecycleStore
from test_sandbox_service import FakeRuntime
from test_sandbox_runtime import RecordingClient, make_spec


@pytest.mark.anyio
@pytest.mark.parametrize("environment", ["basic", "python", "node"])
@pytest.mark.parametrize("custom", [False, True])
async def test_http_environment_combines_with_resources_and_initial_ttl(environment, custom):
    """Each permitted environment travels with existing limits and times through the real Service."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    app = create_app(runtime, store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        payload = {"environment": environment}
        if custom:
            payload.update(ttl_seconds=123, resources={"cpu_millis": 750, "memory_mib": 384})
        response = await client.post("/v1/sandboxes", json=payload)
    assert response.status_code == 201
    spec, = runtime.specs
    assert spec.environment == environment
    assert spec.resources == (SandboxResources(750, 384) if custom else SandboxResources(1000, 256))
    assert (spec.expires_at - spec.created_at).total_seconds() == (123 if custom else 1800)
    assert store.records[spec.sandbox_id].expires_at == spec.expires_at
    assert set(response.json()) == {"sandbox_id", "status", "created_at", "expires_at"}


@pytest.mark.anyio
@pytest.mark.parametrize("environment", ["", " ", "Python", "NODE", " python", "node\n", "ruby_script", "agent-sandbox:dev", "a" * 33, None, True, 1, 1.0, [], {}])
async def test_invalid_environment_has_no_lifecycle_or_backend_effects(environment):
    """Malformed names and non-strings fail at both HTTP and direct domain entry points."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    store.create = AsyncMock(side_effect=AssertionError("unexpected prewrite"))
    app = create_app(runtime, store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", json={"environment": environment})
    assert response.status_code == 400 and response.json()["error"]["code"] == "invalid_request"
    with pytest.raises(ValueError):
        SandboxCreateOptions(environment=environment)
    with pytest.raises(ValueError):
        replace(make_spec(), environment=environment)
    assert runtime.specs == runtime.queries == runtime.deletions == [] and runtime.ping_calls == 0
    store.create.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("environment", ["ruby", "data-science", "a" * 32])
async def test_valid_unregistered_environment_is_rejected_before_lifecycle_effects(environment):
    """Domain names have independent syntax validation while Service enforces its configured registry."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    store.create = AsyncMock(side_effect=AssertionError("unexpected prewrite"))
    app = create_app(runtime, store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", json={"environment": environment})
    assert response.status_code == 400
    assert response.json()["error"] == {
        "code": "invalid_request", "message": "invalid sandbox creation options",
    }
    assert SandboxCreateOptions(environment=environment).environment == environment
    assert replace(make_spec(), environment=environment).environment == environment
    with pytest.raises(SandboxEnvironmentNotRegistered):
        await SandboxService(runtime, store).create(SandboxCreateOptions(environment=environment))
    assert not runtime.specs and not store.records
    store.create.assert_not_awaited()


@pytest.mark.anyio
async def test_environment_defaults_and_forged_service_input_are_validated():
    """No-argument creation stays basic; Service revalidates even a forged frozen option."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    service = SandboxService(runtime, store)
    result = await service.create()
    assert runtime.specs[0].environment == "basic"
    assert make_spec().environment == SandboxCreateOptions().environment == "basic"
    options = SandboxCreateOptions()
    object.__setattr__(options, "environment", "agent-sandbox-unapproved:latest")
    with pytest.raises(ValueError):
        await service.create(options)
    assert len(runtime.specs) == 1 and list(store.records) == [result.sandbox_id]


@pytest.mark.anyio
@pytest.mark.parametrize("body,status", [
    (b'{"environment":"python","environment":"node"}', 400),
    (b'{"environment":"basic","image":"unapproved"}', 400),
    (b'{"environment":NaN}', 400),
    (b'{"environment":"node"}' + b' ' * 4096, 413),
])
async def test_environment_keeps_strict_json_and_body_boundaries(body, status):
    """Environment selection cannot bypass duplicate-key, unknown-field or body-size restrictions."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    app = create_app(runtime, store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", content=body)
    assert response.status_code == status and not runtime.specs and not store.records


@pytest.mark.anyio
async def test_interleaved_presets_preflight_only_selected_images_and_create_by_id():
    """Mutable tags cannot change the selected image between lookup and container creation."""
    selected = [
        ("node", "agent-sandbox-node:dev", "sha256:" + "1" * 64),
        ("basic", "agent-sandbox:dev", "sha256:" + "2" * 64),
        ("python", "agent-sandbox-python:dev", "sha256:" + "3" * 64),
        ("node", "agent-sandbox-node:dev", "sha256:" + "4" * 64),
    ]
    clients = [RecordingClient() for _ in selected]
    runtime = DockerSandboxRuntime(iter(clients).__next__)
    for client, (environment, tag, image_id) in zip(clients, selected):
        calls = []

        def image_get(image):
            """Expose only the selected tag and proxy; invalidate the earlier model after preflight."""
            calls.append(image)
            if image == tag:
                return model
            assert image == "agent-sandbox-proxy:dev"
            model.id = "sha256:" + "f" * 64
            return SimpleNamespace(id="sha256:" + "0" * 64)

        model = SimpleNamespace(id=image_id, attrs={"Config": {}})
        client.images.get = image_get
        await runtime.create(replace(make_spec(), environment=environment))
        assert calls == [tag, "agent-sandbox-proxy:dev"]
        main, proxy = client.containers.create_calls
        assert main["image"] == image_id
        assert proxy["image"] == "agent-sandbox-proxy:dev"
        assert main["host_config"]["nano_cpus"] == 1_000_000_000
        assert main["host_config"]["mem_limit"] == main["host_config"]["memswap_limit"] == 268_435_456


@pytest.mark.anyio
@pytest.mark.parametrize("environment,tag", [("basic", "agent-sandbox:dev"), ("python", "agent-sandbox-python:dev"), ("node", "agent-sandbox-node:dev")])
async def test_missing_selected_image_does_not_fallback_or_leave_pending(environment, tag):
    """A missing preset creates no resources and compensates its exact pending prewrite."""
    client, store = RecordingClient(), FakeLifecycleStore()
    calls = []

    def missing_image(image):
        """Reject the requested tag; any fallback or extra lookup would be observable."""
        calls.append(image)
        raise ImageNotFound("private registry failure")

    client.images.get = missing_image
    app = create_app(DockerSandboxRuntime(lambda: client), store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as control:
        response = await control.post("/v1/sandboxes", json={"environment": environment})
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "sandbox_image_unavailable"
    assert "private" not in response.text
    assert calls == [tag]
    assert not store.records and not client.networks.create_calls and not client.containers.create_calls
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("image_id", [None, "", 123])
async def test_unusable_preflight_image_id_cannot_create_resources(image_id):
    """A missing immutable identity cannot silently send a mutable image tag to Docker."""
    client = RecordingClient()
    client.images.get = lambda image: SimpleNamespace(id=image_id)
    with pytest.raises(SandboxImageUnavailable):
        await DockerSandboxRuntime(lambda: client).create(make_spec())
    assert not client.networks.create_calls and not client.containers.create_calls
    assert client.close_calls == 1
