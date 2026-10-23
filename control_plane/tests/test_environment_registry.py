"""Validate deployment-owned environment registration without Docker access."""

import asyncio
import importlib
import importlib.util
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from unittest.mock import AsyncMock, Mock

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.models import SandboxCreateOptions
from control_plane.core.service import SandboxService
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from lifecycle_fakes import FakeLifecycleStore
from test_sandbox_runtime import make_spec
from test_sandbox_service import FakeRuntime


BUILTINS = {"basic", "python", "node"}


class StringSubclass(str):
    """Expose accidental coercion or acceptance of values outside exact string inputs."""


def build_images(additional=None):
    """Use the runtime-owned registration helper without sharing Docker state."""
    from control_plane.runtime.docker.transaction import build_environment_images

    return build_environment_images(additional)


@pytest.mark.anyio
async def test_custom_environment_reaches_runtime_as_name_only():
    """A registered name retains the existing limits, TTL and response contract."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    app = create_app(
        runtime, store=store, reaper_enabled=False,
        additional_environments={"tools": "agent-sandbox-tools:dev"},
    )
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", json={
            "environment": "tools", "ttl_seconds": 123,
            "resources": {"cpu_millis": 750, "memory_mib": 384},
        })
    assert response.status_code == 201
    spec, = runtime.specs
    assert spec.environment == "tools"
    assert spec.resources.cpu_millis == 750 and spec.resources.memory_mib == 384
    assert (spec.expires_at - spec.created_at).total_seconds() == 123
    assert set(response.json()) == {"sandbox_id", "status", "created_at", "expires_at"}
    assert store.records[spec.sandbox_id].management_state == "active"
    assert not hasattr(spec, "image")


@pytest.mark.parametrize("name", ["tools", "a", "a" * 32, "a-0", "a-"])
def test_domain_environment_models_accept_valid_unregistered_names(name):
    """Domain models validate syntax without importing a deployment image registry."""
    assert SandboxCreateOptions(environment=name).environment == name
    assert replace(make_spec(), environment=name).environment == name


@pytest.mark.parametrize("name", [
    "", "a" * 33, "Tools", "TOOLS", "-tools", "0tools", "tools_name",
    "tools.name", " tools", "tools ", "tools\n", "tools\r", "tools\x00",
    "工具", "ｔools", "toolſ", "image:tag", None, True, 1, 1.0, [], {}, StringSubclass("tools"),
])
def test_domain_environment_models_reject_invalid_name_syntax(name):
    """Names are strict bounded ASCII identifiers at both domain entry points."""
    with pytest.raises(ValueError):
        SandboxCreateOptions(environment=name)
    with pytest.raises(ValueError):
        replace(make_spec(), environment=name)


@pytest.mark.parametrize("image", [
    "tools", "agent-sandbox-tools:dev", "registry.example/team/tools:v1.2-rc_1",
    "localhost:5000/team/tools", "registry.example:5000/team/tools:v2",
    "team/tools@sha256:" + "a" * 64,
    "registry.example:5000/team/tools:v2@sha256:" + "0123456789abcdef" * 4,
])
def test_registry_accepts_supported_docker_image_references(image):
    """Operator registration permits local names, registries, tags and SHA256 digests."""
    images = build_images({"tools": image})
    assert images["tools"] == image and set(images) == BUILTINS | {"tools"}


@pytest.mark.parametrize("image", [
    "", "a" * 513, "https://registry.example/tools:dev", "docker://tools",
    "tools name", " tools", "tools ", "tools\n", "tools\r", "tools\t",
    "tools\x00", "工具", "tools:", "tools@", "/tools", "tools/", "tools//name",
    "tools@sha256:" + "a" * 63, "tools@sha256:" + "a" * 65,
    "tools@sha256:" + "A" * 64, "tools@sha512:" + "a" * 64,
    "tools@sha256:" + "g" * 64, None, True, 1, 1.0, [], {}, StringSubclass("tools:dev"),
])
def test_registry_rejects_invalid_image_references(image):
    """Invalid Docker references fail during assembly without performing backend lookups."""
    with pytest.raises(ValueError):
        build_images({"tools": image})


@pytest.mark.parametrize("additional", [
    "tools", [ ("tools", "tools:dev") ], (), 1, True,
    {"basic": "different:dev"}, {"python": "agent-sandbox-python:dev"},
    {"node": "agent-sandbox-node:dev"}, {"bad_name": "tools"},
    {"工具": "tools"}, {"a" * 33: "tools"}, {None: "tools"}, {1: "tools"},
    {StringSubclass("tools"): "tools"},
])
def test_registry_rejects_invalid_maps_and_builtin_overrides(additional):
    """Only mappings of additional valid names may extend the fixed builtins."""
    with pytest.raises(ValueError):
        build_images(additional)


def test_registry_is_copied_immutable_and_bounded():
    """Up to 32 extra mappings are snapshotted and cannot mutate builtin registration."""
    source = {f"tools-{index}": "tools:dev" for index in range(32)}
    images = build_images(MappingProxyType(source))
    assert len(images) == 35
    source["tools-0"] = "changed:dev"
    source["new-tools"] = "new:dev"
    assert images["tools-0"] == "tools:dev" and "new-tools" not in images
    with pytest.raises(TypeError):
        images["tools-0"] = "changed:dev"
    with pytest.raises(ValueError):
        build_images(source)
    assert set(build_images()) == BUILTINS
    assert set(build_images({})) == BUILTINS


@pytest.mark.parametrize("names", [
    [], {"tools"}, {"basic", "python"}, "basic", 1, True,
    ["basic", "python", "node", "Tools"], ["basic", "python", "node", None],
    ["basic", "python", "node", "a" * 33], ["basic", "python", "node", StringSubclass("tools")],
    ["basic", "python", "node"] + [f"tools-{index}" for index in range(33)],
])
def test_service_rejects_invalid_name_registries(names):
    """Core admits bounded name collections containing every builtin, without image values."""
    with pytest.raises(ValueError):
        SandboxService(FakeRuntime(), FakeLifecycleStore(), environment_names=names)


def test_service_accepts_full_bounded_registry_and_rejects_unbounded_iterators():
    """A 35-name collection is valid while generators cannot bypass upfront size bounds."""
    names = BUILTINS | {f"tools-{index}" for index in range(32)}
    SandboxService(FakeRuntime(), FakeLifecycleStore(), environment_names=names)
    with pytest.raises(ValueError):
        SandboxService(FakeRuntime(), FakeLifecycleStore(), environment_names=iter(names))


@pytest.mark.anyio
async def test_service_copies_names_and_rejects_unregistered_before_all_side_effects():
    """Mutation of a caller's collection cannot grant or revoke core environment access."""
    from control_plane.core.errors import SandboxEnvironmentNotRegistered

    names = BUILTINS | {"tools"}
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    service = SandboxService(runtime, store, environment_names=names)
    names.remove("tools")
    names.add("other-tools")
    await service.create(SandboxCreateOptions(environment="tools"))
    with pytest.raises(SandboxEnvironmentNotRegistered):
        await service.create(SandboxCreateOptions(environment="other-tools"))
    assert [spec.environment for spec in runtime.specs] == ["tools"]


@pytest.mark.anyio
@pytest.mark.parametrize("at_capacity", [False, True])
async def test_unregistered_name_fails_before_capacity_clock_ids_store_and_runtime(at_capacity):
    """Unknown environment errors take priority even when the local create slots are full."""
    from control_plane.core.errors import SandboxEnvironmentNotRegistered

    runtime, store = FakeRuntime(), FakeLifecycleStore()
    runtime.create = AsyncMock(side_effect=AssertionError("runtime was touched"))
    store.create = AsyncMock(side_effect=AssertionError("pending row was written"))
    forbidden = Mock(side_effect=AssertionError("identity or clock was read"))
    service = SandboxService(
        runtime, store, id_factory=forbidden, transaction_id_factory=forbidden,
        clock=forbidden, max_concurrent_creates=1,
    )
    if at_capacity:
        service._creates_in_progress = 1
    with pytest.raises(SandboxEnvironmentNotRegistered):
        await service.create(SandboxCreateOptions(environment="tools"))
    forbidden.assert_not_called()
    store.create.assert_not_awaited()
    runtime.create.assert_not_awaited()
    assert service._creates_in_progress == int(at_capacity)


@pytest.mark.anyio
@pytest.mark.parametrize("body", [
    b'{"environment":"unregistered"}',
    b'{"environment":"tools","environment":"basic"}',
    b'{"environment":"tools","image":"tools:dev"}',
    b'{"environment":"tools","additional_environments":{"tools":"tools"}}',
])
async def test_http_environment_registration_cannot_be_supplied_by_request(body):
    """Unknown names and registry-shaped request fields share the stable invalid request error."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    store.create = AsyncMock(side_effect=AssertionError("unexpected pending row"))
    app = create_app(
        runtime, store=store, reaper_enabled=False,
        additional_environments={"tools": "tools:dev"},
    )
    app.state.service._creates_in_progress = 2
    forbidden = Mock(side_effect=AssertionError("identity or clock was read"))
    app.state.service._clock = forbidden
    app.state.service._id_factory = forbidden
    app.state.service._transaction_id_factory = forbidden
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", content=body)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"
    assert response.json()["error"]["message"] == "invalid sandbox creation options"
    assert runtime.specs == runtime.queries == runtime.deletions == []
    forbidden.assert_not_called()
    store.create.assert_not_awaited()


@pytest.mark.anyio
async def test_apps_keep_independent_snapshots_of_operator_registration():
    """Two applications sharing a mutable source preserve their own registration snapshots."""
    source = {"tools": "tools:dev"}
    first_runtime, second_runtime = FakeRuntime(), FakeRuntime()
    first = create_app(first_runtime, store=FakeLifecycleStore(), reaper_enabled=False, additional_environments=source)
    source.clear()
    source["other-tools"] = "other-tools:dev"
    second = create_app(second_runtime, store=FakeLifecycleStore(), reaper_enabled=False, additional_environments=source)
    source["later-tools"] = "later-tools:dev"
    for app, accepted, rejected in ((first, "tools", "other-tools"), (second, "other-tools", "tools")):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.post("/v1/sandboxes", json={"environment": accepted})).status_code == 201
            assert (await client.post("/v1/sandboxes", json={"environment": rejected})).status_code == 400
            assert (await client.post("/v1/sandboxes", json={"environment": "later-tools"})).status_code == 400
    assert [spec.environment for spec in first_runtime.specs] == ["tools"]
    assert [spec.environment for spec in second_runtime.specs] == ["other-tools"]


def test_default_app_builds_runtime_registration_from_copied_configuration():
    """Normal app assembly owns a Docker runtime whose exact image map is immutable."""
    source = {"tools": "agent-sandbox-tools:dev"}
    app = create_app(store=FakeLifecycleStore(), reaper_enabled=False, additional_environments=source)
    source["tools"] = "changed:dev"
    assert isinstance(app.state.runtime, DockerSandboxRuntime)
    assert app.state.runtime.environment_images["tools"] == "agent-sandbox-tools:dev"
    with pytest.raises(TypeError):
        app.state.runtime.environment_images["tools"] = "changed:dev"


@pytest.mark.parametrize("additional", [{}, {"other-tools": "other:dev"}, {"tools": "changed:dev"}])
def test_explicit_app_registration_must_match_injected_docker_runtime(additional):
    """Conflicting app and runtime configurations fail before serving requests."""
    runtime = DockerSandboxRuntime(additional_environments={"tools": "tools:dev"})
    with pytest.raises(ValueError):
        create_app(runtime, store=FakeLifecycleStore(), reaper_enabled=False, additional_environments=additional)


@pytest.mark.anyio
@pytest.mark.parametrize("explicit", [False, True])
async def test_injected_docker_runtime_supplies_its_actual_registered_names(explicit):
    """A configured runtime works naturally and accepts an explicitly matching app map."""
    runtime = DockerSandboxRuntime(additional_environments={"tools": "tools:dev"})
    runtime.create = AsyncMock()
    kwargs = {"additional_environments": {"tools": "tools:dev"}} if explicit else {}
    app = create_app(runtime, store=FakeLifecycleStore(), reaper_enabled=False, **kwargs)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", json={"environment": "tools"})
    assert response.status_code == 201
    assert runtime.create.await_args.args[0].environment == "tools"


def test_import_and_application_assembly_do_not_open_docker_or_sqlite(monkeypatch, tmp_path):
    """Registration validation and default module app creation remain free of external I/O."""
    import sqlite3
    import docker
    import control_plane.app as app_module

    forbidden = Mock(side_effect=AssertionError("assembly performed external I/O"))
    monkeypatch.setattr(docker, "from_env", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    importlib.reload(app_module)
    database = tmp_path / "not-created" / "lifecycle.sqlite3"
    app_module.create_app(database_path=database, additional_environments={"tools": "tools:dev"})
    forbidden.assert_not_called()
    assert not database.parent.exists()


def test_documented_example_factory_registers_only_the_tools_image(monkeypatch):
    """The importable operator example forwards its fixed registry without backend I/O."""
    import sqlite3
    import docker

    forbidden = Mock(side_effect=AssertionError("example performed external I/O"))
    monkeypatch.setattr(docker, "from_env", forbidden)
    monkeypatch.setattr(sqlite3, "connect", forbidden)
    example_path = Path(__file__).resolve().parents[2] / "examples/custom-environment/custom_environment_app.py"
    spec = importlib.util.spec_from_file_location("custom_environment_example_under_test", example_path)
    assert spec is not None and spec.loader is not None
    example = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(example)
    assembled = object()
    create_control_app = Mock(return_value=assembled)
    monkeypatch.setattr(example, "create_control_app", create_control_app)
    assert example.create_app() is assembled
    create_control_app.assert_called_once_with(additional_environments={"tools": "agent-sandbox-tools:dev"})
    forbidden.assert_not_called()


@pytest.mark.anyio
async def test_custom_environment_failure_cleans_its_pending_transaction(tmp_path):
    """Registered names use the ordinary durable prewrite and confirmed failure cleanup."""
    from control_plane.core.errors import SandboxImageUnavailable
    from test_service_lifecycle import ControlledRuntime, FaultStore

    store = FaultStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    runtime = ControlledRuntime(store)
    runtime.create_error = SandboxImageUnavailable()
    service = SandboxService(runtime, store, environment_names=BUILTINS | {"tools"})
    with pytest.raises(SandboxImageUnavailable):
        await service.create(SandboxCreateOptions(environment="tools"))
    spec, = runtime.specs
    assert spec.environment == "tools"
    assert await store.get(spec.sandbox_id) is None
    assert runtime.live_resources == set()
    assert service._creates_in_progress == 0


@pytest.mark.anyio
async def test_custom_environment_cancellation_waits_for_runtime_rollback(tmp_path):
    """Repeated caller cancellation retains admission until custom creation rollback settles."""
    from test_service_lifecycle import ControlledRuntime, FaultStore

    store = FaultStore(tmp_path / "lifecycle.sqlite3")
    await store.initialize()
    runtime = ControlledRuntime(store)
    runtime.block_create = runtime.block_rollback = True
    service = SandboxService(runtime, store, environment_names=BUILTINS | {"tools"})
    task = asyncio.create_task(service.create(SandboxCreateOptions(environment="tools")))
    try:
        await asyncio.wait_for(runtime.create_entered.wait(), timeout=3)
        task.cancel()
        await asyncio.wait_for(runtime.rollback_entered.wait(), timeout=3)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and service._creates_in_progress == 1
        spec, = runtime.specs
        assert spec.environment == "tools"
        assert (await store.get(spec.sandbox_id)).management_state == "pending"
        runtime.rollback_release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=3)
        assert await store.get(spec.sandbox_id) is None
        assert runtime.live_resources == set() and service._creates_in_progress == 0
    finally:
        runtime.create_release.set()
        runtime.rollback_release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
