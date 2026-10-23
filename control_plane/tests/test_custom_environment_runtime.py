"""Operator environment configuration and fixed Agent launch without Docker access."""

import asyncio
import threading
from dataclasses import replace
from types import SimpleNamespace

import pytest
from docker.errors import ImageNotFound

from control_plane.core import errors
from control_plane.core.errors import CreationResourceState, SandboxCreateFailed, SandboxImageUnavailable
from control_plane.runtime.docker import transaction
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from test_sandbox_egress_runtime import EgressCreationClient, PROXY_ENV, egress_spec
from test_sandbox_runtime import RecordingClient, make_spec


def test_runtime_copies_and_freezes_operator_environment_configuration():
    """Later source mutation and callers cannot rewrite either runtime's registered images."""
    additional = {"science": "registry.example:5000/team/science:v1"}
    runtime = DockerSandboxRuntime(additional_environments=additional)
    additional["science"] = "unapproved:latest"
    assert dict(runtime.environment_images) == {
        "basic": "agent-sandbox:dev", "python": "agent-sandbox-python:dev",
        "node": "agent-sandbox-node:dev", "science": "registry.example:5000/team/science:v1",
    }
    assert "science" not in DockerSandboxRuntime().environment_images
    with pytest.raises(TypeError):
        runtime.environment_images["science"] = "unapproved:latest"
    with pytest.raises(AttributeError):
        runtime.environment_images = {"science": "unapproved:latest"}
    with pytest.raises(TypeError):
        transaction.ENVIRONMENT_IMAGES["basic"] = "unapproved:latest"


@pytest.mark.parametrize("additional", [
    [], [("science", "science:dev")], "science:dev", 1, True,
    {"basic": "unapproved:latest"}, {"python": "agent-sandbox-python:dev"},
    {"node": "agent-sandbox-node:dev"}, {1: "science:dev"},
    {"Science": "science:dev"}, {" science": "science:dev"},
    {"science\n": "science:dev"}, {"a" * 33: "science:dev"},
    {"science": None}, {"science": 42}, {"science": True},
    {"science": ""}, {"science": " science:dev"}, {"science": "science:dev "},
    {"science": "science:\tdev"}, {"science": "science:dev\n"},
    {"science": "https://registry.example/science:v1"},
    {"science": "user@registry.example/science:v1"},
    {"science": "registry.example/Science:v1"}, {"science": "science@sha256:abc"},
    {"science": "science:dev\x00"}, {"science": "镜像:dev"},
    {"science": "a" * 513}, {"science": "../science:dev"},
    {"science": "registry.example:0/science:v1"},
    {"science": "registry.example:65536/science:v1"},
    {"science": "science:dev:other"}, {"science": "science@sha512:" + "a" * 64},
    {"science": "science@sha256:" + "A" * 64},
    {f"custom-{index}": "science:dev" for index in range(33)},
])
def test_invalid_operator_configuration_fails_without_opening_client(additional):
    """Malformed trusted configuration fails synchronously before any Docker transport is opened."""
    calls = []

    def open_client():
        """Record an unexpected assembly-time Docker access."""
        calls.append(True)
        raise AssertionError("unexpected Docker connection")

    with pytest.raises(ValueError):
        DockerSandboxRuntime(open_client, additional_environments=additional)
    assert not calls


@pytest.mark.parametrize("image", [
    "science", "science:dev", "team/science:V1_2.3", "localhost:5000/team/science:v1",
    "registry.example/team/science", "127.0.0.1:5000/team/science:v1",
    "science@sha256:" + "a" * 64,
    "registry.example:443/team/science:v1@sha256:" + "f" * 64,
])
def test_supported_operator_image_references_are_preserved(image):
    """Local names, registries, tags and sha256 references pass validation without rewriting."""
    images = transaction.build_environment_images({"science": image})
    assert images["science"] == image
    with pytest.raises(TypeError):
        images["science"] = "other:dev"


def test_registration_accepts_at_most_32_additional_environment_names():
    """The configured count excludes the three builtins and accepts the documented boundary."""
    images = transaction.build_environment_images({f"custom-{index}": "science:dev" for index in range(32)})
    assert len(images) == 35 and images["custom-31"] == "science:dev"


@pytest.mark.anyio
async def test_unknown_environment_rejects_before_runtime_or_transaction_opens_client():
    """Direct runtime and transaction callers get a stable unchanged error before SDK access."""
    spec = make_spec()
    object.__setattr__(spec, "environment", "science")
    calls = []

    def open_client():
        """Expose any forbidden client opening for an unregistered environment."""
        calls.append(True)
        raise AssertionError("unexpected Docker connection")

    runtime = DockerSandboxRuntime(open_client)
    with pytest.raises(errors.SandboxEnvironmentNotRegistered) as caught:
        await runtime.create(spec)
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED
    with pytest.raises(errors.SandboxEnvironmentNotRegistered):
        transaction.DockerCreationTransaction(open_client).create_sync(spec, threading.Event())
    assert not calls


@pytest.mark.anyio
async def test_transaction_copies_its_environment_mapping_before_creation():
    """A direct transaction does not re-read a mutable map between construction and preflight."""
    images = {"science": "science:v1"}
    client = RecordingClient()
    creation = transaction.DockerCreationTransaction(lambda: client, environment_images=images)
    images["science"] = "unapproved:latest"
    spec = make_spec()
    object.__setattr__(spec, "environment", "science")
    creation.create_sync(spec, threading.Event())
    assert "images.get:science:v1" in client.events
    assert "images.get:unapproved:latest" not in client.events


@pytest.mark.anyio
async def test_interleaved_custom_runtime_images_are_isolated_and_pinned():
    """Equal environment names on independent runtimes resolve only their own selected local image."""
    clients = [RecordingClient(), RecordingClient(), RecordingClient()]
    selections = ["science:v1", "science:v2", "science:v1"]
    runtime_one = DockerSandboxRuntime(iter([clients[0], clients[2]]).__next__, additional_environments={"science": "science:v1"})
    runtime_two = DockerSandboxRuntime(lambda: clients[1], additional_environments={"science": "science:v2"})
    for index, (runtime, client, reference) in enumerate(zip([runtime_one, runtime_two, runtime_one], clients, selections)):
        calls, resolved = [], SimpleNamespace(id="sha256:" + str(index + 1) * 64, attrs={"Config": {}})
        expected_id = resolved.id

        def lookup(image):
            """Move the selected tag's model after lookup to expose unpinned creation."""
            calls.append(image)
            if image == reference:
                return resolved
            assert image == "agent-sandbox-proxy:dev"
            resolved.id = "sha256:" + "f" * 64
            return SimpleNamespace(id="sha256:" + "0" * 64)

        client.images.get = lookup
        await runtime.create(replace(make_spec(), environment="science"))
        assert calls == [reference, "agent-sandbox-proxy:dev"]
        assert client.containers.create_calls[0]["image"] == expected_id
        assert len(client.networks.create_calls) == 2 and len(client.containers.create_calls) == 2
        assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("environment", ["basic", "python", "node", "science"])
@pytest.mark.parametrize("egress", [False, True])
async def test_all_main_images_use_fixed_agent_launch_health_and_safety(environment, egress):
    """Custom image defaults cannot replace the Agent, workspace, health check or safety baseline."""
    spec = replace(egress_spec() if egress else make_spec(), environment=environment)
    client = EgressCreationClient(spec)
    runtime = DockerSandboxRuntime(lambda: client, additional_environments={"science": "science:dev"})
    await runtime.create(spec)
    main = client.containers.create_calls[0]
    assert main["entrypoint"] == ["/usr/local/bin/agent"]
    assert main["command"] == ["-listen", "0.0.0.0:8080"]
    assert main["user"] == "sandbox" and main["working_dir"] == "/workspace"
    assert main["volumes"] == ["/workspace"]
    assert main["healthcheck"] == {
        "test": ["CMD", "wget", *(["-Y", "off"] if egress else []), "-q", "-T", "2", "-O", "/dev/null", "http://127.0.0.1:8080/healthz"],
        "interval": 2_000_000_000, "timeout": 3_000_000_000,
        "start_period": 5_000_000_000, "retries": 3,
    }
    host = main["host_config"]
    assert host["read_only"] is True and host["cap_drop"] == ["ALL"]
    assert host["security_opt"] == ["no-new-privileges:true"] and host["pids_limit"] == 64
    assert host["nano_cpus"] == 1_000_000_000
    assert host["mem_limit"] == host["memswap_limit"] == 268_435_456
    assert host["network_mode"] == spec.network_name and not host.get("port_bindings")
    assert host["binds"] == {spec.volume_name: {"bind": "/workspace", "mode": "rw"}}
    assert main.get("environment", {}) == (PROXY_ENV if egress else {})
    assert len(client.containers.create_calls) == (3 if egress else 2)


@pytest.mark.anyio
@pytest.mark.parametrize("image_id", [None, "", 123])
async def test_custom_image_requires_pinned_identity_before_creating_resources(image_id):
    """A custom image with no usable immutable ID fails without fallback or resource mutation."""
    client = RecordingClient()
    client.images.get = lambda reference: SimpleNamespace(id=image_id)
    runtime = DockerSandboxRuntime(lambda: client, additional_environments={"science": "science:dev"})
    with pytest.raises(SandboxImageUnavailable) as caught:
        await runtime.create(replace(make_spec(), environment="science"))
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED
    assert not client.networks.create_calls and not client.containers.create_calls
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("volumes", [{"/cache": {}}, {"/workspace": {}, "/cache": {}}, ["/cache"], "cache"])
async def test_image_declared_extra_volumes_are_rejected_before_resource_creation(volumes):
    """Image VOLUME defaults cannot create untracked anonymous mounts outside the workspace."""
    client = RecordingClient()
    client.images.get = lambda reference: SimpleNamespace(
        id="sha256:" + "a" * 64, attrs={"Config": {"Volumes": volumes}},
    )
    runtime = DockerSandboxRuntime(lambda: client, additional_environments={"science": "science:dev"})
    with pytest.raises(SandboxImageUnavailable) as caught:
        await runtime.create(replace(make_spec(), environment="science"))
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED
    assert not client.networks.create_calls and not client.containers.create_calls
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("volumes", [None, {}, {"/workspace": {}}])
async def test_image_workspace_volume_declaration_preserves_the_single_managed_mount(volumes):
    """Absent volumes and a workspace declaration use only the explicitly bound managed volume."""
    client = RecordingClient()
    client.images.get = lambda reference: SimpleNamespace(
        id="sha256:" + "a" * 64, attrs={"Config": {"Volumes": volumes}},
    )
    await DockerSandboxRuntime(lambda: client, additional_environments={"science": "science:dev"}).create(
        replace(make_spec(), environment="science"),
    )
    main = client.containers.create_calls[0]
    assert main["volumes"] == ["/workspace"]
    assert main["host_config"]["binds"] == {
        make_spec().volume_name: {"bind": "/workspace", "mode": "rw"},
    }


@pytest.mark.anyio
@pytest.mark.parametrize("attributes", [None, {}, {"Config": None}, {"Config": []}])
async def test_image_missing_mount_metadata_is_not_assumed_safe(attributes):
    """An incomplete image inspect cannot establish the fixed workspace-only mount contract."""
    client = RecordingClient()
    client.images.get = lambda reference: SimpleNamespace(id="sha256:" + "a" * 64, attrs=attributes)
    runtime = DockerSandboxRuntime(lambda: client, additional_environments={"science": "science:dev"})
    with pytest.raises(SandboxImageUnavailable) as caught:
        await runtime.create(replace(make_spec(), environment="science"))
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED
    assert not client.networks.create_calls and not client.volumes.create_calls
    assert not client.containers.create_calls and client.close_calls == 1


@pytest.mark.anyio
async def test_missing_custom_image_never_falls_back_or_pulls():
    """A registered but absent local image produces unchanged evidence and no second lookup."""
    client, calls = RecordingClient(), []

    def missing(reference):
        """Reject the only allowed selected lookup and expose any attempted fallback."""
        calls.append(reference)
        raise ImageNotFound("missing private image")

    client.images.get = missing
    runtime = DockerSandboxRuntime(lambda: client, additional_environments={"science": "science:dev"})
    with pytest.raises(SandboxImageUnavailable) as caught:
        await runtime.create(replace(make_spec(), environment="science"))
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED
    assert calls == ["science:dev"] and not client.networks.create_calls
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_custom_image_failure_preserves_reverse_rollback_and_evidence():
    """A failed custom-image start cleans the same complete resource transaction as builtin images."""
    client = RecordingClient()
    client.proxy.start_error = RuntimeError("failed proxy start")
    runtime = DockerSandboxRuntime(lambda: client, additional_environments={"science": "science:dev"})
    with pytest.raises(SandboxCreateFailed) as caught:
        await runtime.create(replace(make_spec(), environment="science"))
    assert caught.value.creation_failure.phase == "start_proxy"
    assert caught.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
    assert [event for event in client.events if event.endswith(".remove")] == [
        "proxy.remove", "container.remove", "workspace.remove", "ingress.remove", "network.remove",
    ]
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_custom_image_repeated_cancellation_waits_for_reverse_rollback():
    """Repeated cancellation during custom creation still waits for full exact-ID compensation."""
    client = RecordingClient()
    entered, release = threading.Event(), threading.Event()
    client.proxy.start_entered, client.proxy.start_release = entered, release
    runtime = DockerSandboxRuntime(lambda: client, additional_environments={"science": "science:dev"})
    task = asyncio.create_task(runtime.create(replace(make_spec(), environment="science")))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        task.cancel()
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert [event for event in client.events if event.endswith(".remove")] == [
            "proxy.remove", "container.remove", "workspace.remove", "ingress.remove", "network.remove",
        ]
        assert client.close_calls == 1
    finally:
        release.set()
