"""Verify independent Docker workspace identity and data protection without a daemon."""

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from docker.errors import NotFound

from control_plane.core.errors import WorkspaceConflict, WorkspaceNotFound
from control_plane.core.models import WorkspaceRecord
from control_plane.runtime.docker.workspaces import inspect_workspace_sync, workspace_labels


def workspace() -> WorkspaceRecord:
    """Build a deterministic independent workspace record."""
    return WorkspaceRecord(
        workspace_id="wsp_" + "a" * 32, creation_token="wct_" + "b" * 32,
        volume_name="agent-sandbox-wsp_" + "a" * 32,
        image_id="sha256:" + "c" * 64, created_at=datetime(2026, 11, 3, tzinfo=UTC),
    )


class WorkspaceVolume:
    """Expose mutable inspect facts and observable nonforce deletion."""

    def __init__(self, record: WorkspaceRecord) -> None:
        """Seed a local volume with the complete independent ownership labels."""
        self.id = record.volume_name
        self.attrs = {"Name": self.id, "Driver": "local", "Options": {}, "Labels": workspace_labels(record)}
        self.removed = False
        self.remove_calls: list[dict[str, Any]] = []

    def reload(self) -> None:
        """Reject reads after confirmed deletion."""
        if self.removed:
            raise NotFound("gone")

    def remove(self, **kwargs: Any) -> None:
        """Record deletion and make the exact volume disappear."""
        self.remove_calls.append(kwargs)
        self.removed = True


class WorkspaceClient:
    """Supply a tiny fake Docker client with strict independent volume lookups."""

    def __init__(self, record: WorkspaceRecord) -> None:
        """Create local state and track closure and accidental volume creation."""
        self.volume = WorkspaceVolume(record)
        self.references: list[Any] = []
        self.close_calls = 0
        self.volumes = SimpleNamespace(get=self.get_volume)
        self.containers = SimpleNamespace(list=lambda **kwargs: self.references, get=lambda key: next(item for item in self.references if item.id == key))

    def get_volume(self, name: str) -> WorkspaceVolume:
        """Return only the exact expected volume, never a same-name replacement."""
        if name != self.volume.id or self.volume.removed:
            raise NotFound("gone")
        return self.volume

    def close(self) -> None:
        """Record transport closure on every outcome."""
        self.close_calls += 1


def test_independent_workspace_inspection_requires_local_driver() -> None:
    """Nonlocal drivers must not turn a named volume into a host filesystem escape."""
    record = workspace()
    client = WorkspaceClient(record)
    client.volume.attrs["Driver"] = "nfs"
    with pytest.raises(WorkspaceConflict):
        inspect_workspace_sync(lambda: client, record)
    assert client.close_calls == 1


def test_missing_workspace_inspection_never_creates_replacement() -> None:
    """Loss of an existing volume reports absence rather than creating empty data."""
    record = workspace()
    client = WorkspaceClient(record)
    client.volume.removed = True
    with pytest.raises(WorkspaceNotFound):
        inspect_workspace_sync(lambda: client, record)
    assert client.close_calls == 1


def binding() -> Any:
    """Build a fixed database occupancy identity for runtime operations."""
    from control_plane.core.models import WorkspaceBinding
    from test_sandbox_runtime import SANDBOX_ID, TRANSACTION_ID

    record = workspace()
    return WorkspaceBinding(record.workspace_id, record.creation_token, record.volume_name,
                            record.image_id, 2, SANDBOX_ID, TRANSACTION_ID)


def workspace_inventory(domains: Any = ()) -> tuple[Any, Any]:
    """Reuse a realistic topology while replacing only its private volume with independent storage."""
    from control_plane.runtime.docker.workspaces import binding_labels
    from test_sandbox_pause_runtime import pause_inventory

    client, selected = pause_inventory(domains), binding()
    volume = client.state.original["workspace"]
    del client.state.resources["volumes"][volume.key]
    volume.key = volume.id = selected.volume_name
    volume.attrs.update(Name=selected.volume_name, Driver="local", Options={}, Labels=workspace_labels(selected))
    client.state.resources["volumes"][volume.key] = volume
    for role, resource in client.state.original.items():
        if role != "workspace":
            resource.labels.update(binding_labels(selected))
    client.state.original["container"].attrs["Mounts"] = [{
        "Type": "volume", "Name": selected.volume_name, "Destination": "/workspace", "RW": True,
    }]
    old_list = client.containers.list

    def list_containers(**kwargs: Any) -> list[Any]:
        """Resolve references from actual mounts, including containers without ownership labels."""
        volume_name = kwargs.get("filters", {}).get("volume")
        if volume_name is not None:
            return [item for item in client.state.resources["containers"].values()
                    if any(mount.get("Name") == volume_name for mount in item.attrs.get("Mounts", []))]
        return old_list(**kwargs)

    client.containers.list = list_containers
    return client, selected


@pytest.mark.anyio
@pytest.mark.parametrize("domains", [(), ("example.com",)])
async def test_workspace_get_pause_inspect_restart_and_delete_preserve_data(domains: Any) -> None:
    """Restart discovery finds independent storage without instance labels and never deletes it."""
    from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime

    client, selected = workspace_inventory(domains)
    runtime = DockerSandboxRuntime(lambda: client)
    assert (await runtime.get(selected.sandbox_id)).workspace_id == selected.workspace_id
    assert (await runtime.inspect_resources(selected.sandbox_id,
        expected_transaction_id=selected.transaction_id, expected_workspace=selected)).classification == "all_present"
    await runtime.pause(selected.sandbox_id, expected_transaction_id=selected.transaction_id, expected_workspace=selected)
    await runtime.resume(selected.sandbox_id, expected_transaction_id=selected.transaction_id, expected_workspace=selected)
    await runtime.delete(selected.sandbox_id, expected_transaction_id=selected.transaction_id, expected_workspace=selected)
    assert client.state.remaining() == {"workspace"}
    assert not client.state.original["workspace"].remove_calls
    # A restarted runtime still validates independent ownership without any remaining instance.
    await DockerSandboxRuntime(lambda: client).delete(selected.sandbox_id,
        expected_transaction_id=selected.transaction_id, expected_workspace=selected)


@pytest.mark.anyio
@pytest.mark.parametrize("damage", ["missing", "token", "image", "driver", "options", "mount", "extra_mount", "partial", "mismatch"])
async def test_workspace_damage_blocks_all_mutations_and_pending_inspection(damage: str) -> None:
    """Any lost storage identity or incorrect actual mount prevents lifecycle mutations."""
    from control_plane.core.errors import SandboxDeleteConflict, SandboxPauseConflict
    from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
    from control_plane.runtime.docker.workspaces import BINDING_KEYS

    client, selected = workspace_inventory()
    volume = client.state.original["workspace"]
    if damage == "missing":
        del client.state.resources["volumes"][volume.key]
    elif damage == "token":
        volume.labels["io.agent-sandbox.workspace-token"] = "wct_" + "d" * 32
    elif damage == "image":
        volume.labels["io.agent-sandbox.workspace-image-id"] = "sha256:" + "d" * 64
    elif damage == "driver":
        volume.attrs["Driver"] = "nfs"
    elif damage == "options":
        volume.attrs["Options"] = {"type": "none", "device": "/", "o": "bind"}
    elif damage == "mount":
        client.state.original["container"].attrs["Mounts"][0]["Name"] = "foreign-volume"
    elif damage == "extra_mount":
        client.state.original["container"].attrs["Mounts"].append({"Type": "bind", "Source": "/", "Destination": "/host"})
    elif damage == "partial":
        del client.state.original["proxy"].labels[BINDING_KEYS[0]]
    else:
        client.state.original["ingress"].labels[BINDING_KEYS[2]] = "wct_" + "d" * 32
    runtime = DockerSandboxRuntime(lambda: client)
    assert (await runtime.inspect_resources(selected.sandbox_id, expected_transaction_id=selected.transaction_id,
        expected_workspace=selected)).classification == "conflict"
    with pytest.raises(SandboxPauseConflict):
        await runtime.pause(selected.sandbox_id, expected_transaction_id=selected.transaction_id, expected_workspace=selected)
    assert not client.state.mutations()
    if damage == "missing":
        await runtime.delete(selected.sandbox_id, expected_transaction_id=selected.transaction_id, expected_workspace=selected)
        assert not client.state.remaining()
    else:
        with pytest.raises(SandboxDeleteConflict):
            await runtime.delete(selected.sandbox_id, expected_transaction_id=selected.transaction_id, expected_workspace=selected)
        assert not client.state.mutations()


@pytest.mark.anyio
async def test_expected_workspace_is_required_even_when_main_and_all_instance_resources_are_gone() -> None:
    """An empty instance inventory cannot bypass the database's original storage identity."""
    from control_plane.core.errors import SandboxDeleteConflict
    from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime

    client, selected = workspace_inventory()
    client.state.resources["containers"].clear()
    client.state.resources["networks"].clear()
    client.state.original["workspace"].labels["io.agent-sandbox.workspace-token"] = "wct_" + "d" * 32
    with pytest.raises(SandboxDeleteConflict):
        await DockerSandboxRuntime(lambda: client).delete(selected.sandbox_id,
            expected_transaction_id=selected.transaction_id, expected_workspace=selected)
    assert not client.state.mutations()


@pytest.mark.anyio
async def test_main_missing_partial_deletion_preserves_external_workspace() -> None:
    """Remaining networks and proxies retain immutable binding proof when main is lost."""
    from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime

    client, selected = workspace_inventory()
    del client.state.resources["containers"][client.state.original["container"].key]
    await DockerSandboxRuntime(lambda: client).delete(selected.sandbox_id,
        expected_transaction_id=selected.transaction_id, expected_workspace=selected)
    assert client.state.remaining() == {"workspace"}


@pytest.mark.anyio
async def test_instance_delete_workspace_flag_cannot_remove_independent_storage() -> None:
    """Even direct runtime calls must reject private-workspace deletion for explicit bindings."""
    from control_plane.core.errors import WorkspaceDeleteProtected
    from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime

    client, selected = workspace_inventory()
    with pytest.raises(WorkspaceDeleteProtected):
        await DockerSandboxRuntime(lambda: client).delete(selected.sandbox_id, delete_workspace=True,
            expected_transaction_id=selected.transaction_id, expected_workspace=selected)
    assert not client.state.mutations()


def test_workspace_delete_requires_no_actual_references_and_nonforce_removal() -> None:
    """Reference discovery includes unlabeled containers and unused deletion stays idempotent."""
    from control_plane.runtime.docker.workspaces import delete_workspace_sync

    record, client = workspace(), WorkspaceClient(workspace())
    client.references = [SimpleNamespace(id="foreign")]
    with pytest.raises(WorkspaceConflict):
        delete_workspace_sync(lambda: client, record)
    assert not client.volume.remove_calls
    client.references = []
    delete_workspace_sync(lambda: client, record)
    delete_workspace_sync(lambda: client, record)
    assert client.volume.remove_calls == [{}]
    assert client.close_calls == 3


@pytest.mark.parametrize("damage", ["missing", "token", "image", "driver", "options"])
def test_workspace_bound_creation_preflights_without_creating_empty_replacement(damage: str) -> None:
    """Failures before mutation preserve independent data and supply unchanged resource evidence."""
    from dataclasses import replace
    import threading
    from control_plane.core.errors import CreationResourceState, SandboxResourceConflict
    from control_plane.runtime.docker.transaction import DockerCreationTransaction
    from test_sandbox_runtime import RecordingClient, make_spec

    selected, client = binding(), RecordingClient()
    client.volume.id = selected.volume_name
    client.volume.attrs.update(Name=selected.volume_name, Driver="local", Options={}, Labels=workspace_labels(selected))
    client.containers.list = lambda **kwargs: []
    client.images.get = lambda name: SimpleNamespace(id=selected.image_id, attrs={"Config": {}})
    if damage == "missing":
        client.volume.removed.set()
    elif damage == "token":
        client.volume.attrs["Labels"]["io.agent-sandbox.workspace-token"] = "wct_" + "d" * 32
    elif damage == "image":
        client.images.get = lambda name: SimpleNamespace(id="sha256:" + "d" * 64, attrs={"Config": {}})
    elif damage == "driver":
        client.volume.attrs["Driver"] = "nfs"
    else:
        client.volume.attrs["Options"] = {"device": "/"}
    spec = replace(make_spec(), volume_name=selected.volume_name, workspace=selected)
    with pytest.raises(SandboxResourceConflict) as caught:
        DockerCreationTransaction(lambda: client).create_sync(spec, threading.Event())
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNCHANGED
    assert not client.volumes.create_calls and not client.networks.create_calls
    assert not client.volume.remove_calls
    assert client.close_calls == 1


def creation_client() -> tuple[Any, Any, Any]:
    """Prepare existing data and independent labels for the ordinary creation transaction fake."""
    from dataclasses import replace
    from control_plane.runtime.docker.workspaces import binding_labels
    from test_sandbox_runtime import RecordingClient, make_spec

    selected, client = binding(), RecordingClient()
    spec = replace(make_spec(), volume_name=selected.volume_name, workspace=selected)
    client.volume.id = selected.volume_name
    client.volume.attrs.update(Name=selected.volume_name, Driver="local", Options={}, Labels=workspace_labels(selected))
    for role in ("network", "ingress", "container", "proxy"):
        getattr(client, role).attrs["Labels"].update(binding_labels(selected))
    client.container.attrs["Mounts"] = [{"Type": "volume", "Name": selected.volume_name, "Destination": "/workspace", "RW": True}]
    client.containers.list = lambda **kwargs: []
    client.images.get = lambda name: SimpleNamespace(id=selected.image_id, attrs={"Config": {"Labels": {"io.agent-sandbox.proxy-auth-version": "1"}}})
    return client, selected, spec


def test_bound_creation_uses_nocopy_and_excludes_data_from_rollback_ledger() -> None:
    """Existing contents never receive image copy-up and late cancellation owns instance resources only."""
    import threading
    from control_plane.runtime.docker.transaction import DockerCreationTransaction
    from control_plane.runtime.docker.workspaces import binding_labels

    client, selected, spec = creation_client()
    transaction = DockerCreationTransaction(lambda: client)
    records = transaction.create_sync(spec, threading.Event())
    assert {item.kind for item in records} == {"network", "ingress", "container", "proxy"}
    calls = [*client.networks.create_calls, *client.containers.create_calls]
    assert all(all(call["labels"][key] == value for key, value in binding_labels(selected).items()) for call in calls)
    host = client.containers.create_calls[0]["host_config"]
    assert "binds" not in host
    assert host["mounts"][0] == {
        "Target": "/workspace", "Source": selected.volume_name, "Type": "volume",
        "ReadOnly": False, "VolumeOptions": {"NoCopy": True},
    }
    transaction.rollback_committed(spec, records)
    assert not client.volumes.create_calls and not client.volume.remove_calls


def test_image_tag_moves_between_checks_rolls_back_networks_without_touching_data() -> None:
    """The second preflight detects a moved tag immediately before mounting an existing workspace."""
    import threading
    from control_plane.core.errors import CreationResourceState, SandboxResourceConflict
    from control_plane.runtime.docker.transaction import DockerCreationTransaction, SANDBOX_IMAGE

    client, selected, spec = creation_client()
    count = 0

    def image_get(name: str) -> Any:
        """Move only the selected image tag after its initial preflight."""
        nonlocal count
        if name == SANDBOX_IMAGE:
            count += 1
        return SimpleNamespace(id=selected.image_id if count < 2 else "sha256:" + "d" * 64, attrs={"Config": {"Labels": {"io.agent-sandbox.proxy-auth-version": "1"}}})

    client.images.get = image_get
    with pytest.raises(SandboxResourceConflict) as caught:
        DockerCreationTransaction(lambda: client).create_sync(spec, threading.Event())
    assert caught.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
    assert not client.volume.remove_calls and not client.volumes.create_calls
    assert client.network.removed.is_set() and client.ingress.removed.is_set()
    assert not client.containers.create_calls


class InitializerClient(WorkspaceClient):
    """Model one fresh volume and exact short-lived initializer lifecycle."""

    def __init__(self, record: Any) -> None:
        """Expose low-level API calls and bounded initializer hooks without real Docker."""
        super().__init__(record)
        self.volume.removed = True
        self.create_calls: list[dict[str, Any]] = []
        self.host_calls: list[dict[str, Any]] = []
        self.initializer: Any = None
        self.wait_result = 0
        self.wait_error: Exception | None = None
        self.start_hook: Any = None
        self.remove_error: Exception | None = None
        self.images = SimpleNamespace(get=lambda name: SimpleNamespace(id=record.image_id, attrs={"Config": {}}))
        self.api = SimpleNamespace(create_volume=self.create_volume, create_host_config=self.host_config,
                                   create_container=self.create_container)
        self.containers = SimpleNamespace(get=self.get_container, list=lambda **kwargs: [])

    def create_volume(self, name: str, **kwargs: Any) -> dict[str, Any]:
        """Create exactly one independent local volume and echo daemon ownership metadata."""
        assert name == self.volume.id and kwargs["driver"] == "local"
        assert self.volume.removed
        self.volume.removed = False
        return {"Name": name, "Labels": dict(kwargs["labels"])}

    def host_config(self, **kwargs: Any) -> dict[str, Any]:
        """Record effective security policy for first-volume copy-up."""
        self.host_calls.append(kwargs)
        return kwargs

    def create_container(self, **kwargs: Any) -> dict[str, str]:
        """Store the exact initializer ID and its complete ownership labels."""
        self.create_calls.append(kwargs)
        self.initializer = SimpleNamespace(id="initializer-exact-id", attrs={"Config": {"Labels": dict(kwargs["labels"])}})
        self.initializer.reload = lambda: None
        self.initializer.start = self.start_initializer
        self.initializer.wait = self.wait_initializer
        self.initializer.remove = self.remove_initializer
        return {"Id": self.initializer.id}

    def get_container(self, key: str) -> Any:
        """Never resolve cleanup through a mutable container name."""
        if self.initializer is None or key != self.initializer.id:
            raise NotFound("initializer absent")
        return self.initializer

    def start_initializer(self) -> None:
        """Allow a deterministic cancellation or ownership mutation during start."""
        if self.start_hook is not None:
            self.start_hook()

    def wait_initializer(self, *, timeout: int) -> dict[str, int]:
        """Require a finite exit wait and expose failed initialization outcomes."""
        assert timeout == 5
        if self.wait_error is not None:
            raise self.wait_error
        return {"StatusCode": self.wait_result}

    def remove_initializer(self, **kwargs: Any) -> None:
        """Remove only the short-lived container and explicitly preserve mounted storage."""
        assert kwargs == {"force": True, "v": False}
        if self.remove_error is not None:
            raise self.remove_error
        self.initializer = None


def test_first_workspace_initialization_is_pinned_nonroot_offline_and_shortlived() -> None:
    """Initialization performs image copy-up exactly once through the fixed Agent entrypoint."""
    from control_plane.runtime.docker.workspaces import create_workspace_sync

    selected, client = workspace(), InitializerClient(workspace())
    create_workspace_sync(lambda: client, selected)
    call = client.create_calls[0]
    assert call["image"] == selected.image_id
    assert call["entrypoint"] == ["/usr/local/bin/agent"] and call["command"] == ["-help"]
    assert call["user"] == "sandbox" and call["network_disabled"] is True
    host = call["host_config"]
    assert host["network_mode"] == "none" and host["read_only"] is True
    assert host["cap_drop"] == ["ALL"] and host["security_opt"] == ["no-new-privileges:true"]
    assert host["mem_limit"] == host["memswap_limit"] == 128 * 1024 * 1024
    assert host["binds"] == {selected.volume_name: {"bind": "/workspace", "mode": "rw"}}
    assert client.initializer is None and not client.volume.removed
    assert client.close_calls == 1
    with pytest.raises(WorkspaceConflict):
        create_workspace_sync(lambda: client, selected)
    assert len(client.create_calls) == 1


@pytest.mark.parametrize("damage", ["exit", "timeout", "remove", "ownership"])
def test_uncertain_workspace_initialization_retains_data_and_closes_client(damage: str) -> None:
    """Failed init or cleanup never reports available storage and never deletes user data."""
    from control_plane.core.errors import WorkspaceError
    from control_plane.runtime.docker.workspaces import create_workspace_sync

    selected, client = workspace(), InitializerClient(workspace())
    if damage == "exit":
        client.wait_result = 2
    elif damage == "timeout":
        client.wait_error = TimeoutError("private backend error")
    elif damage == "remove":
        client.remove_error = RuntimeError("cleanup uncertain")
    else:
        client.start_hook = lambda: client.initializer.attrs["Config"]["Labels"].update({"io.agent-sandbox.workspace-token": "wct_" + "d" * 32})
    with pytest.raises(WorkspaceError):
        create_workspace_sync(lambda: client, selected)
    assert not client.volume.removed and not client.volume.remove_calls
    assert client.close_calls == 1
    assert (client.initializer is None) == (damage in {"exit", "timeout"})


@pytest.mark.anyio
async def test_repeated_workspace_create_cancellation_waits_for_initializer_cleanup() -> None:
    """Repeated cancellation must settle the bounded worker and leave its independent volume intact."""
    import asyncio
    import threading
    from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime

    selected, client = workspace(), InitializerClient(workspace())
    entered, release = threading.Event(), threading.Event()

    def start_hook() -> None:
        """Hold initialization until the caller sends repeated cancellation signals."""
        entered.set()
        assert release.wait(2)

    client.start_hook = start_hook
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).create_workspace(selected))
    assert await asyncio.to_thread(entered.wait, 2)
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.initializer is None and not client.volume.removed and client.close_calls == 1


@pytest.mark.anyio
async def test_pause_rechecks_actual_workspace_mount_immediately_before_mutation() -> None:
    """A topology change after discovery cannot authorize a freeze from stale mount evidence."""
    from control_plane.core.errors import SandboxPauseConflict
    from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime

    client, selected = workspace_inventory()
    network = client.state.original["network"]

    def change_mount(resource: Any) -> None:
        """Substitute an unrelated actual mount during the second network observation."""
        if resource.reload_calls >= 2:
            client.state.original["container"].attrs["Mounts"][0]["Name"] = "foreign-volume"

    network.reload_hook = change_mount
    with pytest.raises(SandboxPauseConflict):
        await DockerSandboxRuntime(lambda: client).pause(selected.sandbox_id,
            expected_transaction_id=selected.transaction_id, expected_workspace=selected)
    assert not client.state.mutations()
    assert not [event for event in client.state.events if event[0] == "pause"]


def test_bound_creation_verifies_actual_mount_before_starting_user_agent() -> None:
    """Requested NoCopy metadata alone cannot prove the resulting Docker mount is the bound volume."""
    import threading
    from control_plane.core.errors import SandboxResourceConflict
    from control_plane.runtime.docker.transaction import DockerCreationTransaction

    client, selected, spec = creation_client()
    client.container.attrs["Mounts"][0]["Name"] = "foreign-volume"
    with pytest.raises(SandboxResourceConflict):
        DockerCreationTransaction(lambda: client).create_sync(spec, threading.Event())
    assert "container.start" not in client.events
    assert not client.volume.remove_calls


@pytest.mark.anyio
async def test_query_preserves_full_internal_workspace_binding_for_service_verification() -> None:
    """Consistent Docker facts still carry the immutable token/image/volume to compare with SQLite."""
    from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime

    client, selected = workspace_inventory()
    observed = await DockerSandboxRuntime(lambda: client).get(selected.sandbox_id)
    assert observed.workspace is not None
    assert observed.workspace.creation_token == selected.creation_token
    assert observed.workspace.volume_name == selected.volume_name
    assert observed.workspace.image_id == selected.image_id


@pytest.mark.anyio
async def test_missing_independent_volume_does_not_block_owned_instance_cleanup() -> None:
    """Verified storage absence permits cleanup, without creating a replacement or deleting data."""
    from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime

    client, selected = workspace_inventory()
    volume = client.state.original['workspace']
    del client.state.resources['volumes'][volume.key]
    runtime = DockerSandboxRuntime(lambda: client)
    await runtime.delete(selected.sandbox_id, expected_transaction_id=selected.transaction_id, expected_workspace=selected)
    assert not client.state.remaining()
    assert not volume.remove_calls
