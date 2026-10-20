"""Read-only inventory classification, ownership checks and worker cancellation tests."""

import asyncio
import threading
from copy import deepcopy
from typing import Any

import pytest
from docker.errors import DockerException

from control_plane.core.models import ResourceKind
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from test_sandbox_deletion import DeletionClient, DeletionState, ROLES, SPEC
from test_sandbox_runtime import SANDBOX_ID


def resource_snapshot(client: DeletionClient) -> dict[str, dict[str, Any]]:
    """Capture exact IDs and inspected state while ignoring read-only call counters."""
    return {
        key: {"id": resource.id, "attrs": deepcopy(resource.attrs)}
        for group in client.state.resources.values() for key, resource in group.items()
    }


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("roles", "classification", "reason"),
    [
        (ROLES, "all_present", "resources_present"),
        ((), "not_found", "resources_not_found"),
        (("proxy", "network", "ingress", "workspace"), "partial", "resources_partial"),
        *(((role,), "partial", "resources_partial") for role in ROLES),
    ],
)
async def test_inspect_classifies_full_partial_and_missing_inventories_without_writes(
    roles: tuple[ResourceKind, ...], classification: str, reason: str,
) -> None:
    """Every role is inspected even without a main container and no state is changed."""
    client = DeletionClient(DeletionState(roles))
    before = resource_snapshot(client)

    result = await DockerSandboxRuntime(lambda: client).inspect_resources(
        SANDBOX_ID, expected_transaction_id=SPEC.transaction_id,
    )

    assert result.sandbox_id == SANDBOX_ID
    assert result.classification == classification
    assert result.reason == reason
    assert set(result.found_resources) == set(roles)
    assert set(result.missing_resources) == set(ROLES) - set(roles)
    assert resource_snapshot(client) == before
    assert client.state.mutations() == []
    assert client.close_calls == 1
    assert sum(len(manager.list_calls) for manager in (
        client.containers, client.networks, client.volumes,
    )) == 5


@pytest.mark.anyio
@pytest.mark.parametrize("role", ROLES)
async def test_inspect_rejects_duplicate_roles_without_modifying_any_resource(
    role: ResourceKind,
) -> None:
    """Two candidates for one role are ambiguous even when every ownership label agrees."""
    client = DeletionClient()
    client.state.add(role, "-duplicate")
    before = resource_snapshot(client)

    result = await DockerSandboxRuntime(lambda: client).inspect_resources(
        SANDBOX_ID, expected_transaction_id=SPEC.transaction_id,
    )

    assert result.classification == "conflict"
    assert result.reason == "resource_ownership_conflict"
    assert result.missing_resources == ()
    assert resource_snapshot(client) == before
    assert client.state.mutations() == []
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("role", ROLES)
async def test_inspect_does_not_hide_resources_from_another_transaction(
    role: ResourceKind,
) -> None:
    """The expected transaction protects every role rather than filtering conflicts away."""
    client = DeletionClient()
    client.state.original[role].labels["io.agent-sandbox.transaction-id"] = (
        "txn_ffffffffffffffffffffffffffffffff"
    )
    before = resource_snapshot(client)

    result = await DockerSandboxRuntime(lambda: client).inspect_resources(
        SANDBOX_ID, expected_transaction_id=SPEC.transaction_id,
    )

    assert result.classification == "conflict"
    assert result.reason == "resource_ownership_conflict"
    for manager in (client.containers, client.networks, client.volumes):
        for call in manager.list_calls:
            assert not any("transaction-id=" in label for label in call["filters"]["label"])
    assert resource_snapshot(client) == before
    assert client.state.mutations() == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "label", ["managed", "project", "sandbox-id", "transaction-id", "resource"],
)
async def test_inspect_rechecks_complete_ownership_after_discovery(label: str) -> None:
    """Ownership changes between list and exact-ID inspect must be reported as conflict."""
    client = DeletionClient()
    target = client.state.original["proxy"]

    def change_ownership(_resource_id: str) -> None:
        """Simulate a peer altering labels after the filtered listing returned."""
        target.labels[f"io.agent-sandbox.{label}"] = "wrong"

    client.containers.get_hook = change_ownership
    result = await DockerSandboxRuntime(lambda: client).inspect_resources(
        SANDBOX_ID, expected_transaction_id=SPEC.transaction_id,
    )

    assert result.classification == "conflict"
    assert result.reason == "resource_ownership_conflict"
    assert client.state.mutations() == []
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("phase", ["listing", "inspect"])
async def test_inspect_rejects_missing_or_changed_exact_resource_identity(phase: str) -> None:
    """A matching role cannot be trusted when its listed or refreshed exact ID is invalid."""
    client = DeletionClient()
    target = client.state.original["proxy"]
    if phase == "listing":
        target.id = ""
    else:
        def change_identity(_resource: Any) -> None:
            """Simulate inspect returning a different resource than the listed exact identifier."""
            target.id = "another-resource-id"

        target.reload_hook = change_identity

    result = await DockerSandboxRuntime(lambda: client).inspect_resources(
        SANDBOX_ID, expected_transaction_id=SPEC.transaction_id,
    )

    assert result.classification == "conflict"
    assert result.reason == "resource_ownership_conflict"
    assert result.missing_resources == ()
    assert client.state.mutations() == []
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_inspect_treats_disappeared_resource_as_missing_and_continues() -> None:
    """A listed resource can vanish before inspect without hiding other surviving roles."""
    client = DeletionClient()

    def disappear(resource_id: str) -> None:
        """Remove only the main container externally during the read-only inspection."""
        if resource_id == "container-id":
            del client.state.resources["containers"][resource_id]

    client.containers.get_hook = disappear
    result = await DockerSandboxRuntime(lambda: client).inspect_resources(
        SANDBOX_ID, expected_transaction_id=SPEC.transaction_id,
    )

    assert result.classification == "partial"
    assert set(result.found_resources) == set(ROLES) - {"container"}
    assert result.missing_resources == ("container",)
    assert client.state.mutations() == []
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["factory", "list", "get", "reload", "close"])
async def test_inspect_reports_unavailable_without_leaking_backend_details(stage: str) -> None:
    """Backend or transport failures never become absence or expose private messages."""
    client = DeletionClient()
    before = resource_snapshot(client)

    def fail(*_args: Any) -> None:
        """Raise a sensitive-looking daemon error at the selected read boundary."""
        raise DockerException("private Docker configuration and socket details")

    if stage == "list":
        client.networks.list_error = DockerException("private network details")
    elif stage == "get":
        client.containers.get_hook = fail
    elif stage == "reload":
        client.state.original["proxy"].reload_hook = fail
    elif stage == "close":
        client.close_hook = fail

    result = await DockerSandboxRuntime(fail if stage == "factory" else lambda: client).inspect_resources(
        SANDBOX_ID, expected_transaction_id=SPEC.transaction_id,
    )

    assert result.classification == "unavailable"
    assert result.reason == "docker_unavailable"
    assert result.missing_resources == ()
    assert "private" not in repr(result)
    assert resource_snapshot(client) == before
    assert client.state.mutations() == []
    assert client.close_calls == (0 if stage == "factory" else 1)


@pytest.mark.anyio
@pytest.mark.parametrize("worker_fails", [False, True])
async def test_inspect_repeated_cancellation_settles_read_and_client_close(worker_fails: bool) -> None:
    """Cancellation waits for the read-only thread and observes its result before propagating."""
    client = DeletionClient()
    entered, release = threading.Event(), threading.Event()
    close_entered, close_release = threading.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, Any]] = []
    previous_handler = loop.get_exception_handler()

    def block_reload(_resource: Any) -> None:
        """Keep the first inspection in flight until cancellation has been delivered twice."""
        entered.set()
        assert release.wait(5)
        if worker_fails:
            raise DockerException("private late read failure")

    def block_close() -> None:
        """Keep client finalization observable after resource reads have completed."""
        close_entered.set()
        assert close_release.wait(5)

    def observe_loop_error(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        """Capture late task errors that the adapter must consume itself."""
        loop_errors.append(context)

    client.state.original["proxy"].reload_hook = block_reload
    client.close_hook = block_close
    before = resource_snapshot(client)
    loop.set_exception_handler(observe_loop_error)
    task = asyncio.create_task(DockerSandboxRuntime(lambda: client).inspect_resources(
        SANDBOX_ID, expected_transaction_id=SPEC.transaction_id,
    ))
    try:
        assert await asyncio.to_thread(entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        assert await asyncio.to_thread(close_entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        close_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.closed.is_set()
        assert resource_snapshot(client) == before
        assert client.state.mutations() == []
        await asyncio.sleep(0)
        assert loop_errors == []
    finally:
        release.set()
        close_release.set()
        loop.set_exception_handler(previous_handler)
