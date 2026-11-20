"""Stateful Docker deletion tests for ownership, convergence and cancellation."""

import asyncio
import threading
from collections.abc import Callable
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import docker
import pytest
from docker.errors import APIError, DockerException, NotFound

from control_plane.domain.errors import (
    DockerUnavailable,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
)
from control_plane.domain.models import ResourceKind
from control_plane.runtime.docker.adapter import DockerRuntime
from control_plane.tests.runtime.fakes import (
    SANDBOX_ID,
    make_spec,
)
from control_plane.tests.runtime.fakes import (
    DeletionClient,
    DeletionResource,
    DeletionState,
    ROLES,
    DELETE_SPEC as SPEC,
)


def deletion_api_error(status: int = 409) -> APIError:
    """Build a transport-shaped error without depending on a Docker daemon."""
    return APIError(
        "private backend details",
        response=SimpleNamespace(status_code=status, reason="Conflict", url="docker://test"),
    )


@pytest.mark.anyio
@pytest.mark.parametrize("delete_workspace", [False, True])
async def test_delete_reclaims_runtime_in_order_with_explicit_workspace_policy(delete_workspace: bool) -> None:
    """Normal deletion must close the entry before sandbox and networks, then the volume."""
    client = DeletionClient()
    result = await DockerRuntime(lambda: client).delete(
        SANDBOX_ID, delete_workspace=delete_workspace
    )
    assert result is None
    assert client.state.remaining() == (set() if delete_workspace else {"workspace"})
    assert [key for action, key in client.state.mutations() if action == "remove"] == [
        f"{role}-id" for role in ROLES if role != "workspace" or delete_workspace
    ]
    assert client.close_calls == 1
    for role in ("proxy", "container"):
        timeout = client.state.original[role].stop_calls[0]["timeout"]
        assert isinstance(timeout, (int, float)) and 0 < timeout <= 5
    for role in ("ingress", "network", "workspace"):
        assert all(not call.get("force", False) for call in client.state.original[role].remove_calls)


@pytest.mark.anyio
async def test_delete_rechecks_exact_identity_before_every_destructive_operation() -> None:
    """Each stop, kill and remove must follow exact-ID get and ownership reload."""
    client = DeletionClient()
    await DockerRuntime(lambda: client).delete(SANDBOX_ID, delete_workspace=True)
    for index, (action, key) in enumerate(client.state.events):
        if action in {"stop", "kill", "remove"}:
            assert client.state.events[index - 2:index] == [("get", key), ("reload", key)]


@pytest.mark.anyio
@pytest.mark.parametrize("roles", [(), ("proxy",), ("container",), ("ingress",), ("network",), ("workspace",), ("ingress", "network"), ("proxy", "workspace")])
@pytest.mark.parametrize("delete_workspace", [False, True])
async def test_delete_handles_unknown_ids_and_incomplete_residue(roles: tuple[ResourceKind, ...], delete_workspace: bool) -> None:
    """Missing containers and incomplete topology must not prevent residual cleanup."""
    client = DeletionClient(DeletionState(roles))
    await DockerRuntime(lambda: client).delete(SANDBOX_ID, delete_workspace=delete_workspace)
    assert client.state.remaining() == ({"workspace"} if "workspace" in roles and not delete_workspace else set())
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("roles", [ROLES, *((role,) for role in ROLES)])
@pytest.mark.parametrize("delete_workspace", [False, True])
async def test_delete_expected_transaction_checks_all_surviving_roles_before_mutation(
    roles: tuple[ResourceKind, ...], delete_workspace: bool,
) -> None:
    """Stored identity must protect full, partial and retained-workspace inventories."""
    client = DeletionClient(DeletionState(roles))

    with pytest.raises(SandboxDeleteConflict):
        await DockerRuntime(lambda: client).delete(
            SANDBOX_ID,
            delete_workspace=delete_workspace,
            expected_transaction_id="txn_ffffffffffffffffffffffffffffffff",
        )

    assert client.state.mutations() == []
    assert client.state.remaining() == set(roles)
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("roles", [(), ROLES, *((role,) for role in ROLES)])
@pytest.mark.parametrize("delete_workspace", [False, True])
async def test_delete_matching_expected_transaction_supports_partial_resources(
    roles: tuple[ResourceKind, ...], delete_workspace: bool,
) -> None:
    """Expected ownership must not require a main container or a healthy endpoint."""
    client = DeletionClient(DeletionState(roles))

    await DockerRuntime(lambda: client).delete(
        SANDBOX_ID,
        delete_workspace=delete_workspace,
        expected_transaction_id=SPEC.transaction_id,
    )

    assert client.state.remaining() == (
        {"workspace"} if "workspace" in roles and not delete_workspace else set()
    )
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_delete_rechecks_expected_transaction_on_retained_workspace() -> None:
    """A retained volume changing identity during cleanup must prevent confirmation."""
    client = DeletionClient()

    def replace_workspace_transaction() -> None:
        """Change only the retained workspace after preflight passed for every role."""
        client.state.original["workspace"].labels["io.agent-sandbox.transaction-id"] = (
            "txn_ffffffffffffffffffffffffffffffff"
        )

    client.state.original["network"].remove_hook = replace_workspace_transaction
    with pytest.raises(SandboxDeleteConflict):
        await DockerRuntime(lambda: client).delete(
            SANDBOX_ID, expected_transaction_id=SPEC.transaction_id,
        )

    assert client.state.remaining() == {"workspace"}
    assert client.state.original["workspace"].remove_calls == []
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_delete_fresh_runtime_and_retries_recover_only_from_labels() -> None:
    """New runtime instances must finish repeated deletion without creation memory."""
    state = DeletionState()
    clients: list[DeletionClient] = []
    for delete_workspace in (False, False, True, True):
        client = DeletionClient(state)
        clients.append(client)
        await DockerRuntime(lambda: client).delete(SANDBOX_ID, delete_workspace=delete_workspace)
    assert state.remaining() == set()
    assert all(client.close_calls == 1 for client in clients)
    assert len([event for event in state.mutations() if event[0] == "remove"]) == 5


@pytest.mark.anyio
async def test_concurrent_deletes_converge_without_recreating_resources() -> None:
    """Independent deletion requests must both accept resources removed by their peer."""
    state = DeletionState()
    clients = [DeletionClient(state), DeletionClient(state)]
    stopping = threading.Barrier(2)

    def overlap_stops() -> None:
        """Require both requests to finish preflight before either can remove proxy."""
        stopping.wait(timeout=5)

    state.original["proxy"].stop_hook = overlap_stops
    await asyncio.gather(*(
        DockerRuntime(lambda client=client: client).delete(SANDBOX_ID, delete_workspace=True)
        for client in clients
    ))
    assert state.remaining() == set()
    assert all(client.close_calls == 1 for client in clients)


@pytest.mark.anyio
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("change", ["missing_transaction", "invalid_transaction", "other_transaction", "duplicate"])
async def test_delete_preflight_rejects_ambiguous_ownership_before_mutation(role: ResourceKind, change: str) -> None:
    """All roles, including retained workspace, must pass one complete ownership preflight."""
    client = DeletionClient()
    target = client.state.original[role]
    if change == "missing_transaction":
        del target.labels["io.agent-sandbox.transaction-id"]
    elif change == "invalid_transaction":
        target.labels["io.agent-sandbox.transaction-id"] = "invalid"
    elif change == "other_transaction":
        target.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    else:
        client.state.add(role, "-duplicate")
    with pytest.raises(SandboxDeleteConflict):
        await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert client.state.mutations() == []
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("label", ["managed", "project", "sandbox-id", "resource"])
async def test_delete_never_selects_foreign_or_unmanaged_resources(label: str) -> None:
    """Resources outside the complete discovery selector must remain untouched."""
    state = DeletionState(("workspace",))
    del state.original["workspace"].labels[f"io.agent-sandbox.{label}"]
    client = DeletionClient(state)
    await DockerRuntime(lambda: client).delete(SANDBOX_ID, delete_workspace=True)
    assert state.remaining() == {"workspace"}
    assert state.mutations() == []


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["id", "managed", "project", "sandbox-id", "transaction-id", "resource"])
async def test_delete_rejects_changed_identity_during_preflight(change: str) -> None:
    """A listed candidate that changes before reload must fail before any deletion."""
    client = DeletionClient()

    def mutate(resource: DeletionResource) -> None:
        """Replace one inspected ownership fact while retaining the original lookup key."""
        if change == "id":
            resource.id = "foreign-id"
        else:
            resource.labels[f"io.agent-sandbox.{change}"] = "foreign"

    client.state.original["workspace"].reload_hook = mutate
    with pytest.raises(SandboxDeleteConflict):
        await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert client.state.mutations() == []
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["id", "transaction-id", "managed"])
async def test_delete_rechecks_ownership_after_stop_before_remove(change: str) -> None:
    """Changes made while stopping must prevent removing the now-untrusted container."""
    client = DeletionClient()
    target = client.state.original["proxy"]

    def mutate() -> None:
        """Change ownership after the bounded Docker stop returns."""
        if change == "id":
            target.id = "foreign-id"
        else:
            target.labels[f"io.agent-sandbox.{change}"] = "foreign"

    target.stop_hook = mutate
    with pytest.raises(SandboxDeleteConflict):
        await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert target.remove_calls == []
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("role", ROLES)
@pytest.mark.parametrize("change", ["id", "transaction-id"])
async def test_delete_rechecks_each_record_before_its_first_mutation(role: ResourceKind, change: str) -> None:
    """Every recorded role must be refreshed after preflight and before its own mutation."""
    client = DeletionClient()
    target = client.state.original[role]

    def mutate_after_preflight(resource: DeletionResource) -> None:
        """Change one exact identity only on the first post-discovery reload."""
        if resource.reload_calls == 2:
            if change == "id":
                resource.id = "replacement-id"
            else:
                resource.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32

    target.reload_hook = mutate_after_preflight
    with pytest.raises(SandboxDeleteConflict):
        await DockerRuntime(lambda: client).delete(SANDBOX_ID, delete_workspace=True)
    assert not [event for event in client.state.mutations() if event[1] == target.key]
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("group,key", [("containers", "proxy-id"), ("networks", "network-id"), ("volumes", "workspace-id")])
async def test_delete_accepts_disappearance_after_list(group: str, key: str) -> None:
    """A resource gone between discovery and exact lookup is already cleaned up."""
    client = DeletionClient()
    manager = getattr(client, group)

    def disappear(resource_id: str) -> None:
        """Remove only the requested test resource immediately before exact lookup."""
        if resource_id == key:
            client.state.resources[group].pop(key, None)

    manager.get_hook = disappear
    await DockerRuntime(lambda: client).delete(SANDBOX_ID, delete_workspace=True)
    assert client.state.remaining() == set()
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("role", ROLES)
async def test_delete_accepts_disappearance_during_remove(role: ResourceKind) -> None:
    """Docker NotFound during a destructive operation must count as an absent resource."""
    client = DeletionClient()
    target = client.state.original[role]

    def disappear() -> None:
        """Model a concurrent request removing the exact current resource."""
        client.state.resources[target.group].pop(target.key, None)

    target.remove_hook = disappear
    await DockerRuntime(lambda: client).delete(SANDBOX_ID, delete_workspace=True)
    assert client.state.remaining() == set()
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_delete_accepts_failed_remove_response_when_resource_is_really_absent() -> None:
    """An SDK error after daemon-side removal may succeed only after absence is verified."""
    client = DeletionClient()
    target = client.state.original["workspace"]

    def remove_before_error() -> None:
        """Apply removal while simulating a response lost after the Docker mutation."""
        client.state.resources["volumes"].pop(target.key, None)

    target.remove_hook = remove_before_error
    target.remove_error = deletion_api_error(500)
    await DockerRuntime(lambda: client).delete(SANDBOX_ID, delete_workspace=True)
    assert client.state.remaining() == set()
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_delete_does_not_trust_successful_remove_response_with_residue() -> None:
    """A successful SDK response must not conceal remaining runtime resources."""
    state = DeletionState(("network",))
    target = state.original["network"]
    target.remove_keeps_resource = True
    first = DeletionClient(state)
    with pytest.raises(SandboxDeleteFailed) as captured:
        await DockerRuntime(lambda: first).delete(SANDBOX_ID)
    assert captured.value.sandbox_id == SANDBOX_ID
    assert state.remaining() == {"network"}
    assert first.close_calls == 1
    target.remove_keeps_resource = False
    second = DeletionClient(state)
    await DockerRuntime(lambda: second).delete(SANDBOX_ID)
    assert state.remaining() == set()
    assert second.close_calls == 1


@pytest.mark.anyio
async def test_delete_stop_failure_revalidates_before_forced_container_removal() -> None:
    """A bounded stop failure may force only the same revalidated owned container."""
    client = DeletionClient()
    client.state.original["container"].stop_error = deletion_api_error(500)
    await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert client.state.remaining() == {"workspace"}
    assert client.state.original["container"].remove_calls[-1].get("force") is True
    for index, (action, key) in enumerate(client.state.events):
        if action == "remove":
            assert client.state.events[index - 2:index] == [("get", key), ("reload", key)]


@pytest.mark.anyio
async def test_delete_stop_failure_does_not_force_a_changed_container() -> None:
    """A failed stop must not bypass the ownership guard for forced fallback removal."""
    client = DeletionClient()
    target = client.state.original["proxy"]

    def change_transaction() -> None:
        """Simulate ownership replacement while a Docker stop request is in flight."""
        target.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32

    target.stop_hook = change_transaction
    target.stop_error = deletion_api_error(500)
    with pytest.raises(SandboxDeleteConflict):
        await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert target.remove_calls == []
    assert not [event for event in client.state.mutations() if event[0] == "kill"]
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("role", ["proxy", "container", "ingress", "network", "workspace"])
async def test_delete_partial_failure_is_reported_and_retry_converges(role: ResourceKind) -> None:
    """Occupied or failed resources must yield a stable failure, then be rediscovered on retry."""
    state = DeletionState()
    target = state.original[role]
    target.remove_error = deletion_api_error()
    first = DeletionClient(state)
    with pytest.raises(SandboxDeleteFailed) as captured:
        await DockerRuntime(lambda: first).delete(SANDBOX_ID, delete_workspace=True)
    assert captured.value.sandbox_id == SANDBOX_ID
    assert role in state.remaining()
    assert first.close_calls == 1
    target.remove_error = None
    second = DeletionClient(state)
    await DockerRuntime(lambda: second).delete(SANDBOX_ID, delete_workspace=True)
    assert state.remaining() == set()
    assert second.close_calls == 1
    if role in {"network", "ingress", "workspace"}:
        assert all(not kwargs.get("force", False) for kwargs in target.remove_calls)


@pytest.mark.anyio
async def test_delete_accepts_peer_finishing_an_earlier_failed_resource() -> None:
    """A peer that finishes a failed exact resource must let deletion reach its target."""
    client = DeletionClient()
    client.state.original["proxy"].remove_error = deletion_api_error()

    def finish_proxy() -> None:
        """Model a concurrent request completing the earlier failed proxy removal."""
        client.state.resources["containers"].pop("proxy-id", None)

    client.state.original["container"].stop_hook = finish_proxy
    await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert client.state.remaining() == {"workspace"}
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["managed", "id"])
async def test_delete_rechecks_failed_identity_even_if_final_selector_excludes_it(change: str) -> None:
    """A failed resource cannot be declared absent merely because ownership filtering lost it."""
    client = DeletionClient()
    target = client.state.original["proxy"]
    target.remove_error = deletion_api_error()

    def replace_identity() -> None:
        """Change the failed resource before the final convergence check."""
        if change == "id":
            target.id = "replacement-id"
        else:
            target.labels["io.agent-sandbox.managed"] = "false"

    client.state.original["container"].stop_hook = replace_identity
    with pytest.raises(SandboxDeleteConflict):
        await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_delete_backend_discovery_failure_closes_client() -> None:
    """SDK discovery transport failures must map to backend unavailable without mutation."""
    client = DeletionClient()
    client.networks.list_error = DockerException("private daemon details")
    with pytest.raises(DockerUnavailable):
        await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert client.state.mutations() == []
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_delete_backend_preflight_inspect_failure_closes_client() -> None:
    """Transport failure while recovering exact ownership is unavailable before mutation."""
    client = DeletionClient()

    def fail_inspect(_resource_id: str) -> None:
        """Fail the exact-ID ownership inspect without returning partial metadata."""
        raise DockerException("private daemon details")

    client.volumes.get_hook = fail_inspect
    with pytest.raises(DockerUnavailable):
        await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert client.state.mutations() == []
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_delete_factory_failure_maps_to_backend_unavailable() -> None:
    """Failure to open the lazy lifecycle transport must remain a backend error."""
    def unavailable() -> DeletionClient:
        """Simulate an unavailable Docker endpoint before a client is created."""
        raise DockerException("private socket")

    with pytest.raises(DockerUnavailable):
        await DockerRuntime(unavailable).delete(SANDBOX_ID)


@pytest.mark.anyio
@pytest.mark.parametrize("scenario", ["success", "conflict", "unavailable"])
async def test_delete_client_close_failure_preserves_primary_domain_error(scenario: str) -> None:
    """Close errors must fail successful deletion and preserve an existing classified failure."""
    client = DeletionClient()
    expected = SandboxDeleteFailed
    if scenario == "conflict":
        client.state.original["workspace"].labels["io.agent-sandbox.transaction-id"] = "invalid"
        expected = SandboxDeleteConflict
    elif scenario == "unavailable":
        client.networks.list_error = DockerException("private socket")
        expected = DockerUnavailable

    def fail_close() -> None:
        """Fail transport finalization after the fake records the close attempt."""
        raise RuntimeError("private close details")

    client.close_hook = fail_close
    with pytest.raises(expected) as captured:
        await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert client.close_calls == 1
    if scenario == "success":
        assert captured.value.sandbox_id == SANDBOX_ID
        assert client.state.remaining() == {"workspace"}
    else:
        assert client.state.mutations() == []


@pytest.mark.anyio
async def test_delete_default_factory_is_lazy_with_finite_lifecycle_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Deletion must lazily use the shared finite five-second Docker SDK transport."""
    client = DeletionClient()
    calls: list[dict[str, Any]] = []

    def from_env(**kwargs: Any) -> DeletionClient:
        """Capture transport construction without opening a real Docker connection."""
        calls.append(kwargs)
        return client

    monkeypatch.setattr(docker, "from_env", from_env)
    runtime = DockerRuntime()
    assert calls == []
    await runtime.delete(SANDBOX_ID)
    assert calls == [{"timeout": 5.0}]
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("role,part,value", [
    ("proxy", "Config", None),
    ("proxy", "Config", []),
    ("proxy", "Config", "malformed"),
    ("container", "Labels", None),
    ("container", "Labels", []),
    ("workspace", "Labels", None),
    ("network", "Labels", "malformed"),
])
async def test_delete_malformed_candidate_metadata_is_an_ownership_conflict(role: ResourceKind, part: str, value: Any) -> None:
    """Malformed reloaded ownership metadata must refuse deletion rather than appear trusted."""
    client = DeletionClient()

    def corrupt_inspect(resource: DeletionResource) -> None:
        """Replace candidate inspect data after Docker label filtering has selected it."""
        if part == "Config" or resource.group != "containers":
            resource.attrs[part] = value
        else:
            resource.attrs["Config"][part] = value

    client.state.original[role].reload_hook = corrupt_inspect
    with pytest.raises(SandboxDeleteConflict):
        await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert client.state.mutations() == []
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.timeout(10)
@pytest.mark.parametrize("worker_fails", [False, True])
async def test_delete_repeated_cancellation_waits_for_worker_and_client_close(worker_fails: bool) -> None:
    """Repeated cancellation must propagate only after cleanup and transport close finish."""
    client = DeletionClient()
    stop_entered, stop_release = threading.Event(), threading.Event()
    close_entered, close_release = threading.Event(), threading.Event()
    target = client.state.original["proxy"]

    def block_stop() -> None:
        """Keep the Docker worker inside its first destructive operation."""
        stop_entered.set()
        assert stop_release.wait(5)

    def block_close() -> None:
        """Keep transport finalization pending after the worker finishes deletion."""
        close_entered.set()
        assert close_release.wait(5)

    target.stop_hook = block_stop
    client.close_hook = block_close
    if worker_fails:
        client.state.original["network"].remove_error = deletion_api_error()
    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, Any]] = []
    previous_handler = loop.get_exception_handler()

    def observe_loop_error(_loop: asyncio.AbstractEventLoop, context: dict[str, Any]) -> None:
        """Capture otherwise unhandled worker exceptions after cancellation."""
        loop_errors.append(context)

    loop.set_exception_handler(observe_loop_error)
    task = asyncio.create_task(DockerRuntime(lambda: client).delete(SANDBOX_ID))
    try:
        assert await asyncio.to_thread(stop_entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        stop_release.set()
        assert await asyncio.to_thread(close_entered.wait, 3)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not client.closed.is_set()
        close_release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.closed.is_set()
        assert client.close_calls == 1
        assert client.state.remaining() == ({"workspace", "network"} if worker_fails else {"workspace"})
        await asyncio.sleep(0)
        assert loop_errors == []
    finally:
        stop_release.set()
        close_release.set()
        loop.set_exception_handler(previous_handler)
