"""Stateful fake-Docker pause/resume tests without touching a real daemon."""

import asyncio
import threading
from typing import Any

import pytest
from docker.errors import DockerException

from control_plane.core.errors import SandboxNotFound
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime, map_container_state
from test_sandbox_egress_runtime import DOMAINS, configured_inventory
from test_sandbox_runtime import SANDBOX_ID, make_spec


async def transition(runtime: DockerSandboxRuntime, action: str) -> None:
    """Request one transaction-bound lifecycle change through the public runtime."""
    await getattr(runtime, action)(SANDBOX_ID, expected_transaction_id=make_spec().transaction_id)


def pause_inventory(domains: Any = ("example.com",)) -> Any:
    """Add observable pause/unpause semantics to the existing realistic inventory."""
    client = configured_inventory(domains)
    for role in ("container", "egress", "proxy"):
        if role not in client.state.original:
            continue
        resource = client.state.original[role]

        def change(paused: bool, resource: Any = resource) -> None:
            """Change actual inspect facts while retaining operation order."""
            client.state.events.append(("pause" if paused else "unpause", resource.key))
            resource.attrs["State"].update(Status="running", Paused=paused)

        resource.pause = lambda change=change: change(True)
        resource.unpause = lambda change=change: change(False)
    return client


def transitions(client: Any) -> list[tuple[str, str]]:
    """Extract externally visible freeze/thaw actions from the daemon event log."""
    return [event for event in client.state.events if event[0] in {"pause", "unpause"}]


@pytest.mark.anyio
@pytest.mark.parametrize("domains", [None, (), ("example.com",)])
async def test_pause_resume_orders_targets_and_is_idempotent(domains: Any) -> None:
    """Freeze main before egress, thaw egress before main and leave ingress proxy alone."""
    client = pause_inventory(domains)
    runtime = DockerSandboxRuntime(lambda: client)
    await transition(runtime, "pause")
    await transition(runtime, "pause")
    assert (await runtime.get(SANDBOX_ID)).status == "paused"
    await transition(runtime, "resume")
    await transition(runtime, "resume")
    expected = [("pause", "container-id")]
    if domains:
        expected += [("pause", "egress-id"), ("unpause", "egress-id")]
    assert transitions(client) == [*expected, ("unpause", "container-id")]
    assert client.state.original["proxy"].attrs["State"].get("Paused") is not True
    assert client.close_calls == 5


@pytest.mark.parametrize("state", [{"Status": "paused"}, {"Status": "running", "Paused": True, "Health": {"Status": "healthy"}}])
def test_paused_mapping_ignores_stale_agent_health(state: dict[str, Any]) -> None:
    """Both Docker paused representations report paused despite cached healthy state."""
    result = map_container_state(SANDBOX_ID, state)
    assert (result.status, result.reason) == ("paused", "container_paused")


@pytest.mark.anyio
@pytest.mark.parametrize("action", ["pause", "resume"])
@pytest.mark.parametrize("damage", ["missing_main", "missing_egress", "missing_workspace", "transaction", "markers", "stopped", "stopped_main", "extra_network"])
async def test_transition_preflights_entire_inventory_before_mutation(action: str, damage: str) -> None:
    """Incomplete, stopped, foreign or topology-drifted resources cannot trigger partial mutation."""
    client = pause_inventory()
    if damage.startswith("missing_"):
        role = {"missing_main": "container", "missing_egress": "egress", "missing_workspace": "workspace"}[damage]
        target = client.state.original[role]
        del client.state.resources[target.group][target.key]
    elif damage == "transaction":
        client.state.original["egress"].labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    elif damage == "markers":
        client.state.original["workspace"].labels[DOMAINS] = "[]"
    elif damage in {"stopped", "stopped_main"}:
        client.state.original["container" if damage == "stopped_main" else "egress"].attrs["State"] = {"Status": "exited"}
    else:
        client.state.original["egress"].attrs["NetworkSettings"]["Networks"]["bridge"] = {"NetworkID": "foreign"}
    from control_plane.core import errors
    expected = SandboxNotFound if damage == "missing_main" else getattr(errors, f"Sandbox{action.title()}Conflict")
    with pytest.raises(expected):
        await transition(DockerSandboxRuntime(lambda: client), action)
    assert not transitions(client) and not client.state.mutations()
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("action", ["pause", "resume"])
@pytest.mark.parametrize("applied", [False, True])
@pytest.mark.parametrize("stage", ["first", "second"])
async def test_lost_response_rechecks_actual_state_without_compensation(
    action: str, applied: bool, stage: str,
) -> None:
    """Inspect lost responses at either target and converge partial retries without compensation."""
    client = pause_inventory()
    if action == "resume":
        for role in ("container", "egress"):
            client.state.original[role].attrs["State"]["Paused"] = True
    roles = ("container", "egress") if action == "pause" else ("egress", "container")
    target = client.state.original[roles[0 if stage == "first" else 1]]
    method = "pause" if action == "pause" else "unpause"
    expected = [(method, f"{role}-id") for role in roles]
    original = getattr(target, method)

    def lost_response() -> None:
        """Optionally apply the daemon mutation before simulating a dropped response."""
        if applied:
            original()
        raise DockerException("private lost response")

    setattr(target, method, lost_response)
    runtime = DockerSandboxRuntime(lambda: client)
    if applied:
        await transition(runtime, action)
        assert transitions(client) == expected
    else:
        from control_plane.core import errors
        with pytest.raises(getattr(errors, f"Sandbox{action.title()}Failed")):
            await transition(runtime, action)
        assert transitions(client) == ([] if stage == "first" else expected[:1])
    assert client.close_calls == 1
    if not applied and stage == "second":
        result = await runtime.get(SANDBOX_ID)
        assert (result.status, result.reason) == ("failed", "pause_incomplete")
        setattr(target, method, original)
        await transition(runtime, action)
        assert transitions(client) == expected
        assert (await runtime.get(SANDBOX_ID)).status == ("paused" if action == "pause" else "ready")


@pytest.mark.anyio
@pytest.mark.parametrize("main_paused", [False, True])
async def test_mixed_pause_query_and_retry_converge(main_paused: bool) -> None:
    """A mixed state is observable and retry freezes only the still-running participant."""
    client = pause_inventory()
    client.state.original["container"].attrs["State"]["Paused"] = main_paused
    client.state.original["egress"].attrs["State"]["Paused"] = not main_paused
    runtime = DockerSandboxRuntime(lambda: client)
    result = await runtime.get(SANDBOX_ID)
    assert (result.status, result.reason) == ("failed", "pause_incomplete")
    await transition(runtime, "pause")
    assert transitions(client) == [("pause", "egress-id" if main_paused else "container-id")]
    assert (await runtime.get(SANDBOX_ID)).status == "paused"


@pytest.mark.anyio
@pytest.mark.parametrize("action", ["pause", "resume"])
@pytest.mark.parametrize("stage", ["first", "second"])
async def test_repeated_cancellation_settles_current_call_without_next_mutation(action: str, stage: str) -> None:
    """Cancellation at either target settles SDK work and close without compensating completed changes."""
    client = pause_inventory()
    if action == "resume":
        for role in ("container", "egress"):
            client.state.original[role].attrs["State"]["Paused"] = True
    roles = ("container", "egress") if action == "pause" else ("egress", "container")
    target = client.state.original[roles[0 if stage == "first" else 1]]
    method = "pause" if action == "pause" else "unpause"
    original = getattr(target, method)
    entered, release, closing, closed = (threading.Event() for _ in range(4))

    def blocked_operation() -> None:
        """Hold the selected operation across repeated cancellation before completing its mutation."""
        entered.set()
        assert release.wait(3)
        original()

    def blocked_close() -> None:
        """Keep the transport alive until the test confirms the caller remains pending."""
        closing.set()
        assert closed.wait(3)

    setattr(target, method, blocked_operation)
    client.close_hook = blocked_close
    task = asyncio.create_task(transition(DockerSandboxRuntime(lambda: client), action))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        release.set()
        assert await asyncio.to_thread(closing.wait, 2)
        assert not task.done()
        task.cancel()
        closed.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        expected_roles = roles[:1] if stage == "first" else roles
        assert transitions(client) == [(method, f"{role}-id") for role in expected_roles]
        assert client.closed.is_set()
    finally:
        release.set()
        closed.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
async def test_delete_paused_containers_never_thaws_or_sends_graceful_stop() -> None:
    """Paused user processes stay frozen until exact-ID force removal destroys them."""
    client = pause_inventory()
    for role in ("container", "egress", "proxy"):
        client.state.original[role].attrs["State"]["Paused"] = True
    await DockerSandboxRuntime(lambda: client).delete(SANDBOX_ID, delete_workspace=True)
    assert not client.state.remaining() and not transitions(client)
    for role in ("container", "egress", "proxy"):
        resource = client.state.original[role]
        assert not resource.stop_calls
        assert resource.remove_calls == [{"force": True, "v": False}]


@pytest.mark.anyio
@pytest.mark.parametrize("health", ["healthy", "starting", "unhealthy"])
async def test_mixed_pause_is_not_masked_by_stale_egress_health(health: str) -> None:
    """A live egress participant makes a frozen main incomplete regardless of cached health."""
    client = pause_inventory()
    client.state.original["container"].attrs["State"]["Paused"] = True
    client.state.original["egress"].attrs["State"]["Health"]["Status"] = health
    result = await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)
    assert (result.status, result.reason) == ("failed", "pause_incomplete")


@pytest.mark.anyio
@pytest.mark.parametrize("action", ["pause", "resume"])
async def test_stale_paused_flag_cannot_make_exited_container_mutable(action: str) -> None:
    """A contradictory frozen flag never authorizes a lifecycle operation on an exited process."""
    client = pause_inventory()
    client.state.original["egress"].attrs["State"] = {"Status": "exited", "Paused": True}
    from control_plane.core import errors
    with pytest.raises(getattr(errors, f"Sandbox{action.title()}Conflict")):
        await transition(DockerSandboxRuntime(lambda: client), action)
    assert not transitions(client)


@pytest.mark.anyio
@pytest.mark.parametrize("action", ["pause", "resume"])
async def test_final_inventory_catches_earlier_target_race(action: str) -> None:
    """A second participant cannot hide concurrent reversal of the first participant's state."""
    client = pause_inventory()
    if action == "resume":
        for role in ("container", "egress"):
            client.state.original[role].attrs["State"]["Paused"] = True
    first = client.state.original["container" if action == "pause" else "egress"]
    second = client.state.original["egress" if action == "pause" else "container"]
    method = "pause" if action == "pause" else "unpause"
    original = getattr(second, method)

    def race() -> None:
        """Complete this mutation while reversing the previously settled first target."""
        original()
        first.attrs["State"]["Paused"] = action != "pause"

    setattr(second, method, race)
    from control_plane.core import errors
    with pytest.raises(getattr(errors, f"Sandbox{action.title()}Failed")):
        await transition(DockerSandboxRuntime(lambda: client), action)
    assert len(transitions(client)) == 2 and client.close_calls == 1


@pytest.mark.anyio
async def test_live_unhealthy_main_with_frozen_egress_reports_mixed_state() -> None:
    """Stale main health cannot hide partial freezing when egress is already paused."""
    client = pause_inventory()
    client.state.original["container"].attrs["State"]["Health"]["Status"] = "unhealthy"
    client.state.original["egress"].attrs["State"]["Paused"] = True
    result = await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)
    assert (result.status, result.reason) == ("failed", "pause_incomplete")


@pytest.mark.anyio
@pytest.mark.parametrize("action", ["pause", "resume"])
async def test_ownership_change_after_first_mutation_stops_second_target(action: str) -> None:
    """Cross-stage ownership drift returns an incomplete outcome without touching a foreign resource."""
    client = pause_inventory()
    if action == "resume":
        for role in ("container", "egress"):
            client.state.original[role].attrs["State"]["Paused"] = True
    first = client.state.original["container" if action == "pause" else "egress"]
    second = client.state.original["egress" if action == "pause" else "container"]
    method = "pause" if action == "pause" else "unpause"
    original = getattr(first, method)

    def ownership_race() -> None:
        """Complete the first target then transfer the next target to another transaction."""
        original()
        second.labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32

    setattr(first, method, ownership_race)
    from control_plane.core import errors
    with pytest.raises(getattr(errors, f"Sandbox{action.title()}Failed")):
        await transition(DockerSandboxRuntime(lambda: client), action)
    assert transitions(client) == [(method, first.key)]
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_egress_disappearing_in_final_topology_inspection_is_not_success() -> None:
    """Final topology inspection must fail if the required egress vanished after inventory reload."""
    client = pause_inventory()
    egress = client.state.original["egress"]
    original = egress.pause

    def disappear_after_pause() -> None:
        """Install a late disappearance after Docker acknowledged freezing the egress."""
        original()
        reads = 0

        def vanish_on_final_topology(resource_id: str) -> None:
            """Remove egress when the last preflight has moved from inventory to topology checks."""
            nonlocal reads
            if resource_id == "egress-id":
                reads += 1
                if reads == 3:
                    del client.state.resources["containers"][resource_id]

        client.containers.get_hook = vanish_on_final_topology

    egress.pause = disappear_after_pause
    from control_plane.core.errors import SandboxPauseFailed
    with pytest.raises(SandboxPauseFailed):
        await transition(DockerSandboxRuntime(lambda: client), "pause")
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("domains", [None, ()])
@pytest.mark.parametrize("role", ["network", "ingress"])
@pytest.mark.parametrize("action", ["pause", "resume"])
async def test_five_role_transition_rejects_same_label_replacement_network(
    domains: Any, role: str, action: str,
) -> None:
    """A same-name, same-label replacement bridge cannot replace an original inventory identity."""
    from copy import deepcopy
    from control_plane.core import errors

    client = pause_inventory(domains)
    main = client.state.original["container"]
    if action == "resume":
        main.attrs["State"]["Paused"] = True
    method = "pause" if action == "pause" else "unpause"
    original = getattr(main, method)

    def replace_network_after_mutation() -> None:
        """Keep the old bridge while moving attachments onto a fully relabelled replacement."""
        original()
        previous = client.state.original[role]
        replacement = client.state.add(role, "-replacement")
        replacement.attrs = deepcopy(previous.attrs)
        network_name = previous.attrs["Name"]
        for participant in ("container", "proxy") if role == "network" else ("proxy",):
            client.state.original[participant].attrs["NetworkSettings"]["Networks"][network_name]["NetworkID"] = replacement.id

    setattr(main, method, replace_network_after_mutation)
    with pytest.raises(getattr(errors, f"Sandbox{action.title()}Failed")):
        await transition(DockerSandboxRuntime(lambda: client), action)
    assert transitions(client) == [(method, "container-id")]
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("initial_health", ["healthy", "unhealthy"])
async def test_query_adopts_both_paused_during_egress_inventory(initial_health: str) -> None:
    """A later fully paused observation must replace health mapped from the earlier main snapshot."""
    client = pause_inventory()
    client.state.original["container"].attrs["State"]["Health"]["Status"] = initial_health

    def freeze_during_inventory(resource: Any) -> None:
        """Freeze both participants after main mapping, when egress is first discovered."""
        if resource.reload_calls == 1:
            for role in ("container", "egress"):
                client.state.original[role].attrs["State"]["Paused"] = True

    client.state.original["egress"].reload_hook = freeze_during_inventory
    result = await DockerSandboxRuntime(lambda: client).get(SANDBOX_ID)
    assert (result.status, result.reason) == ("paused", "container_paused")
    assert not transitions(client) and client.close_calls == 1
