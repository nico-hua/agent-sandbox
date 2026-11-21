"""Default fake-Docker tests for per-sandbox egress creation and lifecycle ownership."""

import asyncio
import json
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from types import SimpleNamespace

import pytest
from docker.errors import DockerException

from control_plane.domain.errors import (
    CreationResourceState,
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxDeleteConflict,
    SandboxEndpointUnavailable,
    SandboxQueryFailed,
)
from control_plane.domain.sandbox import SandboxLifecycleRecord, SandboxNetwork, SandboxSpec
from control_plane.services.sandbox_service import SandboxService
from control_plane.storage.sqlite.sqlite_store import SQLiteLifecycleStore
from control_plane.runtime.docker.docker_runtime import DockerRuntime
from control_plane.tests.runtime.fakes import (
    DeletionClient,
    DeletionState,
    ROLES,
)
from control_plane.tests.runtime.fakes import (
    RecordingClient,
    RecordingManager,
    RecordingResource,
    SANDBOX_ID,
    creation_labels,
    make_api_error,
    make_spec,
)
from control_plane.tests.runtime.fakes import (
    ALL_ROLES,
    DOMAINS,
    EGRESS_ID,
    EGRESS_IMAGE,
    EgressCreationClient,
    PROXY_ENV,
    VERSION,
    configured_inventory,
    egress_spec,
)

def environment_mapping(value: Any) -> dict[str, str]:
    """Normalize Docker SDK's accepted environment shapes for semantic assertions."""
    return value if isinstance(value, dict) else dict(item.split("=", 1) for item in value or [])


@pytest.mark.anyio
async def test_enabled_egress_creation_pins_image_and_enforces_security_and_policy() -> None:
    """Only an enabled policy adds a locked-down egress container and sandbox proxy variables."""
    spec = egress_spec()
    client = EgressCreationClient(spec)
    await DockerRuntime(lambda: client).create(spec)
    calls = client.containers.create_calls
    sandbox, proxy, egress = calls
    assert egress["image"] == EGRESS_IMAGE
    assert egress["name"] == spec.egress_name
    assert egress["user"] == "10001:10001"
    host = egress["host_config"]
    assert host["nano_cpus"] == 500_000_000
    assert host["mem_limit"] == host["memswap_limit"] == 134217728
    assert host["pids_limit"] == 32 and host["cap_drop"] == ["ALL"]
    assert host["read_only"] is True and host["security_opt"] == ["no-new-privileges:true"]
    assert not host.get("binds") and not host.get("port_bindings") and not egress.get("ports")
    assert not egress.get("volumes") and host["network_mode"] == spec.ingress_network_name
    assert egress["networking_config"] == {"EndpointsConfig": {spec.ingress_network_name: {}}}
    arguments = egress["command"]
    assert [arguments[i + 1] for i, item in enumerate(arguments) if item == "-allow-domain"] == ["api.example.com", "example.com"]
    assert arguments == ["-allow-domain", "api.example.com", "-allow-domain", "example.com"]
    assert client.attachments[-1] == (EGRESS_ID, "network-id", {"aliases": ["egress"]})
    assert environment_mapping(sandbox.get("environment")) == PROXY_ENV
    assert sandbox["healthcheck"]["test"] == [
        "CMD", "wget", "-Y", "off", "-q", "-T", "2", "-O", "/dev/null", "http://127.0.0.1:8080/healthz",
    ]
    assert environment_mapping(proxy.get("environment")) == {"SANDBOX_ACCESS_TOKEN": spec.access_token, "SANDBOX_PROXY_AUTH_VERSION": "1"}
    for call in [*client.networks.create_calls, *client.volumes.create_calls, *calls]:
        assert call["labels"][VERSION] == "1"
        assert call["labels"][DOMAINS] == '["api.example.com","example.com"]'
    assert client.events.index("proxy.start") < client.events.index("egress.connect") < client.events.index("egress.start")
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_disabled_network_omits_egress_image_container_and_proxy_environment() -> None:
    """An empty allowlist retains five resources and records explicit disabled metadata."""
    spec = make_spec()
    client = EgressCreationClient(spec)
    await DockerRuntime(lambda: client).create(spec)
    assert len(client.containers.create_calls) == 2
    assert "images.get:agent-sandbox-egress:dev" not in client.events
    assert not environment_mapping(client.containers.create_calls[0].get("environment"))
    for call in [*client.networks.create_calls, *client.volumes.create_calls, *client.containers.create_calls]:
        assert call["labels"][VERSION] == "1" and call["labels"][DOMAINS] == "[]"


@pytest.mark.anyio
async def test_per_sandbox_policies_do_not_share_mutable_creation_configuration() -> None:
    """Independent runtime instances propagate their own canonical allowlists without cross-talk."""
    specs = [egress_spec(("one.example",)), egress_spec(("two.example",))]
    clients = [EgressCreationClient(spec) for spec in specs]
    await asyncio.gather(*(DockerRuntime(lambda client=client: client).create(spec) for spec, client in zip(specs, clients)))
    for spec, client in zip(specs, clients):
        call = client.containers.create_calls[-1]
        assert json.loads(call["labels"][DOMAINS]) == list(spec.network.allow_domains)
        assert call["command"][-2:] == ["-allow-domain", spec.network.allow_domains[0]]


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["create", "connect", "start"])
async def test_egress_failure_rolls_back_raw_ids_in_exact_reverse_order(stage: str) -> None:
    """Failures after recording egress identity must clean it before the older five resources."""
    spec = egress_spec()
    client = EgressCreationClient(spec)
    original_create, original_connect = client.api.create_container, client.api.connect_container_to_network

    def fail_create(**kwargs: Any) -> dict[str, str]:
        """Reject only the sixth resource before a create ID is confirmed."""
        if kwargs["labels"]["io.agent-sandbox.resource"] == "egress":
            raise make_api_error(500)
        return original_create(**kwargs)

    def fail_connect(container: str, network: str, **kwargs: Any) -> None:
        """Fail attachment only after the exact egress create ID was recorded."""
        if container == EGRESS_ID:
            raise make_api_error(500)
        original_connect(container, network, **kwargs)

    if stage == "create":
        client.api.create_container = fail_create
    elif stage == "connect":
        client.api.connect_container_to_network = fail_connect
    else:
        client.egress.start_error = make_api_error(500)
    with pytest.raises(SandboxCreateFailed) as caught:
        await DockerRuntime(lambda: client).create(spec)
    expected = ([] if stage == "create" else ["egress.remove"]) + ["proxy.remove", "container.remove", "workspace.remove", "ingress.remove", "network.remove"]
    assert [item for item in client.events if item.endswith(".remove")] == expected
    assert caught.value.creation_failure.phase == f"{stage}_egress"
    assert caught.value.creation_failure.resource_state == CreationResourceState.ROLLED_BACK
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("stage", ["create", "connect", "start"])
async def test_repeated_cancel_during_egress_waits_then_rolls_back_all_ids(stage: str) -> None:
    """Repeated caller cancellation cannot interrupt late-create recording or reverse rollback."""
    spec = egress_spec()
    client = EgressCreationClient(spec)
    entered, release = threading.Event(), threading.Event()
    original_create, original_connect = client.api.create_container, client.api.connect_container_to_network

    def blocked_create(**kwargs: Any) -> dict[str, str]:
        """Hold a successful egress create response until cancellation has been delivered."""
        if kwargs["labels"]["io.agent-sandbox.resource"] == "egress":
            entered.set()
            assert release.wait(3)
        return original_create(**kwargs)

    def blocked_connect(container: str, network: str, **kwargs: Any) -> None:
        """Hold the recorded egress attachment while cancellation is repeated."""
        if container == EGRESS_ID:
            entered.set()
            assert release.wait(3)
        original_connect(container, network, **kwargs)

    if stage == "create":
        client.api.create_container = blocked_create
    elif stage == "connect":
        client.api.connect_container_to_network = blocked_connect
    else:
        client.egress.start_entered, client.egress.start_release = entered, release
    task = asyncio.create_task(DockerRuntime(lambda: client).create(spec))
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert [item for item in client.events if item.endswith(".remove")] == ["egress.remove", "proxy.remove", "container.remove", "workspace.remove", "ingress.remove", "network.remove"]
        assert client.close_calls == 1
    finally:
        release.set()
        if not task.done():
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.anyio
@pytest.mark.parametrize("cancelled", [False, True])
async def test_lost_egress_create_response_retains_unknown_after_known_ids_rollback(cancelled: bool) -> None:
    """A transport failure may hide daemon-side egress creation and cannot become rolled back."""
    spec = egress_spec()
    client = EgressCreationClient(spec)
    original = client.api.create_container
    entered, release = threading.Event(), threading.Event()

    def lost_response(**kwargs: Any) -> dict[str, str]:
        """Raise only after simulating an unconfirmed egress create response."""
        if kwargs["labels"]["io.agent-sandbox.resource"] == "egress":
            if cancelled:
                entered.set()
                assert release.wait(3)
            raise DockerException("private lost create response")
        return original(**kwargs)

    client.api.create_container = lost_response
    task = asyncio.create_task(DockerRuntime(lambda: client).create(spec))
    try:
        if cancelled:
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            release.set()
        with pytest.raises((SandboxCleanupFailed, DockerUnavailable)) as caught:
            await task
    finally:
        release.set()
        if not task.done():
            await asyncio.gather(task, return_exceptions=True)
    assert caught.value.creation_failure.phase == "create_egress"
    assert caught.value.creation_failure.resource_state == CreationResourceState.UNKNOWN
    assert not client.egress.remove_calls
    assert client.container.removed.is_set() and client.proxy.removed.is_set()
    assert client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize(("state", "expected_status", "reason"), [
    (None, "failed", "egress_missing"),
    ({"Status": "exited"}, "failed", "egress_stopped"),
    ({"Status": "running", "Health": {"Status": "unhealthy"}}, "failed", "egress_unhealthy"),
    ({"Status": "running", "Health": {"Status": "starting"}}, "starting", "egress_starting"),
    ({"Status": "running", "Health": {"Status": "healthy"}}, "ready", "health_check_passed"),
])
async def test_query_requires_healthy_declared_egress(state: Any, expected_status: str, reason: str) -> None:
    """Main Agent health cannot mask missing, stopped, unhealthy or starting declared egress."""
    client = configured_inventory()
    if state is None:
        del client.state.resources["containers"]["egress-id"]
    else:
        client.state.original["egress"].attrs["State"] = state
    result = await DockerRuntime(lambda: client).get(SANDBOX_ID)
    assert (result.status, result.reason) == (expected_status, reason)
    assert client.state.mutations() == [] and client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("domains", [None, (), ("example.com",)])
async def test_inspection_requires_six_roles_only_when_immutable_policy_enabled(domains: Any) -> None:
    """Both absent markers preserve five legacy roles while enabled markers require six."""
    client = configured_inventory(domains)
    result = await DockerRuntime(lambda: client).inspect_resources(SANDBOX_ID, expected_transaction_id=make_spec().transaction_id)
    assert result.classification == "all_present"
    assert set(result.found_resources) == set(ALL_ROLES if domains else ROLES)
    assert not result.missing_resources and client.state.mutations() == []
    assert sum(len(manager.list_calls) for manager in (client.containers, client.networks, client.volumes)) == 6


@pytest.mark.anyio
async def test_missing_declared_egress_is_partial_inventory() -> None:
    """Required egress absence prevents pending resource inventory from becoming all-present."""
    client = configured_inventory()
    del client.state.resources["containers"]["egress-id"]
    result = await DockerRuntime(lambda: client).inspect_resources(SANDBOX_ID, expected_transaction_id=make_spec().transaction_id)
    assert result.classification == "partial" and result.missing_resources == ("egress",)


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["version_missing", "domains_missing", "both_missing", "unknown_version", "invalid_json", "noncanonical", "inconsistent", "stray_disabled"])
async def test_damaged_network_markers_block_reads_but_preserve_ownership_cleanup(change: str) -> None:
    """Configuration uncertainty prevents query/endpoint/inspection success without stranding owned resources."""
    client = configured_inventory()
    main = client.state.original["container"].labels
    if change == "version_missing":
        del main[VERSION]
    elif change == "domains_missing":
        del main[DOMAINS]
    elif change == "both_missing":
        del main[VERSION]
        del main[DOMAINS]
    elif change == "unknown_version":
        main[VERSION] = "2"
    elif change == "invalid_json":
        main[DOMAINS] = "private malformed"
    elif change == "noncanonical":
        main[DOMAINS] = '["EXAMPLE.COM"]'
    elif change == "inconsistent":
        client.state.original["proxy"].labels[DOMAINS] = '["different.example"]'
    else:
        for target in client.state.original.values():
            target.labels[DOMAINS] = "[]"
    runtime = DockerRuntime(lambda: client)
    with pytest.raises(SandboxQueryFailed):
        await runtime.get(SANDBOX_ID)
    with pytest.raises(SandboxEndpointUnavailable):
        await runtime.get_endpoint(SANDBOX_ID)
    inspection = await runtime.inspect_resources(SANDBOX_ID, expected_transaction_id=make_spec().transaction_id)
    assert inspection.classification == "conflict" and not client.state.mutations()
    await runtime.delete(SANDBOX_ID, delete_workspace=True)
    assert not client.state.remaining()
    assert [key for action, key in client.state.mutations() if action == "remove"] == ["egress-id", "proxy-id", "container-id", "ingress-id", "network-id", "workspace-id"]


@pytest.mark.anyio
async def test_delete_discovers_residual_egress_when_main_container_is_absent() -> None:
    """A standalone owned egress container remains discoverable through its complete ownership labels."""
    client = DeletionClient(DeletionState(("egress",)))
    await DockerRuntime(lambda: client).delete(SANDBOX_ID)
    assert not client.state.remaining()
    assert client.state.original["egress"].stop_calls == [{"timeout": 2}]
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_wrong_egress_transaction_prevents_all_deletion_mutations() -> None:
    """The sixth role participates in complete transaction preflight before any resource is stopped."""
    client = configured_inventory()
    client.state.original["egress"].labels["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    with pytest.raises(SandboxDeleteConflict):
        await DockerRuntime(lambda: client).delete(SANDBOX_ID, delete_workspace=True)
    assert not client.state.mutations()


@pytest.mark.anyio
@pytest.mark.parametrize("state", ["missing", "unhealthy", "starting"])
async def test_pending_recovery_does_not_activate_unready_egress(tmp_path: Path, state: str) -> None:
    """Real SQLite pending rows remain untouched when actual runtime inventory or egress health is insufficient."""
    client = configured_inventory()
    now = datetime(2026, 10, 22, 8, tzinfo=UTC)
    record = SandboxLifecycleRecord(SANDBOX_ID, make_spec().transaction_id, now - timedelta(minutes=1), now + timedelta(minutes=29))
    for target in client.state.original.values():
        target.labels.update({
            "io.agent-sandbox.created-at": record.created_at.isoformat().replace("+00:00", "Z"),
            "io.agent-sandbox.expires-at": record.expires_at.isoformat().replace("+00:00", "Z"),
        })
    if state == "missing":
        del client.state.resources["containers"]["egress-id"]
    else:
        client.state.original["egress"].attrs["State"]["Health"]["Status"] = state
    store = SQLiteLifecycleStore(tmp_path / "egress-pending.sqlite3")
    await store.initialize()
    await store.create(record)
    runtime = DockerRuntime(lambda: client)
    result = await SandboxService(runtime, store, clock=lambda: now).recover_pending_once()
    assert result.recovered == 0 and result.skipped == 1 and not result.error
    assert result.items[0].reason == ("resources_partial" if state == "missing" else "sandbox_not_ready")
    assert await store.get(SANDBOX_ID) == record
    assert not client.state.mutations()


@pytest.mark.anyio
@pytest.mark.parametrize("change", ["extra_network", "network_id", "missing_alias", "published_port", "mount", "main_extra_network"])
async def test_query_rejects_egress_topology_drift(change: str) -> None:
    """Healthy container health does not override unexpected networks, mounts or published ports."""
    client = configured_inventory()
    attrs = client.state.original["egress"].attrs
    internal_name = make_spec().network_name
    if change == "extra_network":
        attrs["NetworkSettings"]["Networks"]["bridge"] = {"NetworkID": "foreign"}
    elif change == "network_id":
        attrs["NetworkSettings"]["Networks"][internal_name]["NetworkID"] = "foreign"
    elif change == "missing_alias":
        attrs["NetworkSettings"]["Networks"][internal_name]["Aliases"] = []
    elif change == "published_port":
        attrs["NetworkSettings"]["Ports"] = {"18084/tcp": [{"HostIp": "127.0.0.1", "HostPort": "49154"}]}
    elif change == "mount":
        attrs["Mounts"] = [{"Source": "/unexpected", "Destination": "/data"}]
    else:
        client.state.original["container"].attrs["NetworkSettings"]["Networks"]["bridge"] = {"NetworkID": "foreign"}
    with pytest.raises(SandboxQueryFailed):
        await DockerRuntime(lambda: client).get(SANDBOX_ID)
    assert not client.state.mutations() and client.close_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [ValueError("private decoder details"), TypeError("private transport shape")])
async def test_backend_decoder_errors_are_not_network_marker_conflicts(failure: Exception) -> None:
    """Only failures parsing actual policy labels are classified as configuration conflicts."""
    client = configured_inventory()
    client.networks.list_error = failure
    result = await DockerRuntime(lambda: client).inspect_resources(
        SANDBOX_ID, expected_transaction_id=make_spec().transaction_id,
    )
    assert result.classification == "unavailable" and result.reason == "docker_unavailable"
    assert not client.state.mutations() and client.close_calls == 1
