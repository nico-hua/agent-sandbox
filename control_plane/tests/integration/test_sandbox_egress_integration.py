"""Explicit Docker lifecycle checks; public traffic requires a separate opt-in."""

import asyncio
import json
import ipaddress
import os
import re
import secrets
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import docker
import httpx
import pytest

from control_plane.tests.fakes import create_test_app
from control_plane.runtime.docker_runtime import DockerRuntime
from control_plane.storage.sqlite_store import SQLiteLifecycleStore
from control_plane.tests.integration.helpers import (
    assert_http_error,
    response_data,
    CreatedResource,
    RecordingDockerClient,
    _has_expected_labels,
    _labels as _existing_labels,
    _manager as _existing_manager,
    _spec_for,
    _wait_for_endpoint,
    _endpoint_headers,
    _wait_for_ready,
)
from control_plane.tests.integration.helpers import (
    _cleanup_owned,
    _create,
    _egress_resource_labels as _labels,
    _egress_resource_manager as _manager,
    _owned_resource,
    _python,
    _resources,
)

pytestmark = [
    pytest.mark.docker_integration,
    pytest.mark.skipif(
        os.getenv("RUN_DOCKER_INTEGRATION") != "1",
        reason="set RUN_DOCKER_INTEGRATION=1 to modify Docker resources",
    ),
    pytest.mark.anyio,
]


def _verify_configuration(client: Any, resources: dict[str, CreatedResource], domains: list[str]) -> None:
    """Inspect real policy metadata, proxy variables, topology and fixed egress limits."""
    assert set(resources) == {"network", "ingress", "workspace", "container", "proxy", "egress"}
    expected_policy = json.dumps(sorted(domains), separators=(",", ":"))
    for record in resources.values():
        actual = _labels(_owned_resource(client, record), record.kind)
        assert actual["io.agent-sandbox.network-version"] == "1"
        assert actual["io.agent-sandbox.allow-domains"] == expected_policy
    sandbox = _owned_resource(client, resources["container"])
    egress = _owned_resource(client, resources["egress"])
    network = _owned_resource(client, resources["network"])
    ingress = _owned_resource(client, resources["ingress"])
    internal_name, ingress_name = network.name, ingress.name
    assert network.attrs["Internal"] is True and ingress.attrs["Internal"] is False
    assert set(sandbox.attrs["NetworkSettings"]["Networks"]) == {internal_name}
    assert set(egress.attrs["NetworkSettings"]["Networks"]) == {internal_name, ingress_name}
    assert "egress" in egress.attrs["NetworkSettings"]["Networks"][internal_name]["Aliases"]
    for name in (internal_name, ingress_name):
        assert egress.attrs["NetworkSettings"]["Networks"][name]["NetworkID"] in {
            resources["network"].resource_id, resources["ingress"].resource_id,
        }
    env = dict(item.split("=", 1) for item in sandbox.attrs["Config"]["Env"])
    for key in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"):
        assert env[key] == "http://egress:18084"
    for key in ("NO_PROXY", "no_proxy"):
        assert env[key] == "localhost,127.0.0.1,::1"
    for role in ("egress", "proxy"):
        component = _owned_resource(client, resources[role])
        actual_env = dict(item.split("=", 1) for item in component.attrs["Config"].get("Env", []))
        assert not any(key in actual_env for key in ("HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy"))
    config, host = egress.attrs["Config"], egress.attrs["HostConfig"]
    assert config["User"] == "10001:10001"
    assert host["NanoCpus"] == 500_000_000
    assert host["Memory"] == host["MemorySwap"] == 128 * 1024 * 1024
    assert host["PidsLimit"] == 32
    assert host["ReadonlyRootfs"] is True and host["CapDrop"] == ["ALL"]
    assert "no-new-privileges:true" in host["SecurityOpt"]
    assert host.get("PortBindings") in (None, {})
    assert egress.attrs["Mounts"] == []
    assert not any(egress.attrs["NetworkSettings"]["Ports"].values())


async def _pendingize(store: SQLiteLifecycleStore, sandbox_id: str) -> None:
    """Replace only this test's committed record with the same original pending identity."""
    record = await store.get(sandbox_id)
    assert record is not None
    credential = await store.get_credential(sandbox_id, record.transaction_id)
    assert credential is not None
    await store.delete(sandbox_id, record.transaction_id, expected_version=record.version)
    await store.create(replace(record, version=0, management_state="pending"), credential=credential)


@pytest.mark.timeout(150)
async def test_egress_lifecycle_restart_partial_pending_and_owned_deletion(tmp_path: Path) -> None:
    """Recover all six ready resources, preserve partial pending and delete legal survivors."""
    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    database = tmp_path / "egress.sqlite3"
    runtime = DockerRuntime(lambda: RecordingDockerClient(client, created))
    app = create_test_app(runtime, database_path=database, reaper_enabled=False, sandbox_query_timeout=5)
    ids = []
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as control:
                for domains in (["EXAMPLE.COM."], ["iana.org"]):
                    sandbox_id = await _create(control, created, domains)
                    ids.append(sandbox_id)
                    resources = _resources(created, sandbox_id)
                    await _wait_for_ready(control, sandbox_id, _owned_resource(client, resources["container"]))
                    await _wait_for_endpoint(control, sandbox_id)
                    _verify_configuration(client, resources, [domains[0].lower().rstrip(".")])
                    if len(ids) == 1:
                        # Endpoint retains its entry-only semantics while readiness fails closed.
                        exit_container = _owned_resource(client, resources["egress"])
                        exit_container.stop(timeout=5)
                        stopped = await control.get(f"/v1/sandboxes/{sandbox_id}")
                        assert stopped.status_code == 200 and response_data(stopped)["reason"] == "egress_stopped"
                        assert response_data(stopped)["status"] == "failed"
                        assert (await control.get(f"/v1/sandboxes/{sandbox_id}/readyz")).status_code == 503
                        assert (await control.get(f"/v1/sandboxes/{sandbox_id}/endpoint")).status_code == 200
                        exit_container.start()
                        await _wait_for_ready(control, sandbox_id, _owned_resource(client, resources["container"]))
                    await _pendingize(app.state.lifecycle_store, sandbox_id)
                # Removing only the exact owned exit component leaves a partial transaction.
                _owned_resource(client, _resources(created, ids[1])["egress"]).remove(force=True)
        restarted = create_test_app(
            DockerRuntime(), database_path=database, reaper_enabled=False,
            pending_recovery_enabled=True, sandbox_query_timeout=5,
        )
        async with restarted.router.lifespan_context(restarted):
            result = restarted.state.pending_recovery_result
            assert result.recovered == 1
            assert (await restarted.state.lifecycle_store.get(ids[0])).management_state == "active"
            assert (await restarted.state.lifecycle_store.get(ids[1])).management_state == "pending"
            inspection = await restarted.state.service.inspect_pending_once()
            assert len(inspection.items) == 1
            assert inspection.items[0].classification == "partial"
            assert "egress" in inspection.items[0].missing_resources
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted), base_url="http://restarted") as control:
                query = await control.get(f"/v1/sandboxes/{ids[1]}")
                assert query.status_code == 200 and response_data(query)["status"] != "ready"
                assert response_data(query)["reason"].startswith("egress_")
                assert (await control.get(f"/v1/sandboxes/{ids[1]}/readyz")).status_code == 503
                assert (await control.get(f"/v1/sandboxes/{ids[0]}/endpoint")).status_code == 200
                for sandbox_id in ids:
                    deleted = await control.delete(f"/v1/sandboxes/{sandbox_id}")
                    assert response_data(deleted) is None
                    assert await restarted.state.lifecycle_store.get(sandbox_id) is None
                    for role, record in _resources(created, sandbox_id).items():
                        if role == "workspace":
                            _owned_resource(client, record)
                        else:
                            with pytest.raises(docker.errors.NotFound):
                                _manager(client, role).get(record.resource_id)
                    assert response_data(await control.delete(f"/v1/sandboxes/{sandbox_id}", params={"delete_workspace": "true"})) is None
    finally:
        try:
            _cleanup_owned(client, created)
        finally:
            client.close()


@pytest.mark.timeout(90)
async def test_egress_expiration_reaps_owned_component_and_preserves_workspace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Advance only the service clock to reap all runtime roles without a TTL sleep."""
    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    app = create_test_app(
        DockerRuntime(lambda: RecordingDockerClient(client, created)),
        database_path=tmp_path / "reap.sqlite3", reaper_enabled=False, sandbox_query_timeout=5,
    )
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as control:
                sandbox_id = await _create(control, created, ["example.com"])
                resources = _resources(created, sandbox_id)
                record = await app.state.lifecycle_store.get(sandbox_id)
                assert record is not None
                monkeypatch.setattr(app.state.service, "_clock", lambda: record.expires_at + timedelta(seconds=1))
                result = await app.state.service.reap_expired_once()
                assert result.succeeded == 1 and result.failed == 0
                assert await app.state.lifecycle_store.get(sandbox_id) is None
                for role, resource in resources.items():
                    if role == "workspace":
                        _owned_resource(client, resource)
                    else:
                        with pytest.raises(docker.errors.NotFound):
                            _manager(client, role).get(resource.resource_id)
    finally:
        try:
            _cleanup_owned(client, created)
        finally:
            client.close()


@pytest.mark.timeout(90)
async def test_disabled_network_does_not_require_egress_image(tmp_path: Path) -> None:
    """Simulate only a missing egress image without removing or retagging user images."""
    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    recording = RecordingDockerClient(client, created)
    images = Mock(wraps=client.images)

    def get_image(name: str) -> Any:
        """Reject only the fixed egress image and delegate other preflight lookups."""
        if name == "agent-sandbox-egress:dev":
            raise docker.errors.ImageNotFound("test-only missing egress image")
        return client.images.get(name)

    images.get.side_effect = get_image
    recording.images = images
    app = create_test_app(
        DockerRuntime(lambda: recording), database_path=tmp_path / "disabled.sqlite3",
        reaper_enabled=False, sandbox_query_timeout=5,
    )
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as control:
                for domains in (None, []):
                    sandbox_id = await _create(control, created, domains)
                    resources = _resources(created, sandbox_id)
                    assert "egress" not in resources and len(resources) == 5
                    for record in resources.values():
                        assert record.labels["io.agent-sandbox.network-version"] == "1"
                        assert record.labels["io.agent-sandbox.allow-domains"] == "[]"
                    env = _owned_resource(client, resources["container"]).attrs["Config"]["Env"]
                    assert not any(item.startswith(("HTTP_PROXY=", "http_proxy=", "HTTPS_PROXY=", "https_proxy=")) for item in env)
                assert all(call.args[0] != "agent-sandbox-egress:dev" for call in images.get.call_args_list)
                count = len(created)
                denied = await control.post("/v1/sandboxes", json={"environment": "python", "network": {"allow_domains": ["example.com"]}})
                assert denied.status_code == 503
                assert_http_error(denied, 503, "sandbox image is unavailable")
                assert len(created) == count
    finally:
        try:
            _cleanup_owned(client, created)
        finally:
            client.close()


def _public_request_code(domain: str, allowed: bool, schemes: tuple[str, ...] = ("http", "https")) -> str:
    """Build explicit proxy requests with system CA checks and no redirect or direct fallback."""
    return f'''
import http.client, json, ssl
proxy = "egress"
results = []
for scheme in {schemes!r}:
    if scheme == "http":
        connection = http.client.HTTPConnection(proxy, 18084, timeout=5)
        path = "http://{domain}/"
    else:
        connection = http.client.HTTPSConnection(proxy, 18084, timeout=5, context=ssl.create_default_context())
        connection.set_tunnel("{domain}", 443)
        path = "/"
    try:
        connection.request("GET", path, headers={{"Host": "{domain}"}})
        response = connection.getresponse()
        data = response.read(4096)
        if {allowed!r}:
            assert response.status < 400, (scheme, response.status)
        else:
            assert response.status == 403 and json.loads(data)["error"]["code"] == "egress_policy_denied"
        results.append([scheme, response.status])
    except OSError as error:
        if {allowed!r} or scheme != "https" or "403" not in str(error):
            raise
        results.append([scheme, 403])
    finally:
        connection.close()
print(json.dumps(results))
'''


async def _assert_direct_denied(agent: httpx.AsyncClient, public_ip: str) -> None:
    """Accept only an executed socket refusal or timeout, never an Agent or Python startup failure."""
    result = await _python(agent, f"""
import errno, json, socket
try:
    connection = socket.create_connection(({public_ip!r}, 443), 3)
except OSError as error:
    assert isinstance(error, TimeoutError) or error.errno in {{
        errno.EACCES, errno.EPERM, errno.ECONNREFUSED, errno.ENETUNREACH,
        errno.EHOSTUNREACH, errno.ETIMEDOUT,
    }}, (type(error).__name__, error.errno)
    print(json.dumps({{"target": {public_ip!r}, "port": 443, "result": "blocked"}}))
else:
    connection.close()
    raise SystemExit("unexpected direct TCP success")
""")
    assert result["exit_code"] == 0, result
    assert json.loads(result["stdout"]) == {"target": public_ip, "port": 443, "result": "blocked"}


def _bridge_control(client: Any, created: list[CreatedResource], public_ip: str) -> None:
    """Prove ordinary bridge TCP reachability using a temporary fully owned Python container."""
    spec = _spec_for(f"sbx_{secrets.token_hex(16)}")
    labels = spec.labels_for("container")
    image = client.images.get("agent-sandbox-python:dev")
    response = client.api.create_container(
        image=image.id, name=spec.container_name, labels=labels,
        user="sandbox", entrypoint=["sleep"], command=["60"],
        host_config=client.api.create_host_config(
            network_mode="bridge", read_only=True, cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"], mem_limit="128m", memswap_limit="128m",
            nano_cpus=500_000_000, pids_limit=32,
        ),
    )
    record = CreatedResource("container", response["Id"], labels)
    created.append(record)
    control = _owned_resource(client, record)
    control.start()
    result = control.exec_run(["python3", "-c", f"import socket; socket.create_connection(({public_ip!r},443),3).close()"])
    assert result.exit_code == 0, "ordinary bridge cannot reach public TCP target; isolation unverified"


@pytest.mark.skipif(
    os.getenv("RUN_EGRESS_PUBLIC_INTEGRATION") != "1",
    reason="public HTTP/HTTPS additionally require RUN_EGRESS_PUBLIC_INTEGRATION=1",
)
@pytest.mark.timeout(180)
async def test_two_egress_policies_public_http_https_direct_denial_and_no_fallback(tmp_path: Path) -> None:
    """Exercise independent allowlists and stopped egress through real Agent HTTP endpoints."""
    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    app = create_test_app(
        DockerRuntime(lambda: RecordingDockerClient(client, created)),
        database_path=tmp_path / "public.sqlite3", reaper_enabled=False, sandbox_query_timeout=5,
    )
    public_ip = os.getenv("AGENT_SANDBOX_EGRESS_IP", "1.1.1.1")
    address = ipaddress.ip_address(public_ip)
    assert isinstance(address, ipaddress.IPv4Address) and address.is_global
    # The control is established before any direct-denial claim is made.
    try:
        await asyncio.to_thread(_bridge_control, client, created, public_ip)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as control:
                for allowed, denied in (("example.com", "iana.org"), ("iana.org", "example.com")):
                    sandbox_id = await _create(control, created, [allowed])
                    resources = _resources(created, sandbox_id)
                    await _wait_for_ready(control, sandbox_id, _owned_resource(client, resources["container"]))
                    endpoint = await _wait_for_endpoint(control, sandbox_id)
                    _verify_configuration(client, resources, [allowed])
                    async with httpx.AsyncClient(base_url=endpoint, headers=_endpoint_headers(endpoint), timeout=25, trust_env=False) as agent:
                        permitted = await _python(agent, _public_request_code(allowed, True))
                        assert permitted["exit_code"] == 0, permitted
                        rejected = await _python(agent, _public_request_code(denied, False))
                        assert rejected["exit_code"] == 0, rejected
                        await _assert_direct_denied(agent, public_ip)
                        _owned_resource(client, resources["egress"]).stop(timeout=5)
                        observed = await control.get(f"/v1/sandboxes/{sandbox_id}")
                        assert observed.status_code == 200 and response_data(observed)["status"] != "ready"
                        assert response_data(observed)["reason"].startswith("egress_")
                        assert (await control.get(f"/v1/sandboxes/{sandbox_id}/readyz")).status_code == 503
                        for scheme in ("http", "https"):
                            assert (await _python(agent, _public_request_code(allowed, True, (scheme,))))["exit_code"] != 0
                        await _assert_direct_denied(agent, public_ip)
    finally:
        try:
            _cleanup_owned(client, created)
        finally:
            client.close()
