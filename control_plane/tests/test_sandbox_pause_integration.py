"""Opt-in Docker freeze/thaw coverage using only this test's exact resource ledger."""

import asyncio
import io
import json
import os
import tarfile
from datetime import timedelta
from pathlib import Path
from typing import Any

import docker
import httpx
import pytest

from control_plane.app import create_app
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from test_sandbox_docker_integration import (
    CreatedResource,
    RecordingDockerClient,
    _wait_for_endpoint,
    _wait_for_ready,
)
from test_sandbox_egress_integration import (
    _cleanup_owned,
    _create,
    _manager,
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

_COUNTER = """
import json, os, secrets, time
token = secrets.token_hex(32)
pid = os.getpid()
started = open('/proc/self/stat').read().split()[21]
counter = 0
while True:
    counter += 1
    with open('/workspace/pause-state.next', 'w') as target:
        json.dump(dict(token=token, pid=pid, started=started, counter=counter), target)
    os.replace('/workspace/pause-state.next', '/workspace/pause-state.json')
    time.sleep(0.02)
"""


class _RecordingContainer:
    """Delegate a real container while recording completed freezer operations by exact ID."""

    def __init__(self, container: Any, operations: list[tuple[str, str]]) -> None:
        """Retain the Docker model and this test's operation trace."""
        self._container = container
        self._operations = operations

    def __getattr__(self, name: str) -> Any:
        """Delegate inspection and unrelated lifecycle methods unchanged."""
        return getattr(self._container, name)

    def pause(self) -> None:
        """Pause the real model and record Docker-confirmed completion."""
        self._container.pause()
        self._operations.append(("pause", self._container.id))

    def unpause(self) -> None:
        """Unpause the real model and record Docker-confirmed completion."""
        self._container.unpause()
        self._operations.append(("unpause", self._container.id))


class _RecordingContainers:
    """Wrap exact-ID model loads without changing Docker discovery behavior."""

    def __init__(self, containers: Any, operations: list[tuple[str, str]]) -> None:
        """Retain the real collection and the freezer trace."""
        self._containers = containers
        self._operations = operations

    def __getattr__(self, name: str) -> Any:
        """Delegate discovery methods to the original real collection."""
        return getattr(self._containers, name)

    def get(self, resource_id: str) -> _RecordingContainer:
        """Wrap the real model selected by Runtime's exact resource ID."""
        return _RecordingContainer(self._containers.get(resource_id), self._operations)


def _recording_client(
    client: Any, created: list[CreatedResource], operations: list[tuple[str, str]],
) -> RecordingDockerClient:
    """Combine the existing creation ledger with an observable freezer operation trace."""
    recording = RecordingDockerClient(client, created)
    recording.containers = _RecordingContainers(client.containers, operations)
    return recording


async def _read_counter(agent: httpx.AsyncClient) -> dict[str, Any]:
    """Wait briefly for the background process and read its snapshot through Agent commands."""
    result = await _python(agent, """
import pathlib, time
path = pathlib.Path('/workspace/pause-state.json')
for attempt in range(100):
    if path.exists():
        print(path.read_text())
        break
    time.sleep(0.02)
else:
    raise RuntimeError('background counter did not start')
""")
    assert result["exit_code"] == 0, result
    return json.loads(result["stdout"])


async def _wait_for_resumed_ready(control: httpx.AsyncClient, sandbox_id: str) -> None:
    """Bound post-thaw readiness while cached health and entry probes recover asynchronously."""
    async with asyncio.timeout(15):
        while True:
            response = await control.get(f"/v1/sandboxes/{sandbox_id}/readyz")
            if response.status_code == 200:
                assert response.json() == {"sandbox_id": sandbox_id, "status": "ready"}
                return
            assert response.status_code == 503, response.text
            assert response.json()["error"]["code"] in {
                "sandbox_not_ready", "sandbox_endpoint_unavailable", "sandbox_agent_unavailable",
            }
            # Docker resumes health probes separately from unpause completion;
            # this test wait must not change the API's immediate 204 contract.
            await asyncio.sleep(0.1)


def _frozen_counter(client: Any, record: CreatedResource) -> dict[str, Any]:
    """Read a paused container's file through Docker archive without executing a process."""
    container = _owned_resource(client, record)
    chunks, _ = container.get_archive("/workspace/pause-state.json")
    with tarfile.open(fileobj=io.BytesIO(b"".join(chunks))) as archive:
        source = archive.extractfile(archive.getmembers()[0])
        assert source is not None
        return json.load(source)


def _assert_paused(client: Any, resources: dict[str, CreatedResource], paused: bool) -> None:
    """Confirm the actual Docker freezer state while the entry proxy keeps running."""
    for role in ("container", "egress"):
        if role in resources:
            state = _owned_resource(client, resources[role]).attrs["State"]
            assert state["Paused"] is paused, (role, state)
            assert state["Running"] is True, (role, state)
    proxy = _owned_resource(client, resources["proxy"])
    assert proxy.attrs["State"]["Running"] is True
    assert proxy.attrs["State"]["Paused"] is False


def _assert_runtime_deleted(client: Any, resources: dict[str, CreatedResource]) -> None:
    """Require every exact runtime ID to disappear and the original workspace to survive."""
    for role, record in resources.items():
        if role == "workspace":
            _owned_resource(client, record)
        else:
            with pytest.raises(docker.errors.NotFound):
                _manager(client, role).get(record.resource_id)


async def _assert_paused_endpoint(
    control: httpx.AsyncClient, sandbox_id: str, client: Any,
    resources: dict[str, CreatedResource], endpoint: str,
) -> None:
    """Require the original published port while allowing proxy health to expire during freeze."""
    proxy = _owned_resource(client, resources["proxy"])
    bindings = proxy.attrs["NetworkSettings"]["Ports"]["8080/tcp"]
    assert len(bindings) == 1 and bindings[0]["HostIp"] == "127.0.0.1"
    assert endpoint == f"http://127.0.0.1:{bindings[0]['HostPort']}"
    response = await control.get(f"/v1/sandboxes/{sandbox_id}/endpoint")
    if response.status_code == 200:
        assert response.json()["url"] == endpoint
    else:
        assert response.status_code == 503, response.text
        assert response.json()["error"]["code"] == "sandbox_endpoint_unavailable"


@pytest.mark.parametrize("domains", [None, ["example.com"]], ids=["five-roles", "six-roles"])
@pytest.mark.timeout(150)
async def test_pause_restart_resume_retains_memory_process_endpoint_and_workspace(
    tmp_path: Path, domains: list[str] | None,
) -> None:
    """Freeze a live in-memory task, rebuild Control, and resume the same process and resources."""
    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    operations: list[tuple[str, str]] = []
    database = tmp_path / "pause.sqlite3"
    app = create_app(
        DockerSandboxRuntime(lambda: _recording_client(client, created, operations)),
        database_path=database, reaper_enabled=False, sandbox_query_timeout=5,
    )
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as control:
                sandbox_id = await _create(control, created, domains)
                resources = _resources(created, sandbox_id)
                assert len(resources) == (5 if domains is None else 6)
                container = _owned_resource(client, resources["container"])
                await _wait_for_ready(control, sandbox_id, container)
                endpoint = await _wait_for_endpoint(control, sandbox_id)
                original_record = await app.state.lifecycle_store.get(sandbox_id)
                labels = {role: dict(record.labels) for role, record in resources.items()}
                async with httpx.AsyncClient(base_url=endpoint, timeout=25, trust_env=False) as agent:
                    uploaded = await agent.post("/v1/files", params={"path": "retained.bin"}, content=b"pause\x00retained")
                    assert uploaded.status_code == 201, uploaded.text
                    task = await agent.post("/v1/commands", json={
                        "argv": ["python3", "-c", _COUNTER], "timeout_ms": 30000,
                    })
                    assert task.status_code == 202, task.text
                    command_id = task.json()["command_id"]
                    before = await _read_counter(agent)
                # Keep this sequence short: command deadlines are wall-clock bounds,
                # and pause does not promise to suspend timeout or retention clocks.
                paused = await control.post(f"/v1/sandboxes/{sandbox_id}/pause")
                assert paused.status_code == 204 and paused.content == b"", paused.text
                _assert_paused(client, resources, True)
                observed = await control.get(f"/v1/sandboxes/{sandbox_id}")
                assert observed.status_code == 200 and observed.json()["status"] == "paused"
                assert observed.json()["expires_at"] is not None
                assert await app.state.lifecycle_store.get(sandbox_id) == original_record
                await _assert_paused_endpoint(control, sandbox_id, client, resources, endpoint)
                readiness = await control.get(f"/v1/sandboxes/{sandbox_id}/readyz")
                assert readiness.status_code == 503
                assert readiness.json()["error"]["code"] == "sandbox_not_ready"
                frozen = await asyncio.to_thread(_frozen_counter, client, resources["container"])
                await asyncio.sleep(0.15)
                assert await asyncio.to_thread(_frozen_counter, client, resources["container"]) == frozen
        restarted = create_app(
            DockerSandboxRuntime(lambda: _recording_client(client, created, operations)), database_path=database,
            reaper_enabled=False, sandbox_query_timeout=5,
        )
        async with restarted.router.lifespan_context(restarted):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted), base_url="http://restarted") as control:
                restored = await control.get(f"/v1/sandboxes/{sandbox_id}")
                assert restored.status_code == 200 and restored.json() == observed.json()
                assert await restarted.state.lifecycle_store.get(sandbox_id) == original_record
                await _assert_paused_endpoint(control, sandbox_id, client, resources, endpoint)
                resumed = await control.post(f"/v1/sandboxes/{sandbox_id}/resume")
                assert resumed.status_code == 204 and resumed.content == b"", resumed.text
                _assert_paused(client, resources, False)
                await _wait_for_resumed_ready(control, sandbox_id)
                async with httpx.AsyncClient(base_url=endpoint, timeout=25, trust_env=False) as agent:
                    async with asyncio.timeout(5):
                        while True:
                            after = await _read_counter(agent)
                            if after["counter"] > frozen["counter"]:
                                break
                            await asyncio.sleep(0.05)
                    for key in ("token", "pid", "started"):
                        assert before[key] == frozen[key] == after[key]
                    task_state = await agent.get(f"/v1/commands/{command_id}")
                    assert task_state.status_code == 200 and task_state.json()["status"] == "running"
                    downloaded = await agent.get("/v1/files", params={"path": "retained.bin"})
                    assert downloaded.status_code == 200 and downloaded.content == b"pause\x00retained"
                    assert (await _python(agent, "print('resumed')"))["stdout"] == "resumed\n"
                assert await _wait_for_endpoint(control, sandbox_id) == endpoint
                assert _resources(created, sandbox_id) == resources
                roles = ["container", "egress"] if domains else ["container"]
                assert operations == [
                    ("pause", resources[role].resource_id) for role in roles
                ] + [
                    ("unpause", resources[role].resource_id) for role in reversed(roles)
                ]
                for role, record in resources.items():
                    actual = _owned_resource(client, record)
                    actual_labels = actual.attrs["Config"]["Labels"] if role in {"container", "proxy", "egress"} else actual.attrs["Labels"]
                    assert all(actual_labels.get(key) == value for key, value in labels[role].items())
    finally:
        try:
            _cleanup_owned(client, created)
        finally:
            client.close()


@pytest.mark.parametrize("domains", [None, ["example.com"]], ids=["five-roles", "six-roles"])
@pytest.mark.parametrize("termination", ["delete", "expire"])
@pytest.mark.timeout(120)
async def test_paused_sandbox_deletion_and_expiration_preserve_owned_workspace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    domains: list[str] | None, termination: str,
) -> None:
    """End frozen five/six-role instances through manual delete or a controlled expiry clock."""
    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    app = create_app(
        DockerSandboxRuntime(lambda: RecordingDockerClient(client, created)),
        database_path=tmp_path / "termination.sqlite3", reaper_enabled=False, sandbox_query_timeout=5,
    )
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as control:
                sandbox_id = await _create(control, created, domains)
                resources = _resources(created, sandbox_id)
                await _wait_for_ready(control, sandbox_id, _owned_resource(client, resources["container"]))
                paused = await control.post(f"/v1/sandboxes/{sandbox_id}/pause")
                assert paused.status_code == 204, paused.text
                _assert_paused(client, resources, True)
                renewed = await control.post(f"/v1/sandboxes/{sandbox_id}/renew", json={"ttl_seconds": 3600})
                assert renewed.status_code == 200, renewed.text
                _assert_paused(client, resources, True)
                if termination == "delete":
                    deleted = await control.delete(f"/v1/sandboxes/{sandbox_id}")
                    assert deleted.status_code == 204, deleted.text
                else:
                    record = await app.state.lifecycle_store.get(sandbox_id)
                    assert record is not None
                    monkeypatch.setattr(app.state.service, "_clock", lambda: record.expires_at + timedelta(seconds=1))
                    denied = await control.post(f"/v1/sandboxes/{sandbox_id}/resume")
                    assert denied.status_code == 409, denied.text
                    assert denied.json()["error"]["code"] == "sandbox_resume_conflict"
                    _assert_paused(client, resources, True)
                    reaped = await app.state.service.reap_expired_once()
                    assert reaped.succeeded == 1 and reaped.failed == 0
                assert await app.state.lifecycle_store.get(sandbox_id) is None
                _assert_runtime_deleted(client, resources)
                assert (await control.get(f"/v1/sandboxes/{sandbox_id}")).status_code == 404
    finally:
        try:
            _cleanup_owned(client, created)
        finally:
            client.close()
