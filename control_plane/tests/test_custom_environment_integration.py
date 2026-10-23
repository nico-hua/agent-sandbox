"""Explicit Docker coverage for a trusted prebuilt custom environment's lifecycle."""

import os
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
from test_sandbox_egress_integration import _cleanup_owned, _owned_resource, _resources
from test_sandbox_pause_integration import (
    _assert_paused,
    _assert_runtime_deleted,
    _wait_for_resumed_ready,
)

pytestmark = [
    pytest.mark.docker_integration,
    pytest.mark.skipif(
        os.getenv("RUN_DOCKER_INTEGRATION") != "1",
        reason="set RUN_DOCKER_INTEGRATION=1 to modify Docker resources",
    ),
    pytest.mark.anyio,
]

_IMAGE = "agent-sandbox-tools:dev"
_MARKER = "agent-sandbox custom tools environment v1\n"


def _assert_custom_configuration(
    client: Any, resources: dict[str, CreatedResource], image_id: str,
) -> None:
    """Inspect the selected immutable image and the fixed execution and security contract."""
    container = _owned_resource(client, resources["container"])
    config, host = container.attrs["Config"], container.attrs["HostConfig"]
    assert container.attrs["Image"] == image_id
    assert config["Entrypoint"] == ["/usr/local/bin/agent"]
    assert config["Cmd"] == ["-listen", "0.0.0.0:8080"]
    assert config["User"] == "sandbox" and config["WorkingDir"] == "/workspace"
    health = config["Healthcheck"]
    assert health["Test"] == [
        "CMD", "wget", *(["-Y", "off"] if "egress" in resources else []),
        "-q", "-T", "2", "-O", "/dev/null", "http://127.0.0.1:8080/healthz",
    ]
    assert health["Interval"] == 2_000_000_000 and health["Timeout"] == 3_000_000_000
    assert health["StartPeriod"] == 5_000_000_000 and health["Retries"] == 3
    assert host["NanoCpus"] == 1_000_000_000
    assert host["Memory"] == host["MemorySwap"] == 256 * 1024 * 1024
    assert host["PidsLimit"] == 64 and host["Init"] is True
    assert host["ReadonlyRootfs"] is True and host["CapDrop"] == ["ALL"]
    assert "no-new-privileges:true" in host["SecurityOpt"]
    assert host["Tmpfs"] == {"/tmp": "rw,nosuid,nodev,size=32m,mode=1777"}
    assert host.get("PortBindings") in (None, {})
    assert len(container.attrs["Mounts"]) == 1
    mount = container.attrs["Mounts"][0]
    assert mount["Type"] == "volume" and mount["RW"] is True
    assert mount["Name"] == resources["workspace"].resource_id
    assert mount["Destination"] == "/workspace"
    network = _owned_resource(client, resources["network"])
    assert network.attrs["Internal"] is True
    assert set(container.attrs["NetworkSettings"]["Networks"]) == {network.name}


async def _assert_image_marker_and_workspace(endpoint: str, *, upload: bool) -> None:
    """Read image-only content through Agent commands and round-trip a workspace file."""
    async with httpx.AsyncClient(base_url=endpoint, timeout=10, trust_env=False) as agent:
        command = await agent.post("/v1/commands:run", json={
            "argv": ["cat", "/usr/local/share/agent-sandbox/tools-marker.txt"],
            "timeout_ms": 5000,
        })
        assert command.status_code == 200, command.text
        result = command.json()
        assert result["exit_code"] == 0 and result["stderr"] == "", result
        assert result["stdout"] == _MARKER
        if upload:
            response = await agent.post(
                "/v1/files", params={"path": "custom-retained.bin"},
                content=b"custom-workspace\x00\xff",
            )
            assert response.status_code == 201, response.text
        downloaded = await agent.get("/v1/files", params={"path": "custom-retained.bin"})
        assert downloaded.status_code == 200 and downloaded.content == b"custom-workspace\x00\xff"


@pytest.mark.parametrize("domains", [None, ["example.com"]], ids=["five-roles", "six-roles"])
@pytest.mark.parametrize("termination", ["delete", "expire"])
@pytest.mark.timeout(150)
async def test_custom_environment_survives_configuration_removal_and_lifecycle(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    domains: list[str] | None, termination: str,
) -> None:
    """Create tools over HTTP, remove registration, then freeze/thaw and end the original instance."""
    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    database = tmp_path / "custom.sqlite3"
    try:
        # Images must already exist: this explicit test never pulls or builds.
        image_id = client.images.get(_IMAGE).id
        runtime = DockerSandboxRuntime(
            lambda: RecordingDockerClient(client, created),
            additional_environments={"tools": _IMAGE},
        )
        app = create_app(
            runtime, database_path=database, reaper_enabled=False, sandbox_query_timeout=5,
        )
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://control",
            ) as control:
                body: dict[str, Any] = {"environment": "tools"}
                if domains is not None:
                    body["network"] = {"allow_domains": domains}
                response = await control.post("/v1/sandboxes", json=body)
                assert response.status_code == 201, response.text
                initial = response.json()
                assert set(initial) == {"sandbox_id", "status", "created_at", "expires_at"}
                sandbox_id = initial["sandbox_id"]
                resources = _resources(created, sandbox_id)
                assert len(resources) == (5 if domains is None else 6)
                _assert_custom_configuration(client, resources, image_id)
                await _wait_for_ready(control, sandbox_id, _owned_resource(client, resources["container"]))
                endpoint = await _wait_for_endpoint(control, sandbox_id)
                await _assert_image_marker_and_workspace(endpoint, upload=True)
                original_record = await app.state.lifecycle_store.get(sandbox_id)
                assert original_record is not None
                assert original_record.expires_at - original_record.created_at == timedelta(seconds=1800)

        restarted = create_app(
            DockerSandboxRuntime(lambda: RecordingDockerClient(client, created)),
            database_path=database, reaper_enabled=False, sandbox_query_timeout=5,
        )
        async with restarted.router.lifespan_context(restarted):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=restarted), base_url="http://restarted",
            ) as control:
                observed = await control.get(f"/v1/sandboxes/{sandbox_id}")
                assert observed.status_code == 200 and observed.json()["status"] == "ready"
                assert observed.json()["created_at"] == initial["created_at"]
                assert observed.json()["expires_at"] == initial["expires_at"]
                denied = await control.post("/v1/sandboxes", json={"environment": "tools"})
                assert denied.status_code == 400, denied.text
                assert denied.json() == {"error": {
                    "code": "invalid_request", "message": "invalid sandbox creation options",
                }}
                assert _resources(created, sandbox_id) == resources
                assert len(created) == len(resources)
                assert await restarted.state.lifecycle_store.get(sandbox_id) == original_record
                paused = await control.post(f"/v1/sandboxes/{sandbox_id}/pause")
                assert paused.status_code == 204, paused.text
                _assert_paused(client, resources, True)
                resumed = await control.post(f"/v1/sandboxes/{sandbox_id}/resume")
                assert resumed.status_code == 204, resumed.text
                _assert_paused(client, resources, False)
                await _wait_for_resumed_ready(control, sandbox_id)
                assert await _wait_for_endpoint(control, sandbox_id) == endpoint
                await _assert_image_marker_and_workspace(endpoint, upload=False)
                _assert_custom_configuration(client, resources, image_id)
                assert _resources(created, sandbox_id) == resources
                assert await restarted.state.lifecycle_store.get(sandbox_id) == original_record
                paused = await control.post(f"/v1/sandboxes/{sandbox_id}/pause")
                assert paused.status_code == 204, paused.text
                if termination == "delete":
                    deleted = await control.delete(f"/v1/sandboxes/{sandbox_id}")
                    assert deleted.status_code == 204, deleted.text
                else:
                    monkeypatch.setattr(
                        restarted.state.service, "_clock",
                        lambda: original_record.expires_at + timedelta(seconds=1),
                    )
                    reaped = await restarted.state.service.reap_expired_once()
                    assert reaped.succeeded == 1 and reaped.failed == 0
                assert await restarted.state.lifecycle_store.get(sandbox_id) is None
                _assert_runtime_deleted(client, resources)
                assert (await control.get(f"/v1/sandboxes/{sandbox_id}")).status_code == 404
    finally:
        try:
            _cleanup_owned(client, created)
        finally:
            client.close()
