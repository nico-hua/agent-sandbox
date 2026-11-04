"""Opt-in Docker workspace reuse, restart and exact test-owned cleanup."""

import os
import re
from datetime import timedelta
from pathlib import Path
from typing import Any

import docker
import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.service import SandboxService
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from test_sandbox_docker_integration import CreatedResource, RecordingDockerClient, _wait_for_endpoint, _wait_for_ready, _endpoint_headers

pytestmark = [
    pytest.mark.docker_integration,
    pytest.mark.skipif(os.getenv("RUN_DOCKER_INTEGRATION") != "1", reason="set RUN_DOCKER_INTEGRATION=1 to modify Docker resources"),
    pytest.mark.anyio,
]
_PREFIX = "io.agent-sandbox."


def _owned_resource(client: Any, record: CreatedResource) -> Any:
    """Authorize an exact ledger ID and complete independent or instance ownership."""
    labels = record.labels
    assert labels[_PREFIX + "managed"] == "true"
    assert labels[_PREFIX + "project"] == "agent-sandbox"
    assert labels[_PREFIX + "resource"] == record.kind
    if _PREFIX + "workspace-version" in labels:
        assert labels[_PREFIX + "workspace-version"] == "1"
        workspace_id = labels[_PREFIX + "workspace-id"]
        assert re.fullmatch(r"wsp_[0-9a-f]{32}", workspace_id)
        assert re.fullmatch(r"wct_[0-9a-f]{32}", labels[_PREFIX + "workspace-token"])
        assert labels[_PREFIX + "workspace-image-id"]
        assert record.kind in {"workspace", "workspace-initializer"}
        if record.kind == "workspace":
            assert record.resource_id == "agent-sandbox-" + workspace_id
    else:
        assert re.fullmatch(r"sbx_[0-9a-f]{32}", labels[_PREFIX + "sandbox-id"])
        assert re.fullmatch(r"txn_[0-9a-f]{32}", labels[_PREFIX + "transaction-id"])
    manager = client.volumes if record.kind == "workspace" else (
        client.networks if record.kind in {"network", "ingress"} else client.containers
    )
    resource = manager.get(record.resource_id)
    resource.reload()
    assert resource.id == record.resource_id
    actual = resource.attrs.get("Labels", {}) if record.kind in {"workspace", "network", "ingress"} else resource.attrs["Config"]["Labels"]
    assert all(actual.get(key) == value for key, value in labels.items())
    return resource


def _cleanup_test_resources(client: Any, created: list[CreatedResource]) -> None:
    """Clean only confirmed test-created resources after exact ownership checks."""
    failures = []
    for record in reversed(created):
        try:
            resource = _owned_resource(client, record)
            if record.kind in {"workspace", "network", "ingress"}:
                resource.remove()
            else:
                resource.remove(force=True, v=False)
        except docker.errors.NotFound:
            continue
        except Exception as error:
            failures.append(f"{record.kind}:{type(error).__name__}")
    for record in created:
        try:
            _owned_resource(client, record)
        except docker.errors.NotFound:
            continue
        except Exception as error:
            failures.append(f"{record.kind}:absence-{type(error).__name__}")
        else:
            failures.append(f"{record.kind}:still-present")
    assert not failures, "workspace integration cleanup failed: " + ", ".join(failures)


def _container(client: Any, created: list[CreatedResource], sandbox_id: str) -> Any:
    """Load the exact test-created main container without name-based discovery."""
    records = [record for record in created if record.kind == "container" and record.labels.get(_PREFIX + "sandbox-id") == sandbox_id]
    assert len(records) == 1
    return _owned_resource(client, records[0])


def _assert_workspace_unchanged(client: Any, record: CreatedResource) -> None:
    """Verify the same independent volume and complete immutable ownership labels."""
    volume = _owned_resource(client, record)
    assert volume.attrs["Labels"] == record.labels
    assert _PREFIX + "sandbox-id" not in record.labels
    assert _PREFIX + "transaction-id" not in record.labels
    assert volume.attrs["Driver"] == "local"


def _assert_workspace_mount(container: Any, workspace_name: str) -> None:
    """Inspect the actual reusable volume mount and its explicit no-copy contract."""
    container.reload()
    mounts = container.attrs["Mounts"]
    assert len(mounts) == 1
    assert mounts[0]["Destination"] == "/workspace"
    assert mounts[0]["Name"] == workspace_name
    assert mounts[0]["RW"] is True
    configured = container.attrs["HostConfig"]["Mounts"]
    assert len(configured) == 1
    assert configured[0]["Source"] == workspace_name
    assert configured[0]["Target"] == "/workspace"
    assert configured[0]["Type"] == "volume"
    assert configured[0]["VolumeOptions"]["NoCopy"] is True


def _assert_resources_absent(client: Any, created: list[CreatedResource]) -> None:
    """Confirm recorded exact resources were removed by the tested operation."""
    for record in created:
        with pytest.raises(docker.errors.NotFound):
            _owned_resource(client, record)


async def _assert_agent_file(endpoint: str, expected: bytes) -> None:
    """Read nested binary content using the unchanged public Agent endpoint."""
    async with httpx.AsyncClient(base_url=endpoint, headers=_endpoint_headers(endpoint), timeout=5, trust_env=False) as agent:
        downloaded = await agent.get("/v1/files", params={"path": "retained/workspace.bin"})
        assert downloaded.status_code == 200, downloaded.text
        assert downloaded.content == expected


@pytest.mark.timeout(150)
@pytest.mark.parametrize("domains", [[], ["example.com"]], ids=["deny-egress", "allow-domain"])
@pytest.mark.parametrize("termination", ["delete", "expire"])
async def test_workspace_files_survive_instance_replacement_and_control_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, domains: list[str], termination: str,
) -> None:
    """Real nonroot files survive rollback, exclusive reuse, restart and paused cleanup."""
    client = docker.from_env(timeout=5)
    created: list[CreatedResource] = []
    database = tmp_path / "workspace-integration.sqlite3"
    payload = bytes(range(256)) + b"\x00workspace-reuse\xff"
    edited = b"second-nonroot-writer"

    def runtime() -> DockerSandboxRuntime:
        """Record all confirmed resources across both application instances."""
        return DockerSandboxRuntime(lambda: RecordingDockerClient(client, created))

    try:
        app = create_app(runtime(), allow_unauthenticated=True, database_path=database, sandbox_query_timeout=5, reaper_enabled=False)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control", timeout=10) as control:
                response = await control.post("/v1/workspaces", json={"environment": "basic"})
                assert response.status_code == 201, response.text
                workspace_id = response.json()["workspace_id"]
                volumes = [record for record in created if record.kind == "workspace"]
                assert len(volumes) == 1
                workspace = volumes[0]
                assert any(record.kind == "workspace-initializer" for record in created)
                _assert_workspace_unchanged(client, workspace)
                options = {"workspace_id": workspace_id, "network": {"allow_domains": domains}}
                response = await control.post("/v1/sandboxes", json=options)
                assert response.status_code == 201, response.text
                first_id = response.json()["sandbox_id"]
                first = _container(client, created, first_id)
                await _wait_for_ready(control, first_id, first)
                endpoint = await _wait_for_endpoint(control, first_id)
                _assert_workspace_mount(first, workspace.resource_id)
                async with httpx.AsyncClient(base_url=endpoint, headers=_endpoint_headers(endpoint), timeout=5, trust_env=False) as agent:
                    directory = await agent.post("/v1/commands:run", json={"argv": ["mkdir", "/workspace/retained"]})
                    assert directory.status_code == 200 and directory.json()["exit_code"] == 0, directory.text
                    uploaded = await agent.post("/v1/files", params={"path": "retained/workspace.bin"}, content=payload)
                    assert uploaded.status_code == 201, uploaded.text
                await _assert_agent_file(endpoint, payload)
                protected = await control.delete(f"/v1/sandboxes/{first_id}?delete_workspace=true")
                assert protected.status_code == 409, protected.text
                assert protected.json()["error"]["code"] == "workspace_delete_protected"
                assert (await control.delete(f"/v1/workspaces/{workspace_id}")).status_code == 409
                assert (await control.post("/v1/sandboxes", json=options)).status_code == 409
                first.reload()
                assert first.attrs["State"]["Running"] is True
                _assert_workspace_unchanged(client, workspace)
                first_resources = [record for record in created if record.labels.get(_PREFIX + "sandbox-id") == first_id]
                assert (await control.delete(f"/v1/sandboxes/{first_id}")).status_code == 204
                _assert_resources_absent(client, first_resources)
                assert (await control.get(f"/v1/workspaces/{workspace_id}")).json()["status"] == "available"
                _assert_workspace_unchanged(client, workspace)

                # Fail before Docker creates the proxy, after real networks/main creation.
                original_create = client.api.create_container
                rejected = []

                def reject_proxy_create(**kwargs: Any) -> dict[str, Any]:
                    """Inject a proxy failure while real prior resources roll back."""
                    if kwargs.get("labels", {}).get(_PREFIX + "resource") == "proxy":
                        rejected.append(True)
                        raise docker.errors.APIError("injected workspace integration proxy failure")
                    return original_create(**kwargs)

                before_failure = len(created)
                with monkeypatch.context() as patch:
                    patch.setattr(client.api, "create_container", reject_proxy_create)
                    failure = await control.post("/v1/sandboxes", json=options)
                assert rejected == [True]
                assert failure.status_code == 500, failure.text
                assert failure.json()["error"]["code"] == "sandbox_create_failed"
                failed_resources = created[before_failure:]
                assert {record.kind for record in failed_resources} == {"network", "ingress", "container"}
                _assert_resources_absent(client, failed_resources)
                available = await control.get(f"/v1/workspaces/{workspace_id}")
                assert available.status_code == 200, available.text
                assert available.json()["status"] == "available" and available.json()["sandbox_id"] is None
                _assert_workspace_unchanged(client, workspace)

                response = await control.post("/v1/sandboxes", json=options)
                assert response.status_code == 201, response.text
                second_id = response.json()["sandbox_id"]
                assert second_id != first_id
                second = _container(client, created, second_id)
                assert second.id != first.id
                await _wait_for_ready(control, second_id, second)
                _assert_workspace_mount(second, workspace.resource_id)
                endpoint = await _wait_for_endpoint(control, second_id)
                await _assert_agent_file(endpoint, payload)
                async with httpx.AsyncClient(base_url=endpoint, headers=_endpoint_headers(endpoint), timeout=5, trust_env=False) as agent:
                    rewritten = await agent.post("/v1/commands:run", json={
                        "argv": ["sh", "-c", "printf second-nonroot-writer > /workspace/retained/workspace.bin"],
                    })
                    assert rewritten.status_code == 200 and rewritten.json()["exit_code"] == 0, rewritten.text
                await _assert_agent_file(endpoint, edited)
                _assert_workspace_unchanged(client, workspace)

        # The holder's data plane remains usable throughout Control shutdown/restart.
        await _assert_agent_file(endpoint, edited)
        restarted = create_app(runtime(), allow_unauthenticated=True, database_path=database, sandbox_query_timeout=5, reaper_enabled=False)
        async with restarted.router.lifespan_context(restarted):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted), base_url="http://restarted", timeout=10) as control:
                occupancy = await control.get(f"/v1/workspaces/{workspace_id}")
                assert occupancy.status_code == 200, occupancy.text
                assert occupancy.json()["sandbox_id"] == second_id
                assert occupancy.json()["status"] == "occupied"
                assert (await control.post("/v1/sandboxes", json=options)).status_code == 409
                assert (await control.delete(f"/v1/workspaces/{workspace_id}")).status_code == 409
                assert await _wait_for_endpoint(control, second_id) == endpoint
                await _assert_agent_file(endpoint, edited)
                _assert_workspace_unchanged(client, workspace)
                paused = await control.post(f"/v1/sandboxes/{second_id}/pause")
                assert paused.status_code == 204, paused.text
                second.reload()
                assert second.attrs["State"]["Paused"] is True
                assert (await control.delete(f"/v1/workspaces/{workspace_id}")).status_code == 409
                _assert_workspace_unchanged(client, workspace)
                if termination == "delete":
                    removed = await control.delete(f"/v1/sandboxes/{second_id}")
                    assert removed.status_code == 204, removed.text
                else:
                    record = await restarted.state.lifecycle_store.get(second_id)
                    assert record is not None
                    reaper = SandboxService(runtime(), restarted.state.lifecycle_store, clock=lambda: record.expires_at + timedelta(seconds=1))
                    reaped = await reaper.reap_expired_once()
                    assert reaped.error is None
                    assert [(item.sandbox_id, item.outcome) for item in reaped.items] == [(second_id, "succeeded")], reaped
                second_resources = [record for record in created if record.labels.get(_PREFIX + "sandbox-id") == second_id]
                _assert_resources_absent(client, second_resources)
                available = await control.get(f"/v1/workspaces/{workspace_id}")
                assert available.status_code == 200, available.text
                assert available.json()["status"] == "available" and available.json()["sandbox_id"] is None
                _assert_workspace_unchanged(client, workspace)
                assert (await control.delete(f"/v1/workspaces/{workspace_id}")).status_code == 204
                assert (await control.delete(f"/v1/workspaces/{workspace_id}")).status_code == 204
                assert (await control.get(f"/v1/workspaces/{workspace_id}")).status_code == 404
                with pytest.raises(docker.errors.NotFound):
                    client.volumes.get(workspace.resource_id)
    finally:
        try:
            _cleanup_test_resources(client, created)
        finally:
            client.close()
