"""Opt-in integration coverage for real Docker sandbox lifecycle operations."""

import asyncio
import os
import base64
import json
import re
import time
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import docker
import httpx
import pytest

from control_plane.app import create_app
import control_plane.app as app_module
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
import control_plane.core.service as service_module
from control_plane.core.models import ResourceKind, SandboxSpec
from control_plane.core.service import SandboxService
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore


@dataclass(frozen=True)
class CreatedResource:
    """Record one exact resource ID and its expected ownership labels."""

    kind: ResourceKind
    resource_id: str
    labels: dict[str, str]


def _spec_for(sandbox_id: str) -> SandboxSpec:
    """Build the names that SandboxService derives from one ID."""
    return SandboxSpec(
        sandbox_id=sandbox_id,
        container_name=f"agent-sandbox-{sandbox_id}",
        network_name=f"agent-sandbox-{sandbox_id}-internal",
        volume_name=f"agent-sandbox-{sandbox_id}-workspace",
    )


def _labels(resource: Any, kind: ResourceKind) -> dict[str, str]:
    """Read labels from the Docker inspect shape for one resource kind."""
    if kind in {"container", "proxy"}:
        value = resource.attrs.get("Config", {}).get("Labels", {})
    else:
        value = resource.attrs.get("Labels", {})
    return value if isinstance(value, dict) else {}


def _manager(client: Any, kind: ResourceKind) -> Any:
    """Return the Docker collection that loads a resource by exact ID."""
    if kind in {"container", "proxy"}:
        return client.containers
    if kind == "workspace":
        return client.volumes
    return client.networks


def _has_expected_labels(actual: dict[str, str], expected: dict[str, str]) -> bool:
    """Report whether all expected ownership labels are present and equal."""
    return all(actual.get(key) == value for key, value in expected.items())


def _remove_owned_resources(client: Any, resources: list[CreatedResource]) -> None:
    """Remove recorded resources only after exact ID and label verification."""
    errors: list[str] = []
    for record in resources:
        try:
            sandbox_id = record.labels.get("io.agent-sandbox.sandbox-id", "")
            transaction_id = record.labels.get("io.agent-sandbox.transaction-id", "")
            if (
                record.labels.get("io.agent-sandbox.managed") != "true"
                or record.labels.get("io.agent-sandbox.project") != "agent-sandbox"
                or record.labels.get("io.agent-sandbox.resource") != record.kind
                or re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None
                or re.fullmatch(r"txn_[0-9a-f]{32}", transaction_id) is None
            ):
                errors.append(f"{record.kind} ledger has incomplete ownership labels")
                continue
            resource = _manager(client, record.kind).get(record.resource_id)
            resource.reload()
            if resource.id != record.resource_id:
                errors.append(f"{record.kind} ID changed")
                continue
            actual_labels = _labels(resource, record.kind)
            if not _has_expected_labels(actual_labels, record.labels):
                errors.append(f"{record.kind} labels changed")
                continue
            if record.kind in {"container", "proxy"}:
                resource.remove(force=True)
            else:
                resource.remove()
        except docker.errors.NotFound:
            # Successful DELETE operations already removed some ledger entries.
            continue
        except Exception as error:  # pragma: no cover - real daemon diagnostic
            errors.append(f"{record.kind}: {type(error).__name__}")
    if errors:
        pytest.fail("integration cleanup failed: " + ", ".join(errors))


class RecordingAPIProxy:
    """Record confirmed low-level create IDs before diagnostic inspection."""

    def __init__(self, api: Any, created: list[CreatedResource]) -> None:
        """Store the real API and transaction-owned resource ledger."""
        self._api = api
        self._created = created

    def __getattr__(self, name: str) -> Any:
        """Delegate non-create Docker API operations unchanged."""
        return getattr(self._api, name)

    def create_network(self, name: str, **kwargs: Any) -> dict[str, Any]:
        """Record the network ID immediately after Docker confirms creation."""
        response = self._api.create_network(name, **kwargs)
        self._created.append(
            CreatedResource(kwargs["labels"]["io.agent-sandbox.resource"], response["Id"], dict(kwargs["labels"]))
        )
        return response

    def create_volume(self, name: str, **kwargs: Any) -> dict[str, Any]:
        """Record a volume only when Docker returns this transaction's labels."""
        response = self._api.create_volume(name, **kwargs)
        expected = dict(kwargs["labels"])
        actual = response.get("Labels")
        if isinstance(actual, dict) and _has_expected_labels(actual, expected):
            self._created.append(
                CreatedResource("workspace", response["Name"], expected)
            )
        return response

    def create_container(self, **kwargs: Any) -> dict[str, Any]:
        """Record the container ID immediately after Docker confirms creation."""
        response = self._api.create_container(**kwargs)
        self._created.append(
            CreatedResource(kwargs["labels"]["io.agent-sandbox.resource"], response["Id"], dict(kwargs["labels"]))
        )
        return response


class RecordingDockerClient:
    """Wrap a real client while retaining its transport for final cleanup."""

    def __init__(self, client: Any, created: list[CreatedResource]) -> None:
        """Expose real managers and a create-recording API proxy."""
        self.images = client.images
        self.networks = client.networks
        self.volumes = client.volumes
        self.containers = client.containers
        self.api = RecordingAPIProxy(client.api, created)

    def close(self) -> None:
        """Leave the shared real client open for inspection and cleanup."""


class FakeCreateAPI:
    """Return fixed successful low-level create responses without Docker."""

    def create_network(self, name: str, **kwargs: Any) -> dict[str, str]:
        """Return a fixed network ID."""
        return {"Id": "network-id"}

    def create_volume(self, name: str, **kwargs: Any) -> dict[str, Any]:
        """Return the requested volume name and labels."""
        return {"Name": name, "Labels": kwargs["labels"]}

    def create_container(self, **kwargs: Any) -> dict[str, str]:
        """Return a fixed container ID."""
        return {"Id": "container-id"}


def test_recording_proxy_captures_ids_before_any_model_inspection() -> None:
    """Integration cleanup IDs must exist before diagnostic inspection."""
    created: list[CreatedResource] = []
    proxy = RecordingAPIProxy(FakeCreateAPI(), created)
    labels = {
        "io.agent-sandbox.resource": "network",
        "io.agent-sandbox.managed": "true",
        "io.agent-sandbox.transaction-id": "txn_test",
    }

    proxy.create_network("network", labels=labels)
    proxy.create_volume("volume", labels=labels)
    proxy.create_container(labels={**labels, "io.agent-sandbox.resource": "container"})

    assert [(record.kind, record.resource_id) for record in created] == [
        ("network", "network-id"),
        ("workspace", "volume"),
        ("container", "container-id"),
    ]


def test_integration_cleanup_accepts_previously_deleted_exact_resource() -> None:
    """Final cleanup must accept resources already removed by successful DELETE."""
    client = Mock()
    client.containers.get.side_effect = docker.errors.NotFound("already deleted")
    record = CreatedResource(
        "container", "exact-test-container-id",
        {
            "io.agent-sandbox.managed": "true",
            "io.agent-sandbox.project": "agent-sandbox",
            "io.agent-sandbox.sandbox-id": "sbx_" + "1" * 32,
            "io.agent-sandbox.transaction-id": "txn_" + "2" * 32,
            "io.agent-sandbox.resource": "container",
        },
    )

    _remove_owned_resources(client, [record])

    client.containers.get.assert_called_once_with("exact-test-container-id")


@pytest.mark.parametrize(
    ("label", "value"),
    [
        ("io.agent-sandbox.managed", "false"),
        ("io.agent-sandbox.project", "other-project"),
        ("io.agent-sandbox.sandbox-id", ""),
        ("io.agent-sandbox.transaction-id", "txn_invalid"),
        ("io.agent-sandbox.resource", "proxy"),
    ],
)
def test_integration_cleanup_refuses_incomplete_ledger(
    label: str, value: str
) -> None:
    """An incomplete ledger must never authorize a Docker resource lookup or removal."""
    client = Mock()
    labels = {
        "io.agent-sandbox.managed": "true",
        "io.agent-sandbox.project": "agent-sandbox",
        "io.agent-sandbox.sandbox-id": "sbx_" + "1" * 32,
        "io.agent-sandbox.transaction-id": "txn_" + "2" * 32,
        "io.agent-sandbox.resource": "container",
    }
    labels[label] = value

    with pytest.raises(pytest.fail.Exception, match="incomplete ownership labels"):
        _remove_owned_resources(
            client, [CreatedResource("container", "exact-test-container-id", labels)]
        )

    client.containers.get.assert_not_called()


async def _wait_for_ready(
    http_client: httpx.AsyncClient,
    sandbox_id: str,
    container: Any,
) -> dict[str, Any]:
    """Poll the real query endpoint with a total deadline and bounded diagnostics."""
    last_response: dict[str, Any] = {}
    try:
        async with asyncio.timeout(30):
            while True:
                response = await http_client.get(f"/v1/sandboxes/{sandbox_id}")
                assert response.status_code == 200, response.text[:500]
                last_response = response.json()
                if last_response["status"] == "ready":
                    return last_response
                if last_response["status"] in {"failed", "stopped"}:
                    break
                await asyncio.sleep(0.25)
    except TimeoutError:
        pass

    await asyncio.to_thread(container.reload)
    state = container.attrs["State"]
    health = state.get("Health", {})
    exit_codes = [entry.get("ExitCode") for entry in health.get("Log", [])[-3:]]
    pytest.fail(
        f"sandbox did not become ready within 30 seconds: {last_response}; "
        f"container_status={state.get('Status')}, "
        f"health_status={health.get('Status')}, "
        f"failing_streak={health.get('FailingStreak')}, "
        f"recent_health_exit_codes={exit_codes}"
    )


@pytest.mark.docker_integration
@pytest.mark.skipif(
    os.getenv("RUN_DOCKER_INTEGRATION") != "1",
    reason="set RUN_DOCKER_INTEGRATION=1 to modify Docker resources",
)
@pytest.mark.anyio
@pytest.mark.timeout(90)
async def test_real_docker_creation_uses_fixed_isolated_configuration(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Persist initial TTL labels and keep an expired instance usable after restart."""
    sandbox_id = f"sbx_{secrets.token_hex(16)}"
    transaction_id = f"txn_{secrets.token_hex(16)}"
    created_at = datetime(2000, 1, 1, 8, tzinfo=UTC)
    expires_at = created_at + timedelta(seconds=1800)
    expected_times = {
        "created_at": "2000-01-01T08:00:00Z",
        "expires_at": "2000-01-01T08:30:00Z",
    }
    clock = Mock(return_value=created_at)
    monkeypatch.setattr(
        app_module, "SandboxService",
        lambda backend, store: SandboxService(backend, store, clock=clock),
    )
    spec = _spec_for(sandbox_id)
    spec = SandboxSpec(
        sandbox_id=spec.sandbox_id,
        container_name=spec.container_name,
        network_name=spec.network_name,
        volume_name=spec.volume_name,
        transaction_id=transaction_id,
        created_at=created_at,
        expires_at=expires_at,
    )
    database_path = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database_path)
    await store.initialize()
    client = docker.from_env(timeout=5.0)
    created: list[CreatedResource] = []
    recording_client = RecordingDockerClient(client, created)
    runtime = DockerSandboxRuntime(lambda: recording_client)
    monkeypatch.setattr(service_module, "_new_sandbox_id", lambda: sandbox_id)
    monkeypatch.setattr(service_module, "_new_transaction_id", lambda: transaction_id)

    try:
        transport = httpx.ASGITransport(app=create_app(runtime, store=store))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://control"
        ) as http_client:
            response = await http_client.post("/v1/sandboxes")
        assert response.status_code == 201
        assert response.json() == {
            "sandbox_id": sandbox_id, "status": "started", **expected_times
        }
        clock.assert_called_once_with()
        persisted = await store.get(sandbox_id)
        assert persisted is not None
        assert persisted.transaction_id == transaction_id
        assert persisted.created_at == created_at
        assert persisted.expires_at == expires_at
        assert persisted.management_state == "active"
        assert persisted.version == 1

        resources = {record.kind: record for record in created}
        assert set(resources) == {
            "network", "ingress", "workspace", "container", "proxy"
        }
        for kind, record in resources.items():
            resource = _manager(client, kind).get(record.resource_id)
            resource.reload()
            assert resource.id == record.resource_id
            actual_labels = _labels(resource, kind)
            assert _has_expected_labels(actual_labels, spec.labels_for(kind))
            assert actual_labels["io.agent-sandbox.created-at"] == expected_times["created_at"]
            assert actual_labels["io.agent-sandbox.expires-at"] == expected_times["expires_at"]
        container = client.containers.get(resources["container"].resource_id)
        volume = client.volumes.get(resources["workspace"].resource_id)
        network = client.networks.get(resources["network"].resource_id)
        container.reload()
        volume.reload()
        network.reload()

        assert _has_expected_labels(
            _labels(container, "container"), spec.labels_for("container")
        )
        assert _has_expected_labels(
            _labels(volume, "workspace"), spec.labels_for("workspace")
        )
        assert _has_expected_labels(
            _labels(network, "network"), spec.labels_for("network")
        )
        assert network.attrs["Driver"] == "bridge"
        assert network.attrs["Internal"] is True

        config = container.attrs["Config"]
        host_config = container.attrs["HostConfig"]
        assert config["User"] == "sandbox"
        assert config["WorkingDir"] == "/workspace"
        healthcheck = config.get("Healthcheck")
        assert healthcheck is not None, "sandbox image has no HEALTHCHECK"
        assert healthcheck["Test"] == [
            "CMD", "wget", "-q", "-T", "2", "-O", "/dev/null",
            "http://127.0.0.1:8080/healthz",
        ]
        assert 0 < healthcheck["Timeout"] <= 3_000_000_000
        assert 0 < healthcheck["Interval"] <= 5_000_000_000
        assert 0 < healthcheck["Retries"] <= 10
        assert host_config.get("PortBindings") in (None, {})
        assert set(container.attrs["NetworkSettings"]["Networks"]) == {
            spec.network_name
        }
        assert host_config["NanoCpus"] == 1_000_000_000
        assert host_config["Memory"] == 256 * 1024 * 1024
        assert host_config["MemorySwap"] == 256 * 1024 * 1024
        assert host_config["PidsLimit"] == 64
        assert host_config["ReadonlyRootfs"] is True
        assert host_config["CapDrop"] == ["ALL"]
        assert "no-new-privileges:true" in host_config["SecurityOpt"]
        assert host_config["Tmpfs"] == {
            "/tmp": "rw,nosuid,nodev,size=32m,mode=1777"
        }
        assert any(
            mount["Type"] == "volume"
            and mount["Name"] == spec.volume_name
            and mount["Destination"] == "/workspace"
            and mount["RW"] is True
            for mount in container.attrs["Mounts"]
        )

        async with httpx.AsyncClient(
            transport=transport, base_url="http://control"
        ) as http_client:
            observed = await _wait_for_ready(http_client, sandbox_id, container)
        assert observed == {
            "sandbox_id": sandbox_id,
            "status": "ready",
            "reason": "health_check_passed",
            "message": "Agent health check passed",
            **expected_times,
        }
        clock.assert_called_once_with()
        async with httpx.AsyncClient(
            transport=transport, base_url="http://control"
        ) as http_client:
            original_endpoint = await _wait_for_endpoint(http_client, sandbox_id)

        # Fresh runtime and Store instances must recover SQLite metadata without
        # consulting a clock or sliding the initial TTL.
        restarted_clock = Mock(side_effect=AssertionError("query must not read the clock"))
        monkeypatch.setattr(
            app_module, "SandboxService",
            lambda backend, store: SandboxService(backend, store, clock=restarted_clock),
        )
        restarted_store = SQLiteLifecycleStore(database_path)
        await restarted_store.initialize()
        restarted = create_app(
            DockerSandboxRuntime(), sandbox_query_timeout=5, store=restarted_store,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://restarted"
        ) as http_client:
            restored = await http_client.get(f"/v1/sandboxes/{sandbox_id}")
            assert restored.status_code == 200, restored.text
            assert restored.json() == observed
            assert await _wait_for_endpoint(http_client, sandbox_id) == original_endpoint
            async with httpx.AsyncClient(
                base_url=original_endpoint, timeout=5, trust_env=False
            ) as agent:
                health = await agent.get("/healthz")
                assert health.status_code == 200
                command = await agent.post(
                    "/v1/commands:run", json={"argv": ["printf", "expired-still-running"]}
                )
                assert command.status_code == 200
                assert command.json()["exit_code"] == 0
                assert command.json()["stdout"] == "expired-still-running"
            after_command = await http_client.get(f"/v1/sandboxes/{sandbox_id}")
            assert after_command.status_code == 200
            assert after_command.json() == observed
            restored_record = await restarted_store.get(sandbox_id)
            assert restored_record == persisted
            updated = await restarted_store.update_expiration(
                sandbox_id, transaction_id, expected_version=persisted.version,
                expires_at=expires_at + timedelta(minutes=15),
            )
            assert updated.version == persisted.version + 1
            current = await http_client.get(f"/v1/sandboxes/{sandbox_id}")
            assert current.status_code == 200, current.text
            assert current.json() == {**observed, "expires_at": "2000-01-01T08:45:00Z"}
            for kind, resource_record in resources.items():
                resource = _manager(client, kind).get(resource_record.resource_id)
                resource.reload()
                assert _labels(resource, kind)["io.agent-sandbox.expires-at"] == expected_times["expires_at"]
        restarted_clock.assert_not_called()
        container.reload()
        assert container.attrs["State"]["Health"]["Status"] == "healthy"
    finally:
        try:
            _remove_owned_resources(client, list(reversed(created)))
        finally:
            client.close()


def _assert_recorded_resources_absent(
    client: Any, resources: list[CreatedResource]
) -> None:
    """Verify deletion by the precise IDs confirmed during this test's creation."""
    for record in resources:
        with pytest.raises(docker.errors.NotFound):
            _manager(client, record.kind).get(record.resource_id)


def _read_retained_workspace(
    client: Any,
    created: list[CreatedResource],
    workspace: CreatedResource,
) -> bytes:
    """Read preserved data through one recorded, isolated, read-only volume mount."""
    volume = client.volumes.get(workspace.resource_id)
    volume.reload()
    assert volume.id == workspace.resource_id
    assert _has_expected_labels(_labels(volume, "workspace"), workspace.labels)
    helper_spec = _spec_for(f"sbx_{secrets.token_hex(16)}")
    labels = helper_spec.labels_for("container")
    response = client.api.create_container(
        image="agent-sandbox:dev",
        name=helper_spec.container_name,
        labels=labels,
        user="sandbox",
        entrypoint=["base64"],
        command=["/workspace/identity.bin"],
        volumes=["/workspace"],
        healthcheck={"test": ["NONE"]},
        host_config=client.api.create_host_config(
            network_mode="none",
            read_only=True,
            cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"],
            mem_limit="64m",
            memswap_limit="64m",
            nano_cpus=500_000_000,
            pids_limit=16,
            binds={workspace.resource_id: {"bind": "/workspace", "mode": "ro"}},
        ),
    )
    record = CreatedResource("container", response["Id"], labels)
    created.append(record)
    try:
        helper = client.containers.get(record.resource_id)
        helper.start()
        assert helper.wait(timeout=10)["StatusCode"] == 0
        # Docker logging drivers may replace invalid UTF-8. Transfer ASCII so
        # this assertion checks the original bytes, including the 0xff marker.
        return base64.b64decode(helper.logs(stdout=True, stderr=False))
    finally:
        # An exited container still retains its volume reference until removal.
        _remove_owned_resources(client, [record])


@pytest.mark.docker_integration
@pytest.mark.skipif(
    os.getenv("RUN_DOCKER_INTEGRATION") != "1",
    reason="explicit Docker opt-in required",
)
@pytest.mark.anyio
@pytest.mark.timeout(150)
async def test_delete_after_restart_preserves_workspace_and_other_sandbox(
    record_property: Any,
    tmp_path: Path,
) -> None:
    """Delete A from fresh runtime, retain then delete its data, and keep B usable."""
    database_path = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database_path)
    await store.initialize()
    client = docker.from_env(timeout=5.0)
    created: list[CreatedResource] = []
    recording_client = RecordingDockerClient(client, created)
    sandbox_ids: list[str] = []
    resources_by_id: dict[str, list[CreatedResource]] = {}
    results: list[dict[str, Any]] = []
    try:
        application = create_app(
            DockerSandboxRuntime(lambda: recording_client), sandbox_query_timeout=5,
            store=store,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://control"
        ) as control:
            for _ in range(2):
                response = await control.post("/v1/sandboxes")
                assert response.status_code == 201, response.text
                sandbox_id = response.json()["sandbox_id"]
                sandbox_ids.append(sandbox_id)
                assert await store.get(sandbox_id) is not None
                records = [
                    record for record in created
                    if record.labels["io.agent-sandbox.sandbox-id"] == sandbox_id
                ]
                resources_by_id[sandbox_id] = records
                resources = {record.kind: record for record in records}
                assert set(resources) == {
                    "network", "ingress", "workspace", "container", "proxy"
                }
                await _wait_for_ready(
                    control, sandbox_id,
                    client.containers.get(resources["container"].resource_id),
                )
                results.append(
                    await _exercise_agent(
                        await _wait_for_endpoint(control, sandbox_id), sandbox_id
                    )
                )

        sandbox_a, sandbox_b = sandbox_ids
        records_a = resources_by_id[sandbox_a]
        workspace_a = next(record for record in records_a if record.kind == "workspace")
        runtime_records_a = [record for record in records_a if record.kind != "workspace"]
        # This app uses fresh SDK clients and cannot access the creation ledger.
        restarted_store = SQLiteLifecycleStore(database_path)
        await restarted_store.initialize()
        restarted = create_app(
            DockerSandboxRuntime(), sandbox_query_timeout=5, store=restarted_store,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=restarted), base_url="http://restarted"
        ) as control:
            deleted = await control.delete(f"/v1/sandboxes/{sandbox_a}")
            assert deleted.status_code == 204, deleted.text
            assert deleted.content == b""
            assert await restarted_store.get(sandbox_a) is None
            assert (await control.get(f"/v1/sandboxes/{sandbox_a}")).status_code == 404
            assert (await control.get(f"/v1/sandboxes/{sandbox_a}/endpoint")).status_code == 404
            async with httpx.AsyncClient(timeout=3, trust_env=False) as old_entry:
                with pytest.raises(httpx.RequestError):
                    await old_entry.get(f"{results[0]['url']}/healthz")
            await asyncio.to_thread(_assert_recorded_resources_absent, client, runtime_records_a)
            assert await asyncio.to_thread(
                _read_retained_workspace, client, created, workspace_a
            ) == sandbox_a.encode() + b"\x00\xff"

            repeated = await control.delete(f"/v1/sandboxes/{sandbox_a}")
            assert repeated.status_code == 204 and repeated.content == b""
            retained = client.volumes.get(workspace_a.resource_id)
            assert _has_expected_labels(_labels(retained, "workspace"), workspace_a.labels)

            observed_b = await control.get(f"/v1/sandboxes/{sandbox_b}")
            assert observed_b.status_code == 200 and observed_b.json()["status"] == "ready"
            endpoint_b = await control.get(f"/v1/sandboxes/{sandbox_b}/endpoint")
            assert endpoint_b.status_code == 200
            assert endpoint_b.json()["url"] == results[1]["url"]
            async with httpx.AsyncClient(
                base_url=results[1]["url"], timeout=5, trust_env=False
            ) as agent_b:
                download = await agent_b.get("/v1/files", params={"path": "identity.bin"})
                assert download.status_code == 200
                assert download.content == sandbox_b.encode() + b"\x00\xff"
                command = await agent_b.post(
                    "/v1/commands:run", json={"argv": ["printf", "B-still-running"]}
                )
                assert command.status_code == 200 and command.json()["exit_code"] == 0
                assert command.json()["stdout"] == "B-still-running"
            record_property("other_sandbox", json.dumps({
                "sandbox_id": sandbox_b, "status": "ready", "command_exit_code": 0,
                "workspace_file_preserved": True,
            }))

            # A now consists only of its retained workspace, which fresh discovery
            # must find despite the missing container, proxy and both networks.
            for _ in range(2):
                deleted_workspace = await control.delete(
                    f"/v1/sandboxes/{sandbox_a}", params={"delete_workspace": "true"}
                )
                assert deleted_workspace.status_code == 204, deleted_workspace.text
                assert deleted_workspace.content == b""
            await asyncio.to_thread(_assert_recorded_resources_absent, client, records_a)
            record_property("deleted_sandbox", json.dumps({
                "sandbox_id": sandbox_a, "entry_unreachable": True,
                "query_status": 404, "default_workspace_retained": True,
                "retained_file_verified": True, "explicit_workspace_deleted": True,
                "resource_count": len(records_a),
            }))

            deleted_b = await control.delete(
                f"/v1/sandboxes/{sandbox_b}", params={"delete_workspace": "true"}
            )
            assert deleted_b.status_code == 204, deleted_b.text
            assert deleted_b.content == b""
            assert await restarted_store.get(sandbox_b) is None
            await asyncio.to_thread(
                _assert_recorded_resources_absent, client, resources_by_id[sandbox_b]
            )
    finally:
        try:
            _remove_owned_resources(client, list(reversed(created)))
            _assert_recorded_resources_absent(client, created)
            record_property("cleanup", f"verified absent: {len(created)} exact resources")
        finally:
            client.close()


@pytest.mark.docker_integration
@pytest.mark.skipif(
    os.getenv("RUN_DOCKER_INTEGRATION") != "1",
    reason="explicit Docker opt-in required",
)
@pytest.mark.anyio
@pytest.mark.timeout(150)
async def test_reap_expired_once_retains_workspace_and_unexpired_sandbox(
    tmp_path: Path,
) -> None:
    """Reap expired A once with fresh metadata while preserving its data and live B."""
    database_path = tmp_path / "reaper.sqlite3"
    store = SQLiteLifecycleStore(database_path)
    await store.initialize()
    created_at = datetime(2026, 10, 19, 8, 0, 0, 123456, tzinfo=UTC)
    now = created_at + timedelta(minutes=15)
    client = docker.from_env(timeout=5.0)
    created: list[CreatedResource] = []
    recording_client = RecordingDockerClient(client, created)
    runtime = DockerSandboxRuntime(lambda: recording_client)
    service = SandboxService(runtime, store, clock=lambda: created_at)
    try:
        sandbox_a = (await service.create()).sandbox_id
        sandbox_b = (await service.create()).sandbox_id
        records_a = [
            record for record in created
            if record.labels["io.agent-sandbox.sandbox-id"] == sandbox_a
        ]
        records_b = [
            record for record in created
            if record.labels["io.agent-sandbox.sandbox-id"] == sandbox_b
        ]
        assert len(records_a) == len(records_b) == 5
        resources_a = {record.kind: record for record in records_a}
        resources_b = {record.kind: record for record in records_b}
        record_a = await store.get(sandbox_a)
        record_b = await store.get(sandbox_b)
        assert record_a is not None and record_b is not None
        assert record_a.management_state == record_b.management_state == "active"
        expired_a = await store.update_expiration(
            sandbox_a, record_a.transaction_id, expected_version=record_a.version,
            expires_at=now,
        )
        assert expired_a.expires_at == now < record_b.expires_at

        restarted_store = SQLiteLifecycleStore(database_path)
        await restarted_store.initialize()
        restarted_runtime = DockerSandboxRuntime()
        clock = Mock(return_value=now)
        reaper = SandboxService(restarted_runtime, restarted_store, clock=clock)
        application = create_app(
            restarted_runtime, sandbox_query_timeout=5, store=restarted_store,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=application), base_url="http://control"
        ) as control:
            for sandbox_id, resources in (
                (sandbox_a, resources_a), (sandbox_b, resources_b)
            ):
                await _wait_for_ready(
                    control, sandbox_id,
                    client.containers.get(resources["container"].resource_id),
                )
            endpoint_a = await _wait_for_endpoint(control, sandbox_a)
            endpoint_b = await _wait_for_endpoint(control, sandbox_b)
            async with httpx.AsyncClient(
                base_url=endpoint_a, timeout=5, trust_env=False
            ) as agent_a:
                uploaded = await agent_a.post(
                    "/v1/files", params={"path": "identity.bin"},
                    content=sandbox_a.encode() + b"\x00\xff",
                )
                assert uploaded.status_code == 201, uploaded.text

            result = await reaper.reap_expired_once(limit=100)
            clock.assert_called_once_with()
            assert result.now == now and result.error is None
            assert (result.succeeded, result.skipped, result.failed) == (1, 0, 0)
            assert len(result.items) == 1
            assert result.items[0].sandbox_id == sandbox_a
            assert result.items[0].outcome == "succeeded"
            assert result.items[0].reason == "deleted"
            assert await restarted_store.get(sandbox_a) is None
            assert await restarted_store.get(sandbox_b) == record_b
            assert (await control.get(f"/v1/sandboxes/{sandbox_a}")).status_code == 404
            assert (await control.get(f"/v1/sandboxes/{sandbox_a}/endpoint")).status_code == 404
            async with httpx.AsyncClient(timeout=3, trust_env=False) as old_entry:
                with pytest.raises(httpx.RequestError):
                    await old_entry.get(f"{endpoint_a}/healthz")
            await asyncio.to_thread(
                _assert_recorded_resources_absent, client,
                [record for record in records_a if record.kind != "workspace"],
            )
            assert await asyncio.to_thread(
                _read_retained_workspace, client, created, resources_a["workspace"]
            ) == sandbox_a.encode() + b"\x00\xff"

            repeated = await reaper.reap_expired_once(limit=100)
            assert clock.call_count == 2
            assert repeated.error is None and repeated.items == ()
            assert (repeated.succeeded, repeated.skipped, repeated.failed) == (0, 0, 0)
            workspace = client.volumes.get(resources_a["workspace"].resource_id)
            workspace.reload()
            assert _has_expected_labels(_labels(workspace, "workspace"), resources_a["workspace"].labels)
            observed_b = await control.get(f"/v1/sandboxes/{sandbox_b}")
            assert observed_b.status_code == 200 and observed_b.json()["status"] == "ready"
            assert await _wait_for_endpoint(control, sandbox_b) == endpoint_b
            async with httpx.AsyncClient(
                base_url=endpoint_b, timeout=5, trust_env=False
            ) as agent_b:
                command = await agent_b.post(
                    "/v1/commands:run", json={"argv": ["printf", "B-still-running"]}
                )
                assert command.status_code == 200 and command.json()["exit_code"] == 0
                assert command.json()["stdout"] == "B-still-running"
    finally:
        try:
            _remove_owned_resources(client, list(reversed(created)))
            _assert_recorded_resources_absent(client, created)
        finally:
            client.close()


async def _wait_for_endpoint(http_client: httpx.AsyncClient, sandbox_id: str) -> str:
    """Poll entry readiness within a finite deadline using only the public API."""
    async with asyncio.timeout(30):
        while True:
            response = await http_client.get(f"/v1/sandboxes/{sandbox_id}/endpoint")
            if response.status_code == 200:
                return response.json()["url"]
            assert response.status_code == 503, response.text[:500]
            await asyncio.sleep(0.25)


async def _exercise_agent(url: str, marker: str) -> dict[str, Any]:
    """Verify health, instance identity, binary file round-trip and realtime SSE."""
    async with httpx.AsyncClient(base_url=url, timeout=8, trust_env=False) as agent:
        health = await agent.get("/healthz")
        assert health.status_code == 200
        command = await agent.post("/v1/commands:run", json={"argv": ["cat", "/etc/hostname"]})
        assert command.status_code == 200 and command.json()["exit_code"] == 0
        hostname = command.json()["stdout"].strip()
        payload = marker.encode() + b"\x00\xff"
        upload = await agent.post("/v1/files", params={"path": "identity.bin"}, content=payload)
        assert upload.status_code == 201, upload.text
        download = await agent.get("/v1/files", params={"path": "identity.bin"})
        assert download.status_code == 200 and download.content == payload
        processed = await agent.post("/v1/commands:run", json={"argv": ["wc", "-c", "/workspace/identity.bin"]})
        assert processed.status_code == 200 and processed.json()["exit_code"] == 0
        assert int(processed.json()["stdout"].split()[0]) == len(payload)
        # Buffering until command completion would delay this first frame by 2s.
        started = time.monotonic()
        first_seconds = None
        output = b""
        complete = False
        async with asyncio.timeout(8):
            async with agent.stream("POST", "/v1/commands:stream", json={
                "argv": ["sh", "-c", "printf first; sleep 2; printf second"],
            }) as stream:
                assert stream.status_code == 200
                assert stream.headers["content-type"].startswith("text/event-stream")
                event = ""
                async for line in stream.aiter_lines():
                    if line.startswith("event: "):
                        event = line[7:]
                    elif line.startswith("data: "):
                        data = json.loads(line[6:])
                        if event == "stdout":
                            if first_seconds is None:
                                first_seconds = time.monotonic() - started
                                assert first_seconds < 1.5
                            output += base64.b64decode(data["data_base64"])
                        elif event == "complete":
                            assert data["exit_code"] == 0
                            complete = True
        assert output == b"firstsecond" and complete
        assert time.monotonic() - started >= 2
        return {"url": url, "hostname": hostname, "health": 200, "command": 200,
                "upload": 201, "download": 200, "sse_first_seconds": round(first_seconds, 3)}


def _verify_proxy_configuration(client: Any, resources: dict[str, CreatedResource], spec: SandboxSpec) -> None:
    """Inspect proxy limits, two networks, effective port and absence of host mounts."""
    proxy = client.containers.get(resources["proxy"].resource_id)
    sandbox = client.containers.get(resources["container"].resource_id)
    proxy.reload()
    sandbox.reload()
    host = proxy.attrs["HostConfig"]
    assert proxy.attrs["Config"]["User"] == "nginx"
    assert proxy.attrs["Config"]["Entrypoint"] == ["nginx", "-g", "daemon off;"]
    assert proxy.attrs["Mounts"] == []
    assert host["ReadonlyRootfs"] is True and host["CapDrop"] == ["ALL"]
    assert "no-new-privileges:true" in host["SecurityOpt"]
    assert host["NanoCpus"] == 500_000_000
    assert host["Memory"] == host["MemorySwap"] == 64 * 1024 * 1024
    assert host["PidsLimit"] == 32
    assert host["Tmpfs"] == {"/tmp": "rw,nosuid,nodev,size=16m,mode=1777"}
    assert set(proxy.attrs["NetworkSettings"]["Networks"]) == {spec.network_name, spec.ingress_network_name}
    assert set(sandbox.attrs["NetworkSettings"]["Networks"]) == {spec.network_name}
    assert "sandbox" in sandbox.attrs["NetworkSettings"]["Networks"][spec.network_name]["Aliases"]
    assert sandbox.attrs["HostConfig"].get("PortBindings") in (None, {})
    assert proxy.attrs["NetworkSettings"]["Ports"]["8080/tcp"][0]["HostIp"] == "127.0.0.1"
    for kind, internal in [("network", True), ("ingress", False)]:
        network = client.networks.get(resources[kind].resource_id)
        assert network.attrs["Internal"] is internal
        assert network.attrs["Driver"] == "bridge"
        assert _has_expected_labels(_labels(network, kind), spec.labels_for(kind))


def _verify_egress_with_bridge_control(client: Any, created: list[CreatedResource], sandbox_ids: list[str]) -> dict[str, Any]:
    """Require successful ordinary bridge TCP access before testing internal denial."""
    public_ip = os.getenv("AGENT_SANDBOX_EGRESS_IP", "1.1.1.1")
    control_spec = _spec_for(f"sbx_{secrets.token_hex(16)}")
    expected = control_spec.labels_for("container")
    response = client.api.create_container(
        image="agent-sandbox:dev", name=control_spec.container_name, labels=expected,
        user="sandbox", entrypoint=["sleep"], command=["60"],
        host_config=client.api.create_host_config(
            network_mode="bridge", read_only=True, cap_drop=["ALL"],
            security_opt=["no-new-privileges:true"], mem_limit="64m", memswap_limit="64m",
            nano_cpus=500_000_000, pids_limit=32,
        ),
    )
    control_id = response["Id"]
    created.append(CreatedResource("container", control_id, expected))
    control = client.containers.get(control_id)
    control.start()
    argv = ["nc", "-n", "-z", "-w", "3", public_ip, "80"]
    baseline = control.exec_run(argv)
    assert baseline.exit_code == 0, "ordinary bridge cannot reach public IP; isolation is unverified"
    denied = []
    for sandbox_id in sandbox_ids:
        result = client.containers.get(sandbox_id).exec_run(argv)
        assert result.exit_code != 0, "internal sandbox unexpectedly reached public IP"
        denied.append(result.exit_code)
    return {"target": f"{public_ip}:80", "bridge_exit_code": 0, "internal_exit_codes": denied}


@pytest.mark.docker_integration
@pytest.mark.skipif(os.getenv("RUN_DOCKER_INTEGRATION") != "1", reason="explicit Docker opt-in required")
@pytest.mark.anyio
@pytest.mark.timeout(120)
async def test_two_dynamic_entries_agent_calls_isolation_and_restart_recovery(
    record_property: Any,
    tmp_path: Path,
) -> None:
    """Create two entries, prove correct routing and isolation, then clean only this ledger."""
    database_path = tmp_path / "lifecycle.sqlite3"
    store = SQLiteLifecycleStore(database_path)
    await store.initialize()
    client = docker.from_env(timeout=5.0)
    created: list[CreatedResource] = []
    recording_client = RecordingDockerClient(client, created)
    specs: list[SandboxSpec] = []
    results: list[dict[str, Any]] = []
    sandbox_container_ids: list[str] = []
    try:
        application = create_app(
            DockerSandboxRuntime(lambda: recording_client), sandbox_query_timeout=5,
            store=store,
        )
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://control") as control:
            for _ in range(2):
                response = await control.post("/v1/sandboxes")
                assert response.status_code == 201, response.text
                sandbox_id = response.json()["sandbox_id"]
                records = [r for r in created if r.labels["io.agent-sandbox.sandbox-id"] == sandbox_id]
                resources = {r.kind: r for r in records}
                assert set(resources) == {"network", "ingress", "workspace", "container", "proxy"}
                spec = _spec_for(sandbox_id)
                spec = SandboxSpec(spec.sandbox_id, spec.container_name, spec.network_name, spec.volume_name,
                                   resources["proxy"].labels["io.agent-sandbox.transaction-id"])
                specs.append(spec)
                container_id = resources["container"].resource_id
                sandbox_container_ids.append(container_id)
                observed = await _wait_for_ready(control, sandbox_id, client.containers.get(container_id))
                assert observed["status"] == "ready"
                url = await _wait_for_endpoint(control, sandbox_id)
                await asyncio.to_thread(_verify_proxy_configuration, client, resources, spec)
                results.append(await _exercise_agent(url, sandbox_id))
                container = client.containers.get(container_id)
                assert results[-1]["hostname"] == container.attrs["Config"]["Hostname"]
        assert results[0]["url"] != results[1]["url"]
        assert results[0]["hostname"] != results[1]["hostname"]
        # A new app and runtime have no creation memory and must recover both URLs.
        restarted_store = SQLiteLifecycleStore(database_path)
        await restarted_store.initialize()
        restarted = create_app(
            DockerSandboxRuntime(), sandbox_query_timeout=5, store=restarted_store,
        )
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=restarted), base_url="http://restarted") as control:
            for spec, result in zip(specs, results):
                response = await control.get(f"/v1/sandboxes/{spec.sandbox_id}/endpoint")
                assert response.status_code == 200 and response.json()["url"] == result["url"]
                async with httpx.AsyncClient(base_url=result["url"], timeout=5, trust_env=False) as agent:
                    download = await agent.get("/v1/files", params={"path": "identity.bin"})
                    assert download.content == spec.sandbox_id.encode() + b"\x00\xff"
        egress = await asyncio.to_thread(_verify_egress_with_bridge_control, client, created, sandbox_container_ids)
        record_property("entries", json.dumps(results))
        record_property("egress", json.dumps(egress))
    finally:
        try:
            _remove_owned_resources(client, list(reversed(created)))
            # Absence is verified by exact ID, never by a project-wide deletion scan.
            for record in created:
                with pytest.raises(docker.errors.NotFound):
                    _manager(client, record.kind).get(record.resource_id)
            record_property("cleanup", f"verified absent: {len(created)} exact resources")
        finally:
            client.close()
