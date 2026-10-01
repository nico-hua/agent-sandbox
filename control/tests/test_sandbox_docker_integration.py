"""Opt-in integration coverage for real Docker sandbox creation."""

import asyncio
import os
import secrets
from dataclasses import dataclass
from typing import Any, Literal

import docker
import httpx
import pytest

from control.app import create_app
from control.sandbox_runtime import DockerSandboxRuntime
from control.sandbox_service import SandboxService, SandboxSpec


ResourceKind = Literal["container", "workspace", "network"]


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
    if kind == "container":
        value = resource.attrs.get("Config", {}).get("Labels", {})
    else:
        value = resource.attrs.get("Labels", {})
    return value if isinstance(value, dict) else {}


def _manager(client: Any, kind: ResourceKind) -> Any:
    """Return the Docker collection that loads a resource by exact ID."""
    if kind == "container":
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
            resource = _manager(client, record.kind).get(record.resource_id)
            resource.reload()
            if resource.id != record.resource_id:
                errors.append(f"{record.kind} ID changed")
                continue
            actual_labels = _labels(resource, record.kind)
            if not _has_expected_labels(actual_labels, record.labels):
                errors.append(f"{record.kind} labels changed")
                continue
            if record.kind == "container":
                resource.remove(force=True)
            else:
                resource.remove()
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
            CreatedResource("network", response["Id"], dict(kwargs["labels"]))
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
            CreatedResource("container", response["Id"], dict(kwargs["labels"]))
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
        "io.agent-sandbox.managed": "true",
        "io.agent-sandbox.transaction-id": "txn_test",
    }

    proxy.create_network("network", labels=labels)
    proxy.create_volume("volume", labels=labels)
    proxy.create_container(labels=labels)

    assert [(record.kind, record.resource_id) for record in created] == [
        ("network", "network-id"),
        ("workspace", "volume"),
        ("container", "container-id"),
    ]


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
async def test_real_docker_creation_uses_fixed_isolated_configuration() -> None:
    """Create via HTTP, reach ready, inspect isolation, and clean exact resources."""
    sandbox_id = f"sbx_{secrets.token_hex(16)}"
    transaction_id = f"txn_{secrets.token_hex(16)}"
    spec = _spec_for(sandbox_id)
    spec = SandboxSpec(
        sandbox_id=spec.sandbox_id,
        container_name=spec.container_name,
        network_name=spec.network_name,
        volume_name=spec.volume_name,
        transaction_id=transaction_id,
    )
    client = docker.from_env(timeout=5.0)
    created: list[CreatedResource] = []
    recording_client = RecordingDockerClient(client, created)
    service = SandboxService(
        DockerSandboxRuntime(lambda: recording_client),
        id_factory=lambda: sandbox_id,
        transaction_id_factory=lambda: transaction_id,
    )

    try:
        transport = httpx.ASGITransport(app=create_app(sandbox_creator=service))
        async with httpx.AsyncClient(
            transport=transport, base_url="http://control"
        ) as http_client:
            response = await http_client.post("/v1/sandboxes")
        assert response.status_code == 201
        assert response.json() == {"sandbox_id": sandbox_id, "status": "started"}

        resources = {record.kind: record for record in created}
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

        # The default query dependency is a fresh runtime with no creation memory.
        async with httpx.AsyncClient(
            transport=transport, base_url="http://control"
        ) as http_client:
            observed = await _wait_for_ready(http_client, sandbox_id, container)
        assert observed == {
            "sandbox_id": sandbox_id,
            "status": "ready",
            "reason": "health_check_passed",
            "message": "Agent health check passed",
        }
        container.reload()
        assert container.attrs["State"]["Health"]["Status"] == "healthy"
    finally:
        try:
            _remove_owned_resources(client, list(reversed(created)))
        finally:
            client.close()
