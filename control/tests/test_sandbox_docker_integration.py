"""Opt-in integration coverage for real Docker sandbox creation."""

import os
import secrets
from dataclasses import dataclass
from typing import Any, Literal

import docker
import pytest

from control.sandbox_runtime import DockerSandboxRuntime
from control.sandbox_service import SandboxService, SandboxSpec


pytestmark = [
    pytest.mark.docker_integration,
    pytest.mark.skipif(
        os.getenv("RUN_DOCKER_INTEGRATION") != "1",
        reason="set RUN_DOCKER_INTEGRATION=1 to modify Docker resources",
    ),
]


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


@pytest.mark.anyio
async def test_real_docker_creation_uses_fixed_isolated_configuration() -> None:
    """Create, inspect, and exactly clean one fixed-policy Docker sandbox."""
    sandbox_id = f"sbx_{secrets.token_hex(16)}"
    spec = _spec_for(sandbox_id)
    service = SandboxService(
        DockerSandboxRuntime(),
        id_factory=lambda: sandbox_id,
    )
    client = docker.from_env(timeout=5.0)
    created: list[CreatedResource] = []

    try:
        result = await service.create()

        container = client.containers.get(spec.container_name)
        volume = client.volumes.get(spec.volume_name)
        network = client.networks.get(spec.network_name)
        container.reload()
        volume.reload()
        network.reload()
        created.extend(
            [
                CreatedResource(
                    "container", container.id, spec.labels_for("container")
                ),
                CreatedResource(
                    "workspace", volume.id, spec.labels_for("workspace")
                ),
                CreatedResource("network", network.id, spec.labels_for("network")),
            ]
        )

        assert result.sandbox_id == sandbox_id
        assert result.status == "started"
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
    finally:
        try:
            _remove_owned_resources(client, created)
        finally:
            client.close()
