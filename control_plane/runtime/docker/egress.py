"""Verify Docker egress proxy attachments, ownership and runtime state."""

from typing import Literal

from docker.errors import NotFound

from control_plane.domain.errors import SandboxQueryFailed
from control_plane.domain.sandbox import SandboxNetwork
from control_plane.runtime.docker.client import DockerClient
from control_plane.runtime.docker.resources import (
    _ResourceRecord,
    labels_match,
    network_configuration,
    resource_labels,
    resource_manager,
)


def _egress_observation(
    client: DockerClient, sandbox_id: str, records: tuple[_ResourceRecord, ...], configuration: SandboxNetwork,
    *, observe_state: bool = True,
) -> tuple[Literal["failed", "starting", "paused"], str, str] | None:
    """Verify dual-network topology and optionally observe frozen state and local egress health."""
    record = next((item for item in records if item.kind == "egress"), None)
    if record is None:
        return "failed", "egress_missing", "required egress proxy is missing"
    try:
        egress = client.containers.get(record.resource_id)
        egress.reload()
    except NotFound:
        return "failed", "egress_missing", "required egress proxy is missing"
    labels = resource_labels(egress, "egress")
    if egress.id != record.resource_id or not labels_match(labels, record.labels) or network_configuration(labels) != configuration:
        raise SandboxQueryFailed()
    attrs = egress.attrs
    attached = attrs["NetworkSettings"]["Networks"]
    internal_name = f"agent-sandbox-{sandbox_id}-internal"
    ingress_name = f"agent-sandbox-{sandbox_id}-ingress"
    if set(attached) != {internal_name, ingress_name} or "egress" not in (attached[internal_name].get("Aliases") or []):
        raise SandboxQueryFailed()
    if any(attrs["NetworkSettings"].get("Ports", {}).values()) or attrs.get("HostConfig", {}).get("PortBindings") or attrs.get("Mounts"):
        raise SandboxQueryFailed()
    for name, role, internal in ((internal_name, "network", True), (ingress_name, "ingress", False)):
        target = next((item for item in records if item.kind == role), None)
        network_id = attached[name].get("NetworkID")
        if target is None or target.resource_id != network_id:
            raise SandboxQueryFailed()
        try:
            network = resource_manager(client, role).get(network_id)
            network.reload()
        except NotFound as error:
            raise SandboxQueryFailed() from error
        if (network.id != network_id or not labels_match(resource_labels(network, role), target.labels)
            or network_configuration(resource_labels(network, role)) != configuration
            or network.attrs.get("Name") != name or network.attrs.get("Driver") != "bridge"
            or network.attrs.get("Internal") is not internal):
            raise SandboxQueryFailed()
    main = next(item for item in records if item.kind == "container")
    sandbox = client.containers.get(main.resource_id)
    sandbox.reload()
    if sandbox.id != main.resource_id or not labels_match(resource_labels(sandbox, "container"), main.labels):
        raise SandboxQueryFailed()
    sandbox_networks = sandbox.attrs["NetworkSettings"]["Networks"]
    if set(sandbox_networks) != {internal_name} or sandbox_networks[internal_name].get("NetworkID") != attached[internal_name].get("NetworkID") or any(sandbox.attrs["NetworkSettings"].get("Ports", {}).values()):
        raise SandboxQueryFailed()
    if not observe_state:
        return None
    state = attrs.get("State", {})
    main_state = sandbox.attrs.get("State", {})
    main_paused = main_state.get("Paused") is True or main_state.get("Status") == "paused"
    egress_paused = state.get("Paused") is True or state.get("Status") == "paused"
    if main_paused and egress_paused:
        return "paused", "container_paused", "container is paused"
    if egress_paused:
        return "failed", "pause_incomplete", "sandbox and required egress proxy have inconsistent pause states"
    if state.get("Status") != "running":
        return "failed", "egress_stopped", "required egress proxy is not running"
    if main_paused:
        return "failed", "pause_incomplete", "sandbox and required egress proxy have inconsistent pause states"
    health = state.get("Health", {}).get("Status")
    if health == "healthy":
        return None
    if health == "starting":
        return "starting", "egress_starting", "egress proxy health check is starting"
    return "failed", "egress_unhealthy", "required egress proxy health check is not passing"
