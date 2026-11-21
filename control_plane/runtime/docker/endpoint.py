"""Verify Docker entry proxy ownership, topology and advertised capabilities."""

import hashlib
import re
from typing import Any

from docker.errors import NotFound

from control_plane.domain.errors import SandboxEndpointUnavailable, SandboxNotFound
from control_plane.domain.sandbox import ResourceKind, SandboxSpec
from control_plane.runtime.docker.client import DockerClient
from control_plane.runtime.docker.resources import (
    ENDPOINT_AUTH_LABEL,
    ENDPOINT_SERVICES_LABEL,
    PROXY_AUTH_IMAGE_LABEL,
    PROXY_SERVICES_IMAGE_LABEL,
    _lifecycle_times,
    _ResourceRecord,
    labels_match,
    query_labels_match,
    resource_labels,
)


def _owned_container(
    client: DockerClient, sandbox_id: str, role: ResourceKind
) -> Any:
    """Resolve one container per role, then re-inspect its ID and complete ownership."""
    candidates = client.containers.list(
        all=True,
        filters={"label": [
            "io.agent-sandbox.managed=true",
            "io.agent-sandbox.project=agent-sandbox",
            f"io.agent-sandbox.sandbox-id={sandbox_id}",
            f"io.agent-sandbox.resource={role}",
        ]},
    )
    # Missing main means not found; a missing or ambiguous proxy means entry unavailable.
    if not candidates and role == "container":
        raise SandboxNotFound()
    if len(candidates) != 1:
        raise SandboxEndpointUnavailable()
    # Re-inspect the listing snapshot to detect disappearance or ownership changes.
    resource_id = candidates[0].id
    try:
        container = client.containers.get(resource_id)
        container.reload()
    except NotFound as error:
        if role == "container":
            raise SandboxNotFound() from error
        raise SandboxEndpointUnavailable() from error
    if container.id != resource_id or not query_labels_match(
        resource_labels(container, role), sandbox_id, role
    ):
        if role == "container":
            raise SandboxNotFound()
        raise SandboxEndpointUnavailable()
    return container


def _verify_entry_networks(
    client: DockerClient, sandbox: Any, proxy: Any, spec: SandboxSpec,
    *, expected_network_ids: dict[str, str] | None = None,
) -> None:
    """Verify attachments, alias and ownership, optionally pinning original inventory bridge IDs."""
    sandbox_settings = sandbox.attrs["NetworkSettings"]
    sandbox_networks = sandbox_settings["Networks"]
    proxy_networks = proxy.attrs["NetworkSettings"]["Networks"]
    # Sandbox uses internal; proxy uses ingress plus internal, with no extra attachments.
    if set(sandbox_networks) != {spec.network_name} or set(proxy_networks) != {
        spec.network_name,
        spec.ingress_network_name,
    }:
        raise SandboxEndpointUnavailable()
    internal_connection = sandbox_networks[spec.network_name]
    if "sandbox" not in (internal_connection.get("Aliases") or []):
        raise SandboxEndpointUnavailable()
    # Publish only through the proxy; matching names do not prove a shared internal bridge.
    if any(sandbox_settings.get("Ports", {}).values()):
        raise SandboxEndpointUnavailable()
    if internal_connection["NetworkID"] != proxy_networks[spec.network_name]["NetworkID"]:
        raise SandboxEndpointUnavailable()
    # Re-inspect connected NetworkIDs instead of trusting names in container snapshots.
    for name, role, internal in [
        (spec.network_name, "network", True),
        (spec.ingress_network_name, "ingress", False),
    ]:
        network_id = proxy_networks[name]["NetworkID"]
        if not isinstance(network_id, str) or not network_id:
            raise SandboxEndpointUnavailable()
        if expected_network_ids is not None and network_id != expected_network_ids[role]:
            # Matching labels/names cannot authorize a bridge substituted after preflight.
            raise SandboxEndpointUnavailable()
        network = client.networks.get(network_id)
        network.reload()
        attrs = network.attrs
        if (
            network.id != network_id
            or attrs.get("Name") != name
            or attrs.get("Driver") != "bridge"
            or attrs.get("Internal") is not internal
            or not labels_match(resource_labels(network, role), spec.labels_for(role))
        ):
            raise SandboxEndpointUnavailable()


def _endpoint_auth_facts(
    client: DockerClient, records: tuple[_ResourceRecord, ...], proxy: Any | None,
) -> tuple[str | None, str | None]:
    """Validate survivor markers and hash the credential from verified current proxy config."""
    markers = [(record.observed_labels or {}).get(ENDPOINT_AUTH_LABEL)
               for record in records if not record.external]
    if not markers or all(value is None for value in markers):
        if proxy is not None:
            environment = proxy.attrs.get("Config", {}).get("Env", []) or []
            if any(isinstance(value, str) and value.startswith(("SANDBOX_ACCESS_TOKEN=", "SANDBOX_PROXY_AUTH_VERSION=")) for value in environment):
                raise SandboxEndpointUnavailable()
            image_id = proxy.attrs.get("Image")
            if image_id is not None:
                image = client.images.get(image_id)
                if (image.attrs.get("Config", {}).get("Labels") or {}).get(PROXY_AUTH_IMAGE_LABEL) is not None:
                    raise SandboxEndpointUnavailable()
        return None, None
    if any(value != "1" for value in markers):
        raise SandboxEndpointUnavailable()
    main = next((record for record in records if record.kind == "container"), None)
    if main is not None and proxy is not None:
        created_at, _ = _lifecycle_times(main.observed_labels or {})
        proxy_created_at, _ = _lifecycle_times(resource_labels(proxy, "proxy"))
        if created_at is None or proxy_created_at != created_at:
            raise SandboxEndpointUnavailable()
    if proxy is None:
        return "1", None
    environment = proxy.attrs.get("Config", {}).get("Env")
    if not isinstance(environment, list):
        raise SandboxEndpointUnavailable()
    tokens = [value.partition("=")[2] for value in environment
              if isinstance(value, str) and value.startswith("SANDBOX_ACCESS_TOKEN=")]
    versions = [value.partition("=")[2] for value in environment
                if isinstance(value, str) and value.startswith("SANDBOX_PROXY_AUTH_VERSION=")]
    if len(tokens) != 1 or re.fullmatch(r"[A-Za-z0-9_-]{43}", tokens[0]) is None or versions != ["1"]:
        raise SandboxEndpointUnavailable()
    image_id = proxy.attrs.get("Image")
    if not isinstance(image_id, str) or not image_id:
        raise SandboxEndpointUnavailable()
    image = client.images.get(image_id)
    if image.id != image_id or (image.attrs.get("Config", {}).get("Labels") or {}).get(PROXY_AUTH_IMAGE_LABEL) != "1":
        raise SandboxEndpointUnavailable()
    return "1", hashlib.sha256(tokens[0].encode("ascii")).hexdigest()


def _endpoint_services_version(client: DockerClient, records: tuple[_ResourceRecord, ...], proxy: Any) -> str | None:
    """Distinguish old proxies from matching current resource and pinned-image capabilities."""
    markers = [(record.observed_labels or {}).get(ENDPOINT_SERVICES_LABEL)
               for record in records if not record.external]
    image_id = proxy.attrs.get("Image")
    image_version = None
    if image_id is not None:
        image = client.images.get(image_id)
        if image.id != image_id:
            raise SandboxEndpointUnavailable()
        image_version = (image.attrs.get("Config", {}).get("Labels") or {}).get(PROXY_SERVICES_IMAGE_LABEL)
    if markers and all(value is None for value in markers) and image_version is None:
        return None
    if not markers or any(value != "1" for value in markers) or image_version != "1":
        raise SandboxEndpointUnavailable()
    return "1"


def _published_loopback_url(proxy: Any) -> str:
    """Accept only a unique effective IPv4 loopback mapping for Agent HTTP."""
    # HostConfig records intent; NetworkSettings contains the actual allocated port.
    mappings = proxy.attrs["NetworkSettings"]["Ports"].get("8080/tcp")
    if not isinstance(mappings, list) or len(mappings) != 1:
        raise SandboxEndpointUnavailable()
    binding = mappings[0]
    port = binding.get("HostPort")
    if (
        binding.get("HostIp") != "127.0.0.1"
        or not isinstance(port, str)
        or re.fullmatch(r"[0-9]{1,5}", port) is None
        or not 1 <= int(port) <= 65535
    ):
        raise SandboxEndpointUnavailable()
    return f"http://127.0.0.1:{int(port)}"
