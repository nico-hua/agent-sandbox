"""Discover Docker resources and guard exact-ID ownership, inspection and removal."""

import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

import docker
from docker.errors import NotFound

from control_plane.domain.errors import SandboxDeleteConflict, WorkspaceError, WorkspaceNotFound
from control_plane.domain.models import ResourceKind, SandboxNetwork, WorkspaceBinding
from control_plane.runtime.workspaces import (
    binding_from_labels,
    binding_labels,
    verified_volume,
    verify_workspace_mount,
    verify_workspace_references,
    workspace_labels,
    workspace_record,
)


class DockerClient(Protocol):
    """Describe the Docker client surface used by sandbox resource operations."""

    images: Any
    networks: Any
    volumes: Any
    containers: Any
    api: Any

    def close(self) -> None:
        """Close client-owned transport resources."""
        ...


def create_runtime_client() -> DockerClient:
    """Create a Docker SDK client with a finite request timeout."""
    return docker.from_env(timeout=5.0)


ENDPOINT_AUTH_LABEL = "io.agent-sandbox.endpoint-auth-version"


PROXY_AUTH_IMAGE_LABEL = "io.agent-sandbox.proxy-auth-version"


PROXY_SERVICES_IMAGE_LABEL = "io.agent-sandbox.proxy-services-version"


ENDPOINT_SERVICES_LABEL = "io.agent-sandbox.endpoint-services-version"


NETWORK_VERSION_LABEL = "io.agent-sandbox.network-version"


ALLOW_DOMAINS_LABEL = "io.agent-sandbox.allow-domains"


@dataclass(frozen=True)
class _ResourceRecord:
    """Track exact identity and ownership for a created or recovered resource."""

    kind: ResourceKind
    resource_id: str
    labels: dict[str, str]
    observed_labels: dict[str, str] | None = None
    external: bool = False


def resource_manager(client: DockerClient, kind: ResourceKind) -> Any:
    """Select the exact-ID Docker manager for one recorded kind."""
    if kind in {"container", "proxy", "egress"}:
        return client.containers
    if kind == "workspace":
        return client.volumes
    return client.networks


def resource_labels(resource: Any, kind: ResourceKind) -> dict[str, str]:
    """Read Docker labels from the shape used by each resource type."""
    attrs = resource.attrs
    if not isinstance(attrs, dict):
        return {}
    if kind in {"container", "proxy", "egress"}:
        config = attrs.get("Config")
        labels = config.get("Labels") if isinstance(config, dict) else None
    else:
        labels = attrs.get("Labels")
    return labels if isinstance(labels, dict) else {}


def labels_match(actual: Any, expected: dict[str, str]) -> bool:
    """Check complete expected ownership labels on a Docker response."""
    return isinstance(actual, dict) and all(
        actual.get(key) == value for key, value in expected.items()
    )


def query_labels_match(
    labels: dict[str, str], sandbox_id: str, role: ResourceKind = "container"
) -> bool:
    """Verify all ownership labels required for a readable container."""
    expected = {
        "io.agent-sandbox.managed": "true",
        "io.agent-sandbox.project": "agent-sandbox",
        "io.agent-sandbox.sandbox-id": sandbox_id,
        "io.agent-sandbox.resource": role,
    }
    transaction_id = labels.get("io.agent-sandbox.transaction-id", "")
    return (
        labels_match(labels, expected)
        and isinstance(transaction_id, str)
        and re.fullmatch(r"txn_[0-9a-f]{32}", transaction_id) is not None
    )


def network_configuration(labels: dict[str, str]) -> SandboxNetwork | None:
    """Restore canonical v1 network metadata; only complete absence denotes legacy topology."""
    if NETWORK_VERSION_LABEL not in labels and ALLOW_DOMAINS_LABEL not in labels:
        return None
    if labels.get(NETWORK_VERSION_LABEL) != "1":
        raise ValueError("invalid network metadata version")
    raw = labels.get(ALLOW_DOMAINS_LABEL)
    if not isinstance(raw, str):
        raise ValueError("missing domain policy metadata")
    domains = json.loads(raw)
    if type(domains) is not list:
        raise ValueError("invalid domain policy metadata")
    configuration = SandboxNetwork(domains)
    if raw != json.dumps(list(configuration.allow_domains), separators=(",", ":")):
        raise ValueError("noncanonical domain policy metadata")
    return configuration


def inventory_network_configuration(records: tuple["_ResourceRecord", ...]) -> SandboxNetwork:
    """Compare every observed survivor's markers without imposing deletion completeness rules."""
    configurations = [network_configuration(
        record.observed_labels if record.observed_labels is not None else record.labels
    ) for record in records if not record.external]
    if configurations and any(value != configurations[0] for value in configurations):
        raise ValueError("inconsistent network metadata")
    configuration = configurations[0] if configurations else None
    if any(record.kind == "egress" for record in records) and (
        configuration is None or not configuration.allow_domains
    ):
        raise ValueError("unexpected egress resource")
    return configuration if configuration is not None else SandboxNetwork()


_DELETION_ORDER: tuple[ResourceKind, ...] = (
    "egress", "proxy", "container", "ingress", "network", "workspace"
)


def _inspect_owned(client: DockerClient, record: _ResourceRecord) -> Any:
    """Recheck exact ID and full ownership, rejecting changes since inventory discovery."""
    resource = resource_manager(client, record.kind).get(record.resource_id)
    resource.reload()
    if resource.id != record.resource_id or not labels_match(
        resource_labels(resource, record.kind), record.labels
    ):
        raise SandboxDeleteConflict()
    return resource


def discover_sandbox_resources(
    client: DockerClient,
    sandbox_id: str,
    expected_transaction_id: str | None = None,
    *, expected_workspace: WorkspaceBinding | None = None,
    allow_missing_workspace: bool = False,
) -> tuple[_ResourceRecord, ...]:
    """Read all resource roles and verify exact identities and one consistent transaction."""
    records: list[_ResourceRecord] = []
    transaction_ids: set[str] = set()
    for kind in _DELETION_ORDER:
        filters = {
            "io.agent-sandbox.managed": "true",
            "io.agent-sandbox.project": "agent-sandbox",
            "io.agent-sandbox.sandbox-id": sandbox_id,
            "io.agent-sandbox.resource": kind,
        }
        kwargs: dict[str, Any] = {
            "filters": {"label": [f"{key}={value}" for key, value in filters.items()]}
        }
        if kind in {"container", "proxy", "egress"}:
            kwargs["all"] = True
        candidates = resource_manager(client, kind).list(**kwargs)
        if len(candidates) > 1:
            raise SandboxDeleteConflict()
        if not candidates:
            continue
        resource_id = candidates[0].id
        if not isinstance(resource_id, str) or not resource_id:
            raise SandboxDeleteConflict()
        try:
            resource = resource_manager(client, kind).get(resource_id)
            resource.reload()
        except NotFound:
            # A concurrent delete can remove a listed candidate before inspection.
            continue
        labels = resource_labels(resource, kind)
        if resource.id != resource_id or not query_labels_match(labels, sandbox_id, kind):
            raise SandboxDeleteConflict()
        transaction_id = labels["io.agent-sandbox.transaction-id"]
        if (
            expected_transaction_id is not None
            and transaction_id != expected_transaction_id
        ):
            raise SandboxDeleteConflict()
        transaction_ids.add(transaction_id)
        if len(transaction_ids) > 1:
            raise SandboxDeleteConflict()
        records.append(_ResourceRecord(
            kind, resource_id, {**filters, "io.agent-sandbox.transaction-id": transaction_id},
            dict(labels),
        ))
    try:
        bindings = [binding_from_labels(
            item.observed_labels or {}, sandbox_id,
            item.labels["io.agent-sandbox.transaction-id"],
        ) for item in records]
        observed = next((value for value in bindings if value is not None), None)
        if observed is not None and any(value is None or binding_labels(value) != binding_labels(observed) for value in bindings):
            raise SandboxDeleteConflict()
        if expected_workspace is not None:
            if (expected_workspace.sandbox_id != sandbox_id
                or (expected_transaction_id is not None and expected_workspace.transaction_id != expected_transaction_id)
                or (observed is not None and (binding_labels(observed) != binding_labels(expected_workspace)
                    or observed.transaction_id != expected_workspace.transaction_id))
                or (records and observed is None)):
                raise SandboxDeleteConflict()
            observed = expected_workspace
        if observed is not None:
            if any(item.kind == "workspace" for item in records):
                raise SandboxDeleteConflict()
            try:
                volume = verified_volume(client, observed)
            except WorkspaceNotFound:
                # Only destructive instance cleanup tolerates proven absence.
                # Existing foreign or substituted volumes still fail ownership checks.
                if not allow_missing_workspace:
                    raise
                volume = None
            verify_workspace_references(client, workspace_record(observed))
            for item in records:
                if item.kind == "container":
                    verify_workspace_mount(resource_manager(client, item.kind).get(item.resource_id), observed)
            records = [
                _ResourceRecord(item.kind, item.resource_id,
                    {**item.labels, **binding_labels(observed)}, item.observed_labels)
                for item in records
            ]
            if volume is not None:
                records.append(_ResourceRecord("workspace", volume.id, workspace_labels(observed), dict(volume.attrs["Labels"]), True))
    except WorkspaceError as error:
        raise SandboxDeleteConflict() from error
    return tuple(records)


def _is_absent(client: DockerClient, record: _ResourceRecord) -> bool:
    """Confirm disappearance by exact identity instead of trusting a remove response."""
    try:
        _inspect_owned(client, record)
    except NotFound:
        return True
    return False


def _remove_resource(client: DockerClient, record: _ResourceRecord) -> None:
    """Recheck ownership before stopping/removing, accepting races only for confirmed absence."""
    try:
        resource = _inspect_owned(client, record)
    except NotFound:
        return
    force = False
    if record.kind in {"container", "proxy", "egress"}:
        state = resource.attrs.get("State", {})
        if state.get("Paused") is True or state.get("Status") == "paused":
            # Force-delete frozen processes; graceful stop can hang and thawing would run user code.
            force = True
        else:
            try:
                # Docker sends SIGKILL after this grace period; SDK transport is also bounded.
                resource.stop(timeout=2)
            except Exception:
                # Stop failure cannot prove exit; recheck ownership before force removal.
                force = True
        try:
            resource = _inspect_owned(client, record)
        except NotFound:
            return
    try:
        if record.kind in {"container", "proxy", "egress"}:
            resource.remove(force=force, v=False)
        else:
            # Never forcibly disconnect other endpoints or remove a volume still in use.
            resource.remove()
    except Exception:
        # SDK failure may follow daemon-side removal; exact-ID reinspection decides the result.
        if not _is_absent(client, record):
            raise
        return
    if not _is_absent(client, record):
        raise RuntimeError("Docker resource still exists after removal")
