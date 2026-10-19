"""Synchronous Docker resource lifecycle, ownership checks and creation rollback."""

import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

import docker
from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from requests import exceptions as requests_exceptions

from control_plane.core.errors import (
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxError,
    SandboxImageUnavailable,
    SandboxResourceConflict,
)
from control_plane.core.models import ResourceKind, SandboxSpec


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


def resource_manager(client: DockerClient, kind: ResourceKind) -> Any:
    """Select the exact-ID Docker manager for one recorded kind."""
    if kind in {"container", "proxy"}:
        return client.containers
    if kind == "workspace":
        return client.volumes
    return client.networks


def resource_labels(resource: Any, kind: ResourceKind) -> dict[str, str]:
    """Read Docker labels from the shape used by each resource type."""
    attrs = resource.attrs
    if not isinstance(attrs, dict):
        return {}
    if kind in {"container", "proxy"}:
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


SANDBOX_IMAGE = "agent-sandbox:dev"
PROXY_IMAGE = "agent-sandbox-proxy:dev"


@dataclass(frozen=True)
class _ResourceRecord:
    """Track exact identity and ownership for a created or recovered resource."""

    kind: ResourceKind
    resource_id: str
    labels: dict[str, str]


class CreationCancelled(Exception):
    """Signal that a worker observed cancellation between Docker steps."""


class DockerCreationTransaction:
    """Own synchronous creation and rollback for one injected client factory."""

    def __init__(
        self,
        client_factory: Callable[[], DockerClient],
    ) -> None:
        """Store a lazy Docker client factory without connecting."""
        self._client_factory = client_factory

    def create_sync(
        self,
        spec: SandboxSpec,
        cancelled: threading.Event,
    ) -> tuple[_ResourceRecord, ...]:
        """Create and start fixed Docker resources in transaction order."""
        try:
            client = self._client_factory()
        except Exception as error:
            raise DockerUnavailable() from error

        records: list[_ResourceRecord] = []
        try:
            try:
                client.images.get(SANDBOX_IMAGE)
                client.images.get(PROXY_IMAGE)
                self._raise_if_cancelled(cancelled)

                network_ids: dict[str, str] = {}
                for role, name, internal in [
                    ("network", spec.network_name, True),
                    ("ingress", spec.ingress_network_name, False),
                ]:
                    network_labels = spec.labels_for(role)
                    network_response = client.api.create_network(
                        name,
                        driver="bridge",
                        internal=internal,
                        labels=network_labels,
                        check_duplicate=True,
                    )
                    network_id = network_response.get("Id")
                    if not isinstance(network_id, str) or not network_id:
                        raise SandboxCleanupFailed(spec.sandbox_id)
                    records.append(_ResourceRecord(role, network_id, network_labels))
                    network_ids[role] = network_id
                    self._raise_if_cancelled(cancelled)

                volume_labels = spec.labels_for("workspace")
                volume_response = client.api.create_volume(
                    spec.volume_name,
                    labels=volume_labels,
                )
                volume_id = volume_response.get("Name")
                actual_volume_labels = volume_response.get("Labels")
                if not isinstance(volume_id, str) or not volume_id:
                    raise SandboxCleanupFailed(spec.sandbox_id)
                if not labels_match(actual_volume_labels, volume_labels):
                    raise SandboxResourceConflict()
                records.append(
                    _ResourceRecord("workspace", volume_id, volume_labels)
                )
                self._raise_if_cancelled(cancelled)

                container_labels = spec.labels_for("container")
                host_config = client.api.create_host_config(
                    init=True,
                    nano_cpus=1_000_000_000,
                    mem_limit="256m",
                    memswap_limit="256m",
                    pids_limit=64,
                    cap_drop=["ALL"],
                    security_opt=["no-new-privileges:true"],
                    read_only=True,
                    tmpfs={"/tmp": "rw,nosuid,nodev,size=32m,mode=1777"},
                    binds={
                        spec.volume_name: {
                            "bind": "/workspace",
                            "mode": "rw",
                        }
                    },
                    network_mode=spec.network_name,
                )
                container_response = client.api.create_container(
                    image=SANDBOX_IMAGE,
                    name=spec.container_name,
                    labels=container_labels,
                    user="sandbox",
                    working_dir="/workspace",
                    volumes=["/workspace"],
                    host_config=host_config,
                    networking_config={
                        "EndpointsConfig": {
                            spec.network_name: {"Aliases": ["sandbox"]}
                        }
                    },
                )
                container_id = container_response.get("Id")
                if not isinstance(container_id, str) or not container_id:
                    raise SandboxCleanupFailed(spec.sandbox_id)
                records.append(
                    _ResourceRecord(
                        "container",
                        container_id,
                        container_labels,
                    )
                )
                self._raise_if_cancelled(cancelled)

                container = client.containers.get(container_id)
                container.start()
                self._raise_if_cancelled(cancelled)

                proxy_labels = spec.labels_for("proxy")
                proxy_host_config = client.api.create_host_config(
                    init=True,
                    nano_cpus=500_000_000,
                    mem_limit="64m",
                    memswap_limit="64m",
                    pids_limit=32,
                    cap_drop=["ALL"],
                    security_opt=["no-new-privileges:true"],
                    read_only=True,
                    tmpfs={"/tmp": "rw,nosuid,nodev,size=16m,mode=1777"},
                    network_mode=spec.ingress_network_name,
                    # An empty host port asks Docker to allocate without a preflight race.
                    port_bindings={8080: ("127.0.0.1", None)},
                )
                proxy_response = client.api.create_container(
                    image=PROXY_IMAGE,
                    name=spec.proxy_name,
                    labels=proxy_labels,
                    user="nginx",
                    ports=[8080],
                    host_config=proxy_host_config,
                    networking_config={
                        "EndpointsConfig": {spec.ingress_network_name: {}}
                    },
                )
                proxy_id = proxy_response.get("Id")
                if not isinstance(proxy_id, str) or not proxy_id:
                    raise SandboxCleanupFailed(spec.sandbox_id)
                records.append(_ResourceRecord("proxy", proxy_id, proxy_labels))
                self._raise_if_cancelled(cancelled)
                client.api.connect_container_to_network(
                    proxy_id, network_ids["network"]
                )
                self._raise_if_cancelled(cancelled)
                proxy = client.containers.get(proxy_id)
                proxy.start()
                self._raise_if_cancelled(cancelled)
                return tuple(records)
            except Exception as error:
                if isinstance(error, CreationCancelled) or cancelled.is_set():
                    classified: Exception = CreationCancelled()
                else:
                    classified = self._classify_error(error)
                cleanup_failed = self._rollback(client, records)
                if cleanup_failed:
                    try:
                        raise classified from error
                    except Exception as mapped:
                        raise SandboxCleanupFailed(spec.sandbox_id) from mapped
                raise classified from error
        finally:
            client.close()

    def _classify_error(self, error: Exception) -> SandboxError:
        """Map Docker failures to stable sandbox domain errors."""
        if isinstance(error, SandboxError):
            return error
        if isinstance(error, ImageNotFound):
            return SandboxImageUnavailable()
        if isinstance(error, APIError):
            if error.status_code == 409:
                return SandboxResourceConflict()
            return SandboxCreateFailed()
        if isinstance(error, (DockerException, requests_exceptions.RequestException)):
            return DockerUnavailable()
        return SandboxCreateFailed()

    def _rollback(
        self,
        client: DockerClient,
        records: list[_ResourceRecord],
    ) -> bool:
        """Remove all safely owned resources and report cleanup failures."""
        failed = False
        for record in reversed(records):
            try:
                resource = resource_manager(client, record.kind).get(
                    record.resource_id
                )
                resource.reload()
                if resource.id != record.resource_id:
                    failed = True
                    continue
                actual_labels = resource_labels(resource, record.kind)
                if not labels_match(actual_labels, record.labels):
                    failed = True
                    continue
                if record.kind in {"container", "proxy"}:
                    resource.remove(force=True)
                else:
                    resource.remove()
            except Exception:
                failed = True
        return failed

    def rollback_committed(
        self,
        spec: SandboxSpec,
        records: tuple[_ResourceRecord, ...],
    ) -> None:
        """Reopen committed resources by exact ID and remove them safely."""
        try:
            client = self._client_factory()
        except Exception as error:
            raise SandboxCleanupFailed(spec.sandbox_id) from error

        try:
            failed = self._rollback(client, list(records))
        finally:
            client.close()

        if failed:
            raise SandboxCleanupFailed(spec.sandbox_id)

    def _raise_if_cancelled(self, cancelled: threading.Event) -> None:
        """Stop the transaction between Docker operations when cancelled."""
        if cancelled.is_set():
            raise CreationCancelled()


_DELETION_ORDER: tuple[ResourceKind, ...] = (
    "proxy", "container", "ingress", "network", "workspace"
)


def _inspect_owned(client: DockerClient, record: _ResourceRecord) -> Any:
    """Reopen an exact resource and reject identity or ownership changes."""
    resource = resource_manager(client, record.kind).get(record.resource_id)
    resource.reload()
    if resource.id != record.resource_id or not labels_match(
        resource_labels(resource, record.kind), record.labels
    ):
        raise SandboxDeleteConflict()
    return resource


def _discover_resources(
    client: DockerClient, sandbox_id: str
) -> tuple[_ResourceRecord, ...]:
    """Validate the entire surviving inventory before any destructive operation."""
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
        if kind in {"container", "proxy"}:
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
            # A concurrent delete may have removed a listed resource already.
            continue
        labels = resource_labels(resource, kind)
        if resource.id != resource_id or not query_labels_match(labels, sandbox_id, kind):
            raise SandboxDeleteConflict()
        transaction_id = labels["io.agent-sandbox.transaction-id"]
        transaction_ids.add(transaction_id)
        if len(transaction_ids) > 1:
            raise SandboxDeleteConflict()
        records.append(_ResourceRecord(
            kind, resource_id, {**filters, "io.agent-sandbox.transaction-id": transaction_id}
        ))
    return tuple(records)


def _is_absent(client: DockerClient, record: _ResourceRecord) -> bool:
    """Confirm disappearance by exact identity instead of trusting a remove response."""
    try:
        _inspect_owned(client, record)
    except NotFound:
        return True
    return False


def _remove_resource(client: DockerClient, record: _ResourceRecord) -> None:
    """Stop and remove one owned resource, accepting races only after reinspection."""
    try:
        resource = _inspect_owned(client, record)
    except NotFound:
        return
    force = False
    if record.kind in {"container", "proxy"}:
        try:
            # Docker sends SIGKILL after this grace period; transport is also bounded.
            resource.stop(timeout=2)
        except Exception:
            # A failed/timed-out stop does not imply the daemon stopped the process.
            force = True
        try:
            resource = _inspect_owned(client, record)
        except NotFound:
            return
    try:
        if record.kind in {"container", "proxy"}:
            resource.remove(force=force, v=False)
        else:
            # Never disconnect foreign endpoints or force an in-use volume removal.
            resource.remove()
    except Exception:
        # Even a failed SDK response may follow a successful daemon-side removal.
        if not _is_absent(client, record):
            raise
        return
    if not _is_absent(client, record):
        raise RuntimeError("Docker resource still exists after removal")


def delete_sandbox_sync(
    client_factory: Callable[[], DockerClient],
    sandbox_id: str,
    *,
    delete_workspace: bool = False,
) -> None:
    """Discover, verify and reclaim an instance, closing the finite-timeout client."""
    try:
        client = client_factory()
    except Exception as error:
        raise DockerUnavailable() from error

    completed = False
    try:
        try:
            records = _discover_resources(client, sandbox_id)
        except SandboxDeleteConflict:
            raise
        except Exception as error:
            raise DockerUnavailable() from error

        failed_records: list[_ResourceRecord] = []
        for record in records:
            if record.kind == "workspace" and not delete_workspace:
                continue
            try:
                _remove_resource(client, record)
            except SandboxDeleteConflict:
                # Once identity becomes untrustworthy, stop destructive work.
                raise
            except Exception:
                # Other owned leftovers can still be reclaimed; a retry rediscovers
                # the failed item without restoring anything already removed.
                failed_records.append(record)

        failed = False
        for record in failed_records:
            try:
                # A peer may finish a resource after our first failed attempt.
                # Only exact-ID absence can clear that failure: a new label scan
                # alone could hide a resource whose ownership changed meanwhile.
                if not _is_absent(client, record):
                    failed = True
            except SandboxDeleteConflict:
                raise
            except Exception:
                failed = True

        try:
            remaining = _discover_resources(client, sandbox_id)
        except SandboxDeleteConflict:
            raise
        except Exception as error:
            raise SandboxDeleteFailed(sandbox_id) from error
        if failed or any(
            record.kind != "workspace" or delete_workspace for record in remaining
        ):
            raise SandboxDeleteFailed(sandbox_id)
        completed = True
    finally:
        try:
            client.close()
        except Exception as error:
            if completed:
                raise SandboxDeleteFailed(sandbox_id) from error
