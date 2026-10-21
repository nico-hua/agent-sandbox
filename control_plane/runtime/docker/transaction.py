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
    CleanupIssue,
    CreationFailureInfo,
    CreationResourceState,
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxError,
    SandboxImageUnavailable,
    SandboxResourceConflict,
)
from control_plane.core.models import ResourceKind, SandboxSpec, format_utc_datetime


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


def _creation_labels(spec: SandboxSpec, role: ResourceKind) -> dict[str, str]:
    """Add precomputed initial lifecycle metadata to complete ownership labels."""
    labels = spec.labels_for(role)
    if spec.created_at is not None and spec.expires_at is not None:
        labels.update({
            "io.agent-sandbox.created-at": format_utc_datetime(spec.created_at),
            "io.agent-sandbox.expires-at": format_utc_datetime(spec.expires_at),
        })
    return labels


@dataclass(frozen=True)
class _ResourceRecord:
    """Track exact identity and ownership for a created or recovered resource."""

    kind: ResourceKind
    resource_id: str
    labels: dict[str, str]


class CreationCancelled(Exception):
    """Signal that a worker observed cancellation between Docker steps."""


@dataclass(frozen=True)
class _RollbackReport:
    """Summarize verified absence, leftovers and sanitized cleanup diagnostics."""

    resource_state: CreationResourceState
    cleanup_issues: tuple[CleanupIssue, ...] = ()


def _cleanup_error_type(error: Exception) -> str:
    """Map backend exceptions to a bounded set of diagnostic type names."""
    for error_class in (
        NotFound, APIError, DockerException, requests_exceptions.RequestException,
        TimeoutError, RuntimeError, ValueError, TypeError,
    ):
        if isinstance(error, error_class):
            return error_class.__name__
    return "Exception"


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
        """Create in dependency order and roll back recorded exact identities on failure."""
        try:
            client = self._client_factory()
        except Exception as error:
            unavailable = DockerUnavailable()
            unavailable.creation_failure = CreationFailureInfo(
                "open_client", CreationResourceState.UNKNOWN,
            )
            raise unavailable from error

        records: list[_ResourceRecord] = []
        primary_error: Exception | None = None
        phase = "check_images"
        try:
            try:
                client.images.get(SANDBOX_IMAGE)
                client.images.get(PROXY_IMAGE)
                self._raise_if_cancelled(cancelled)

                # Networks and workspace precede sandbox; proxy needs both networks and sandbox.
                # Record raw create IDs before inspect/start can fail and lose a rollback target.
                network_ids: dict[str, str] = {}
                for role, name, internal in [
                    ("network", spec.network_name, True),
                    ("ingress", spec.ingress_network_name, False),
                ]:
                    network_labels = _creation_labels(spec, role)
                    phase = f"create_{role}"
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

                volume_labels = _creation_labels(spec, "workspace")
                phase = "create_workspace"
                volume_response = client.api.create_volume(
                    spec.volume_name,
                    labels=volume_labels,
                )
                volume_id = volume_response.get("Name")
                actual_volume_labels = volume_response.get("Labels")
                if not isinstance(volume_id, str) or not volume_id:
                    raise SandboxCleanupFailed(spec.sandbox_id)
                # Volume create may reuse a name; record only after full ownership matches.
                if not labels_match(actual_volume_labels, volume_labels):
                    raise SandboxResourceConflict()
                records.append(
                    _ResourceRecord("workspace", volume_id, volume_labels)
                )
                self._raise_if_cancelled(cancelled)

                container_labels = _creation_labels(spec, "container")
                phase = "configure_container"
                host_config = client.api.create_host_config(
                    init=True,
                    nano_cpus=spec.resources.cpu_millis * 1_000_000,
                    mem_limit=spec.resources.memory_mib * 1024 * 1024,
                    memswap_limit=spec.resources.memory_mib * 1024 * 1024,
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
                phase = "create_container"
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

                phase = "start_container"
                container = client.containers.get(container_id)
                container.start()
                self._raise_if_cancelled(cancelled)

                # Proxy joins ingress for entry, then the private network to reach sandbox.
                proxy_labels = _creation_labels(spec, "proxy")
                phase = "configure_proxy"
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
                    # Let Docker allocate a local port atomically, avoiding a probe-then-bind race.
                    port_bindings={8080: ("127.0.0.1", None)},
                )
                phase = "create_proxy"
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
                phase = "connect_proxy"
                client.api.connect_container_to_network(
                    proxy_id, network_ids["network"]
                )
                self._raise_if_cancelled(cancelled)
                phase = "start_proxy"
                proxy = client.containers.get(proxy_id)
                proxy.start()
                self._raise_if_cancelled(cancelled)
                return tuple(records)
            except Exception as error:
                if isinstance(error, SandboxCleanupFailed):
                    # Missing create identity stays uncertain despite concurrent cancellation.
                    classified: Exception = error
                elif (
                    cancelled.is_set()
                    and isinstance(error, (
                        DockerException, requests_exceptions.RequestException,
                    ))
                    and not isinstance(error, APIError)
                ):
                    # Transport failure may hide a committed create without a recorded ID.
                    # Cancellation cannot establish whether that resource was rolled back.
                    classified = SandboxCleanupFailed(spec.sandbox_id)
                elif isinstance(error, CreationCancelled) or cancelled.is_set():
                    classified = CreationCancelled()
                else:
                    classified = self._classify_error(error)
                report = self._rollback(client, records)
                state = report.resource_state
                if isinstance(classified, (DockerUnavailable, SandboxCleanupFailed)):
                    # Complete known-ID cleanup cannot rule out a create whose response was lost.
                    state = CreationResourceState.UNKNOWN
                elif not records and state == CreationResourceState.ROLLED_BACK:
                    state = CreationResourceState.UNCHANGED
                if report.resource_state in {
                    CreationResourceState.REMAINING, CreationResourceState.UNKNOWN,
                }:
                    if classified is not error:
                        classified.__cause__ = error
                    primary_error = SandboxCleanupFailed(spec.sandbox_id)
                    primary_error.creation_failure = CreationFailureInfo(
                        phase, state, report.cleanup_issues,
                    )
                    raise primary_error from classified
                primary_error = classified
                if isinstance(classified, SandboxError):
                    classified.creation_failure = CreationFailureInfo(
                        phase, state, report.cleanup_issues,
                    )
                raise classified from error
        finally:
            try:
                client.close()
            except Exception as error:
                issue = CleanupIssue("client", "close", _cleanup_error_type(error))
                if primary_error is None:
                    # Close failed after creation; retain recovery data instead of reporting success.
                    cleanup_error = SandboxCleanupFailed(spec.sandbox_id)
                    cleanup_error.creation_failure = CreationFailureInfo(
                        "close_client", CreationResourceState.UNKNOWN, (issue,),
                    )
                    raise cleanup_error from error
                if isinstance(primary_error, SandboxError):
                    # Close diagnostics must not replace the primary failure or its resource state.
                    info = primary_error.creation_failure
                    if info is not None:
                        primary_error.creation_failure = CreationFailureInfo(
                            info.phase, info.resource_state,
                            (*info.cleanup_issues, issue),
                        )

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
    ) -> _RollbackReport:
        """Roll back in reverse order, verifying exact identity, ownership and final absence."""
        issues: list[CleanupIssue] = []
        remaining = False
        unknown = False
        # Remove containers before volumes/networks; keep cleaning after an item fails.
        for record in reversed(records):
            try:
                resource = resource_manager(client, record.kind).get(
                    record.resource_id
                )
                resource.reload()
                if resource.id != record.resource_id:
                    remaining = True
                    issues.append(CleanupIssue(record.kind, "verify_ownership", "IdentityMismatch"))
                    continue
                actual_labels = resource_labels(resource, record.kind)
                if not labels_match(actual_labels, record.labels):
                    remaining = True
                    issues.append(CleanupIssue(record.kind, "verify_ownership", "OwnershipMismatch"))
                    continue
            except NotFound:
                # This exact ID is already absent; never seek a same-name replacement to remove.
                continue
            except Exception as error:
                unknown = True
                issues.append(CleanupIssue(record.kind, "inspect", _cleanup_error_type(error)))
                continue
            try:
                if record.kind in {"container", "proxy"}:
                    resource.remove(force=True)
                else:
                    resource.remove()
            except Exception as error:
                issues.append(CleanupIssue(record.kind, "remove", _cleanup_error_type(error)))
            # Neither success nor failure of remove proves absence; inspect the same exact ID again.
            try:
                survivor = resource_manager(client, record.kind).get(record.resource_id)
                survivor.reload()
            except NotFound:
                continue
            except Exception as error:
                unknown = True
                issues.append(CleanupIssue(record.kind, "verify_removal", _cleanup_error_type(error)))
                continue
            remaining = True
            issues.append(CleanupIssue(record.kind, "verify_removal", "ResourceRemaining"))
        # Unverifiable inspection takes precedence over known leftovers or confirmed absence.
        state = (
            CreationResourceState.UNKNOWN if unknown
            else CreationResourceState.REMAINING if remaining
            else CreationResourceState.ROLLED_BACK
        )
        return _RollbackReport(state, tuple(issues))

    def rollback_committed(
        self,
        spec: SandboxSpec,
        records: tuple[_ResourceRecord, ...],
    ) -> None:
        """Compensate late cancellation by rechecking and rolling back the completed creation."""
        try:
            client = self._client_factory()
        except Exception as error:
            cleanup_error = SandboxCleanupFailed(spec.sandbox_id)
            cleanup_error.creation_failure = CreationFailureInfo(
                "rollback_committed", CreationResourceState.UNKNOWN,
                (CleanupIssue("client", "open", _cleanup_error_type(error)),),
            )
            raise cleanup_error from error

        report = _RollbackReport(CreationResourceState.UNKNOWN)
        primary_error: Exception | None = None
        try:
            report = self._rollback(client, list(records))
            if report.resource_state != CreationResourceState.ROLLED_BACK:
                cleanup_error = SandboxCleanupFailed(spec.sandbox_id)
                cleanup_error.creation_failure = CreationFailureInfo(
                    "rollback_committed", report.resource_state, report.cleanup_issues,
                )
                raise cleanup_error
        except Exception as error:
            primary_error = error
            raise
        finally:
            try:
                client.close()
            except Exception as error:
                issue = CleanupIssue("client", "close", _cleanup_error_type(error))
                if primary_error is None:
                    cleanup_error = SandboxCleanupFailed(spec.sandbox_id)
                    cleanup_error.creation_failure = CreationFailureInfo(
                        # Keep UNKNOWN on close failure even after verified absence.
                        "rollback_committed", CreationResourceState.UNKNOWN,
                        (*report.cleanup_issues, issue),
                    )
                    raise cleanup_error from error
                if isinstance(primary_error, SandboxError):
                    primary_error.creation_failure = CreationFailureInfo(
                        "rollback_committed", report.resource_state,
                        (*report.cleanup_issues, issue),
                    )

    def _raise_if_cancelled(self, cancelled: threading.Event) -> None:
        """Stop the transaction between Docker operations when cancelled."""
        if cancelled.is_set():
            raise CreationCancelled()


_DELETION_ORDER: tuple[ResourceKind, ...] = (
    "proxy", "container", "ingress", "network", "workspace"
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
    """Recheck ownership before stopping/removing, accepting races only for confirmed absence."""
    try:
        resource = _inspect_owned(client, record)
    except NotFound:
        return
    force = False
    if record.kind in {"container", "proxy"}:
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
        if record.kind in {"container", "proxy"}:
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


def delete_sandbox_sync(
    client_factory: Callable[[], DockerClient],
    sandbox_id: str,
    *,
    delete_workspace: bool = False,
    expected_transaction_id: str | None = None,
) -> None:
    """Preflight and reclaim resources, verify absence, and retain workspace by default."""
    try:
        client = client_factory()
    except Exception as error:
        raise DockerUnavailable() from error

    completed = False
    try:
        try:
            records = discover_sandbox_resources(client, sandbox_id, expected_transaction_id)
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
                # Stop destructive work immediately if ownership changes after discovery.
                raise
            except Exception:
                # Continue verified items; retries reclaim leftovers without restoring removals.
                failed_records.append(record)

        failed = False
        for record in failed_records:
            try:
                # A peer may finish a failed item; only its exact-ID absence clears the failure.
                # A label rescan alone could hide a leftover whose ownership changed.
                if not _is_absent(client, record):
                    failed = True
            except SandboxDeleteConflict:
                raise
            except Exception:
                failed = True

        # Rescan for overlooked owned resources; a workspace retained by default is not a failure.
        try:
            remaining = discover_sandbox_resources(client, sandbox_id, expected_transaction_id)
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
