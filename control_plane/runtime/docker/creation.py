"""Run the Docker creation transaction and compensate known partial or late success."""

import json
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

import docker
from docker.errors import APIError, DockerException, ImageNotFound, NotFound
from requests import exceptions as requests_exceptions

from control_plane.domain.errors import (
    CleanupIssue,
    CreationFailureInfo,
    CreationResourceState,
    DockerUnavailable,
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxDeleteConflict,
    SandboxEnvironmentNotRegistered,
    SandboxError,
    SandboxImageUnavailable,
    SandboxResourceConflict,
    WorkspaceError,
)
from control_plane.domain.models import ResourceKind, SandboxSpec, format_utc_datetime
from control_plane.runtime.docker.client import DockerClient
from control_plane.runtime.docker.images import (
    EGRESS_IMAGE,
    ENVIRONMENT_IMAGES,
    PROXY_IMAGE,
    _validate_image_mounts,
)
from control_plane.runtime.docker.resources import (
    ALLOW_DOMAINS_LABEL,
    ENDPOINT_AUTH_LABEL,
    ENDPOINT_SERVICES_LABEL,
    NETWORK_VERSION_LABEL,
    PROXY_AUTH_IMAGE_LABEL,
    PROXY_SERVICES_IMAGE_LABEL,
    _inspect_owned,
    _ResourceRecord,
    labels_match,
    resource_labels,
    resource_manager,
)
from control_plane.runtime.docker.workspaces import (
    binding_labels,
    verified_volume,
    verify_workspace_mount,
    verify_workspace_references,
    workspace_record,
)


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


def _creation_labels(spec: SandboxSpec, role: ResourceKind) -> dict[str, str]:
    """Add precomputed initial lifecycle metadata to complete ownership labels."""
    labels = spec.labels_for(role)
    if spec.workspace is not None:
        labels.update(binding_labels(spec.workspace))
    labels.update({
        ENDPOINT_AUTH_LABEL: "1",
        ENDPOINT_SERVICES_LABEL: "1",
        NETWORK_VERSION_LABEL: "1",
        ALLOW_DOMAINS_LABEL: json.dumps(list(spec.network.allow_domains), separators=(",", ":")),
    })
    if spec.created_at is not None and spec.expires_at is not None:
        labels.update({
            "io.agent-sandbox.created-at": format_utc_datetime(spec.created_at),
            "io.agent-sandbox.expires-at": format_utc_datetime(spec.expires_at),
        })
    return labels


class DockerCreationTransaction:
    """Own synchronous creation and rollback for one injected client factory."""

    def __init__(
        self,
        client_factory: Callable[[], DockerClient],
        *,
        environment_images: Mapping[str, str] | None = None,
    ) -> None:
        """Freeze trusted image configuration and store a lazy factory without connecting."""
        self._client_factory = client_factory
        self._environment_images = MappingProxyType(dict(
            ENVIRONMENT_IMAGES if environment_images is None else environment_images
        ))

    def create_sync(
        self,
        spec: SandboxSpec,
        cancelled: threading.Event,
    ) -> tuple[_ResourceRecord, ...]:
        """Create in dependency order and roll back recorded exact identities on failure."""
        if spec.environment not in self._environment_images:
            raise SandboxEnvironmentNotRegistered()
        if spec.access_token is None:
            raise SandboxCreateFailed()
        selected_image = self._environment_images[spec.environment]
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
                image = client.images.get(selected_image)
                image_id = image.id
                if not isinstance(image_id, str) or not image_id:
                    raise SandboxImageUnavailable()
                _validate_image_mounts(image)
                if spec.workspace is not None:
                    if image_id != spec.workspace.image_id:
                        raise SandboxResourceConflict()
                    verified_volume(client, spec.workspace)
                    verify_workspace_references(client, workspace_record(spec.workspace), require_unused=True)
                proxy_image = client.images.get(PROXY_IMAGE)
                proxy_image_id = proxy_image.id
                if (not isinstance(proxy_image_id, str) or not proxy_image_id
                    or (proxy_image.attrs.get("Config", {}).get("Labels") or {}).get(PROXY_AUTH_IMAGE_LABEL) != "1"
                    or (proxy_image.attrs.get("Config", {}).get("Labels") or {}).get(PROXY_SERVICES_IMAGE_LABEL) != "1"):
                    raise SandboxImageUnavailable()
                egress_image_id = None
                if spec.network.allow_domains:
                    egress_image_id = client.images.get(EGRESS_IMAGE).id
                    if not isinstance(egress_image_id, str) or not egress_image_id:
                        raise SandboxImageUnavailable()
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

                if spec.workspace is not None:
                    # Recheck after network creation; existing data is never created or ledger-owned.
                    image = client.images.get(selected_image)
                    if image.id != spec.workspace.image_id:
                        raise SandboxResourceConflict()
                    _validate_image_mounts(image)
                    verified_volume(client, spec.workspace)
                    verify_workspace_references(client, workspace_record(spec.workspace), require_unused=True)
                else:
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
                    **({"mounts": [docker.types.Mount(
                        target="/workspace", source=spec.volume_name,
                        type="volume", no_copy=True,
                    )]} if spec.workspace is not None else {"binds": {
                        spec.volume_name: {"bind": "/workspace", "mode": "rw"},
                    }}),
                    network_mode=spec.network_name,
                )
                phase = "create_container"
                environment = {}
                if spec.network.allow_domains:
                    environment["environment"] = {
                        "HTTP_PROXY": "http://egress:18084", "http_proxy": "http://egress:18084",
                        "HTTPS_PROXY": "http://egress:18084", "https_proxy": "http://egress:18084",
                        "NO_PROXY": "localhost,127.0.0.1,::1", "no_proxy": "localhost,127.0.0.1,::1",
                    }
                # Always probe the Agent, even when a custom image defines business health.
                # BusyBox wget ignores NO_PROXY; explicitly bypass proxies for local checks.
                environment["healthcheck"] = {
                    "test": ["CMD", "wget", *(["-Y", "off"] if spec.network.allow_domains else []),
                             "-q", "-T", "2", "-O", "/dev/null", "http://127.0.0.1:8080/healthz"],
                    "interval": 2_000_000_000, "timeout": 3_000_000_000,
                    "start_period": 5_000_000_000, "retries": 3,
                }
                container_response = client.api.create_container(
                    # Bind creation to the preflight result, even if its tag moves.
                    image=image_id,
                    entrypoint=["/usr/local/bin/agent"],
                    command=["-listen", "0.0.0.0:8080"],
                    name=spec.container_name,
                    labels=container_labels,
                    user="sandbox",
                    working_dir="/workspace",
                    volumes=["/workspace"],
                    **environment,
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
                if spec.workspace is not None:
                    try:
                        container = _inspect_owned(client, records[-1])
                        verified_volume(client, spec.workspace)
                        verify_workspace_mount(container, spec.workspace)
                    except (WorkspaceError, SandboxDeleteConflict) as error:
                        raise SandboxResourceConflict() from error
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
                    image=proxy_image_id,
                    environment={
                        "SANDBOX_ACCESS_TOKEN": spec.access_token,
                        "SANDBOX_PROXY_AUTH_VERSION": "1",
                    },
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

                if spec.network.allow_domains:
                    egress_labels = _creation_labels(spec, "egress")
                    phase = "configure_egress"
                    egress_host_config = client.api.create_host_config(
                        nano_cpus=500_000_000, mem_limit=128 * 1024 * 1024,
                        memswap_limit=128 * 1024 * 1024, pids_limit=32,
                        cap_drop=["ALL"], security_opt=["no-new-privileges:true"],
                        read_only=True, network_mode=spec.ingress_network_name,
                    )
                    arguments = [argument for domain in spec.network.allow_domains
                                 for argument in ("-allow-domain", domain)]
                    phase = "create_egress"
                    egress_response = client.api.create_container(
                        image=egress_image_id, name=spec.egress_name,
                        labels=egress_labels, user="10001:10001", command=arguments,
                        host_config=egress_host_config,
                        networking_config={"EndpointsConfig": {spec.ingress_network_name: {}}},
                    )
                    egress_id = egress_response.get("Id")
                    if not isinstance(egress_id, str) or not egress_id:
                        raise SandboxCleanupFailed(spec.sandbox_id)
                    records.append(_ResourceRecord("egress", egress_id, egress_labels))
                    self._raise_if_cancelled(cancelled)
                    phase = "connect_egress"
                    client.api.connect_container_to_network(
                        egress_id, network_ids["network"], aliases=["egress"],
                    )
                    self._raise_if_cancelled(cancelled)
                    phase = "start_egress"
                    client.containers.get(egress_id).start()
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
        if isinstance(error, WorkspaceError):
            return SandboxResourceConflict()
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
            if record.external:
                continue
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
                # Feature markers describe completeness; ownership alone authorizes cleanup.
                ownership_labels = {
                    key: value for key, value in record.labels.items()
                    if key not in (ENDPOINT_AUTH_LABEL, ENDPOINT_SERVICES_LABEL)
                }
                if not labels_match(actual_labels, ownership_labels):
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
                if record.kind in {"container", "proxy", "egress"}:
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
