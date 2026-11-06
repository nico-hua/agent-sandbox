"""Lazy Kubernetes readiness, primary Pod queries and exact single-Pod deletion."""

import asyncio
import re
from collections.abc import Callable
from pathlib import Path
from typing import Protocol, TypeVar
from urllib.parse import urlsplit

import yaml
from kubernetes import client
from kubernetes.config import ConfigException
from kubernetes.config.kube_config import KubeConfigLoader
from urllib3 import exceptions as transport_errors

from control_plane.core.errors import (
    KubernetesConfigurationError,
    KubernetesConnectionFailed,
    KubernetesError,
    KubernetesOperationUnsupported,
    KubernetesPermissionDenied,
    KubernetesTimeout,
    SandboxError,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxNotFound,
    SandboxQueryFailed,
)
from control_plane.core.models import (
    SandboxEndpoint,
    SandboxQueryResult,
    SandboxResourceInspection,
    SandboxSpec,
    WorkspaceBinding,
    WorkspaceRecord,
)
from control_plane.core.tasks import settle_task
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.runtime.kubernetes.pod_query import (
    pod_identity, primary_pod_selector, query_result, sandbox_pod_name,
)
from control_plane.runtime.kubernetes.pod_delete import PodDeletion

_Result = TypeVar("_Result")


class CloseableClient(Protocol):
    """Describe ownership of one acquired SDK connection pool."""

    def close(self) -> None:
        """Close the acquired client's resources in its owning worker."""
        ...


class ProbeKubernetesClient(Protocol):
    """Restrict dependencies to bounded CoreV1 operations and the owned client handle."""

    api_client: CloseableClient

    def list_namespaced_pod(
        self, namespace: str, *, limit: int, watch: bool,
        _request_timeout: tuple[float, float], label_selector: str = "",
    ) -> object:
        """Read at most one page without watching or retrying."""
        ...

    def read_namespaced_pod(
        self, name: str, namespace: str, *, _request_timeout: tuple[float, float],
    ) -> object:
        """Read one exact-name Pod after bounded candidate discovery."""
        ...

    def delete_namespaced_pod(self, name: str, namespace: str, *, body: client.V1DeleteOptions, _request_timeout: tuple[float, float]) -> object:
        """Delete one exact Pod with caller-provided identity preconditions."""
        ...

    def read_namespace(self, name: str, *, _request_timeout: tuple[float, float]) -> object:
        """Confirm the target namespace exists before interpreting a Pod 404 as cleanup."""
        ...


def _selected_entry(document: dict, section: str, name: str, key: str) -> dict:
    """Select a unique named kubeconfig entry without consulting current-context."""
    entries = document[section]
    if not isinstance(entries, list):
        raise KubernetesConfigurationError()
    matches = [entry[key] for entry in entries if entry["name"] == name]
    if len(matches) != 1 or not isinstance(matches[0], dict):
        raise KubernetesConfigurationError()
    return matches[0]


def _load_configuration(kubeconfig_path: Path, context: str) -> client.Configuration:
    """Load one explicit static-certificate context into a private, proxy-free SDK configuration."""
    try:
        with kubeconfig_path.open(encoding="utf-8") as source:
            document = yaml.safe_load(source)
        if not isinstance(document, dict):
            raise KubernetesConfigurationError()
        selected = _selected_entry(document, "contexts", context, "context")
        user = _selected_entry(document, "users", selected["user"], "user")
        cluster = _selected_entry(document, "clusters", selected["cluster"], "cluster")
        # Validate before invoking the SDK loader: it can otherwise execute an
        # exec/auth-provider plugin or read token files even for a probe.
        certificate_fields = {"client-certificate", "client-certificate-data", "client-key", "client-key-data"}
        if set(user) - certificate_fields:
            raise KubernetesConfigurationError()
        if not (user.get("client-certificate") or user.get("client-certificate-data")):
            raise KubernetesConfigurationError()
        if not (user.get("client-key") or user.get("client-key-data")):
            raise KubernetesConfigurationError()
        server = urlsplit(cluster["server"])
        if (
            server.scheme != "https" or not server.hostname or server.username is not None
            or server.password is not None or server.query or server.fragment
            or cluster.get("insecure-skip-tls-verify", False) is not False
            or "proxy-url" in cluster
            or not (cluster.get("certificate-authority") or cluster.get("certificate-authority-data"))
        ):
            raise KubernetesConfigurationError()
        configuration = client.Configuration()
        # Use the setter before loading host so the SDK cannot re-resolve
        # HTTP_PROXY/HTTPS_PROXY when the selected API URL is assigned.
        configuration.proxy = None
        configuration.no_proxy = "*"
        configuration.retries = False
        configuration.client_go_retries = False
        configuration.debug = False
        KubeConfigLoader(
            document, active_context=context, config_base_path=str(kubeconfig_path.parent),
        ).load_and_set(configuration)
        return configuration
    except Exception:
        raise KubernetesConfigurationError() from None


def _close_client(api_client: CloseableClient, *, primary_failed: bool) -> None:
    """Close on the owning thread while preserving any prior operation failure."""
    try:
        api_client.close()
    except Exception:
        if not primary_failed:
            raise KubernetesConnectionFailed() from None


def create_probe_client(*, kubeconfig_path: Path, context: str) -> ProbeKubernetesClient:
    """Create a private SDK client only after explicit configuration validates successfully."""
    configuration = _load_configuration(kubeconfig_path, context)
    api_client = client.ApiClient(configuration=configuration)
    try:
        return client.CoreV1Api(api_client)
    except Exception:
        _close_client(api_client, primary_failed=True)
        raise


def _probe_failure(error: Exception) -> SandboxError:
    """Classify SDK and transport failures without using messages, headers or response bodies."""
    if isinstance(error, (KubernetesError, SandboxNotFound, SandboxQueryFailed, SandboxDeleteConflict, SandboxDeleteFailed)):
        return error
    if isinstance(error, ConfigException):
        return KubernetesConfigurationError()
    if isinstance(error, client.ApiException):
        if error.status in {401, 403}:
            return KubernetesPermissionDenied()
        if error.status in {408, 504}:
            return KubernetesTimeout()
    if isinstance(error, transport_errors.MaxRetryError):
        # urllib3 may wrap its final transport failure even with no retries.
        return _probe_failure(error.reason) if isinstance(error.reason, Exception) else KubernetesConnectionFailed()
    if isinstance(error, (TimeoutError, transport_errors.TimeoutError)):
        return KubernetesTimeout()
    return KubernetesConnectionFailed()


class KubernetesSandboxRuntime(SandboxRuntime):
    """Probe, query and safely delete a primary Pod in one explicitly configured namespace."""

    def __init__(
        self, *, kubeconfig_path: str | Path, context: str, namespace: str,
        client_factory: Callable[..., ProbeKubernetesClient] | None = None,
    ) -> None:
        """Validate inert target values and retain a lazy client factory without any file or network I/O."""
        if not isinstance(kubeconfig_path, (str, Path)) or not str(kubeconfig_path):
            raise KubernetesConfigurationError()
        path = str(kubeconfig_path)
        if path != path.strip() or any(ord(char) < 32 or ord(char) == 127 for char in path):
            raise KubernetesConfigurationError()
        if (
            not isinstance(context, str) or not 1 <= len(context) <= 253
            or not context.isascii() or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in context)
            or not isinstance(namespace, str)
            or re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", namespace) is None
        ):
            raise KubernetesConfigurationError()
        self._kubeconfig_path = Path(kubeconfig_path)
        self._context = context
        self._namespace = namespace
        self._client_factory = create_probe_client if client_factory is None else client_factory

    async def ping(self) -> None:
        """Wait through cancellation until the probe worker has completed and closed its own client."""
        worker = asyncio.create_task(asyncio.to_thread(self._ping_and_close))
        await settle_task(worker, propagate_cancellation=True)

    def _ping_and_close(self) -> None:
        """Perform exactly one bounded namespace probe without following pagination."""
        self._call_and_close(lambda api: api.list_namespaced_pod(
            self._namespace, limit=1, watch=False, _request_timeout=(0.5, 0.5),
        ))

    def _call_and_close(self, operation: Callable[[ProbeKubernetesClient], _Result]) -> _Result:
        """Share worker-owned acquisition, typed error conversion and close priority across operations."""
        api = None
        primary_failed = True
        try:
            api = self._client_factory(kubeconfig_path=self._kubeconfig_path, context=self._context)
            result = operation(api)
            primary_failed = False
            return result
        except Exception as error:
            raise _probe_failure(error) from None
        finally:
            if api is not None:
                _close_client(api.api_client, primary_failed=primary_failed)

    async def create(self, spec: SandboxSpec) -> None:
        """Reject creation with UNCHANGED evidence because no Kubernetes resource was touched."""
        raise KubernetesOperationUnsupported()

    async def resolve_environment_image(self, environment: str) -> str:
        """Reject image resolution until Kubernetes resource support is implemented."""
        raise KubernetesOperationUnsupported()

    async def create_workspace(self, record: WorkspaceRecord) -> None:
        """Reject workspace creation without accessing storage or the cluster."""
        raise KubernetesOperationUnsupported()

    async def inspect_workspace(self, record: WorkspaceRecord) -> None:
        """Reject workspace inspection without inventing backend facts."""
        raise KubernetesOperationUnsupported()

    async def delete_workspace(self, record: WorkspaceRecord) -> None:
        """Reject workspace deletion without accessing the cluster."""
        raise KubernetesOperationUnsupported()

    async def pause(self, sandbox_id: str, *, expected_transaction_id: str, expected_workspace: WorkspaceBinding | None = None) -> None:
        """Reject freezing without changing any workload or lifecycle state."""
        raise KubernetesOperationUnsupported()

    async def resume(self, sandbox_id: str, *, expected_transaction_id: str, expected_workspace: WorkspaceBinding | None = None) -> None:
        """Reject thawing without creating or restarting any workload."""
        raise KubernetesOperationUnsupported()

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
        expected_workspace: WorkspaceBinding | None = None,
    ) -> None:
        """Settle exact single-Pod deletion before propagating cancellation or releasing its client."""
        sandbox_pod_name(sandbox_id)
        if expected_transaction_id is not None and (
            type(expected_transaction_id) is not str
            or re.fullmatch(r"txn_[0-9a-f]{32}", expected_transaction_id) is None
        ):
            raise SandboxDeleteConflict()
        if delete_workspace or expected_workspace is not None:
            raise KubernetesOperationUnsupported()
        worker = asyncio.create_task(asyncio.to_thread(
            self._call_and_close,
            lambda api: PodDeletion(api, sandbox_id, self._namespace, expected_transaction_id).run(),
        ))
        await settle_task(worker, propagate_cancellation=True)

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Validate a public identity before opening a worker-owned, bounded primary Pod lookup."""
        sandbox_pod_name(sandbox_id)
        worker = asyncio.create_task(asyncio.to_thread(
            self._call_and_close, lambda api: self._read_primary_pod(api, sandbox_id),
        ))
        return await settle_task(worker, propagate_cancellation=True)

    def _read_primary_pod(self, api: ProbeKubernetesClient, sandbox_id: str) -> SandboxQueryResult:
        """Reject ambiguous discovery and compare immutable identity before accepting a fresh exact read."""
        try:
            candidates = api.list_namespaced_pod(
                self._namespace, label_selector=primary_pod_selector(sandbox_id),
                limit=2, watch=False, _request_timeout=(0.5, 0.5),
            )
        except client.ApiException as error:
            if error.status == 404:
                # A collection-level 404 cannot establish that an individual
                # sandbox is missing; the explicitly configured namespace may not exist.
                raise KubernetesConfigurationError() from None
            raise
        try:
            if (
                not isinstance(candidates.items, list) or candidates.metadata is None
                or candidates.metadata._continue not in (None, "")
                or getattr(candidates.metadata, "remaining_item_count", None) not in (None, 0)
                or len(candidates.items) > 1
            ):
                raise SandboxQueryFailed()
            if not candidates.items:
                raise SandboxNotFound()
            identity = pod_identity(candidates.items[0], sandbox_id, self._namespace)
        except (AttributeError, TypeError, ValueError):
            raise SandboxQueryFailed() from None
        try:
            pod = api.read_namespaced_pod(
                sandbox_pod_name(sandbox_id), self._namespace, _request_timeout=(0.5, 0.5),
            )
        except client.ApiException as error:
            if error.status == 404:
                raise SandboxNotFound() from None
            raise
        if pod_identity(pod, sandbox_id, self._namespace) != identity:
            raise SandboxQueryFailed()
        return query_result(pod, sandbox_id, self._namespace)

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Reject entry delivery without fabricating a Kubernetes endpoint."""
        raise KubernetesOperationUnsupported()

    async def inspect_resources(
        self, sandbox_id: str, *, expected_transaction_id: str,
        expected_workspace: WorkspaceBinding | None = None,
    ) -> SandboxResourceInspection:
        """Reject resource inspection rather than misclassifying an unsupported lookup as absent."""
        raise KubernetesOperationUnsupported()
