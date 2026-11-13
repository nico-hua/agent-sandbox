"""Lazy Kubernetes lifecycle operations and application-owned authenticated local entries."""

import asyncio
import hmac
import re
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from types import MappingProxyType
from time import monotonic
from typing import Protocol, TypeVar
from urllib.parse import urlsplit

import yaml
from kubernetes import client
from kubernetes.config.kube_config import KubeConfigLoader

from control_plane.core.errors import (
    CreationFailureInfo,
    CreationResourceState,
    KubernetesConfigurationError,
    KubernetesConnectionFailed,
    KubernetesError,
    KubernetesOperationUnsupported,
    KubernetesPermissionDenied,
    KubernetesTimeout,
    SandboxError,
    SandboxCreateFailed,
    SandboxCleanupFailed,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxEndpointUnavailable,
    SandboxNotFound,
    SandboxQueryFailed,
)
from control_plane.runtime.kubernetes.failures import _probe_failure

from control_plane.core.models import (
    SandboxEndpoint,
    SandboxQueryResult,
    SandboxResourceInspection,
    SandboxSpec,
    WorkspaceBinding,
    WorkspaceRecord,
)
from control_plane.core.tasks import settle_task, wait_for_task
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.runtime.kubernetes.pod_query import (
    pod_identity, primary_pod_selector, query_result, sandbox_pod_name,
)
from control_plane.runtime.kubernetes.pod_delete import PodDeletion
from control_plane.runtime.kubernetes.pod_create import PodCreation, PodCreationCancelled
from control_plane.runtime.kubernetes.pod_spec import _validate_backend_configuration, build_pod_spec, validate_agent_image
from control_plane.runtime.kubernetes.network_policy import NetworkPolicyResource
from control_plane.runtime.kubernetes.pod_query import network_layout, agent_service_layout, pod_resource_bindings
from control_plane.runtime.kubernetes.agent_service import AgentServiceResource
from control_plane.runtime.kubernetes.local_entry import LocalEntryError

ENTRY_READY_TIMEOUT_SECONDS = 30.0
ENTRY_READY_POLL_SECONDS = 0.1

_Result = TypeVar("_Result")
_UNSET_AGENT_IMAGE = object()


class CloseableClient(Protocol):
    """Describe ownership of one acquired SDK connection pool."""

    def close(self) -> None:
        """Close the acquired client's resources in its owning worker."""
        ...


class ProbeKubernetesClient(Protocol):
    """Restrict dependencies to bounded CoreV1 operations and the owned client handle."""

    api_client: CloseableClient
    networking: object

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

    def create_namespaced_pod(self, namespace: str, *, body: client.V1Pod, _request_timeout: tuple[float, float]) -> object:
        """Submit one constrained Pod without retries, apply or dry-run fallback."""
        ...

    def list_namespaced_service(self, namespace: str, *, limit: int, watch: bool,
                                label_selector: str, _request_timeout: tuple[float, float]) -> object:
        """Read one bounded same-sandbox Service inventory through the shared Core transport."""
        ...

    def read_namespaced_service(self, name: str, namespace: str, *, _request_timeout: tuple[float, float]) -> object:
        """Read a precise internal Service without acquiring another client."""
        ...

    def create_namespaced_service(self, namespace: str, *, body: client.V1Service,
                                  _request_timeout: tuple[float, float]) -> object:
        """Submit the constrained ClusterIP Service once without automatic retry."""
        ...

    def delete_namespaced_service(self, name: str, namespace: str, *, body: client.V1DeleteOptions,
                                  _request_timeout: tuple[float, float]) -> object:
        """Delete one original Service with UID and resourceVersion preconditions."""
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
        core = client.CoreV1Api(api_client)
        # Both facades borrow exactly one transport. Only the owning Runtime
        # worker closes it; NetworkPolicy operations never close a second client.
        core.networking = client.NetworkingV1Api(api_client)
        return core
    except Exception:
        _close_client(api_client, primary_failed=True)
        raise




class KubernetesSandboxRuntime(SandboxRuntime):
    """Probe, create, query and safely delete supported layouts in one explicitly configured namespace."""

    def __init__(
        self, *, kubeconfig_path: str | Path, context: str, namespace: str,
        agent_image: str | object = _UNSET_AGENT_IMAGE, proxy_image: str | None = None,
        environment_images: Mapping[str, str] | None = None,
        workspace_storage_class: str | None = None,
        prepare_entry_on_create: bool = False,
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
        if environment_images is not None and (
            not isinstance(environment_images, Mapping) or not 1 <= len(environment_images) <= 3
        ):
            raise KubernetesConfigurationError()
        images = {} if environment_images is None else dict(environment_images)
        if any(type(name) is not str or name not in {"basic", "python", "node"} for name in images):
            raise KubernetesConfigurationError()
        if agent_image is _UNSET_AGENT_IMAGE:
            basic_image = images.get("basic", "agent-sandbox-basic:dev")
        else:
            basic_image = agent_image
            if "basic" in images and images["basic"] != basic_image:
                raise KubernetesConfigurationError()
        images["basic"] = basic_image
        for image in images.values():
            validate_agent_image(image)
        self._environment_images = MappingProxyType(images)
        if proxy_image is not None:
            _validate_backend_configuration(namespace, proxy_image)
        if type(prepare_entry_on_create) is not bool or (prepare_entry_on_create and proxy_image is None):
            raise KubernetesConfigurationError()
        if workspace_storage_class is not None:
            from control_plane.runtime.kubernetes.private_workspace import _dns_name
            if proxy_image is None or not _dns_name(workspace_storage_class):
                raise KubernetesConfigurationError()
        self._workspace_storage_class = workspace_storage_class
        self._local_entry_manager = None
        self._proxy_image = proxy_image
        self._prepare_entry_on_create = prepare_entry_on_create
        self._client_factory = create_probe_client if client_factory is None else client_factory

    @property
    def environment_images(self) -> Mapping[str, str]:
        """Expose the copied, immutable set of trusted images accepted for future creates only."""
        return self._environment_images

    @property
    def prepare_entry_on_create(self) -> bool:
        """Expose the explicit authenticated creation mode without constructing a manager."""
        return self._prepare_entry_on_create

    @property
    def local_entry_manager(self):
        """Expose the explicitly attached application owner without constructing or starting one."""
        return self._local_entry_manager

    def bind_local_entry_manager(self, manager) -> None:
        """Accept only an explicit owner of this Runtime and reject ambiguous replacement ownership."""
        if manager.runtime is not self or (self._local_entry_manager is not None and self._local_entry_manager is not manager):
            raise ValueError("local entry manager ownership does not match runtime")
        self._local_entry_manager = manager

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
        """Prepare an original authenticated entry before returning success in the explicit automatic mode."""
        if not self._prepare_entry_on_create:
            await self._create_resources(spec)
            return
        manager = self._local_entry_manager
        if manager is None:
            error = SandboxCreateFailed("Kubernetes entry manager is unavailable")
            error.creation_failure = CreationFailureInfo("prepare_entry", CreationResourceState.UNCHANGED)
            raise error
        try:
            async with manager.creation_scope():
                operation = await self._create_resources(spec)
                preparing = asyncio.create_task(self._prepare_created_entry(operation))
                try:
                    await wait_for_task(preparing)
                except BaseException as primary:
                    preparing.cancel()
                    try:
                        await settle_task(preparing)
                    except BaseException:
                        pass
                    cleanup = asyncio.create_task(self._rollback_entry_and_resources(operation))
                    await settle_task(cleanup)
                    if isinstance(primary, asyncio.CancelledError):
                        raise primary
                    raise operation.failure("prepare_entry", CreationResourceState.ROLLED_BACK) from None
        except LocalEntryError:
            error = SandboxCreateFailed("Kubernetes entry manager is unavailable")
            error.creation_failure = CreationFailureInfo("prepare_entry", CreationResourceState.UNCHANGED)
            raise error from None

    async def _prepare_created_entry(self, operation: PodCreation) -> None:
        """Poll only original resource state, then open exactly one entry and verify its complete identity."""
        deadline = monotonic() + ENTRY_READY_TIMEOUT_SECONDS
        manager = self._local_entry_manager
        while True:
            if manager.is_stopping or monotonic() >= deadline:
                raise LocalEntryError("local_entry_timeout")
            observation = asyncio.create_task(asyncio.to_thread(
                self._call_and_close, lambda api: self._created_entry_state(api, operation, deadline),
            ))
            status = await settle_task(observation, propagate_cancellation=True)
            if status == "ready":
                break
            if status not in {"starting", "running"}:
                raise SandboxQueryFailed()
            await asyncio.sleep(min(ENTRY_READY_POLL_SECONDS, max(0.0, deadline - monotonic())))
        if manager.is_stopping or monotonic() >= deadline:
            raise LocalEntryError("local_entry_timeout")
        # open() owns the nonblocking capacity decision and its bounded startup;
        # never use a racy precheck or retry a failed process launch.
        from control_plane.runtime.kubernetes.pod_query import PROXY_DIGEST_ANNOTATION
        from control_plane.runtime.kubernetes.local_entry_target import LocalEntryTarget
        from control_plane.runtime.kubernetes.proxy_spec import proxy_pod_name
        from control_plane.runtime.kubernetes.proxy_service import proxy_service_name
        expected = LocalEntryTarget(
            sandbox_id=operation.sandbox_id, transaction_id=operation.transaction_id, namespace=self._namespace,
            proxy_pod_name=proxy_pod_name(operation.sandbox_id), proxy_pod_uid=operation.proxy.uids["proxy"],
            proxy_service_name=proxy_service_name(operation.sandbox_id), proxy_service_uid=operation.proxy.uids["proxy_service"],
            resource_uids=tuple(sorted(operation.proxy.uids.items())), created_at=operation.created_at,
            expires_at=operation.expires_at, credential_digest=operation.pod.metadata.annotations[PROXY_DIGEST_ANNOTATION],
            layout_version=operation.proxy.version,
        )
        connection = await manager.open(operation.sandbox_id, expected_target=expected)
        if manager.is_stopping or connection.target != expected:
            raise SandboxQueryFailed()

    def _created_entry_state(self, api, operation: PodCreation, deadline: float) -> str:
        """Reject replacement UIDs/configuration while observing readiness without writing resources."""
        from control_plane.runtime.kubernetes.proxy_resources import ProxyResources
        from control_plane.runtime.kubernetes.pod_query import PROXY_DIGEST_ANNOTATION
        resources = ProxyResources(api, operation.sandbox_id, self._namespace, operation.transaction_id)
        resources.deadline = deadline
        found = resources.inventory()
        bindings = resources._bindings(found)
        if bindings != operation.proxy.uids or "agent" not in found:
            raise SandboxQueryFailed()
        agent, persistent = resources._verify_configuration(found, bindings, found["agent"])
        if not hmac.compare_digest(agent.metadata.annotations[PROXY_DIGEST_ANNOTATION],
                                   operation.pod.metadata.annotations[PROXY_DIGEST_ANNOTATION]):
            raise SandboxQueryFailed()
        if pod_identity(agent, operation.sandbox_id, self._namespace) != (
            operation.uid, operation.transaction_id, operation.created_at, operation.expires_at,
        ):
            raise SandboxQueryFailed()
        if persistent and found["workspace"].status.phase == "Pending":
            return "starting"
        return resources.query(agent).status

    async def _rollback_entry_and_resources(self, operation: PodCreation) -> None:
        """Confirm this transaction's entry close before original-UID compute/PVC rollback, retaining uncertainty."""
        try:
            async with self._local_entry_manager.delete_guard(
                operation.sandbox_id, expected_transaction_id=operation.transaction_id,
            ):
                cleanup = asyncio.create_task(asyncio.to_thread(self._rollback_created_and_close, operation))
                await settle_task(cleanup)
        except LocalEntryError:
            # A failed close or conflicting session must retain both the handle
            # and original resources; it does not authorize destructive cleanup.
            raise operation.failure("entry_cleanup", CreationResourceState.UNKNOWN) from None

    async def _create_resources(self, spec: SandboxSpec) -> PodCreation:
        """Return original receipts after one-shot resource creation, settling and compensating cancellation."""
        try:
            if type(spec) is not SandboxSpec:
                raise ValueError("spec must be SandboxSpec")
            if type(spec.environment) is not str or spec.environment not in self._environment_images:
                raise KubernetesOperationUnsupported()
            pod = build_pod_spec(spec, namespace=self._namespace, agent_image=self._environment_images[spec.environment],
                                 workspace_storage_class=self._workspace_storage_class)
            if self._proxy_image is not None:
                from control_plane.runtime.kubernetes.proxy_resources import mark_proxy_layout
                mark_proxy_layout(pod, spec.access_token)
        except SandboxError as error:
            error.creation_failure = CreationFailureInfo("build_spec", CreationResourceState.UNCHANGED)
            raise
        except ValueError:
            error = SandboxCreateFailed("invalid kubernetes sandbox specification")
            error.creation_failure = CreationFailureInfo("build_spec", CreationResourceState.UNCHANGED)
            raise error from None
        operation = PodCreation(pod, threading.Event(), proxy_image=self._proxy_image, access_token=spec.access_token,
                                workspace_spec=spec if self._workspace_storage_class is not None else None,
                                workspace_storage_class=self._workspace_storage_class)
        worker = asyncio.create_task(asyncio.to_thread(self._create_and_close, operation))
        try:
            await wait_for_task(worker)
        except asyncio.CancelledError as cancellation:
            operation.cancelled.set()
            try:
                await settle_task(worker)
            except PodCreationCancelled:
                raise cancellation
            except SandboxError as error:
                evidence = error.creation_failure
                if evidence is not None and evidence.resource_state in {
                    CreationResourceState.UNCHANGED, CreationResourceState.ROLLED_BACK,
                }:
                    raise cancellation
                if operation.uid is None or operation.rollback_attempted:
                    raise
                # A close-only failure may arrive after startup committed.
                # Cancellation still requires original-UID compensation below.
            cleanup = asyncio.create_task(asyncio.to_thread(self._rollback_created_and_close, operation))
            await settle_task(cleanup)
            raise cancellation
        return operation

    def _create_and_close(self, operation: PodCreation) -> str:
        """Keep creation and its primary cleanup on one worker, preserving evidence across close failures."""
        api = None
        primary_failed = True
        try:
            if operation.cancelled.is_set():
                raise PodCreationCancelled()
            api = self._client_factory(kubeconfig_path=self._kubeconfig_path, context=self._context)
            uid = operation.run(api)
            primary_failed = False
            return uid
        except PodCreationCancelled:
            raise
        except Exception as error:
            failure = _probe_failure(error)
            if failure.creation_failure is None:
                failure.creation_failure = CreationFailureInfo("open_client", CreationResourceState.UNCHANGED)
            raise failure from None
        finally:
            if api is not None:
                try:
                    api.api_client.close()
                except Exception:
                    if not primary_failed:
                        raise operation.failure("close_client", CreationResourceState.REMAINING) from None

    def _rollback_created_and_close(self, operation: PodCreation) -> None:
        """Reopen a worker client to compensate recorded UIDs under the existing separate cleanup budgets."""
        api = None
        state = CreationResourceState.UNKNOWN
        try:
            api = self._client_factory(kubeconfig_path=self._kubeconfig_path, context=self._context)
            state = operation.rollback(api)
            if state is not CreationResourceState.ROLLED_BACK:
                raise operation.failure("cancel_rollback", state)
        except SandboxCleanupFailed:
            raise
        except Exception:
            raise operation.failure("cancel_rollback", state) from None
        finally:
            if api is not None:
                try:
                    api.api_client.close()
                except Exception:
                    if state is CreationResourceState.ROLLED_BACK:
                        raise operation.failure("close_client", state) from None

    async def resolve_environment_image(self, environment: str) -> str:
        """Reject image resolution until Kubernetes workspace/image binding is implemented."""
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
        """Close the matching local entry and settle exact resource deletion before propagating cancellation."""
        sandbox_pod_name(sandbox_id)
        if expected_transaction_id is not None and (
            type(expected_transaction_id) is not str
            or re.fullmatch(r"txn_[0-9a-f]{32}", expected_transaction_id) is None
        ):
            raise SandboxDeleteConflict()
        if type(delete_workspace) is not bool:
            raise ValueError("delete_workspace must be a boolean")
        if expected_workspace is not None:
            raise KubernetesOperationUnsupported()
        async def remove():
            """Keep the deletion exclusion until session close, SDK work and repeated-cancel cleanup complete."""
            from control_plane.runtime.kubernetes.local_entry import LocalEntryError

            async def delete_resources():
                """Run the original exact-identity deletion in its owning SDK thread."""
                worker = asyncio.create_task(asyncio.to_thread(
                    self._call_and_close,
                    lambda api: PodDeletion(api, sandbox_id, self._namespace, expected_transaction_id,
                                            delete_workspace=delete_workspace).run(),
                ))
                await settle_task(worker, propagate_cancellation=True)

            if self._local_entry_manager is None:
                await delete_resources()
                return
            try:
                async with self._local_entry_manager.delete_guard(
                    sandbox_id, expected_transaction_id=expected_transaction_id,
                ):
                    await delete_resources()
            except LocalEntryError as error:
                if error.code == "local_entry_identity_conflict":
                    raise SandboxDeleteConflict() from None
                raise SandboxDeleteFailed(sandbox_id) from None

        operation = asyncio.create_task(remove())
        await settle_task(operation, propagate_cancellation=True)

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
            bindings = pod_resource_bindings(candidates.items[0])
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
        if (pod_identity(pod, sandbox_id, self._namespace) != identity
            or pod_resource_bindings(pod) != bindings):
            raise SandboxQueryFailed()
        from control_plane.runtime.kubernetes.pod_query import proxy_layout
        if proxy_layout(pod.metadata):
            from control_plane.runtime.kubernetes.proxy_resources import ProxyResources
            return ProxyResources(api, sandbox_id, self._namespace, identity[1]).query(pod)
        if network_layout(pod.metadata):
            NetworkPolicyResource(api, sandbox_id, self._namespace, identity[1]).verify_for(pod)
        if agent_service_layout(pod.metadata):
            AgentServiceResource(api, sandbox_id, self._namespace, identity[1]).verify_for(pod)
        return query_result(pod, sandbox_id, self._namespace)

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Deliver only an existing application-owned session after its per-request original-identity check."""
        sandbox_pod_name(sandbox_id)
        if self._local_entry_manager is None:
            raise SandboxEndpointUnavailable()
        try:
            connection = await self._local_entry_manager.lookup(sandbox_id)
            target = connection.target
            return SandboxEndpoint(
                sandbox_id, connection.url, auth_required=True,
                transaction_id=target.transaction_id, created_at=target.created_at,
                credential_digest=target.credential_digest, auth_version=target.auth_version,
            )
        except Exception:
            raise SandboxEndpointUnavailable() from None

    async def inspect_resources(
        self, sandbox_id: str, *, expected_transaction_id: str,
        expected_workspace: WorkspaceBinding | None = None,
    ) -> SandboxResourceInspection:
        """Inspect persisted resource identity without readiness probes, mutation or automatic recovery."""
        sandbox_pod_name(sandbox_id)
        if type(expected_transaction_id) is not str or re.fullmatch(r"txn_[0-9a-f]{32}", expected_transaction_id) is None:
            raise SandboxDeleteConflict()
        if expected_workspace is not None:
            raise KubernetesOperationUnsupported()

        def inspect_and_close():
            """Preserve primary integrity evidence through client closure and return fixed backend categories."""
            from control_plane.runtime.kubernetes.resource_inspection import (
                ResourceInspection, InspectionConflict, InspectionChanged,
            )
            try:
                return self._call_and_close(lambda api: ResourceInspection(
                    api, sandbox_id, self._namespace, expected_transaction_id).run())
            except InspectionChanged:
                return SandboxResourceInspection(sandbox_id, "changed", "kubernetes_resources_changed")
            except (InspectionConflict, SandboxQueryFailed, SandboxDeleteConflict):
                return SandboxResourceInspection(sandbox_id, "conflict", "kubernetes_resource_conflict")
            except Exception:
                return SandboxResourceInspection(sandbox_id, "unavailable", "kubernetes_inspection_unavailable")

        worker = asyncio.create_task(asyncio.to_thread(inspect_and_close))
        return await settle_task(worker, propagate_cancellation=True)
