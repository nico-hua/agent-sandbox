"""Versioned proxy resource transactions, durable UID receipts and conservative observations."""

import base64
import binascii
import hashlib
import json
import re
from copy import deepcopy
from dataclasses import replace
from time import monotonic, sleep
from uuid import UUID

from kubernetes import client
from kubernetes.utils.quantity import parse_quantity

from control_plane.core.errors import (
    CleanupIssue, CreationResourceState, KubernetesConfigurationError, KubernetesPermissionDenied,
    SandboxDeleteConflict, SandboxDeleteFailed, SandboxQueryFailed,
)
from control_plane.runtime.kubernetes.agent_service import agent_service_name, verify_agent_service
from control_plane.runtime.kubernetes.network_policy import network_policy_name, verify_deny_policy
from control_plane.runtime.kubernetes.proxy_service import (
    build_proxy_service, proxy_service_name, verify_proxy_service,
)
from control_plane.runtime.kubernetes.pod_query import (
    LABEL_PREFIX, PROXY_IMAGE_ANNOTATION, PROXY_LAYOUT_ANNOTATION, PROXY_LAYOUT_VERSION,
    PROXY_LEGACY_LAYOUT_VERSION, PROXY_LAYOUT_VERSIONS, PROXY_RECORD_ANNOTATION, PROXY_DIGEST_ANNOTATION,
    lifecycle_annotations, pod_identity,
    query_result, resource_ownership, sandbox_pod_name, sandbox_pod_selector,
    PROXY_PERSISTENT_LAYOUT_VERSION, PRIVATE_WORKSPACE_VERSION, PRIVATE_STORAGE_CLASS, PRIVATE_WORKSPACE_UID,
    persistent_workspace_layout,
)
from control_plane.runtime.kubernetes.private_workspace import private_workspace_name, private_workspace_from_metadata
from control_plane.runtime.kubernetes.pod_spec import _validate_backend_configuration
from control_plane.runtime.kubernetes.proxy_spec import (
    PROXY_UPSTREAM_ANNOTATION, PROXY_UPSTREAM_VERSION, TOKEN_KEY,
    build_proxy_network_policies, build_proxy_pod, build_proxy_secret, proxy_pod_name, proxy_secret_name,
)

RESOURCE_TIMEOUT_SECONDS = 5.0
DELETE_TIMEOUT_SECONDS = 45.0
RECONCILE_TIMEOUT_SECONDS = 5.0
_LEGACY_KEYS = frozenset({"agent", "policy", "service", "secret", "proxy", "ingress", "isolation"})
_KEYS = _LEGACY_KEYS | {"proxy_service"}
_PERSISTENT_KEYS = _KEYS | {"workspace"}
_LAYOUT_KEYS = {PROXY_LEGACY_LAYOUT_VERSION: _LEGACY_KEYS, PROXY_LAYOUT_VERSION: _KEYS,
                PROXY_PERSISTENT_LAYOUT_VERSION: _PERSISTENT_KEYS}
_ROLES = {"agent": "container", "policy": "network-policy", "service": "agent-service",
          "secret": "proxy-access", "proxy": "proxy", "ingress": "agent-proxy-ingress",
          "isolation": "proxy-isolation", "proxy_service": "proxy-service", "workspace": "private-workspace"}
_KINDS = {"agent": "pod", "proxy": "pod", "policy": "network_policy", "ingress": "network_policy",
          "isolation": "network_policy", "service": "service", "secret": "secret", "proxy_service": "service",
          "workspace": "persistent_volume_claim"}
_MARKERS = frozenset({"managed", "project", "sandbox-id", "resource", "transaction-id", "created-at",
    "expires-at", "kubernetes-network-version", "network-policy-uid", "agent-service-version",
    "agent-service-uid", "kubernetes-proxy-version", "proxy-resource-uids", "proxy-token-sha256",
    "proxy-upstream-version", "proxy-image", "private-workspace-version", "private-workspace-storage-class", "private-workspace-uid"})
_REJECTED = frozenset({400, 401, 403, 404, 405, 409, 413, 415, 422, 429})


def _token_digest(access_token: str) -> str:
    """Validate the exact ASCII credential representation without echoing invalid input."""
    if type(access_token) is not str or re.fullmatch(r"[A-Za-z0-9_-]{43}", access_token) is None:
        raise KubernetesConfigurationError()
    return hashlib.sha256(access_token.encode("ascii")).hexdigest()


def mark_proxy_layout(pod, access_token: str) -> None:
    """Declare the intended layout and expected credential digest before any resource POST."""
    digest = _token_digest(access_token)
    pod.metadata.annotations[PROXY_LAYOUT_ANNOTATION] = (
        PROXY_PERSISTENT_LAYOUT_VERSION if PRIVATE_STORAGE_CLASS in pod.metadata.annotations else PROXY_LAYOUT_VERSION)
    pod.metadata.annotations[PROXY_DIGEST_ANNOTATION] = digest


def _record(metadata) -> dict[str, str]:
    """Parse a bounded closed-key receipt map without accepting duplicate JSON fields or noncanonical UIDs."""
    annotations = metadata.annotations or {}
    raw = annotations.get(PROXY_RECORD_ANNOTATION)
    version = annotations.get(PROXY_LAYOUT_ANNOTATION)
    allowed = _LAYOUT_KEYS.get(version)
    bound = 512 if version == PROXY_LEGACY_LAYOUT_VERSION else 1024 if version == PROXY_PERSISTENT_LAYOUT_VERSION else 768
    if allowed is None or type(raw) is not str or not 2 <= len(raw) <= bound:
        raise SandboxDeleteConflict()
    try:
        pairs = json.loads(raw, object_pairs_hook=list)
        if not isinstance(pairs, list) or any(not isinstance(pair, tuple) or len(pair) != 2 for pair in pairs):
            raise ValueError()
        result = dict(pairs)
        if len(result) != len(pairs) or not result or not set(result) <= allowed:
            raise ValueError()
        if any(type(value) is not str or str(UUID(value)) != value for value in result.values()):
            raise ValueError()
        return result
    except (TypeError, ValueError, RecursionError):
        raise SandboxDeleteConflict() from None


def _spec_matches(expected, observed, path=()) -> bool:
    """Compare the complete proxy contract, admitting only documented Kubernetes scalar defaults."""
    if expected is None:
        if observed is None or observed == []:
            return True
        defaults = {"dns_policy": "ClusterFirst", "scheduler_name": "default-scheduler",
                    "termination_grace_period_seconds": 30, "termination_message_path": "/dev/termination-log",
                    "termination_message_policy": "File", "protocol": "TCP", "initial_delay_seconds": 0,
                    "success_threshold": 1, "failure_threshold": 3, "timeout_seconds": 1,
                    "period_seconds": 10, "stdin": False, "stdin_once": False, "tty": False,
                    "service_account_name": "default", "service_account": "default", "priority": 0,
                    "preemption_policy": "PreemptLowerPriority"}
        if path == ("node_name",):
            return type(observed) is str and bool(observed)
        if path == ("tolerations",):
            expected_tolerations = [client.V1Toleration(
                key="node.kubernetes.io/" + condition, operator="Exists", effect="NoExecute", toleration_seconds=300,
            ).to_dict() for condition in ("not-ready", "unreachable")]
            return observed == expected_tolerations
        return bool(path) and path[-1] in defaults and type(observed) is type(defaults[path[-1]]) and observed == defaults[path[-1]]
    if isinstance(expected, dict):
        return isinstance(observed, dict) and set(expected) == set(observed) and all(
            _spec_matches(value, observed[key], (*path, key)) for key, value in expected.items())
    if isinstance(expected, list):
        if expected == [] and observed is None:
            return True
        if path and path[-1] == "policy_types":
            return isinstance(observed, list) and len(expected) == len(observed) and set(expected) == set(observed)
        return isinstance(observed, list) and len(expected) == len(observed) and all(
            _spec_matches(left, right, path) for left, right in zip(expected, observed))
    if len(path) >= 2 and path[-2] in {"limits", "requests"}:
        try:
            return parse_quantity(expected) == parse_quantity(observed)
        except (ValueError, TypeError):
            return False
    if expected is False and observed is None and path and path[-1] in {"host_network", "host_pid", "host_ipc", "privileged", "read_only"}:
        return True
    return type(expected) is type(observed) and expected == observed


class ProxyResources:
    """Operate on the closed, versioned proxy layouts using the caller's shared client."""

    def __init__(self, api, sandbox_id: str, namespace: str, transaction_id: str | None = None):
        """Keep deterministic names and an operation budget without owning the caller's transport."""
        self.api, self.sandbox_id, self.namespace, self.transaction_id = api, sandbox_id, namespace, transaction_id
        root = sandbox_pod_name(sandbox_id)
        self.names = {"agent": root, "proxy": proxy_pod_name(sandbox_id), "secret": proxy_secret_name(sandbox_id),
                      "service": agent_service_name(sandbox_id), "policy": network_policy_name(sandbox_id),
                      "ingress": root + "-proxy-in", "isolation": root + "-proxy-deny",
                      "proxy_service": proxy_service_name(sandbox_id), "workspace": private_workspace_name(sandbox_id)}
        self.deadline = monotonic() + RESOURCE_TIMEOUT_SECONDS
        self.cancelled = None

    def _timeout(self):
        """Bound requests and honor cancellation only at safe SDK call boundaries."""
        if self.cancelled is not None and self.cancelled.is_set():
            from control_plane.runtime.kubernetes.pod_create import PodCreationCancelled
            raise PodCreationCancelled()
        remaining = self.deadline - monotonic()
        if remaining <= 0:
            raise SandboxDeleteFailed(self.sandbox_id)
        phase = min(0.5, remaining / 2)
        return phase, phase

    def _facade(self, key):
        """Choose only the fixed Core or Networking API associated with a closed resource key."""
        return self.api.networking if _KINDS[key] == "network_policy" else self.api

    def _read(self, key):
        """Distinguish an exact-name 404 from authorization, transport or configuration failure."""
        try:
            return getattr(self._facade(key), "read_namespaced_" + _KINDS[key])(
                self.names[key], self.namespace, _request_timeout=self._timeout())
        except client.ApiException as error:
            if error.status == 404:
                return None
            raise

    def _identity(self, key, resource):
        """Check cleanup ownership and resourceVersion independently of health and business timestamps."""
        try:
            metadata = resource.metadata
            uid, transaction = resource_ownership(metadata, self.sandbox_id, self.namespace,
                                                   name=self.names[key], role=_ROLES[key], check_proxy_layout=False)
            version = metadata.resource_version
            if (self.transaction_id is not None and transaction != self.transaction_id
                or type(version) is not str or not version
                or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in version)
                or any(name.startswith(LABEL_PREFIX) and name[len(LABEL_PREFIX):] not in _MARKERS
                       for name in (*metadata.labels, *(metadata.annotations or {})))
                or any(name in metadata.labels for name in (PROXY_LAYOUT_ANNOTATION, PROXY_RECORD_ANNOTATION,
                                                            PROXY_DIGEST_ANNOTATION, PROXY_IMAGE_ANNOTATION))):
                raise SandboxDeleteConflict()
            annotations = metadata.annotations or {}
            if PROXY_LAYOUT_ANNOTATION in annotations and annotations[PROXY_LAYOUT_ANNOTATION] not in PROXY_LAYOUT_VERSIONS:
                raise SandboxDeleteConflict()
            if key in {"agent", "proxy"}:
                from control_plane.runtime.kubernetes.pod_delete import verify_temporary_volumes, verify_workspace_volumes
                (verify_workspace_volumes if key == "agent" else verify_temporary_volumes)(resource)
            if key == "workspace":
                workspace = private_workspace_from_metadata(self.api, metadata,
                    storage_class=resource.spec.storage_class_name if PROXY_LAYOUT_ANNOTATION not in annotations else None)
                workspace._identity(resource)
            return uid, transaction
        except (AttributeError, TypeError, ValueError, SandboxQueryFailed):
            raise SandboxDeleteConflict() from None

    def inventory(self):
        """Cross-check bounded unfiltered role discovery against every deterministic exact name."""
        found = {}
        for kind, keys in (("pod", ("agent", "proxy")), ("network_policy", ("policy", "ingress", "isolation")),
                           ("service", ("service", "proxy_service")), ("secret", ("secret",)),
                           ("persistent_volume_claim", ("workspace",))):
            facade = self.api.networking if kind == "network_policy" else self.api
            try:
                page = getattr(facade, "list_namespaced_" + kind)(
                    self.namespace, label_selector=sandbox_pod_selector(self.sandbox_id), limit=len(keys) + 1,
                    watch=False, _request_timeout=self._timeout())
            except client.ApiException as error:
                if error.status == 404:
                    raise KubernetesConfigurationError() from None
                raise
            try:
                if (not isinstance(page.items, list) or len(page.items) > len(keys) or page.metadata is None
                    or page.metadata._continue not in (None, "")
                    or getattr(page.metadata, "remaining_item_count", None) not in (None, 0)):
                    raise SandboxDeleteConflict()
                listed = {}
                for resource in page.items:
                    role = resource.metadata.labels.get(LABEL_PREFIX + "resource")
                    matching = [key for key in keys if _ROLES[key] == role]
                    if len(matching) != 1 or matching[0] in listed:
                        raise SandboxDeleteConflict()
                    key = matching[0]
                    listed[key] = self._identity(key, resource)
                for key in keys:
                    resource = self._read(key)
                    if resource is None:
                        if key in listed:
                            raise SandboxDeleteConflict()
                    elif key not in listed or self._identity(key, resource) != listed[key]:
                        raise SandboxDeleteConflict()
                    else:
                        found[key] = resource
            except (AttributeError, TypeError, ValueError):
                raise SandboxDeleteConflict() from None
        if len({self._identity(key, value)[1] for key, value in found.items()}) > 1:
            raise SandboxDeleteConflict()
        return found

    def preflight(self) -> None:
        """Require all roles and all exact names absent before the transaction's first mutation."""
        if self.inventory():
            raise SandboxDeleteConflict()

    def declared(self) -> bool:
        """Recognize durable proxy layout from any survivor while preserving conservative legacy cleanup."""
        found = self.inventory()
        declared = any(any(marker in (resource.metadata.annotations or {}) for marker in (
            PROXY_LAYOUT_ANNOTATION, PROXY_RECORD_ANNOTATION, PROXY_DIGEST_ANNOTATION, PROXY_IMAGE_ANNOTATION))
            for resource in found.values())
        if not declared and set(found) - {"agent", "service", "policy"}:
            raise SandboxDeleteConflict()
        return declared

    def _bindings(self, found, expected_uids=None):
        """Merge durable receipts from surviving resources while rejecting self replacement or conflicting anchors."""
        bindings = {} if expected_uids is None else dict(expected_uids)
        if not set(bindings) <= _PERSISTENT_KEYS:
            raise SandboxDeleteConflict()
        versions = {resource.metadata.annotations[PROXY_LAYOUT_ANNOTATION]
                    for resource in found.values() if PROXY_LAYOUT_ANNOTATION in (resource.metadata.annotations or {})}
        if len(versions) != 1 and found:
            raise SandboxDeleteConflict()
        allowed = _LAYOUT_KEYS[next(iter(versions))] if versions else _PERSISTENT_KEYS
        if not set(found) <= allowed or not set(bindings) <= allowed:
            raise SandboxDeleteConflict()
        for key, resource in found.items():
            annotations = resource.metadata.annotations or {}
            if PROXY_RECORD_ANNOTATION not in annotations:
                # A resource POST can succeed before its own metadata PATCH.
                # Only another already-published receipt may authorize it below.
                continue
            if annotations.get(PROXY_LAYOUT_ANNOTATION) not in versions:
                raise SandboxDeleteConflict()
            receipt = _record(resource.metadata)
            if receipt.get(key) != resource.metadata.uid:
                raise SandboxDeleteConflict()
            for name, uid in receipt.items():
                if name in bindings and bindings[name] != uid:
                    raise SandboxDeleteConflict()
                bindings[name] = uid
        if any(bindings.get(key) != resource.metadata.uid for key, resource in found.items()):
            raise SandboxDeleteConflict()
        return bindings

    def query(self, agent_pod):
        """Verify the declared layout and aggregate actual Agent and proxy readiness without an endpoint."""
        try:
            found = self.inventory()
            bindings = self._bindings(found)
            version = agent_pod.metadata.annotations.get(PROXY_LAYOUT_ANNOTATION)
            required = _LAYOUT_KEYS.get(version)
            if required is None or set(found) != required or set(bindings) != required:
                raise SandboxQueryFailed()
            agent = found["agent"]
            if (pod_identity(agent, self.sandbox_id, self.namespace) != pod_identity(agent_pod, self.sandbox_id, self.namespace)
                or agent.metadata.annotations.get(PROXY_LAYOUT_ANNOTATION) != version):
                raise SandboxQueryFailed()
            annotations = agent.metadata.annotations
            digest = annotations.get(PROXY_DIGEST_ANNOTATION)
            expected_image = annotations.get(PROXY_IMAGE_ANNOTATION)
            _validate_backend_configuration(self.namespace, expected_image)
            if type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
                raise SandboxQueryFailed()
            times = lifecycle_annotations(annotations)
            for key, resource in found.items():
                if (resource.metadata.annotations.get(PROXY_LAYOUT_ANNOTATION) != version
                    or lifecycle_annotations(resource.metadata.annotations) != times
                    or resource.metadata.annotations.get(PROXY_DIGEST_ANNOTATION) != digest
                    or resource.metadata.annotations.get(PROXY_IMAGE_ANNOTATION) != expected_image
                    or set(_record(resource.metadata)) != required
                    or resource.metadata.deletion_timestamp is not None):
                    raise SandboxQueryFailed()
                if version == PROXY_PERSISTENT_LAYOUT_VERSION and any(
                    resource.metadata.annotations.get(name) != annotations.get(name)
                    for name in (PRIVATE_WORKSPACE_VERSION, PRIVATE_STORAGE_CLASS, PRIVATE_WORKSPACE_UID)):
                    raise SandboxQueryFailed()
            if version == PROXY_PERSISTENT_LAYOUT_VERSION:
                if annotations.get(PRIVATE_WORKSPACE_UID) != bindings["workspace"]:
                    raise SandboxQueryFailed()
                workspace = private_workspace_from_metadata(self.api, agent.metadata)
                observation = workspace.inspect(bindings["workspace"])
                if observation is None or observation.phase != "Bound":
                    raise SandboxQueryFailed()
            verify_deny_policy(found["policy"], agent, expected_uid=bindings["policy"])
            verify_agent_service(found["service"], agent, expected_uid=bindings["service"])
            if "proxy_service" in required:
                verify_proxy_service(found["proxy_service"], agent, expected_uid=bindings["proxy_service"])
            secret = found["secret"]
            if (secret.type != "Opaque" or secret.immutable is not True or secret.string_data not in (None, {})
                or not isinstance(secret.data, dict) or set(secret.data) != {TOKEN_KEY}):
                raise SandboxQueryFailed()
            try:
                token = base64.b64decode(secret.data[TOKEN_KEY], validate=True).decode("ascii")
                if _token_digest(token) != digest or base64.b64encode(token.encode()).decode() != secret.data[TOKEN_KEY]:
                    raise SandboxQueryFailed()
            except (ValueError, TypeError, UnicodeError, binascii.Error, KubernetesConfigurationError):
                raise SandboxQueryFailed() from None
            proxy = found["proxy"]
            expected = build_proxy_pod(agent, found["service"], proxy_image=expected_image)
            if (not _spec_matches(expected.spec.to_dict(), proxy.spec.to_dict())
                or any(found[key].metadata.annotations.get(PROXY_UPSTREAM_ANNOTATION) != PROXY_UPSTREAM_VERSION
                       for key in ("proxy", "secret", "ingress", "isolation"))):
                raise SandboxQueryFailed()
            for key, expected_policy in zip(("ingress", "isolation"), build_proxy_network_policies(agent)):
                if not _spec_matches(expected_policy.spec.to_dict(), found[key].spec.to_dict()):
                    raise SandboxQueryFailed()
            agent_result = query_result(agent, self.sandbox_id, self.namespace)
            proxy_state = self._proxy_state(proxy)
            if agent_result.status in {"failed", "stopped"}:
                return agent_result
            if proxy_state == "failed":
                return replace(agent_result, status="failed", reason="proxy_failed", message="Sandbox proxy failed")
            if agent_result.status == "ready" and proxy_state == "ready":
                return agent_result
            if agent_result.status == "starting" or proxy_state == "starting":
                return replace(agent_result, status="starting", reason="proxy_starting", message="Sandbox components are starting")
            return replace(agent_result, status="running", reason="proxy_not_ready", message="Sandbox components are not ready")
        except (AttributeError, KeyError, IndexError, TypeError, ValueError, SandboxDeleteConflict, KubernetesConfigurationError):
            raise SandboxQueryFailed() from None

    def _proxy_state(self, proxy):
        """Apply the same strict single-container state interpreter to the proxy's fixed health probe."""
        observed = deepcopy(proxy)
        observed.metadata.name = sandbox_pod_name(self.sandbox_id)
        observed.metadata.labels[LABEL_PREFIX + "resource"] = "container"
        observed.spec.containers[0].name = "agent"
        if observed.status is not None and observed.status.container_statuses:
            if len(observed.status.container_statuses) != 1 or observed.status.container_statuses[0].name != "proxy":
                raise SandboxQueryFailed()
            observed.status.container_statuses[0].name = "agent"
        state = query_result(observed, self.sandbox_id, self.namespace).status
        return "failed" if state == "stopped" else state

    def _delete(self, key, uid):
        """Issue at most one UID/resourceVersion DELETE and confirm the same object's absence within a bound."""
        self.deadline = monotonic() + (DELETE_TIMEOUT_SECONDS if _KINDS[key] == "pod" else RESOURCE_TIMEOUT_SECONDS)
        resource = self._read(key)
        if key == "workspace":
            if resource is not None:
                if self._identity(key, resource)[0] != uid:
                    raise SandboxDeleteConflict()
                private_workspace_from_metadata(self.api, resource.metadata).delete(uid)
            return
        if resource is not None:
            if self._identity(key, resource)[0] != uid:
                raise SandboxDeleteConflict()
            if resource.metadata.deletion_timestamp is None:
                try:
                    getattr(self._facade(key), "delete_namespaced_" + _KINDS[key])(
                        self.names[key], self.namespace, body=client.V1DeleteOptions(preconditions=client.V1Preconditions(
                            uid=uid, resource_version=resource.metadata.resource_version)), _request_timeout=self._timeout())
                except client.ApiException as error:
                    if error.status == 409:
                        raise SandboxDeleteConflict() from None
                    if error.status in {401, 403}:
                        raise KubernetesPermissionDenied() from None
                except SandboxDeleteFailed:
                    raise
                except Exception:
                    pass
        while True:
            resource = self._read(key)
            if resource is None:
                return
            if self._identity(key, resource)[0] != uid:
                raise SandboxDeleteConflict()
            sleep(min(0.1, max(0, self.deadline - monotonic())))

    def delete_all(self, expected_uids=None, *, delete_workspace=False) -> None:
        """Validate all surviving receipts first, then remove proxy, Agent and supporting resources with deny last."""
        found = self.inventory()
        bindings = self._bindings(found, expected_uids)
        for key in ("proxy", "agent", "secret", "proxy_service", "service", "ingress", "isolation", "policy", "workspace"):
            if key in found:
                if key == "workspace" and not delete_workspace:
                    continue
                self._delete(key, bindings[key])
        self.deadline = monotonic() + RESOURCE_TIMEOUT_SECONDS
        remaining = self.inventory()
        self._bindings(remaining, bindings)
        if set(remaining) - ({"workspace"} if not delete_workspace else set()):
            raise SandboxDeleteConflict()
        namespace = self.api.read_namespace(self.namespace, _request_timeout=self._timeout())
        if namespace.metadata.name != self.namespace:
            raise KubernetesConfigurationError()


class ProxyCreationResources:
    """Keep companion submission receipts and original UIDs across cancellation and rollback workers."""

    def __init__(self, pod, access_token, proxy_image, cancelled):
        """Validate credentials and retain them only inside the redacted Secret body."""
        mark_proxy_layout(pod, access_token)
        _validate_backend_configuration(pod.metadata.namespace, proxy_image)
        pod.metadata.annotations[PROXY_IMAGE_ANNOTATION] = proxy_image
        self.pod, self.proxy_image, self.cancelled = pod, proxy_image, cancelled
        self.version = pod.metadata.annotations[PROXY_LAYOUT_ANNOTATION]
        self.__access_token = access_token
        self.sandbox_id = pod.metadata.labels[LABEL_PREFIX + "sandbox-id"]
        self.namespace = pod.metadata.namespace
        self.transaction_id = pod.metadata.labels[LABEL_PREFIX + "transaction-id"]
        self.times = lifecycle_annotations(pod.metadata.annotations)
        self.uids, self.submitted, self.progress = {}, set(), "preflight"
        self.cleanup_issues = ()

    def _manager(self, api, *, deadline=None, cancel=False):
        """Create a lightweight facade sharing the worker's existing transport and explicit operation budget."""
        manager = ProxyResources(api, self.sandbox_id, self.namespace, self.transaction_id)
        if deadline is not None:
            manager.deadline = deadline
        if cancel:
            manager.cancelled = self.cancelled
        return manager

    def preflight(self, api):
        """Reserve no resources; reject all occupancy across all supported roles before base creation."""
        self._manager(api, cancel=True).preflight()

    def _publish(self, manager, key, resource):
        """Publish original UID receipts with UID and resourceVersion JSON Patch compare-and-swap guards."""
        if manager._identity(key, resource)[0] != self.uids[key]:
            raise SandboxDeleteConflict()
        annotations = {PROXY_LAYOUT_ANNOTATION: self.version,
                       PROXY_IMAGE_ANNOTATION: self.proxy_image,
                       PROXY_DIGEST_ANNOTATION: self.pod.metadata.annotations[PROXY_DIGEST_ANNOTATION],
                       PROXY_RECORD_ANNOTATION: json.dumps(self.uids, separators=(",", ":"), sort_keys=True)}
        if self.version == PROXY_PERSISTENT_LAYOUT_VERSION:
            annotations.update({PRIVATE_WORKSPACE_VERSION: "1",
                                PRIVATE_STORAGE_CLASS: self.pod.metadata.annotations[PRIVATE_STORAGE_CLASS],
                                PRIVATE_WORKSPACE_UID: self.uids["workspace"]})
        patch = [{"op": "test", "path": "/metadata/uid", "value": self.uids[key]},
                 {"op": "test", "path": "/metadata/resourceVersion", "value": resource.metadata.resource_version}]
        for name, value in annotations.items():
            patch.append({"op": "add", "path": "/metadata/annotations/" + name.replace("~", "~0").replace("/", "~1"), "value": value})
        result = getattr(manager._facade(key), "patch_namespaced_" + _KINDS[key])(
            manager.names[key], manager.namespace, body=patch, _request_timeout=manager._timeout())
        if manager._identity(key, result)[0] != self.uids[key] or _record(result.metadata) != self.uids:
            raise SandboxDeleteConflict()
        return result

    def _publish_all(self, manager):
        """Refresh every accepted original resource so any later survivor can anchor safe cleanup retries."""
        for key in ("workspace", "secret", "agent", "policy", "service", "ingress", "isolation", "proxy_service", "proxy"):
            if key in self.uids:
                resource = manager._read(key)
                if resource is None:
                    raise SandboxDeleteConflict()
                self._publish(manager, key, resource)

    def _submit(self, manager, key, body):
        """Submit one resource once and pin its response UID before validating any mutable fields."""
        timeout = manager._timeout()
        self.progress = "submit_" + key
        self.submitted.add(key)
        try:
            resource = getattr(manager._facade(key), "create_namespaced_" + _KINDS[key])(
                manager.namespace, body=body, _request_timeout=timeout)
        except client.ApiException as error:
            if error.status in _REJECTED:
                self.submitted.remove(key)
            raise RuntimeError("Kubernetes proxy creation failed") from None
        except Exception:
            raise RuntimeError("Kubernetes proxy creation failed") from None
        self.uids[key] = manager._identity(key, resource)[0]
        if lifecycle_annotations(resource.metadata.annotations) != self.times:
            raise SandboxDeleteConflict()
        self._publish_all(manager)
        return resource

    def create(self, api, agent, service, base_uids, deadline):
        """Create credentials, restrictive policies and the internal Service before observing proxy startup."""
        manager = self._manager(api, deadline=deadline, cancel=True)
        self.uids.update(base_uids)
        if set(self.uids) != ({"agent", "service", "policy", "workspace"}
                             if self.version == PROXY_PERSISTENT_LAYOUT_VERSION else {"agent", "service", "policy"}):
            raise SandboxDeleteConflict()
        self._publish_all(manager)
        ingress, isolation = build_proxy_network_policies(agent)
        secret = build_proxy_secret(agent, access_token=self.__access_token)
        self.__access_token = None
        self._submit(manager, "secret", secret)
        self._submit(manager, "isolation", isolation)
        self._submit(manager, "ingress", ingress)
        proxy_service = self._submit(manager, "proxy_service", build_proxy_service(agent))
        verify_proxy_service(proxy_service, agent, expected_uid=self.uids["proxy_service"])
        proxy = build_proxy_pod(agent, service, proxy_image=self.proxy_image)
        self._submit(manager, "proxy", proxy)
        self.progress = "wait_proxy_started"
        while True:
            current = manager._read("proxy")
            if current is not None:
                if (manager._identity("proxy", current)[0] != self.uids["proxy"]
                    or lifecycle_annotations(current.metadata.annotations) != self.times
                    or current.metadata.deletion_timestamp is not None
                    or not _spec_matches(proxy.spec.to_dict(), current.spec.to_dict())):
                    raise SandboxQueryFailed()
                state = manager._proxy_state(current)
                if state == "failed":
                    raise RuntimeError("Kubernetes proxy failed to start")
                if state in {"ready", "running"}:
                    aggregate = manager.query(agent)
                    if aggregate.status in {"failed", "stopped"}:
                        raise RuntimeError("Kubernetes sandbox components failed to start")
                    if aggregate.status in {"running", "ready"}:
                        manager._timeout()
                        return
            sleep(min(0.1, max(0, deadline - monotonic())))

    def _reconcile(self, manager, key):
        """Resolve ambiguous POST only through unique positive ownership and original business-time evidence."""
        manager.deadline = monotonic() + RECONCILE_TIMEOUT_SECONDS
        while monotonic() < manager.deadline:
            try:
                found = manager.inventory()
                resource = found.get(key)
                if resource is not None:
                    uid = manager._identity(key, resource)[0]
                    if lifecycle_annotations(resource.metadata.annotations) != self.times:
                        return False
                    if key in self.uids and self.uids[key] != uid:
                        return False
                    self.uids[key] = uid
                    return True
            except (SandboxDeleteConflict, SandboxQueryFailed, ValueError, TypeError, AttributeError):
                return False
            except Exception:
                pass
            sleep(min(0.1, max(0, manager.deadline - monotonic())))
        return False

    def _cleanup_key(self, api, key):
        """Delete only one accepted or positively reconciled original companion and classify uncertain evidence."""
        if key not in self.submitted:
            return None
        manager = self._manager(api)
        if key not in self.uids and not self._reconcile(manager, key):
            return CreationResourceState.UNKNOWN
        try:
            manager._delete(key, self.uids[key])
            return None
        except Exception as error:
            self.cleanup_issues += (CleanupIssue(_ROLES[key], "rollback", type(error).__name__),)
            try:
                manager.deadline = monotonic() + 1
                resource = manager._read(key)
                if resource is not None and manager._identity(key, resource)[0] == self.uids[key]:
                    return CreationResourceState.REMAINING
            except Exception:
                pass
            return CreationResourceState.UNKNOWN

    def stop_proxy(self, api):
        """Confirm proxy absence before allowing the parent transaction to remove the original Agent."""
        return self._cleanup_key(api, "proxy")

    def cleanup_companions(self, api):
        """Remove Secret, proxy Service and extra policies only after both original Pods are confirmed absent."""
        for key in ("secret", "proxy_service", "ingress", "isolation"):
            result = self._cleanup_key(api, key)
            if result is not None:
                return result
        return None
