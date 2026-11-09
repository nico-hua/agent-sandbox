"""Internal Agent Service specification, integrity checks and exact UID-protected operations."""

from ipaddress import ip_address
from time import monotonic, sleep

from kubernetes import client

from control_plane.core.errors import (
    KubernetesConfigurationError, KubernetesPermissionDenied, SandboxDeleteConflict,
    SandboxDeleteFailed, SandboxQueryFailed,
)
from control_plane.runtime.kubernetes.pod_query import (
    LABEL_PREFIX, AGENT_SERVICE_VERSION_ANNOTATION, AGENT_SERVICE_VERSION,
    AGENT_SERVICE_UID_ANNOTATION, agent_service_layout, lifecycle_annotations,
    resource_ownership, sandbox_pod_name, sandbox_pod_selector, primary_pod_labels,
)

SERVICE_TIMEOUT_SECONDS = 5.0
SERVICE_ROLE = "agent-service"


def agent_service_name(sandbox_id: str) -> str:
    """Derive a stable internal Service name from a validated public sandbox ID."""
    return sandbox_pod_name(sandbox_id) + "-agent"


def build_agent_service(pod: client.V1Pod) -> client.V1Service:
    """Build an independent, unallocated internal Service without adding selector labels."""
    metadata = pod.metadata
    if not agent_service_layout(metadata):
        raise SandboxQueryFailed()
    lifecycle_annotations(metadata.annotations)
    sandbox_id = metadata.labels[LABEL_PREFIX + "sandbox-id"]
    transaction = metadata.labels[LABEL_PREFIX + "transaction-id"]
    return client.V1Service(
        api_version="v1", kind="Service",
        metadata=client.V1ObjectMeta(
            name=agent_service_name(sandbox_id), namespace=metadata.namespace,
            labels={**metadata.labels, LABEL_PREFIX + "resource": SERVICE_ROLE},
            annotations={**{LABEL_PREFIX + key: metadata.annotations[LABEL_PREFIX + key]
                           for key in ("created-at", "expires-at")},
                         AGENT_SERVICE_VERSION_ANNOTATION: AGENT_SERVICE_VERSION},
        ),
        spec=client.V1ServiceSpec(
            type="ClusterIP", selector=primary_pod_labels(sandbox_id, transaction),
            ports=[client.V1ServicePort(name="agent-http", protocol="TCP", port=8080, target_port=8080)],
            publish_not_ready_addresses=False,
        ),
    )


def service_identity(service, sandbox_id: str, namespace: str, transaction_id: str | None = None):
    """Check cleanup ownership and version independently of mutable routing configuration."""
    try:
        metadata = service.metadata
        uid, transaction = resource_ownership(metadata, sandbox_id, namespace,
            name=agent_service_name(sandbox_id), role=SERVICE_ROLE)
        version = metadata.resource_version
        known = {"managed", "project", "sandbox-id", "transaction-id", "resource",
                 "created-at", "expires-at", "kubernetes-proxy-version", "proxy-resource-uids", "proxy-token-sha256", "proxy-image", "kubernetes-network-version", "agent-service-version"}
        if (
            not agent_service_layout(metadata)
            or (transaction_id is not None and transaction != transaction_id)
            or type(version) is not str or not version
            or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in version)
            or any(key.startswith(LABEL_PREFIX) and key[len(LABEL_PREFIX):] not in known
                   for key in (*metadata.labels, *(metadata.annotations or {})))
        ):
            raise SandboxDeleteConflict()
        return uid, transaction, *lifecycle_annotations(metadata.annotations)
    except (AttributeError, TypeError, ValueError, SandboxQueryFailed):
        raise SandboxDeleteConflict() from None


def _allocated_addresses(spec) -> bool:
    """Accept valid Kubernetes-assigned single/dual-stack IPs and their optional defaulted fields."""
    try:
        address = ip_address(spec.cluster_ip)
        addresses = spec.cluster_ips if spec.cluster_ips is not None else [spec.cluster_ip]
        if not 1 <= len(addresses) <= 2 or addresses[0] != spec.cluster_ip or len(set(addresses)) != len(addresses):
            return False
        parsed = [ip_address(value) for value in addresses]
        if any(value.is_unspecified or value.is_multicast or value.is_loopback for value in parsed):
            return False
        if len(parsed) == 2 and parsed[0].version == parsed[1].version:
            return False
        families = ["IPv4" if value.version == 4 else "IPv6" for value in parsed]
        return (not address.is_unspecified and spec.ip_families in (None, families)
                and spec.ip_family_policy in (None, "SingleStack", "PreferDualStack", "RequireDualStack"))
    except (TypeError, ValueError):
        return False


def verify_agent_service(service, pod, *, expected_uid: str | None = None) -> None:
    """Require the Pod's original Service binding and exact internal Agent routing contract."""
    try:
        sandbox_id = pod.metadata.labels[LABEL_PREFIX + "sandbox-id"]
        transaction = pod.metadata.labels[LABEL_PREFIX + "transaction-id"]
        uid, _, created, expires = service_identity(service, sandbox_id, pod.metadata.namespace, transaction)
        spec = service.spec
        if (
            not agent_service_layout(pod.metadata) or service.metadata.deletion_timestamp is not None
            or pod.metadata.annotations.get(AGENT_SERVICE_UID_ANNOTATION) != uid
            or (expected_uid is not None and expected_uid != uid)
            or (created, expires) != lifecycle_annotations(pod.metadata.annotations)
            or spec.type != "ClusterIP" or not _allocated_addresses(spec)
            or spec.selector != primary_pod_labels(sandbox_id, transaction)
            or not isinstance(spec.ports, list) or len(spec.ports) != 1
            or spec.publish_not_ready_addresses not in (None, False)
            or spec.external_ips not in (None, []) or spec.external_name not in (None, "")
            or spec.load_balancer_ip not in (None, "") or spec.load_balancer_class is not None
            or spec.load_balancer_source_ranges not in (None, [])
            or spec.health_check_node_port not in (None, 0)
            or spec.allocate_load_balancer_node_ports not in (None, False)
            or spec.external_traffic_policy not in (None, "", "Cluster")
            or spec.internal_traffic_policy not in (None, "Cluster")
        ):
            raise SandboxQueryFailed()
        port = spec.ports[0]
        if (port.protocol != "TCP" or type(port.port) is not int or port.port != 8080
            or type(port.target_port) is not int or port.target_port != 8080
            or port.node_port not in (None, 0) or port.app_protocol is not None):
            raise SandboxQueryFailed()
    except (AttributeError, KeyError, TypeError, ValueError, SandboxDeleteConflict):
        raise SandboxQueryFailed() from None


class AgentServiceResource:
    """Operate on one exact service with the caller's shared, worker-owned ApiClient."""

    def __init__(self, api, sandbox_id: str, namespace: str, transaction_id: str | None = None):
        """Retain Core and Networking facades without creating or taking ownership of a transport."""
        self.core, self.api = api, api
        self.sandbox_id, self.namespace, self.transaction_id = sandbox_id, namespace, transaction_id
        self.name = agent_service_name(sandbox_id)
        self.deadline = monotonic() + SERVICE_TIMEOUT_SECONDS

    def _timeout(self):
        """Bound each request phase and the complete service operation with monotonic time."""
        remaining = self.deadline - monotonic()
        if remaining <= 0:
            raise SandboxDeleteFailed(self.sandbox_id)
        phase = min(0.5, remaining / 2)
        return phase, phase

    def _inventory(self):
        """Read one limited inventory without hiding other transactions or resource roles."""
        try:
            page = self.api.list_namespaced_service(
                self.namespace, label_selector=sandbox_pod_selector(self.sandbox_id),
                limit=2, watch=False, _request_timeout=self._timeout(),
            )
        except client.ApiException as error:
            if error.status == 404:
                raise KubernetesConfigurationError() from None
            raise
        try:
            if (
                not isinstance(page.items, list) or page.metadata is None or len(page.items) > 1
                or page.metadata._continue not in (None, "")
                or getattr(page.metadata, "remaining_item_count", None) not in (None, 0)
            ):
                raise SandboxDeleteConflict()
        except (AttributeError, TypeError):
            raise SandboxDeleteConflict() from None
        return page.items

    def _read(self):
        """Read the deterministic name even when label discovery returned no candidate."""
        try:
            return self.api.read_namespaced_service(self.name, self.namespace, _request_timeout=self._timeout())
        except client.ApiException as error:
            if error.status == 404:
                return None
            raise

    def ensure_absent(self) -> None:
        """Refuse any existing candidate or same-name object before the first mutation."""
        if self._inventory() or self._read() is not None:
            raise SandboxDeleteConflict()

    def lookup(self):
        """Observe a unique owned service, comparing discovery and exact-read identity."""
        candidates, service = self._inventory(), self._read()
        if service is None:
            if candidates:
                raise SandboxDeleteConflict()
            return None
        identity = service_identity(service, self.sandbox_id, self.namespace, self.transaction_id)
        if len(candidates) != 1 or service_identity(
            candidates[0], self.sandbox_id, self.namespace, self.transaction_id,
        ) != identity:
            raise SandboxDeleteConflict()
        return service

    def verify_for(self, pod, *, expected_uid: str | None = None) -> None:
        """Require a matching intact service for the new Pod layout, mapping only integrity errors to query failure."""
        try:
            service = self.lookup()
            if service is None:
                raise SandboxQueryFailed()
            verify_agent_service(service, pod, expected_uid=expected_uid)
        except SandboxDeleteConflict:
            raise SandboxQueryFailed() from None

    def delete(self, expected_uid: str) -> None:
        """Delete once with exact UID/version, then confirm disappearance without adopting replacements."""
        service = self.lookup()
        if service is not None:
            if service.metadata.uid != expected_uid:
                raise SandboxDeleteConflict()
            if service.metadata.deletion_timestamp is None:
                try:
                    self.api.delete_namespaced_service(
                        self.name, self.namespace,
                        body=client.V1DeleteOptions(preconditions=client.V1Preconditions(
                            uid=expected_uid, resource_version=service.metadata.resource_version,
                        )), _request_timeout=self._timeout(),
                    )
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
            try:
                current = self.lookup()
                if current is None:
                    namespace = self.core.read_namespace(self.namespace, _request_timeout=self._timeout())
                    if namespace.metadata.name != self.namespace:
                        raise KubernetesConfigurationError()
                    return
                if current.metadata.uid != expected_uid:
                    raise SandboxDeleteConflict()
            except (SandboxDeleteConflict, SandboxDeleteFailed, KubernetesConfigurationError):
                raise
            except client.ApiException as error:
                if error.status in {401, 403}:
                    raise KubernetesPermissionDenied() from None
                if error.status == 404:
                    raise KubernetesConfigurationError() from None
            except Exception:
                pass
            remaining = self.deadline - monotonic()
            if remaining <= 0:
                raise SandboxDeleteFailed(self.sandbox_id)
            sleep(min(0.1, remaining))
