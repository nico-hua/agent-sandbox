"""Pure specifications and integrity checks for the Runtime v2 proxy-facing ClusterIP Service."""

from kubernetes import client

from control_plane.core.errors import SandboxDeleteConflict, SandboxQueryFailed
from control_plane.runtime.kubernetes.agent_service import verify_clusterip_service
from control_plane.runtime.kubernetes.pod_query import (
    LABEL_PREFIX, PROXY_DIGEST_ANNOTATION, PROXY_IMAGE_ANNOTATION,
    PROXY_LAYOUT_ANNOTATION, PROXY_LAYOUT_VERSION, PROXY_RECORD_ANNOTATION,
    lifecycle_annotations, network_layout, pod_identity, primary_pod_labels,
    proxy_layout, resource_ownership, sandbox_pod_name,
)

PROXY_SERVICE_ROLE = "proxy-service"
_MARKERS = frozenset({
    "managed", "project", "sandbox-id", "transaction-id", "resource", "created-at", "expires-at",
    "kubernetes-network-version", "network-policy-uid", "agent-service-version", "agent-service-uid",
    "kubernetes-proxy-version", "proxy-resource-uids", "proxy-token-sha256", "proxy-image",
    "proxy-upstream-version",
})


def proxy_service_name(sandbox_id: str) -> str:
    """Derive the stable proxy-facing Service name from the validated public sandbox ID."""
    return sandbox_pod_name(sandbox_id) + "-proxy"


def build_proxy_service(agent_pod: client.V1Pod) -> client.V1Service:
    """Build an independent internal Service from an observed Runtime v2 Agent without I/O."""
    try:
        metadata = agent_pod.metadata
        sandbox_id = metadata.labels[LABEL_PREFIX + "sandbox-id"]
        _, transaction, _, _ = pod_identity(agent_pod, sandbox_id, metadata.namespace)
        if (not network_layout(metadata) or not proxy_layout(metadata)
            or metadata.annotations.get(PROXY_LAYOUT_ANNOTATION) != PROXY_LAYOUT_VERSION
            or metadata.deletion_timestamp is not None):
            raise SandboxQueryFailed()
        annotations = {LABEL_PREFIX + key: metadata.annotations[LABEL_PREFIX + key]
                       for key in ("created-at", "expires-at")}
        annotations[PROXY_LAYOUT_ANNOTATION] = PROXY_LAYOUT_VERSION
        for key in (PROXY_DIGEST_ANNOTATION, PROXY_IMAGE_ANNOTATION):
            if key in metadata.annotations:
                annotations[key] = metadata.annotations[key]
        labels = primary_pod_labels(sandbox_id, transaction)
        return client.V1Service(
            api_version="v1", kind="Service",
            metadata=client.V1ObjectMeta(
                name=proxy_service_name(sandbox_id), namespace=metadata.namespace,
                labels={**labels, LABEL_PREFIX + "resource": PROXY_SERVICE_ROLE},
                annotations=annotations,
            ),
            spec=client.V1ServiceSpec(
                type="ClusterIP", selector={**labels, LABEL_PREFIX + "resource": "proxy"},
                ports=[client.V1ServicePort(name="proxy-http", protocol="TCP", port=8080, target_port=8080)],
                publish_not_ready_addresses=False,
            ),
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        raise SandboxQueryFailed() from None


def proxy_service_identity(service, sandbox_id: str, namespace: str, transaction_id: str | None = None):
    """Require exact cleanup ownership and Runtime v2 while allowing routing or credential damage."""
    try:
        metadata = service.metadata
        uid, transaction = resource_ownership(
            metadata, sandbox_id, namespace, name=proxy_service_name(sandbox_id),
            role=PROXY_SERVICE_ROLE, check_proxy_layout=False,
        )
        version = metadata.resource_version
        if (
            not network_layout(metadata)
            or metadata.annotations.get(PROXY_LAYOUT_ANNOTATION) != PROXY_LAYOUT_VERSION
            or (transaction_id is not None and transaction != transaction_id)
            or type(version) is not str or not version
            or any(char.isspace() or ord(char) < 32 or ord(char) == 127 for char in version)
            or any(key in metadata.labels for key in (
                PROXY_LAYOUT_ANNOTATION, PROXY_RECORD_ANNOTATION, PROXY_DIGEST_ANNOTATION, PROXY_IMAGE_ANNOTATION,
            ))
            or any(key.startswith(LABEL_PREFIX) and key[len(LABEL_PREFIX):] not in _MARKERS
                   for key in (*metadata.labels, *(metadata.annotations or {})))
        ):
            raise SandboxDeleteConflict()
        return uid, transaction, *lifecycle_annotations(metadata.annotations)
    except (AttributeError, KeyError, TypeError, ValueError, SandboxQueryFailed):
        raise SandboxDeleteConflict() from None


def verify_proxy_service(service, agent_pod, *, expected_uid: str) -> None:
    """Verify the original Service and exact internal proxy routing before or after proxy creation."""
    try:
        metadata = agent_pod.metadata
        sandbox_id = metadata.labels[LABEL_PREFIX + "sandbox-id"]
        _, transaction, created, expires = pod_identity(agent_pod, sandbox_id, metadata.namespace)
        uid, _, service_created, service_expires = proxy_service_identity(
            service, sandbox_id, metadata.namespace, transaction,
        )
        if (
            metadata.annotations.get(PROXY_LAYOUT_ANNOTATION) != PROXY_LAYOUT_VERSION
            or not proxy_layout(metadata) or not proxy_layout(service.metadata)
            or expected_uid != uid or (service_created, service_expires) != (created, expires)
            or service.metadata.deletion_timestamp is not None
            or any(service.metadata.annotations.get(key) != metadata.annotations.get(key)
                   for key in (PROXY_DIGEST_ANNOTATION, PROXY_IMAGE_ANNOTATION))
        ):
            raise SandboxQueryFailed()
        verify_clusterip_service(service.spec, {
            **primary_pod_labels(sandbox_id, transaction), LABEL_PREFIX + "resource": "proxy",
        })
        if service.spec.ports[0].name != "proxy-http":
            raise SandboxQueryFailed()
    except (AttributeError, KeyError, IndexError, TypeError, ValueError, SandboxDeleteConflict):
        raise SandboxQueryFailed() from None
