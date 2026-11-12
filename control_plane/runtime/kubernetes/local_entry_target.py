"""Read-only original target snapshots for WSL Kubernetes authenticated proxy sessions."""

import asyncio
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from uuid import UUID

from control_plane.core.errors import KubernetesOperationUnsupported, SandboxQueryFailed
from control_plane.core.tasks import settle_task
from control_plane.runtime.kubernetes.pod_query import (
    PROXY_DIGEST_ANNOTATION, PROXY_LAYOUT_ANNOTATION, PROXY_LAYOUT_VERSION,
    PROXY_PERSISTENT_LAYOUT_VERSION, sandbox_pod_name,
)
from control_plane.runtime.kubernetes.proxy_resources import ProxyResources
from control_plane.runtime.kubernetes.proxy_service import proxy_service_name
from control_plane.runtime.kubernetes.proxy_spec import proxy_pod_name

_RESOURCE_KEYS = frozenset({"agent", "policy", "service", "secret", "proxy", "ingress", "isolation", "proxy_service"})
_LAYOUT_RESOURCE_KEYS = {
    PROXY_LAYOUT_VERSION: _RESOURCE_KEYS,
    PROXY_PERSISTENT_LAYOUT_VERSION: _RESOURCE_KEYS | {"workspace"},
}


@dataclass(frozen=True)
class LocalEntryTarget:
    """Hold immutable original identities and business time, never a credential or arbitrary target."""

    sandbox_id: str
    transaction_id: str
    namespace: str
    proxy_pod_name: str
    proxy_pod_uid: str
    proxy_service_name: str
    proxy_service_uid: str
    resource_uids: tuple[tuple[str, str], ...]
    created_at: datetime
    expires_at: datetime
    credential_digest: str = field(repr=False)
    auth_version: str = "1"
    layout_version: str = "2"

    def __post_init__(self) -> None:
        """Reject incomplete or forged resolver output before it can become a kubectl argument."""
        try:
            sandbox_pod_name(self.sandbox_id)
            resource_keys = _LAYOUT_RESOURCE_KEYS.get(self.layout_version)
            if (type(self.transaction_id) is not str
                or re.fullmatch(r"txn_[0-9a-f]{32}", self.transaction_id) is None
                or type(self.namespace) is not str
                or re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", self.namespace) is None
                or type(self.credential_digest) is not str
                or re.fullmatch(r"[0-9a-f]{64}", self.credential_digest) is None
                or self.auth_version != "1"
                or resource_keys is None
                or self.proxy_pod_name != proxy_pod_name(self.sandbox_id)
                or self.proxy_service_name != proxy_service_name(self.sandbox_id)
                or type(self.resource_uids) is not tuple or len(self.resource_uids) != len(resource_keys)
                or any(type(pair) is not tuple or len(pair) != 2 for pair in self.resource_uids)):
                raise ValueError()
            records = dict(self.resource_uids)
            if (set(records) != resource_keys or len(set(records.values())) != len(resource_keys)
                or any(type(uid) is not str or str(UUID(uid)) != uid for uid in records.values())
                or records["proxy"] != self.proxy_pod_uid or records["proxy_service"] != self.proxy_service_uid
                or any(not isinstance(value, datetime) or value.utcoffset() != timedelta(0)
                       for value in (self.created_at, self.expires_at))
                or self.expires_at < self.created_at):
                raise ValueError()
        except Exception:
            raise SandboxQueryFailed() from None


async def resolve_local_entry_target(runtime, sandbox_id: str) -> LocalEntryTarget:
    """Resolve one ready v2/v3 proxy through worker-owned, bounded, read-only Runtime checks."""
    sandbox_pod_name(sandbox_id)

    def resolve(api):
        """Reuse the complete Runtime query before extracting only nonsecret original UID bindings."""
        result = runtime._read_primary_pod(api, sandbox_id)
        manager = ProxyResources(api, sandbox_id, runtime._namespace, result.transaction_id)
        found = manager.inventory()
        agent = found.get("agent")
        if agent is None:
            raise SandboxQueryFailed()
        layout_version = agent.metadata.annotations.get(PROXY_LAYOUT_ANNOTATION)
        if layout_version not in _LAYOUT_RESOURCE_KEYS:
            raise KubernetesOperationUnsupported()
        verified = manager.query(agent)
        if result.status != "ready" or verified.status != "ready" or verified != result:
            raise SandboxQueryFailed()
        bindings = manager._bindings(found)
        return LocalEntryTarget(
            sandbox_id=sandbox_id, transaction_id=result.transaction_id, namespace=runtime._namespace,
            proxy_pod_name=found["proxy"].metadata.name, proxy_pod_uid=bindings["proxy"],
            proxy_service_name=found["proxy_service"].metadata.name, proxy_service_uid=bindings["proxy_service"],
            resource_uids=tuple(sorted(bindings.items())), created_at=result.created_at, expires_at=result.expires_at,
            credential_digest=agent.metadata.annotations[PROXY_DIGEST_ANNOTATION], auth_version="1",
            layout_version=layout_version,
        )

    worker = asyncio.create_task(asyncio.to_thread(runtime._call_and_close, resolve))
    return await settle_task(worker, propagate_cancellation=True)
