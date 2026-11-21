"""Read-only sandbox listing, runtime reconciliation and readiness checks."""

import re
from dataclasses import replace

from control_plane.domain.errors import (
    DockerUnavailable,
    SandboxAgentUnavailable,
    SandboxEndpointUnavailable,
    SandboxListUnavailable,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    SandboxNotFound,
    SandboxNotReady,
    SandboxQueryFailed,
)
from control_plane.domain.sandbox import (
    SandboxLifecycleRecord,
    SandboxListResult,
    SandboxQueryResult,
    SandboxReadyResult,
    validate_sandbox_list_query,
)
from control_plane.runtime.base_runtime import AgentHealthProbe, SandboxRuntime
from control_plane.services.workspace_service import WorkspaceService
from control_plane.storage.base import LifecycleStore


def require_sandbox_id(sandbox_id: str) -> None:
    """Reject malformed public instance IDs before reading or mutating resources."""
    if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
        raise SandboxNotFound()


def identities_match(
    sandbox_id: str, record: SandboxLifecycleRecord, observed: SandboxQueryResult,
) -> bool:
    """Compare the immutable facts needed before combining runtime and persisted state."""
    return (
        record.sandbox_id == sandbox_id == observed.sandbox_id
        and record.transaction_id == observed.transaction_id
        and record.created_at == observed.created_at
        and record.workspace_id == observed.workspace_id
    )


async def read_lifecycle_record(store: LifecycleStore, sandbox_id: str) -> SandboxLifecycleRecord | None:
    """Translate storage read failures consistently at the service boundary."""
    try:
        return await store.get(sandbox_id)
    except Exception as error:
        raise SandboxMetadataUnavailable(sandbox_id) from error


class SandboxQueries:
    """Reconcile read snapshots without changing lifecycle metadata."""

    def __init__(
        self, runtime: SandboxRuntime, store: LifecycleStore, workspaces: WorkspaceService,
        agent_probe: AgentHealthProbe | None, max_instances: int,
    ) -> None:
        """Use the service's existing adapters and capacity for consistent read results."""
        self._runtime = runtime
        self._store = store
        self._workspaces = workspaces
        self._agent_probe = agent_probe
        self._max_instances = max_instances

    async def list_sandboxes(self, *, limit: int = 20, cursor: str | None = None) -> SandboxListResult:
        """List persisted public summaries without reading runtime facts or execution credentials."""
        validate_sandbox_list_query(limit, cursor)
        try:
            page = await self._store.list_sandboxes(limit=limit, cursor=cursor)
        except Exception as error:
            raise SandboxListUnavailable() from error
        return SandboxListResult(page.items, page.next_cursor, page.managed_count, self._max_instances)

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Read runtime facts and merge persisted expiration only after matching identities."""
        require_sandbox_id(sandbox_id)
        observed = await self._runtime.get(sandbox_id)
        record = await read_lifecycle_record(self._store, sandbox_id)
        if record is None:
            if observed.workspace_id is not None:
                raise SandboxMetadataConflict(sandbox_id)
            return observed
        # Docker labels retain initial times; the store holds renewed expiration.
        # Match the transaction and created_at before combining resources with metadata.
        if not identities_match(sandbox_id, record, observed):
            raise SandboxMetadataConflict(sandbox_id)
        await self._workspaces.verify_observed(record, observed)
        return replace(observed, expires_at=record.expires_at)

    async def check_ready(self, sandbox_id: str) -> SandboxReadyResult:
        """Verify runtime and metadata, then check the Agent through its verified local entry."""
        observed = await self.get(sandbox_id)
        if observed.sandbox_id != sandbox_id:
            raise SandboxQueryFailed()
        if observed.status != "ready":
            raise SandboxNotReady()
        try:
            # Local /healthz stays anonymous. Readiness observes the existing
            # HTTP chain without handing execution credentials to the probe.
            endpoint = await self._runtime.get_endpoint(sandbox_id)
        except (DockerUnavailable, SandboxQueryFailed, SandboxEndpointUnavailable):
            # Retain the existing endpoint route's policy for entry lookup failures.
            raise SandboxEndpointUnavailable() from None
        if endpoint.sandbox_id != sandbox_id:
            raise SandboxEndpointUnavailable()
        if self._agent_probe is None:
            raise SandboxAgentUnavailable()
        await self._agent_probe.check(endpoint.url)
        return SandboxReadyResult(sandbox_id)
