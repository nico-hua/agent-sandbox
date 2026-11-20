"""Reusable API and lifecycle service fakes."""

import asyncio
import hashlib
import threading
from dataclasses import replace
from datetime import (
    UTC,
    datetime,
    timedelta,
)
from pathlib import Path
from typing import Any

from fastapi import FastAPI

from control_plane.domain.errors import (
    DockerUnavailable,
    SandboxDeleteFailed,
    WorkspaceConflict,
)
from control_plane.domain.models import (
    SandboxAccessCredential,
    SandboxEndpoint,
    SandboxLifecycleRecord,
    SandboxListPage,
    SandboxManagementSummary,
    SandboxQueryResult,
    SandboxResourceInspection,
    SandboxSpec,
    encode_sandbox_cursor,
    validate_sandbox_list_query,
)
from control_plane.runtime.base import AgentHealthProbe, SandboxRuntime
from control_plane.storage.base import (
    LifecycleInstanceLimitExceeded,
    LifecycleStore,
    LifecycleStoreConflict,
    LifecycleStoreError,
)
from control_plane.storage.sqlite_store import SQLiteLifecycleStore


def create_test_app(
    runtime: SandboxRuntime | None = None,
    readiness_timeout: float = 1.0,
    sandbox_query_timeout: float = 1.0,
    *,
    store: LifecycleStore | None = None,
    agent_probe: AgentHealthProbe | None = None,
    **options: Any,
) -> FastAPI:
    """Exercise real routing and lifespan with controlled dependencies and settings."""
    from control_plane.app import _assemble_application
    from control_plane.config.settings import ControlPlaneSettings
    from control_plane.runtime.agent_probe import HTTPAgentHealthProbe
    from control_plane.runtime.docker.adapter import DockerRuntime
    from control_plane.runtime.docker.images import build_environment_images

    settings = ControlPlaneSettings(
        readiness_timeout=readiness_timeout, sandbox_query_timeout=sandbox_query_timeout, **options,
    )
    backend = runtime if runtime is not None else DockerRuntime(additional_environments=settings.additional_environments)
    images = (
        backend.environment_images if isinstance(backend, DockerRuntime)
        else build_environment_images(settings.additional_environments)
    )
    lifecycle_store = store if store is not None else SQLiteLifecycleStore(settings.database_path)
    probe = agent_probe if agent_probe is not None else HTTPAgentHealthProbe()
    return _assemble_application(settings, backend, lifecycle_store, probe, frozenset(images))


class FakeLifecycleStore:
    """Persist records and enforce identity/version checks without touching a database."""

    def __init__(self) -> None:
        """Initialize per-test metadata and explicit initialization call tracking."""
        self.records: dict[str, SandboxLifecycleRecord] = {}
        self.credentials: dict[str, SandboxAccessCredential] = {}
        self.initialize_calls = 0

    async def initialize(self) -> None:
        """Record application startup initialization without changing stored records."""
        self.initialize_calls += 1

    async def create(self, record: SandboxLifecycleRecord, *, credential: SandboxAccessCredential | None = None, max_instances: int = 10) -> None:
        """Insert real test metadata while rejecting an already occupied identity."""
        if record.sandbox_id in self.records:
            raise LifecycleStoreConflict("already_exists")
        if credential is not None and (credential.sandbox_id, credential.transaction_id) != (record.sandbox_id, record.transaction_id):
            raise LifecycleStoreConflict("transaction_mismatch")
        if len(self.records) >= max_instances:
            raise LifecycleInstanceLimitExceeded()
        self.records[record.sandbox_id] = record
        if credential is not None:
            self.credentials[record.sandbox_id] = credential

    async def list_sandboxes(self, *, limit: int = 20, cursor: str | None = None) -> SandboxListPage:
        """Expose the same stable public page semantics without credential lookup."""
        position = validate_sandbox_list_query(limit, cursor)
        records = sorted(self.records.values(), key=lambda row: (row.created_at, row.sandbox_id))
        if position is not None:
            anchor = (datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=position[0]), position[1])
            records = [row for row in records if (row.created_at, row.sandbox_id) > anchor]
        items = tuple(SandboxManagementSummary(row.sandbox_id, row.management_state, row.created_at,
                      row.expires_at, row.workspace_id) for row in records[:limit])
        next_cursor = encode_sandbox_cursor(items[-1].created_at, items[-1].sandbox_id) if len(records) > limit else None
        return SandboxListPage(items, next_cursor, len(self.records))

    async def get_credential(self, sandbox_id: str, transaction_id: str) -> SandboxAccessCredential | None:
        """Return a secret only for the current lifecycle and credential transaction."""
        record = self.records.get(sandbox_id)
        if record is None:
            return None
        credential = self.credentials.get(sandbox_id)
        if record.transaction_id != transaction_id or (credential is not None and credential.transaction_id != transaction_id):
            raise LifecycleStoreConflict("transaction_mismatch")
        return credential

    async def get(self, sandbox_id: str) -> SandboxLifecycleRecord | None:
        """Return the currently persisted test record."""
        return self.records.get(sandbox_id)

    async def activate(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> SandboxLifecycleRecord:
        """Confirm one pending creation while preserving transaction and version guards."""
        record = self._match(sandbox_id, transaction_id, expected_version)
        if record.management_state != "pending":
            raise LifecycleStoreConflict("state_conflict")
        updated = replace(record, management_state="active", version=record.version + 1)
        self.records[sandbox_id] = updated
        return updated

    async def mark_deleting(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> SandboxLifecycleRecord:
        """Claim manual deletion, allowing an already claimed version to be retried."""
        record = self._match(sandbox_id, transaction_id, expected_version)
        if record.management_state == "deleting":
            return record
        updated = replace(record, management_state="deleting", version=record.version + 1)
        self.records[sandbox_id] = updated
        return updated

    async def claim_expired(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
        now: datetime,
    ) -> SandboxLifecycleRecord | None:
        """Return a claim only if the exact active version remains expired."""
        record = self.records.get(sandbox_id)
        if (
            record is None or record.transaction_id != transaction_id
            or record.version != expected_version or record.management_state != "active"
            or record.expires_at > now
        ):
            return None
        return await self.mark_deleting(
            sandbox_id, transaction_id, expected_version=expected_version,
        )

    async def list_reap_candidates(
        self, now: datetime, *, limit: int = 100,
    ) -> list[SandboxLifecycleRecord]:
        """Bound and order only expired active or retryable deleting metadata."""
        candidates = [
            record for record in self.records.values()
            if record.management_state == "deleting"
            or (record.management_state == "active" and record.expires_at <= now)
        ]
        return sorted(candidates, key=lambda record: (record.expires_at, record.sandbox_id))[:limit]

    async def list_pending(self, *, limit: int = 100) -> list[SandboxLifecycleRecord]:
        """Return a stable bounded pending batch without changing test metadata."""
        if type(limit) is not int or not 1 <= limit <= 2**63 - 1:
            raise ValueError("limit must be a positive SQLite integer")
        candidates = [
            record for record in self.records.values() if record.management_state == "pending"
        ]
        return sorted(candidates, key=lambda record: (record.created_at, record.sandbox_id))[:limit]

    def _match(
        self, sandbox_id: str, transaction_id: str, expected_version: int,
    ) -> SandboxLifecycleRecord:
        """Require exact identity and version before test persistence changes."""
        record = self.records[sandbox_id]
        if record.transaction_id != transaction_id:
            raise LifecycleStoreConflict("transaction_mismatch")
        if record.version != expected_version:
            raise LifecycleStoreConflict("version_conflict")
        return record

    async def delete(
        self, sandbox_id: str, transaction_id: str, *, expected_version: int,
    ) -> None:
        """Match transaction and version before idempotently removing metadata."""
        record = self.records.get(sandbox_id)
        if record is None:
            return
        if record.transaction_id != transaction_id:
            raise LifecycleStoreConflict("transaction_mismatch")
        if record.version != expected_version:
            raise LifecycleStoreConflict("version_conflict")
        del self.records[sandbox_id]
        self.credentials.pop(sandbox_id, None)


class APIRuntime(SandboxRuntime):
    """Record all backend calls and provide deterministic runtime observations."""

    def __init__(self, error: BaseException | None = None) -> None:
        """Initialize call records and an optional domain failure."""
        self.calls = 0
        self.error = error
        self.specs: list[SandboxSpec] = []
        self.sandbox_ids: list[str] = []
        self.deletions: list[tuple[str, bool]] = []
        self.deletion_transactions: list[str | None] = []
        self.transitions: list[tuple[str, str, str]] = []
        self.result = SandboxQueryResult(
            sandbox_id="sbx_0123456789abcdef0123456789abcdef",
            status="ready",
            reason="health_check_passed",
            message="Agent health check passed",
        )

    async def ping(self) -> None:
        """Record readiness and propagate the configured backend error."""
        self.calls += 1
        if self.error is not None:
            raise self.error

    async def create(self, spec: SandboxSpec) -> None:
        """Record the service-generated specification before creation succeeds."""
        self.calls += 1
        self.specs.append(spec)
        if self.error is not None:
            raise self.error
        self.result = replace(
            self.result, sandbox_id=spec.sandbox_id,
            transaction_id=spec.transaction_id,
            created_at=spec.created_at, expires_at=spec.expires_at,
        )

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Record the validated public ID and return the configured state."""
        self.calls += 1
        self.sandbox_ids.append(sandbox_id)
        if self.error is not None:
            raise self.error
        return self.result

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Record a verified ID and return a deterministic local entry."""
        self.sandbox_ids.append(sandbox_id)
        if self.error is not None:
            raise self.error
        spec = next((item for item in self.specs if item.sandbox_id == sandbox_id), None)
        if spec is None:
            return SandboxEndpoint(sandbox_id, "http://127.0.0.1:49153")
        return SandboxEndpoint(
            sandbox_id, "http://127.0.0.1:49153", auth_required=True,
            transaction_id=spec.transaction_id, created_at=spec.created_at,
            credential_digest=hashlib.sha256(spec.access_token.encode()).hexdigest(),
            auth_version="1",
        )

    async def inspect_resources(
        self, sandbox_id: str, *, expected_transaction_id: str,
    ) -> SandboxResourceInspection:
        """Expose a read-only fake observation without creating or repairing resources."""
        self.calls += 1
        if self.error is not None:
            raise self.error
        return SandboxResourceInspection(sandbox_id, "not_found", "resources_not_found")

    async def pause(self, sandbox_id: str, *, expected_transaction_id: str) -> None:
        """Record exact-transaction freezing and expose a paused observation."""
        self.calls += 1
        self.transitions.append(("pause", sandbox_id, expected_transaction_id))
        if self.error is not None:
            raise self.error
        self.result = replace(self.result, status="paused", reason="container_paused")

    async def resume(self, sandbox_id: str, *, expected_transaction_id: str) -> None:
        """Record exact-transaction thawing and resume the existing fake observation."""
        self.calls += 1
        self.transitions.append(("resume", sandbox_id, expected_transaction_id))
        if self.error is not None:
            raise self.error
        self.result = replace(self.result, status="ready", reason="health_check_passed")

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Record deletion policy and propagate the configured backend failure."""
        self.calls += 1
        self.deletions.append((sandbox_id, delete_workspace))
        self.deletion_transactions.append(expected_transaction_id)
        if self.error is not None:
            raise self.error


class WorkspaceAPIRuntime(APIRuntime):
    """Keep independent workspace and instance facts outside SQLite."""

    def __init__(self) -> None:
        """Initialize per-test independent resources and operation records."""
        super().__init__()
        self.workspaces = {}
        self.instances = {}
        self.workspace_calls = []

    async def resolve_environment_image(self, environment: str) -> str:
        """Resolve a stable image identity without contacting Docker."""
        self.workspace_calls.append(("image", environment))
        return "sha256:" + "1" * 64

    async def create_workspace(self, record) -> None:
        """Store the immutable workspace identity once."""
        self.workspace_calls.append(("create", record.workspace_id))
        self.workspaces[record.workspace_id] = record

    async def inspect_workspace(self, record) -> None:
        """Require persisted workspace identity to match runtime facts."""
        self.workspace_calls.append(("inspect", record.workspace_id))
        assert self.workspaces[record.workspace_id].creation_token == record.creation_token

    async def delete_workspace(self, record) -> None:
        """Remove only the requested independent workspace."""
        self.workspace_calls.append(("delete", record.workspace_id))
        self.workspaces.pop(record.workspace_id, None)

    async def create(self, spec) -> None:
        """Record a distinct instance with its immutable workspace binding."""
        await super().create(spec)
        self.result = replace(
            self.result, workspace_id=spec.workspace.workspace_id if spec.workspace else None,
            workspace=spec.workspace,
        )
        self.instances[spec.sandbox_id] = self.result

    async def get(self, sandbox_id):
        """Return the requested instance rather than the latest-created instance."""
        return self.instances[sandbox_id]

    async def delete(self, sandbox_id, *, expected_workspace=None, **kwargs) -> None:
        """Delete an instance while preserving independent workspace facts."""
        await super().delete(sandbox_id, **kwargs)
        self.instances.pop(sandbox_id, None)


REAP_NOW = datetime(2026, 10, 19, 8, 30, 0, 123456, tzinfo=UTC)


FIXED_SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"


class FakeRuntime(SandboxRuntime):
    """Implement the runtime contract and record service delegation."""

    def __init__(self, error: Exception | None = None) -> None:
        """Initialize readiness, specification and query records with an optional error."""
        self.error = error
        self.ping_calls = 0
        self.specs: list[SandboxSpec] = []
        self.queries: list[str] = []
        self.deletions: list[tuple[str, bool]] = []
        self.transitions: list[tuple[str, str, str]] = []
        self.query_result = SandboxQueryResult(
            sandbox_id=FIXED_SANDBOX_ID,
            status="running",
            reason="health_check_not_configured",
            message="container is running but no health check is configured",
        )

    async def ping(self) -> None:
        """Record a ping and propagate the configured domain failure."""
        self.ping_calls += 1
        if self.error is not None:
            raise self.error

    async def create(self, spec: SandboxSpec) -> None:
        """Record the specification and raise the configured error."""
        self.specs.append(spec)
        if self.error is not None:
            raise self.error
        self.query_result = replace(
            self.query_result, sandbox_id=spec.sandbox_id,
            transaction_id=spec.transaction_id, created_at=spec.created_at,
            expires_at=spec.expires_at,
        )

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Record one status query and return the configured observation."""
        self.queries.append(sandbox_id)
        if self.error is not None:
            raise self.error
        return self.query_result

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Record a verified ID and return a deterministic local entry."""
        self.queries.append(sandbox_id)
        if self.error is not None:
            raise self.error
        spec = next((item for item in self.specs if item.sandbox_id == sandbox_id), None)
        if spec is None:
            return SandboxEndpoint(sandbox_id, "http://127.0.0.1:49153")
        return SandboxEndpoint(
            sandbox_id, "http://127.0.0.1:49153", auth_required=True,
            transaction_id=spec.transaction_id, created_at=spec.created_at,
            credential_digest=hashlib.sha256(spec.access_token.encode()).hexdigest(),
            auth_version="1",
        )

    async def inspect_resources(
        self, sandbox_id: str, *, expected_transaction_id: str,
    ) -> SandboxResourceInspection:
        """Provide an explicit read-only fake inspection for the backend contract."""
        if self.error is not None:
            raise self.error
        return SandboxResourceInspection(sandbox_id, "not_found", "resources_not_found")

    async def pause(self, sandbox_id: str, *, expected_transaction_id: str) -> None:
        """Record the verified transaction and freeze the fake observation without replacing it."""
        self.transitions.append(("pause", sandbox_id, expected_transaction_id))
        if self.error is not None:
            raise self.error
        self.query_result = replace(self.query_result, status="paused", reason="container_paused")

    async def resume(self, sandbox_id: str, *, expected_transaction_id: str) -> None:
        """Record thawing without promising Agent health or starting another container."""
        self.transitions.append(("resume", sandbox_id, expected_transaction_id))
        if self.error is not None:
            raise self.error
        self.query_result = replace(self.query_result, status="running", reason="health_check_not_configured")

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Record explicit workspace policy and preserve runtime failures."""
        self.deletions.append((sandbox_id, delete_workspace))
        if self.error is not None:
            raise self.error


class ReapRuntime(FakeRuntime):
    """Expose only precise deletion effects and gates without connecting to Docker."""

    def __init__(self) -> None:
        """Initialize per-instance failures and a reusable deletion synchronization gate."""
        super().__init__()
        self.expected_deletions: list[tuple[str, bool, str | None]] = []
        self.failures: set[str] = set()
        self.deleted: set[str] = set()
        self.block_delete = False
        self.delete_entered = asyncio.Event()
        self.delete_release = asyncio.Event()

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Complete idempotent deletion only after the test allows its critical section."""
        self.expected_deletions.append((sandbox_id, delete_workspace, expected_transaction_id))
        self.delete_entered.set()
        if self.block_delete:
            await self.delete_release.wait()
        if sandbox_id in self.failures:
            raise SandboxDeleteFailed(sandbox_id)
        self.deleted.add(sandbox_id)


async def add_record(
    store: SQLiteLifecycleStore, number: int, *, state: str = "active",
    expires_at: datetime = REAP_NOW,
) -> SandboxLifecycleRecord:
    """Persist one deterministic identity and use real guarded state transitions."""
    record = SandboxLifecycleRecord(
        sandbox_id=f"sbx_{number:032x}", transaction_id=f"txn_{number:032x}",
        created_at=REAP_NOW - timedelta(hours=1), expires_at=expires_at,
    )
    await store.create(record)
    if state in ("active", "deleting"):
        record = await store.activate(
            record.sandbox_id, record.transaction_id, expected_version=record.version,
        )
    if state == "deleting":
        record = await store.mark_deleting(
            record.sandbox_id, record.transaction_id, expected_version=record.version,
        )
    return record


class FaultStore(SQLiteLifecycleStore):
    """Gate actual SQLite workers and inject failures without replacing persistence."""

    def __init__(self, database: Path) -> None:
        """Initialize a real file store with deterministic worker coordination."""
        super().__init__(database)
        self.block_write = False
        self.block_delete = False
        self.fail_write = False
        self.fail_after_commit = False
        self.fail_read = False
        self.fail_delete = False
        self.block_activation = False
        self.fail_activation = False
        self.fail_after_activation = False
        self.write_entered = threading.Event()
        self.write_release = threading.Event()
        self.delete_entered = threading.Event()
        self.delete_release = threading.Event()
        self.activation_entered = threading.Event()
        self.activation_release = threading.Event()

    def _create_sync(self, record, credential=None, max_instances=10) -> None:
        """Delay a real commit until released, then optionally fail before or after it."""
        if self.block_write:
            self.write_entered.set()
            assert self.write_release.wait(timeout=3), "write gate was not released"
        if self.fail_write:
            raise LifecycleStoreError()
        super()._create_sync(record, credential, max_instances)
        if self.fail_after_commit:
            raise LifecycleStoreError()

    async def get(self, sandbox_id: str):
        """Read real persisted records unless the read failure is explicitly enabled."""
        if self.fail_read:
            raise LifecycleStoreError()
        return await super().get(sandbox_id)

    def _activate_sync(self, sandbox_id: str, transaction_id: str, expected_version: int):
        """Gate the real activation worker to expose late commits and stable failures."""
        if self.block_activation:
            self.activation_entered.set()
            assert self.activation_release.wait(timeout=3), "activation gate was not released"
        if self.fail_activation:
            raise LifecycleStoreError()
        result = super()._activate_sync(sandbox_id, transaction_id, expected_version)
        if self.fail_after_activation:
            raise LifecycleStoreError()
        return result

    def _delete_sync(self, sandbox_id: str, transaction_id: str, expected_version: int) -> None:
        """Gate guarded real deletion and preserve the row when failure is enabled."""
        if self.block_delete:
            self.delete_entered.set()
            assert self.delete_release.wait(timeout=3), "delete gate was not released"
        if self.fail_delete:
            raise LifecycleStoreError()
        super()._delete_sync(sandbox_id, transaction_id, expected_version)


class ControlledRuntime(FakeRuntime):
    """Expose creation, rollback and deletion boundaries independently of SQLite."""

    def __init__(self, store: FaultStore) -> None:
        """Set independent runtime failure and cancellation controls."""
        super().__init__()
        self.store = store
        self.create_error: Exception | None = None
        self.delete_error: Exception | None = None
        self.rollback_error: Exception | None = None
        self.block_create = False
        self.block_rollback = False
        self.block_delete = False
        self.ignore_create_cancel = False
        self.create_entered = asyncio.Event()
        self.create_release = asyncio.Event()
        self.rollback_entered = asyncio.Event()
        self.rollback_release = asyncio.Event()
        self.delete_entered = asyncio.Event()
        self.delete_release = asyncio.Event()
        self.expected_deletions: list[tuple[str, bool, str | None]] = []
        self.live_resources: set[tuple[str, str]] = set()

    async def create(self, spec: SandboxSpec) -> None:
        """Assert durable prewrite and emulate confirmed rollback or a late commit."""
        self.specs.append(spec)
        record = await self.store.get(spec.sandbox_id)
        assert record is not None and record.transaction_id == spec.transaction_id
        assert record.management_state == "pending" and record.version == 0
        self.create_entered.set()
        if self.block_create:
            try:
                await self.create_release.wait()
            except asyncio.CancelledError:
                self.rollback_entered.set()
                if self.ignore_create_cancel:
                    await self.create_release.wait()
                else:
                    if self.block_rollback:
                        await self.rollback_release.wait()
                    if self.rollback_error is not None:
                        raise self.rollback_error
                    raise
        if self.create_error is not None:
            raise self.create_error
        self.query_result = replace(
            self.query_result, sandbox_id=spec.sandbox_id,
            transaction_id=spec.transaction_id, created_at=spec.created_at,
            expires_at=spec.expires_at,
        )
        self.live_resources.add((spec.sandbox_id, spec.transaction_id))

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Record identity protection and complete deletion only after the test releases it."""
        self.expected_deletions.append((sandbox_id, delete_workspace, expected_transaction_id))
        self.delete_entered.set()
        if self.block_delete:
            await self.delete_release.wait()
        if self.delete_error is not None:
            raise self.delete_error
        self.live_resources.difference_update({
            identity for identity in self.live_resources
            if identity[0] == sandbox_id
            and (expected_transaction_id is None or identity[1] == expected_transaction_id)
        })


ADMIN = "endpoint-admin-test-key"


class CredentialRuntime(APIRuntime):
    """Observe credential startup configuration without exposing it in public runtime results."""

    async def get_endpoint(self, sandbox_id):
        """Return a protected, independently fingerprinted proxy for the created transaction."""
        spec = next(item for item in self.specs if item.sandbox_id == sandbox_id)
        return SandboxEndpoint(
            sandbox_id, "http://127.0.0.1:49153", auth_required=True,
            transaction_id=spec.transaction_id, created_at=spec.created_at,
            credential_digest=hashlib.sha256(spec.access_token.encode()).hexdigest(), auth_version="1",
        )


IMAGE = 'sha256:' + 'a' * 64


class WorkspaceRuntime(FakeRuntime):
    """Keep independent volume data separate from fake sandbox resources and controlled waits."""

    def __init__(self):
        """Set isolated resource maps and deterministic cancellation/deletion gates."""
        super().__init__()
        self.image_id = IMAGE
        self.workspaces = {}
        self.contents = {}
        self.instances = {}
        self.create_error = None
        self.delete_error = None
        self.block_create = False
        self.late_success = False
        self.block_delete = False
        self.create_entered = asyncio.Event()
        self.create_release = asyncio.Event()
        self.cleanup_entered = asyncio.Event()
        self.cleanup_release = asyncio.Event()
        self.delete_entered = asyncio.Event()
        self.delete_release = asyncio.Event()
        self.workspace_calls = []
        self.statuses = {}
        self.query_overrides = {}
        self.inspections = []

    async def resolve_environment_image(self, environment):
        """Resolve a fixed test image without any network or Docker access."""
        self.workspace_calls.append(('resolve', environment))
        return self.image_id

    async def create_workspace(self, record):
        """Create an independent data object once, without any sandbox resources."""
        self.workspaces[record.workspace_id] = record
        self.contents[record.workspace_id] = {}

    async def inspect_workspace(self, record):
        """Check independent identity and every existing fake container reference."""
        assert self.workspaces[record.workspace_id].creation_token == record.creation_token
        for spec in self.instances.values():
            if spec.workspace and spec.workspace.workspace_id == record.workspace_id:
                if spec.sandbox_id != record.sandbox_id or spec.transaction_id != record.transaction_id:
                    raise WorkspaceConflict(record.workspace_id)

    async def delete_workspace(self, record):
        """Reject live references and remove only the independent workspace object."""
        await self.inspect_workspace(record)
        if any(spec.workspace and spec.workspace.workspace_id == record.workspace_id for spec in self.instances.values()):
            raise WorkspaceConflict(record.workspace_id)
        self.workspaces.pop(record.workspace_id, None)
        self.contents.pop(record.workspace_id, None)

    async def create(self, spec):
        """Expose a deterministic pending interval and model confirmed versus late cancellation."""
        self.specs.append(spec)
        self.create_entered.set()
        if self.block_create:
            try:
                await self.create_release.wait()
            except asyncio.CancelledError:
                if self.late_success:
                    await self.create_release.wait()
                else:
                    self.cleanup_entered.set()
                    await self.cleanup_release.wait()
                    raise
        if self.create_error:
            raise self.create_error
        self.instances[spec.sandbox_id] = spec

    async def get(self, sandbox_id):
        """Return original identity and workspace while preserving original TTL."""
        from control_plane.domain.models import SandboxQueryResult
        from control_plane.domain.errors import SandboxNotFound
        if sandbox_id not in self.instances:
            raise SandboxNotFound()
        spec = self.instances[sandbox_id]
        result = SandboxQueryResult(
            sandbox_id, self.statuses.get(sandbox_id, 'ready'), 'health_check_passed', 'Agent is healthy',
            spec.created_at, spec.expires_at, spec.transaction_id,
            workspace_id=spec.workspace.workspace_id if spec.workspace else None,
            workspace=spec.workspace,
        )
        return replace(result, **self.query_overrides)

    async def inspect_resources(self, sandbox_id, *, expected_transaction_id, expected_workspace=None):
        """Verify the supplied occupancy before reporting complete fake resources."""
        from control_plane.domain.models import SandboxResourceInspection
        self.inspections.append((sandbox_id, expected_transaction_id, expected_workspace))
        spec = self.instances.get(sandbox_id)
        if spec and spec.workspace != expected_workspace:
            return SandboxResourceInspection(sandbox_id, 'conflict', 'workspace_binding_conflict')
        return SandboxResourceInspection(sandbox_id, 'all_present' if spec else 'not_found', 'checked')

    async def delete(self, sandbox_id, *, delete_workspace=False, expected_transaction_id=None, expected_workspace=None):
        """Settle deletion without removing independent workspace data or losing identity guards."""
        self.deletions.append((sandbox_id, delete_workspace))
        if expected_workspace is not None:
            assert not delete_workspace
            assert expected_workspace.sandbox_id == sandbox_id
            assert expected_workspace.transaction_id == expected_transaction_id
        self.delete_entered.set()
        if self.block_delete:
            await self.delete_release.wait()
        if self.delete_error:
            raise self.delete_error
        self.instances.pop(sandbox_id, None)

    async def pause(self, sandbox_id, *, expected_transaction_id, expected_workspace=None):
        """Record a freeze without changing the stored identity, data or occupancy."""
        self.transitions.append(('pause', sandbox_id, expected_transaction_id))
        self.statuses[sandbox_id] = 'paused'

    async def resume(self, sandbox_id, *, expected_transaction_id, expected_workspace=None):
        """Record thawing the original identity without changing workspace contents."""
        self.transitions.append(('resume', sandbox_id, expected_transaction_id))
        self.statuses[sandbox_id] = 'running'


class GatedWorkspaceStore(SQLiteLifecycleStore):
    """Expose acknowledgement boundaries around real SQLite commits without worker sleeps."""

    def __init__(self, database_path):
        """Initialize independent prewrite, activation and metadata-cleanup failure controls."""
        super().__init__(database_path)
        self.block_prewrite = False
        self.block_prewrite_ack = False
        self.block_activation = False
        self.block_activation_ack = False
        self.fail_activation_ack = False
        self.fail_delete = False
        self.prewrite_entered = asyncio.Event()
        self.prewrite_release = asyncio.Event()
        self.prewrite_committed = asyncio.Event()
        self.prewrite_ack_release = asyncio.Event()
        self.activation_entered = asyncio.Event()
        self.activation_release = asyncio.Event()
        self.activation_committed = asyncio.Event()
        self.activation_ack_release = asyncio.Event()

    async def create_with_workspace(self, record, binding, *, expected_workspace_version, credential=None, max_instances=10):
        """Hold before and after the actual atomic occupancy/pending transaction commits."""
        self.prewrite_entered.set()
        if self.block_prewrite:
            await self.prewrite_release.wait()
        await super().create_with_workspace(
            record, binding, expected_workspace_version=expected_workspace_version, credential=credential, max_instances=max_instances,
        )
        self.prewrite_committed.set()
        if self.block_prewrite_ack:
            await self.prewrite_ack_release.wait()

    async def activate(self, sandbox_id, transaction_id, *, expected_version):
        """Commit real activation before optionally withholding or failing its acknowledgement."""
        self.activation_entered.set()
        if self.block_activation:
            await self.activation_release.wait()
        result = await super().activate(sandbox_id, transaction_id, expected_version=expected_version)
        self.activation_committed.set()
        if self.block_activation_ack:
            await self.activation_ack_release.wait()
        if self.fail_activation_ack:
            raise LifecycleStoreError()
        return result

    async def delete(self, sandbox_id, transaction_id, *, expected_version):
        """Fail before guarded metadata removal to leave a durable deletion retry."""
        if self.fail_delete:
            raise LifecycleStoreError()
        await super().delete(sandbox_id, transaction_id, expected_version=expected_version)


RECOVERY_NOW = datetime(2026, 10, 21, 8, 0, 0, 123456, tzinfo=UTC)


ROLES = ("container", "proxy", "network", "ingress", "workspace")


class RecoveryRuntime(FakeRuntime):
    """Keep backend facts independent of SQLite and expose controllable read boundaries."""

    def __init__(self) -> None:
        """Initialize fixed observations without creating any runtime resources."""
        super().__init__()
        self.records = {}
        self.classification = "all_present"
        self.status = "ready"
        self.failure_stage = None
        self.failure = DockerUnavailable("private runtime details")
        self.identity_change = None
        self.calls = []
        self.block_endpoint = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def inspect_resources(self, sandbox_id, *, expected_transaction_id):
        """Confirm resource ownership against the candidate's original transaction."""
        self.calls.append(("inspect", sandbox_id))
        assert self.records[sandbox_id].transaction_id == expected_transaction_id
        if self.failure_stage == "inspect":
            raise self.failure
        return SandboxResourceInspection(
            sandbox_id, self.classification, "private untrusted reason", ROLES,
        )

    async def get(self, sandbox_id):
        """Return independently saved identity and health without using merged Service queries."""
        self.calls.append(("get", sandbox_id))
        if self.failure_stage == "get":
            raise self.failure
        record = self.records[sandbox_id]
        result = SandboxQueryResult(
            sandbox_id, self.status, "observed", "observed",
            record.created_at, record.expires_at, record.transaction_id,
        )
        if self.identity_change is not None:
            field, value = self.identity_change
            result = replace(result, **{field: value})
        return result

    async def get_endpoint(self, sandbox_id):
        """Gate the final runtime read while a competing SQLite connection changes metadata."""
        self.calls.append(("endpoint", sandbox_id))
        self.entered.set()
        if self.block_endpoint:
            await self.release.wait()
        if self.failure_stage == "endpoint":
            raise self.failure
        return SandboxEndpoint(sandbox_id, "http://127.0.0.1:49153")


class RecoveryStore(SQLiteLifecycleStore):
    """Exercise real guarded activation with deterministic late commits and lost responses."""

    def __init__(self, path):
        """Create thread gates without touching the configured database."""
        super().__init__(path)
        self.block_activation = False
        self.fail_before_commit = False
        self.fail_after_commit = False
        self.entered = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()
        self.before_activate = None

    async def activate(self, sandbox_id, transaction_id, *, expected_version):
        """Allow a competing independent connection to win just before the guarded SQL."""
        if self.before_activate is not None:
            await self.before_activate()
        return await super().activate(
            sandbox_id, transaction_id, expected_version=expected_version,
        )

    def _activate_sync(self, sandbox_id, transaction_id, expected_version):
        """Pause or lose acknowledgement without undoing the actual SQLite commit."""
        self.entered.set()
        try:
            if self.block_activation:
                assert self.release.wait(5), "activation test gate timed out"
            if self.fail_before_commit:
                raise LifecycleStoreError()
            result = super()._activate_sync(sandbox_id, transaction_id, expected_version)
            if self.fail_after_commit:
                raise LifecycleStoreError()
            return result
        finally:
            self.finished.set()


async def add_pending(store, runtime, number=1, *, expires_at=None, offset_us=0):
    """Save matching independent backend and pending facts with exact original UTC times."""
    record = SandboxLifecycleRecord(
        f"sbx_{number:032x}", f"txn_{number:032x}",
        RECOVERY_NOW - timedelta(minutes=1) + timedelta(microseconds=offset_us),
        expires_at if expires_at is not None else RECOVERY_NOW + timedelta(minutes=29),
    )
    await store.create(record)
    runtime.records[record.sandbox_id] = record
    return record


FIXED_TRANSACTION_ID = "txn_0123456789abcdef0123456789abcdef"


CREATED_AT = datetime(2026, 10, 19, 8, tzinfo=UTC)


EXPIRES_AT = datetime(2026, 10, 19, 8, 30, tzinfo=UTC)
