"""Explicit in-memory lifecycle persistence for existing non-storage regression tests."""

from dataclasses import replace
from datetime import datetime

from control_plane.core.models import SandboxLifecycleRecord
from control_plane.storage.lifecycle_store import LifecycleStoreConflict


class FakeLifecycleStore:
    """Persist records and enforce identity/version checks without touching a database."""

    def __init__(self) -> None:
        """Initialize per-test metadata and explicit initialization call tracking."""
        self.records: dict[str, SandboxLifecycleRecord] = {}
        self.initialize_calls = 0

    async def initialize(self) -> None:
        """Record application startup initialization without changing stored records."""
        self.initialize_calls += 1

    async def create(self, record: SandboxLifecycleRecord) -> None:
        """Insert real test metadata while rejecting an already occupied identity."""
        if record.sandbox_id in self.records:
            raise LifecycleStoreConflict("already_exists")
        self.records[record.sandbox_id] = record

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
