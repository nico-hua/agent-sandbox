"""Explicit in-memory lifecycle persistence for existing non-storage regression tests."""

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
