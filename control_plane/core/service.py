"""Sandbox identity generation, validation and lifecycle orchestration."""

import asyncio
import re
import secrets
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from dataclasses import replace
from typing import TypeVar

from control_plane.core.errors import (
    SandboxCleanupFailed,
    SandboxCreateFailed,
    SandboxCreateUncertain,
    SandboxImageUnavailable,
    SandboxMetadataCleanupFailed,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    SandboxNotFound,
    SandboxResourceConflict,
)
from control_plane.core.models import (
    SandboxCreateResult,
    SandboxEndpoint,
    SandboxQueryResult,
    SandboxLifecycleRecord,
    SandboxSpec,
    _new_transaction_id,
)
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.storage.lifecycle_store import LifecycleStore, LifecycleStoreConflict


DEFAULT_TTL_SECONDS = 1800
_ROLLED_BACK = (SandboxCreateFailed, SandboxImageUnavailable, SandboxResourceConflict)
_Result = TypeVar("_Result")


async def _settle(
    task: asyncio.Task[_Result], *, propagate_cancellation: bool = False,
) -> _Result:
    """Wait through repeated cancellation and retrieve the final background outcome."""
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.wait({task})
        except asyncio.CancelledError as error:
            cancellation = error
    result = task.result()
    if propagate_cancellation and cancellation is not None:
        raise cancellation
    return result


def _utc_now() -> datetime:
    """Read the current timezone-aware UTC time for one creation snapshot."""
    return datetime.now(UTC)


def _new_sandbox_id() -> str:
    """Generate an unpredictable sandbox ID with 128 random bits."""
    return f"sbx_{secrets.token_hex(16)}"


class SandboxService:
    """Generate sandbox metadata and delegate lifecycle operations."""

    def __init__(
        self,
        runtime: SandboxRuntime,
        store: LifecycleStore,
        id_factory: Callable[[], str] | None = None,
        transaction_id_factory: Callable[[], str] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        """Store runtime, identity factories and a lazily sampled creation clock."""
        self._runtime = runtime
        self._store = store
        self._id_factory = id_factory if id_factory is not None else _new_sandbox_id
        self._transaction_id_factory = (
            transaction_id_factory
            if transaction_id_factory is not None
            else _new_transaction_id
        )
        self._clock = clock if clock is not None else _utc_now

    async def ping(self) -> None:
        """Check backend availability and propagate its domain errors."""
        await self._runtime.ping()

    async def create(self) -> SandboxCreateResult:
        """Snapshot initial UTC lifecycle times before creating one fixed-TTL sandbox."""
        created_at = self._clock()
        if created_at.utcoffset() is None:
            raise ValueError("creation clock must return a timezone-aware datetime")
        created_at = created_at.astimezone(UTC)
        expires_at = created_at + timedelta(seconds=DEFAULT_TTL_SECONDS)
        sandbox_id = self._id_factory()
        spec = SandboxSpec(
            sandbox_id=sandbox_id,
            container_name=f"agent-sandbox-{sandbox_id}",
            network_name=f"agent-sandbox-{sandbox_id}-internal",
            volume_name=f"agent-sandbox-{sandbox_id}-workspace",
            transaction_id=self._transaction_id_factory(),
            created_at=created_at,
            expires_at=expires_at,
        )
        record = SandboxLifecycleRecord(
            sandbox_id, spec.transaction_id, created_at, expires_at,
        )
        await self._prewrite(record)
        await self._create_runtime(spec, record)
        return SandboxCreateResult(
            sandbox_id=sandbox_id, status="started",
            created_at=created_at, expires_at=expires_at,
        )

    async def _prewrite(self, record: SandboxLifecycleRecord) -> None:
        """Finish SQLite insertion before creating resources, compensating a cancelled write."""
        worker = asyncio.create_task(self._store.create(record))
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancellation:
            # Cancelling an await does not stop sqlite3 in its worker thread. Never
            # cancel this task: first observe its commit, then compensate our version.
            try:
                await _settle(worker)
            except LifecycleStoreConflict:
                raise cancellation
            except (Exception, asyncio.CancelledError) as error:
                raise SandboxMetadataUnavailable(record.sandbox_id) from error
            await self._remove_metadata(record)
            raise cancellation
        except LifecycleStoreConflict as error:
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataUnavailable(record.sandbox_id) from error

    async def _remove_metadata(self, record: SandboxLifecycleRecord) -> None:
        """Finish exact transaction/version cleanup without refreshing a changed record."""
        worker = asyncio.create_task(self._store.delete(
            record.sandbox_id, record.transaction_id, expected_version=record.version,
        ))
        try:
            await _settle(worker, propagate_cancellation=True)
        except asyncio.CancelledError as error:
            if not worker.cancelled():
                raise
            raise SandboxMetadataCleanupFailed(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataCleanupFailed(record.sandbox_id) from error

    async def _create_runtime(
        self, spec: SandboxSpec, record: SandboxLifecycleRecord,
    ) -> None:
        """Create resources and compensate metadata only after a confirmed runtime rollback."""
        worker = asyncio.create_task(self._runtime.create(spec))
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancellation:
            worker.cancel()
            try:
                await _settle(worker)
            except (asyncio.CancelledError, *_ROLLED_BACK):
                # Runtime's cancellation contract confirms resource rollback before
                # returning CancelledError; incomplete cleanup has its own error.
                await self._remove_metadata(record)
                raise cancellation
            except SandboxCleanupFailed:
                raise
            except Exception as error:
                raise SandboxCreateUncertain(record.sandbox_id) from error
            # Creation may have committed just before cancellation, or a runtime
            # may finish despite cancellation. Delete only this transaction's resources.
            cleanup = asyncio.create_task(self._runtime.delete(
                record.sandbox_id, delete_workspace=True,
                expected_transaction_id=record.transaction_id,
            ))
            try:
                await _settle(cleanup)
            except (Exception, asyncio.CancelledError) as error:
                raise SandboxCleanupFailed(record.sandbox_id) from error
            await self._remove_metadata(record)
            raise cancellation
        except _ROLLED_BACK:
            await self._remove_metadata(record)
            raise
        except SandboxCleanupFailed:
            raise
        except Exception as error:
            raise SandboxCreateUncertain(record.sandbox_id) from error

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Validate a public ID and resolve its state through the runtime."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        observed = await self._runtime.get(sandbox_id)
        try:
            record = await self._store.get(sandbox_id)
        except Exception as error:
            raise SandboxMetadataUnavailable(sandbox_id) from error
        if record is None:
            return observed
        if (
            record.sandbox_id != sandbox_id
            or observed.sandbox_id != sandbox_id
            or record.transaction_id != observed.transaction_id
            or record.created_at != observed.created_at
        ):
            raise SandboxMetadataConflict(sandbox_id)
        return replace(observed, expires_at=record.expires_at)

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False
    ) -> None:
        """Validate a public ID and reclaim resources with an explicit data policy."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        worker = asyncio.create_task(self._delete(sandbox_id, delete_workspace))
        try:
            await asyncio.shield(worker)
        except asyncio.CancelledError as cancellation:
            # Finish runtime deletion and metadata removal as one observable unit.
            # A cancellation response must not strand a late metadata write.
            await _settle(worker)
            raise cancellation

    async def _delete(self, sandbox_id: str, delete_workspace: bool) -> None:
        """Read metadata, guard all runtime remnants and then remove the original version."""
        try:
            record = await self._store.get(sandbox_id)
        except Exception as error:
            raise SandboxMetadataUnavailable(sandbox_id) from error
        if record is not None and record.sandbox_id != sandbox_id:
            raise SandboxMetadataConflict(sandbox_id)
        await self._runtime.delete(
            sandbox_id, delete_workspace=delete_workspace,
            expected_transaction_id=record.transaction_id if record is not None else None,
        )
        if record is not None:
            await self._remove_metadata(record)

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Validate a public ID and resolve its local entry through the runtime."""
        if re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None:
            raise SandboxNotFound()
        return await self._runtime.get_endpoint(sandbox_id)
