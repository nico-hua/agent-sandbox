"""Shared guards for lifecycle deletion and creation compensation."""

import asyncio

from control_plane.common.async_tasks import settle_task
from control_plane.domain.errors import (
    SandboxMetadataCleanupFailed,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
)
from control_plane.domain.sandbox import SandboxLifecycleRecord
from control_plane.storage.base import (
    LifecycleRecordNotFound,
    LifecycleStore,
    LifecycleStoreConflict,
)


async def mark_deleting(
    store: LifecycleStore, record: SandboxLifecycleRecord, *, allow_absent: bool = False,
) -> SandboxLifecycleRecord:
    """Confirm an exact transaction/version before any destructive backend work."""
    try:
        return await store.mark_deleting(
            record.sandbox_id, record.transaction_id, expected_version=record.version,
        )
    except LifecycleRecordNotFound as error:
        # Only compensation of our confirmed creation permits an absent row.
        # Its original identity/version still prevents deleting replacement metadata.
        if allow_absent:
            return record
        raise SandboxMetadataConflict(record.sandbox_id) from error
    except LifecycleStoreConflict as error:
        raise SandboxMetadataConflict(record.sandbox_id) from error
    except Exception as error:
        raise SandboxMetadataUnavailable(record.sandbox_id) from error


async def remove_metadata(store: LifecycleStore, record: SandboxLifecycleRecord) -> None:
    """Delete only the supplied transaction/version without refreshing a changed record."""
    worker = asyncio.create_task(store.delete(
        record.sandbox_id, record.transaction_id, expected_version=record.version,
    ))
    try:
        await settle_task(worker, propagate_cancellation=True)
    except asyncio.CancelledError as error:
        if not worker.cancelled():
            raise
        raise SandboxMetadataCleanupFailed(record.sandbox_id) from error
    except Exception as error:
        raise SandboxMetadataCleanupFailed(record.sandbox_id) from error
