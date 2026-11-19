"""Independent workspace lifecycle and shared verification of sandbox bindings."""

import asyncio
import secrets
from collections.abc import Callable, Collection
from dataclasses import replace
from datetime import datetime

from control_plane.common.async_tasks import settle_task
from control_plane.domain.errors import (
    SandboxEnvironmentNotRegistered,
    SandboxMetadataConflict,
    SandboxMetadataUnavailable,
    WorkspaceConflict,
    WorkspaceError,
    WorkspaceNotFound,
    WorkspaceOperationFailed,
    WorkspaceUnavailable,
)
from control_plane.domain.models import (
    SandboxLifecycleRecord,
    SandboxQueryResult,
    SandboxSpec,
    WorkspaceBinding,
    WorkspaceRecord,
    _validate_environment,
    _validate_workspace_id,
)
from control_plane.runtime.base import SandboxRuntime
from control_plane.storage.base import LifecycleRecordNotFound, LifecycleStore, LifecycleStoreConflict


class WorkspaceService:
    """Coordinate independent volumes while the shared Store owns atomic workspace occupancy."""

    def __init__(
        self, runtime: SandboxRuntime, store: LifecycleStore,
        environment_names: Collection[str], clock: Callable[[], datetime],
    ) -> None:
        """Use the same Store, runtime and trusted configuration as sandbox lifecycle."""
        self._runtime = runtime
        self._store = store
        self._environment_names = environment_names
        self._clock = clock

    async def _read_workspace(self, workspace_id: str) -> WorkspaceRecord:
        """Read only registered workspace metadata, rejecting invalid and absent opaque IDs."""
        try:
            _validate_workspace_id(workspace_id)
        except ValueError:
            raise WorkspaceNotFound() from None
        try:
            record = await self._store.get_workspace(workspace_id)
        except Exception as error:
            raise WorkspaceUnavailable(workspace_id) from error
        if record is None:
            raise WorkspaceNotFound(workspace_id)
        return record

    async def _verify_current_workspace(self, record: WorkspaceRecord) -> None:
        """Reject a changed workspace snapshot before forwarding its identity to Docker."""
        worker = asyncio.create_task(self._store.get_workspace(record.workspace_id))
        try:
            current = await settle_task(worker, propagate_cancellation=True)
        except (LifecycleRecordNotFound, LifecycleStoreConflict):
            raise WorkspaceConflict(record.workspace_id) from None
        except Exception:
            raise WorkspaceUnavailable(record.workspace_id) from None
        if current is None or (current.creation_token, current.version) != (record.creation_token, record.version):
            raise WorkspaceConflict(record.workspace_id)

    async def create_workspace(self, environment: str = "basic") -> WorkspaceRecord:
        """Create one independently owned volume using a trusted, immutable image selection."""
        _validate_environment(environment)
        if environment not in self._environment_names:
            raise SandboxEnvironmentNotRegistered()
        resolving = asyncio.create_task(self._runtime.resolve_environment_image(environment))
        image_id = await settle_task(resolving, propagate_cancellation=True)
        workspace_id = f"wsp_{secrets.token_hex(16)}"
        record = WorkspaceRecord(
            workspace_id, f"wct_{secrets.token_hex(16)}", f"agent-sandbox-{workspace_id}",
            image_id, self._clock(),
        )
        worker = asyncio.create_task(self._create_workspace_record(record))
        # A request cancellation cannot abandon a submitted volume/initializer
        # operation. Preserve its final metadata and data; never undo by deleting it.
        return await settle_task(worker, propagate_cancellation=True)

    async def _create_workspace_record(self, record: WorkspaceRecord) -> WorkspaceRecord:
        """Prewrite and confirm the original workspace, retaining pending data on failure."""
        try:
            await self._store.create_workspace(record)
        except LifecycleStoreConflict as error:
            raise WorkspaceConflict(record.workspace_id) from error
        except Exception as error:
            raise WorkspaceUnavailable(record.workspace_id) from error
        try:
            await self._runtime.create_workspace(record)
        except WorkspaceError as error:
            error.workspace_id = record.workspace_id
            raise
        except Exception as error:
            failed = WorkspaceOperationFailed(record.workspace_id)
            failed.creation_failure = getattr(error, "creation_failure", None)
            raise failed from None
        try:
            return await self._store.confirm_workspace(
                record.workspace_id, record.creation_token, expected_version=record.version,
            )
        except LifecycleStoreConflict:
            raise WorkspaceConflict(record.workspace_id) from None
        except Exception:
            raise WorkspaceUnavailable(record.workspace_id) from None

    async def get_workspace(self, workspace_id: str) -> WorkspaceRecord:
        """Combine durable ownership with verified current volume and container-reference facts."""
        record = await self._read_workspace(workspace_id)
        await self._verify_current_workspace(record)
        await self._runtime.inspect_workspace(record)
        return record

    async def delete_workspace(self, workspace_id: str) -> None:
        """Settle guarded independent-volume deletion before allowing cancellation to return."""
        try:
            _validate_workspace_id(workspace_id)
        except ValueError:
            raise WorkspaceNotFound() from None
        worker = asyncio.create_task(self._delete_workspace(workspace_id))
        await settle_task(worker, propagate_cancellation=True)

    async def _delete_workspace(self, workspace_id: str) -> None:
        """Claim before inspecting/removing an idle volume, retaining a retry on uncertain removal."""
        try:
            record = await self._store.get_workspace(workspace_id)
            if record is None:
                return
            deleting = await self._store.claim_workspace_deletion(
                workspace_id, record.creation_token, expected_version=record.version,
            )
        except LifecycleStoreConflict as error:
            raise WorkspaceConflict(workspace_id) from error
        except Exception as error:
            raise WorkspaceUnavailable(workspace_id) from error
        await self._verify_current_workspace(deleting)
        try:
            await self._runtime.delete_workspace(deleting)
        except WorkspaceError:
            raise
        except Exception as error:
            raise WorkspaceOperationFailed(workspace_id) from error
        try:
            await self._store.delete_workspace(
                workspace_id, deleting.creation_token, expected_version=deleting.version,
            )
        except LifecycleStoreConflict as error:
            raise WorkspaceConflict(workspace_id) from error
        except Exception as error:
            raise WorkspaceUnavailable(workspace_id) from error

    async def binding_for(self, record: SandboxLifecycleRecord) -> WorkspaceBinding | None:
        """Require the persisted holder, token, immutable volume identity and occupancy version to match."""
        if record.workspace_id is None:
            return None
        try:
            binding = await self._store.get_workspace_binding(record.sandbox_id, record.transaction_id)
            workspace = await self._store.get_workspace(record.workspace_id)
        except LifecycleStoreConflict as error:
            raise SandboxMetadataConflict(record.sandbox_id) from error
        except Exception as error:
            raise SandboxMetadataUnavailable(record.sandbox_id) from error
        if (
            binding is None or workspace is None
            or binding.workspace_id != record.workspace_id
            or binding.sandbox_id != record.sandbox_id or binding.transaction_id != record.transaction_id
            or workspace.sandbox_id != record.sandbox_id or workspace.transaction_id != record.transaction_id
            or workspace.creation_token != binding.creation_token
            or workspace.image_id != binding.image_id or workspace.volume_name != binding.volume_name
            or workspace.version != binding.version or workspace.management_state != "available"
        ):
            raise SandboxMetadataConflict(record.sandbox_id)
        return binding

    async def verify_observed(
        self, record: SandboxLifecycleRecord, observed: SandboxQueryResult,
    ) -> WorkspaceBinding | None:
        """Compare actual runtime ownership to durable occupancy rather than only matching a public ID."""
        expected = await self.binding_for(record)
        actual = observed.workspace
        if expected is None:
            if actual is not None or observed.workspace_id is not None:
                raise SandboxMetadataConflict(record.sandbox_id)
        elif actual is None or replace(actual, version=expected.version) != expected:
            # Runtime labels carry immutable identity, not mutable database CAS versions.
            raise SandboxMetadataConflict(record.sandbox_id)
        return expected

    async def prepare_binding(self, workspace_id: str, spec: SandboxSpec) -> WorkspaceBinding:
        """Check a free independent volume and pin compatibility before atomic occupancy/pending insertion."""
        record = await self._read_workspace(workspace_id)
        if record.management_state != "available" or record.sandbox_id is not None:
            raise WorkspaceConflict(workspace_id)
        image = await self._runtime.resolve_environment_image(spec.environment)
        if image != record.image_id:
            raise WorkspaceConflict(workspace_id)
        await self._verify_current_workspace(record)
        await self._runtime.inspect_workspace(record)
        return WorkspaceBinding(
            workspace_id, record.creation_token, record.volume_name, record.image_id,
            record.version + 1, spec.sandbox_id, spec.transaction_id,
        )
