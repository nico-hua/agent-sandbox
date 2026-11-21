"""Canonical independent workspace fixtures for metadata tests."""

from datetime import (
    UTC,
    datetime,
    timedelta,
)


NOW = datetime(2026, 11, 3, tzinfo=UTC)


WORKSPACE_ID = 'wsp_' + 'a' * 32


TOKEN = 'wct_' + 'b' * 32


def workspace():
    """Build the canonical pending workspace using the shared model."""
    from control_plane.domain.workspace import WorkspaceRecord
    return WorkspaceRecord(WORKSPACE_ID, TOKEN, 'agent-sandbox-' + WORKSPACE_ID, 'sha256:' + 'c' * 64, NOW)


def lifecycle(sandbox_id='sbx_' + 'a' * 32, transaction_id='txn_' + 'a' * 32):
    """Build pending metadata pointing at the explicitly selected workspace."""
    from control_plane.domain.sandbox import SandboxLifecycleRecord
    return SandboxLifecycleRecord(sandbox_id, transaction_id, NOW, NOW + timedelta(hours=1), workspace_id=WORKSPACE_ID)


def binding(record, version=2):
    """Describe the workspace version resulting from acquisition."""
    from control_plane.domain.workspace import WorkspaceBinding
    item = workspace()
    return WorkspaceBinding(item.workspace_id, item.creation_token, item.volume_name, item.image_id, version, record.sandbox_id, record.transaction_id)
