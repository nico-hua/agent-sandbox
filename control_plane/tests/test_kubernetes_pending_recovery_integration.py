"""Double-opt-in lifespan recovery of original pending Kubernetes instances, without resource repair."""

import asyncio
import secrets
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.models import SandboxAccessCredential, SandboxLifecycleRecord, SandboxSpec
from control_plane.runtime.kubernetes.local_entry_manager import KubernetesLocalEntryManager
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_endpoint_integration import _delivered_endpoint
from test_kubernetes_local_entry_integration import _assert_port_closed, _protocol
from test_kubernetes_persistent_workspace_integration import (
    _STORAGE_CLASS, owned_persistent_workspace, pytestmark,
)
from test_kubernetes_private_workspace_integration import _wait_volume_absent
from test_kubernetes_proxy_integration import _PROXY_IMAGE, _RedactedText
from test_kubernetes_proxy_lifecycle_integration import owned_proxy_lifecycle
from test_kubernetes_resource_inspection_integration import _snapshot
from test_kubernetes_service_create_integration import _ready


@pytest.mark.parametrize("persistent", [False, True])
def test_real_lifespan_recovers_original_pending_and_delivers_authenticated_endpoint(
    persistent, request, tmp_path,
):
    """Recover eight/nine originals from isolated SQLite, use the original token, then delete by original UID."""
    chain = request.getfixturevalue("owned_persistent_workspace" if persistent else "owned_proxy_lifecycle")

    async def verify():
        """Keep Runtime creation, simulated interruption and restarted application ownership on one event loop."""
        # Older read-only fixtures intentionally carry historical business time;
        # recovery must instead exercise a genuinely unexpired new transaction.
        created = datetime.now(UTC)
        spec = SandboxSpec(
            "sbx_" + secrets.token_hex(16), "unused", "unused", "unused",
            transaction_id="txn_" + secrets.token_hex(16), created_at=created,
            expires_at=created + timedelta(minutes=30), access_token=secrets.token_urlsafe(32),
        )
        chain["spec"] = spec
        store = SQLiteLifecycleStore(tmp_path / "pending-recovery.sqlite3")
        await store.initialize()
        pending = SandboxLifecycleRecord(spec.sandbox_id, spec.transaction_id, spec.created_at, spec.expires_at)
        credential = SandboxAccessCredential(spec.sandbox_id, spec.transaction_id, spec.access_token)
        # Model the normal pending/credential prewrite followed by an interruption
        # between Runtime completion and Service's activation acknowledgement.
        await store.create(pending, credential=credential)
        options = {"workspace_storage_class": _STORAGE_CLASS} if persistent else {}
        creator = chain["make_runtime"](proxy_image=_PROXY_IMAGE, **options)
        await creator.create(spec)
        await _ready(creator, spec.sandbox_id)
        chain["managed"].assert_complete()
        assert await store.get(spec.sandbox_id) == pending
        before = await asyncio.to_thread(_snapshot, chain)

        # Restarted query/cleanup and recovery use persisted resource identities,
        # not the creator's image or StorageClass configuration.
        restarted = chain["make_runtime"]()
        manager = KubernetesLocalEntryManager(restarted)
        admin = _RedactedText(secrets.token_urlsafe(32))
        token = _RedactedText(spec.access_token)
        app = create_app(
            restarted, store=SQLiteLifecycleStore(store.database_path), api_key=str(admin),
            reaper_enabled=False, pending_recovery_enabled=True, pending_recovery_batch_limit=1,
            kubernetes_entry_manager=manager, kubernetes_initial_sandbox_ids=(spec.sandbox_id,),
            sandbox_query_timeout=10,
        )
        try:
            assert manager.session_count == 0
            async with app.router.lifespan_context(app):
                result = app.state.pending_recovery_result
                assert result.error is None and result.recovered == 1 and len(result.items) == 1
                assert await store.get(spec.sandbox_id) == replace(pending, management_state="active", version=1)
                persisted = await store.get_credential(spec.sandbox_id, spec.transaction_id)
                same_credential = persisted == credential
                assert same_credential, "recovery changed the persisted execution credential"
                after = await asyncio.to_thread(_snapshot, chain)
                unchanged = before == after
                assert unchanged, "recovery changed original Kubernetes identities or desired configuration"
                assert manager.session_count == 1
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://isolated-control", trust_env=False,
                ) as control:
                    entry, delivered = await _delivered_endpoint(
                        control, f"/v1/sandboxes/{spec.sandbox_id}/endpoint", admin, token,
                    )
                    await _protocol(entry, delivered)
                    again, _ = await _delivered_endpoint(
                        control, f"/v1/sandboxes/{spec.sandbox_id}/endpoint", admin, token,
                    )
                    assert again.url == entry.url and manager.session_count == 1
                await app.state.service.delete(spec.sandbox_id, delete_workspace=True)
                assert manager.session_count == 0
                assert await store.get(spec.sandbox_id) is None
                assert await store.get_credential(spec.sandbox_id, spec.transaction_id) is None
                await asyncio.to_thread(chain["managed"].assert_absent)
                await _assert_port_closed(entry.url)
            assert manager.is_closed
            if persistent:
                await asyncio.to_thread(_wait_volume_absent, chain["api"], chain["volume"])
        finally:
            await manager.close_all()

    asyncio.run(verify())
