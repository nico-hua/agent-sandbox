"""Double-opt-in authenticated endpoint delivery through application-owned local sessions."""

import asyncio
import secrets
import time
from types import SimpleNamespace
from urllib.parse import urlsplit

import httpx

from control_plane.app import create_app
from control_plane.core.models import SandboxAccessCredential, SandboxLifecycleRecord
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_local_entry_integration import _assert_port_closed, _create_ready, _protocol
from test_kubernetes_proxy_integration import _RedactedMapping, _RedactedText
from test_kubernetes_proxy_lifecycle_integration import owned_proxy_lifecycle, pytestmark


async def _persist_owned_record(chain, path):
    """Persist only this fixture's original transaction and credential in an isolated temporary database."""
    spec = chain["spec"]
    store = SQLiteLifecycleStore(path)
    await store.initialize()
    await store.create(
        SandboxLifecycleRecord(spec.sandbox_id, spec.transaction_id, spec.created_at, spec.expires_at),
        credential=SandboxAccessCredential(spec.sandbox_id, spec.transaction_id, spec.access_token),
    )
    active = await store.activate(spec.sandbox_id, spec.transaction_id, expected_version=0)
    assert active.management_state == "active"
    return store


def _application(backend, manager, store, sandbox_id, admin):
    """Assemble an authenticated application whose lifespan alone opens the requested local entry."""
    return create_app(
        backend, store=store, api_key=str(admin), reaper_enabled=False,
        pending_recovery_enabled=False, sandbox_query_timeout=10,
        kubernetes_entry_manager=manager, kubernetes_initial_sandbox_ids=(sandbox_id,),
    )


async def _delivered_endpoint(control, path, admin, expected_token):
    """Check authenticated private delivery while retaining credentials only in redacted wrappers."""
    for headers in ({}, {"Authorization": "Bearer wrong-management-test-key"}):
        denied = await control.get(path, headers=headers)
        assert denied.status_code == 401
        hidden = expected_token not in denied.text
        assert hidden, "rejected management request exposed an execution credential"
    response = await control.get(path, headers={"Authorization": "Bearer " + admin})
    assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
    payload = _RedactedMapping(response.json())
    token = _RedactedText(payload.pop("access_token"))
    same_credential = token == expected_token
    assert same_credential, "endpoint did not deliver the original persisted credential"
    parsed = urlsplit(payload["url"])
    assert parsed.scheme == "http" and parsed.hostname == "127.0.0.1" and parsed.port
    assert not parsed.query and not parsed.fragment and parsed.username is None and parsed.password is None
    assert payload == {"sandbox_id": path.split("/")[3], "url": payload["url"], "auth_required": True}
    hidden = expected_token not in payload["url"] and admin not in payload["url"]
    assert hidden, "local URL included a credential"
    return SimpleNamespace(url=payload["url"]), token


def test_authenticated_endpoint_protocol_shutdown_and_original_runtime_delete(owned_proxy_lifecycle, tmp_path):
    """Deliver the actual loopback endpoint, close it on shutdown, then delete through a fresh owned manager."""
    chain = owned_proxy_lifecycle

    async def verify():
        """Keep both application lifespans and their subprocess cleanup on the same live event loop."""
        from control_plane.runtime.kubernetes.local_entry_manager import KubernetesLocalEntryManager

        backend = await _create_ready(chain)
        store = await _persist_owned_record(chain, tmp_path / "endpoint.sqlite3")
        sandbox_id, transaction_id = chain["spec"].sandbox_id, chain["spec"].transaction_id
        admin = _RedactedText(secrets.token_urlsafe(32))
        token = _RedactedText(chain["spec"].access_token)
        manager = KubernetesLocalEntryManager(backend)
        managers = [manager]
        try:
            app = _application(backend, manager, store, sandbox_id, admin)
            assert backend.local_entry_manager is manager and manager.session_count == 0
            async with app.router.lifespan_context(app):
                assert manager.session_count == 1
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://isolated-control", trust_env=False,
                ) as control:
                    path = f"/v1/sandboxes/{sandbox_id}/endpoint"
                    entry, delivered = await _delivered_endpoint(control, path, admin, token)
                    repeated, _ = await _delivered_endpoint(control, path, admin, token)
                    assert repeated.url == entry.url and manager.session_count == 1
                    # A separate data-plane client must explicitly submit the
                    # execution credential returned by the management API.
                    async with httpx.AsyncClient(trust_env=False, timeout=10) as data:
                        denied = await data.post(
                            entry.url + "/v1/commands:run", json={"argv": ["true"]},
                            headers={"Authorization": "Bearer " + admin},
                        )
                        assert denied.status_code == 401
                    await _protocol(entry, delivered)
            assert manager.is_closed and manager.session_count == 0
            await _assert_port_closed(entry.url)
            observed = await backend.get(sandbox_id)
            assert observed.status == "ready" and observed.transaction_id == transaction_id

            # A closed manager stays closed. A fresh runtime can query the same
            # original identity without requiring proxy-image creation config.
            restarted = chain["make_runtime"]()
            second = KubernetesLocalEntryManager(restarted)
            managers.append(second)
            next_app = _application(restarted, second, store, sandbox_id, admin)
            async with next_app.router.lifespan_context(next_app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=next_app), base_url="http://isolated-control", trust_env=False,
                ) as control:
                    next_entry, _ = await _delivered_endpoint(control, path, admin, token)
                assert second.session_count == 1
                await restarted.delete(sandbox_id, expected_transaction_id=transaction_id)
                assert second.session_count == 0
                await _assert_port_closed(next_entry.url)
                chain["managed"].assert_absent()
            assert second.is_closed and second.session_count == 0
        finally:
            cleanup = await asyncio.gather(*(owned.close_all() for owned in managers), return_exceptions=True)
            for outcome in cleanup:
                if isinstance(outcome, BaseException):
                    raise outcome

    asyncio.run(verify())


def test_invalid_original_proxy_rejects_endpoint_without_reopening(owned_proxy_lifecycle, tmp_path, monkeypatch):
    """A deleted original proxy makes HTTP endpoint lookup fail without spawning a replacement forward."""
    chain = owned_proxy_lifecycle

    async def verify():
        """Observe the real process count and independently remove only the fixture's recorded proxy UID."""
        from control_plane.runtime.kubernetes.local_entry_manager import KubernetesLocalEntryManager

        backend = await _create_ready(chain)
        store = await _persist_owned_record(chain, tmp_path / "invalid-endpoint.sqlite3")
        sandbox_id = chain["spec"].sandbox_id
        admin = _RedactedText(secrets.token_urlsafe(32))
        token = _RedactedText(chain["spec"].access_token)
        manager = KubernetesLocalEntryManager(backend)
        original_spawn = asyncio.create_subprocess_exec
        spawned = []

        async def counted_spawn(*args, **kwargs):
            """Count actual subprocess creation while preserving the real kubectl process and its streams."""
            spawned.append(None)
            return await original_spawn(*args, **kwargs)

        monkeypatch.setattr(asyncio, "create_subprocess_exec", counted_spawn)
        try:
            app = _application(backend, manager, store, sandbox_id, admin)
            async with app.router.lifespan_context(app):
                assert manager.session_count == 1 and len(spawned) == 1
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://isolated-control", trust_env=False,
                ) as control:
                    path = f"/v1/sandboxes/{sandbox_id}/endpoint"
                    entry, _ = await _delivered_endpoint(control, path, admin, token)
                    await asyncio.to_thread(chain["managed"].delete, chain["managed"].role("proxy"))
                    deadline = time.monotonic() + 10
                    while True:
                        response = await control.get(path, headers={"Authorization": "Bearer " + admin})
                        if response.status_code == 503:
                            hidden = token not in response.text
                            assert hidden, "invalid endpoint response exposed an execution credential"
                            break
                        assert response.status_code == 200
                        assert time.monotonic() < deadline, "deleted original proxy retained endpoint availability"
                        await asyncio.sleep(0.05)
                    assert manager.session_count <= 1 and len(spawned) == 1
                    rejected = await control.get(path, headers={"Authorization": "Bearer " + admin})
                    assert rejected.status_code == 503 and len(spawned) == 1
            assert manager.is_closed and manager.session_count == 0
            await _assert_port_closed(entry.url)
        finally:
            await manager.close_all()
        await backend.delete(sandbox_id, expected_transaction_id=chain["spec"].transaction_id)
        chain["managed"].assert_absent()

    asyncio.run(verify())
