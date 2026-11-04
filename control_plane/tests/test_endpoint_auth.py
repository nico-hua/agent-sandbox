"""Credential creation, guarded delivery and lifecycle cleanup at the Service boundary."""

import hashlib
from dataclasses import replace

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core import service as service_module
from control_plane.core.errors import SandboxEndpointAuthRequired, SandboxEndpointAuthUnavailable, SandboxMetadataConflict
from control_plane.core.models import SandboxAccessCredential, SandboxEndpoint, SandboxSpec
from control_plane.core.service import SandboxService
from lifecycle_fakes import FakeLifecycleStore
from test_app import FakeRuntime

ADMIN = "endpoint-admin-test-key"
TOKEN = "A" * 43
SBX = "sbx_" + "a" * 32
TXN = "txn_" + "b" * 32


@pytest.fixture
def anyio_backend():
    """Run lifecycle and ASGI tests under the existing asyncio backend."""
    return "asyncio"


class CredentialRuntime(FakeRuntime):
    """Observe credential startup configuration without exposing it in public runtime results."""

    async def get_endpoint(self, sandbox_id):
        """Return a protected, independently fingerprinted proxy for the created transaction."""
        spec = next(item for item in self.specs if item.sandbox_id == sandbox_id)
        return SandboxEndpoint(
            sandbox_id, "http://127.0.0.1:49153", auth_required=True,
            transaction_id=spec.transaction_id, created_at=spec.created_at,
            credential_digest=hashlib.sha256(spec.access_token.encode()).hexdigest(), auth_version="1",
        )


def test_credential_and_runtime_spec_hide_token_repr():
    """Secret-bearing records validate bounded tokens without printing them."""
    credential = SandboxAccessCredential(SBX, TXN, TOKEN)
    spec = SandboxSpec(SBX, "main", "net", "volume", transaction_id=TXN, access_token=TOKEN)
    endpoint = SandboxEndpoint(SBX, "http://127.0.0.1:1234", auth_required=True, access_token=TOKEN)
    assert TOKEN not in repr(credential) + repr(spec) + repr(endpoint)
    assert credential.access_token == spec.access_token == endpoint.access_token == TOKEN


@pytest.mark.parametrize("token", ["", None, "x" * 42, "x" * 44, "/" * 43, "é" * 43, "A" * 42 + "$", 3, True])
def test_invalid_credentials_are_rejected_without_echo(token):
    """Credentials cannot become unsafe Nginx configuration or be silently normalized."""
    with pytest.raises(ValueError):
        SandboxAccessCredential(SBX, TXN, token)


@pytest.mark.anyio
async def test_creation_persists_one_independent_credential_before_runtime(monkeypatch):
    """New identities receive one token each, atomically attached before Docker begins."""
    tokens = iter(("A" * 43, "B" * 43))
    calls = []

    def token_factory(size):
        """Record entropy size while avoiding real secrets in assertions."""
        calls.append(size)
        return next(tokens)

    monkeypatch.setattr(service_module.secrets, "token_urlsafe", token_factory)
    runtime, store = CredentialRuntime(), FakeLifecycleStore()
    service = SandboxService(runtime, store)
    first, second = await service.create(), await service.create()
    assert calls == [32, 32]
    for result, spec, token in zip((first, second), runtime.specs, ("A" * 43, "B" * 43)):
        credential = await store.get_credential(result.sandbox_id, spec.transaction_id)
        assert credential.access_token == spec.access_token == token
        assert store.records[result.sandbox_id].management_state == "active"
        assert token not in repr(result)
    assert runtime.specs[0].access_token != runtime.specs[1].access_token


@pytest.mark.anyio
async def test_endpoint_delivers_stored_secret_and_no_store_only_to_admin():
    """Protected endpoint delivery is authenticated and ordinary queries remain secret-free."""
    runtime, store = CredentialRuntime(), FakeLifecycleStore()
    app = create_app(runtime, store=store, reaper_enabled=False, api_key=ADMIN)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
        created = await client.post("/v1/sandboxes", headers={"Authorization": "Bearer " + ADMIN})
        assert created.status_code == 201
        sandbox_id = created.json()["sandbox_id"]
        spec = runtime.specs[0]
        endpoint_path = f"/v1/sandboxes/{sandbox_id}/endpoint"
        denied = await client.get(endpoint_path)
        rejected_cleanly = denied.status_code == 401 and spec.access_token not in denied.text
        assert rejected_cleanly
        accepted = await client.get(endpoint_path, headers={"Authorization": "Bearer " + ADMIN})
        assert accepted.status_code == 200 and accepted.headers["cache-control"] == "no-store"
        payload = accepted.json()
        valid_token = payload.pop("access_token") == spec.access_token
        assert valid_token
        assert payload == {"sandbox_id": sandbox_id, "url": "http://127.0.0.1:49153", "auth_required": True}
        observed = await client.get(f"/v1/sandboxes/{sandbox_id}", headers={"Authorization": "Bearer " + ADMIN})
        public_clean = spec.access_token not in created.text + observed.text + accepted.json()["url"]
        assert public_clean


@pytest.mark.anyio
async def test_service_restart_and_admin_change_do_not_regenerate_token(monkeypatch):
    """Delivery after Service reconstruction uses the original record and proxy fingerprint."""
    runtime, store = CredentialRuntime(), FakeLifecycleStore()
    first_service = SandboxService(runtime, store)
    created = await first_service.create()
    original = await first_service.get_endpoint(created.sandbox_id)

    def forbidden(size):
        """Reject replacement credential generation during a read-only operation."""
        pytest.fail("query regenerated an execution credential")

    monkeypatch.setattr(service_module.secrets, "token_urlsafe", forbidden)
    second_service = SandboxService(runtime, store)
    recovered = await second_service.get_endpoint(created.sandbox_id)
    same_token = recovered.access_token == original.access_token
    assert same_token
    for key in (ADMIN, "changed-admin-test-key"):
        app = create_app(runtime, store=store, reaper_enabled=False, api_key=key)
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
            denied = await client.get(f"/v1/sandboxes/{created.sandbox_id}/endpoint", headers={"Authorization": "Bearer unrelated-key"})
            assert denied.status_code == 401
            accepted = await client.get(f"/v1/sandboxes/{created.sandbox_id}/endpoint", headers={"Authorization": "Bearer " + key})
            assert accepted.status_code == 200
            same_token = accepted.json()["access_token"] == original.access_token
            assert same_token


@pytest.mark.anyio
async def test_successful_delete_removes_credential_after_runtime_cleanup():
    """Only a completed instance deletion removes its same-transaction credential."""
    runtime, store = CredentialRuntime(), FakeLifecycleStore()
    service = SandboxService(runtime, store)
    created = await service.create()
    spec = runtime.specs[0]
    await service.delete(created.sandbox_id)
    assert await store.get_credential(created.sandbox_id, spec.transaction_id) is None


@pytest.mark.anyio
async def test_legacy_endpoint_is_refused_without_credential_generation():
    """A formerly anonymous proxy remains managed but is not silently handed to callers."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    with pytest.raises(SandboxEndpointAuthRequired):
        await SandboxService(runtime, store).get_endpoint(SBX)


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["missing", "store", "token", "marker", "digest", "transaction", "created_at", "record"])
async def test_endpoint_never_downgrades_damaged_credentials(fault):
    """Missing or contradictory persisted/runtime identity cannot yield anonymous access."""
    from datetime import timedelta
    from control_plane.storage.lifecycle_store import LifecycleStoreError

    runtime, store = CredentialRuntime(), FakeLifecycleStore()
    service = SandboxService(runtime, store)
    created = await service.create()
    spec = runtime.specs[0]
    endpoint = await runtime.get_endpoint(created.sandbox_id)
    if fault == "missing":
        store.credentials.clear()
    elif fault == "record":
        store.records.clear()
    elif fault == "token":
        store.credentials[created.sandbox_id] = SandboxAccessCredential(created.sandbox_id, spec.transaction_id, "B" * 43)
    elif fault == "store":
        async def fail_read(sandbox_id, transaction_id):
            """Simulate unavailable credential storage without leaking its internal error."""
            raise LifecycleStoreError("private storage detail")
        store.get_credential = fail_read
    else:
        changes = {
            "marker": {"auth_version": "2"}, "digest": {"credential_digest": "0" * 64},
            "transaction": {"transaction_id": TXN},
            "created_at": {"created_at": spec.created_at + timedelta(microseconds=1)},
        }[fault]
        endpoint = replace(endpoint, **changes)

        async def changed_endpoint(sandbox_id):
            """Return a stable contradictory proxy observation."""
            return endpoint
        runtime.get_endpoint = changed_endpoint
    expected = SandboxMetadataConflict if fault in {"transaction", "created_at"} else SandboxEndpointAuthUnavailable
    with pytest.raises(expected) as captured:
        await service.get_endpoint(created.sandbox_id)
    sanitized_error = spec.access_token not in str(captured.value)
    assert sanitized_error


@pytest.mark.anyio
async def test_legacy_endpoint_error_is_no_store_and_query_remains_supported():
    """Legacy resource management stays available while credential delivery fails closed."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    app = create_app(runtime, store=store, api_key=ADMIN, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control", headers={"Authorization": "Bearer " + ADMIN}) as client:
        result = await client.get(f"/v1/sandboxes/{SBX}/endpoint")
        assert result.status_code == 409 and result.headers["cache-control"] == "no-store"
        assert result.json()["error"]["code"] == "sandbox_endpoint_auth_required"
        queried = await client.get(f"/v1/sandboxes/{runtime.result.sandbox_id}")
        assert queried.status_code == 200
        assert "access_token" not in queried.json()
        assert (await client.delete(f"/v1/sandboxes/{SBX}")).status_code == 204
