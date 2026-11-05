"""Controlled service connection delivery, strict ports and Docker capability evidence."""

import asyncio
from dataclasses import FrozenInstanceError, replace
from unittest.mock import Mock

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.errors import SandboxEndpointUnavailable, SandboxImageUnavailable
from control_plane.core.service import SandboxService
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from lifecycle_fakes import FakeLifecycleStore
from test_endpoint_auth import ADMIN, CredentialRuntime
from test_endpoint_auth_runtime import authenticated_client
from test_sandbox_endpoint import SPEC
from test_sandbox_runtime import RecordingClient, make_spec

SERVICES_LABEL = "io.agent-sandbox.endpoint-services-version"
IMAGE_LABEL = "io.agent-sandbox.proxy-services-version"


class ServicesRuntime(CredentialRuntime):
    """Provide verified service capability alongside the existing authenticated entry."""

    async def get_endpoint(self, sandbox_id):
        """Report capability independently of Service's credential and identity checks."""
        return replace(await super().get_endpoint(sandbox_id), services_version="1")


@pytest.mark.anyio
@pytest.mark.parametrize("port", [1024, 8081, 65535])
async def test_service_connection_is_authenticated_immutable_and_read_only(port):
    """Service reuses stored credentials without probing or changing the target application."""
    runtime, store = ServicesRuntime(), FakeLifecycleStore()
    service = SandboxService(runtime, store)
    created = await service.create()
    before = dict(store.records)
    result = await service.get_service_endpoint(created.sandbox_id, port)
    assert result.port == port and result.auth_required is True
    assert result.url == f"http://127.0.0.1:49153/services/{port}/"
    same_secret = result.access_token == runtime.specs[0].access_token
    assert same_secret and result.access_token not in repr(result)
    assert store.records == before
    with pytest.raises(FrozenInstanceError):
        result.port = 1025


@pytest.mark.anyio
@pytest.mark.parametrize("port", [0, -1, 80, 1023, 8080, 65536, True, 1024.0, "1024", None])
async def test_service_port_validation_precedes_runtime_and_storage(port):
    """Direct Service calls cannot route reserved or non-integer application ports."""
    runtime, store = Mock(), Mock()
    with pytest.raises(ValueError):
        await SandboxService(runtime, store).get_service_endpoint("sbx_" + "a" * 32, port)
    assert not runtime.mock_calls and not store.mock_calls


@pytest.mark.anyio
async def test_api_delivers_service_credential_only_after_admin_authentication():
    """No-store responses expose credentials only to the administrator, never ordinary query."""
    runtime, store = ServicesRuntime(), FakeLifecycleStore()
    app = create_app(runtime, store=store, api_key=ADMIN, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
        auth = {"Authorization": "Bearer " + ADMIN}
        created = (await client.post("/v1/sandboxes", headers=auth)).json()
        sandbox_id = created["sandbox_id"]
        path = f"/v1/sandboxes/{sandbox_id}/services/3000/endpoint"
        assert (await client.get(path)).status_code == 401
        response = await client.get(path, headers=auth)
        assert response.status_code == 200 and response.headers["cache-control"] == "no-store"
        data = response.json()
        token = data.pop("access_token")
        assert data == {"sandbox_id": sandbox_id, "port": 3000, "url": "http://127.0.0.1:49153/services/3000/", "auth_required": True}
        public = await client.get(f"/v1/sandboxes/{sandbox_id}", headers=auth)
        assert token not in public.text


@pytest.mark.anyio
@pytest.mark.parametrize("raw", ["0", "80", "1023", "8080", "65536", "01024", "+1024", "true", "1.5", "655350"])
async def test_api_rejects_ambiguous_ports_after_auth_without_business_calls(raw):
    """HTTP port parsing is strict and remains behind the administrator boundary."""
    runtime, store = Mock(), Mock()
    app = create_app(runtime, store=store, api_key=ADMIN, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control") as client:
        path = "/v1/sandboxes/sbx_" + "a" * 32 + f"/services/{raw}/endpoint"
        assert (await client.get(path)).status_code == 401
        response = await client.get(path, headers={"Authorization": "Bearer " + ADMIN})
        assert response.status_code == 400 and response.json()["error"]["code"] == "invalid_request"
    assert not store.mock_calls and not runtime.mock_calls


@pytest.mark.anyio
async def test_old_authenticated_proxy_keeps_agent_endpoint_but_rejects_service():
    """Absence of service capability does not remove the previous Agent access contract."""
    runtime, store = CredentialRuntime(), FakeLifecycleStore()
    service = SandboxService(runtime, store)
    created = await service.create()
    assert (await service.get_endpoint(created.sandbox_id)).auth_required is True
    app = create_app(runtime, store=store, api_key=ADMIN, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control", headers={"Authorization": "Bearer " + ADMIN}) as client:
        response = await client.get(f"/v1/sandboxes/{created.sandbox_id}/services/3000/endpoint")
    assert response.status_code == 409 and response.headers["cache-control"] == "no-store"
    assert response.json()["error"] == {"code": "sandbox_service_unsupported", "message": "proxy does not support service forwarding; recreate sandbox"}


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["missing_token", "transaction", "identity", "timeout"])
async def test_service_endpoint_never_bypasses_existing_identity_checks(fault):
    """Conflicts, credential failures and deadlines cannot yield a service URL."""
    runtime, store = ServicesRuntime(), FakeLifecycleStore()
    service = SandboxService(runtime, store)
    created = await service.create()
    original = await runtime.get_endpoint(created.sandbox_id)
    if fault == "missing_token":
        store.credentials.clear()
    else:
        async def damaged(sandbox_id):
            """Provide a controlled inconsistent observation or wait for API cancellation."""
            if fault == "timeout":
                await asyncio.Event().wait()
            return replace(original, **({"transaction_id": "txn_" + "f" * 32} if fault == "transaction" else {"sandbox_id": "sbx_" + "f" * 32}))
        runtime.get_endpoint = damaged
    app = create_app(runtime, store=store, api_key=ADMIN, reaper_enabled=False, sandbox_query_timeout=0.01)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://control", headers={"Authorization": "Bearer " + ADMIN}) as client:
        response = await client.get(f"/v1/sandboxes/{created.sandbox_id}/services/3000/endpoint")
        missing = await client.get("/v1/sandboxes/bad/services/3000/endpoint")
    assert missing.status_code == 404
    assert response.status_code == {"missing_token": 503, "transaction": 409, "identity": 503, "timeout": 504}[fault]
    assert runtime.specs[0].access_token not in response.text


def services_client():
    """Equip all inspected resource identities and the exact image with matching capability."""
    client = authenticated_client()
    for resource in [client.sandbox, client.proxy, *client.networks.resources.values()]:
        resource.attrs.get("Config", {}).get("Labels", resource.attrs.get("Labels", {}))[SERVICES_LABEL] = "1"
    image_get = client.images.get
    image = image_get("ignored")
    image.attrs["Config"]["Labels"][IMAGE_LABEL] = "1"
    client.images.get = lambda _: image
    return client


@pytest.mark.anyio
async def test_runtime_recovers_capability_from_immutable_image_and_resource_facts():
    """A fresh Runtime derives service support without creation-memory or registry lookups."""
    client = services_client()
    result = await DockerSandboxRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)
    assert result.services_version == "1" and client.close_calls == 1
    assert result.access_token is None


@pytest.mark.anyio
@pytest.mark.parametrize("fault", ["missing_resource", "unknown_version", "missing_image", "unknown_image"])
async def test_runtime_refuses_damaged_capability(fault):
    """Contradictory immutable markers cannot produce a trusted service connection."""
    client = services_client()
    if fault == "missing_resource":
        del client.proxy.attrs["Config"]["Labels"][SERVICES_LABEL]
    elif fault == "unknown_version":
        client.proxy.attrs["Config"]["Labels"][SERVICES_LABEL] = "2"
    else:
        labels = client.images.get("ignored").attrs["Config"]["Labels"]
        if fault == "missing_image":
            del labels[IMAGE_LABEL]
        else:
            labels[IMAGE_LABEL] = "2"
    with pytest.raises(SandboxEndpointUnavailable):
        await DockerSandboxRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_creation_rejects_old_auth_only_image_before_resource_mutation():
    """Matching the old authentication version alone is insufficient for a new proxy."""
    client = RecordingClient()
    original = client.images.get

    def auth_only(name):
        """Expose an old image with authentication but no service-forwarding contract."""
        image = original(name)
        image.attrs["Config"]["Labels"].pop(IMAGE_LABEL, None)
        return image

    client.images.get = auth_only
    with pytest.raises(SandboxImageUnavailable):
        await DockerSandboxRuntime(lambda: client).create(make_spec())
    assert not client.networks.create_calls
