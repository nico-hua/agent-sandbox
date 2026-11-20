"""Endpoint contract, recovery and defensive Docker inspect tests."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from requests.exceptions import ReadTimeout

from control_plane.app import create_app
from control_plane.domain.errors import SandboxEndpointUnavailable, SandboxNotFound
from control_plane.domain.models import SandboxEndpoint
from control_plane.services.sandbox_service import SandboxService
from control_plane.tests.http_assertions import assert_failure, success_data
from control_plane.tests.fakes import FakeLifecycleStore
from control_plane.runtime import resources
from control_plane.runtime.docker_runtime import DockerRuntime
from control_plane.tests.fakes import APIRuntime as FakeRuntime
from control_plane.tests.runtime.fakes import make_spec
from control_plane.tests.runtime.fakes import (
    EndpointClient,
    ENDPOINT_SPEC as SPEC,
    resource,
)


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [RuntimeError("private close"), ReadTimeout("private close")])
async def test_endpoint_close_failure_preserves_not_found(failure: Exception) -> None:
    """Closing a client must not turn a missing sandbox into unavailable or timeout."""
    client = EndpointClient()
    client.containers.resources.clear()

    def fail_close() -> None:
        """Fail finalization independently of the missing resource."""
        client.close_calls += 1
        raise failure

    client.close = fail_close
    with pytest.raises(SandboxNotFound):
        await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_endpoint_recovers_actual_port_with_fresh_runtime() -> None:
    """Fresh runtimes must derive the same address from actual port mappings."""
    client = EndpointClient()
    expected = SandboxEndpoint(SPEC.sandbox_id, "http://127.0.0.1:49153")
    assert await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id) == expected
    assert await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id) == expected
    assert client.close_calls == 2


@pytest.mark.anyio
@pytest.mark.parametrize("case", [
    "old", "unhealthy", "stopped", "missing_health", "transaction", "ownership", "ambiguous",
    "sandbox_extra_network", "proxy_missing_network", "proxy_extra_network", "wrong_network_id",
    "network_transaction", "internal_not_internal", "ingress_internal", "wrong_driver", "missing_alias",
    "missing_mapping", "wrong_ip", "wildcard_ip", "multiple_mappings", "invalid_port", "zero_port", "overflow_port",
])
async def test_endpoint_rejects_unavailable_or_untrusted_entry(case: str) -> None:
    """Incomplete health, ownership, topology or port facts must never yield a URL."""
    client = EndpointClient()
    proxy = client.proxy.attrs
    networks = proxy["NetworkSettings"]["Networks"]
    if case == "old":
        del client.containers.resources["proxy-id"]
    elif case == "unhealthy":
        proxy["State"]["Health"]["Status"] = "starting"
    elif case == "stopped":
        proxy["State"]["Status"] = "exited"
    elif case == "missing_health":
        del proxy["State"]["Health"]
    elif case in {"transaction", "ownership"}:
        key = "transaction-id" if case == "transaction" else "project"
        proxy["Config"]["Labels"][f"io.agent-sandbox.{key}"] = "foreign"
    elif case == "ambiguous":
        client.containers.resources["second-proxy"] = resource("second-proxy", deepcopy(proxy))
    elif case == "sandbox_extra_network":
        client.sandbox.attrs["NetworkSettings"]["Networks"]["bridge"] = {}
    elif case == "proxy_missing_network":
        del networks[SPEC.network_name]
    elif case == "proxy_extra_network":
        networks["bridge"] = {}
    elif case == "wrong_network_id":
        networks[SPEC.network_name]["NetworkID"] = "foreign"
    elif case == "network_transaction":
        client.networks.resources["ingress-id"].attrs["Labels"]["io.agent-sandbox.transaction-id"] = "foreign"
    elif case == "internal_not_internal":
        client.networks.resources["internal-id"].attrs["Internal"] = False
    elif case == "ingress_internal":
        client.networks.resources["ingress-id"].attrs["Internal"] = True
    elif case == "wrong_driver":
        client.networks.resources["ingress-id"].attrs["Driver"] = "overlay"
    elif case == "missing_alias":
        client.sandbox.attrs["NetworkSettings"]["Networks"][SPEC.network_name]["Aliases"] = []
    else:
        ports = proxy["NetworkSettings"]["Ports"]
        if case == "missing_mapping":
            ports["8080/tcp"] = None
        elif case in {"wrong_ip", "wildcard_ip"}:
            ports["8080/tcp"][0]["HostIp"] = "::1" if case == "wrong_ip" else "0.0.0.0"
        elif case == "multiple_mappings":
            ports["8080/tcp"] *= 2
        else:
            ports["8080/tcp"][0]["HostPort"] = {"invalid_port": "secret", "zero_port": "0", "overflow_port": "65536"}[case]
    with pytest.raises(SandboxEndpointUnavailable):
        await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_unknown_endpoint_is_not_found() -> None:
    """An unknown sandbox must remain distinct from an unavailable entry."""
    client = EndpointClient()
    del client.containers.resources["sandbox-id"]
    with pytest.raises(SandboxNotFound):
        await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)


@pytest.mark.anyio
async def test_endpoint_transport_timeout_is_preserved() -> None:
    """Docker read timeouts must map to the public endpoint timeout contract."""
    client = EndpointClient()
    client.containers.error = ReadTimeout("secret socket")
    with pytest.raises(TimeoutError):
        await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_endpoint_api_uses_shared_service(monkeypatch: pytest.MonkeyPatch) -> None:
    """The endpoint HTTP route must delegate through Service to FakeRuntime."""
    runtime = FakeRuntime()
    calls: list[str] = []
    original = SandboxService.get_endpoint

    async def observe(self: SandboxService, sandbox_id: str) -> SandboxEndpoint:
        """Record Service entry before delegating to the runtime."""
        calls.append(sandbox_id)
        return await original(self, sandbox_id)

    monkeypatch.setattr(SandboxService, "get_endpoint", observe)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(runtime, store=FakeLifecycleStore(), reaper_enabled=False)), base_url="http://control") as client:
        created = await client.post("/v1/sandboxes")
        sandbox_id = success_data(created)["sandbox_id"]
        response = await client.get(f"/v1/sandboxes/{sandbox_id}/endpoint")
    assert response.status_code == 200
    payload = success_data(response)
    valid_token = payload.pop("access_token") == runtime.specs[0].access_token
    assert valid_token
    assert payload == {
        "sandbox_id": sandbox_id, "url": "http://127.0.0.1:49153", "auth_required": True,
    }
    assert calls == [sandbox_id]
    assert runtime.sandbox_ids == [sandbox_id, sandbox_id]


@pytest.mark.anyio
@pytest.mark.parametrize("error,status,message", [
    (SandboxNotFound("secret"), 404, "sandbox not found"),
    (SandboxEndpointUnavailable("secret"), 503, "sandbox endpoint is unavailable"),
    (TimeoutError("secret"), 504, "sandbox endpoint query timed out"),
])
async def test_endpoint_api_sanitizes_errors(error: Exception, status: int, message: str) -> None:
    """Endpoint failures must have stable status and hide backend diagnostics."""
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(FakeRuntime(error), store=FakeLifecycleStore(), reaper_enabled=False)), base_url="http://control") as client:
        response = await client.get(f"/v1/sandboxes/{SPEC.sandbox_id}/endpoint")
    assert response.status_code == status
    assert_failure(response, message)
    assert "secret" not in response.text


@pytest.mark.anyio
async def test_endpoint_api_invalid_id_never_calls_runtime() -> None:
    """Malformed public IDs are rejected at the HTTP boundary before lookup."""
    runtime = FakeRuntime()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(runtime, store=FakeLifecycleStore(), reaper_enabled=False)), base_url="http://control") as client:
        response = await client.get("/v1/sandboxes/invalid/endpoint")
    assert response.status_code == 400
    assert_failure(response, "invalid sandbox ID")
    assert runtime.calls == 0


@pytest.mark.anyio
async def test_endpoint_api_deadline_cancels_runtime() -> None:
    """A hanging endpoint query must receive cancellation at the API deadline."""
    cancelled = asyncio.Event()

    class BlockingRuntime(FakeRuntime):
        """Block only endpoint reads to exercise the route deadline."""

        async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
            """Wait until the route cancels this lookup."""
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(BlockingRuntime(), sandbox_query_timeout=0.02, store=FakeLifecycleStore(), reaper_enabled=False)), base_url="http://control") as client:
        response = await client.get(f"/v1/sandboxes/{SPEC.sandbox_id}/endpoint")
    assert response.status_code == 504
    assert cancelled.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("label", ["managed", "project", "sandbox-id", "transaction-id", "resource"])
async def test_endpoint_incomplete_sandbox_ownership_is_unknown(label: str) -> None:
    """Sandbox ownership must be complete before its existence is revealed."""
    client = EndpointClient()
    del client.sandbox.attrs["Config"]["Labels"][f"io.agent-sandbox.{label}"]
    with pytest.raises(SandboxNotFound):
        await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_endpoint_malformed_sandbox_transaction_is_unknown() -> None:
    """A malformed transaction type must not bypass unknown-sandbox semantics."""
    client = EndpointClient()
    client.sandbox.attrs["Config"]["Labels"]["io.agent-sandbox.transaction-id"] = None
    with pytest.raises(SandboxNotFound):
        await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)


@pytest.mark.anyio
async def test_endpoint_refuses_sandbox_published_port() -> None:
    """An accidentally published sandbox must fail entry topology verification."""
    client = EndpointClient()
    client.sandbox.attrs["NetworkSettings"]["Ports"] = {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "49154"}]}
    with pytest.raises(SandboxEndpointUnavailable):
        await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)


@pytest.mark.anyio
async def test_endpoint_rejects_valid_but_different_proxy_transaction() -> None:
    """Two individually valid transaction labels must still belong to one attempt."""
    client = EndpointClient()
    client.proxy.attrs["Config"]["Labels"]["io.agent-sandbox.transaction-id"] = "txn_" + "f" * 32
    with pytest.raises(SandboxEndpointUnavailable):
        await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)


@pytest.mark.anyio
async def test_endpoint_client_close_failure_is_sanitized() -> None:
    """Transport close failures must keep the endpoint error contract sanitized."""
    client = EndpointClient()

    def fail_close() -> None:
        """Simulate a transport cleanup error containing backend details."""
        raise RuntimeError("secret socket")

    client.close = fail_close
    with pytest.raises(SandboxEndpointUnavailable):
        await DockerRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)


@pytest.mark.anyio
async def test_endpoint_default_factory_is_lazy_and_uses_five_second_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fresh endpoint reads must use the lifecycle client timeout and close it."""
    client = EndpointClient()
    calls: list[dict[str, Any]] = []

    def from_env(**kwargs: Any) -> EndpointClient:
        """Capture the lazy SDK factory arguments without connecting."""
        calls.append(kwargs)
        return client

    monkeypatch.setattr(resources.docker, "from_env", from_env)
    runtime = DockerRuntime()
    assert calls == []
    assert (await runtime.get_endpoint(SPEC.sandbox_id)).url == "http://127.0.0.1:49153"
    assert calls == [{"timeout": 5.0}]
    assert client.close_calls == 1
