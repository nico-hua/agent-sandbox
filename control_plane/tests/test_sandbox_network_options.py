"""Strict exact-domain network creation policy without Docker or external networking."""

import json
from dataclasses import FrozenInstanceError, replace
from unittest.mock import AsyncMock

import httpx
import pytest

from control_plane.app import create_app
import control_plane.core.models as models
from control_plane.core.service import SandboxService
from lifecycle_fakes import FakeLifecycleStore
from test_sandbox_service import FakeRuntime
from test_sandbox_runtime import make_spec


@pytest.mark.anyio
async def test_network_creation_normalizes_and_propagates_exact_domains():
    """HTTP network policy reaches the runtime alongside the selected preset and limits."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    app = create_app(runtime, store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", json={
            "environment": "python", "ttl_seconds": 123,
            "resources": {"cpu_millis": 750, "memory_mib": 384},
            "network": {"allow_domains": ["EXAMPLE.COM.", "api.example.com", "example.com"]},
        })
    assert response.status_code == 201, response.text
    spec, = runtime.specs
    assert spec.network.allow_domains == ("api.example.com", "example.com")
    assert spec.environment == "python" and spec.resources == models.SandboxResources(750, 384)
    assert (spec.expires_at - spec.created_at).total_seconds() == 123
    assert store.records[spec.sandbox_id].management_state == "active"
    assert set(response.json()) == {"sandbox_id", "status", "created_at", "expires_at"}


@pytest.mark.anyio
@pytest.mark.parametrize("body", [b"", b"{}", b'{"network":{}}', b'{"network":{"allow_domains":[]}}'])
async def test_omitted_and_empty_network_remain_disabled(body):
    """Omitted network settings and empty allowlists carry the same disabled policy."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    app = create_app(runtime, store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", content=body)
    assert response.status_code == 201
    assert runtime.specs[0].network.allow_domains == ()


def test_network_copies_input_and_preserves_positional_options_and_specs():
    """Canonical policy is immutable while existing positional construction remains compatible."""
    supplied = ["EXAMPLE.COM.", "api.example.com", "example.com"]
    network = models.SandboxNetwork(supplied)
    supplied[0] = "attacker.test"
    assert network.allow_domains == ("api.example.com", "example.com")
    assert models.SandboxNetwork(("Example.Com",)).allow_domains == ("example.com",)
    with pytest.raises(FrozenInstanceError):
        network.allow_domains = ()
    options = models.SandboxCreateOptions(123, models.SandboxResources(750, 384), "python")
    assert options.network.allow_domains == make_spec().network.allow_domains == ()
    spec = replace(make_spec(), network=network)
    assert spec.egress_name == "agent-sandbox-" + spec.sandbox_id + "-egress"
    assert spec.labels_for("egress")["io.agent-sandbox.resource"] == "egress"
    assert set(spec.labels_for("egress")) == {
        "io.agent-sandbox.managed", "io.agent-sandbox.project", "io.agent-sandbox.sandbox-id",
        "io.agent-sandbox.transaction-id", "io.agent-sandbox.resource",
    }


@pytest.mark.parametrize("domains", [None, "example.com", {}, {"example.com"}, True, 1, [None], [True], [1], [[]]])
def test_network_rejects_unvalidated_container_and_domain_types(domains):
    """Direct models reject iterable coercion and non-string policy entries."""
    with pytest.raises(ValueError):
        models.SandboxNetwork(domains)


@pytest.mark.parametrize("domain", [
    "", ".", "example.com..", " example.com", "example.com ", "ex ample.com", "example.com\n", "a\x00.test",
    "https://example.com", "user@example.com", "example.com:443", "example.com/path", "*.example.com",
    "example.com?x=1", "example.com#part", "a_b.test", "-a.test", "a-.test", "a..test",
    "例子.test", "exämple.test", "K.example.com", "a" * 64 + ".test", "a." * 127 + "a",
    "127.0.0.1", "8.8.8.8.", "::1", "[::1]", "::ffff:8.8.8.8", "[fe80::1%eth0]",
    "2130706433", "127.1", "127.0.1", "0177.0.0.1", "0x7f000001", "0x7f.0x0.0.1", "0xffffffff",
    "1.2.3.999", "9999999999999999999999999999999999999", "0xFFFFFFFFFFFFFFFFFFFFFFFF", "00.00.00.01",
    "0x", "0x.0", "0x.0x",
])
def test_network_rejects_same_malformed_and_numeric_hosts_as_go(domain):
    """Go proxy policy samples cannot pass Python preflight through a different spelling."""
    with pytest.raises(ValueError):
        models.SandboxNetwork([domain])


def test_network_dns_and_input_capacity_boundaries():
    """DNS limits apply after one root dot and the 32-entry bound precedes deduplication."""
    longest = "a" * 63 + "." + "b" * 63 + "." + "c" * 63 + "." + "d" * 61
    for domain in (longest, "a", "a-b.test", "123.example", "0xff.example", "0x.example"):
        assert models.SandboxNetwork([domain + "."]).allow_domains == (domain,)
    with pytest.raises(ValueError):
        models.SandboxNetwork([longest + "d"])
    assert models.SandboxNetwork(["EXAMPLE.COM."] * 32).allow_domains == ("example.com",)
    with pytest.raises(ValueError):
        models.SandboxNetwork(["example.com"] * 33)


@pytest.mark.parametrize("network", [None, {}, [], True, "example.com"])
def test_options_and_specs_require_validated_network_objects(network):
    """Direct creation options and runtime specs cannot accept arbitrary network mappings."""
    with pytest.raises(ValueError):
        models.SandboxCreateOptions(network=network)
    with pytest.raises(ValueError):
        replace(make_spec(), network=network)


@pytest.mark.anyio
@pytest.mark.parametrize("forged", [None, [], ("EXAMPLE.COM",), ("example.com", "example.com"), ("127.0.0.1",), (True,), ("b.test", "a.test")])
async def test_service_revalidates_forged_network_before_clock_or_side_effects(forged):
    """Forged frozen policy cannot bypass canonical validation or create pending metadata."""
    network = models.SandboxNetwork()
    object.__setattr__(network, "allow_domains", forged)
    with pytest.raises(ValueError):
        network.validate()
    with pytest.raises(ValueError):
        replace(make_spec(), network=network)
    options = models.SandboxCreateOptions()
    object.__setattr__(options, "network", network)
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    store.create = AsyncMock(side_effect=AssertionError("unexpected prewrite"))

    def forbidden_clock():
        """Fail if invalid network settings reach lifecycle work before validation."""
        pytest.fail("clock read for invalid network policy")

    with pytest.raises(ValueError):
        await SandboxService(runtime, store, clock=forbidden_clock).create(options)
    assert not runtime.specs and not store.records
    store.create.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("body", [
    json.dumps({"network": network}).encode()
    for network in (None, [], True, 1, "default", {"unknown": []}, {"allow_domains": None},
                    {"allow_domains": "example.com"}, {"allow_domains": {}}, {"allow_domains": True},
                    {"allow_domains": [True]}, {"allow_domains": [1]}, {"allow_domains": [None]},
                    {"allow_domains": ["127.0.0.1"]}, {"allow_domains": ["example.com"] * 33},
                    {"allow_domains": [], "proxy_url": "private"})
] + [
    b'{"network":null,"network":{}}',
    b'{"network":{"allow_domains":null,"allow_domains":[]}}',
    b'{"network":{"allow_domains":["example.com"]},"network":{}}',
])
async def test_invalid_network_http_has_no_store_or_runtime_effects(body):
    """Nested shape, entry types, unknown fields and duplicate keys fail before persistence."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    store.create = AsyncMock(side_effect=AssertionError("unexpected prewrite"))
    app = create_app(runtime, store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", content=body)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request" and "private" not in response.text
    assert runtime.specs == runtime.queries == runtime.deletions == []
    assert runtime.ping_calls == 0 and not store.records
    store.create.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("size,status", [(4096, 201), (4097, 413)])
async def test_network_keeps_existing_raw_body_limit(size, status):
    """A valid network object shares creation's original 4096-byte limit exactly."""
    body = b'{"network":{"allow_domains":["example.com"]}}'
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    app = create_app(runtime, store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", content=body + b" " * (size - len(body)))
    assert response.status_code == status
    assert len(runtime.specs) == (1 if status == 201 else 0)
