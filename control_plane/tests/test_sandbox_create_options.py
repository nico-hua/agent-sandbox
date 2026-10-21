"""Constrained creation inputs, side-effect boundaries and resource/TTL propagation."""

import json
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
from unittest.mock import AsyncMock

import httpx
import pytest

import control_plane.core.service as service_module
from control_plane.app import create_app
from control_plane.core.models import SandboxCreateOptions, SandboxResources
from control_plane.core.service import SandboxService
from control_plane.core.errors import SandboxMetadataUnavailable
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from lifecycle_fakes import FakeLifecycleStore
from test_sandbox_service import CREATED_AT, FakeRuntime
from test_sandbox_runtime import RecordingClient, make_spec
from test_service_lifecycle import FaultStore
from test_pending_recovery import RecoveryRuntime


@pytest.mark.parametrize("field,bad", [
    (field, bad)
    for field in ("ttl_seconds", "cpu_millis", "memory_mib")
    for bad in (0, -1, None, True, False, 1.0, "1000", 10**200)
] + [("ttl_seconds", 86401), ("cpu_millis", 99), ("cpu_millis", 2001),
     ("memory_mib", 127), ("memory_mib", 2049)])
def test_domain_models_reject_non_integer_and_out_of_range_values(field, bad):
    """Direct domain construction enforces the same strict bounds as HTTP creation."""
    with pytest.raises(ValueError):
        if field == "ttl_seconds":
            SandboxCreateOptions(ttl_seconds=bad)
        else:
            SandboxResources(**{field: bad})


def test_creation_options_and_resources_are_immutable():
    """Validated settings cannot be mutated or shared through mutable defaults."""
    first, second = SandboxCreateOptions(), SandboxCreateOptions()
    with pytest.raises(FrozenInstanceError):
        first.ttl_seconds = 1
    with pytest.raises(FrozenInstanceError):
        first.resources.memory_mib = 128
    assert second.ttl_seconds == 1800
    assert second.resources == SandboxResources(1000, 256)


@pytest.mark.parametrize("resources", [None, {}, [], True, 1000])
def test_domain_options_and_specs_reject_unvalidated_resource_objects(resources):
    """Neither creation options nor Runtime specs accept arbitrary resource mappings or values."""
    with pytest.raises(ValueError):
        SandboxCreateOptions(resources=resources)
    with pytest.raises(ValueError):
        replace(make_spec(), resources=resources)


@pytest.fixture
async def create_context(tmp_path, monkeypatch):
    """Use a fixed clock and real temporary SQLite with an isolated runtime double."""
    store = SQLiteLifecycleStore(tmp_path / "create.sqlite3")
    await store.initialize()
    runtime = FakeRuntime()
    monkeypatch.setattr(service_module, "_utc_now", lambda: CREATED_AT)
    app = create_app(runtime, store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        yield client, runtime, store


@pytest.mark.anyio
@pytest.mark.parametrize("body,ttl,cpu,memory", [
    (b"", 1800, 1000, 256), (b"{}", 1800, 1000, 256),
    (b'{"resources":{}}', 1800, 1000, 256),
    (b'{"ttl_seconds":1}', 1, 1000, 256),
    (b'{"resources":{"cpu_millis":100}}', 1800, 100, 256),
    (b'{"resources":{"memory_mib":128}}', 1800, 1000, 128),
    (b'{"ttl_seconds":86400,"resources":{"cpu_millis":2000,"memory_mib":2048}}', 86400, 2000, 2048),
    (b'{"ttl_seconds":90,"resources":{"cpu_millis":750,"memory_mib":384}}', 90, 750, 384),
])
async def test_http_creation_defaults_and_explicit_values_persist(create_context, body, ttl, cpu, memory):
    """Only omitted fields default; requested limits and one original TTL reach the real row."""
    client, runtime, store = create_context
    response = await client.post("/v1/sandboxes", content=body)
    assert response.status_code == 201, response.text
    spec, = runtime.specs
    assert (spec.resources.cpu_millis, spec.resources.memory_mib) == (cpu, memory)
    assert spec.created_at == CREATED_AT
    assert spec.expires_at == CREATED_AT + timedelta(seconds=ttl)
    record = await store.get(spec.sandbox_id)
    assert record.created_at == spec.created_at and record.expires_at == spec.expires_at
    assert record.management_state == "active" and record.version == 1
    assert response.json() == {
        "sandbox_id": spec.sandbox_id, "status": "started",
        "created_at": spec.created_at.isoformat().replace("+00:00", "Z"),
        "expires_at": spec.expires_at.isoformat().replace("+00:00", "Z"),
    }


@pytest.mark.anyio
@pytest.mark.parametrize("body", [
    json.dumps({field: bad} if field == "ttl_seconds" else {"resources": {field: bad}}).encode()
    for field in ("ttl_seconds", "cpu_millis", "memory_mib")
    for bad in (0, -1, None, True, False, 1.0, "1000", 10**200)
] + [
    b'{"ttl_seconds":86401}', b'{"resources":{"cpu_millis":99}}',
    b'{"resources":{"cpu_millis":2001}}', b'{"resources":{"memory_mib":127}}',
    b'{"resources":{"memory_mib":2049}}', b' ', b'{', b'null', b'[]', b'1', b'true',
    b'"options"', b'{"resources":null}', b'{"resources":[]}', b'{"resources":1}',
    b'{"resources":true}', b'{"resources":"default"}', b'{"image":"private"}',
    b'{"resources":{"privileged":true}}', b'{"ttl_seconds":NaN}',
    b'{"resources":{"cpu_millis":Infinity}}', b'{"ttl_seconds":-Infinity}',
    b'{"ttl_seconds":NaN,"ttl_seconds":1}', b'\xff',
    b'{"ttl_seconds":0,"ttl_seconds":1800}',
    b'{"resources":{"cpu_millis":0},"resources":{}}',
    b'{"ttl_seconds":' + b'9' * 3900 + b'}',
])
async def test_invalid_http_creation_has_no_store_or_runtime_effects(body):
    """Malformed or unsafe settings are rejected before persistence or Docker delegation."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    store.create = AsyncMock(side_effect=AssertionError("unexpected write"))
    app = create_app(runtime, store=store, reaper_enabled=False)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", content=body)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_request"
    assert "private" not in response.text
    assert runtime.specs == runtime.queries == runtime.deletions == []
    assert runtime.ping_calls == 0 and not store.records
    store.create.assert_not_awaited()


@pytest.mark.anyio
@pytest.mark.parametrize("content_length", [None, "1", "999999"])
async def test_creation_body_limit_counts_streamed_bytes_and_stops_early(content_length):
    """The 4 KiB cap uses actual chunks, including absent or misleading length headers."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    app = create_app(runtime, store=store, reaper_enabled=False)
    remainder_read = False

    async def chunks():
        """Overflow after two chunks; draining the remainder would violate the bounded read."""
        nonlocal remainder_read
        yield b" " * 2048
        yield b" " * 2049
        remainder_read = True
        yield b"unbounded remainder"

    headers = {} if content_length is None else {"content-length": content_length}
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", content=chunks(), headers=headers)
    assert response.status_code == 413
    assert response.json()["error"]["code"] == "invalid_request"
    assert not remainder_read and not runtime.specs and not store.records


@pytest.mark.anyio
async def test_exact_body_limit_accepts_valid_creation(create_context):
    """Exactly 4096 raw bytes remains a legal JSON request, independent of length headers."""
    client, runtime, _ = create_context
    response = await client.post("/v1/sandboxes", content=b"{}" + b" " * 4094)
    assert response.status_code == 201 and len(runtime.specs) == 1


@pytest.mark.anyio
async def test_body_limit_counts_bytes_instead_of_unicode_characters(create_context):
    """A body shorter than 4096 characters can still exceed the actual byte budget."""
    client, runtime, _ = create_context
    body = '{"ttl_seconds":"' + "汉" * 1500 + '"}'
    assert len(body) < 4096 < len(body.encode())
    response = await client.post("/v1/sandboxes", content=body.encode())
    assert response.status_code == 413 and not runtime.specs


@pytest.mark.anyio
async def test_creation_body_cap_does_not_apply_to_renewal(create_context):
    """The new 4 KiB limit is local to creation, leaving renewal's existing contract intact."""
    client, _, _ = create_context
    created = await client.post("/v1/sandboxes")
    sandbox_id = created.json()["sandbox_id"]
    response = await client.post(
        f"/v1/sandboxes/{sandbox_id}/renew",
        content=b'{"ttl_seconds":3600}' + b" " * 4096,
    )
    assert response.status_code == 200


@pytest.mark.anyio
async def test_service_revalidates_options_before_clock_store_or_runtime():
    """Even malformed direct inputs cannot bypass validation and leave a pending row."""
    runtime, store = FakeRuntime(), FakeLifecycleStore()
    store.create = AsyncMock(side_effect=AssertionError("unexpected write"))

    def forbidden_clock():
        """Reject any creation work before the domain input has been accepted."""
        pytest.fail("clock read for invalid options")

    service = SandboxService(runtime, store, clock=forbidden_clock)
    forged = SandboxCreateOptions()
    object.__setattr__(forged.resources, "cpu_millis", True)
    for options in ({}, True, 10, forged):
        with pytest.raises(ValueError):
            await service.create(options)
    store.create.assert_not_awaited()
    assert not runtime.specs


@pytest.mark.anyio
async def test_custom_creation_reads_clock_once_and_preserves_initial_labels(tmp_path):
    """A nondefault TTL reaches SQLite, all five Docker labels and the response unchanged."""
    store = SQLiteLifecycleStore(tmp_path / "labels.sqlite3")
    await store.initialize()
    client = RecordingClient()
    client.volume.attrs["Labels"].update({
        "io.agent-sandbox.created-at": "2026-10-19T08:00:00Z",
        "io.agent-sandbox.expires-at": "2026-10-19T08:02:03Z",
    })
    calls = 0

    def clock():
        """Allow one snapshot only, before either persistence or resource creation."""
        nonlocal calls
        calls += 1
        assert calls == 1 and not client.events
        return CREATED_AT

    result = await SandboxService(
        DockerSandboxRuntime(lambda: client), store, clock=clock,
        id_factory=lambda: make_spec().sandbox_id,
        transaction_id_factory=lambda: make_spec().transaction_id,
    ).create(
        SandboxCreateOptions(123, SandboxResources(750, 384)),
    )
    record = await store.get(result.sandbox_id)
    assert result.expires_at == record.expires_at == CREATED_AT + timedelta(seconds=123)
    labels = [item["labels"] for item in client.networks.create_calls + client.volumes.create_calls + client.containers.create_calls]
    assert len(labels) == 5
    assert all(item["io.agent-sandbox.created-at"] == "2026-10-19T08:00:00Z" for item in labels)
    assert all(item["io.agent-sandbox.expires-at"] == "2026-10-19T08:02:03Z" for item in labels)


@pytest.mark.anyio
async def test_resource_mapping_is_per_instance_and_proxy_stays_fixed():
    """Two requested specs produce independent integer limits and identical proxy safety settings."""
    configs = []
    proxies = []
    for resources in (SandboxResources(100, 128), SandboxResources(2000, 2048)):
        client = RecordingClient()
        await DockerSandboxRuntime(lambda: client).create(replace(make_spec(), resources=resources))
        configs.append(client.containers.create_calls[0]["host_config"])
        proxies.append(client.containers.create_calls[1]["host_config"])
    assert [(c["nano_cpus"], c["mem_limit"], c["memswap_limit"]) for c in configs] == [
        (100_000_000, 134_217_728, 134_217_728),
        (2_000_000_000, 2_147_483_648, 2_147_483_648),
    ]
    assert proxies[0] == proxies[1]
    assert (proxies[0]["nano_cpus"], proxies[0]["mem_limit"], proxies[0]["memswap_limit"]) == (
        500_000_000, "64m", "64m",
    )
    for config in configs:
        assert config["pids_limit"] == 64 and config["cap_drop"] == ["ALL"]
        assert config["read_only"] and config["security_opt"] == ["no-new-privileges:true"]


@pytest.mark.anyio
@pytest.mark.parametrize("environment", ["basic", "python", "node"])
async def test_custom_ttl_survives_pending_recovery_renewal_and_reaping(tmp_path, environment):
    """Lost activation preserves custom times; confirmation and renewal never adjust resources."""
    store = FaultStore(tmp_path / "custom-lifecycle.sqlite3")
    await store.initialize()
    store.fail_activation = True
    runtime = FakeRuntime()
    service = SandboxService(runtime, store, clock=lambda: CREATED_AT)
    options = SandboxCreateOptions(123, SandboxResources(750, 384), environment=environment)
    with pytest.raises(SandboxMetadataUnavailable):
        await service.create(options)
    spec, = runtime.specs
    pending = await store.get(spec.sandbox_id)
    assert pending.management_state == "pending"
    assert pending.expires_at == CREATED_AT + timedelta(seconds=123)
    assert spec.resources == options.resources and spec.environment == environment
    store.fail_activation = False
    recovering = RecoveryRuntime()
    recovering.records[spec.sandbox_id] = pending
    result = await SandboxService(recovering, store, clock=lambda: CREATED_AT).recover_pending_once()
    assert result.recovered == 1
    active = await store.get(spec.sandbox_id)
    assert active.expires_at == pending.expires_at and active.created_at == pending.created_at
    assert not recovering.specs and not recovering.deletions
    now = CREATED_AT + timedelta(seconds=60)
    resumed = SandboxService(runtime, SQLiteLifecycleStore(store.database_path), clock=lambda: now)
    renewed = await resumed.renew(spec.sandbox_id, 180)
    assert renewed.expires_at == CREATED_AT + timedelta(seconds=240)
    assert runtime.specs == [spec] and spec.resources == SandboxResources(750, 384)
    assert (await resumed.get(spec.sandbox_id)).expires_at == renewed.expires_at
    now = renewed.expires_at
    assert (await resumed.reap_expired_once()).succeeded == 1
    assert runtime.deletions == [(spec.sandbox_id, False)]
    assert await store.get(spec.sandbox_id) is None
