"""Offline versioned local-entry snapshots bind each persistent workspace identity."""

from dataclasses import fields, replace
from types import SimpleNamespace

import pytest

from control_plane.core.errors import KubernetesOperationUnsupported, SandboxQueryFailed
from control_plane.runtime.kubernetes.local_entry import LocalEntryError, LocalEntryTarget
from test_kubernetes_local_entry import FakeForwardProcess, entry_harness, target_snapshot
from test_kubernetes_local_entry_manager import manager_harness
from test_kubernetes_query import SID

WORKSPACE_UID = "45667ab6-3f02-40ba-9b29-b1967503a3f0"
REPLACEMENT_UID = "55667ab6-3f02-40ba-9b29-b1967503a3f0"


def persistent_snapshot(**changes):
    """Extend an independent v2 snapshot with the original private PVC identity."""
    original = target_snapshot()
    records = (*original.resource_uids, ("workspace", WORKSPACE_UID))
    options = dict(layout_version="3", resource_uids=tuple(sorted(records)))
    options.update(changes)
    return replace(original, **options)


def test_persistent_target_binds_workspace_without_exposing_credential_digest():
    """A v3 target admits nine distinct resource UIDs and keeps credentials out of diagnostics."""
    target = persistent_snapshot()
    assert target.layout_version == "3"
    assert len(target.resource_uids) == 9
    assert dict(target.resource_uids)["workspace"] == WORKSPACE_UID
    assert target.credential_digest not in repr(target)
    assert all("token" not in item.name for item in fields(LocalEntryTarget))
    assert target != persistent_snapshot(resource_uids=tuple(sorted(
        {**dict(target.resource_uids), "workspace": REPLACEMENT_UID}.items())))


@pytest.mark.parametrize("version", ["1", "4", "03", "", None, 2, 3, True])
@pytest.mark.parametrize("persistent", [False, True])
def test_target_rejects_unknown_layout_versions(version, persistent):
    """Only explicit supported string versions can authorize a local entry target."""
    records = target_snapshot().resource_uids
    if persistent:
        records = (*records, ("workspace", WORKSPACE_UID))
    with pytest.raises(SandboxQueryFailed):
        target_snapshot(layout_version=version, resource_uids=records)


@pytest.mark.parametrize("version,fault", [
    ("2", "extra-workspace"), ("3", "missing-workspace"), ("3", "unknown-key"),
    ("3", "duplicate-key"), ("3", "duplicate-uid"), ("3", "invalid-uid"),
])
def test_target_requires_exact_keys_and_unique_canonical_uids(version, fault):
    """Each version requires its complete closed identity map without duplicate or malformed bindings."""
    records = [*target_snapshot().resource_uids, ("workspace", WORKSPACE_UID)]
    if fault == "missing-workspace":
        records.pop()
    elif fault == "unknown-key":
        records[-1] = ("other", WORKSPACE_UID)
    elif fault == "duplicate-key":
        records[-1] = ("agent", WORKSPACE_UID)
    elif fault == "duplicate-uid":
        records[-1] = ("workspace", dict(records)["agent"])
    elif fault == "invalid-uid":
        records[-1] = ("workspace", "not-a-uid")
    with pytest.raises(SandboxQueryFailed):
        target_snapshot(layout_version=version, resource_uids=tuple(records))


@pytest.fixture
def resolver_backend(monkeypatch):
    """Stub only the validated Runtime inventory boundary to isolate resolver snapshot extraction."""
    from control_plane.runtime.kubernetes import local_entry_target
    original = target_snapshot()
    state = SimpleNamespace(version="3", closed=False)
    result = SimpleNamespace(status="ready", transaction_id=original.transaction_id,
                             created_at=original.created_at, expires_at=original.expires_at)
    api = SimpleNamespace()

    class VerifiedInventory:
        """Supply a previously verified inventory without cluster access or resource creation."""

        def __init__(self, transport, sandbox_id, namespace, transaction_id):
            """Bind the inventory to the exact borrowed worker transport and original identity."""
            assert transport is api and sandbox_id == original.sandbox_id
            assert namespace == original.namespace and transaction_id == original.transaction_id

        def inventory(self):
            """Expose the agent annotations and deterministic names required by snapshot extraction."""
            return {
                "agent": SimpleNamespace(metadata=SimpleNamespace(annotations={
                    "io.agent-sandbox.kubernetes-proxy-version": state.version,
                    "io.agent-sandbox.proxy-token-sha256": original.credential_digest,
                })),
                "proxy": SimpleNamespace(metadata=SimpleNamespace(name=original.proxy_pod_name)),
                "proxy_service": SimpleNamespace(metadata=SimpleNamespace(name=original.proxy_service_name)),
            }

        def query(self, agent):
            """Represent the successful full resource validation performed before extraction."""
            return result

        def _bindings(self, found):
            """Supply the independently defined original identities for the declared version."""
            records = dict(original.resource_uids)
            if state.version == "3":
                records["workspace"] = WORKSPACE_UID
            return records

    def call_and_close(callback):
        """Run the resolver callback on its borrowed transport and close on every outcome."""
        try:
            return callback(api)
        finally:
            state.closed = True

    monkeypatch.setattr(local_entry_target, "ProxyResources", VerifiedInventory)
    backend = SimpleNamespace(_namespace=original.namespace, _call_and_close=call_and_close,
                              _read_primary_pod=lambda transport, sandbox_id: result)
    return backend, state


@pytest.mark.anyio
@pytest.mark.parametrize("version,count", [("2", 8), ("3", 9)])
async def test_resolver_preserves_actual_validated_layout_version(version, count, resolver_backend):
    """The agent's actual version and complete original bindings survive resolver extraction."""
    from control_plane.runtime.kubernetes.local_entry import resolve_local_entry_target
    backend, state = resolver_backend
    state.version = version
    target = await resolve_local_entry_target(backend, SID)
    assert target.layout_version == version and len(target.resource_uids) == count
    if version == "3":
        assert dict(target.resource_uids)["workspace"] == WORKSPACE_UID
    assert state.closed


@pytest.mark.anyio
@pytest.mark.parametrize("version", ["1", "4"])
async def test_resolver_rejects_unsupported_layout_and_closes_transport(version, resolver_backend):
    """A historical or unknown layout cannot obtain a local entry through an eight-resource fallback."""
    from control_plane.runtime.kubernetes.local_entry import resolve_local_entry_target
    backend, state = resolver_backend
    state.version = version
    with pytest.raises(KubernetesOperationUnsupported):
        await resolve_local_entry_target(backend, SID)
    assert state.closed


@pytest.mark.anyio
async def test_workspace_uid_replacement_before_delivery_reaps_owned_forward(entry_harness):
    """A same-name replacement PVC invalidates the complete snapshot before a URL can be handed out."""
    harness = entry_harness
    harness.target = persistent_snapshot()
    harness.process = FakeForwardProcess()
    entry = harness.make()
    calls = 0

    async def resolver(backend, sandbox_id):
        """Expose a separately valid replacement only after the owned forward reports listening."""
        nonlocal calls
        calls += 1
        if calls == 1:
            return harness.target
        records = dict(harness.target.resource_uids)
        records["workspace"] = REPLACEMENT_UID
        return replace(harness.target, resource_uids=tuple(sorted(records.items())))

    entry._target_resolver = resolver
    with pytest.raises(LocalEntryError) as captured:
        await entry.open()
    assert captured.value.code == "local_entry_target_changed"
    assert harness.process.reaps == 1 and len(harness.calls) == 1


@pytest.mark.anyio
async def test_manager_lookup_rejects_replaced_workspace_without_opening_new_session(manager_harness):
    """A live cached forward never authorizes a separately valid target with a replacement PVC UID."""
    harness = manager_harness
    harness.targets[SID] = persistent_snapshot()
    manager = harness.make()
    await manager.open(SID)
    records = dict(harness.targets[SID].resource_uids)
    records["workspace"] = REPLACEMENT_UID
    harness.targets[SID] = replace(harness.targets[SID], resource_uids=tuple(sorted(records.items())))
    try:
        with pytest.raises(LocalEntryError):
            await manager.lookup(SID)
        assert len(harness.sessions) == 1
    finally:
        await manager.close_all()
