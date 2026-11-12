"""Double-opt-in read-only inspection of independently owned real Kubernetes layouts."""

import asyncio
import os

import httpx
import pytest
from kubernetes import client

from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime, create_probe_client
from control_plane.runtime.kubernetes.local_entry import KubernetesLocalEntry
from control_plane.runtime.kubernetes.local_entry_manager import KubernetesLocalEntryManager
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_persistent_workspace_integration import (
    _STORAGE_CLASS, _claim, owned_persistent_workspace,
)
from test_kubernetes_private_workspace_integration import _read_optional, _wait_volume_absent
from test_kubernetes_proxy_integration import _PROXY_IMAGE, _RedactedMapping, _RedactedSecret
from test_kubernetes_proxy_lifecycle_integration import _MANAGED_ROLES, owned_proxy_lifecycle
from test_kubernetes_query_integration import _verify_owned
from test_kubernetes_service_create_integration import (
    _application, _observe_creation, _ready, _running_application,
)

pytestmark = [
    pytest.mark.kubernetes_integration,
    pytest.mark.kubernetes_resource_integration,
    pytest.mark.timeout(300),
    pytest.mark.skipif(
        os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
        or os.getenv("RUN_KUBERNETES_RESOURCE_INTEGRATION") != "1"
        or os.getenv("KUBERNETES_TEST_CONTEXT") != "agent-sandbox-k8s"
        or os.getenv("KUBERNETES_TEST_NAMESPACE") != "agent-sandbox-test",
        reason="both Kubernetes opt-ins and the exact existing local context/namespace are required",
    ),
]

_WORKSPACE_ROLES = _MANAGED_ROLES | {"private-workspace"}


class _ReadOnlyFacade:
    """Expose only SDK reads and client closure to the inspection worker."""

    def __init__(self, delegate, denied, *, networking=False):
        """Borrow one worker-owned client and share a record of every forbidden operation."""
        self._delegate, self._denied = delegate, denied
        self.api_client = delegate.api_client
        if not networking:
            self.networking = _ReadOnlyFacade(delegate.networking, denied, networking=True)

    def __getattr__(self, name):
        """Reject writes, exec and streaming operations even if inspection would catch their failures."""
        if not name.startswith(("read_", "list_")):
            self._denied.append(name)
            raise AssertionError("inspection attempted an operation outside the read-only facade")
        operation = getattr(self._delegate, name)
        if "secret" not in name:
            return operation

        def redacted(*args, **kwargs):
            """Preserve real Secret reads while preventing credential-bearing SDK diagnostics."""
            try:
                response = operation(*args, **kwargs)
            except client.ApiException as error:
                raise client.ApiException(status=error.status, reason="Secret diagnostics suppressed") from None
            except Exception:
                raise RuntimeError("Secret operation failed; diagnostics suppressed") from None
            if isinstance(response, client.V1Secret):
                response.__class__ = _RedactedSecret
            if isinstance(response, client.V1SecretList):
                for item in response.items or []:
                    item.__class__ = _RedactedSecret
            return response

        return redacted


def _inspector(chain, denied):
    """Create a fresh inspector without proxy image or StorageClass creation configuration."""
    def factory(**kwargs):
        """Acquire a private real SDK client whose facade cannot mutate cluster resources."""
        return _ReadOnlyFacade(create_probe_client(**kwargs), denied)

    return KubernetesSandboxRuntime(
        kubeconfig_path=chain["path"], context=chain["context"], namespace=chain["namespace"],
        client_factory=factory,
    )


def _snapshot(chain):
    """Capture original UID metadata/specs without Secret contents or normal controller bookkeeping."""
    result = _RedactedMapping({})
    for entry in chain["managed"].entries:
        expected = entry["expected"]
        facade = chain["api"].networking if entry["kind"] == "network_policy" else chain["api"]
        resource = _read_optional(getattr(facade, "read_namespaced_" + entry["kind"]),
                                  expected["name"], expected["namespace"])
        key = (entry["kind"], expected["name"])
        if resource is None:
            result[key] = None
            continue
        _verify_owned(resource, expected, entry["uid"])
        metadata = resource.metadata.to_dict()
        # Status updates can advance resourceVersion and managedFields timestamps
        # without changing the desired resource or its ownership authority.
        metadata.pop("resource_version", None)
        metadata.pop("managed_fields", None)
        spec = getattr(resource, "spec", None)
        result[key] = (metadata, None if spec is None else spec.to_dict())
    return result


async def _inspect_unchanged(chain, inspector, denied, monkeypatch):
    """Inspect behind fail-fast endpoint/session/HTTP guards and compare safe original snapshots."""
    before = await asyncio.to_thread(_snapshot, chain)
    side_effects = []

    def forbidden(*args, **kwargs):
        """Record forbidden data-plane access without exposing any credentials or arguments."""
        side_effects.append(True)
        raise AssertionError("inspection attempted endpoint, session or application HTTP access")

    with monkeypatch.context() as patch:
        patch.setattr(KubernetesSandboxRuntime, "get_endpoint", forbidden)
        patch.setattr(KubernetesLocalEntryManager, "open", forbidden)
        patch.setattr(KubernetesLocalEntry, "open", forbidden)
        patch.setattr(httpx.AsyncClient, "request", forbidden)
        patch.setattr(httpx.Client, "request", forbidden)
        spec = chain["spec"]
        result = await inspector.inspect_resources(
            spec.sandbox_id, expected_transaction_id=spec.transaction_id,
        )
    after = await asyncio.to_thread(_snapshot, chain)
    unchanged = before == after
    assert unchanged, "inspection changed original resource metadata or desired specifications"
    assert not denied, "inspection attempted a non-read SDK operation"
    assert not side_effects, "inspection attempted data-plane access"
    assert result.sandbox_id == spec.sandbox_id
    return result


def _assert_roles(result, classification, reason, found, missing):
    """Require the exact classification and unique public resource roles without printing metadata."""
    assert result.classification == classification
    assert result.reason == reason
    assert set(result.found_resources) == found
    assert len(result.found_resources) == len(found)
    assert set(result.missing_resources) == missing
    assert len(result.missing_resources) == len(missing)


def test_real_private_workspace_inspection_is_read_only_across_resource_loss_and_deletion(
    owned_persistent_workspace, tmp_path, monkeypatch,
):
    """Inspect all nine originals, a missing Service, retained storage and final absence without writes."""
    chain = owned_persistent_workspace

    async def verify():
        """Create via real Service/SQLite, then distinguish partial ownership from retained private storage."""
        backend = chain["make_runtime"](proxy_image=_PROXY_IMAGE, workspace_storage_class=_STORAGE_CLASS)
        store = SQLiteLifecycleStore(tmp_path / "resource-inspection.sqlite3")
        manager = KubernetesLocalEntryManager(backend)
        _observe_creation(chain, backend, store, monkeypatch)
        app, _ = _application(backend, manager, store)
        denied = []
        inspector = _inspector(chain, denied)
        async with _running_application(app, [], manager):
            service = app.state.service
            await service.create()
            spec = chain["spec"]
            await _ready(service, spec.sandbox_id)
            chain["managed"].assert_complete()
            original_claim = await asyncio.to_thread(_claim, chain)
            assert original_claim.status.phase == "Bound"
            result = await _inspect_unchanged(chain, inspector, denied, monkeypatch)
            _assert_roles(result, "all_present", "kubernetes_resources_complete", _WORKSPACE_ROLES, set())
            assert manager.session_count == 0

            await asyncio.to_thread(chain["managed"].delete, chain["managed"].role("agent-service"))
            result = await _inspect_unchanged(chain, inspector, denied, monkeypatch)
            _assert_roles(result, "partial", "kubernetes_resources_partial",
                          _WORKSPACE_ROLES - {"agent-service"}, {"agent-service"})

            # Only explicit Runtime deletion mutates managed objects; inspection
            # must recover the retained claim without creation configuration.
            restarted = chain["make_runtime"]()
            await restarted.delete(spec.sandbox_id, expected_transaction_id=spec.transaction_id)
            await asyncio.to_thread(chain["managed"].assert_compute_absent)
            retained = await asyncio.to_thread(_claim, chain)
            assert retained.metadata.uid == original_claim.metadata.uid
            result = await _inspect_unchanged(chain, inspector, denied, monkeypatch)
            _assert_roles(result, "partial", "kubernetes_private_workspace_retained",
                          {"private-workspace"}, _MANAGED_ROLES)
            assert manager.session_count == 0

            await restarted.delete(spec.sandbox_id, delete_workspace=True,
                                   expected_transaction_id=spec.transaction_id)
            await asyncio.to_thread(chain["managed"].assert_absent)
            await asyncio.to_thread(_wait_volume_absent, chain["api"], chain["volume"])
            result = await _inspect_unchanged(chain, inspector, denied, monkeypatch)
            assert result.classification == "not_found"
            assert result.reason == "kubernetes_resources_not_found"
            assert not result.found_resources
            assert not result.missing_resources
            assert manager.session_count == 0

    asyncio.run(verify())


def test_real_legacy_proxy_inspection_preserves_eight_resource_layout(owned_proxy_lifecycle, monkeypatch):
    """Inspect the existing eight-resource proxy layout and its original missing-Service case read-only."""
    chain = owned_proxy_lifecycle

    async def verify():
        """Create with cached images, then inspect originals without inventing a private workspace role."""
        backend = chain["make_runtime"](proxy_image=_PROXY_IMAGE)
        await backend.create(chain["spec"])
        await _ready(backend, chain["spec"].sandbox_id)
        chain["managed"].assert_complete()
        denied = []
        inspector = _inspector(chain, denied)
        result = await _inspect_unchanged(chain, inspector, denied, monkeypatch)
        _assert_roles(result, "all_present", "kubernetes_resources_complete", _MANAGED_ROLES, set())
        await asyncio.to_thread(chain["managed"].delete, chain["managed"].role("agent-service"))
        result = await _inspect_unchanged(chain, inspector, denied, monkeypatch)
        _assert_roles(result, "partial", "kubernetes_resources_partial",
                      _MANAGED_ROLES - {"agent-service"}, {"agent-service"})

    asyncio.run(verify())
