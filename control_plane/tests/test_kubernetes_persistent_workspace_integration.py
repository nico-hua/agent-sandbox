"""Double-opt-in Service transactions using private PVCs and cached local images.

The fixture independently captures original identities before every POST. Cleanup
removes controlled consumers first, retains storage on Pod cleanup failure, and
only observes the provisioner's Delete reclaim policy; it never deletes a PV.
"""

import asyncio
import json
import os
import secrets
import sys
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from kubernetes import client

from control_plane.core.errors import CreationResourceState, SandboxCreateFailed, SandboxNotFound
from control_plane.runtime.kubernetes import proxy_resources
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime, create_probe_client
from control_plane.runtime.kubernetes.local_entry_manager import KubernetesLocalEntryManager
from control_plane.runtime.kubernetes.private_workspace import private_workspace_name
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_endpoint_integration import _delivered_endpoint
from test_kubernetes_local_entry_integration import _assert_port_closed
from test_kubernetes_private_workspace_integration import (
    _read_optional, _verify_volume, _wait_succeeded, _wait_volume_absent,
)
from test_kubernetes_proxy_integration import (
    _OwnedResources, _PROXY_IMAGE, _ROLE, _RedactedMapping, _RedactedSecret, _RedactedText,
)
from test_kubernetes_proxy_lifecycle_integration import _MANAGED_ROLES, _RuntimeResources
from test_kubernetes_query_integration import _verify_owned
from test_kubernetes_service_create_integration import (
    _CommittedActivationStore, _application, _assert_removed, _observe_creation,
    _ready, _running_application,
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
        reason="both Kubernetes opt-ins and the explicit existing local test context/namespace are required",
    ),
]

_IMAGE = "agent-sandbox-basic:dev"
_STORAGE_CLASS = "agent-sandbox-workspace"
_PREFIX = "io.agent-sandbox."
_RECORD_ROLES = {
    "agent": "container", "policy": "network-policy", "service": "agent-service",
    "secret": "proxy-access", "proxy": "proxy", "ingress": "agent-proxy-ingress",
    "isolation": "proxy-isolation", "proxy_service": "proxy-service", "workspace": "private-workspace",
}


class _WorkspaceResources(_RuntimeResources):
    """Extend original POST ownership capture to the nine-resource persistent layout."""

    def assert_complete(self):
        """Require a positive original UID receipt for exactly the nine expected roles."""
        assert len(self.entries) == 9
        assert {entry["expected"]["labels"][_ROLE] for entry in self.entries} == _MANAGED_ROLES | {"private-workspace"}
        assert all(entry["uid"] for entry in self.entries)

    def assert_compute_absent(self):
        """Require original non-storage names to be absent while allowing the one preserved claim."""
        for entry in self.entries:
            if entry["kind"] == "persistent_volume_claim":
                continue
            facade = self.api.networking if entry["kind"] == "network_policy" else self.api
            assert _read_optional(getattr(facade, "read_namespaced_" + entry["kind"]),
                                  entry["expected"]["name"], entry["expected"]["namespace"]) is None


class _WorkspaceRecordingFacade:
    """Record real creates and schedule cached-image Pods through the normal scheduler."""

    def __init__(self, delegate, registry, hostname, *, volume=None, networking=False):
        """Borrow a worker-owned SDK client without sharing transports between Runtime calls."""
        self.delegate, self.registry, self.hostname = delegate, registry, hostname
        self.volume = volume
        self.claim_registry = registry
        if not networking:
            self.networking = _WorkspaceRecordingFacade(delegate.networking, registry, hostname,
                                                         volume=volume, networking=True)

    def __getattr__(self, name):
        """Capture real POSTs and pre-delete PV identity while suppressing Secret diagnostics."""
        operation = getattr(self.delegate, name)
        if name == "delete_namespaced_persistent_volume_claim" and self.claim_registry is not None:
            def recorded_claim_delete(*args, **kwargs):
                """Observe an original bound PV before Runtime rollback can remove its claim evidence."""
                claim_name = kwargs.get("name", args[0] if args else None)
                namespace = kwargs.get("namespace", args[1] if len(args) > 1 else None)
                matches = [entry for entry in self.claim_registry.entries
                           if entry["kind"] == "persistent_volume_claim"
                           and (entry["expected"]["name"], entry["expected"]["namespace"]) == (claim_name, namespace)]
                assert len(matches) == 1, "PVC deletion has no unique independently recorded original POST"
                entry = matches[0]
                assert entry["uid"], "PVC deletion has no independently confirmed original UID"
                current = _read_optional(self.delegate.read_namespaced_persistent_volume_claim, claim_name, namespace)
                if current is not None:
                    _verify_owned(current, entry["expected"], entry["uid"])
                    if current.status.phase == "Bound":
                        _verify_volume(self.delegate, current, entry, self.volume)
                return operation(*args, **kwargs)

            return recorded_claim_delete
        if not name.startswith("create_namespaced_") and "secret" not in name:
            return operation

        def recorded(*args, **kwargs):
            """Capture positional or keyword POST manifests before preserving the real SDK operation."""
            entry = None
            if name.startswith("create_namespaced_") and self.registry is not None:
                kind = name.removeprefix("create_namespaced_")
                manifest = kwargs["body"] if "body" in kwargs else args[1]
                if kind == "secret":
                    manifest.__class__ = _RedactedSecret
                if kind == "pod":
                    assert manifest.spec.node_name is None, "nodeName would bypass WFFC scheduling"
                    manifest.spec.node_selector = {"kubernetes.io/hostname": self.hostname}
                    assert all(container.image_pull_policy == "Never" for container in manifest.spec.containers)
                entry = self.registry.prepare(kind, manifest)
            try:
                response = operation(*args, **kwargs)
            except client.ApiException as error:
                if "secret" in name:
                    raise client.ApiException(status=error.status, reason="Secret diagnostics suppressed") from None
                raise
            except Exception:
                if "secret" in name:
                    raise RuntimeError("Secret operation failed; diagnostics suppressed") from None
                raise
            if "secret" in name:
                if isinstance(response, client.V1Secret):
                    response.__class__ = _RedactedSecret
                for item in response.items or [] if isinstance(response, client.V1SecretList) else []:
                    item.__class__ = _RedactedSecret
            if entry is not None:
                _verify_owned(response, entry["expected"], None)
                entry["uid"] = response.metadata.uid
            return response

        return recorded


def _cached_workspace_node(api):
    """Find one Ready untainted node holding the basic/proxy images without loading or pulling anything."""
    nodes = api.list_node(limit=10, _request_timeout=(0.5, 1))
    assert not nodes.metadata._continue, "node inventory exceeded its test bound"
    for node in sorted(nodes.items, key=lambda item: item.metadata.name):
        names = {name.removeprefix("docker.io/library/") for image in node.status.images or [] for name in image.names or []}
        hostname = (node.metadata.labels or {}).get("kubernetes.io/hostname")
        ready = any(condition.type == "Ready" and condition.status == "True" for condition in node.status.conditions or [])
        if (ready and hostname and not node.spec.unschedulable and not node.spec.taints
                and {_IMAGE, _PROXY_IMAGE} <= names):
            return node.metadata.name, hostname
    pytest.fail("no Ready schedulable node already holds both required cached images")


def _claim(chain):
    """Read only the independently captured original claim and verify its full ownership and backing PV."""
    entry = chain["managed"].role("private-workspace")
    expected = entry["expected"]
    pvc = chain["api"].read_namespaced_persistent_volume_claim(expected["name"], expected["namespace"],
                                                            _request_timeout=(0.5, 1))
    _verify_owned(pvc, expected, entry["uid"])
    if pvc.status.phase == "Bound":
        _verify_volume(chain["api"], pvc, entry, chain["volume"])
    return pvc


def _assert_layout(chain):
    """Require v3 durable ownership records, the original PVC identity, and a PVC-only workspace mount."""
    managed = chain["managed"]
    managed.assert_complete()
    claim = managed.role("private-workspace")
    assert claim["expected"]["name"] == private_workspace_name(chain["spec"].sandbox_id)
    wanted = {key: managed.role(role)["uid"] for key, role in _RECORD_ROLES.items()}
    for entry in managed.entries:
        facade = chain["api"].networking if entry["kind"] == "network_policy" else chain["api"]
        resource = getattr(facade, "read_namespaced_" + entry["kind"])(
            entry["expected"]["name"], chain["namespace"], _request_timeout=(0.5, 1))
        _verify_owned(resource, entry["expected"], entry["uid"])
        annotations = resource.metadata.annotations
        assert annotations[_PREFIX + "kubernetes-proxy-version"] == "3"
        assert json.loads(annotations[_PREFIX + "proxy-resource-uids"]) == wanted
        assert annotations[_PREFIX + "private-workspace-version"] == "1"
        assert annotations[_PREFIX + "private-workspace-storage-class"] == _STORAGE_CLASS
        assert annotations[_PREFIX + "private-workspace-uid"] == claim["uid"]
        if entry["expected"]["labels"][_ROLE] == "container":
            volumes = [volume for volume in resource.spec.volumes if volume.name == "workspace"]
            assert len(volumes) == 1 and volumes[0].empty_dir is None
            assert volumes[0].persistent_volume_claim.claim_name == claim["expected"]["name"]
            assert resource.spec.node_name == chain["node"]
    assert _claim(chain).status.phase == "Bound"


def _consumer(chain, filename, contents):
    """Create a cached non-root scheduler-bound consumer that checks the original endpoint-written file."""
    spec = chain["spec"]
    manifest = client.V1Pod(
        api_version="v1", kind="Pod",
        metadata=client.V1ObjectMeta(
            name="workspace-reader-" + secrets.token_hex(12), namespace=chain["namespace"],
            labels=spec.labels_for("workspace") | {_ROLE: "private-workspace-test-consumer"},
            annotations={key: value for key, value in chain["managed"].role("private-workspace")["expected"]["annotations"].items()
                         if key in {_PREFIX + "created-at", _PREFIX + "expires-at"}},
        ),
        spec=client.V1PodSpec(
            node_selector={"kubernetes.io/hostname": chain["hostname"]}, restart_policy="Never",
            active_deadline_seconds=55, termination_grace_period_seconds=2,
            automount_service_account_token=False, enable_service_links=False,
            security_context=client.V1PodSecurityContext(run_as_non_root=True, run_as_user=1000,
                run_as_group=1000, fs_group=1000, seccomp_profile=client.V1SeccompProfile(type="RuntimeDefault")),
            containers=[client.V1Container(
                name="workspace-check", image=_IMAGE, image_pull_policy="Never",
                command=["/bin/sh", "-ec", f'test "$(id -u)" = 1000; test "$(id -g)" = 1000; test "$(cat /workspace/{filename})" = {contents}'],
                security_context=client.V1SecurityContext(allow_privilege_escalation=False,
                    read_only_root_filesystem=True, capabilities=client.V1Capabilities(drop=["ALL"])),
                resources=client.V1ResourceRequirements(requests={"cpu": "100m", "memory": "64Mi"},
                                                        limits={"cpu": "500m", "memory": "128Mi"}),
                volume_mounts=[client.V1VolumeMount(name="workspace", mount_path="/workspace")],
            )],
            volumes=[client.V1Volume(name="workspace", persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=chain["managed"].role("private-workspace")["expected"]["name"]))],
        ),
    )
    assert manifest.spec.node_name is None
    return chain["registry"].create("pod", manifest)


@pytest.fixture
def owned_persistent_workspace(monkeypatch):
    """Guard every cluster access and independently clean original consumers before original storage."""
    if (os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
            or os.getenv("RUN_KUBERNETES_RESOURCE_INTEGRATION") != "1"
            or os.getenv("KUBERNETES_TEST_CONTEXT") != "agent-sandbox-k8s"
            or os.getenv("KUBERNETES_TEST_NAMESPACE") != "agent-sandbox-test"):
        pytest.skip("both opt-ins and exact existing local context/namespace are required")
    path = Path(os.environ.get("KUBERNETES_TEST_KUBECONFIG", str(Path.home() / ".kube" / "config")))
    context, namespace = os.environ["KUBERNETES_TEST_CONTEXT"], os.environ["KUBERNETES_TEST_NAMESPACE"]
    volume = {"name": None, "uid": None}
    from control_plane.runtime.kubernetes.pod_create import PodCreation
    original_failure = PodCreation.failure

    def failure(operation, phase, state):
        """Retain safe classification and source locations while diagnosing an opt-in test failure."""
        error = original_failure(operation, phase, state)
        primary = sys.exc_info()[1]
        locations = []
        traceback = None if primary is None else primary.__traceback__
        while traceback is not None:
            locations.append(f"{Path(traceback.tb_frame.f_code.co_filename).name}:{traceback.tb_lineno}")
            traceback = traceback.tb_next
        error.add_note(f"phase={phase} state={state.value} type={type(primary).__name__} locations={locations}")
        return error
    monkeypatch.setattr(PodCreation, "failure", failure)
    api = _WorkspaceRecordingFacade(create_probe_client(kubeconfig_path=path, context=context), None, None, volume=volume)
    managed, consumers = _WorkspaceResources(api), _OwnedResources(api)
    api.claim_registry = managed
    chain = _RedactedMapping({"api": api, "path": path, "context": context, "namespace": namespace,
                              "managed": managed, "registry": consumers, "volume": volume,
                              "spec": SimpleNamespace(sandbox_id="unused", transaction_id="unused")})
    try:
        storage = client.StorageV1Api(api.api_client).read_storage_class(_STORAGE_CLASS, _request_timeout=(0.5, 1))
        assert storage.metadata.name == _STORAGE_CLASS and storage.provisioner == "hostpath.csi.k8s.io"
        assert storage.volume_binding_mode == "WaitForFirstConsumer" and storage.reclaim_policy == "Delete"
        node, hostname = _cached_workspace_node(api)
        chain.update(node=node, hostname=hostname)
        original_proxy_builder = proxy_resources.build_proxy_pod

        def scheduled_proxy(*args, **kwargs):
            """Apply the same scheduler constraint to pure expected and submitted proxy specifications."""
            manifest = original_proxy_builder(*args, **kwargs)
            assert manifest.spec.node_name is None, "nodeName would bypass WFFC scheduling"
            manifest.spec.node_selector = {"kubernetes.io/hostname": hostname}
            return manifest

        # Query rebuilds the expected proxy and compares its full specification.
        # Scope this trusted scheduling adjustment to both builds so production
        # comparison stays strict while the local test uses cached images.
        monkeypatch.setattr(proxy_resources, "build_proxy_pod", scheduled_proxy)

        def factory(**kwargs):
            """Give each Runtime worker its own real recorded SDK client on the cached-image node."""
            return _WorkspaceRecordingFacade(create_probe_client(**kwargs), managed, hostname, volume=volume)

        def make_runtime(**changes):
            """Construct fresh Runtime instances with only explicitly requested creation configuration."""
            return KubernetesSandboxRuntime(kubeconfig_path=path, context=context, namespace=namespace,
                                            client_factory=factory, **changes)

        chain["make_runtime"] = make_runtime
        yield chain
    finally:
        primary = sys.exc_info()[1]
        try:
            # Failure to establish consumer disappearance retains all managed
            # resources. The inherited registry then orders managed Pods first
            # and retains companions/PVC if either managed Pod cannot be removed.
            consumers.cleanup()
            claims = [entry for entry in managed.entries if entry["kind"] == "persistent_volume_claim"]
            if claims:
                current = _read_optional(api.read_namespaced_persistent_volume_claim,
                                         claims[0]["expected"]["name"], namespace)
                if current is not None:
                    _verify_owned(current, claims[0]["expected"], claims[0]["uid"])
                    if current.status.phase == "Bound":
                        _verify_volume(api, current, claims[0], chain["volume"])
            managed.cleanup()
            _wait_volume_absent(api, chain["volume"])
        except BaseException as error:
            if primary is not None:
                primary.add_note(str(error))
            else:
                raise
        finally:
            api.api_client.close()


async def _write_read_endpoint(entry, token, filename, contents):
    """Execute a command and write/read persistent data solely through the delivered authenticated endpoint."""
    headers = _RedactedMapping({"Authorization": _RedactedText("Bearer " + token)})
    async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=10) as data:
        command = await data.post(entry.url + "/v1/commands:run", headers=headers,
                                  json={"argv": ["sh", "-ec", "test $(id -u) = 1000; printf persistent-ready"]})
        assert command.status_code == 200
        assert command.json()["exit_code"] == 0 and command.json()["stdout"] == "persistent-ready"
        uploaded = await data.post(entry.url + "/v1/files", params={"path": filename}, headers=headers,
                                   content=contents.encode())
        assert uploaded.status_code == 201
        downloaded = await data.get(entry.url + "/v1/files", params={"path": filename}, headers=headers)
        assert downloaded.status_code == 200 and downloaded.content == contents.encode()


def test_service_default_delete_preserves_original_pvc_for_same_node_consumer(
    owned_persistent_workspace, tmp_path, monkeypatch,
):
    """Default Service deletion closes its session and leaves only the original readable workspace PVC."""
    chain = owned_persistent_workspace

    async def verify():
        """Use a real Service/SQLite/lifespan, then reclaim the sole remaining PVC with a fresh Runtime."""
        backend = chain["make_runtime"](proxy_image=_PROXY_IMAGE, workspace_storage_class=_STORAGE_CLASS)
        store = SQLiteLifecycleStore(tmp_path / "persistent-default.sqlite3")
        manager = KubernetesLocalEntryManager(backend)
        observed = _observe_creation(chain, backend, store, monkeypatch)
        app, admin = _application(backend, manager, store)
        tasks = []
        async with _running_application(app, tasks, manager):
            service = app.state.service
            result = await service.create()
            assert len(observed) == 1 and result.sandbox_id == chain["spec"].sandbox_id
            spec = chain["spec"]
            active = await store.get(spec.sandbox_id)
            assert active.management_state == "active" and active.version == 1
            assert (await store.list_sandboxes()).managed_count == 1
            await _ready(service, spec.sandbox_id)
            await asyncio.to_thread(_assert_layout, chain)
            restarted = chain["make_runtime"]()
            current = await restarted.get(spec.sandbox_id)
            assert current.status == "ready" and current.transaction_id == spec.transaction_id
            assert manager.session_count == 0
            connection = await manager.open(spec.sandbox_id)
            assert manager.session_count == 1
            assert connection.target.proxy_pod_uid == chain["managed"].role("proxy")["uid"]
            filename, contents = "persistent-" + secrets.token_hex(12), secrets.token_hex(24)
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                          base_url="http://isolated-control", trust_env=False) as control:
                entry, token = await _delivered_endpoint(control, f"/v1/sandboxes/{spec.sandbox_id}/endpoint",
                                                        admin, _RedactedText(spec.access_token))
                assert entry.url == connection.url
                await _write_read_endpoint(entry, token, filename, contents)
            await service.delete(spec.sandbox_id)
            assert manager.session_count == 0
            await _assert_port_closed(entry.url)
            await asyncio.to_thread(chain["managed"].assert_compute_absent)
            original = await asyncio.to_thread(_claim, chain)
            assert original.metadata.uid == chain["managed"].role("private-workspace")["uid"]
            assert original.metadata.annotations[_PREFIX + "kubernetes-proxy-version"] == "3"
            assert original.metadata.annotations[_PREFIX + "private-workspace-uid"] == original.metadata.uid
            assert json.loads(original.metadata.annotations[_PREFIX + "proxy-resource-uids"]) == {
                key: chain["managed"].role(role)["uid"] for key, role in _RECORD_ROLES.items()
            }
            assert await store.get(spec.sandbox_id) is None
            assert await store.get_credential(spec.sandbox_id, spec.transaction_id) is None
            assert (await store.list_sandboxes()).managed_count == 0
            with pytest.raises(SandboxNotFound):
                await service.get(spec.sandbox_id)
            reader = await asyncio.to_thread(_consumer, chain, filename, contents)
            await asyncio.to_thread(_wait_succeeded, chain["api"], reader, chain["node"])
            await asyncio.to_thread(chain["registry"].delete, reader)
            assert await asyncio.to_thread(_read_optional, chain["api"].read_namespaced_pod,
                                           reader["expected"]["name"], chain["namespace"]) is None
            await asyncio.to_thread(_claim, chain)
            await restarted.delete(spec.sandbox_id, delete_workspace=True, expected_transaction_id=spec.transaction_id)
            await asyncio.to_thread(chain["managed"].assert_absent)
            await asyncio.to_thread(_wait_volume_absent, chain["api"], chain["volume"])
        assert manager.is_closed

    asyncio.run(verify())


def test_service_explicit_delete_removes_original_pvc_and_all_companions(
    owned_persistent_workspace, tmp_path, monkeypatch,
):
    """Explicit Service workspace deletion confirms every original resource and provisioned PV disappears."""
    chain = owned_persistent_workspace

    async def verify():
        """Create all nine resources through the actual Service and delete them under its real metadata lock."""
        backend = chain["make_runtime"](proxy_image=_PROXY_IMAGE, workspace_storage_class=_STORAGE_CLASS)
        store = SQLiteLifecycleStore(tmp_path / "persistent-delete.sqlite3")
        manager = KubernetesLocalEntryManager(backend)
        _observe_creation(chain, backend, store, monkeypatch)
        app, _ = _application(backend, manager, store)
        async with _running_application(app, [], manager):
            service = app.state.service
            await service.create()
            spec = chain["spec"]
            assert (await store.get(spec.sandbox_id)).management_state == "active"
            await _ready(service, spec.sandbox_id)
            await asyncio.to_thread(_assert_layout, chain)
            await service.delete(spec.sandbox_id, delete_workspace=True)
            await _assert_removed(chain, service, store, spec)
            await asyncio.to_thread(_wait_volume_absent, chain["api"], chain["volume"])

    asyncio.run(verify())


def test_service_missing_cached_proxy_image_rolls_back_nine_resources(
    owned_persistent_workspace, tmp_path, monkeypatch,
):
    """A unique Never-pull cache miss rolls back real Service metadata and every original managed resource."""
    chain = owned_persistent_workspace

    async def verify():
        """Drive the real startup failure without permitting image pulls or manually activating records."""
        backend = chain["make_runtime"](proxy_image="agent-sandbox-proxy-missing-" + secrets.token_hex(12) + ":never",
                                         workspace_storage_class=_STORAGE_CLASS)
        store = SQLiteLifecycleStore(tmp_path / "persistent-failure.sqlite3")
        manager = KubernetesLocalEntryManager(backend)
        _observe_creation(chain, backend, store, monkeypatch)
        app, _ = _application(backend, manager, store)
        async with _running_application(app, [], manager):
            service = app.state.service
            with pytest.raises(SandboxCreateFailed) as captured:
                await service.create()
            assert captured.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
            chain["managed"].assert_complete()
            await _assert_removed(chain, service, store, chain["spec"])
            await asyncio.to_thread(_wait_volume_absent, chain["api"], chain["volume"])
            assert manager.session_count == 0

    asyncio.run(verify())


def test_service_cancel_after_activation_compensates_original_pvc(
    owned_persistent_workspace, tmp_path, monkeypatch,
):
    """Repeated cancellation after real SQLite activation compensates the PVC as well as all eight companions."""
    chain = owned_persistent_workspace

    async def verify():
        """Hold the committed activation result until cancellation requires Service's delete_workspace=True compensation."""
        backend = chain["make_runtime"](proxy_image=_PROXY_IMAGE, workspace_storage_class=_STORAGE_CLASS)
        store = _CommittedActivationStore(tmp_path / "persistent-cancel.sqlite3")
        manager = KubernetesLocalEntryManager(backend)
        _observe_creation(chain, backend, store, monkeypatch)
        app, _ = _application(backend, manager, store)
        tasks = []
        async with _running_application(app, tasks, manager, release=store.activation_release):
            service = app.state.service
            creation = asyncio.create_task(service.create())
            entered = asyncio.create_task(store.activation_entered.wait())
            tasks.extend((creation, entered))
            done, _ = await asyncio.wait({creation, entered}, return_when=asyncio.FIRST_COMPLETED)
            if creation in done:
                await creation
                pytest.fail("creation completed before the committed activation gate was released")
            spec = chain["spec"]
            assert (await store.get(spec.sandbox_id)).management_state == "active"
            await asyncio.to_thread(_assert_layout, chain)
            creation.cancel()
            await asyncio.sleep(0)
            creation.cancel()
            await asyncio.sleep(0)
            assert not creation.done()
            store.activation_release.set()
            with pytest.raises(asyncio.CancelledError):
                await creation
            await _assert_removed(chain, service, store, spec)
            await asyncio.to_thread(_wait_volume_absent, chain["api"], chain["volume"])
            assert manager.session_count == 0

    asyncio.run(verify())
