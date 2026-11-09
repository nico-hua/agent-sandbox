"""Opt-in Runtime proxy lifecycle checks with independent eight-resource UID cleanup."""

import asyncio
import base64
import copy
import json
import os
import secrets
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from kubernetes import client

from control_plane.core.errors import (
    CreationResourceState, KubernetesOperationUnsupported, SandboxCreateFailed, SandboxNotFound,
)
from control_plane.core.models import SandboxSpec
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime, create_probe_client
from test_kubernetes_agent_service_integration import _ready_endpoint_slice
from test_kubernetes_network_integration import _eventually_blocked, _ready_pod
from test_kubernetes_proxy_integration import (
    _OwnedResources, _PROXY_IMAGE, _PYTHON_IMAGE, _ROLE, _RedactedMapping,
    _RedactedSecret, _RedactedText, _cached_node, _client_allow, _control_pod,
    _eventually_status, _proxy_control_reachable, _request, pytestmark as proxy_marks,
)
from test_kubernetes_query_integration import _verify_owned

pytestmark = [mark for mark in proxy_marks if mark.name != "timeout"] + [pytest.mark.timeout(300)]
_MANAGED_ROLES = {
    "container", "network-policy", "agent-service", "proxy-access", "proxy",
    "agent-proxy-ingress", "proxy-isolation", "proxy-service",
}


class _RuntimeResources(_OwnedResources):
    """Record Runtime POSTs before submission; cleanup never uses production inventory authority."""

    def prepare(self, kind, manifest):
        """Capture complete original metadata before POST, including the lost-response fallback case."""
        expected = {"name": manifest.metadata.name, "namespace": manifest.metadata.namespace,
                    "labels": dict(manifest.metadata.labels),
                    "annotations": dict(manifest.metadata.annotations or {})}
        facade = self.api.networking if kind == "network_policy" else self.api
        try:
            getattr(facade, "read_namespaced_" + kind)(
                expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        except client.ApiException as error:
            assert error.status == 404, "unique identity absence was not confirmed"
        else:
            pytest.fail("unique identity already exists; refusing to adopt it")
        entry = {"kind": kind, "expected": expected, "uid": None}
        self.entries.append(entry)
        return entry

    def role(self, role):
        """Resolve exactly one recorded resource by its complete captured ownership role."""
        matches = [entry for entry in self.entries if entry["expected"]["labels"][_ROLE] == role]
        assert len(matches) == 1, "Runtime did not submit exactly one required role"
        return matches[0]

    def assert_complete(self):
        """Require eight positive real POST receipts, one per expected managed resource role."""
        assert len(self.entries) == 8
        assert {entry["expected"]["labels"][_ROLE] for entry in self.entries} == _MANAGED_ROLES
        assert all(entry["uid"] for entry in self.entries)

    def assert_absent(self):
        """Independently check every captured exact name after Runtime cleanup claims success."""
        for entry in self.entries:
            facade = self.api.networking if entry["kind"] == "network_policy" else self.api
            try:
                getattr(facade, "read_namespaced_" + entry["kind"])(
                    entry["expected"]["name"], entry["expected"]["namespace"], _request_timeout=(0.5, 0.5))
            except client.ApiException as error:
                assert error.status == 404, "managed resource absence was not established"
            else:
                pytest.fail("Runtime cleanup left a managed resource behind")


class _RecordingFacade:
    """Borrow one worker SDK facade and record every managed identity before its official POST."""

    def __init__(self, delegate, registry, node, *, networking=False):
        """Retain the registry and cached node without opening another transport."""
        self.delegate, self.registry, self.node = delegate, registry, node
        if not networking:
            self.networking = _RecordingFacade(delegate.networking, registry, node, networking=True)

    def __getattr__(self, name):
        """Forward ordinary operations and intercept creates plus Secret diagnostics."""
        operation = getattr(self.delegate, name)
        if not name.startswith("create_namespaced_") and "secret" not in name:
            return operation

        def recorded(*args, **kwargs):
            """Preserve wire behavior while recording POST metadata and suppressing Secret representations."""
            entry = None
            if name.startswith("create_namespaced_") and self.registry is not None:
                kind, manifest = name.removeprefix("create_namespaced_"), kwargs["body"]
                if kind == "secret":
                    manifest.__class__ = _RedactedSecret
                if kind == "pod":
                    manifest.spec.node_name = self.node
                    assert manifest.spec.containers[0].image_pull_policy == "Never"
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
                items = response.items if isinstance(response, client.V1SecretList) else []
                for item in items or []:
                    if isinstance(item, client.V1Secret):
                        item.__class__ = _RedactedSecret
            if entry is not None:
                _verify_owned(response, entry["expected"], None)
                entry["uid"] = response.metadata.uid
            return response

        return recorded


@pytest.fixture
def owned_proxy_lifecycle():
    """Prepare explicitly gated targets and independent registries without provisioning managed resources."""
    if (os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
            or os.getenv("RUN_KUBERNETES_RESOURCE_INTEGRATION") != "1"
            or os.getenv("KUBERNETES_TEST_CONTEXT") != "agent-sandbox-k8s"
            or os.getenv("KUBERNETES_TEST_NAMESPACE") != "agent-sandbox-test"):
        pytest.skip("both opt-ins and exact existing local context/namespace are required")
    path = Path(os.environ.get("KUBERNETES_TEST_KUBECONFIG", str(Path.home() / ".kube" / "config")))
    context, namespace = os.environ["KUBERNETES_TEST_CONTEXT"], os.environ["KUBERNETES_TEST_NAMESPACE"]
    api = _RecordingFacade(create_probe_client(kubeconfig_path=path, context=context), None, None)
    managed, extras = _RuntimeResources(api), _OwnedResources(api)
    try:
        node = _cached_node(api)
        created = datetime(2026, 11, 9, 8, 0, 0, 123456, tzinfo=UTC)
        spec = SandboxSpec("sbx_" + secrets.token_hex(16), "unused", "unused", "unused",
            transaction_id="txn_" + secrets.token_hex(16), created_at=created,
            expires_at=created + timedelta(minutes=30), access_token=secrets.token_urlsafe(32))

        def factory(**kwargs):
            """Wrap the fresh client owned by each Runtime worker without sharing its transport."""
            return _RecordingFacade(create_probe_client(**kwargs), managed, node)

        def make_runtime(**changes):
            """Fix the explicit target and allow proxy_image only where creation enables the new layout."""
            return KubernetesSandboxRuntime(kubeconfig_path=path, context=context, namespace=namespace,
                                            client_factory=factory, **changes)

        yield _RedactedMapping({"api": api, "path": path, "context": context, "namespace": namespace,
            "node": node, "spec": spec, "managed": managed, "registry": extras, "make_runtime": make_runtime})
    finally:
        primary = sys.exc_info()[1]
        try:
            # Probe Pods may reference the managed Secret. The inherited cleanup
            # waits for each original Pod before removing any restrictions.
            extras.cleanup()
            managed.cleanup()
        except BaseException as error:
            if primary is not None:
                primary.add_note(str(error))
            else:
                raise
        finally:
            api.api_client.close()


@pytest.fixture
def runtime_proxy_chain(owned_proxy_lifecycle):
    """Create eight objects solely through Runtime, then add two controls and unique client ingress."""
    chain = owned_proxy_lifecycle
    runtime = chain["make_runtime"](proxy_image=_PROXY_IMAGE)
    assert asyncio.run(runtime.create(chain["spec"])) is None
    managed, api, extras = chain["managed"], chain["api"], chain["registry"]
    managed.assert_complete()
    agent_entry, proxy_entry = managed.role("container"), managed.role("proxy")
    agent = _ready_pod(api, agent_entry["expected"], agent_entry["uid"])
    proxy = _ready_pod(api, proxy_entry["expected"], proxy_entry["uid"])
    service_entry = managed.role("agent-service")
    service = api.read_namespaced_service(service_entry["expected"]["name"], chain["namespace"],
                                          _request_timeout=(0.5, 0.5))
    _verify_owned(service, service_entry["expected"], service_entry["uid"])
    _ready_endpoint_slice(api, service, agent)
    source = extras.create("pod", _control_pod(chain["spec"], chain["namespace"], chain["node"],
                                               image=_PYTHON_IMAGE, role="proxy-test-client"))
    _ready_pod(api, source["expected"], source["uid"])
    control = extras.create("pod", _control_pod(chain["spec"], chain["namespace"], chain["node"],
                                                image="agent-sandbox-basic:dev", role="proxy-test-positive"))
    control_ip = _ready_pod(api, control["expected"], control["uid"]).status.pod_ip
    chain.update({"runtime": runtime, "agent": agent, "proxy": proxy, "proxy_entry": proxy_entry,
        "service": service, "client": source, "control_ip": control_ip,
        "token": _RedactedText(chain["spec"].access_token)})
    assert _request(chain, control_ip, "/healthz")["status"] == 200
    _eventually_blocked(lambda: _request(chain, proxy.status.pod_ip, "/healthz")["status"] != 0)
    allowed_client = _client_allow(agent, proxy, source)
    # The client rule belongs to the independent probe transaction. Its Pod
    # selector still targets the exact proxy, without expanding Runtime inventory.
    allowed_client.metadata.labels = {**source["expected"]["labels"], _ROLE: "proxy-test-client-ingress"}
    extras.create("network_policy", allowed_client)
    _eventually_status(lambda: _request(chain, proxy.status.pod_ip, "/healthz"), {200})
    return chain


def _assert_real_protocol(chain, address=None):
    """Exercise denied credentials, commands, binary files and completed SSE through the actual proxy."""
    address = address or chain["proxy"].status.pod_ip
    denied_command = json.dumps({"argv": ["sh", "-c", "touch /workspace/runtime-proxy-denied"]}).encode()
    for credential in (None, _RedactedText(secrets.token_urlsafe(32))):
        denied = _request(chain, address, "/v1/commands:run", method="POST", credential=credential,
                          content=denied_command)
        assert denied["status"] == 401 and denied["challenge"] == "Bearer"
    command = json.dumps({"argv": ["sh", "-c",
        'test ! -e /workspace/runtime-proxy-denied && test -z "${SANDBOX_ACCESS_TOKEN:-}" && '
        'test -z "${AGENT_SANDBOX_API_KEY:-}" && printf runtime-proxy-ok']}).encode()
    result = _request(chain, address, "/v1/commands:run", method="POST", credential=chain["token"], content=command)
    assert result["status"] == 200
    assert json.loads(result["body"])["stdout"] == "runtime-proxy-ok"
    assert json.loads(result["body"])["exit_code"] == 0
    payload = b"\x00\xffruntime-proxy-file\x80\r\n"
    assert _request(chain, address, "/v1/files?path=runtime-proxy-file", method="POST",
                    credential=chain["token"], content=payload)["status"] == 201
    download = _request(chain, address, "/v1/files?path=runtime-proxy-file", credential=chain["token"])
    assert download["status"] == 200 and download["body"] == payload
    stream = _request(chain, address, "/v1/commands:stream", method="POST", credential=chain["token"],
                      content=json.dumps({"argv": ["printf", "runtime-proxy-stream"]}).encode())
    assert stream["status"] == 200 and stream["content_type"].startswith("text/event-stream")
    frames = [frame.splitlines() for frame in stream["body"].decode().split("\n\n") if frame]
    stdout = b"".join(base64.b64decode(json.loads(lines[1].removeprefix("data: "))["data_base64"])
                      for lines in frames if lines[0] == "event: stdout")
    completions = [json.loads(lines[1].removeprefix("data: ")) for lines in frames if lines[0] == "event: complete"]
    assert stdout == b"runtime-proxy-stream" and completions == [{"exit_code": 0}]


def _assert_network_boundaries(chain):
    """Prove direct-Agent, cross-identity upstream and unrelated proxy egress denial against real controls."""
    _eventually_blocked(lambda: _request(chain, chain["service"].spec.cluster_ip, "/healthz")["status"] != 0)
    _eventually_blocked(lambda: _request(chain, chain["agent"].status.pod_ip, "/healthz")["status"] != 0)
    assert _request(chain, chain["control_ip"], "/healthz")["status"] == 200
    _eventually_blocked(lambda: _proxy_control_reachable(chain))
    assert _request(chain, chain["control_ip"], "/healthz")["status"] == 200
    # Open egress on a different proxy identity makes its upstream failure
    # demonstrate the Agent's exact ingress selector rather than source egress.
    wrong = copy.deepcopy(chain["proxy"])
    wrong.metadata = client.V1ObjectMeta(
        name="as-proxy-other-" + secrets.token_hex(12), namespace=chain["namespace"],
        labels={**chain["proxy"].metadata.labels, "io.agent-sandbox.sandbox-id": "sbx_" + secrets.token_hex(16),
                "io.agent-sandbox.transaction-id": "txn_" + secrets.token_hex(16)},
        annotations={key: chain["proxy"].metadata.annotations[key] for key in (
            "io.agent-sandbox.created-at", "io.agent-sandbox.expires-at")})
    wrong.status = None
    wrong_entry = chain["registry"].create("pod", wrong)
    deadline = time.monotonic() + 45
    while True:
        other = chain["api"].read_namespaced_pod(wrong.metadata.name, chain["namespace"], _request_timeout=(0.5, 0.5))
        _verify_owned(other, wrong_entry["expected"], wrong_entry["uid"])
        if other.status.phase == "Running" and other.status.pod_ip:
            break
        assert time.monotonic() < deadline, "other-identity proxy did not start"
        time.sleep(0.1)
    _eventually_status(lambda: _request(chain, other.status.pod_ip, "/v1/commands:run", method="POST",
        credential=chain["token"], content=json.dumps({"argv": ["true"]}).encode()), {502, 504})
    assert _request(chain, chain["proxy"].status.pod_ip, "/healthz")["status"] == 200
    chain["registry"].delete(wrong_entry)
    _ready_endpoint_slice(chain["api"], chain["service"], chain["agent"])


def test_real_runtime_proxy_chain_and_unconfigured_restart_get_delete(runtime_proxy_chain):
    """Create a real full chain, then query and delete it through a fresh Runtime with no proxy image."""
    chain = runtime_proxy_chain
    _assert_real_protocol(chain)
    _assert_network_boundaries(chain)
    restarted = chain["make_runtime"]()

    async def verify():
        """Observe readiness and endpoint rejection, then prove eight-resource and idempotent cleanup."""
        deadline = time.monotonic() + 45
        while True:
            observed = await restarted.get(chain["spec"].sandbox_id)
            if observed.status == "ready":
                break
            assert observed.status in {"starting", "running"}
            assert time.monotonic() < deadline, "full proxy layout never became ready"
            await asyncio.sleep(0.1)
        assert (observed.transaction_id, observed.created_at, observed.expires_at) == (
            chain["spec"].transaction_id, chain["spec"].created_at, chain["spec"].expires_at)
        with pytest.raises(KubernetesOperationUnsupported):
            await restarted.get_endpoint(chain["spec"].sandbox_id)
        await restarted.delete(chain["spec"].sandbox_id, expected_transaction_id=chain["spec"].transaction_id)
        chain["managed"].assert_absent()
        with pytest.raises(SandboxNotFound):
            await restarted.get(chain["spec"].sandbox_id)
        await restarted.delete(chain["spec"].sandbox_id, expected_transaction_id=chain["spec"].transaction_id)

    asyncio.run(verify())


def test_missing_cached_proxy_image_rolls_back_all_eight_managed_resources(owned_proxy_lifecycle):
    """A unique missing Never-pull proxy image fails startup and leaves no managed resource behind."""
    chain = owned_proxy_lifecycle
    runtime = chain["make_runtime"](proxy_image="agent-sandbox-proxy-missing-" + secrets.token_hex(12) + ":never")

    async def verify():
        """Require positive eight-resource receipts plus confirmed rollback after a real cache miss."""
        with pytest.raises(SandboxCreateFailed) as captured:
            await runtime.create(chain["spec"])
        assert captured.value.creation_failure.resource_state is CreationResourceState.ROLLED_BACK
        chain["managed"].assert_complete()
        chain["managed"].assert_absent()

    asyncio.run(verify())


def test_unconfigured_runtime_removes_partial_layout_after_original_agent_disappears(runtime_proxy_chain):
    """Recover the seven surviving original companions after independently deleting only the Agent."""
    chain = runtime_proxy_chain
    chain["managed"].delete(chain["managed"].role("container"))
    restarted = chain["make_runtime"]()

    async def verify():
        """Treat the missing Agent as absent while still recovering all persisted companion ownership."""
        with pytest.raises(SandboxNotFound):
            await restarted.get(chain["spec"].sandbox_id)
        await restarted.delete(chain["spec"].sandbox_id, expected_transaction_id=chain["spec"].transaction_id)
        chain["managed"].assert_absent()
        with pytest.raises(SandboxNotFound):
            await restarted.get(chain["spec"].sandbox_id)

    asyncio.run(verify())
