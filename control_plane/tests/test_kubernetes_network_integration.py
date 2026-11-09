"""Double-opt-in cross-node Pod-IP isolation, positive controls and independently owned cleanup."""

import asyncio
import ipaddress
import secrets
import shlex
import subprocess
import sys
import time
from dataclasses import replace

import pytest
from kubernetes import client

from control_plane.runtime.kubernetes import kubernetes_runtime as runtime_module
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from control_plane.runtime.kubernetes.network_policy import network_policy_name
from control_plane.runtime.kubernetes.pod_query import (
    NETWORK_LAYOUT_LABEL, NETWORK_POLICY_UID_ANNOTATION,
    AGENT_SERVICE_UID_ANNOTATION, AGENT_SERVICE_VERSION_ANNOTATION,
)
from control_plane.runtime.kubernetes.agent_service import agent_service_name
from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
from test_kubernetes_create_integration import (
    owned_runtime_creation,
    test_missing_cached_image_fails_startup_and_confirms_precise_rollback as _missing_image_case,
    pytestmark,
)
from test_kubernetes_query_integration import _delete_owned, _verify_owned

pytestmark = [*pytestmark, pytest.mark.timeout(180)]


def _ready_pod(api, expected, uid, *, timeout=45):
    """Wait only for this exact owned Pod to report Ready and an IPv4 Pod address."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        pod = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        _verify_owned(pod, expected, uid)
        if any(item.type == "Ready" and item.status == "True" for item in pod.status.conditions or []):
            assert ipaddress.ip_address(pod.status.pod_ip).version == 4
            return pod
        assert pod.status.phase not in {"Failed", "Succeeded"}, "test Pod terminated before readiness"
        time.sleep(0.1)
    pytest.fail("test Pod readiness did not converge")


def _http_probe(api, source, target_ip, path, context):
    """Execute wget inside a verified Pod; the tested connection uses numeric Pod IP through its CNI network."""
    expected, uid = source["expected"], source["uid"]
    current = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
    _verify_owned(current, expected, uid)
    address = ipaddress.ip_address(target_ip)
    assert address.version == 4
    url = f"http://{address}:8080/healthz"
    script = f"if wget -q -T 1 -O /dev/null {shlex.quote(url)}; then printf reachable; else printf blocked; fi"
    completed = subprocess.run(
        ["kubectl", "--kubeconfig", str(path), "--context", context, "--namespace", expected["namespace"],
         "--request-timeout=5s", "exec", expected["name"], "-c", "agent", "--", "/bin/sh", "-c", script],
        capture_output=True, text=True, timeout=8, check=False,
    )
    # A failed exec/control connection is an environment failure, never evidence
    # that the actual Pod-to-Pod HTTP connection was blocked by a policy.
    assert completed.returncode == 0 and completed.stdout in {"reachable", "blocked"}, "Pod network probe did not execute"
    return completed.stdout == "reachable"


def _eventually_blocked(probe):
    """Allow bounded CNI convergence without promising an instantaneous policy enforcement window."""
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if not probe():
            return
        time.sleep(0.1)
    pytest.fail("expected Pod-IP isolation was not observed")


def test_cross_node_default_deny_with_live_positive_controls(owned_runtime_creation, monkeypatch):
    """Prove ingress, egress and sandbox-to-sandbox denial using healthy cross-node HTTP controls."""
    make_runtime, spec_a, api, expected_a, receipt_a, path, context = owned_runtime_creation
    runtime_a = make_runtime()
    resources = []

    def register(spec, *, node=None, image="agent-sandbox-basic:dev", control=False):
        """Record exact expected ownership before any request, including response-loss cleanup cases."""
        pod = build_pod_spec(spec, namespace=expected_a["namespace"], agent_image=image)
        if control:
            pod.metadata.labels.pop(NETWORK_LAYOUT_LABEL)
            pod.metadata.annotations.pop(AGENT_SERVICE_VERSION_ANNOTATION)
            pod.metadata.labels["io.agent-sandbox.resource"] = "network-test-control"
        pod.spec.node_name = node
        entry = {"expected": {"name": pod.metadata.name, "namespace": pod.metadata.namespace,
                              "labels": dict(pod.metadata.labels), "annotations": dict(pod.metadata.annotations)},
                 "uid": None, "policy_uid": None, "service_uid": None}
        resources.append(entry)
        return pod, entry

    def new_spec():
        """Give each test-owned control or second sandbox its own unpredictable transaction identity."""
        return replace(spec_a, sandbox_id="sbx_" + secrets.token_hex(16), transaction_id="txn_" + secrets.token_hex(16))

    async def verify():
        """Use production Runtime operations while pinning only test scheduling for cross-node coverage."""
        await runtime_a.create(spec_a)
        pod_a = _ready_pod(api, expected_a, receipt_a["uid"])
        assert (await runtime_a.get(spec_a.sandbox_id)).status == "ready"
        node_a = pod_a.spec.node_name
        nodes = api.list_node(limit=4, _request_timeout=(0.5, 0.5))
        assert not nodes.metadata._continue
        node_b = next(node.metadata.name for node in nodes.items if node.metadata.name != node_a
                      and not node.spec.unschedulable and not node.spec.taints)

        controls = []
        for node in (node_a, node_b):
            manifest, entry = register(new_spec(), node=node, control=True)
            created = api.create_namespaced_pod(expected_a["namespace"], body=manifest, _request_timeout=(0.5, 2))
            _verify_owned(created, entry["expected"], None)
            entry["uid"] = created.metadata.uid
            ready = _ready_pod(api, entry["expected"], entry["uid"])
            entry["ip"] = ready.status.pod_ip
            controls.append(entry)

        control_a, control_b = controls
        assert _http_probe(api, control_a, control_b["ip"], path, context), "cross-node control target is unreachable"
        assert _http_probe(api, control_b, control_a["ip"], path, context), "cross-node reverse control is unreachable"
        sandbox_a = {"expected": expected_a, "uid": receipt_a["uid"]}
        _eventually_blocked(lambda: _http_probe(api, sandbox_a, control_b["ip"], path, context))
        assert _http_probe(api, control_a, control_b["ip"], path, context), "egress control lost reachability"
        _eventually_blocked(lambda: _http_probe(api, control_b, pod_a.status.pod_ip, path, context))
        assert _http_probe(api, control_b, control_a["ip"], path, context), "ingress control lost reachability"

        spec_b = new_spec()
        _, sandbox_b = register(spec_b)
        runtime_b = KubernetesSandboxRuntime(kubeconfig_path=path, context=context, namespace=expected_a["namespace"])
        original_builder = runtime_module.build_pod_spec

        def scheduled_builder(spec, **kwargs):
            """Constrain only this integration Pod's placement, leaving all network/security fields unchanged."""
            pod = original_builder(spec, **kwargs)
            pod.spec.node_name = node_b
            return pod

        with monkeypatch.context() as patch:
            patch.setattr(runtime_module, "build_pod_spec", scheduled_builder)
            await runtime_b.create(spec_b)
        pod_b = api.read_namespaced_pod(sandbox_b["expected"]["name"], expected_a["namespace"], _request_timeout=(0.5, 0.5))
        _verify_owned(pod_b, sandbox_b["expected"], None)
        sandbox_b["uid"] = pod_b.metadata.uid
        sandbox_b["policy_uid"] = pod_b.metadata.annotations[NETWORK_POLICY_UID_ANNOTATION]
        sandbox_b["service_uid"] = pod_b.metadata.annotations[AGENT_SERVICE_UID_ANNOTATION]
        pod_b = _ready_pod(api, sandbox_b["expected"], sandbox_b["uid"])
        assert pod_a.spec.node_name != pod_b.spec.node_name
        _eventually_blocked(lambda: _http_probe(api, sandbox_a, pod_b.status.pod_ip, path, context))
        _eventually_blocked(lambda: _http_probe(api, sandbox_b, pod_a.status.pod_ip, path, context))
        assert (await runtime_a.get(spec_a.sandbox_id)).status == (await runtime_b.get(spec_b.sandbox_id)).status == "ready"
        assert _http_probe(api, control_a, control_b["ip"], path, context)
        assert _http_probe(api, control_b, control_a["ip"], path, context)

        await runtime_b.delete(spec_b.sandbox_id, expected_transaction_id=spec_b.transaction_id)
        await runtime_a.delete(spec_a.sandbox_id, expected_transaction_id=spec_a.transaction_id)
        for spec in (spec_a, spec_b):
            with pytest.raises(client.ApiException) as policy_after:
                api.networking.read_namespaced_network_policy(network_policy_name(spec.sandbox_id), expected_a["namespace"],
                                                              _request_timeout=(0.5, 0.5))
            assert policy_after.value.status == 404
            with pytest.raises(client.ApiException) as service_after:
                api.read_namespaced_service(agent_service_name(spec.sandbox_id), expected_a["namespace"],
                                            _request_timeout=(0.5, 0.5))
            assert service_after.value.status == 404

    try:
        asyncio.run(verify())
    finally:
        primary = sys.exc_info()[1]
        failures = []
        for resource in reversed(resources):
            try:
                _delete_owned(api, resource["expected"], resource["uid"], policy_uid=resource["policy_uid"],
                              service_uid=resource["service_uid"])
            except Exception:
                failures.append(f"{resource['expected']['name']} uid={resource['uid']}")
        if failures:
            message = "test cleanup incomplete: " + ", ".join(failures)
            if primary is not None:
                primary.add_note(message)
            else:
                pytest.fail(message)


def test_failed_creation_leaves_neither_pod_nor_policy(owned_runtime_creation):
    """Run the never-pull missing-image regression with independent three-resource cleanup."""
    _missing_image_case(owned_runtime_creation)
