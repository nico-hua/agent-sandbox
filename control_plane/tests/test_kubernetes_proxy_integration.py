"""Double-opt-in Pod-network proxy checks with independent UID-guarded fixture cleanup."""

import asyncio
import base64
import copy
import ipaddress
import json
import os
import secrets
import subprocess
import sys
import time
from dataclasses import replace

import pytest
from kubernetes import client

from control_plane.runtime.kubernetes import kubernetes_runtime as runtime_module
from control_plane.runtime.kubernetes.agent_service import agent_service_name
from control_plane.runtime.kubernetes.pod_query import AGENT_SERVICE_VERSION_ANNOTATION, NETWORK_LAYOUT_LABEL
from control_plane.runtime.kubernetes.pod_spec import build_pod_spec
from control_plane.runtime.kubernetes.proxy_spec import (
    build_proxy_network_policies, build_proxy_pod, build_proxy_secret,
)
from test_kubernetes_agent_service_integration import _ready_endpoint_slice
from test_kubernetes_create_integration import owned_runtime_creation, pytestmark as creation_marks
from test_kubernetes_network_integration import _eventually_blocked, _ready_pod
from test_kubernetes_query_integration import _verify_owned

pytestmark = [
    *creation_marks,
    pytest.mark.timeout(240),
    pytest.mark.skipif(
        os.getenv("KUBERNETES_TEST_CONTEXT") != "agent-sandbox-k8s"
        or os.getenv("KUBERNETES_TEST_NAMESPACE") != "agent-sandbox-test",
        reason="proxy integration requires the explicit existing local test context and namespace",
    ),
]

class _RedactedText(str):
    """Preserve exact protocol bytes while suppressing credentials in pytest argument representations."""

    def __repr__(self):
        """Hide token and stdin payload values when an assertion renders stack arguments."""
        return "<redacted fixture text>"


class _RedactedMapping(dict):
    """Keep fixture data usable while suppressing credential-bearing request and chain representations."""

    def __repr__(self):
        """Avoid dumping nested credentials when pytest renders fixture values."""
        return "<redacted fixture mapping>"


class _RedactedSecret(client.V1Secret):
    """Retain the SDK Secret wire fields while hiding encoded data in assertion diagnostics."""

    def __repr__(self):
        """Suppress the SDK's default full Secret representation."""
        return "<fixture Secret contents redacted>"

    def __str__(self):
        """Suppress Secret contents in diagnostic string conversion as well as repr."""
        return "<fixture Secret contents redacted>"


_PROXY_IMAGE = "agent-sandbox-proxy-upstream-test:dev"
_PYTHON_IMAGE = "agent-sandbox-python:dev"
_ROLE = "io.agent-sandbox.resource"
_AUTH_HEADERS = [
    "Authorization", "Proxy-Authorization", "X-API-Key", "X-Access-Token",
    "X-Sandbox-Token", "X-Sandbox-Access-Token", "X-Auth-Token",
]

_CLIENT_SOURCE = '''import base64, json, sys, urllib.error, urllib.request
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Observe redirects without following them outside the numeric fixture target."""
        return None
request = json.load(sys.stdin)
try:
    body = None if request['body'] is None else base64.b64decode(request['body'])
    headers = dict(request['headers'])
    req = urllib.request.Request(request['url'], data=body, headers=headers, method=request['method'])
    try:
        response = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect()).open(req, timeout=4)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        payload = response.read(1048577)
        if len(payload) > 1048576 or any(value.encode() in payload for value in request['secrets']):
            raise ValueError('unsafe response')
        result = {'status': response.status, 'body': base64.b64encode(payload).decode(),
                  'content_type': response.headers.get('Content-Type', ''),
                  'challenge': response.headers.get('WWW-Authenticate', '')}
except (urllib.error.URLError, TimeoutError):
    result = {'status': 0, 'body': '', 'content_type': '', 'challenge': ''}
except Exception:
    sys.exit(9)
print(json.dumps(result))
'''

_RECORDER_SOURCE = '''import http.client, http.server, json, threading
names = ['Authorization', 'Proxy-Authorization', 'X-API-Key', 'X-Access-Token', 'X-Sandbox-Token', 'X-Sandbox-Access-Token', 'X-Auth-Token']
class Handler(http.server.BaseHTTPRequestHandler):
    def relay(self):
        """Record only header-presence facts, then forward a bounded request to the real loopback Agent."""
        if self.headers.get('X-Ordinary') == 'kept':
            with open('/tmp/proxy-header-facts.jsonl', 'a', encoding='utf-8') as output:
                output.write(json.dumps({'path': self.path, 'credential_present': any(self.headers.get(name) is not None for name in names), 'ordinary_present': True}) + '\\n')
        size = int(self.headers.get('Content-Length', '0'))
        if not 0 <= size <= 1048576:
            self.send_error(413)
            return
        upstream = http.client.HTTPConnection('127.0.0.1', 8081, timeout=5)
        try:
            upstream.request(self.command, self.path, body=self.rfile.read(size), headers=dict(self.headers))
            response = upstream.getresponse()
            self.send_response(response.status)
            for key, value in response.getheaders():
                if key.lower() not in {'connection', 'transfer-encoding'}:
                    self.send_header(key, value)
            self.end_headers()
            copied = 0
            while True:
                chunk = response.read1(65536)
                if not chunk:
                    break
                copied += len(chunk)
                if copied > 1048576:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
        finally:
            upstream.close()
    def do_GET(self):
        """Relay test GET requests without logging their headers."""
        self.relay()
    def do_POST(self):
        """Relay test POST requests without logging their headers."""
        self.relay()
    def log_message(self, *args):
        """Suppress HTTP diagnostics so submitted credentials cannot enter logs."""
        pass
server = http.server.ThreadingHTTPServer(('0.0.0.0', 8080), Handler)
server.daemon_threads = True
stop = threading.Timer(120, server.shutdown)
stop.daemon = True
stop.start()
try:
    server.serve_forever(poll_interval=0.1)
finally:
    server.server_close()
'''


def _exec(api, source, path, context, argv, *, stdin=None, timeout=10, container="agent"):
    """Run one bounded trusted exec only after checking the exact original Pod ownership."""
    expected = source["expected"]
    current = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
    _verify_owned(current, expected, source["uid"])
    command = ["kubectl", "--kubeconfig", str(path), "--context", context,
               "--namespace", expected["namespace"], "--request-timeout=5s", "exec"]
    if stdin is not None:
        command.append("-i")
    command.extend([expected["name"], "-c", container, "--", *argv])
    completed = subprocess.run(command, input=stdin, capture_output=True, text=True, timeout=timeout, check=False)
    executed = completed.returncode == 0
    assert executed, "controlled Pod exec failed; captured diagnostics intentionally suppressed"
    return completed.stdout


def _request(chain, target_ip, route, *, method="GET", credential=None, content=None, headers=None):
    """Issue real numeric-IP HTTP from the client Pod, passing credentials only on exec stdin."""
    address = ipaddress.ip_address(target_ip)
    assert address.version == 4
    request_headers = dict(headers or {})
    if credential is not None:
        request_headers["Authorization"] = _RedactedText("Bearer " + credential)
    request = _RedactedMapping({"url": f"http://{address}:8080{route}", "method": method, "headers": request_headers,
               "body": None if content is None else base64.b64encode(content).decode(),
               "secrets": [chain["token"]]})
    result = _exec(chain["api"], chain["client"], chain["path"], chain["context"],
                   ["python3", "-c", _CLIENT_SOURCE], stdin=_RedactedText(json.dumps(request)))
    try:
        decoded = json.loads(result)
        decoded["body"] = base64.b64decode(decoded["body"])
        return decoded
    except Exception:
        pytest.fail("controlled HTTP client returned an invalid result")


def _proxy_control_reachable(chain):
    """Probe ordinary-target egress from the exact proxy, distinguishing denied traffic from failed exec."""
    address = ipaddress.ip_address(chain["control_ip"])
    assert address.version == 4
    script = (f"if wget -q -T 1 -O /dev/null http://{address}:8080/healthz; "
              "then printf connected; else printf blocked; fi")
    output = _exec(chain["api"], chain["proxy_entry"], chain["path"], chain["context"],
                   ["/bin/sh", "-c", script], container="proxy")
    assert output in {"connected", "blocked"}, "controlled proxy egress probe did not execute"
    return output == "connected"


def _redacted_secret(agent, access_token):
    """Build the exact production Secret while keeping failure tracebacks free of its credential value."""
    try:
        manifest = build_proxy_secret(agent, access_token=str(access_token))
        manifest.__class__ = _RedactedSecret
        return manifest
    except Exception:
        raise RuntimeError("trusted Secret specification failed; credential diagnostics suppressed") from None


def _eventually_status(probe, accepted, *, timeout=20):
    """Wait for an actual HTTP status while keeping control-channel failures distinct from network denial."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = probe()
        if response["status"] in accepted:
            return response
        time.sleep(0.1)
    pytest.fail("expected Pod-network HTTP status was not observed")


def _cached_node(api):
    """Require all fixed test images already cached on one existing node without pulling or loading anything."""
    nodes = api.list_node(limit=4, _request_timeout=(0.5, 0.5))
    assert not nodes.metadata._continue, "test node inventory exceeded its bound"
    required = {_PROXY_IMAGE, _PYTHON_IMAGE, "agent-sandbox-basic:dev"}
    for node in nodes.items:
        names = {name.removeprefix("docker.io/library/") for image in node.status.images or [] for name in image.names or []}
        if required <= names and not node.spec.unschedulable and not node.spec.taints:
            return node.metadata.name
    pytest.fail("all three required fixed test images must already be cached on one schedulable node")


class _OwnedResources:
    """Track test-only additions independently of production Runtime inventory and deletion."""

    def __init__(self, api):
        """Retain the fixture-owned SDK client and an initially empty exact resource registry."""
        self.api, self.entries = api, []

    def create(self, kind, manifest):
        """Register complete expected identity before POST so response loss still permits guarded cleanup."""
        facade = self.api.networking if kind == "network_policy" else self.api
        if kind == "secret":
            manifest.__class__ = _RedactedSecret
        expected = {"name": manifest.metadata.name, "namespace": manifest.metadata.namespace,
                    "labels": dict(manifest.metadata.labels), "annotations": dict(manifest.metadata.annotations or {})}
        read = getattr(facade, "read_namespaced_" + kind)
        with pytest.raises(client.ApiException) as absent:
            read(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        assert absent.value.status == 404, "fixture identity already exists"
        entry = {"kind": kind, "expected": expected, "uid": None}
        self.entries.append(entry)
        response = getattr(facade, "create_namespaced_" + kind)(expected["namespace"], body=manifest,
                                                                _request_timeout=(0.5, 2))
        if kind == "secret":
            response.__class__ = _RedactedSecret
        _verify_owned(response, expected, None)
        entry["uid"] = response.metadata.uid
        return entry

    def delete(self, entry):
        """Delete only a fully verified original UID/version and wait for that exact resource to disappear."""
        kind, expected = entry["kind"], entry["expected"]
        facade = self.api.networking if kind == "network_policy" else self.api
        read = getattr(facade, "read_namespaced_" + kind)
        try:
            current = read(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        except client.ApiException as error:
            if error.status == 404:
                return
            raise
        if kind == "secret":
            current.__class__ = _RedactedSecret
        _verify_owned(current, expected, entry["uid"])
        entry["uid"] = current.metadata.uid
        assert current.metadata.resource_version, "cleanup resourceVersion is missing"
        getattr(facade, "delete_namespaced_" + kind)(
            expected["name"], expected["namespace"],
            body=client.V1DeleteOptions(preconditions=client.V1Preconditions(
                uid=entry["uid"], resource_version=current.metadata.resource_version)), _request_timeout=(0.5, 2),
        )
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            try:
                remaining = read(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
            except client.ApiException as error:
                if error.status == 404:
                    return
                raise
            if kind == "secret":
                remaining.__class__ = _RedactedSecret
            _verify_owned(remaining, expected, entry["uid"])
            time.sleep(0.1)
        pytest.fail("fixture resource disappearance was not observed")

    def cleanup(self):
        """Remove Pods before Secrets and policies, retaining isolation until their workloads have disappeared."""
        failures, retained = [], []
        entries = sorted(reversed(self.entries), key=lambda entry: entry["kind"] != "pod")
        for entry in entries:
            # Retain Secret material and all isolation rules if a Pod's exact
            # disappearance could not be established independently.
            if failures and entry["kind"] != "pod":
                retained.append(f"{entry['expected']['name']} uid={entry['uid']}")
                continue
            try:
                self.delete(entry)
            except Exception:
                failures.append(f"{entry['expected']['name']} uid={entry['uid']}")
        if failures:
            pytest.fail("independent proxy fixture cleanup incomplete: " + ", ".join(failures)
                        + "; companion resources retained for inspection: " + ", ".join(retained))


def _control_pod(spec, namespace, node, *, image, role):
    """Build an ordinary fixture-only Agent control with a unique complete transaction identity."""
    control = replace(spec, sandbox_id="sbx_" + secrets.token_hex(16), transaction_id="txn_" + secrets.token_hex(16))
    manifest = build_pod_spec(control, namespace=namespace, agent_image=image)
    manifest.metadata.labels.pop(NETWORK_LAYOUT_LABEL)
    manifest.metadata.labels[_ROLE] = role
    manifest.metadata.annotations.pop(AGENT_SERVICE_VERSION_ANNOTATION)
    manifest.spec.node_name = node
    return manifest


def _client_allow(agent, proxy, source):
    """Allow only this fixture's exact client labels to reach this exact proxy on TCP 8080."""
    return client.V1NetworkPolicy(
        api_version="networking.k8s.io/v1", kind="NetworkPolicy",
        metadata=client.V1ObjectMeta(name=proxy.metadata.name + "-test-client", namespace=proxy.metadata.namespace,
            labels={**proxy.metadata.labels, _ROLE: "proxy-test-client-ingress"},
            annotations={key: agent.metadata.annotations[key] for key in (
                "io.agent-sandbox.created-at", "io.agent-sandbox.expires-at")}),
        spec=client.V1NetworkPolicySpec(
            pod_selector=client.V1LabelSelector(match_labels=dict(proxy.metadata.labels)), policy_types=["Ingress"],
            ingress=[client.V1NetworkPolicyIngressRule(
                _from=[client.V1NetworkPolicyPeer(pod_selector=client.V1LabelSelector(
                    match_labels=dict(source["expected"]["labels"])))],
                ports=[client.V1NetworkPolicyPort(port=8080, protocol="TCP")])]),
    )


@pytest.fixture
def proxy_chain(owned_runtime_creation, monkeypatch, request):
    """Create only unique fixture resources after both opt-ins and exact local target guards have passed."""
    make_runtime, spec, api, expected, receipt, path, context = owned_runtime_creation
    node = _cached_node(api)
    instrumented = getattr(request, "param", False)
    runtime = make_runtime(agent_image=_PYTHON_IMAGE if instrumented else "agent-sandbox-basic:dev")
    registry = _OwnedResources(api)
    original_builder = runtime_module.build_pod_spec

    def scheduled_builder(specification, **kwargs):
        """Pin cached-image scheduling and alter only the instrumented case's loopback Agent listener."""
        pod = original_builder(specification, **kwargs)
        pod.spec.node_name = node
        if instrumented:
            pod.spec.containers[0].args = ["-listen", "127.0.0.1:8081"]
        return pod

    try:
        with monkeypatch.context() as patch:
            patch.setattr(runtime_module, "build_pod_spec", scheduled_builder)
            asyncio.run(runtime.create(spec))
        agent_entry = {"expected": expected, "uid": receipt["uid"]}
        if instrumented:
            bootstrap = ("import subprocess,sys; subprocess.Popen(['python3','-c',sys.stdin.read()], "
                         "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)")
            _exec(api, agent_entry, path, context, ["python3", "-c", bootstrap], stdin=_RECORDER_SOURCE)
        agent = _ready_pod(api, expected, receipt["uid"])
        service = api.read_namespaced_service(agent_service_name(spec.sandbox_id), expected["namespace"],
                                              _request_timeout=(0.5, 0.5))
        assert service.metadata.uid == receipt["service_uid"]
        _ready_endpoint_slice(api, service, agent)
        source = registry.create("pod", _control_pod(spec, expected["namespace"], node,
            image=_PYTHON_IMAGE, role="proxy-test-client"))
        source_pod = _ready_pod(api, source["expected"], source["uid"])
        assert source_pod.spec.automount_service_account_token is False
        control = registry.create("pod", _control_pod(spec, expected["namespace"], node,
            image="agent-sandbox-basic:dev", role="proxy-test-positive"))
        control_ip = _ready_pod(api, control["expected"], control["uid"]).status.pod_ip
        token = _RedactedText(secrets.token_urlsafe(32))
        registry.create("secret", _redacted_secret(agent, token))
        agent_policy, isolation = build_proxy_network_policies(agent)
        registry.create("network_policy", isolation)
        agent_allow = registry.create("network_policy", agent_policy)
        proxy_manifest = build_proxy_pod(agent, service, proxy_image=_PROXY_IMAGE)
        proxy_manifest.spec.node_name = node
        proxy_entry = registry.create("pod", proxy_manifest)
        proxy = _ready_pod(api, proxy_entry["expected"], proxy_entry["uid"])
        chain = _RedactedMapping({"api": api, "path": path, "context": context, "client": source, "token": token,
                 "agent": agent, "service": service, "proxy": proxy, "proxy_entry": proxy_entry, "proxy_manifest": proxy_manifest,
                 "registry": registry, "agent_allow": agent_allow, "control_ip": control_ip,
                 "runtime": runtime, "spec": spec})
        assert _request(chain, control_ip, "/healthz")["status"] == 200, "ordinary positive control is unreachable"
        _eventually_blocked(lambda: _request(chain, proxy.status.pod_ip, "/healthz")["status"] != 0)
        registry.create("network_policy", _client_allow(agent, proxy, source))
        _eventually_status(lambda: _request(chain, proxy.status.pod_ip, "/healthz"), {200})
        yield chain
    finally:
        primary = sys.exc_info()[1]
        try:
            registry.cleanup()
        except BaseException as error:
            if primary is not None:
                primary.add_note(str(error))
            else:
                raise error
        # The original creation fixture independently removes its three resources.
        # Runtime inventory intentionally rejects additional proxy roles, so it is
        # never used as cleanup authority while fixture proxy resources remain.


def test_proxy_real_agent_auth_files_stream_and_network_boundaries(proxy_chain):
    """Exercise an untouched basic Agent through the real proxy/ClusterIP path and scoped network policies."""
    chain = proxy_chain
    proxy_ip = chain["proxy"].status.pod_ip
    route = "/v1/commands:run"
    forbidden = json.dumps({"argv": ["sh", "-c", "touch /workspace/proxy-denied"]}).encode()
    for credential in (None, _RedactedText(secrets.token_urlsafe(32))):
        denied = _request(chain, proxy_ip, route, method="POST", credential=credential, content=forbidden)
        assert denied["status"] == 401 and denied["challenge"] == "Bearer"
    command = json.dumps({"argv": ["sh", "-c",
        'test ! -e /workspace/proxy-denied && test -z "${SANDBOX_ACCESS_TOKEN:-}" && '
        'test -z "${AGENT_SANDBOX_API_KEY:-}" && printf proxy-command-ok']}).encode()
    response = _request(chain, proxy_ip, route, method="POST", credential=chain["token"], content=command)
    assert response["status"] == 200
    result = json.loads(response["body"])
    assert result["exit_code"] == 0 and result["stdout"] == "proxy-command-ok"
    payload = b"\x00\xffproxy-binary\x80\r\n"
    assert _request(chain, proxy_ip, "/v1/files?path=proxy-binary", method="POST",
                    credential=chain["token"], content=payload)["status"] == 201
    downloaded = _request(chain, proxy_ip, "/v1/files?path=proxy-binary", credential=chain["token"])
    assert downloaded["status"] == 200 and downloaded["body"] == payload
    stream = _request(chain, proxy_ip, "/v1/commands:stream", method="POST", credential=chain["token"],
                      content=json.dumps({"argv": ["printf", "proxy-stream-ok"]}).encode())
    assert stream["status"] == 200 and stream["content_type"].startswith("text/event-stream")
    frames = [frame.splitlines() for frame in stream["body"].decode().split("\n\n") if frame]
    stdout = b"".join(base64.b64decode(json.loads(lines[1].removeprefix("data: "))["data_base64"])
                      for lines in frames if lines[0] == "event: stdout")
    completions = [json.loads(lines[1].removeprefix("data: ")) for lines in frames
                   if lines[0] == "event: complete"]
    assert stdout == b"proxy-stream-ok" and completions == [{"exit_code": 0}]
    _eventually_blocked(lambda: _request(chain, chain["service"].spec.cluster_ip, "/healthz")["status"] != 0)
    assert _request(chain, chain["control_ip"], "/healthz")["status"] == 200
    _eventually_blocked(lambda: _proxy_control_reachable(chain))
    assert _request(chain, chain["control_ip"], "/healthz")["status"] == 200
    _ready_endpoint_slice(chain["api"], chain["service"], chain["agent"])

    # No policy selects this other identity, leaving its egress open. A real
    # gateway response therefore exercises the Agent's exact ingress selector.
    wrong = copy.deepcopy(chain["proxy_manifest"])
    wrong.metadata.name = "as-proxy-other-" + secrets.token_hex(12)
    wrong.metadata.labels["io.agent-sandbox.sandbox-id"] = "sbx_" + secrets.token_hex(16)
    wrong.metadata.labels["io.agent-sandbox.transaction-id"] = "txn_" + secrets.token_hex(16)
    wrong_entry = chain["registry"].create("pod", wrong)
    deadline = time.monotonic() + 45
    while True:
        other = chain["api"].read_namespaced_pod(wrong.metadata.name, wrong.metadata.namespace, _request_timeout=(0.5, 0.5))
        _verify_owned(other, wrong_entry["expected"], wrong_entry["uid"])
        if other.status.phase == "Running" and other.status.pod_ip:
            break
        assert time.monotonic() < deadline, "other-identity proxy did not start"
        time.sleep(0.1)
    _eventually_status(lambda: _request(chain, other.status.pod_ip, route, method="POST", credential=chain["token"],
                                       content=json.dumps({"argv": ["true"]}).encode()), {502, 504})
    assert _request(chain, proxy_ip, "/healthz")["status"] == 200
    chain["registry"].delete(chain["agent_allow"])
    _eventually_status(lambda: _request(chain, proxy_ip, route, method="POST", credential=chain["token"],
                                       content=json.dumps({"argv": ["true"]}).encode()), {502, 504})
    assert _request(chain, chain["control_ip"], "/healthz")["status"] == 200
    current = _ready_pod(chain["api"], chain["client"]["expected"], chain["client"]["uid"])
    assert current.status.pod_ip
    _ready_endpoint_slice(chain["api"], chain["service"], chain["agent"])


@pytest.mark.parametrize("proxy_chain", [True], indirect=True)
def test_proxy_strips_platform_auth_headers_before_real_agent(proxy_chain):
    """A bounded fixture-only recorder forwards to the actual loopback Agent and exposes only header facts."""
    chain = proxy_chain
    headers = {name: "synthetic" for name in _AUTH_HEADERS if name != "Authorization"}
    headers["X-Ordinary"] = "kept"
    proxy_ip = chain["proxy"].status.pod_ip
    for route in ("/healthz", "/v1/files?path=header-check", "/v1/commands:stream", "/unknown"):
        response = _request(chain, proxy_ip, route, credential=chain["token"], headers=headers)
        assert response["status"] in {200, 404, 405}, "instrumented real Agent did not receive the request"
    command = json.dumps({"argv": ["cat", "/tmp/proxy-header-facts.jsonl"]}).encode()
    response = _request(chain, proxy_ip, "/v1/commands:run", method="POST", credential=chain["token"], content=command)
    assert response["status"] == 200
    result = json.loads(response["body"])
    assert result["exit_code"] == 0
    facts = [json.loads(line) for line in result["stdout"].splitlines()]
    assert len(facts) == 4
    assert {item["path"] for item in facts} == {"/healthz", "/v1/files?path=header-check", "/v1/commands:stream", "/unknown"}
    assert all(item["credential_present"] is False and item["ordinary_present"] is True for item in facts)
