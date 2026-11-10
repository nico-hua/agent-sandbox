"""Double-opt-in WSL kubectl sessions against only original test-owned authenticated proxies."""

import asyncio
import base64
import copy
import json
import time
from urllib.parse import urlsplit

import httpx
import pytest
from kubernetes import client

from control_plane.core.errors import SandboxCreateFailed
from control_plane.runtime.kubernetes.local_entry import KubernetesLocalEntry, LocalEntryError
from test_kubernetes_proxy_integration import _PROXY_IMAGE, _RedactedText
from test_kubernetes_proxy_lifecycle_integration import (
    owned_proxy_lifecycle, pytestmark,
)
from test_kubernetes_query_integration import _verify_owned


async def _create_ready(chain):
    """Create eight resources through Runtime and wait for full readiness without opening any network policy."""
    backend = chain["make_runtime"](proxy_image=_PROXY_IMAGE)
    try:
        await backend.create(chain["spec"])
    except SandboxCreateFailed as error:
        evidence = error.creation_failure
        pytest.fail(f"test Runtime creation failed: phase={evidence.phase}, state={evidence.resource_state.value}")
    chain["managed"].assert_complete()
    deadline = time.monotonic() + 45
    while True:
        observed = await backend.get(chain["spec"].sandbox_id)
        if observed.status == "ready":
            return backend
        assert observed.status in {"starting", "running"}
        assert time.monotonic() < deadline, "test proxy chain did not become ready"
        await asyncio.sleep(0.1)


async def _assert_port_closed(url):
    """Verify the local listener is gone independently of the session's own status flag."""
    parsed = urlsplit(url)
    try:
        _, writer = await asyncio.wait_for(asyncio.open_connection(parsed.hostname, parsed.port), 1)
    except OSError:
        return
    writer.close()
    await writer.wait_closed()
    pytest.fail("owned loopback listener remained open after session close")


async def _protocol(entry, token):
    """Verify actual local authentication, commands, binary files and SSE without displaying credentials."""
    async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=10) as http:
        url = entry.url
        assert (await http.get(url + "/healthz")).status_code == 200
        payload = {"argv": ["sh", "-c", "touch /workspace/local-entry-denied"]}
        for header in ({}, {"Authorization": "Bearer wrong-test-token"}):
            response = await http.post(url + "/v1/commands:run", json=payload, headers=header)
            assert response.status_code == 401 and response.headers["www-authenticate"] == "Bearer"
        headers = {"Authorization": "Bearer " + token}
        response = await http.post(url + "/v1/commands:run", headers=headers, json={"argv": ["sh", "-c",
            'test ! -e /workspace/local-entry-denied && test -z "${SANDBOX_ACCESS_TOKEN:-}" && '
            'test -z "${AGENT_SANDBOX_API_KEY:-}" && printf local-entry-ok']})
        assert response.status_code == 200
        assert response.json()["exit_code"] == 0 and response.json()["stdout"] == "local-entry-ok"
        data = b"\x00\xfflocal-entry-file\x80\r\n"
        uploaded = await http.post(url + "/v1/files", params={"path": "local entry.bin"},
                                   headers=headers, content=data)
        assert uploaded.status_code == 201
        downloaded = await http.get(url + "/v1/files", params={"path": "local entry.bin"}, headers=headers)
        assert downloaded.status_code == 200 and downloaded.content == data
        stream = await http.post(url + "/v1/commands:stream", headers=headers,
                                 json={"argv": ["printf", "local-entry-stream"]})
        assert stream.status_code == 200 and stream.headers["content-type"].startswith("text/event-stream")
        frames = [frame.splitlines() for frame in stream.text.split("\n\n") if frame]
        output = b"".join(base64.b64decode(json.loads(lines[1].removeprefix("data: "))["data_base64"])
                          for lines in frames if lines[0] == "event: stdout")
        assert output == b"local-entry-stream" and any(lines[0] == "event: complete" for lines in frames)


def test_wsl_loopback_protocol_and_close_preserves_original_sandbox(owned_proxy_lifecycle):
    """An actual local session protects requests and releases its listener without destroying the sandbox."""
    chain = owned_proxy_lifecycle

    async def verify():
        """Keep process, pipes and all HTTP operations on one live loop for the entire owned session."""
        backend = await _create_ready(chain)
        session = KubernetesLocalEntry(backend, chain["spec"].sandbox_id)
        try:
            async with session:
                url = session.url
                process = session._process
                target = session.target
                assert target.proxy_service_uid == chain["managed"].role("proxy-service")["uid"]
                assert target.proxy_pod_uid == chain["managed"].role("proxy")["uid"]
                await _protocol(session, _RedactedText(chain["spec"].access_token))
            assert process.returncode is not None and not session.is_active
            await session.close()
            await _assert_port_closed(url)
            observed = await backend.get(chain["spec"].sandbox_id)
            assert observed.status == "ready" and observed.transaction_id == chain["spec"].transaction_id
            await backend.delete(chain["spec"].sandbox_id, expected_transaction_id=chain["spec"].transaction_id)
            chain["managed"].assert_absent()
        finally:
            await session.close()

    asyncio.run(verify())


def test_target_deletion_invalidates_forward_and_never_follows_replacement(owned_proxy_lifecycle):
    """A terminated original proxy invalidates the URL even if a test-owned replacement takes its name."""
    chain = owned_proxy_lifecycle

    async def verify():
        """Independently remove the original UID, create a distinct replacement and clean both with exact guards."""
        backend = await _create_ready(chain)
        session = KubernetesLocalEntry(backend, chain["spec"].sandbox_id)
        replacement_entry = None
        try:
            await session.open()
            url, process, original_uid = session.url, session._process, session.target.proxy_pod_uid
            entry = chain["managed"].role("proxy")
            pod = chain["api"].read_namespaced_pod(entry["expected"]["name"], chain["namespace"],
                                                  _request_timeout=(0.5, 0.5))
            _verify_owned(pod, entry["expected"], original_uid)
            await asyncio.to_thread(chain["managed"].delete, entry)
            deadline = time.monotonic() + 10
            while session.is_active:
                assert time.monotonic() < deadline, "port-forward did not observe its original Pod termination"
                await asyncio.sleep(0.05)
            replacement = copy.deepcopy(pod)
            replacement.metadata = client.V1ObjectMeta(
                name=pod.metadata.name, namespace=pod.metadata.namespace,
                labels=dict(pod.metadata.labels), annotations=dict(pod.metadata.annotations))
            replacement.status = None
            replacement_entry = chain["registry"].create("pod", replacement)
            assert replacement_entry["uid"] != original_uid
            assert not session.is_active and session.target.proxy_pod_uid == original_uid
            with pytest.raises(LocalEntryError):
                _ = session.url
            await session.close()
            assert process.returncode is not None
            await _assert_port_closed(url)
        finally:
            await session.close()
            if replacement_entry is not None:
                await asyncio.to_thread(chain["registry"].delete, replacement_entry)
        await backend.delete(chain["spec"].sandbox_id, expected_transaction_id=chain["spec"].transaction_id)
        chain["managed"].assert_absent()

    asyncio.run(verify())
