"""Double-opt-in HTTP creation and authenticated execution using formally configured Kubernetes applications."""

import asyncio
import secrets

import httpx
import pytest

from control_plane.app import create_app
from control_plane.core.errors import SandboxError
from control_plane.runtime.kubernetes import kubernetes_runtime
from control_plane.runtime.kubernetes.pod_query import sandbox_pod_name
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from test_kubernetes_endpoint_integration import _delivered_endpoint
from test_kubernetes_local_entry_integration import _assert_port_closed, _protocol
from test_kubernetes_persistent_workspace_integration import (
    _STORAGE_CLASS, owned_persistent_workspace, pytestmark,
)
from test_kubernetes_private_workspace_integration import _wait_volume_absent
from test_kubernetes_proxy_integration import _PROXY_IMAGE, _RedactedText
from test_kubernetes_proxy_lifecycle_integration import owned_proxy_lifecycle
from test_kubernetes_service_create_integration import _observe_creation


@pytest.mark.parametrize("persistent", [False, True])
def test_formal_http_create_prepares_original_entry_before_activation(persistent, request, tmp_path, monkeypatch):
    """Use only HTTP create/ready/endpoint/delete; fixtures merely record original UIDs for independent cleanup."""
    _formal_http_workflow(persistent, request, tmp_path, monkeypatch)


def _formal_http_workflow(persistent, request, tmp_path, monkeypatch, *, environment="basic"):
    """Run the shared HTTP/entry/SQLite contract with the selected trusted Agent language image."""
    chain = request.getfixturevalue("owned_persistent_workspace" if persistent else "owned_proxy_lifecycle")

    async def verify():
        """Exercise the actual Service and temporary SQLite without manually opening sessions or writing active."""
        for key, value in {
            "AGENT_SANDBOX_RUNTIME": "kubernetes",
            "AGENT_SANDBOX_K8S_KUBECONFIG": str(chain["path"]),
            "AGENT_SANDBOX_K8S_CONTEXT": chain["context"],
            "AGENT_SANDBOX_K8S_NAMESPACE": chain["namespace"],
            "AGENT_SANDBOX_K8S_AGENT_IMAGE": "agent-sandbox-basic:dev",
            "AGENT_SANDBOX_K8S_PROXY_IMAGE": _PROXY_IMAGE,
        }.items():
            monkeypatch.setenv(key, value)
        for name in ("python", "node"):
            monkeypatch.setenv("AGENT_SANDBOX_K8S_" + name.upper() + "_IMAGE", "agent-sandbox-" + name + ":dev")
        if persistent:
            monkeypatch.setenv("AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", _STORAGE_CLASS)
        else:
            monkeypatch.delenv("AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS", raising=False)
        # Borrow the fixture's independent recording factory, not its Runtime
        # creation method. All managed resources still originate from HTTP.
        recording = chain["make_runtime"]()
        monkeypatch.setattr(kubernetes_runtime, "create_probe_client", recording._client_factory)
        store = SQLiteLifecycleStore(tmp_path / "automatic-http.sqlite3")
        admin = _RedactedText(secrets.token_urlsafe(32))
        app = create_app(store=store, api_key=str(admin), reaper_enabled=False, sandbox_query_timeout=10)
        backend, manager = app.state.runtime, app.state.kubernetes_entry_manager
        _observe_creation(chain, backend, store, monkeypatch)
        create = backend.create
        failures = []

        async def observed_create(spec):
            """Capture only fixed failure evidence, never SDK messages, manifests or credentials."""
            try:
                await create(spec)
            except SandboxError as error:
                info = error.creation_failure
                failures.append((type(error).__name__, None if info is None else info.phase,
                                 None if info is None else info.resource_state.value))
                raise

        monkeypatch.setattr(backend, "create", observed_create)
        spawn = asyncio.create_subprocess_exec
        processes = []

        async def counted(*args, **kwargs):
            """Retain only self-owned process handles, without capturing credentials or command-line diagnostics."""
            process = await spawn(*args, **kwargs)
            processes.append(process)
            return process

        monkeypatch.setattr(asyncio, "create_subprocess_exec", counted)
        try:
            assert manager is not None and manager.session_count == 0 and not processes
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=app), base_url="http://isolated-control",
                    trust_env=False, timeout=120,
                ) as control:
                    headers = {"Authorization": "Bearer " + admin}
                    denied = await control.post("/v1/sandboxes", json={"environment": environment})
                    assert denied.status_code == 401 and not chain["managed"].entries
                    response = await control.post("/v1/sandboxes", json={"environment": environment}, headers=headers)
                    assert response.status_code == 201, failures
                    assert response.json()["status"] == "started"
                    spec = chain["spec"]
                    sandbox_id = response.json()["sandbox_id"]
                    assert sandbox_id == spec.sandbox_id and manager.session_count == 1
                    assert len(processes) == 1
                    active = await store.get(sandbox_id)
                    assert active.management_state == "active" and active.version == 1
                    assert active.transaction_id == spec.transaction_id and active.created_at == spec.created_at
                    chain["managed"].assert_complete()
                    ready = await control.get(f"/v1/sandboxes/{sandbox_id}/readyz", headers=headers)
                    assert ready.status_code == 200
                    token = _RedactedText(spec.access_token)
                    path = f"/v1/sandboxes/{sandbox_id}/endpoint"
                    entry, delivered = await _delivered_endpoint(control, path, admin, token)
                    await _protocol(entry, delivered)
                    agent = chain["api"].read_namespaced_pod(
                        sandbox_pod_name(sandbox_id), chain["namespace"], _request_timeout=(0.5, 0.5),
                    )
                    assert agent.spec.containers[0].image == "agent-sandbox-" + environment + ":dev"
                    if environment != "basic":
                        await _language_protocol(entry.url, delivered, environment)
                    again, _ = await _delivered_endpoint(control, path, admin, token)
                    assert again.url == entry.url and len(processes) == 1
                    deleted = await control.delete(
                        f"/v1/sandboxes/{sandbox_id}", headers=headers,
                        params={"delete_workspace": "true"} if persistent else {},
                    )
                    assert deleted.status_code == 204 and manager.session_count == 0
                    assert await store.get(sandbox_id) is None
                    assert await store.get_credential(sandbox_id, spec.transaction_id) is None
                    await _assert_port_closed(entry.url)
                await asyncio.to_thread(chain["managed"].assert_absent)
            assert manager.is_closed and all(process.returncode is not None for process in processes)
            if persistent:
                await asyncio.to_thread(_wait_volume_absent, chain["api"], chain["volume"])
        finally:
            await manager.close_all()

    asyncio.run(verify())


async def _language_protocol(url, token, environment):
    """Execute the actual language interpreter and read its workspace output through the authenticated file API."""
    source = {
        "python": ["python", "-c", "from pathlib import Path; p=Path('/workspace/language.txt'); p.write_text('python-ok'); print(p.read_text(), end='')"],
        "node": ["node", "-e", "const fs=require('fs'); const p='/workspace/language.txt'; fs.writeFileSync(p,'node-ok'); process.stdout.write(fs.readFileSync(p));"],
    }
    async with httpx.AsyncClient(trust_env=False, follow_redirects=False, timeout=40) as http:
        headers = {"Authorization": "Bearer " + token}
        result = await http.post(url + "/v1/commands:run", headers=headers, json={"argv": source[environment]})
        assert result.status_code == 200
        payload = result.json()
        assert payload["exit_code"] == 0 and payload["stdout"] == environment + "-ok"
        file = await http.get(url + "/v1/files", headers=headers, params={"path": "language.txt"})
        assert file.status_code == 200 and file.content == (environment + "-ok").encode()
