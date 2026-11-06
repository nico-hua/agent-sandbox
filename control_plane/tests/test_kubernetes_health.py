"""Authenticated Kubernetes readiness through the existing app, Service and Runtime chain."""

import asyncio
import subprocess
import sys
import threading
from pathlib import Path

import httpx
import pytest
from kubernetes import client, config

from control_plane.app import create_app
from control_plane.core.service import SandboxService
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from lifecycle_fakes import FakeLifecycleStore
from test_kubernetes_runtime import FakeClient

KEY = "test-only-readiness-key"
AUTH = {"Authorization": "Bearer " + KEY}
UNAVAILABLE = {
    "status": "unavailable",
    "error": {"code": "kubernetes_unavailable", "message": "Kubernetes runtime is unavailable"},
}


def make_app(fake=None, factory=None, *, timeout=1.0):
    """Assemble a real Kubernetes Runtime with an offline factory and no lifecycle workers."""
    runtime = KubernetesSandboxRuntime(
        kubeconfig_path="/not-read/kubeconfig", context="explicit-test", namespace="explicit-test",
        client_factory=factory if factory is not None else lambda **_: fake,
    )
    return create_app(
        runtime=runtime, api_key=KEY, store=FakeLifecycleStore(), readiness_timeout=timeout,
        reaper_enabled=False, pending_recovery_enabled=False,
    )


@pytest.mark.anyio
async def test_health_bypasses_probe_and_authenticated_readiness_enters_service_once(monkeypatch):
    """Anonymous health performs no backend work; one authenticated readiness request delegates once."""
    fake = FakeClient()
    application = make_app(fake)
    observed = []
    original = SandboxService.ping

    async def ping(service):
        """Record real Service entry without replacing its Runtime delegation."""
        observed.append(service)
        await original(service)

    monkeypatch.setattr(SandboxService, "ping", ping)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=application), base_url="http://control") as http:
        assert (await http.get("/healthz")).json() == {"status": "ok"}
        assert not observed and not fake.calls
        for path in ("/healthz-other", "/healthz/", "/readyz"):
            assert (await http.get(path)).status_code == 401
        assert (await http.post("/healthz")).status_code == 401
        assert not observed and not fake.calls
        response = await http.get("/readyz", headers=AUTH)
    assert response.status_code == 200 and response.json() == {"status": "ready"}
    assert observed == [application.state.service] and len(fake.calls) == 1
    assert fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("failure", ["config", "401", "403", "connection", "timeout", "close"])
async def test_kubernetes_failures_have_one_fixed_http_response_without_docker_details(failure, caplog):
    """All internal Kubernetes probe categories collapse into a sanitized backend-specific 503."""
    secret = "private-key https://private-api.invalid /private/cert/path"
    fake = FakeClient()

    def factory(**kwargs):
        """Represent configuration failure before acquisition or one failing acquired client."""
        if failure == "config":
            raise config.ConfigException(secret)
        return fake

    if failure.isdecimal():
        fake.error = client.ApiException(status=int(failure), reason=secret)
    elif failure == "connection":
        fake.error = ConnectionError(secret)
    elif failure == "timeout":
        fake.error = TimeoutError(secret)
    elif failure == "close":
        fake.close_error = RuntimeError(secret)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(factory=factory)), base_url="http://control") as http:
        response = await http.get("/readyz", headers=AUTH)
    assert response.status_code == 503 and response.json() == UNAVAILABLE
    assert secret not in response.text and secret not in caplog.text
    assert fake.closed.is_set() == (failure != "config")


@pytest.mark.anyio
@pytest.mark.parametrize("headers", [{}, {"Authorization": "Bearer wrong-key"}])
async def test_unauthorized_readiness_never_constructs_a_client_or_calls_service(headers, monkeypatch):
    """Authentication remains before all Service, configuration and client work."""
    def forbidden(**kwargs):
        """Reject accidental client acquisition by a denied request."""
        pytest.fail("unauthenticated request reached Kubernetes")

    async def forbidden_ping(service):
        """Reject accidental Service entry before authentication."""
        pytest.fail("unauthenticated request reached Service")

    monkeypatch.setattr(SandboxService, "ping", forbidden_ping)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(factory=forbidden)), base_url="http://control") as http:
        response = await http.get("/readyz", headers=headers)
    assert response.status_code == 401 and response.json()["error"]["code"] == "unauthorized"


@pytest.mark.anyio
@pytest.mark.parametrize("finish", ["success", "failure"])
async def test_http_timeout_waits_for_worker_close_and_sanitizes_late_outcome(finish):
    """The readiness deadline requests cancellation but cannot finish while the worker owns its client."""
    entered, release = threading.Event(), threading.Event()
    fake = FakeClient(entered=entered, release=release,
                      error=ConnectionError("private late error") if finish == "failure" else None)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(fake, timeout=0.01)), base_url="http://control") as http:
        request = asyncio.create_task(http.get("/readyz", headers=AUTH))
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            past_deadline = asyncio.Event()
            timer = asyncio.get_running_loop().call_later(0.03, past_deadline.set)
            try:
                await past_deadline.wait()
            finally:
                timer.cancel()
            assert not request.done() and not fake.closed.is_set()
        finally:
            release.set()
        response = await request
    assert response.status_code == 503 and response.json() == UNAVAILABLE
    assert fake.closed.is_set() and fake.events[0][1] == fake.events[1][1]


@pytest.mark.anyio
async def test_repeated_http_cancellation_settles_probe_before_propagating():
    """Canceling the HTTP waiter repeatedly never abandons or prematurely closes the SDK worker."""
    entered, release = threading.Event(), threading.Event()
    fake = FakeClient(entered=entered, release=release)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=make_app(fake)), base_url="http://control") as http:
        request = asyncio.create_task(http.get("/readyz", headers=AUTH))
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            request.cancel()
            await asyncio.sleep(0)
            request.cancel()
            await asyncio.sleep(0)
            assert not request.done() and not fake.closed.is_set()
        finally:
            release.set()
        with pytest.raises(asyncio.CancelledError):
            await request
    assert fake.closed.is_set() and fake.events[0][1] == fake.events[1][1]


def test_fresh_import_and_kubernetes_app_assembly_do_not_access_external_state(tmp_path):
    """Import and dependency wiring leave kubeconfig, SDK, Docker and SQLite untouched."""
    script = r'''
from pathlib import Path
import sqlite3
import docker
from kubernetes import client, config
def forbidden(*args, **kwargs):
    """Guard deferred external resource access during import and assembly."""
    raise AssertionError("unexpected external I/O")
Path.open = forbidden
sqlite3.connect = forbidden
docker.from_env = forbidden
client.ApiClient = forbidden
config.load_kube_config = forbidden
from control_plane.app import create_app
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
runtime = KubernetesSandboxRuntime(kubeconfig_path="/must-not-read", context="test", namespace="test")
app = create_app(runtime, api_key="test-only-key", reaper_enabled=False, pending_recovery_enabled=False)
assert app.state.runtime is runtime
assert app.state.reaper_task is None
'''
    result = subprocess.run([sys.executable, "-c", script], cwd=Path(__file__).resolve().parents[2],
                            text=True, capture_output=True, timeout=15)
    assert result.returncode == 0, result.stderr
