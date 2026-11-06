"""Offline Kubernetes probe tests with independent clients and controlled worker completion."""

import asyncio
import copy
import importlib
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from kubernetes import client, config
from urllib3 import exceptions as transport_errors

from control_plane.core import errors
from control_plane.core.service import SandboxService
from lifecycle_fakes import FakeLifecycleStore


class FakeClient:
    """Expose only the requested read operation and the SDK-owned close boundary."""

    def __init__(self, error=None, close_error=None, entered=None, release=None):
        """Record calls, thread ownership and optional failures without external I/O."""
        self.api_client = self
        self.error, self.close_error = error, close_error
        self.entered, self.release = entered, release
        self.calls, self.events = [], []
        self.closed = threading.Event()

    def list_namespaced_pod(self, namespace, **kwargs):
        """Return an empty first page with a continuation token that must never be followed."""
        self.calls.append((namespace, kwargs))
        self.events.append(("list", threading.get_ident()))
        if self.entered is not None:
            self.entered.set()
        if self.release is not None and not self.release.wait(3):
            raise RuntimeError("test worker was not released")
        if self.error is not None:
            raise self.error
        return SimpleNamespace(items=[], metadata=SimpleNamespace(_continue="do-not-follow"))

    def close(self):
        """Close in the worker after listing, including failure and cancellation paths."""
        self.events.append(("close", threading.get_ident()))
        self.closed.set()
        if self.close_error is not None:
            raise self.close_error


def runtime_module():
    """Load the concrete backend only when a test intentionally exercises it."""
    return importlib.import_module("control_plane.runtime.kubernetes.kubernetes_runtime")


def make_runtime(fake=None, factory=None, **overrides):
    """Build an explicitly configured backend whose default test factory never touches kubeconfig."""
    settings = dict(kubeconfig_path=Path("/not-read/config"), context="chosen-context", namespace="chosen-namespace")
    settings.update(overrides)
    return runtime_module().KubernetesSandboxRuntime(
        **settings, client_factory=factory if factory is not None else lambda **_: fake,
    )


def static_kubeconfig(tmp_path):
    """Write non-secret certificate-path fixtures and an unrelated selected current context."""
    for name in ("ca.crt", "client.crt", "client.key"):
        (tmp_path / name).write_text("test-only-placeholder")
    document = {
        "apiVersion": "v1", "kind": "Config", "current-context": "other-context",
        "contexts": [{"name": "chosen-context", "context": {"cluster": "chosen", "user": "chosen"}}],
        "clusters": [{"name": "chosen", "cluster": {
            "server": "https://127.0.0.1:16443", "certificate-authority": "ca.crt",
        }}],
        "users": [{"name": "chosen", "user": {
            "client-certificate": "client.crt", "client-key": "client.key",
        }}],
    }
    path = tmp_path / "kubeconfig"
    path.write_text(yaml.safe_dump(document))
    return path, document


def test_import_and_construction_do_not_load_configuration_or_create_clients(monkeypatch):
    """Import and assembly leave file, cluster and SDK client access deferred until ping."""
    def forbidden(*args, **kwargs):
        """Fail any accidental eager SDK configuration or connection attempt."""
        pytest.fail("eager Kubernetes access")

    monkeypatch.setattr(config, "load_kube_config", forbidden)
    monkeypatch.setattr(client, "ApiClient", forbidden)
    monkeypatch.setattr(Path, "open", forbidden)
    runtime = runtime_module().KubernetesSandboxRuntime(
        kubeconfig_path="/must-not-read", context="chosen-context", namespace="chosen-namespace",
    )
    assert runtime is not None


@pytest.mark.anyio
async def test_ping_is_lazy_bounded_single_page_and_closed_in_worker():
    """A successful empty Pod query uses only the explicit target and one bounded SDK call."""
    fake = FakeClient()
    factories = []

    def factory(**kwargs):
        """Observe immutable configuration and the factory's worker thread."""
        factories.append((kwargs, threading.get_ident()))
        return fake

    runtime = make_runtime(factory=factory)
    assert not factories
    assert await SandboxService(runtime, FakeLifecycleStore()).ping() is None
    assert factories[0][0] == {"kubeconfig_path": Path("/not-read/config"), "context": "chosen-context"}
    assert fake.calls == [("chosen-namespace", {"limit": 1, "watch": False, "_request_timeout": (0.5, 0.5)})]
    assert fake.events == [("list", factories[0][1]), ("close", factories[0][1])]
    assert factories[0][1] != threading.get_ident()


@pytest.mark.anyio
@pytest.mark.parametrize("kind,expected", [
    ("config", "KubernetesConfigurationError"), ("401", "KubernetesPermissionDenied"),
    ("403", "KubernetesPermissionDenied"), ("500", "KubernetesConnectionFailed"),
    ("408", "KubernetesTimeout"), ("504", "KubernetesTimeout"),
    ("network", "KubernetesConnectionFailed"), ("timeout", "KubernetesTimeout"),
    ("connect-timeout", "KubernetesTimeout"),
    ("wrapped-timeout", "KubernetesTimeout"), ("unknown", "KubernetesConnectionFailed"),
])
async def test_ping_errors_are_stable_and_close_does_not_mask_primary(kind, expected):
    """Configuration, permissions, transport and wrapped timeouts stay distinguishable and sanitized."""
    secret = "private-token-and-certificate-path"
    if kind.isdecimal():
        failure = client.ApiException(status=int(kind), reason=secret)
        failure.body = secret
    else:
        failure = {
            "config": config.ConfigException(secret), "network": ConnectionError(secret),
            "timeout": transport_errors.ReadTimeoutError(None, secret, secret),
            "connect-timeout": transport_errors.ConnectTimeoutError(secret),
            "wrapped-timeout": transport_errors.MaxRetryError(None, secret, TimeoutError(secret)),
            "unknown": RuntimeError(secret),
        }[kind]
    fake = FakeClient(error=failure, close_error=RuntimeError(secret))
    with pytest.raises(getattr(errors, expected)) as captured:
        await make_runtime(fake).ping()
    assert fake.closed.is_set() and len(fake.calls) == 1
    assert secret not in str(captured.value) and secret not in repr(captured.value)
    assert captured.value.__cause__ is None and captured.value.__suppress_context__


@pytest.mark.anyio
async def test_factory_failure_and_successful_probe_close_failure():
    """No handle is closed before acquisition; a lone cleanup failure is still an explicit failure."""
    def failing_factory(**kwargs):
        """Simulate failure before returning any acquired client."""
        raise config.ConfigException("private kubeconfig")

    with pytest.raises(errors.KubernetesConfigurationError):
        await make_runtime(factory=failing_factory).ping()
    fake = FakeClient(close_error=RuntimeError("private close error"))
    with pytest.raises(errors.KubernetesConnectionFailed):
        await make_runtime(fake).ping()
    assert fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("failure", [None, ConnectionError("private delayed failure")])
async def test_repeated_cancellation_waits_for_worker_owned_close(failure):
    """Canceling the waiter cannot close a client still in use or abandon a late worker failure."""
    entered, release = threading.Event(), threading.Event()
    fake = FakeClient(error=failure, entered=entered, release=release)
    task = asyncio.create_task(make_runtime(fake).ping())
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done() and not fake.closed.is_set()
    finally:
        release.set()
    expected = errors.KubernetesConnectionFailed if failure else asyncio.CancelledError
    with pytest.raises(expected):
        await task
    assert fake.closed.is_set()
    assert fake.events[0][1] == fake.events[1][1] != threading.get_ident()


@pytest.mark.anyio
async def test_default_factory_uses_independent_static_config_and_no_environment_proxy(tmp_path, monkeypatch):
    """Real kubeconfig loading selects the named context without global config changes or proxy inheritance."""
    path, _ = static_kubeconfig(tmp_path)
    fake = FakeClient()
    configurations = []
    global_before = copy.deepcopy(client.Configuration._default)

    def api_client(configuration):
        """Capture the real isolated SDK settings without opening a network connection."""
        configurations.append(configuration)
        return fake

    monkeypatch.setenv("HTTP_PROXY", "http://untrusted.invalid:9999")
    monkeypatch.setenv("HTTPS_PROXY", "http://untrusted.invalid:9999")
    monkeypatch.setattr(client, "ApiClient", api_client)
    monkeypatch.setattr(client, "CoreV1Api", lambda api_client: api_client)
    runtime = runtime_module().KubernetesSandboxRuntime(
        kubeconfig_path=path, context="chosen-context", namespace="chosen-namespace",
    )
    assert not configurations
    await runtime.ping()
    cfg = configurations[0]
    assert cfg.host == "https://127.0.0.1:16443"
    assert cfg.proxy is None and cfg.retries is False and not cfg.client_go_retries
    assert cfg.verify_ssl and not cfg.api_key and not cfg.debug
    assert Path(cfg.cert_file) == tmp_path / "client.crt"
    assert Path(cfg.key_file) == tmp_path / "client.key"
    assert client.Configuration._default == global_before
    assert fake.closed.is_set()


@pytest.mark.anyio
@pytest.mark.parametrize("bad", ["missing-file", "missing-context", "plugin", "token", "insecure", "proxy"])
async def test_invalid_configuration_is_rejected_before_client_creation(tmp_path, monkeypatch, bad):
    """Invalid targets and unsupported authentication never execute plugins or acquire a client."""
    path, document = static_kubeconfig(tmp_path)
    if bad == "missing-file":
        path = tmp_path / "missing"
    elif bad == "missing-context":
        document["contexts"] = []
    elif bad == "plugin":
        document["users"][0]["user"]["exec"] = {"command": "must-never-execute"}
    elif bad == "token":
        document["users"][0]["user"]["token"] = "private-token"
    elif bad == "insecure":
        document["clusters"][0]["cluster"]["insecure-skip-tls-verify"] = True
    elif bad == "proxy":
        document["clusters"][0]["cluster"]["proxy-url"] = "http://untrusted.invalid"
    if bad != "missing-file":
        path.write_text(yaml.safe_dump(document))

    def forbidden(*args, **kwargs):
        """Guard against both unexpected network construction and auth plugin execution."""
        pytest.fail("invalid configuration reached client creation or plugin execution")

    monkeypatch.setattr(client, "ApiClient", forbidden)
    monkeypatch.setattr(config.kube_config.ExecProvider, "run", forbidden)
    with pytest.raises(errors.KubernetesConfigurationError):
        await runtime_module().KubernetesSandboxRuntime(
            kubeconfig_path=path, context="chosen-context", namespace="chosen-namespace",
        ).ping()


@pytest.mark.anyio
async def test_all_unsupported_operations_fail_without_client_access():
    """The complete Runtime contract stays explicit and resource-free, including workspace methods."""
    def forbidden(**kwargs):
        """Reject any accidental Kubernetes access from an unimplemented capability."""
        pytest.fail("unsupported operation created a client")

    runtime = make_runtime(factory=forbidden)
    for method, args, kwargs in [
        ("create", (object(),), {}), ("get", ("sbx_test",), {}),
        ("get_endpoint", ("sbx_test",), {}), ("delete", ("sbx_test",), {}),
        ("pause", ("sbx_test",), {"expected_transaction_id": "txn_test"}),
        ("resume", ("sbx_test",), {"expected_transaction_id": "txn_test"}),
        ("inspect_resources", ("sbx_test",), {"expected_transaction_id": "txn_test"}),
        ("resolve_environment_image", ("basic",), {}),
        ("create_workspace", (object(),), {}), ("inspect_workspace", (object(),), {}),
        ("delete_workspace", (object(),), {}),
    ]:
        with pytest.raises(errors.KubernetesOperationUnsupported) as captured:
            await getattr(runtime, method)(*args, **kwargs)
        if method == "create":
            assert captured.value.creation_failure.resource_state is errors.CreationResourceState.UNCHANGED


@pytest.mark.parametrize("settings", [
    {"kubeconfig_path": ""}, {"kubeconfig_path": None}, {"kubeconfig_path": "/bad\x00file"},
    {"context": ""}, {"context": None}, {"context": "other context"},
    {"context": "x\n"}, {"namespace": ""}, {"namespace": "UPPER"},
    {"namespace": "a" * 64}, {"namespace": "../other"}, {"namespace": None},
])
def test_explicit_target_settings_are_validated_without_io(settings):
    """Reject missing or malformed trusted target values before a client can be constructed."""
    with pytest.raises(errors.KubernetesConfigurationError):
        make_runtime(**settings)


@pytest.mark.anyio
async def test_concurrent_probes_own_independent_handles_and_keep_the_same_namespace():
    """Two pings acquire and close distinct clients instead of sharing a pool across worker threads."""
    clients = []

    def factory(**kwargs):
        """Create one fresh test client for each probe."""
        fake = FakeClient()
        clients.append(fake)
        return fake

    runtime = make_runtime(factory=factory)
    await asyncio.gather(runtime.ping(), runtime.ping())
    assert len(clients) == 2 and clients[0] is not clients[1]
    assert all(fake.closed.is_set() and len(fake.calls) == 1 for fake in clients)
