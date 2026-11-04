"""Real authentication, startup ordering and credential containment tests."""

import hmac
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

import control_plane.app as app_module
from control_plane.app import create_app
from lifecycle_fakes import FakeLifecycleStore
from test_app import FakeRuntime

KEY = "test_admin_key_A"
OTHER = "test_admin_key_B"
SBX = "sbx_0123456789abcdef0123456789abcdef"
WSP = "wsp_0123456789abcdef0123456789abcdef"
DENIED = {"error": {"code": "unauthorized", "message": "authentication required"}}


@pytest.fixture
def anyio_backend():
    """Use the supported asyncio backend without requiring Trio."""
    return "asyncio"


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    """Keep real auth configuration independent of a developer shell's credentials."""
    monkeypatch.delenv("AGENT_SANDBOX_API_KEY", raising=False)


def application(**configuration):
    """Assemble real authentication with observable backend and metadata fakes."""
    return create_app(FakeRuntime(), store=FakeLifecycleStore(), reaper_enabled=False, **configuration)


@pytest.mark.anyio
async def test_missing_configuration_without_lifespan_fails_closed():
    """Skipping startup cannot bypass auth while process health remains anonymous."""
    app = application()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/healthz")).json() == {"status": "ok"}
        for headers in ({}, {"Authorization": "Bearer " + KEY}):
            response = await client.get("/readyz", headers=headers)
            assert response.status_code == 401 and response.json() == DENIED
            assert response.headers["WWW-Authenticate"] == "Bearer"
    assert app.state.runtime.calls == 0 and app.state.lifecycle_store.initialize_calls == 0


@pytest.mark.parametrize("value", [None, "", " ", " a", "a ", "a b", "a\tb", "a\nb", "a\rb", "a\x00b", "a\x7fb", "你好", "a.b", "a/b", "a+b", "a=b", "x" * 129, 1, True, b"key", [], {}])
def test_invalid_explicit_keys_fail_without_echo(value):
    """Reject incorrect types, whitespace, non-ASCII, punctuation and oversized keys."""
    with pytest.raises(ValueError) as caught:
        application(api_key=value)
    if isinstance(value, str) and len(value) > 3:
        assert value not in str(caught.value)


@pytest.mark.parametrize("value", ["", " ", "invalid.key", "x" * 129])
def test_invalid_environment_key_is_not_ignored(value, monkeypatch):
    """An invalid environment credential cannot silently disable authentication."""
    monkeypatch.setenv("AGENT_SANDBOX_API_KEY", value)
    with pytest.raises(ValueError):
        application()


@pytest.mark.parametrize("value", [None, 0, 1, "true", [], {}])
def test_development_mode_requires_boolean(value):
    """Only an actual boolean can select the deliberate development exception."""
    with pytest.raises(ValueError):
        application(allow_unauthenticated=value)


@pytest.mark.parametrize("environment", [False, True])
def test_valid_key_and_development_mode_conflict(environment, monkeypatch):
    """Reject ambiguous authentication choices regardless of key source."""
    if environment:
        monkeypatch.setenv("AGENT_SANDBOX_API_KEY", KEY)
    with pytest.raises(ValueError) as caught:
        application(allow_unauthenticated=True, **({} if environment else {"api_key": KEY}))
    assert KEY not in str(caught.value)


@pytest.mark.anyio
@pytest.mark.parametrize("key", ["x", "x" * 128, "Abc09_-safe"])
async def test_key_boundaries_and_scheme_case(key):
    """Allow bounded ASCII keys and scheme case changes without changing key case."""
    app = application(api_key=key)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        for scheme in ("Bearer", "bearer", "BEARER", "bEaReR"):
            assert (await client.get("/readyz", headers={"Authorization": scheme + " " + key})).status_code == 200
        assert (await client.get("/readyz", headers={"Authorization": "Bearer " + key.swapcase()})).status_code == 401


@pytest.mark.anyio
async def test_configuration_priority_frozen_values_and_independent_apps(monkeypatch):
    """Explicit keys override env and later environment changes never change an app's key."""
    monkeypatch.setenv("AGENT_SANDBOX_API_KEY", "invalid.env")
    explicit = application(api_key=KEY)
    monkeypatch.setenv("AGENT_SANDBOX_API_KEY", OTHER)
    inherited = application()
    monkeypatch.setenv("AGENT_SANDBOX_API_KEY", "third_key")
    for app, expected, wrong in ((explicit, KEY, OTHER), (inherited, OTHER, KEY)):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/readyz", headers={"Authorization": "Bearer " + expected})).status_code == 200
            assert (await client.get("/readyz", headers={"Authorization": "Bearer " + wrong})).status_code == 401


@pytest.mark.anyio
@pytest.mark.parametrize("headers", [
    [], [("Authorization", "")], [("Authorization", KEY)], [("Authorization", "Basic " + KEY)],
    [("Authorization", "Bearer")], [("Authorization", "Bearer " + OTHER)],
    [("Authorization", "Bearer  " + KEY)], [("Authorization", " Bearer " + KEY)],
    [("Authorization", "Bearer\t" + KEY)], [("Authorization", "Bearer " + KEY + " ")],
    [("Authorization", "Bearer " + KEY + "," + KEY)],
    [("Authorization", "Bearer " + KEY + ", Bearer " + KEY)],
    [("Authorization", "Bearer " + KEY), ("Authorization", "Bearer " + KEY)],
    [("Authorization", "Bearer " + KEY), ("authorization", "Bearer " + OTHER)],
    [("Authorization", b"Bearer \xff")], [("Authorization", "Bearer " + "x" * 129)],
])
async def test_bad_credentials_share_fixed_response(headers, caplog):
    """Missing, malformed, combined, duplicate and wrong keys are indistinguishable."""
    app = application(api_key=KEY)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/readyz", headers=headers)
    assert response.status_code == 401 and response.json() == DENIED
    assert response.headers["WWW-Authenticate"] == "Bearer"
    assert KEY not in response.text and OTHER not in response.text
    assert KEY not in caplog.text and OTHER not in caplog.text
    assert app.state.runtime.calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize("method,path", [
    ("GET", "/healthz/"), ("GET", "/healthz-other"), ("GET", "/HEALTHZ"),
    ("GET", "/%68ealthz"), ("GET", "/health%7a"), ("POST", "/healthz"),
    ("HEAD", "/healthz"), ("OPTIONS", "/healthz"), ("GET", "/unknown"),
    ("TRACE", "/readyz"), ("GET", "/docs"), ("GET", "/redoc"),
    ("GET", "/openapi.json"), ("GET", "/v1/sandboxes"), ("POST", "/v1/sandboxes"),
    ("GET", f"/v1/sandboxes/{SBX}"), ("GET", f"/v1/sandboxes/{SBX}/endpoint"),
    ("GET", f"/v1/sandboxes/{SBX}/readyz"), ("POST", f"/v1/sandboxes/{SBX}/renew"),
    ("POST", f"/v1/sandboxes/{SBX}/pause"), ("POST", f"/v1/sandboxes/{SBX}/resume"),
    ("DELETE", f"/v1/sandboxes/{SBX}"), ("POST", "/v1/workspaces"),
    ("GET", f"/v1/workspaces/{WSP}"), ("DELETE", f"/v1/workspaces/{WSP}"),
])
async def test_other_paths_and_methods_require_authentication(method, path):
    """Protect all management routes and path variants before redirects, routing and errors."""
    app = application(api_key=KEY)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", follow_redirects=False) as client:
        response = await client.request(method, path)
    assert response.status_code == 401
    assert response.headers["WWW-Authenticate"] == "Bearer"
    if method != "HEAD":
        assert response.json() == DENIED
    assert app.state.runtime.calls == 0 and not app.state.lifecycle_store.records


@pytest.mark.anyio
async def test_exact_healthz_is_anonymous_and_ignores_bad_credentials():
    """The exact GET health endpoint never queries resources or exposes configuration."""
    app = application(api_key=KEY)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        for headers in ({}, {"Authorization": "bad"}):
            response = await client.get("/healthz", headers=headers)
            assert response.status_code == 200 and response.json() == {"status": "ok"}
    assert app.state.runtime.calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize("source", ["query", "cookie", "body", "alternate"])
async def test_alternate_credentials_are_not_accepted(source):
    """Only Authorization can authenticate; request body and alternate headers cannot."""
    app = application(api_key=KEY)
    options = {"query": {"params": {"api_key": KEY}}, "cookie": {"headers": {"Cookie": "api_key=" + KEY}}, "body": {"json": {"api_key": KEY}}, "alternate": {"headers": {"X-API-Key": KEY}}}[source]
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/v1/sandboxes", **options)
    assert response.status_code == 401 and response.json() == DENIED
    assert app.state.runtime.calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize("path", ["/v1/sandboxes", "/v1/workspaces"])
async def test_unauthorized_does_not_read_body_or_invoke_business(path, monkeypatch, tmp_path):
    """Reject before receive, admission, ID/time factories, SQLite or Runtime work."""
    database = tmp_path / "not-created" / "lifecycle.sqlite3"
    app = create_app(FakeRuntime(), api_key=KEY, database_path=database, reaper_enabled=False)

    def forbidden(*args, **kwargs):
        """Fail if an unauthenticated request reaches any business operation."""
        pytest.fail("unauthenticated request entered business logic")

    for name in ("create", "create_workspace", "_id_factory", "_transaction_id_factory", "_clock"):
        monkeypatch.setattr(app.state.service, name, forbidden)
    app.state.service._creates_in_progress = 2
    messages = []

    async def receive():
        """Prove rejection never consumes the request body."""
        pytest.fail("unauthenticated body was read")

    async def send(message):
        """Capture the fixed error without client-side body buffering."""
        messages.append(message)

    await app({"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "scheme": "http", "method": "POST", "path": path, "raw_path": path.encode(), "query_string": b"", "headers": [], "server": ("test", 80), "client": ("test", 1)}, receive, send)
    assert messages[0]["status"] == 401
    assert app.state.runtime.calls == 0 and not database.parent.exists()
    assert app.state.service._creates_in_progress == 2


@pytest.mark.anyio
async def test_correct_key_business_works_without_key_in_business_state(caplog):
    """Management routing succeeds without forwarding the key into Service or Runtime."""
    app = application(api_key=KEY)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + KEY}) as client:
        assert (await client.get("/readyz")).status_code == 200
        created = await client.post("/v1/sandboxes", json={"environment": "python"})
        assert created.status_code == 201
        sandbox_id = created.json()["sandbox_id"]
        assert (await client.get(f"/v1/sandboxes/{sandbox_id}")).status_code == 200
        assert (await client.get(f"/v1/sandboxes/{sandbox_id}/endpoint")).status_code == 200
        assert (await client.get("/unknown")).status_code == 404
        assert (await client.delete(f"/v1/sandboxes/{sandbox_id}")).status_code == 204
    for value in (created.text, repr(vars(app.state.runtime)), repr(vars(app.state.service)), repr(vars(app.state.lifecycle_store)), repr(app.user_middleware), caplog.text):
        assert KEY not in value


@pytest.mark.anyio
@pytest.mark.parametrize("development", [False, True])
async def test_documentation_urls_are_disabled(development):
    """No schema or UI route is reachable even after valid authentication."""
    app = application(allow_unauthenticated=True) if development else application(api_key=KEY)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + KEY}) as client:
        for path in ("/docs", "/redoc", "/openapi.json"):
            assert (await client.get(path)).status_code == 404
    assert app.openapi_url is None and app.docs_url is None and app.redoc_url is None


@pytest.mark.anyio
async def test_missing_key_aborts_before_lock_database_or_background(tmp_path, monkeypatch):
    """An unconfigured production lifespan fails before any persistent or scheduled work."""
    database = tmp_path / "not-created" / "lifecycle.sqlite3"
    app = create_app(FakeRuntime(), database_path=database)

    def forbidden(*args, **kwargs):
        """Reject startup work before authentication configuration is validated."""
        pytest.fail("startup work preceded credential check")

    monkeypatch.setattr(app_module.SQLiteRunLock, "acquire", forbidden)
    monkeypatch.setattr(app_module.asyncio, "create_task", forbidden)
    with pytest.raises(RuntimeError, match="API key"):
        async with app.router.lifespan_context(app):
            pytest.fail("unconfigured startup must fail")
    assert not database.parent.exists() and app.state.runtime.calls == 0
    assert app.state.reaper_task is None


@pytest.mark.anyio
@pytest.mark.parametrize("configuration", [{"api_key": KEY}, {"allow_unauthenticated": True}])
async def test_explicit_configuration_allows_normal_lifespan(configuration):
    """Each deliberate auth mode preserves explicit initialization of the injected Store."""
    app = application(**configuration)
    async with app.router.lifespan_context(app):
        assert app.state.lifecycle_store.initialize_calls == 1
    assert app.state.runtime.calls == 0


@pytest.mark.anyio
async def test_bounded_comparison_uses_compare_digest(monkeypatch):
    """Verify use of the bounded constant-time primitive, without timing measurements."""
    original = hmac.compare_digest
    observations = []

    def compare(first, second):
        """Record only value types and lengths while retaining real digest semantics."""
        observations.append((type(first), len(first), type(second), len(second)))
        return original(first, second)

    monkeypatch.setattr(hmac, "compare_digest", compare)
    app = application(api_key=KEY)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/readyz", headers={"Authorization": "Bearer " + KEY})).status_code == 200
        assert (await client.get("/readyz", headers={"Authorization": "Bearer " + "x" * 129})).status_code == 401
    assert observations == [(bytes, len(KEY), bytes, len(KEY))]


def test_unconfigured_module_import_has_no_external_io():
    """Module import stays safe without generating keys, touching SQLite or connecting Docker."""
    code = '''
import os
os.environ.pop("AGENT_SANDBOX_API_KEY", None)
import asyncio
import sqlite3
import docker
from pathlib import Path

def forbidden(*args, **kwargs):
    """Reject any import-time filesystem, worker or Docker side effect."""
    raise AssertionError("unexpected external side effect")

asyncio.create_task = forbidden
sqlite3.connect = forbidden
docker.from_env = forbidden
Path.mkdir = forbidden
from control_plane.app import app
assert app.docs_url is None
'''
    result = subprocess.run([sys.executable, "-c", code], cwd=Path(__file__).resolve().parents[2], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr


@pytest.mark.anyio
async def test_authorized_workspace_uses_sqlite_without_persisting_key(tmp_path):
    """Real workspace metadata is created and removed while the admin key stays outside SQLite."""
    from test_workspace_api import WorkspaceRuntime

    database = tmp_path / "lifecycle.sqlite3"
    runtime = WorkspaceRuntime()
    app = create_app(runtime, api_key=KEY, database_path=database, reaper_enabled=False)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={"Authorization": "Bearer " + KEY}) as client:
            result = await client.post("/v1/workspaces", json={})
            assert result.status_code == 201
            workspace_id = result.json()["workspace_id"]
            assert (await client.get(f"/v1/workspaces/{workspace_id}")).status_code == 200
            assert (await client.delete(f"/v1/workspaces/{workspace_id}")).status_code == 204
    assert KEY.encode() not in database.read_bytes()
    assert KEY not in repr(runtime.workspace_calls)
