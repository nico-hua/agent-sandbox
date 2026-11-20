"""Verify trusted Docker settings and side-effect-free application assembly."""

import os
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from control_plane.app import create_app
from control_plane.config import settings as config
from control_plane.runtime.docker_runtime import DockerRuntime


@pytest.mark.anyio
async def test_dotenv_controls_docker_assembly_and_authentication(monkeypatch, tmp_path):
    """Read a trusted file before assembling the runtime, middleware and temporary store."""
    env_file = tmp_path / ".env"
    database_path = tmp_path / "dotenv.sqlite3"
    env_file.write_text(
        "# 控制面配置\n"
        "AGENT_SANDBOX_RUNTIME=docker\n"
        "AGENT_SANDBOX_AUTH_ENABLED=true\n"
        "AGENT_SANDBOX_API_KEY=dotenv-test-key\n"
        "AGENT_SANDBOX_REAPER_ENABLED=false\n"
        f'AGENT_SANDBOX_DATABASE_PATH="{database_path.as_posix()}"\n'
        'AGENT_SANDBOX_ADDITIONAL_ENVIRONMENTS=\'{"tools": "agent-sandbox-tools:dev"}\'\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "ENV_FILE", env_file, raising=False)
    app = create_app()
    assert isinstance(app.state.runtime, DockerRuntime)
    assert app.state.runtime.environment_images["tools"] == "agent-sandbox-tools:dev"
    assert app.state.lifecycle_store.database_path == database_path
    assert os.environ.get("AGENT_SANDBOX_API_KEY") != "dotenv-test-key"
    async with app.router.lifespan_context(app):
        assert app.state.reaper_task is None
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            assert (await client.get("/unknown")).status_code == 401
            headers = {"Authorization": "Bearer dotenv-test-key"}
            assert (await client.get("/unknown", headers=headers)).status_code == 404


@pytest.mark.anyio
async def test_factory_uses_dotenv_only_and_defaults_to_anonymous(monkeypatch, tmp_path):
    """A blank file uses Docker and anonymous routing despite ambient process settings."""
    env_file = tmp_path / ".env"
    env_file.write_text("AGENT_SANDBOX_AUTH_ENABLED=\n", encoding="utf-8")
    monkeypatch.setattr(config, "ENV_FILE", env_file)
    monkeypatch.setenv("AGENT_SANDBOX_RUNTIME", "k8s")
    monkeypatch.setenv("AGENT_SANDBOX_AUTH_ENABLED", "true")
    app = create_app()
    assert isinstance(app.state.runtime, DockerRuntime)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get("/unknown")).status_code == 404


def test_factory_requires_no_python_options():
    """Keep configuration and dependency construction inside the public factory."""
    with pytest.raises(TypeError):
        create_app(reaper_enabled=False)


def test_dotenv_rejects_unavailable_runtime(monkeypatch, tmp_path):
    """Reject an unsupported runtime choice before attempting to use Docker."""
    env_file = tmp_path / ".env"
    env_file.write_text("AGENT_SANDBOX_RUNTIME=k8s\n", encoding="utf-8")
    monkeypatch.setattr(config, "ENV_FILE", env_file, raising=False)
    with pytest.raises(ValueError, match="only supports docker"):
        create_app()


def test_dotenv_defaults_and_types(monkeypatch, tmp_path):
    """Parse file settings and apply defaults without consulting the process environment."""
    env_file = tmp_path / ".env"
    env_file.write_text(
        "AGENT_SANDBOX_RUNTIME=\n"
        'AGENT_SANDBOX_DATABASE_PATH="data with spaces/store.sqlite3"\n'
        "AGENT_SANDBOX_READINESS_TIMEOUT=2.5\n"
        "AGENT_SANDBOX_SANDBOX_QUERY_TIMEOUT=3.5\n"
        "AGENT_SANDBOX_REAPER_INTERVAL_SECONDS=30\n"
        "AGENT_SANDBOX_REAPER_BATCH_LIMIT=20\n"
        "AGENT_SANDBOX_PENDING_RECOVERY_ENABLED=TRUE\n"
        "AGENT_SANDBOX_PENDING_RECOVERY_BATCH_LIMIT=15\n"
        "AGENT_SANDBOX_MAX_CONCURRENT_CREATES=4\n"
        "AGENT_SANDBOX_MAX_INSTANCES=35\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(config, "ENV_FILE", env_file)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("AGENT_SANDBOX_MAX_INSTANCES", "45")
    monkeypatch.setenv("AGENT_SANDBOX_MAX_CONCURRENT_CREATES", " ")
    settings = config.ControlPlaneSettings.from_env()
    assert settings.runtime == "docker"
    assert settings.database_path == config.CONTROL_PLANE_DIR / "data with spaces" / "store.sqlite3"
    assert settings.readiness_timeout == 2.5
    assert settings.sandbox_query_timeout == 3.5
    assert settings.reaper_interval_seconds == 30.0
    assert settings.reaper_batch_limit == 20
    assert settings.pending_recovery_enabled is True
    assert settings.pending_recovery_batch_limit == 15
    assert settings.max_concurrent_creates == 4
    assert settings.max_instances == 35


@pytest.mark.parametrize("variable,value", [
    ("AUTH_ENABLED", "maybe"), ("MAX_INSTANCES", "7.5"),
    ("ADDITIONAL_ENVIRONMENTS", "[]"), ("ADDITIONAL_ENVIRONMENTS", "invalid-json"),
])
def test_invalid_dotenv_values_report_field_without_echoing_input(monkeypatch, tmp_path, variable, value):
    """Malformed typed values produce a fixed error identifying the setting."""
    name = f"AGENT_SANDBOX_{variable}"
    env_file = tmp_path / ".env"
    env_file.write_text(f"{name}={value}\n", encoding="utf-8")
    monkeypatch.setattr(config, "ENV_FILE", env_file)
    with pytest.raises(ValueError, match=f"^invalid {name}$"):
        config.ControlPlaneSettings.from_env()


def test_database_path_is_loaded_once_and_blank_uses_default(monkeypatch, tmp_path):
    """An existing app keeps its database path across later file edits."""
    env_file = tmp_path / ".env"
    monkeypatch.setattr(config, "ENV_FILE", env_file)
    assert create_app().state.lifecycle_store.database_path == config.DEFAULT_DATABASE_PATH
    configured = tmp_path / "configured.sqlite3"
    env_file.write_text(f'AGENT_SANDBOX_DATABASE_PATH="{configured.as_posix()}"\n', encoding="utf-8")
    app = create_app()
    env_file.write_text("AGENT_SANDBOX_DATABASE_PATH=\n", encoding="utf-8")
    assert app.state.lifecycle_store.database_path == configured
    assert create_app().state.lifecycle_store.database_path == config.DEFAULT_DATABASE_PATH
    assert not configured.exists()


def test_fresh_import_with_configuration_defers_all_external_state(tmp_path, isolated_subprocess_startup):
    """The public module's eagerly assembled app must remain inert."""
    database = tmp_path / "absent" / "lifecycle.sqlite3"
    (tmp_path / ".env").write_text(
        f'AGENT_SANDBOX_DATABASE_PATH="{database.as_posix()}"\n', encoding="utf-8",
    )
    script = '''
from pathlib import Path
import asyncio
import sqlite3
import subprocess
import sys
import docker
def forbidden(*args, **kwargs):
    """Reject filesystem/client/process side effects during configured module import."""
    raise AssertionError("unexpected assembly I/O")
original_open = Path.open
database_directory = Path(sys.argv[1]).parent
def guarded_open(path, *args, **kwargs):
    """Allow dependency metadata reads while guarding the application's storage path."""
    if path.is_relative_to(database_directory):
        forbidden()
    return original_open(path, *args, **kwargs)
Path.open = guarded_open
sqlite3.connect = forbidden
docker.from_env = forbidden
subprocess.Popen = forbidden
from control_plane.app import app
from control_plane.runtime.docker_runtime import DockerRuntime
assert isinstance(app.state.runtime, DockerRuntime)
assert app.state.reaper_task is None
assert app.state.lifecycle_store.database_path == Path(sys.argv[1])
'''
    result = subprocess.run([sys.executable, "-c", isolated_subprocess_startup + script, str(database)],
                            cwd=Path(__file__).resolve().parents[3], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "absent").exists()
