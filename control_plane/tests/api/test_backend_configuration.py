"""Verify trusted Docker settings and side-effect-free application assembly."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from control_plane.app import create_app
from control_plane.runtime.docker_runtime import DockerRuntime
from control_plane.tests.fakes import FakeLifecycleStore


def test_docker_assembly_registers_trusted_environments():
    """Assemble Docker with the built-in and explicitly registered images."""
    app = create_app(auth_enabled=True, api_key="synthetic-config-key", store=FakeLifecycleStore(),
                     additional_environments={"tools": "agent-sandbox-tools:dev"})
    assert isinstance(app.state.runtime, DockerRuntime)
    assert set(app.state.runtime.environment_images) == {"basic", "python", "node", "tools"}



def test_docker_default_and_trusted_database_path_are_preserved(monkeypatch, tmp_path):
    """An explicit Store or path takes precedence over environment settings."""
    import control_plane.app as app_module
    monkeypatch.delenv("AGENT_SANDBOX_DATABASE_PATH", raising=False)
    docker_app = create_app(auth_enabled=True, api_key="synthetic-config-key")
    assert docker_app.state.lifecycle_store.database_path == app_module.DEFAULT_DATABASE_PATH
    configured = tmp_path / "configured.sqlite3"
    monkeypatch.setenv("AGENT_SANDBOX_DATABASE_PATH", str(configured))
    app = create_app(auth_enabled=True, api_key="synthetic-config-key")
    monkeypatch.setenv("AGENT_SANDBOX_DATABASE_PATH", "invalid\npath")
    assert app.state.lifecycle_store.database_path == configured
    explicit = tmp_path / "explicit.sqlite3"
    assert create_app(auth_enabled=True, api_key="synthetic-config-key", database_path=explicit).state.lifecycle_store.database_path == explicit
    fake = FakeLifecycleStore()
    assert create_app(auth_enabled=True, api_key="synthetic-config-key", store=fake).state.lifecycle_store is fake
    with pytest.raises(ValueError):
        create_app(auth_enabled=True, api_key="synthetic-config-key")
    assert list(tmp_path.iterdir()) == []



def test_fresh_import_with_configuration_defers_all_external_state(tmp_path):
    """The public module's eagerly assembled app must remain inert."""
    script = '''
from pathlib import Path
import os
import sqlite3
import subprocess
import docker
def forbidden(*args, **kwargs):
    """Reject filesystem/client/process side effects during configured module import."""
    raise AssertionError("unexpected assembly I/O")
original_open = Path.open
database_directory = Path(os.environ["AGENT_SANDBOX_DATABASE_PATH"]).parent
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
'''
    environment = dict(os.environ)
    environment.update({
        "AGENT_SANDBOX_DATABASE_PATH": str(tmp_path / "absent" / "lifecycle.sqlite3"),
    })
    result = subprocess.run([sys.executable, "-c", script], cwd=Path(__file__).resolve().parents[3],
                            env=environment, capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert not (tmp_path / "absent").exists()
