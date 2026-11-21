"""Exercise the production proxy renderer offline with process-level tool stubs."""

import json
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

import pytest


LOCAL = Path(__file__).resolve().parents[3] / "deploy" / "local"
TOKEN = "A" * 43
FAILURE = "proxy authentication configuration failed\n"


def run_entrypoint(tmp_path, overrides=None, *, nginx_fails=False):
    """Run an unchanged entrypoint except its template path and collect private renders."""
    script = tmp_path / "entrypoint.sh"
    script.write_text((LOCAL / "proxy-entrypoint.sh").read_text().replace(
        "/etc/nginx/proxy.conf.template", shlex.quote(str(LOCAL / "proxy.conf.template"))
    ))
    tools = tmp_path / "bin"
    tools.mkdir()
    capture = tmp_path / "capture.jsonl"
    subst = tools / "envsubst"
    subst.write_text(f"#!{sys.executable}\n" + '''"""Substitute only explicitly selected environment variables, like envsubst."""
import json, os, pathlib, re, sys
source = sys.stdin.read()
allowed = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", sys.argv[1]))
pathlib.Path(os.environ["SUBST_CAPTURE"]).write_text(json.dumps(sorted(allowed)))
sys.stdout.write(re.sub(r"\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}|\\$([A-Za-z_][A-Za-z0-9_]*)", lambda match: os.environ.get(match[1] or match[2], "") if (match[1] or match[2]) in allowed else match[0], source))
''')
    nginx = tools / "nginx"
    nginx.write_text(f"#!{sys.executable}\n" + '''"""Record rendered config, modes and inherited credentials for both nginx calls."""
import json, os, pathlib, stat, sys
config = pathlib.Path(sys.argv[sys.argv.index("-c") + 1])
record = {"args": sys.argv[1:], "config": config.read_text(), "path": str(config), "mode": stat.S_IMODE(config.stat().st_mode), "dir_mode": stat.S_IMODE(config.parent.stat().st_mode), "has_token": "SANDBOX_ACCESS_TOKEN" in os.environ}
with open(os.environ["NGINX_CAPTURE"], "a") as output:
    output.write(json.dumps(record) + "\\n")
if "-t" in sys.argv and os.environ.get("NGINX_FAIL"):
    print(config.read_text(), file=sys.stderr)
    sys.exit(1)
''')
    subst.chmod(0o700)
    nginx.chmod(0o700)
    environment = {
        "PATH": f"{tools}:/usr/bin:/bin", "SANDBOX_PROXY_AUTH_VERSION": "1",
        "SANDBOX_ACCESS_TOKEN": TOKEN, "NGINX_CAPTURE": str(capture),
        "SUBST_CAPTURE": str(tmp_path / "substitutions.json"),
        "request_uri": "CORRUPTED", "fixed_upstream": "CORRUPTED",
    }
    environment.update(overrides or {})
    if nginx_fails:
        environment["NGINX_FAIL"] = "1"
    result = subprocess.run(["/bin/sh", str(script)], env=environment,
                            capture_output=True, text=True, timeout=10)
    records = [json.loads(line) for line in capture.read_text().splitlines()] if capture.exists() else []
    for directory in {str(Path(record["path"]).parent) for record in records}:
        shutil.rmtree(directory)
    return result, records


def test_default_proxy_preserves_docker_routing_and_nginx_variables(tmp_path):
    """The proxy uses Docker DNS, service routing and the Agent buffering policy."""
    result, records = run_entrypoint(tmp_path)
    assert result.returncode == 0 and not result.stdout and not result.stderr
    assert len(records) == 2 and "-t" in records[0]["args"]
    config = records[-1]["config"]
    assert "resolver 127.0.0.11 valid=5s ipv6=off;" in config
    assert config.count("set $fixed_upstream sandbox:8080;") == 3
    assert '"sandbox:$app_port$app_request_uri"' in config
    assert '$request_method:$request_uri' in config and "$deny_instance" in config
    assert "proxy_pass http://$fixed_upstream;" in config
    assert "proxy_request_buffering off;" in config and "proxy_buffering off;" in config
    assert f"(?i:Bearer) {TOKEN}$" in config
    assert "CORRUPTED" not in config and "${SANDBOX_ACCESS_TOKEN}" not in config
    assert all(record["mode"] == 0o600 and record["dir_mode"] == 0o700 for record in records)
    assert all(not record["has_token"] for record in records)


@pytest.mark.parametrize("overrides", [
    {"SANDBOX_PROXY_AUTH_VERSION": ""}, {"SANDBOX_PROXY_AUTH_VERSION": "2"},
    {"SANDBOX_ACCESS_TOKEN": ""}, {"SANDBOX_ACCESS_TOKEN": "A" * 42},
    {"SANDBOX_ACCESS_TOKEN": "A" * 44}, {"SANDBOX_ACCESS_TOKEN": ";" * 43},
    {"SANDBOX_ACCESS_TOKEN": "é" * 43},
])
def test_proxy_authentication_still_fails_closed(tmp_path, overrides):
    """The proxy enforces the exact v1 credential validation."""
    result, records = run_entrypoint(tmp_path, overrides)
    assert result.returncode != 0 and result.stderr == FAILURE
    assert not result.stdout and not records


def test_nginx_validation_failure_suppresses_private_configuration(tmp_path):
    """A rejected nginx config is never echoed and nginx is never started afterward."""
    result, records = run_entrypoint(tmp_path, nginx_fails=True)
    assert result.returncode != 0 and result.stderr == FAILURE and not result.stdout
    assert len(records) == 1 and not records[0]["has_token"]


def test_environment_cannot_override_internal_rendering_values(tmp_path):
    """Ambient settings cannot substitute into the fixed DNS or nginx variables."""
    injection = "untrusted; include /tmp/injected.conf;"
    overrides = {name: injection for name in (
        "PROXY_AGENT_AUTHORITY", "PROXY_SERVICE_HOST", "PROXY_RESOLVER", "PROXY_SERVICES_GUARD",
        "PROXY_CONNECT_TIMEOUT",
    )}
    result, records = run_entrypoint(tmp_path, overrides)
    assert result.returncode == 0
    config = records[-1]["config"]
    assert injection not in config and "${PROXY_" not in config
    assert "$request_uri" in config and "$app_port$app_request_uri" in config
    substitutions = json.loads((tmp_path / "substitutions.json").read_text())
    assert "request_uri" not in substitutions and "fixed_upstream" not in substitutions


def test_connect_timeout_is_specific_to_application_forwarding(tmp_path):
    """Application ports use the bounded connect deadline and Agent routes retain their default."""
    result, records = run_entrypoint(tmp_path)
    assert result.returncode == 0
    config = records[-1]["config"]
    service_settings = config.split("location ^~ /services/ {", 1)[1].split("location ", 1)[0]
    assert "proxy_connect_timeout 2s;" in service_settings
    assert config.count("proxy_connect_timeout 2s;") == 1


def test_urlsafe_token_characters_remain_valid(tmp_path):
    """The original 43-byte base64url credential alphabet remains accepted."""
    token = "aZ0_-" * 8 + "abc"
    result, records = run_entrypoint(tmp_path, {"SANDBOX_ACCESS_TOKEN": token})
    assert result.returncode == 0
    assert f"(?i:Bearer) {token}$" in records[-1]["config"]
    assert all(not record["has_token"] for record in records)
