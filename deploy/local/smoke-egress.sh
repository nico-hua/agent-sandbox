#!/usr/bin/env sh
set -eu

if [ "${RUN_EGRESS_DOCKER_SMOKE:-}" != 1 ]; then
  printf '%s\n' 'Explicit opt-in required: RUN_EGRESS_DOCKER_SMOKE=1 sh deploy/local/smoke-egress.sh [public_ipv4]' >&2
  exit 2
fi
for tool in python3 docker; do
  command -v "$tool" >/dev/null 2>&1 || { printf 'required tool unavailable: %s\n' "$tool" >&2; exit 2; }
done
exec python3 - "$@" <<'PYTHON'
"""Run an isolated Docker/public-network smoke; never alter existing sandbox resources."""
import http.client
import ipaddress
import json
import re
import signal
import subprocess
import sys
import time
import uuid


class SmokeError(Exception):
    """Expose a fixed smoke failure reason without raw Docker or HTTP responses."""


# Executed inside the existing Python preset image, with explicit clients only.
CLIENT = r'''
import http.client, json, os, socket, ssl, sys
for key in list(os.environ):
    if key.lower() in ("http_proxy", "https_proxy", "all_proxy", "no_proxy"):
        del os.environ[key]
mode, proxy, domain, public_ip = sys.argv[1:]
connection = None
result = {"ok": False}
try:
    if mode == "tcp":
        connection = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connection.settimeout(3)
        connection.connect((public_ip, 443))
        result = {"ok": True}
    else:
        secure = mode.endswith("https")
        direct = mode.startswith("direct-")
        if secure:
            connection = http.client.HTTPSConnection(domain if direct else proxy, 443 if direct else 18084,
                timeout=8, context=ssl.create_default_context())
            if not direct:
                connection.set_tunnel(domain, 443)
            path = "/"
        else:
            connection = http.client.HTTPConnection(domain if direct else proxy, 80 if direct else 18084, timeout=8)
            path = "/" if direct else "http://" + domain + "/"
        connection.request("GET", path, headers={"Host": domain, "Connection": "close"})
        response = connection.getresponse()
        body = response.read(65537)
        if len(body) > 65536:
            result = {"ok": False, "reason": "response_too_large"}
        else:
            code = None
            try:
                code = json.loads(body).get("error", {}).get("code")
            except (ValueError, AttributeError):
                pass
            result = {"ok": 200 <= response.status < 300, "status": response.status, "code": code,
                      "reason": "http_status" if response.status >= 300 else "ok"}
except ssl.SSLCertVerificationError:
    result = {"ok": False, "reason": "certificate_verification"}
except socket.gaierror:
    result = {"ok": False, "reason": "dns_unavailable"}
except (TimeoutError, socket.timeout):
    result = {"ok": False, "reason": "timeout" if mode != "tcp" else "blocked"}
except (OSError, http.client.HTTPException):
    result = {"ok": False, "reason": "network_or_proxy_failure" if mode != "tcp" else "blocked"}
finally:
    if connection is not None:
        connection.close()
print(json.dumps(result))
'''


class Smoke:
    """Own exact IDs, bounded Docker operations and full-label cleanup for one invocation."""

    def __init__(self):
        """Allocate an opaque run identity without accessing Docker."""
        self.run_id = uuid.uuid4().hex
        self.resources = []
        self.intents = []
        self.stopping = False
        self.cleaning = False
        self.uncertain = False

    def labels(self, role):
        """Return all ownership labels; these do not enroll resources into the control plane."""
        prefix = "io.agent-sandbox.egress-smoke."
        return {prefix + "managed": "true", prefix + "project": "agent-sandbox",
                prefix + "run-id": self.run_id, prefix + "role": role}

    def owned(self, data, kind, identifier, role):
        """Authorize only the exact recorded ID with every expected label intact."""
        labels = data.get("Config", {}).get("Labels") if kind == "container" else data.get("Labels")
        return isinstance(identifier, str) and re.fullmatch("[0-9a-f]{64}", identifier) is not None and data.get("Id") == identifier and isinstance(labels, dict) and all(
            labels.get(key) == value for key, value in self.labels(role).items())

    def docker(self, *arguments, timeout=20, check=True, input=None, accept_signal=False):
        """Run a finite CLI call; cancellation waits for its result before resource compensation."""
        if self.stopping and not self.cleaning and not accept_signal:
            raise SmokeError("interrupted")
        try:
            result = subprocess.run(["docker", *arguments], input=input, text=True,
                                    capture_output=True, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise SmokeError("docker_timeout") from None
        except OSError:
            raise SmokeError("docker_cli_unavailable") from None
        if check and result.returncode:
            raise SmokeError("docker_operation_failed")
        return result

    def inspect(self, kind, identifier, missing=False):
        """Distinguish an absent exact resource from an unavailable Docker backend."""
        result = self.docker(kind, "inspect", identifier, timeout=5, check=False)
        if result.returncode:
            if missing and any(text in result.stderr for text in ("No such container", "No such network", "No such object")):
                return None
            raise SmokeError("inspect_unavailable")
        try:
            return json.loads(result.stdout)[0]
        except (ValueError, IndexError, TypeError):
            raise SmokeError("inspect_invalid") from None

    def create(self, kind, role, *options, image=None, command=()):
        """Record creation intent before I/O, then register the actual returned ID before honoring a signal."""
        name = "egress-smoke-" + self.run_id + "-" + role
        self.intents.append((kind, name, role))
        labels = [argument for pair in self.labels(role).items() for argument in ("--label", "=".join(pair))]
        arguments = [kind, "create", *options, *labels]
        if kind == "container":
            arguments += ["--name", name, image, *command]
        else:
            arguments += [name]
        try:
            result = self.docker(*arguments, timeout=30)
        except SmokeError:
            self.uncertain = True
            raise
        identifier = result.stdout.strip()
        if not re.fullmatch("[0-9a-f]{64}", identifier):
            self.uncertain = True
            raise SmokeError("creation_id_unknown")
        self.resources.append((kind, identifier, role))
        print("CREATED", kind, role, identifier, flush=True)
        if self.stopping:
            raise SmokeError("interrupted")
        return identifier

    def cleanup(self):
        """Reconcile exact creation names, then verify and remove owned IDs in reverse dependency order."""
        self.cleaning = True
        complete = not self.uncertain
        # A canceled/timed-out CLI can lose its response after daemon-side creation.
        # Discover only our exact intent names, never list/prune by a broad prefix.
        for kind, name, role in self.intents:
            try:
                data = self.inspect(kind, name, missing=True)
                if data is None:
                    continue
                identifier = data.get("Id", "")
                if not self.owned(data, kind, identifier, role):
                    raise SmokeError("ownership_conflict")
                if (kind, identifier, role) not in self.resources:
                    self.resources.append((kind, identifier, role))
            except SmokeError:
                complete = False
                print("CLEANUP unresolved creation:", kind, name, file=sys.stderr)
        # Reconciled containers can be discovered after networks; preserve dependencies.
        ordered = [r for r in reversed(self.resources) if r[0] == "container"]
        ordered += [r for r in reversed(self.resources) if r[0] == "network"]
        for kind, identifier, role in ordered:
            try:
                data = self.inspect(kind, identifier, missing=True)
                if data is None:
                    continue
                if not self.owned(data, kind, identifier, role):
                    raise SmokeError("ownership_conflict")
                arguments = ("container", "rm", "--force", identifier) if kind == "container" else ("network", "rm", identifier)
                self.docker(*arguments, timeout=15)
                print("CLEANED", kind, role, identifier, flush=True)
            except SmokeError as error:
                complete = False
                print("CLEANUP residual ID:", kind, identifier, str(error), file=sys.stderr)
        if self.uncertain:
            print("Creation result was ambiguous; verify this run-id before assuming cleanup complete:", self.run_id, file=sys.stderr)
        return complete

    def client(self, container, mode, proxy="egress", domain="example.com", public_ip="1.1.1.1"):
        """Run bounded explicit HTTP/TLS or raw IPv4 TCP checks without environment proxy fallback."""
        result = self.docker("exec", "-i", container, "python3", "-B", "-", mode, proxy, domain, public_ip,
                             input=CLIENT, timeout=25)
        try:
            data = json.loads(result.stdout)
            if type(data.get("ok")) is not bool:
                raise ValueError()
            return data
        except (ValueError, AttributeError):
            raise SmokeError("client_result_invalid") from None

    def wait_healthy(self, identifier):
        """Wait a finite startup interval; health remains a local process check only."""
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            data = self.inspect("container", identifier)
            if not data["State"]["Running"]:
                raise SmokeError("proxy_exited_before_health")
            if data["State"].get("Health", {}).get("Status") == "healthy":
                return
            time.sleep(0.2)
        raise SmokeError("health_deadline")

    def verify_container(self, identifier, role, networks, user=None):
        """Inspect actual network IDs, restrictions, mounts and port bindings before making requests."""
        data = self.inspect("container", identifier)
        host, config = data["HostConfig"], data["Config"]
        facts = {entry["NetworkID"] for entry in data["NetworkSettings"]["Networks"].values()}
        valid = self.owned(data, "container", identifier, role) and facts == set(networks)
        valid &= (host["ReadonlyRootfs"] and not host["Privileged"] and host["NanoCpus"] == 500000000
                  and host["Memory"] == 134217728 and host["MemorySwap"] == 134217728
                  and host["PidsLimit"] == 32 and host["CapDrop"] == ["ALL"]
                  and not host.get("CapAdd") and "no-new-privileges:true" in host["SecurityOpt"]
                  and not data.get("Mounts") and not host.get("PortBindings")
                  and all(not binding for binding in data["NetworkSettings"].get("Ports", {}).values())
                  and not host.get("Binds") and not host.get("Devices"))
        valid &= config["User"].split(":")[0] not in ("", "root", "0")
        if user is not None:
            valid &= config["User"] == user
        if not valid:
            raise SmokeError("container_configuration_mismatch")
        if role in ("egress", "deny"):
            test = config.get("Healthcheck", {}).get("Test", [])
            if "off" not in test or "http://127.0.0.1:18084/healthz" not in test:
                raise SmokeError("healthcheck_not_local")
            uid = self.docker("exec", identifier, "id", "-u", timeout=5).stdout.strip()
            if uid != "10001":
                raise SmokeError("proxy_running_as_wrong_user")

    def terminate(self, identifier, role):
        """Send only SIGTERM, without Docker's forced-kill fallback, and require bounded clean exit."""
        data = self.inspect("container", identifier)
        if not self.owned(data, "container", identifier, role):
            raise SmokeError("ownership_conflict")
        self.docker("kill", "--signal=TERM", identifier, timeout=5)
        result = self.docker("wait", identifier, timeout=8)
        if result.stdout.strip() != "0":
            raise SmokeError("proxy_shutdown_failed")
        print("PASS SIGTERM exit", role, identifier, flush=True)

    @staticmethod
    def public_complete(control, isolated):
        """Count isolation only when a successful same-address control precedes a blocked direct attempt."""
        return control.get("ok") is True and isolated.get("ok") is False and isolated.get("reason") == "blocked"

    def run(self, public_ip):
        """Exercise deny, public HTTP/TLS, direct control and stopped-proxy behavior in a private topology."""
        self.docker("version", "--format", "{{.Server.Version}}", timeout=10)
        images = []
        for tag in ("agent-sandbox-egress:dev", "agent-sandbox-python:dev"):
            result = self.docker("image", "inspect", tag, "--format", "{{.Id}}", timeout=5)
            if not re.fullmatch("sha256:[0-9a-f]{64}", result.stdout.strip()):
                raise SmokeError("required_image_unavailable")
            images.append(result.stdout.strip())
        # No pulls or implicit builds, and tags cannot change the images selected below.
        egress_image, python_image = images
        internal = self.create("network", "internal", "--driver", "bridge", "--internal")
        outbound = self.create("network", "outbound", "--driver", "bridge")
        for identifier, role, is_internal in ((internal, "internal", True), (outbound, "outbound", False)):
            data = self.inspect("network", identifier)
            if not self.owned(data, "network", identifier, role) or data["Driver"] != "bridge" or data["Internal"] is not is_internal:
                raise SmokeError("network_configuration_mismatch")
        constraints = ("--pull=never", "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges:true",
                       "--cpus=0.5", "--memory=128m", "--memory-swap=128m", "--pids-limit=32")
        sandbox = self.create("container", "sandbox", *constraints, "--network", internal, "--entrypoint", "python3",
                              image=python_image, command=("-B", "-c", "import time; time.sleep(600)"))
        control = self.create("container", "control", *constraints, "--network", outbound, "--entrypoint", "python3",
                              image=python_image, command=("-B", "-c", "import time; time.sleep(600)"))
        for identifier, role, network in ((sandbox, "sandbox", internal), (control, "control", outbound)):
            self.docker("start", identifier)
            self.verify_container(identifier, role, (network,))
        # Deliberately poison proxy environment variables: the local healthcheck must ignore them.
        environment = tuple(argument for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
                            for argument in ("--env", key + "=http://127.0.0.1:9")) + ("--env", "NO_PROXY=", "--env", "no_proxy=")
        proxies = []
        for role, alias, command in (("deny", "deny-egress", ()), ("egress", "egress", ("-allow-domain", "example.com"))):
            proxy = self.create("container", role, *constraints, *environment, "--network", outbound,
                                image=egress_image, command=command)
            self.docker("network", "connect", "--alias", alias, internal, proxy)
            self.docker("start", proxy)
            self.verify_container(proxy, role, (internal, outbound), user="10001:10001")
            self.wait_healthy(proxy)
            if role == "deny":
                denied = self.client(sandbox, "http", proxy=alias)
                if denied.get("status") != 403 or denied.get("code") != "egress_policy_denied":
                    raise SmokeError("empty_allowlist_not_denied")
                self.terminate(proxy, role)
                print("PASS empty allowlist and proxy-independent local health", flush=True)
            proxies.append(proxy)
        proxy = proxies[-1]
        for domain in ("not-allowed.invalid",):
            denied = self.client(sandbox, "http", domain=domain)
            if denied.get("status") != 403 or denied.get("code") != "egress_policy_denied":
                raise SmokeError("unlisted_domain_not_denied")
        print("PASS configuration, topology, permissions and policy rejection", flush=True)
        public_ok = True
        control_tcp = self.client(control, "tcp", public_ip=public_ip)
        if control_tcp["ok"]:
            isolated = self.client(sandbox, "tcp", public_ip=public_ip)
            if not self.public_complete(control_tcp, isolated):
                raise SmokeError("sandbox_direct_tcp_isolation_failed")
            print("PASS ordinary bridge reaches", public_ip + ":443; internal sandbox direct TCP blocked", flush=True)
        else:
            public_ok = False
            print("UNVERIFIED 公网链路未完成验证: ordinary bridge TCP control", control_tcp.get("reason"), flush=True)
        for protocol in ("http", "https"):
            baseline = self.client(control, "direct-" + protocol)
            proxied = self.client(sandbox, protocol)
            if not baseline["ok"]:
                public_ok = False
                print("UNVERIFIED public", protocol, "environment control:", baseline.get("reason"), baseline.get("status"), flush=True)
            if not proxied["ok"]:
                public_ok = False
                print("UNVERIFIED proxied", protocol, ":", proxied.get("reason"), proxied.get("code"), proxied.get("status"), flush=True)
            if baseline["ok"] and proxied["ok"]:
                print("PASS real public", protocol, "via explicit proxy (certificate verification enabled)", flush=True)
        self.terminate(proxy, "egress")
        for protocol in ("http", "https"):
            stopped = self.client(sandbox, protocol)
            if stopped["ok"] or stopped.get("status") is not None or stopped.get("reason") not in ("network_or_proxy_failure", "dns_unavailable", "timeout"):
                raise SmokeError("stopped_proxy_request_did_not_fail")
        if control_tcp["ok"] and not self.public_complete(control_tcp, self.client(sandbox, "tcp", public_ip=public_ip)):
            raise SmokeError("direct_tcp_after_proxy_stop_failed")
        print("PASS stopped proxy has no explicit-client direct fallback", flush=True)
        if not public_ok:
            raise SmokeError("公网链路未完成验证; local checks passed, overall acceptance not passed")


def main():
    """Install signal-aware cleanup and never report overall success before all resources are removed."""
    smoke = Smoke()
    if len(sys.argv) > 2:
        print("usage: smoke-egress.sh [public_ipv4]", file=sys.stderr)
        return 2
    public_ip = sys.argv[1] if len(sys.argv) == 2 else "1.1.1.1"
    try:
        address = ipaddress.IPv4Address(public_ip)
        if not address.is_global or address.is_multicast:
            raise ValueError()
    except ValueError:
        print("public_ipv4 must be a global IPv4 address", file=sys.stderr)
        return 2
    def stop(signum, frame):
        """Remember interruption without abandoning an in-flight Docker result or critical cleanup."""
        smoke.stopping = True
    for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, stop)
    status = 0
    try:
        smoke.run(public_ip)
    except SmokeError as error:
        print("FAIL", str(error), file=sys.stderr)
        status = 1
    except Exception:
        print("FAIL unexpected smoke failure; inspect recorded resource IDs", file=sys.stderr)
        status = 1
    finally:
        if not smoke.cleanup():
            status = 1
    if smoke.stopping:
        status = 130
    if status == 0:
        print("PASS standalone Docker egress smoke (HTTP, HTTPS, direct TCP control and cleanup)", flush=True)
    return status


if __name__ == "__main__":
    sys.exit(main())
PYTHON
