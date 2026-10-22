"""Verify the standalone smoke's ownership and failure paths without using Docker."""

from pathlib import Path
import contextlib
import io
import subprocess
import types
import unittest
from unittest.mock import patch


def load_smoke():
    """Load only the embedded Python definitions; never execute the Docker smoke."""
    script = Path(__file__).with_name("smoke-egress.sh").read_text()
    source = script.split("<<'PYTHON'\n", 1)[1].rsplit("\nPYTHON", 1)[0]
    module = types.ModuleType("smoke_egress_test")
    exec(compile(source, "smoke-egress.sh", "exec"), module.__dict__)
    return module


class SmokeSafetyTests(unittest.TestCase):
    """Exercise exact ownership, bounded subprocesses and incomplete public verification."""

    @classmethod
    def setUpClass(cls):
        """Load the smoke without starting any Docker client."""
        cls.module = load_smoke()

    def setUp(self):
        """Capture expected failure diagnostics so negative-path tests do not resemble real smoke errors."""
        self.output = io.StringIO()
        self.errors = io.StringIO()
        self.enterContext(contextlib.redirect_stdout(self.output))
        self.enterContext(contextlib.redirect_stderr(self.errors))

    def test_gate_never_calls_tools(self):
        """Require explicit opt-in before even checking Docker or Python availability."""
        script = Path(__file__).with_name("smoke-egress.sh")
        result = subprocess.run(["/bin/sh", str(script)], env={"PATH": "/nonexistent"}, capture_output=True, timeout=2)
        self.assertEqual(result.returncode, 2)
        self.assertIn(b"RUN_EGRESS_DOCKER_SMOKE=1", result.stderr)

    def test_ownership_checks_every_label_and_exact_id(self):
        """Never authorize cleanup using a name, partial labels or a different ID."""
        smoke = self.module.Smoke()
        labels = smoke.labels("sandbox")
        data = {"Id": "a" * 64, "Name": "/temporary", "Config": {"Labels": labels}}
        self.assertTrue(smoke.owned(data, "container", "a" * 64, "sandbox"))
        self.assertFalse(smoke.owned(data, "container", "b" * 64, "sandbox"))
        for key in labels:
            altered = {**data, "Config": {"Labels": {k: v for k, v in labels.items() if k != key}}}
            self.assertFalse(smoke.owned(altered, "container", "a" * 64, "sandbox"))
        self.assertFalse(smoke.owned(data, "container", "a" * 64, "egress"))

    def test_cleanup_conflict_keeps_resource_and_reports_id(self):
        """Refuse destructive calls when a recorded resource no longer passes ownership."""
        smoke = self.module.Smoke()
        smoke.resources = [("container", "a" * 64, "sandbox")]
        smoke.inspect = lambda *args, **kwargs: {"Id": "a" * 64, "Config": {"Labels": {}}}
        with patch.object(smoke, "docker") as docker:
            self.assertFalse(smoke.cleanup())
            docker.assert_not_called()
        self.assertIn("a" * 64, self.errors.getvalue())

    def test_cleanup_is_reverse_and_idempotent(self):
        """Remove containers before networks, only by fully checked IDs; missing is harmless."""
        smoke = self.module.Smoke()
        smoke.resources = [("network", "a" * 64, "internal"), ("container", "b" * 64, "sandbox")]
        def inspect(kind, identifier, **kwargs):
            """Return project-owned facts for each recorded resource."""
            role = "internal" if kind == "network" else "sandbox"
            labels = smoke.labels(role)
            return {"Id": identifier, "Config": {"Labels": labels}, "Labels": labels}
        smoke.inspect = inspect
        with patch.object(smoke, "docker", return_value=types.SimpleNamespace(returncode=0, stdout="")) as docker:
            self.assertTrue(smoke.cleanup())
            self.assertEqual(docker.call_args_list[0].args[0:3], ("container", "rm", "--force"))
            self.assertEqual(docker.call_args_list[1].args[0:2], ("network", "rm"))
        smoke.inspect = lambda *args, **kwargs: None
        with patch.object(smoke, "docker") as docker:
            self.assertTrue(smoke.cleanup())
            docker.assert_not_called()

    def test_public_incomplete_never_reports_success(self):
        """A failed ordinary-bridge control cannot become an isolation pass."""
        smoke = self.module.Smoke()
        self.assertFalse(smoke.public_complete({"ok": False, "reason": "timeout"}, {"ok": False, "reason": "blocked"}))
        self.assertFalse(smoke.public_complete({"ok": True}, {"ok": True}))
        self.assertTrue(smoke.public_complete({"ok": True}, {"ok": False, "reason": "blocked"}))

    def test_signal_during_create_keeps_returned_id(self):
        """An accepted creation is registered before cancellation can enter cleanup."""
        smoke = self.module.Smoke()
        def finish_creation(*args, **kwargs):
            """Deliver the daemon result after a simulated termination request."""
            smoke.stopping = True
            return types.SimpleNamespace(returncode=0, stdout="a" * 64)
        with patch.object(smoke, "docker", side_effect=finish_creation):
            with self.assertRaisesRegex(self.module.SmokeError, "interrupted"):
                smoke.create("network", "internal", "--driver", "bridge")
        self.assertEqual(smoke.resources, [("network", "a" * 64, "internal")])

    def test_inspect_failure_is_not_absence(self):
        """A broken backend must not masquerade as successful idempotent cleanup."""
        smoke = self.module.Smoke()
        with patch.object(smoke, "docker", return_value=types.SimpleNamespace(returncode=1, stderr="permission denied")):
            with self.assertRaisesRegex(self.module.SmokeError, "inspect_unavailable"):
                smoke.inspect("container", "a" * 64, missing=True)
        with patch.object(smoke, "docker", return_value=types.SimpleNamespace(returncode=1, stderr="No such container")):
            self.assertIsNone(smoke.inspect("container", "a" * 64, missing=True))

    def test_missing_image_does_not_create_resources(self):
        """Preflight all required images before creating a network or container."""
        smoke = self.module.Smoke()
        with patch.object(smoke, "docker", side_effect=[types.SimpleNamespace(stdout="29.8"), self.module.SmokeError("docker_operation_failed")]):
            with patch.object(smoke, "create") as create:
                with self.assertRaises(self.module.SmokeError):
                    smoke.run("1.1.1.1")
                create.assert_not_called()

    def test_unreachable_control_cannot_pass_entire_smoke(self):
        """Continue local checks and stop the proxy, but fail overall when public control is unreachable."""
        smoke = self.module.Smoke()
        identifiers = {role: character * 64 for role, character in zip(
            ("internal", "outbound", "sandbox", "control", "deny", "egress"), "abcdef")}
        stopped = False
        def docker(*args, **kwargs):
            """Return pinned image facts without touching Docker."""
            return types.SimpleNamespace(stdout="sha256:" + "a" * 64)
        def create(kind, role, *args, **kwargs):
            """Return deterministic resources for orchestration-only testing."""
            return identifiers[role]
        def inspect(kind, identifier, **kwargs):
            """Supply the actual network attributes required by this scenario."""
            role = "internal" if identifier == identifiers["internal"] else "outbound"
            return {"Id": identifier, "Labels": smoke.labels(role), "Driver": "bridge", "Internal": role == "internal"}
        def terminate(identifier, role):
            """Remember the allowed proxy's shutdown boundary."""
            nonlocal stopped
            if role == "egress":
                stopped = True
        def client(container, mode, proxy="egress", domain="example.com", **kwargs):
            """Make public control unavailable while preserving independent policy checks."""
            if proxy == "deny-egress" or domain == "not-allowed.invalid":
                return {"ok": False, "status": 403, "code": "egress_policy_denied"}
            if stopped:
                return {"ok": False, "reason": "network_or_proxy_failure"}
            if mode == "tcp" or mode.startswith("direct-"):
                return {"ok": False, "reason": "timeout"}
            return {"ok": True}
        with patch.object(smoke, "docker", side_effect=docker), patch.object(smoke, "create", side_effect=create), \
             patch.object(smoke, "inspect", side_effect=inspect), patch.object(smoke, "verify_container"), \
             patch.object(smoke, "wait_healthy"), patch.object(smoke, "terminate", side_effect=terminate) as shutdown, \
             patch.object(smoke, "client", side_effect=client):
            with self.assertRaisesRegex(self.module.SmokeError, "公网链路未完成验证"):
                smoke.run("1.1.1.1")
        self.assertTrue(stopped)
        self.assertEqual(shutdown.call_count, 2)
        self.assertNotIn("internal sandbox direct TCP blocked", self.output.getvalue())

    def test_docker_timeout_preserves_creation_intent(self):
        """Keep the exact creation name for reconciliation after an ambiguous CLI timeout."""
        smoke = self.module.Smoke()
        with patch.object(smoke, "docker", side_effect=self.module.SmokeError("docker_timeout")):
            with self.assertRaises(self.module.SmokeError):
                smoke.create("network", "internal", "--driver", "bridge")
        self.assertEqual(len(smoke.intents), 1)
        self.assertIn(smoke.run_id, smoke.intents[0][1])


if __name__ == "__main__":
    unittest.main()
