"""Real Docker recording, readiness and exact-ownership cleanup helpers."""

import asyncio
import re
from dataclasses import dataclass
from typing import Any

import docker
import httpx
import pytest

from control_plane.domain.models import (
    ResourceKind,
    SandboxSpec,
)


def _manager(client: Any, kind: ResourceKind) -> Any:
    """Return the Docker collection that loads a resource by exact ID."""
    if kind in {"container", "proxy"}:
        return client.containers
    if kind == "workspace":
        return client.volumes
    return client.networks


def _egress_resource_manager(client: Any, kind: str) -> Any:
    """Reuse the lifecycle collections while recognizing the egress container role."""
    return _manager(client, "container" if kind == "egress" else kind)


def _labels(resource: Any, kind: ResourceKind) -> dict[str, str]:
    """Read labels from the Docker inspect shape for one resource kind."""
    if kind in {"container", "proxy"}:
        value = resource.attrs.get("Config", {}).get("Labels", {})
    else:
        value = resource.attrs.get("Labels", {})
    return value if isinstance(value, dict) else {}


def _egress_resource_labels(resource: Any, kind: str) -> dict[str, str]:
    """Read an egress container's labels using the existing container inspect shape."""
    return _labels(resource, "container" if kind == "egress" else kind)


@dataclass(frozen=True)
class CreatedResource:
    """Record one exact resource ID and its expected ownership labels."""

    kind: ResourceKind
    resource_id: str
    labels: dict[str, str]


def _has_expected_labels(actual: dict[str, str], expected: dict[str, str]) -> bool:
    """Report whether all expected ownership labels are present and equal."""
    return all(actual.get(key) == value for key, value in expected.items())


def _owned_resource(client: Any, record: CreatedResource) -> Any:
    """Require an exact resource ID and every recorded label before any mutation."""
    labels = record.labels
    assert labels.get("io.agent-sandbox.managed") == "true"
    assert labels.get("io.agent-sandbox.project") == "agent-sandbox"
    assert labels.get("io.agent-sandbox.resource") == record.kind
    assert re.fullmatch(r"sbx_[0-9a-f]{32}", labels.get("io.agent-sandbox.sandbox-id", ""))
    assert re.fullmatch(r"txn_[0-9a-f]{32}", labels.get("io.agent-sandbox.transaction-id", ""))
    resource = _egress_resource_manager(client, record.kind).get(record.resource_id)
    resource.reload()
    assert resource.id == record.resource_id
    assert _has_expected_labels(_egress_resource_labels(resource, record.kind), labels)
    return resource


def _cleanup_owned(client: Any, created: list[CreatedResource]) -> None:
    """Clean only the exact recorded transaction resources and report all residuals."""
    errors = []
    for record in reversed(created):
        try:
            resource = _owned_resource(client, record)
            if record.kind in {"container", "proxy", "egress"}:
                resource.remove(force=True)
            else:
                resource.remove()
        except docker.errors.NotFound:
            continue
        except Exception as error:
            errors.append(f"{record.kind}:{type(error).__name__}")
    for record in created:
        try:
            _egress_resource_manager(client, record.kind).get(record.resource_id)
        except docker.errors.NotFound:
            continue
        except Exception as error:
            errors.append(f"{record.kind}:absence-{type(error).__name__}")
        else:
            errors.append(f"{record.kind}:still-present")
    assert not errors, "integration cleanup failed: " + ", ".join(errors)


def _resources(created: list[CreatedResource], sandbox_id: str) -> dict[str, CreatedResource]:
    """Select one test-created instance's exact ledger without discovering by prefix."""
    return {
        record.kind: record for record in created
        if record.labels["io.agent-sandbox.sandbox-id"] == sandbox_id
    }


async def _create(control: httpx.AsyncClient, created: list[CreatedResource], domains: list[str] | None) -> str:
    """Create only a prebuilt Python preset through the public control API."""
    body: dict[str, Any] = {"environment": "python"}
    if domains is not None:
        body["network"] = {"allow_domains": domains}
    response = await control.post("/v1/sandboxes", json=body)
    assert response.status_code == 201, response.text
    sandbox_id = response.json()["sandbox_id"]
    assert _resources(created, sandbox_id)
    return sandbox_id


async def _python(agent: httpx.AsyncClient, code: str) -> dict[str, Any]:
    """Execute bounded Python through the Agent endpoint, never through Docker exec."""
    response = await agent.post("/v1/commands:run", json={
        "argv": ["python3", "-c", code], "timeout_ms": 20000,
    })
    assert response.status_code == 200, response.text
    return response.json()


async def _wait_for_resumed_ready(control: httpx.AsyncClient, sandbox_id: str) -> None:
    """Bound post-thaw readiness while cached health and entry probes recover asynchronously."""
    async with asyncio.timeout(15):
        while True:
            response = await control.get(f"/v1/sandboxes/{sandbox_id}/readyz")
            if response.status_code == 200:
                assert response.json() == {"sandbox_id": sandbox_id, "status": "ready"}
                return
            assert response.status_code == 503, response.text
            assert response.json()["error"]["code"] in {
                "sandbox_not_ready", "sandbox_endpoint_unavailable", "sandbox_agent_unavailable",
            }
            # Docker resumes health probes separately from unpause completion;
            # this test wait must not change the API's immediate 204 contract.
            await asyncio.sleep(0.1)


def _assert_paused(client: Any, resources: dict[str, CreatedResource], paused: bool) -> None:
    """Confirm the actual Docker freezer state while the entry proxy keeps running."""
    for role in ("container", "egress"):
        if role in resources:
            state = _owned_resource(client, resources[role]).attrs["State"]
            assert state["Paused"] is paused, (role, state)
            assert state["Running"] is True, (role, state)
    proxy = _owned_resource(client, resources["proxy"])
    assert proxy.attrs["State"]["Running"] is True
    assert proxy.attrs["State"]["Paused"] is False


def _assert_runtime_deleted(client: Any, resources: dict[str, CreatedResource]) -> None:
    """Require every exact runtime ID to disappear and the original workspace to survive."""
    for role, record in resources.items():
        if role == "workspace":
            _owned_resource(client, record)
        else:
            with pytest.raises(docker.errors.NotFound):
                _egress_resource_manager(client, role).get(record.resource_id)


class _AuthenticatedEndpoint(str):
    """Keep URL comparisons and JSON safe while retaining one instance's private credential."""

    def __new__(cls, url: str, access_token: str):
        """Require a protected endpoint's strict bearer syntax without rendering the token."""
        if type(url) is not str or re.fullmatch(r"http://127\.0\.0\.1:[1-9][0-9]*", url) is None:
            raise ValueError("invalid integration endpoint URL")
        if type(access_token) is not str or re.fullmatch(r"[A-Za-z0-9_-]{43}", access_token) is None:
            raise ValueError("invalid integration endpoint credential")
        value = super().__new__(cls, url)
        value.access_token = access_token
        return value


def _endpoint_headers(endpoint: _AuthenticatedEndpoint) -> dict[str, str]:
    """Attach only the explicit selected instance credential to one Agent client."""
    if not isinstance(endpoint, _AuthenticatedEndpoint):
        raise ValueError("authenticated integration endpoint required")
    return {"Authorization": f"Bearer {endpoint.access_token}"}


def _spec_for(sandbox_id: str) -> SandboxSpec:
    """Build the names that SandboxService derives from one ID."""
    return SandboxSpec(
        sandbox_id=sandbox_id,
        container_name=f"agent-sandbox-{sandbox_id}",
        network_name=f"agent-sandbox-{sandbox_id}-internal",
        volume_name=f"agent-sandbox-{sandbox_id}-workspace",
    )


def _remove_owned_resources(client: Any, resources: list[CreatedResource]) -> None:
    """Remove recorded resources only after exact ID and label verification."""
    errors: list[str] = []
    for record in resources:
        try:
            sandbox_id = record.labels.get("io.agent-sandbox.sandbox-id", "")
            transaction_id = record.labels.get("io.agent-sandbox.transaction-id", "")
            if (
                record.labels.get("io.agent-sandbox.managed") != "true"
                or record.labels.get("io.agent-sandbox.project") != "agent-sandbox"
                or record.labels.get("io.agent-sandbox.resource") != record.kind
                or re.fullmatch(r"sbx_[0-9a-f]{32}", sandbox_id) is None
                or re.fullmatch(r"txn_[0-9a-f]{32}", transaction_id) is None
            ):
                errors.append(f"{record.kind} ledger has incomplete ownership labels")
                continue
            resource = _manager(client, record.kind).get(record.resource_id)
            resource.reload()
            if resource.id != record.resource_id:
                errors.append(f"{record.kind} ID changed")
                continue
            actual_labels = _labels(resource, record.kind)
            if not _has_expected_labels(actual_labels, record.labels):
                errors.append(f"{record.kind} labels changed")
                continue
            if record.kind in {"container", "proxy"}:
                resource.remove(force=True)
            else:
                resource.remove()
        except docker.errors.NotFound:
            # Successful DELETE operations already removed some ledger entries.
            continue
        except Exception as error:  # pragma: no cover - real daemon diagnostic
            errors.append(f"{record.kind}: {type(error).__name__}")
    if errors:
        pytest.fail("integration cleanup failed: " + ", ".join(errors))


class RecordingAPIProxy:
    """Record confirmed low-level create IDs before diagnostic inspection."""

    def __init__(self, api: Any, created: list[CreatedResource]) -> None:
        """Store the real API and transaction-owned resource ledger."""
        self._api = api
        self._created = created

    def __getattr__(self, name: str) -> Any:
        """Delegate non-create Docker API operations unchanged."""
        return getattr(self._api, name)

    def create_network(self, name: str, **kwargs: Any) -> dict[str, Any]:
        """Record the network ID immediately after Docker confirms creation."""
        response = self._api.create_network(name, **kwargs)
        self._created.append(
            CreatedResource(kwargs["labels"]["io.agent-sandbox.resource"], response["Id"], dict(kwargs["labels"]))
        )
        return response

    def create_volume(self, name: str, **kwargs: Any) -> dict[str, Any]:
        """Record a volume only when Docker returns this transaction's labels."""
        response = self._api.create_volume(name, **kwargs)
        expected = dict(kwargs["labels"])
        actual = response.get("Labels")
        if isinstance(actual, dict) and _has_expected_labels(actual, expected):
            self._created.append(
                CreatedResource("workspace", response["Name"], expected)
            )
        return response

    def create_container(self, **kwargs: Any) -> dict[str, Any]:
        """Record the container ID immediately after Docker confirms creation."""
        response = self._api.create_container(**kwargs)
        self._created.append(
            CreatedResource(kwargs["labels"]["io.agent-sandbox.resource"], response["Id"], dict(kwargs["labels"]))
        )
        return response


class RecordingDockerClient:
    """Wrap a real client while retaining its transport for final cleanup."""

    def __init__(self, client: Any, created: list[CreatedResource]) -> None:
        """Expose real managers and a create-recording API proxy."""
        self.images = client.images
        self.networks = client.networks
        self.volumes = client.volumes
        self.containers = client.containers
        self.api = RecordingAPIProxy(client.api, created)

    def close(self) -> None:
        """Leave the shared real client open for inspection and cleanup."""


async def _wait_for_ready(
    http_client: httpx.AsyncClient,
    sandbox_id: str,
    container: Any,
) -> dict[str, Any]:
    """Poll the real query endpoint with a total deadline and bounded diagnostics."""
    last_response: dict[str, Any] = {}
    try:
        async with asyncio.timeout(30):
            while True:
                response = await http_client.get(f"/v1/sandboxes/{sandbox_id}")
                assert response.status_code == 200, response.text[:500]
                last_response = response.json()
                if last_response["status"] == "ready":
                    return last_response
                if last_response["status"] in {"failed", "stopped"}:
                    break
                await asyncio.sleep(0.25)
    except TimeoutError:
        pass

    await asyncio.to_thread(container.reload)
    state = container.attrs["State"]
    health = state.get("Health", {})
    exit_codes = [entry.get("ExitCode") for entry in health.get("Log", [])[-3:]]
    pytest.fail(
        f"sandbox did not become ready within 30 seconds: {last_response}; "
        f"container_status={state.get('Status')}, "
        f"health_status={health.get('Status')}, "
        f"failing_streak={health.get('FailingStreak')}, "
        f"recent_health_exit_codes={exit_codes}"
    )


async def _wait_for_endpoint(http_client: httpx.AsyncClient, sandbox_id: str) -> _AuthenticatedEndpoint:
    """Poll public entry readiness and keep its credential separate from URL serialization."""
    async with asyncio.timeout(30):
        while True:
            response = await http_client.get(f"/v1/sandboxes/{sandbox_id}/endpoint")
            if response.status_code == 200:
                payload = response.json()
                assert payload.get("auth_required") is True
                assert response.headers.get("Cache-Control") == "no-store"
                return _AuthenticatedEndpoint(payload["url"], payload.get("access_token"))
            assert response.status_code == 503, response.text[:500]
            await asyncio.sleep(0.25)


_WORKSPACE_LABEL_PREFIX = "io.agent-sandbox."


def owned_workspace_test_resource(client: Any, record: CreatedResource) -> Any:
    """Authorize an exact ledger ID and complete independent or instance ownership."""
    labels = record.labels
    assert labels[_WORKSPACE_LABEL_PREFIX + "managed"] == "true"
    assert labels[_WORKSPACE_LABEL_PREFIX + "project"] == "agent-sandbox"
    assert labels[_WORKSPACE_LABEL_PREFIX + "resource"] == record.kind
    if _WORKSPACE_LABEL_PREFIX + "workspace-version" in labels:
        assert labels[_WORKSPACE_LABEL_PREFIX + "workspace-version"] == "1"
        workspace_id = labels[_WORKSPACE_LABEL_PREFIX + "workspace-id"]
        assert re.fullmatch(r"wsp_[0-9a-f]{32}", workspace_id)
        assert re.fullmatch(r"wct_[0-9a-f]{32}", labels[_WORKSPACE_LABEL_PREFIX + "workspace-token"])
        assert labels[_WORKSPACE_LABEL_PREFIX + "workspace-image-id"]
        assert record.kind in {"workspace", "workspace-initializer"}
        if record.kind == "workspace":
            assert record.resource_id == "agent-sandbox-" + workspace_id
    else:
        assert re.fullmatch(r"sbx_[0-9a-f]{32}", labels[_WORKSPACE_LABEL_PREFIX + "sandbox-id"])
        assert re.fullmatch(r"txn_[0-9a-f]{32}", labels[_WORKSPACE_LABEL_PREFIX + "transaction-id"])
    manager = client.volumes if record.kind == "workspace" else (
        client.networks if record.kind in {"network", "ingress"} else client.containers
    )
    resource = manager.get(record.resource_id)
    resource.reload()
    assert resource.id == record.resource_id
    actual = resource.attrs.get("Labels", {}) if record.kind in {"workspace", "network", "ingress"} else resource.attrs["Config"]["Labels"]
    assert all(actual.get(key) == value for key, value in labels.items())
    return resource


def cleanup_workspace_test_resources(client: Any, created: list[CreatedResource]) -> None:
    """Clean only confirmed test-created resources after exact ownership checks."""
    failures = []
    for record in reversed(created):
        try:
            resource = owned_workspace_test_resource(client, record)
            if record.kind in {"workspace", "network", "ingress"}:
                resource.remove()
            else:
                resource.remove(force=True, v=False)
        except docker.errors.NotFound:
            continue
        except Exception as error:
            failures.append(f"{record.kind}:{type(error).__name__}")
    for record in created:
        try:
            owned_workspace_test_resource(client, record)
        except docker.errors.NotFound:
            continue
        except Exception as error:
            failures.append(f"{record.kind}:absence-{type(error).__name__}")
        else:
            failures.append(f"{record.kind}:still-present")
    assert not failures, "workspace integration cleanup failed: " + ", ".join(failures)
