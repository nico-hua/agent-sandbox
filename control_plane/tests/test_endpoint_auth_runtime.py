"""Docker credential checks for current endpoint facts and creation preflight."""

import hashlib
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from control_plane.core.errors import SandboxEndpointUnavailable, SandboxImageUnavailable
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from test_sandbox_endpoint import EndpointClient, SPEC
from test_sandbox_runtime import RecordingClient, make_spec

TOKEN = "aB_" + "x" * 40
AUTH_LABEL = "io.agent-sandbox.endpoint-auth-version"
IMAGE_LABEL = "io.agent-sandbox.proxy-auth-version"


def authenticated_client() -> EndpointClient:
    """Upgrade an inspect fixture with consistent v1 identity and configured proxy token."""
    client = EndpointClient()
    for resource in [client.sandbox, client.proxy, *client.networks.resources.values()]:
        resource.attrs.get("Config", {}).get("Labels", resource.attrs.get("Labels", {}))[AUTH_LABEL] = "1"
    for resource in [client.sandbox, client.proxy]:
        resource.attrs["Config"]["Labels"].update({
            "io.agent-sandbox.created-at": "2026-10-19T08:00:00Z",
            "io.agent-sandbox.expires-at": "2026-10-19T08:30:00Z",
        })
    client.proxy.attrs["Config"]["Env"] = [f"SANDBOX_ACCESS_TOKEN={TOKEN}", "SANDBOX_PROXY_AUTH_VERSION=1"]
    client.proxy.attrs["Image"] = "sha256:" + "a" * 64
    client.images = SimpleNamespace(get=lambda _: SimpleNamespace(
        id=client.proxy.attrs["Image"], attrs={"Config": {"Labels": {IMAGE_LABEL: "1"}}},
    ))
    return client


@pytest.mark.anyio
async def test_endpoint_exposes_digest_and_current_identity_without_token() -> None:
    """Runtime returns configured protection proof while withholding the credential itself."""
    endpoint = await DockerSandboxRuntime(authenticated_client).get_endpoint(SPEC.sandbox_id)
    assert endpoint.auth_required is True
    assert endpoint.access_token is None
    assert endpoint.auth_version == "1"
    assert endpoint.transaction_id == SPEC.transaction_id
    assert endpoint.created_at == datetime(2026, 10, 19, 8, tzinfo=UTC)
    assert endpoint.credential_digest == hashlib.sha256(TOKEN.encode("ascii")).hexdigest()
    assert TOKEN not in repr(endpoint)


@pytest.mark.anyio
@pytest.mark.parametrize("case", ["missing_marker", "unknown_marker", "token", "version", "duplicate", "image", "time", "all_markers_missing"])
async def test_authenticated_entry_rejects_inconsistent_current_facts(case: str) -> None:
    """Modified proxy facts never permit fallback to anonymous legacy access."""
    client = authenticated_client()
    if case == "all_markers_missing":
        for resource in [client.sandbox, client.proxy, *client.networks.resources.values()]:
            del resource.attrs.get("Config", {}).get("Labels", resource.attrs.get("Labels", {}))[AUTH_LABEL]
    elif case == "missing_marker":
        del client.sandbox.attrs["Config"]["Labels"][AUTH_LABEL]
    elif case == "unknown_marker":
        client.proxy.attrs["Config"]["Labels"][AUTH_LABEL] = "2"
    elif case == "token":
        client.proxy.attrs["Config"]["Env"][0] = "SANDBOX_ACCESS_TOKEN=invalid"
    elif case == "version":
        client.proxy.attrs["Config"]["Env"][1] = "SANDBOX_PROXY_AUTH_VERSION=2"
    elif case == "duplicate":
        client.proxy.attrs["Config"]["Env"].append(f"SANDBOX_ACCESS_TOKEN={TOKEN}")
    elif case == "image":
        client.images.get = lambda _: SimpleNamespace(id=client.proxy.attrs["Image"], attrs={"Config": {"Labels": {}}})
    else:
        client.proxy.attrs["Config"]["Labels"]["io.agent-sandbox.created-at"] = "2026-10-19T08:00:01Z"
    with pytest.raises(SandboxEndpointUnavailable):
        await DockerSandboxRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)


@pytest.mark.anyio
async def test_creation_pins_labeled_proxy_image_and_limits_secret_to_proxy_env() -> None:
    """Creation pins the verified image and puts credentials only in proxy configuration."""
    client = RecordingClient()
    await DockerSandboxRuntime(lambda: client).create(replace(make_spec(), access_token=TOKEN))
    main, proxy = client.containers.create_calls
    assert proxy["image"] == "sha256:" + "a" * 64
    assert proxy["environment"] == {"SANDBOX_ACCESS_TOKEN": TOKEN, "SANDBOX_PROXY_AUTH_VERSION": "1"}
    assert "SANDBOX_ACCESS_TOKEN" not in main.get("environment", {})
    for call in [*client.networks.create_calls, *client.volumes.create_calls, main, proxy]:
        assert call["labels"][AUTH_LABEL] == "1"
        assert TOKEN not in repr(call["labels"])


@pytest.mark.anyio
@pytest.mark.parametrize("labels", [{}, None])
async def test_creation_rejects_unlabeled_proxy_before_resource_mutation(labels) -> None:
    """An old proxy image cannot satisfy the new fixed image preflight."""
    client = RecordingClient()
    client.images.get = lambda _: SimpleNamespace(id="sha256:" + "a" * 64, attrs={"Config": {"Labels": labels}})
    with pytest.raises(SandboxImageUnavailable):
        await DockerSandboxRuntime(lambda: client).create(replace(make_spec(), access_token=TOKEN))
    assert not client.networks.create_calls


@pytest.mark.anyio
@pytest.mark.parametrize("marker", [None, "2"])
async def test_delete_does_not_treat_auth_markers_as_cleanup_authority(marker: str | None) -> None:
    """Exact owned survivors remain reclaimable when protection metadata is missing or corrupt."""
    from test_sandbox_deletion import DeletionClient, DeletionState, SPEC as DELETE_SPEC

    state = DeletionState()
    client = DeletionClient(state)
    for group in state.resources.values():
        for resource in group.values():
            labels = resource.attrs.get("Config", {}).get("Labels", resource.attrs.get("Labels", {}))
            if marker is not None:
                labels[AUTH_LABEL] = marker
    await DockerSandboxRuntime(lambda: client).delete(
        DELETE_SPEC.sandbox_id, delete_workspace=True,
        expected_transaction_id=DELETE_SPEC.transaction_id,
    )
    assert all(not group for group in state.resources.values())


@pytest.mark.anyio
async def test_inspection_rejects_auth_conflict_while_retaining_partial_inventory() -> None:
    """Recovery refuses inconsistent protection without weakening legacy partial cleanup checks."""
    client = authenticated_client()
    client.proxy.attrs["Config"]["Env"][1] = "SANDBOX_PROXY_AUTH_VERSION=2"
    result = await DockerSandboxRuntime(lambda: client).inspect_resources(
        SPEC.sandbox_id, expected_transaction_id=SPEC.transaction_id,
    )
    assert result.classification == "conflict"
    assert result.reason == "endpoint_auth_configuration_conflict"


@pytest.mark.anyio
async def test_legacy_image_without_labels_remains_readable_by_runtime():
    """Docker's null image Labels means legacy metadata, not a broken query."""
    client = EndpointClient()
    client.proxy.attrs["Image"] = "sha256:" + "c" * 64
    client.images = SimpleNamespace(get=lambda _: SimpleNamespace(attrs={"Config": {"Labels": None}}))
    endpoint = await DockerSandboxRuntime(lambda: client).get_endpoint(SPEC.sandbox_id)
    assert endpoint.auth_required is False
    assert endpoint.access_token is None
