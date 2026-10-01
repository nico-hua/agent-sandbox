"""Domain tests for sandbox creation metadata and results."""

import re

import pytest

from control_plane.core.errors import DockerUnavailable, SandboxCreateFailed, SandboxNotFound
from control_plane.core.models import SandboxQueryResult, SandboxSpec
from control_plane.core.service import SandboxService
from control_plane.runtime.sandbox_runtime import SandboxRuntime


FIXED_SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"
FIXED_TRANSACTION_ID = "txn_0123456789abcdef0123456789abcdef"


class FakeRuntime(SandboxRuntime):
    """Implement the runtime contract and record service delegation."""

    def __init__(self, error: Exception | None = None) -> None:
        """Initialize readiness, specification and query records with an optional error."""
        self.error = error
        self.ping_calls = 0
        self.specs: list[SandboxSpec] = []
        self.queries: list[str] = []
        self.query_result = SandboxQueryResult(
            sandbox_id=FIXED_SANDBOX_ID,
            status="running",
            reason="health_check_not_configured",
            message="container is running but no health check is configured",
        )

    async def ping(self) -> None:
        """Record a ping and propagate the configured domain failure."""
        self.ping_calls += 1
        if self.error is not None:
            raise self.error

    async def create(self, spec: SandboxSpec) -> None:
        """Record the specification and raise the configured error."""
        self.specs.append(spec)
        if self.error is not None:
            raise self.error

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Record one status query and return the configured observation."""
        self.queries.append(sandbox_id)
        if self.error is not None:
            raise self.error
        return self.query_result


@pytest.mark.anyio
async def test_create_builds_names_and_labels_from_deterministic_id() -> None:
    """Creation must derive all names and ownership labels from one ID."""
    runtime = FakeRuntime()
    service = SandboxService(
        runtime,
        id_factory=lambda: FIXED_SANDBOX_ID,
        transaction_id_factory=lambda: FIXED_TRANSACTION_ID,
    )

    await service.create()

    assert runtime.specs == [
        SandboxSpec(
            sandbox_id=FIXED_SANDBOX_ID,
            container_name=f"agent-sandbox-{FIXED_SANDBOX_ID}",
            network_name=f"agent-sandbox-{FIXED_SANDBOX_ID}-internal",
            volume_name=f"agent-sandbox-{FIXED_SANDBOX_ID}-workspace",
            transaction_id=FIXED_TRANSACTION_ID,
        )
    ]
    spec = runtime.specs[0]
    base_labels = {
        "io.agent-sandbox.managed": "true",
        "io.agent-sandbox.project": "agent-sandbox",
        "io.agent-sandbox.sandbox-id": FIXED_SANDBOX_ID,
        "io.agent-sandbox.transaction-id": FIXED_TRANSACTION_ID,
    }
    assert spec.labels_for("container") == {
        **base_labels,
        "io.agent-sandbox.resource": "container",
    }
    assert spec.labels_for("network") == {
        **base_labels,
        "io.agent-sandbox.resource": "network",
    }
    assert spec.labels_for("workspace") == {
        **base_labels,
        "io.agent-sandbox.resource": "workspace",
    }


@pytest.mark.anyio
async def test_create_returns_started_only_after_runtime_success() -> None:
    """The service must report started only after runtime creation returns."""
    runtime = FakeRuntime()
    service = SandboxService(runtime, id_factory=lambda: FIXED_SANDBOX_ID)

    result = await service.create()

    assert result.sandbox_id == FIXED_SANDBOX_ID
    assert result.status == "started"
    assert len(runtime.specs) == 1


@pytest.mark.anyio
async def test_default_id_has_prefix_and_128_bits_of_lowercase_hex() -> None:
    """The default ID must contain a prefix and 128 random bits in hex."""
    runtime = FakeRuntime()
    service = SandboxService(runtime)

    result = await service.create()

    assert re.fullmatch(r"sbx_[0-9a-f]{32}", result.sandbox_id)
    assert runtime.specs[0].sandbox_id == result.sandbox_id
    assert re.fullmatch(r"txn_[0-9a-f]{32}", runtime.specs[0].transaction_id)


@pytest.mark.anyio
async def test_runtime_error_is_preserved_for_http_mapping() -> None:
    """A domain error from the runtime must reach the HTTP boundary intact."""
    error = SandboxCreateFailed()
    service = SandboxService(
        FakeRuntime(error),
        id_factory=lambda: FIXED_SANDBOX_ID,
    )

    with pytest.raises(SandboxCreateFailed) as captured:
        await service.create()

    assert captured.value is error


@pytest.mark.anyio
async def test_get_delegates_valid_id_to_runtime_without_process_state() -> None:
    """A valid ID must be resolved entirely by the injected runtime."""
    runtime = FakeRuntime()
    service = SandboxService(runtime)

    result = await service.get(FIXED_SANDBOX_ID)

    assert result == SandboxQueryResult(
        sandbox_id=FIXED_SANDBOX_ID,
        status="running",
        reason="health_check_not_configured",
        message="container is running but no health check is configured",
    )
    assert runtime.queries == [FIXED_SANDBOX_ID]


@pytest.mark.anyio
async def test_get_rejects_invalid_id_without_querying_runtime() -> None:
    """Malformed IDs must behave as unknown without reaching Docker."""
    runtime = FakeRuntime()

    with pytest.raises(SandboxNotFound):
        await SandboxService(runtime).get("not-a-sandbox-id")

    assert runtime.queries == []


@pytest.mark.anyio
async def test_ping_delegates_to_runtime_without_lifecycle_operations() -> None:
    """Readiness must use only the injected runtime's ping operation."""
    runtime = FakeRuntime()

    result = await SandboxService(runtime).ping()

    assert result is None
    assert runtime.ping_calls == 1
    assert runtime.specs == []
    assert runtime.queries == []


@pytest.mark.anyio
async def test_ping_preserves_domain_error_for_http_mapping() -> None:
    """The service must propagate the same domain error without HTTP mapping."""
    error = DockerUnavailable("sensitive backend details")
    runtime = FakeRuntime(error)

    with pytest.raises(DockerUnavailable) as captured:
        await SandboxService(runtime).ping()

    assert captured.value is error
    assert runtime.ping_calls == 1


@pytest.mark.anyio
async def test_get_preserves_domain_error_for_http_mapping() -> None:
    """A valid query must preserve the exact runtime domain failure."""
    error = DockerUnavailable("sensitive backend details")
    runtime = FakeRuntime(error)

    with pytest.raises(DockerUnavailable) as captured:
        await SandboxService(runtime).get(FIXED_SANDBOX_ID)

    assert captured.value is error
    assert runtime.queries == [FIXED_SANDBOX_ID]


def test_runtime_cannot_omit_creation_and_query_contract() -> None:
    """ABC enforcement must reject a backend that implements readiness only."""
    class PingOnlyRuntime(SandboxRuntime):
        """Represent an incomplete backend for abstract contract enforcement."""

        async def ping(self) -> None:
            """Provide only the readiness operation."""

    with pytest.raises(TypeError, match="abstract.*create.*get"):
        PingOnlyRuntime()
