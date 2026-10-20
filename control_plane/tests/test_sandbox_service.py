"""Domain tests for sandbox metadata, validation and lifecycle delegation."""

import re
from datetime import UTC, datetime, timedelta, timezone
from dataclasses import replace

from lifecycle_fakes import FakeLifecycleStore

import pytest

from control_plane.core.errors import (
    DockerUnavailable,
    SandboxCreateFailed,
    SandboxDeleteConflict,
    SandboxDeleteFailed,
    SandboxNotFound,
)
from control_plane.core.models import (
    SandboxEndpoint, SandboxQueryResult, SandboxResourceInspection, SandboxSpec,
)
from control_plane.core.service import SandboxService
from control_plane.runtime.sandbox_runtime import SandboxRuntime


FIXED_SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"
FIXED_TRANSACTION_ID = "txn_0123456789abcdef0123456789abcdef"
CREATED_AT = datetime(2026, 10, 19, 8, tzinfo=UTC)
EXPIRES_AT = datetime(2026, 10, 19, 8, 30, tzinfo=UTC)


class FakeRuntime(SandboxRuntime):
    """Implement the runtime contract and record service delegation."""

    def __init__(self, error: Exception | None = None) -> None:
        """Initialize readiness, specification and query records with an optional error."""
        self.error = error
        self.ping_calls = 0
        self.specs: list[SandboxSpec] = []
        self.queries: list[str] = []
        self.deletions: list[tuple[str, bool]] = []
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
        self.query_result = replace(
            self.query_result, sandbox_id=spec.sandbox_id,
            transaction_id=spec.transaction_id, created_at=spec.created_at,
            expires_at=spec.expires_at,
        )

    async def get(self, sandbox_id: str) -> SandboxQueryResult:
        """Record one status query and return the configured observation."""
        self.queries.append(sandbox_id)
        if self.error is not None:
            raise self.error
        return self.query_result

    async def get_endpoint(self, sandbox_id: str) -> SandboxEndpoint:
        """Record a verified ID and return a deterministic local entry."""
        self.queries.append(sandbox_id)
        if self.error is not None:
            raise self.error
        return SandboxEndpoint(sandbox_id, "http://127.0.0.1:49153")

    async def inspect_resources(
        self, sandbox_id: str, *, expected_transaction_id: str,
    ) -> SandboxResourceInspection:
        """Provide an explicit read-only fake inspection for the backend contract."""
        if self.error is not None:
            raise self.error
        return SandboxResourceInspection(sandbox_id, "not_found", "resources_not_found")

    async def delete(
        self, sandbox_id: str, *, delete_workspace: bool = False,
        expected_transaction_id: str | None = None,
    ) -> None:
        """Record explicit workspace policy and preserve runtime failures."""
        self.deletions.append((sandbox_id, delete_workspace))
        if self.error is not None:
            raise self.error



@pytest.mark.anyio
async def test_create_builds_names_and_labels_from_deterministic_id() -> None:
    """Creation must derive all names and ownership labels from one ID."""
    runtime = FakeRuntime()
    service = SandboxService(
        runtime, FakeLifecycleStore(),
        id_factory=lambda: FIXED_SANDBOX_ID,
        transaction_id_factory=lambda: FIXED_TRANSACTION_ID,
        clock=lambda: CREATED_AT,
    )

    await service.create()

    assert runtime.specs == [
        SandboxSpec(
            sandbox_id=FIXED_SANDBOX_ID,
            container_name=f"agent-sandbox-{FIXED_SANDBOX_ID}",
            network_name=f"agent-sandbox-{FIXED_SANDBOX_ID}-internal",
            volume_name=f"agent-sandbox-{FIXED_SANDBOX_ID}-workspace",
            transaction_id=FIXED_TRANSACTION_ID,
            created_at=CREATED_AT,
            expires_at=EXPIRES_AT,
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
async def test_create_reads_clock_once_before_runtime_and_uses_exact_ttl() -> None:
    """Creation must snapshot UTC once and pass the same initial times to runtime."""
    runtime = FakeRuntime()
    calls = 0

    def clock() -> datetime:
        """Return one precise timestamp and reject repeated reads or late sampling."""
        nonlocal calls
        calls += 1
        assert calls == 1
        assert runtime.specs == []
        return datetime(2026, 10, 19, 16, 0, 0, 123456,
                        tzinfo=timezone(timedelta(hours=8)))

    result = await SandboxService(runtime, FakeLifecycleStore(), clock=clock).create()

    assert calls == 1
    assert result.created_at == datetime(2026, 10, 19, 8, 0, 0, 123456, tzinfo=UTC)
    assert result.expires_at == datetime(2026, 10, 19, 8, 30, 0, 123456, tzinfo=UTC)
    assert result.created_at.tzinfo is UTC
    assert result.expires_at - result.created_at == timedelta(seconds=1800)
    assert runtime.specs[0].created_at == result.created_at
    assert runtime.specs[0].expires_at == result.expires_at


@pytest.mark.anyio
async def test_create_rejects_naive_clock_before_runtime() -> None:
    """An unzoned clock value must never reach Docker or imply local timezone."""
    runtime = FakeRuntime()
    service = SandboxService(runtime, FakeLifecycleStore(), clock=lambda: datetime(2026, 10, 19, 8))

    with pytest.raises(ValueError, match="timezone"):
        await service.create()

    assert runtime.specs == []


@pytest.mark.parametrize(
    ("created_at", "expires_at"),
    [
        (CREATED_AT, None),
        (None, EXPIRES_AT),
        (datetime(2026, 10, 19, 8), EXPIRES_AT),
        (CREATED_AT, datetime(2026, 10, 19, 8, 30)),
        (CREATED_AT, CREATED_AT - timedelta(microseconds=1)),
        (CREATED_AT.astimezone(timezone(timedelta(hours=8))), EXPIRES_AT),
    ],
)
def test_spec_rejects_incomplete_or_invalid_lifecycle_times(
    created_at: datetime | None, expires_at: datetime | None,
) -> None:
    """A creation specification must reject partial, unzoned or unordered metadata."""
    with pytest.raises(ValueError):
        SandboxSpec(
            sandbox_id=FIXED_SANDBOX_ID,
            container_name="sandbox",
            network_name="internal",
            volume_name="workspace",
            created_at=created_at,
            expires_at=expires_at,
        )


@pytest.mark.anyio
async def test_create_returns_started_only_after_runtime_success() -> None:
    """The service must report started only after runtime creation returns."""
    runtime = FakeRuntime()
    service = SandboxService(runtime, FakeLifecycleStore(), id_factory=lambda: FIXED_SANDBOX_ID)

    result = await service.create()

    assert result.sandbox_id == FIXED_SANDBOX_ID
    assert result.status == "started"
    assert len(runtime.specs) == 1


@pytest.mark.anyio
async def test_default_id_has_prefix_and_128_bits_of_lowercase_hex() -> None:
    """The default ID must contain a prefix and 128 random bits in hex."""
    runtime = FakeRuntime()
    service = SandboxService(runtime, FakeLifecycleStore())

    result = await service.create()

    assert re.fullmatch(r"sbx_[0-9a-f]{32}", result.sandbox_id)
    assert runtime.specs[0].sandbox_id == result.sandbox_id
    assert re.fullmatch(r"txn_[0-9a-f]{32}", runtime.specs[0].transaction_id)


@pytest.mark.anyio
async def test_runtime_error_is_preserved_for_http_mapping() -> None:
    """A domain error from the runtime must reach the HTTP boundary intact."""
    error = SandboxCreateFailed()
    service = SandboxService(
        FakeRuntime(error), FakeLifecycleStore(),
        id_factory=lambda: FIXED_SANDBOX_ID,
    )

    with pytest.raises(SandboxCreateFailed) as captured:
        await service.create()

    assert captured.value is error


@pytest.mark.anyio
async def test_get_delegates_valid_id_to_runtime_without_process_state() -> None:
    """A valid ID must be resolved entirely by the injected runtime."""
    runtime = FakeRuntime()
    service = SandboxService(runtime, FakeLifecycleStore())

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
        await SandboxService(runtime, FakeLifecycleStore()).get("not-a-sandbox-id")

    assert runtime.queries == []


@pytest.mark.anyio
async def test_ping_delegates_to_runtime_without_lifecycle_operations() -> None:
    """Readiness must use only the injected runtime's ping operation."""
    runtime = FakeRuntime()

    result = await SandboxService(runtime, FakeLifecycleStore()).ping()

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
        await SandboxService(runtime, FakeLifecycleStore()).ping()

    assert captured.value is error
    assert runtime.ping_calls == 1


@pytest.mark.anyio
async def test_get_preserves_domain_error_for_http_mapping() -> None:
    """A valid query must preserve the exact runtime domain failure."""
    error = DockerUnavailable("sensitive backend details")
    runtime = FakeRuntime(error)

    with pytest.raises(DockerUnavailable) as captured:
        await SandboxService(runtime, FakeLifecycleStore()).get(FIXED_SANDBOX_ID)

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


@pytest.mark.anyio
@pytest.mark.parametrize("delete_workspace", [False, True])
async def test_delete_delegates_workspace_policy_without_creation_state(
    delete_workspace: bool,
) -> None:
    """A fresh service must pass the validated ID and workspace choice to runtime."""
    runtime = FakeRuntime()

    result = await SandboxService(runtime, FakeLifecycleStore()).delete(
        FIXED_SANDBOX_ID, delete_workspace=delete_workspace
    )

    assert result is None
    assert runtime.deletions == [(FIXED_SANDBOX_ID, delete_workspace)]
    assert runtime.specs == []
    assert runtime.queries == []


@pytest.mark.anyio
async def test_delete_defaults_to_preserving_workspace() -> None:
    """Omitting the destructive workspace option must retain user data."""
    runtime = FakeRuntime()

    await SandboxService(runtime, FakeLifecycleStore()).delete(FIXED_SANDBOX_ID)

    assert runtime.deletions == [(FIXED_SANDBOX_ID, False)]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "sandbox_id",
    ["", "not-a-sandbox-id", "sbx_0123456789abcdef", "sbx_0123456789ABCDEF0123456789ABCDEF"],
)
async def test_delete_rejects_invalid_id_before_runtime(sandbox_id: str) -> None:
    """Malformed public IDs must never reach destructive runtime operations."""
    runtime = FakeRuntime()

    with pytest.raises(SandboxNotFound):
        await SandboxService(runtime, FakeLifecycleStore()).delete(sandbox_id, delete_workspace=True)

    assert runtime.deletions == []


@pytest.mark.anyio
async def test_delete_preserves_runtime_domain_failure() -> None:
    """The service must preserve a runtime failure for sanitized HTTP mapping."""
    error = DockerUnavailable("sensitive backend details")
    runtime = FakeRuntime(error)

    with pytest.raises(DockerUnavailable) as captured:
        await SandboxService(runtime, FakeLifecycleStore()).delete(FIXED_SANDBOX_ID)

    assert captured.value is error
    assert runtime.deletions == [(FIXED_SANDBOX_ID, False)]


def test_runtime_contract_requires_deletion() -> None:
    """Existing runtime operations must not make deletion optional for a backend."""
    class RuntimeWithoutDelete(FakeRuntime):
        """Reuse implemented operations while restoring an abstract delete method."""

        delete = SandboxRuntime.delete

    with pytest.raises(TypeError, match="abstract.*delete"):
        RuntimeWithoutDelete()


@pytest.mark.anyio
async def test_delete_preserves_conflict_domain_failure() -> None:
    """Ownership ambiguity must reach the HTTP boundary as a deletion conflict."""
    error = SandboxDeleteConflict("sensitive ownership details")
    runtime = FakeRuntime(error)

    with pytest.raises(SandboxDeleteConflict) as captured:
        await SandboxService(runtime, FakeLifecycleStore()).delete(FIXED_SANDBOX_ID)

    assert captured.value is error


@pytest.mark.anyio
async def test_delete_preserves_incomplete_cleanup_id() -> None:
    """Incomplete deletion must retain the ID needed to retry remaining resources."""
    error = SandboxDeleteFailed(FIXED_SANDBOX_ID)
    runtime = FakeRuntime(error)

    with pytest.raises(SandboxDeleteFailed) as captured:
        await SandboxService(runtime, FakeLifecycleStore()).delete(FIXED_SANDBOX_ID)

    assert captured.value is error
    assert captured.value.sandbox_id == FIXED_SANDBOX_ID
