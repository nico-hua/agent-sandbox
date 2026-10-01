"""Domain tests for sandbox creation metadata and results."""

import re

import pytest

from control.sandbox_service import (
    SandboxCreateFailed,
    SandboxService,
    SandboxSpec,
)


FIXED_SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"
FIXED_TRANSACTION_ID = "txn_0123456789abcdef0123456789abcdef"


class RecordingRuntime:
    """Record sandbox specifications and optionally fail creation."""

    def __init__(self, error: Exception | None = None) -> None:
        """Initialize recorded specifications and an optional error."""
        self.error = error
        self.specs: list[SandboxSpec] = []

    async def create(self, spec: SandboxSpec) -> None:
        """Record the specification and raise the configured error."""
        self.specs.append(spec)
        if self.error is not None:
            raise self.error


@pytest.mark.anyio
async def test_create_builds_names_and_labels_from_deterministic_id() -> None:
    """Creation must derive all names and ownership labels from one ID."""
    runtime = RecordingRuntime()
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
    runtime = RecordingRuntime()
    service = SandboxService(runtime, id_factory=lambda: FIXED_SANDBOX_ID)

    result = await service.create()

    assert result.sandbox_id == FIXED_SANDBOX_ID
    assert result.status == "started"
    assert len(runtime.specs) == 1


@pytest.mark.anyio
async def test_default_id_has_prefix_and_128_bits_of_lowercase_hex() -> None:
    """The default ID must contain a prefix and 128 random bits in hex."""
    runtime = RecordingRuntime()
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
        RecordingRuntime(error),
        id_factory=lambda: FIXED_SANDBOX_ID,
    )

    with pytest.raises(SandboxCreateFailed) as captured:
        await service.create()

    assert captured.value is error
