"""Exercise typed SDK responses at the untrusted JSON boundary."""

from dataclasses import FrozenInstanceError
from datetime import datetime, timezone

import pytest

from agent_sandbox.errors import ProtocolError
from agent_sandbox.models import (
    CommandResult,
    ReadyResult,
    RenewalResult,
    SandboxInfo,
    WorkspaceInfo,
)

SBX = "sbx_" + "a" * 32
OTHER_SBX = "sbx_" + "c" * 32
WSP = "wsp_" + "b" * 32
CREATED = "2026-11-03T01:02:03.123456Z"
EXPIRES = "2026-11-03T01:12:03.123456+00:00"


@pytest.fixture
def sandbox_payload():
    """Provide a complete successful sandbox response with diagnostic fields."""
    return {
        "sandbox_id": SBX,
        "status": "running",
        "created_at": CREATED,
        "expires_at": EXPIRES,
        "reason": None,
        "message": "available",
        "workspace_id": WSP,
    }


def test_sandbox_parses_utc_times_and_ignores_future_fields(sandbox_payload):
    """UTC precision and future response fields survive API evolution."""
    sandbox_payload["future"] = {"anything": True}
    info = SandboxInfo.from_payload(sandbox_payload, expected_id=SBX, creation=True)
    assert info.sandbox_id == SBX and info.workspace_id == WSP
    assert info.status == "running" and info.message == "available" and info.reason is None
    assert info.created_at == datetime(2026, 11, 3, 1, 2, 3, 123456, tzinfo=timezone.utc)
    assert info.expires_at == datetime(2026, 11, 3, 1, 12, 3, 123456, tzinfo=timezone.utc)
    with pytest.raises(FrozenInstanceError):
        info.status = "changed"


def test_legacy_query_times_must_be_a_complete_null_pair(sandbox_payload):
    """Old query records may lack both times while new creations may not."""
    sandbox_payload.update(created_at=None, expires_at=None)
    info = SandboxInfo.from_payload(sandbox_payload)
    assert info.created_at is None and info.expires_at is None
    with pytest.raises(ProtocolError):
        SandboxInfo.from_payload(sandbox_payload, creation=True)


@pytest.mark.parametrize(
    "field,value",
    [
        ("sandbox_id", "../secret"),
        ("sandbox_id", "sbx_" + "A" * 32),
        ("sandbox_id", 8),
        ("status", ""),
        ("status", 1),
        ("created_at", None),
        ("expires_at", None),
        ("reason", {}),
        ("message", False),
        ("workspace_id", "wsp_bad"),
    ],
)
def test_sandbox_rejects_malformed_required_and_optional_fields(
    sandbox_payload, field, value
):
    """Malformed successful responses fail without exposing response values."""
    sandbox_payload[field] = value
    with pytest.raises(ProtocolError) as caught:
        SandboxInfo.from_payload(sandbox_payload)
    assert str(caught.value) == "invalid server response"


@pytest.mark.parametrize("field", ["sandbox_id", "status", "created_at", "expires_at"])
def test_sandbox_requires_response_keys(sandbox_payload, field):
    """Missing mandatory values cannot silently become local defaults."""
    del sandbox_payload[field]
    with pytest.raises(ProtocolError):
        SandboxInfo.from_payload(sandbox_payload)


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-11-03T01:02:03",
        "2026-11-03T01:02:03+08:00",
        "2026-11-03 01:02:03Z",
        "20261103T010203Z",
        "2026-11-03T01:02:03-00:00",
        "2026-13-03T01:02:03Z",
        "invalid",
        1,
        True,
    ],
)
def test_sandbox_rejects_non_rfc3339_or_non_utc_times(sandbox_payload, timestamp):
    """Only explicit RFC3339 UTC timestamps enter typed response models."""
    sandbox_payload["created_at"] = timestamp
    with pytest.raises(ProtocolError):
        SandboxInfo.from_payload(sandbox_payload)


def test_sandbox_optional_creation_fields_and_future_status_are_compatible(sandbox_payload):
    """Optional diagnostics and future status strings need no SDK release."""
    for field in ("reason", "message", "workspace_id"):
        del sandbox_payload[field]
    sandbox_payload["status"] = "future_state"
    info = SandboxInfo.from_payload(sandbox_payload, creation=True)
    assert info.status == "future_state"
    assert info.reason is None and info.message is None and info.workspace_id is None


def test_workspace_requires_created_time_and_nullable_holder():
    """Workspace metadata includes its UTC creation time and explicit holder."""
    info = WorkspaceInfo.from_payload(
        {
            "workspace_id": WSP,
            "status": "available",
            "created_at": CREATED,
            "sandbox_id": None,
            "future": 1,
        },
        expected_id=WSP,
    )
    assert info.workspace_id == WSP and info.sandbox_id is None
    assert info.created_at.microsecond == 123456
    with pytest.raises(FrozenInstanceError):
        info.status = "changed"


@pytest.mark.parametrize(
    "patch",
    [
        {"workspace_id": "../x"},
        {"status": False},
        {"status": ""},
        {"created_at": None},
        {"sandbox_id": "bad"},
    ],
)
def test_workspace_rejects_invalid_metadata(patch):
    """Workspace success bodies validate IDs, time, state and holder."""
    payload = {
        "workspace_id": WSP,
        "status": "available",
        "created_at": CREATED,
        "sandbox_id": None,
    }
    payload.update(patch)
    with pytest.raises(ProtocolError):
        WorkspaceInfo.from_payload(payload)


@pytest.mark.parametrize("field", ["workspace_id", "status", "created_at", "sandbox_id"])
def test_workspace_requires_all_documented_fields(field):
    """A missing holder differs from an explicitly unoccupied workspace."""
    payload = {
        "workspace_id": WSP,
        "status": "available",
        "created_at": CREATED,
        "sandbox_id": None,
    }
    del payload[field]
    with pytest.raises(ProtocolError):
        WorkspaceInfo.from_payload(payload)


def test_command_result_preserves_nonzero_exit_and_plaintext():
    """Command failures and Unicode output remain ordinary result data."""
    result = CommandResult.from_payload(
        {"exit_code": 17, "stdout": "你好\n", "stderr": "failed", "future": True}
    )
    assert (result.exit_code, result.stdout, result.stderr) == (17, "你好\n", "failed")
    with pytest.raises(FrozenInstanceError):
        result.exit_code = 0


@pytest.mark.parametrize(
    "payload",
    [
        {"exit_code": True, "stdout": "", "stderr": ""},
        {"exit_code": 0.0, "stdout": "", "stderr": ""},
        {"exit_code": 0, "stdout": None, "stderr": ""},
        {"exit_code": 0, "stdout": "", "stderr": []},
        {"exit_code": 0, "stdout": ""},
    ],
)
def test_command_result_rejects_invalid_success_fields(payload):
    """Wrong output and numeric types cannot masquerade as command results."""
    with pytest.raises(ProtocolError):
        CommandResult.from_payload(payload)


def test_renewal_parses_expiry_and_readiness_requires_ready():
    """Lifecycle results preserve IDs and enforce readiness semantics."""
    result = RenewalResult.from_payload(
        {"sandbox_id": SBX, "expires_at": EXPIRES}, expected_id=SBX
    )
    assert result.expires_at == datetime(2026, 11, 3, 1, 12, 3, 123456, tzinfo=timezone.utc)
    assert ReadyResult.from_payload(
        {"sandbox_id": SBX, "status": "ready"}, expected_id=SBX
    ) == ReadyResult(SBX)
    with pytest.raises(ProtocolError):
        ReadyResult.from_payload({"sandbox_id": SBX, "status": "running"})
    with pytest.raises(ProtocolError):
        RenewalResult.from_payload({"sandbox_id": SBX, "expires_at": None})


@pytest.mark.parametrize(
    "model,payload",
    [
        (
            SandboxInfo,
            {
                "sandbox_id": SBX,
                "status": "running",
                "created_at": CREATED,
                "expires_at": EXPIRES,
            },
        ),
        (
            WorkspaceInfo,
            {
                "workspace_id": WSP,
                "status": "available",
                "created_at": CREATED,
                "sandbox_id": None,
            },
        ),
        (RenewalResult, {"sandbox_id": SBX, "expires_at": EXPIRES}),
        (ReadyResult, {"sandbox_id": SBX, "status": "ready"}),
    ],
)
def test_resource_results_reject_mismatched_response_id(model, payload):
    """A successful response must belong to the requested resource."""
    with pytest.raises(ProtocolError):
        model.from_payload(payload, expected_id=OTHER_SBX)


@pytest.mark.parametrize(
    "model", [SandboxInfo, WorkspaceInfo, CommandResult, RenewalResult, ReadyResult]
)
@pytest.mark.parametrize("payload", [None, [], "secret", 1])
def test_models_require_json_objects(model, payload):
    """All typed result parsers reject non-object responses consistently."""
    with pytest.raises(ProtocolError) as caught:
        model.from_payload(payload)
    assert "secret" not in str(caught.value)
