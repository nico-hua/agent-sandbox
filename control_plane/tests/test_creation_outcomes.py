"""Focused checks for trustworthy, sanitized creation recovery information."""

import threading
from dataclasses import FrozenInstanceError
from types import SimpleNamespace

import pytest
from docker.errors import DockerException, NotFound

from control_plane.core import errors
from control_plane.runtime.docker.transaction import (
    DockerCreationTransaction,
    _ResourceRecord,
)
from test_sandbox_runtime import RecordingClient, make_spec


class StatefulResource:
    """Model exact-ID absence and optional lost removal responses."""

    def __init__(self, *, behavior: str = "removed", owned: bool = True) -> None:
        """Set ownership and the daemon's outcome independently of its response."""
        self.id = "network-id"
        self.attrs = {"Labels": {"owner": "ours" if owned else "foreign"}}
        self.behavior = behavior
        self.absent = behavior == "absent"
        self.remove_calls = 0

    def reload(self) -> None:
        """Refresh only resources that still exist by exact identity."""
        if self.absent:
            raise NotFound("private daemon detail")

    def get(self, resource_id: str) -> "StatefulResource":
        """Return this exact resource or Docker's unambiguous absence signal."""
        assert resource_id == self.id
        self.reload()
        return self

    def remove(self) -> None:
        """Commit the configured daemon outcome before optionally losing its reply."""
        self.remove_calls += 1
        self.absent = self.behavior in {"removed", "lost_response"}
        if self.behavior == "lost_response":
            raise DockerException("credential=private")


def rollback_report(resource: StatefulResource):
    """Run production rollback against one fully recorded stateful resource."""
    client = SimpleNamespace(networks=resource)
    record = _ResourceRecord("network", resource.id, {"owner": "ours"})
    return DockerCreationTransaction(lambda: client)._rollback(client, [record])


@pytest.mark.parametrize("behavior", ["removed", "absent", "lost_response"])
def test_rollback_accepts_only_confirmed_exact_id_absence(behavior: str) -> None:
    """An absent resource confirms rollback even when its remove reply was lost."""
    report = rollback_report(StatefulResource(behavior=behavior))
    assert report.resource_state == errors.CreationResourceState.ROLLED_BACK
    if behavior == "lost_response":
        assert report.cleanup_issues == (
            errors.CleanupIssue("network", "remove", "DockerException"),
        )


def test_rollback_checks_successful_remove_response_for_remaining_resource() -> None:
    """A successful SDK response cannot prove that the daemon removed a resource."""
    report = rollback_report(StatefulResource(behavior="remaining"))
    assert report.resource_state == errors.CreationResourceState.REMAINING
    assert report.cleanup_issues == (
        errors.CleanupIssue("network", "verify_removal", "ResourceRemaining"),
    )


def test_rollback_preserves_foreign_resources_and_reports_ownership_issue() -> None:
    """Ownership mismatch must prevent removal and retain a sanitized issue."""
    resource = StatefulResource(owned=False)
    report = rollback_report(resource)
    assert report.resource_state == errors.CreationResourceState.REMAINING
    assert report.cleanup_issues == (
        errors.CleanupIssue("network", "verify_ownership", "OwnershipMismatch"),
    )
    assert resource.remove_calls == 0


def test_creation_failure_defaults_are_immutable_and_legacy_compatible() -> None:
    """Legacy runtime errors retain safe compensation semantics without adapters."""
    assert errors.SandboxError().creation_failure is None
    for error, state in [
        (errors.SandboxCreateFailed(), errors.CreationResourceState.ROLLED_BACK),
        (errors.SandboxResourceConflict(), errors.CreationResourceState.ROLLED_BACK),
        (errors.SandboxImageUnavailable(), errors.CreationResourceState.UNCHANGED),
    ]:
        assert error.creation_failure.resource_state == state
        with pytest.raises(FrozenInstanceError):
            error.creation_failure.phase = "changed"


def test_lost_create_response_stays_unknown_after_known_resources_rollback() -> None:
    """Clearing known IDs cannot rule out resources hidden by a lost create reply."""
    client = RecordingClient()
    client.volumes.error = DockerException("private backend address")
    with pytest.raises(errors.DockerUnavailable) as captured:
        DockerCreationTransaction(lambda: client).create_sync(make_spec(), threading.Event())
    info = captured.value.creation_failure
    assert info.phase == "create_workspace"
    assert info.resource_state == errors.CreationResourceState.UNKNOWN
    assert client.network.removed.is_set()
    assert client.ingress.removed.is_set()


def test_primary_creation_error_survives_close_failure_with_sanitized_issue() -> None:
    """Finalization failures supplement the primary error without replacing it."""
    client = RecordingClient()
    client.images.error = RuntimeError("private image path")

    def fail_close() -> None:
        """Simulate a close error containing content forbidden in diagnostics."""
        raise RuntimeError("token=private")

    client.close = fail_close
    with pytest.raises(errors.SandboxCreateFailed) as captured:
        DockerCreationTransaction(lambda: client).create_sync(make_spec(), threading.Event())
    info = captured.value.creation_failure
    assert info.phase == "check_images"
    assert info.resource_state == errors.CreationResourceState.UNCHANGED
    assert info.cleanup_issues == (errors.CleanupIssue("client", "close", "RuntimeError"),)
    assert "private" not in repr(info)


def test_committed_rollback_factory_failure_carries_unknown_recovery_evidence() -> None:
    """A failed reconciliation connection must identify the unknown resource state."""
    def fail_factory():
        """Fail before inspecting any committed resource."""
        raise RuntimeError("private socket")

    with pytest.raises(errors.SandboxCleanupFailed) as captured:
        DockerCreationTransaction(fail_factory).rollback_committed(make_spec(), ())
    assert captured.value.creation_failure == errors.CreationFailureInfo(
        "rollback_committed", errors.CreationResourceState.UNKNOWN,
        (errors.CleanupIssue("client", "open", "RuntimeError"),),
    )


def test_creation_cleanup_failure_keeps_remaining_evidence_and_primary_cause() -> None:
    """A cleanup failure must retain its exact phase and the original start error."""
    client = RecordingClient()
    start_error = RuntimeError("private launch arguments")
    client.container.start_error = start_error
    client.container.remove_error = RuntimeError("private resource path")
    with pytest.raises(errors.SandboxCleanupFailed) as captured:
        DockerCreationTransaction(lambda: client).create_sync(make_spec(), threading.Event())
    assert captured.value.creation_failure == errors.CreationFailureInfo(
        "start_container", errors.CreationResourceState.REMAINING,
        (
            errors.CleanupIssue("container", "remove", "RuntimeError"),
            errors.CleanupIssue("container", "verify_removal", "ResourceRemaining"),
        ),
    )
    assert isinstance(captured.value.__cause__, errors.SandboxCreateFailed)
    assert captured.value.__cause__.__cause__ is start_error
    assert client.network.removed.is_set()
    assert client.ingress.removed.is_set()
    assert client.volume.removed.is_set()


def test_rollback_unknown_inspection_retains_only_fixed_error_category() -> None:
    """An unreadable exact ID must remain unknown without exposing backend text."""
    resource = StatefulResource()

    def fail_lookup(resource_id: str):
        """Simulate a failed daemon inspection without implying absence."""
        raise DockerException("private daemon socket")

    resource.get = fail_lookup
    report = rollback_report(resource)
    assert report.resource_state == errors.CreationResourceState.UNKNOWN
    assert report.cleanup_issues == (
        errors.CleanupIssue("network", "inspect", "DockerException"),
    )
    assert resource.remove_calls == 0


def test_committed_rollback_close_failure_keeps_unknown_recovery_state() -> None:
    """A close failure must preserve the legacy recovery-record retention contract."""
    client = RecordingClient()

    def fail_close() -> None:
        """Fail after all recorded resources were confirmed absent."""
        raise RuntimeError("private close detail")

    client.close = fail_close
    with pytest.raises(errors.SandboxCleanupFailed) as captured:
        DockerCreationTransaction(lambda: client).rollback_committed(make_spec(), ())
    assert captured.value.creation_failure == errors.CreationFailureInfo(
        "rollback_committed", errors.CreationResourceState.UNKNOWN,
        (errors.CleanupIssue("client", "close", "RuntimeError"),),
    )


def test_committed_rollback_unexpected_failure_still_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Even an internal rollback failure must release the client transport."""
    client = RecordingClient()
    transaction = DockerCreationTransaction(lambda: client)
    failure = RuntimeError("private rollback detail")

    def fail_rollback(client_arg, records):
        """Inject a rollback defect before a structured report can be produced."""
        raise failure

    monkeypatch.setattr(transaction, "_rollback", fail_rollback)
    with pytest.raises(RuntimeError) as captured:
        transaction.rollback_committed(make_spec(), ())
    assert captured.value is failure
    assert client.close_calls == 1
