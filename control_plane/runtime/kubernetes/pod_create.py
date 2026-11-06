"""One-shot single-Pod creation, bounded startup and original-UID compensation."""

from datetime import datetime
from threading import Event
from time import monotonic, sleep

from kubernetes import client
from kubernetes.utils.quantity import parse_quantity

from control_plane.core.errors import (
    CleanupIssue, CreationFailureInfo, CreationResourceState, SandboxCleanupFailed,
    SandboxCreateFailed, SandboxDeleteConflict, SandboxQueryFailed,
)
from control_plane.runtime.kubernetes.pod_delete import PodDeletion, _deletion_identity
from control_plane.runtime.kubernetes.pod_query import (
    LABEL_PREFIX, _WAITING_STATES, _valid_observation_time, pod_identity, sandbox_pod_selector,
)

START_TIMEOUT_SECONDS = 30.0
RECONCILE_TIMEOUT_SECONDS = 5.0
_POLL_INTERVAL_SECONDS = 0.1
_REJECTED_STATUS_CODES = frozenset({400, 401, 403, 404, 405, 409, 413, 415, 422, 429})
_DEFAULT_FALSE_PATHS = frozenset({
    ("host_network",), ("host_pid",), ("host_ipc",),
    ("containers", "security_context", "privileged"),
    ("containers", "volume_mounts", "read_only"),
})


class PodCreationCancelled(Exception):
    """Signal worker cancellation only after unchanged or confirmed rolled-back resource evidence."""


def _requested_fields_match(expected, observed, path: tuple[str, ...] = ()) -> bool:
    """Compare builder-supplied fields while allowing API defaults and equivalent resource quantities."""
    if expected is None:
        return True
    # Kubernetes omits these scalar false defaults on serialization. Do not
    # apply this to deny settings whose absence would enable access instead.
    if expected is False and observed is None and path in _DEFAULT_FALSE_PATHS:
        return True
    if isinstance(expected, dict):
        return isinstance(observed, dict) and all(
            _requested_fields_match(value, observed.get(key), (*path, key)) for key, value in expected.items()
        )
    if isinstance(expected, list):
        return isinstance(observed, list) and len(expected) == len(observed) and all(
            _requested_fields_match(left, right, path) for left, right in zip(expected, observed)
        )
    if len(path) >= 2 and path[-2] in {"limits", "requests"} and path[-1] in {"cpu", "memory"}:
        try:
            return parse_quantity(expected) == parse_quantity(observed)
        except (TypeError, ValueError):
            return False
    return type(expected) is type(observed) and expected == observed


class PodCreation:
    """Keep one transaction's original UID across startup, lost responses and sequential cleanup workers."""

    def __init__(self, pod: client.V1Pod, cancelled: Event):
        """Retain only explicit expected metadata and worker progress; perform no external I/O."""
        self.pod, self.cancelled = pod, cancelled
        self.namespace, self.name = pod.metadata.namespace, pod.metadata.name
        self.sandbox_id = pod.metadata.labels[LABEL_PREFIX + "sandbox-id"]
        self.transaction_id = pod.metadata.labels[LABEL_PREFIX + "transaction-id"]
        self.created_at = datetime.fromisoformat(pod.metadata.annotations[LABEL_PREFIX + "created-at"])
        self.expires_at = datetime.fromisoformat(pod.metadata.annotations[LABEL_PREFIX + "expires-at"])
        self.uid: str | None = None
        self.submitted = self.rollback_attempted = False
        self.phase = "preflight"
        self.cleanup_issues: tuple[CleanupIssue, ...] = ()

    def failure(self, phase: str, state: CreationResourceState):
        """Construct fixed public failure text with immutable evidence and no raw SDK diagnostics."""
        error = (SandboxCleanupFailed(self.sandbox_id) if state in {
            CreationResourceState.REMAINING, CreationResourceState.UNKNOWN,
        } else SandboxCreateFailed("kubernetes sandbox creation failed"))
        error.creation_failure = CreationFailureInfo(phase, state, self.cleanup_issues)
        return error

    def _timeout(self, deadline: float) -> tuple[float, float]:
        """Bound each SDK phase by the remaining monotonic operation budget."""
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError()
        phase = min(0.5, remaining / 2)
        return phase, phase

    def _check_cancelled(self) -> None:
        """Stop between SDK calls without pretending to interrupt an in-flight synchronous request."""
        if self.cancelled.is_set():
            raise PodCreationCancelled()

    def _inventory(self, api, deadline: float):
        """Discover every same-sandbox Pod role without transaction filters or pagination."""
        page = api.list_namespaced_pod(
            self.namespace, label_selector=sandbox_pod_selector(self.sandbox_id),
            limit=2, watch=False, _request_timeout=self._timeout(deadline),
        )
        if (
            not isinstance(page.items, list) or page.metadata is None
            or page.metadata._continue not in (None, "")
            or getattr(page.metadata, "remaining_item_count", None) not in (None, 0)
            or len(page.items) > 1
        ):
            raise ValueError("ambiguous Pod inventory")
        return page.items

    def _read(self, api, deadline: float):
        """Read only the deterministic name, distinguishing object absence from other API failures."""
        try:
            return api.read_namespaced_pod(self.name, self.namespace, _request_timeout=self._timeout(deadline))
        except client.ApiException as error:
            if error.status == 404:
                return None
            raise

    def _verify_identity(self, pod) -> str:
        """Check full original business identity and supported layout, never adopting a replacement UID."""
        uid, transaction_id, created_at, expires_at = pod_identity(pod, self.sandbox_id, self.namespace)
        if (transaction_id, created_at, expires_at) != (self.transaction_id, self.created_at, self.expires_at):
            raise ValueError("Pod identity changed")
        _deletion_identity(pod, self.sandbox_id, self.namespace, self.transaction_id)
        if self.uid is not None and uid != self.uid:
            raise ValueError("Pod UID changed")
        return uid

    def _started(self, pod) -> bool:
        """Require an actually running Agent while allowing readiness to remain false."""
        if pod.metadata.deletion_timestamp is not None:
            raise ValueError("Pod is deleting")
        if not _requested_fields_match(self.pod.spec.to_dict(), pod.spec.to_dict()):
            raise ValueError("Pod specification changed")
        status = pod.status
        if status is None:
            return False
        if status.phase not in {None, "Pending", "Running"}:
            raise ValueError("Pod did not start")
        statuses = status.container_statuses
        if statuses in (None, []):
            return False
        if len(statuses) != 1 or statuses[0].name != self.pod.spec.containers[0].name:
            raise ValueError("Agent state is ambiguous")
        state = statuses[0].state
        if state is None or sum(value is not None for value in (state.running, state.waiting, state.terminated)) != 1:
            raise ValueError("Agent state is invalid")
        if state.terminated is not None:
            raise ValueError("Agent exited before creation completed")
        if state.waiting is not None:
            waiting = _WAITING_STATES.get(state.waiting.reason)
            if waiting is None or waiting[0] == "failed":
                raise ValueError("Agent cannot start")
            return False
        return status.phase == "Running" and _valid_observation_time(state.running.started_at)

    def run(self, api) -> str:
        """Preflight, submit once and wait for startup; every failed or canceled mutation is compensated."""
        deadline = monotonic() + START_TIMEOUT_SECONDS
        try:
            self._check_cancelled()
            namespace = api.read_namespace(self.namespace, _request_timeout=self._timeout(deadline))
            if namespace.metadata.name != self.namespace or self._inventory(api, deadline) or self._read(api, deadline) is not None:
                raise ValueError("Pod already exists or namespace is invalid")
            self._check_cancelled()
            self.phase = "submit_pod"
            request_timeout = self._timeout(deadline)
            self.submitted = True
            try:
                response = api.create_namespaced_pod(
                    self.namespace, body=self.pod, _request_timeout=request_timeout,
                )
            except client.ApiException as error:
                if error.status in _REJECTED_STATUS_CODES:
                    # A definite API rejection is evidence that this one POST
                    # did not create the pre-existing/conflicting object.
                    self.submitted = False
                raise
            self.uid = self._verify_identity(response)
            if not _requested_fields_match(self.pod.spec.to_dict(), response.spec.to_dict()):
                raise ValueError("creation response specification changed")
            self.phase = "wait_started"
            while True:
                self._check_cancelled()
                current = self._read(api, deadline)
                if current is not None:
                    self._verify_identity(current)
                    if self._started(current):
                        self._check_cancelled()
                        return self.uid
                sleep(min(_POLL_INTERVAL_SECONDS, max(0.0, deadline - monotonic())))
        except Exception:
            state = self.rollback(api) if self.submitted else CreationResourceState.UNCHANGED
            if self.cancelled.is_set() and state in {CreationResourceState.UNCHANGED, CreationResourceState.ROLLED_BACK}:
                raise PodCreationCancelled() from None
            raise self.failure(self.phase, state) from None

    def _find_created_uid(self, api) -> bool:
        """Resolve a lost POST response by positive matching ownership; repeated absence remains unknown."""
        deadline = monotonic() + RECONCILE_TIMEOUT_SECONDS
        while monotonic() < deadline:
            try:
                candidates = self._inventory(api, deadline)
                pod = self._read(api, deadline)
                if pod is not None:
                    uid = self._verify_identity(pod)
                    if len(candidates) != 1 or self._verify_identity(candidates[0]) != uid:
                        return False
                    self.uid = uid
                    return True
                if candidates:
                    # A stale list/read pair is not proof of absence or
                    # authority to delete a different object.
                    self._verify_identity(candidates[0])
            except (ValueError, TypeError, AttributeError, KeyError, SandboxDeleteConflict, SandboxQueryFailed):
                # Positive conflicting identity/layout evidence cannot be
                # forgotten in order to adopt a later object at the same name.
                return False
            except Exception:
                pass
            sleep(min(_POLL_INTERVAL_SECONDS, max(0.0, deadline - monotonic())))
        return False

    def rollback(self, api) -> CreationResourceState:
        """Use an independent deletion budget and the original UID, preserving unknown or remaining evidence."""
        self.rollback_attempted = True
        if self.uid is None and not self._find_created_uid(api):
            return CreationResourceState.UNKNOWN
        try:
            PodDeletion(api, self.sandbox_id, self.namespace, self.transaction_id, expected_uid=self.uid).run()
            return CreationResourceState.ROLLED_BACK
        except Exception as error:
            self.cleanup_issues = (CleanupIssue("container", "rollback", type(error).__name__),)
            try:
                pod = self._read(api, monotonic() + 1)
                if pod is not None:
                    identity = pod_identity(pod, self.sandbox_id, self.namespace)
                    if identity == (self.uid, self.transaction_id, self.created_at, self.expires_at):
                        return CreationResourceState.REMAINING
            except Exception:
                pass
            return CreationResourceState.UNKNOWN
