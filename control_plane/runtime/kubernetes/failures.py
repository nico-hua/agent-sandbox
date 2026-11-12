"""Shared fixed Kubernetes failure classification, independent of Runtime orchestration."""

from kubernetes import client
from kubernetes.config import ConfigException
from urllib3 import exceptions as transport_errors

from control_plane.core.errors import (
    SandboxError, KubernetesError, KubernetesConfigurationError, KubernetesPermissionDenied,
    KubernetesTimeout, KubernetesConnectionFailed, SandboxNotFound, SandboxQueryFailed,
    SandboxDeleteConflict, SandboxDeleteFailed, SandboxCreateFailed, SandboxCleanupFailed,
)


def _probe_failure(error: Exception) -> SandboxError:
    """Classify SDK and transport failures without using messages, headers or response bodies."""
    if isinstance(error, (KubernetesError, SandboxNotFound, SandboxQueryFailed, SandboxDeleteConflict,
                          SandboxDeleteFailed, SandboxCreateFailed, SandboxCleanupFailed)):
        return error
    if isinstance(error, ConfigException):
        return KubernetesConfigurationError()
    if isinstance(error, client.ApiException):
        if error.status in {401, 403}:
            return KubernetesPermissionDenied()
        if error.status in {408, 504}:
            return KubernetesTimeout()
    if isinstance(error, transport_errors.MaxRetryError):
        # urllib3 may wrap its final transport failure even with no retries.
        return _probe_failure(error.reason) if isinstance(error.reason, Exception) else KubernetesConnectionFailed()
    if isinstance(error, (TimeoutError, transport_errors.TimeoutError)):
        return KubernetesTimeout()
    return KubernetesConnectionFailed()
