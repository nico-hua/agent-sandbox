"""Stable domain errors for sandbox operations."""




class SandboxError(RuntimeError):
    """Base class for stable sandbox operation failures."""


class SandboxImageUnavailable(SandboxError):
    """Report that the fixed sandbox image is unavailable."""


class DockerUnavailable(SandboxError):
    """Report that the Docker runtime cannot serve the request."""


class SandboxResourceConflict(SandboxError):
    """Report a conflict with a generated sandbox resource name."""


class SandboxCreateFailed(SandboxError):
    """Report a sandbox creation failure after successful rollback."""


class SandboxCleanupFailed(SandboxError):
    """Report a sandbox creation failure with incomplete cleanup."""

    def __init__(self, sandbox_id: str) -> None:
        """Store the sandbox ID needed to locate managed leftovers."""
        super().__init__("sandbox cleanup failed")
        self.sandbox_id = sandbox_id


class SandboxNotFound(SandboxError):
    """Report that no owned sandbox exists for the requested ID."""


class SandboxQueryFailed(SandboxError):
    """Report an ambiguous or malformed managed Docker state."""
