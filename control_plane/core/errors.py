"""Stable domain errors for sandbox operations."""




class SandboxError(RuntimeError):
    """Base class for stable sandbox operation failures."""


class SandboxImageUnavailable(SandboxError):
    """Report that a fixed Agent or entry proxy image is unavailable."""


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


class SandboxEndpointUnavailable(SandboxError):
    """Report an existing sandbox whose dedicated entry cannot be verified."""


class SandboxDeleteConflict(SandboxError):
    """Report ambiguous or conflicting ownership of deletion candidates."""


class SandboxDeleteFailed(SandboxError):
    """Report incomplete sandbox deletion that can converge through a retry."""

    def __init__(self, sandbox_id: str) -> None:
        """Store the public ID needed to rediscover and retry remaining resources."""
        super().__init__("sandbox deletion was incomplete")
        self.sandbox_id = sandbox_id


class SandboxMetadataUnavailable(SandboxError):
    """Report an unavailable lifecycle store with a traceable public sandbox ID."""

    def __init__(self, sandbox_id: str) -> None:
        """Keep storage details private while identifying the affected instance."""
        super().__init__("sandbox lifecycle metadata is unavailable")
        self.sandbox_id = sandbox_id


class SandboxMetadataConflict(SandboxError):
    """Reject conflicting lifecycle identity or an occupied sandbox metadata ID."""

    def __init__(self, sandbox_id: str) -> None:
        """Identify the instance whose metadata cannot safely be used."""
        super().__init__("sandbox lifecycle metadata conflict")
        self.sandbox_id = sandbox_id


class SandboxRenewConflict(SandboxError):
    """Reject an unmanaged, ineligible or concurrently changed renewal target."""

    def __init__(self, sandbox_id: str) -> None:
        """Identify the rejected renewal without exposing persistence or runtime details."""
        super().__init__("sandbox renewal conflict")
        self.sandbox_id = sandbox_id


class SandboxMetadataCleanupFailed(SandboxError):
    """Report incomplete metadata cleanup after runtime rollback or deletion."""

    def __init__(self, sandbox_id: str) -> None:
        """Retain the diagnostic ID needed to inspect and retry partial completion."""
        super().__init__("sandbox lifecycle metadata cleanup failed")
        self.sandbox_id = sandbox_id


class SandboxCreateUncertain(SandboxError):
    """Report an uncertain runtime creation outcome whose metadata is retained."""

    def __init__(self, sandbox_id: str) -> None:
        """Expose the sandbox ID for reconciliation without leaking backend details."""
        super().__init__("sandbox creation outcome is uncertain")
        self.sandbox_id = sandbox_id
