"""Shared validation for metadata in the sandbox and workspace models."""

from datetime import datetime, timedelta


def _validate_integer(value: int, minimum: int, maximum: int, name: str) -> None:
    """Require an actual integer in range without accepting booleans or coercing inputs."""
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError(f"{name} must be an integer between {minimum} and {maximum}")


def _validate_lifecycle_times(
    created_at: datetime | None, expires_at: datetime | None,
) -> None:
    """Allow legacy absence or a complete ordered pair of timezone-aware UTC times."""
    if created_at is None and expires_at is None:
        return
    for value in (created_at, expires_at):
        if not isinstance(value, datetime) or value.utcoffset() != timedelta(0):
            raise ValueError("lifecycle times must be a complete timezone-aware UTC pair")
    if expires_at < created_at:
        raise ValueError("expires_at must not precede created_at")
