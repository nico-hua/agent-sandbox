"""Use the standard factory with custom images registered in control_plane/.env."""

from control_plane.app import create_app

__all__ = ["create_app"]
