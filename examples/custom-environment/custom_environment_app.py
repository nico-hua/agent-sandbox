"""Trusted local application factory registering one prebuilt tools environment."""

from fastapi import FastAPI

from control_plane.app import create_app as create_control_app


def create_app() -> FastAPI:
    """Register the tools image without connecting Docker or initializing storage."""
    return create_control_app(additional_environments={"tools": "agent-sandbox-tools:dev"})
