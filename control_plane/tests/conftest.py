"""Isolate trusted process configuration without replacing application authentication."""

import pytest


@pytest.fixture(autouse=True)
def isolated_admin_key_environment(monkeypatch):
    """Remove ambient credentials; each test must explicitly configure its own auth mode."""
    monkeypatch.delenv("AGENT_SANDBOX_API_KEY", raising=False)
