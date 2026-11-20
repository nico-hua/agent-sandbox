"""Isolate trusted process configuration without replacing application authentication."""

import pytest


@pytest.fixture(autouse=True)
def isolated_admin_key_environment(monkeypatch):
    """Prevent a developer shell's credentials from affecting authentication tests."""
    monkeypatch.delenv("AGENT_SANDBOX_API_KEY", raising=False)
