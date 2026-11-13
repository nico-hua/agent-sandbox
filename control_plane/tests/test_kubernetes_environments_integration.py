"""Explicit formal HTTP creation and authenticated execution for preloaded Kubernetes Agent languages."""

import pytest

from test_kubernetes_auto_entry_integration import _formal_http_workflow
from test_kubernetes_persistent_workspace_integration import owned_persistent_workspace, pytestmark
from test_kubernetes_proxy_lifecycle_integration import owned_proxy_lifecycle


@pytest.mark.parametrize("environment,persistent", [("basic", False), ("python", False), ("node", False), ("python", True)])
def test_formal_languages_complete_http_lifecycle(environment, persistent, request, tmp_path, monkeypatch):
    """Use actual HTTP creation/activation/entry and independent UID cleanup, including Python with a private PVC."""
    _formal_http_workflow(persistent, request, tmp_path, monkeypatch, environment=environment)
