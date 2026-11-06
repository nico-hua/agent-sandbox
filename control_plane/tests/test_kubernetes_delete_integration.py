"""Double-opt-in deletion of uniquely owned test Pods with independent fixture cleanup."""

import asyncio
import os
import time

import pytest
from kubernetes import client

from control_plane.core.errors import SandboxDeleteConflict, SandboxNotFound
from test_kubernetes_query_integration import _verify_owned, owned_primary_pod

pytestmark = [
    pytest.mark.kubernetes_integration,
    pytest.mark.kubernetes_resource_integration,
    pytest.mark.timeout(100),
    pytest.mark.skipif(
        os.getenv("RUN_KUBERNETES_INTEGRATION") != "1"
        or os.getenv("RUN_KUBERNETES_RESOURCE_INTEGRATION") != "1"
        or not os.getenv("KUBERNETES_TEST_CONTEXT")
        or not os.getenv("KUBERNETES_TEST_NAMESPACE"),
        reason="both Kubernetes opt-ins, context and namespace are required for owned Pod deletion",
    ),
]


def test_real_delete_rejects_wrong_transaction_then_confirms_idempotent_absence(owned_primary_pod):
    """Keep a mismatched transaction intact, then delete only the original UID and confirm repeat safety."""
    runtime, record, api, expected, uid = owned_primary_pod

    async def verify():
        """Exercise Runtime deletion without using it as the fixture's only cleanup path."""
        deadline = time.monotonic() + 45
        while (await runtime.get(record.sandbox_id)).status != "ready":
            assert time.monotonic() < deadline, "test Agent did not become ready within its bound"
            await asyncio.sleep(0.1)
        with pytest.raises(SandboxDeleteConflict):
            await runtime.delete(record.sandbox_id, expected_transaction_id="txn_" + "f" * 32)
        remaining = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        _verify_owned(remaining, expected, uid)
        assert remaining.metadata.deletion_timestamp is None
        await runtime.delete(record.sandbox_id, expected_transaction_id=record.transaction_id)
        with pytest.raises(SandboxNotFound):
            await runtime.get(record.sandbox_id)
        with pytest.raises(client.ApiException) as captured:
            api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
        assert captured.value.status == 404
        await runtime.delete(record.sandbox_id, expected_transaction_id=record.transaction_id)

    asyncio.run(verify())


def test_real_delete_waits_for_an_already_deleting_original_pod(owned_primary_pod):
    """Join an independently requested graceful deletion rather than resubmitting or forcing it."""
    runtime, record, api, expected, uid = owned_primary_pod
    current = api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
    _verify_owned(current, expected, uid)
    api.delete_namespaced_pod(
        expected["name"], expected["namespace"],
        body=client.V1DeleteOptions(preconditions=client.V1Preconditions(uid=uid)),
        _request_timeout=(0.5, 0.5),
    )
    asyncio.run(runtime.delete(record.sandbox_id, expected_transaction_id=record.transaction_id))
    with pytest.raises(client.ApiException) as captured:
        api.read_namespaced_pod(expected["name"], expected["namespace"], _request_timeout=(0.5, 0.5))
    assert captured.value.status == 404
