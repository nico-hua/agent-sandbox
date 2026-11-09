"""Shared in-memory NetworkingV1 test boundary; no independent client or resource close ownership."""

import copy
import threading

from kubernetes import client

POLICY_UID = "a4667ab6-3f02-40ba-9b29-b1967503a3f0"


class FakeNetworking:
    """Model NetworkPolicy state separately from Pod state and write-response delivery."""

    def __init__(self, owner):
        """Retain the shared Core client owner and isolated, controllable policy state."""
        self.owner = owner
        self.policy = None
        self.inventory = None
        self.continuation = None
        self.calls = []
        self.create_error = self.read_error = self.list_error = self.delete_error = None
        self.commit = self.remove_on_delete = True
        self.create_entered = self.create_release = None
        self.delete_entered = self.delete_release = None

    def _record(self, method, args, kwargs):
        """Record bounded requests and the worker that owns the one shared transport."""
        self.calls.append((method, args, kwargs))
        self.owner.events.append(("policy-" + method, threading.get_ident()))

    def list_namespaced_network_policy(self, namespace, **kwargs):
        """Return one controlled inventory without contacting a cluster."""
        self._record("list", (namespace,), kwargs)
        if self.list_error is not None:
            raise self.list_error
        items = self.inventory if self.inventory is not None else ([] if self.policy is None else [self.policy])
        return client.V1NetworkPolicyList(items=copy.deepcopy(items), metadata=client.V1ListMeta(_continue=self.continuation))

    def read_namespaced_network_policy(self, name, namespace, **kwargs):
        """Return a fresh exact-name observation or an actual not-found error."""
        self._record("read", (name, namespace), kwargs)
        if self.read_error is not None:
            raise self.read_error
        if self.policy is None or name != self.policy.metadata.name:
            raise client.ApiException(status=404)
        return copy.deepcopy(self.policy)

    def create_namespaced_network_policy(self, namespace, **kwargs):
        """Optionally commit before returning or losing the one policy creation response."""
        self._record("create", (namespace,), kwargs)
        if self.create_entered is not None:
            self.create_entered.set()
        if self.create_release is not None and not self.create_release.wait(3):
            raise AssertionError("test did not release policy creation")
        if self.commit:
            self.policy = copy.deepcopy(kwargs["body"])
            self.policy.metadata.uid = POLICY_UID
            self.policy.metadata.resource_version = "200"
        if self.create_error is not None:
            raise self.create_error
        return copy.deepcopy(self.policy)

    def delete_namespaced_network_policy(self, name, namespace, **kwargs):
        """Record exact preconditions and optionally keep a stuck or uncertain policy."""
        self._record("delete", (name, namespace), kwargs)
        if self.delete_entered is not None:
            self.delete_entered.set()
        if self.delete_release is not None and not self.delete_release.wait(3):
            raise AssertionError("test did not release policy deletion")
        if self.remove_on_delete:
            self.policy = None
        if self.delete_error is not None:
            raise self.delete_error
        return client.V1Status(status="Success")
