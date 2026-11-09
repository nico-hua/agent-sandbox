"""In-memory CoreV1 Service test operations sharing their owning Core client's close boundary."""

import copy
import threading

from kubernetes import client

SERVICE_UID = "c5667ab6-3f02-40ba-9b29-b1967503a3f0"


class FakeServices:
    """Keep accepted Service objects separate from request and response failures."""

    def __init__(self, owner):
        """Prepare isolated state with no SDK clients or external I/O."""
        self.owner = owner
        self.service = None
        self.inventory = None
        self.continuation = None
        self.calls = []
        self.create_error = self.read_error = self.list_error = self.delete_error = None
        self.commit = self.remove_on_delete = True
        self.create_entered = self.create_release = None
        self.delete_entered = self.delete_release = None

    def _record(self, method, args, kwargs):
        """Record finite SDK arguments and the thread borrowing the shared client."""
        self.calls.append((method, args, kwargs))
        self.owner.events.append(("service-" + method, threading.get_ident()))

    def list_namespaced_service(self, namespace, **kwargs):
        """Return exactly one controlled page without contacting a cluster."""
        self._record("list", (namespace,), kwargs)
        if self.list_error is not None:
            raise self.list_error
        items = self.inventory if self.inventory is not None else ([] if self.service is None else [self.service])
        return client.V1ServiceList(items=copy.deepcopy(items), metadata=client.V1ListMeta(_continue=self.continuation))

    def read_namespaced_service(self, name, namespace, **kwargs):
        """Return a fresh exact-name Service or a real not-found response."""
        self._record("read", (name, namespace), kwargs)
        if self.read_error is not None:
            raise self.read_error
        if self.service is None or name != self.service.metadata.name:
            raise client.ApiException(status=404)
        return copy.deepcopy(self.service)

    def create_namespaced_service(self, namespace, **kwargs):
        """Simulate API-assigned UID and internal IP, with optional lost response after commit."""
        self._record("create", (namespace,), kwargs)
        if self.create_entered is not None:
            self.create_entered.set()
        if self.create_release is not None and not self.create_release.wait(3):
            raise AssertionError("test did not release Service POST")
        if self.commit:
            self.service = copy.deepcopy(kwargs["body"])
            self.service.metadata.uid, self.service.metadata.resource_version = SERVICE_UID, "300"
            self.service.spec.cluster_ip = "10.96.0.42"
            self.service.spec.cluster_ips = ["10.96.0.42"]
            self.service.spec.ip_families = ["IPv4"]
            self.service.spec.ip_family_policy = "SingleStack"
        if self.create_error is not None:
            raise self.create_error
        return copy.deepcopy(self.service)

    def delete_namespaced_service(self, name, namespace, **kwargs):
        """Keep precise UID/version preconditions observable during delayed or uncertain deletion."""
        self._record("delete", (name, namespace), kwargs)
        if self.delete_entered is not None:
            self.delete_entered.set()
        if self.delete_release is not None and not self.delete_release.wait(3):
            raise AssertionError("test did not release Service DELETE")
        if self.remove_on_delete:
            self.service = None
        if self.delete_error is not None:
            raise self.delete_error
        return client.V1Status(status="Success")
