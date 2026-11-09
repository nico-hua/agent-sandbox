"""Offline seven-resource Kubernetes transport with bounded races and redacted Secret diagnostics."""

import copy
import threading

from kubernetes import client

from kubernetes_network_fakes import FakeNetworking, POLICY_UID
from kubernetes_service_fakes import FakeServices, SERVICE_UID
from test_kubernetes_create import CreateClient
from test_kubernetes_query import CREATED, UID

PREFIX = "io.agent-sandbox."
ROLE = PREFIX + "resource"
ROLE_UIDS = {
    "container": UID,
    "network-policy": POLICY_UID,
    "agent-service": SERVICE_UID,
    "proxy-access": "d5667ab6-3f02-40ba-9b29-b1967503a3f0",
    "proxy": "e5667ab6-3f02-40ba-9b29-b1967503a3f0",
    "agent-proxy-ingress": "f5667ab6-3f02-40ba-9b29-b1967503a3f0",
    "proxy-isolation": "15667ab6-3f02-40ba-9b29-b1967503a3f0",
}


class RedactedSecret(client.V1Secret):
    """Preserve SDK fields while suppressing reversible credential bytes in diagnostics."""

    def __repr__(self):
        """Return a constant representation safe for assertion and traceback locals."""
        return "<Secret credential redacted>"

    def __str__(self):
        """Keep string conversion as safe as ordinary object representation."""
        return self.__repr__()

    def to_str(self):
        """Override the SDK diagnostic convenience method without changing serialization."""
        return self.__repr__()


class RedactedSecretList(client.V1SecretList):
    """Avoid the SDK list representation recursively serializing credential data."""

    def __repr__(self):
        """Report only inventory cardinality and never nested Secret content."""
        return f"<SecretList count={len(self.items)} redacted>"

    def __str__(self):
        """Provide the same redaction during implicit string formatting."""
        return self.__repr__()

    def to_str(self):
        """Keep the SDK pretty-print entrypoint credential-free."""
        return self.__repr__()


def _redact(value):
    """Copy SDK values and replace Secret diagnostic methods before retaining requests or responses."""
    result = copy.deepcopy(value)
    if isinstance(result, client.V1Secret):
        result.__class__ = RedactedSecret
    return result


def _matches(resource, namespace, kwargs):
    """Implement namespace, equality labels and exact-name SDK field selectors without hiding other roles."""
    if resource.metadata.namespace != namespace:
        return False
    for selector in kwargs.get("label_selector", "").split(","):
        if selector:
            key, separator, value = selector.partition("=")
            if not separator or (resource.metadata.labels or {}).get(key) != value:
                return False
    for selector in kwargs.get("field_selector", "").split(","):
        if selector:
            key, separator, value = selector.partition("=")
            if key != "metadata.name" or not separator or resource.metadata.name != value:
                return False
    return True


class ProxyNetworking(FakeNetworking):
    """Retain the old deny-all policy slot and independent proxy policy objects."""

    def __init__(self, owner):
        """Keep the existing policy fake's configuration surface and add side-policy storage."""
        super().__init__(owner)
        self.policies = {}

    def list_namespaced_network_policy(self, namespace, **kwargs):
        """List the complete matching policy inventory through the owner transport."""
        return self.owner._list("network_policy", namespace, kwargs)

    def read_namespaced_network_policy(self, name, namespace, **kwargs):
        """Read exactly the requested base or side policy."""
        return self.owner._read("network_policy", name, namespace, kwargs)

    def create_namespaced_network_policy(self, namespace, **kwargs):
        """Commit one uniquely named policy with a distinct API-assigned UID."""
        return self.owner._create("network_policy", namespace, kwargs)

    def delete_namespaced_network_policy(self, name, namespace, **kwargs):
        """Delete one original policy only when UID and version preconditions match."""
        return self.owner._delete("network_policy", name, namespace, kwargs)

    def patch_namespaced_network_policy(self, name, namespace, **kwargs):
        """Apply guarded annotation JSON Patch without a second transport."""
        return self.owner._patch("network_policy", name, namespace, kwargs)


class ProxyServices(FakeServices):
    """Reuse the existing allocated Service fake with exact discovery and precondition enforcement."""

    def list_namespaced_service(self, namespace, **kwargs):
        """Filter the existing single-Service inventory using actual SDK selectors."""
        return self.owner._list("service", namespace, kwargs)

    def read_namespaced_service(self, name, namespace, **kwargs):
        """Read the Service only under its exact namespace and deterministic name."""
        return self.owner._read("service", name, namespace, kwargs)

    def create_namespaced_service(self, namespace, **kwargs):
        """Delegate Service allocation while retaining common staged failure controls."""
        return self.owner._create("service", namespace, kwargs)

    def delete_namespaced_service(self, name, namespace, **kwargs):
        """Protect deletion of the original allocated Service with exact preconditions."""
        return self.owner._delete("service", name, namespace, kwargs)

    def patch_namespaced_service(self, name, namespace, **kwargs):
        """Apply metadata-only UID/version guarded JSON Patch."""
        return self.owner._patch("service", name, namespace, kwargs)


class FakeProxyClient(CreateClient):
    """Model two Pods, three policies, a Secret and a Service without constructing SDK clients.

    ``faults[(verb, role)]`` raises after an optional create/delete commit;
    ``no_commit`` prevents selected role POSTs; ``stuck`` retains selected DELETEs;
    ``barriers[(verb, role)] = (entered, release)`` holds a worker for at most three
    seconds. ``replacement[role]`` replaces a resource after DELETE, and
    ``startup[role]`` selects running/not-ready/pending/failed/image states.
    ``trace`` retains (verb, role), and ``requests`` retains redacted SDK arguments.
    """

    def __init__(self):
        """Keep the old fake classes untouched and initialize isolated companion state."""
        super().__init__()
        self.proxy_pod = self.secret = None
        self.networking, self.services = ProxyNetworking(self), ProxyServices(self)
        self.trace, self.requests = [], []
        self.faults, self.barriers, self.replacement = {}, {}, {}
        self.no_commit, self.stuck = set(), set()
        self.startup = {"container": "running", "proxy": "running"}
        self.inventories, self.continuations = {}, {}

    def objects(self, kind=None):
        """Return live object references for deliberate identity or configuration tampering in tests."""
        groups = {
            "pod": [self.pod, self.proxy_pod],
            "network_policy": [self.networking.policy, *self.networking.policies.values()],
            "secret": [self.secret], "service": [self.services.service],
        }
        return [item for key, values in groups.items() if kind is None or key == kind
                for item in values if item is not None]

    def by_role(self, role):
        """Find a live controlled resource without copying or serializing credential fields."""
        return next((item for item in self.objects() if item.metadata.labels.get(ROLE) == role), None)

    def _store(self, kind, resource, *, role=None):
        """Update only the addressed resource slot while retaining compatibility with old triple helpers."""
        role = role if role is not None else resource.metadata.labels[ROLE]
        if kind == "pod":
            if role == "container":
                self.pod = resource
            else:
                self.proxy_pod = resource
        elif kind == "network_policy":
            if role == "network-policy":
                self.networking.policy = resource
            elif resource is None:
                self.networking.policies.pop(role, None)
            else:
                self.networking.policies[role] = resource
        elif kind == "secret":
            self.secret = resource
        else:
            self.services.service = resource

    def _record(self, method, kind, role, args, kwargs):
        """Record finite transport calls with redacted Secret bodies and enforce bounded worker barriers."""
        safe = {key: _redact(value) for key, value in kwargs.items()}
        self.trace.append((method, role))
        self.requests.append((method, kind, args, safe))
        self.events.append((f"{kind}-{method}", threading.get_ident()))
        facade = self.networking if kind == "network_policy" else self.services if kind == "service" else None
        if facade is not None:
            facade.calls.append((method, args, safe))
        barrier = self.barriers.get((method, role))
        if barrier is not None:
            entered, release = barrier
            entered.set()
            if not release.wait(3):
                raise AssertionError("controlled Kubernetes worker barrier was not released")

    def _raise_fault(self, method, role):
        """Raise a staged transport/API failure without interpolating resource or credential data."""
        failure = self.faults.get((method, role))
        if failure is not None:
            raise failure

    def _find(self, kind, name, namespace):
        """Resolve precise API object identity and raise an actual 404 for any other name."""
        resource = next((item for item in self.objects(kind)
                         if item.metadata.name == name and item.metadata.namespace == namespace), None)
        if resource is None:
            raise client.ApiException(status=404)
        return resource

    def _list(self, kind, namespace, kwargs):
        """Return one controlled selector-filtered page, including duplicate or unknown roles when injected."""
        self._record("list", kind, kind, (namespace,), kwargs)
        self._raise_fault("list", kind)
        if kind == "pod":
            self.calls.append((namespace, kwargs))
        facade = self.networking if kind == "network_policy" else self.services if kind == "service" else None
        source = self.inventories.get(kind)
        if source is None and facade is not None:
            source = facade.inventory
        if source is None:
            source = self.objects(kind)
        items = [_redact(item) for item in source if _matches(item, namespace, kwargs)]
        list_type = {"pod": client.V1PodList, "network_policy": client.V1NetworkPolicyList,
                     "secret": RedactedSecretList, "service": client.V1ServiceList}[kind]
        return list_type(items=items, metadata=client.V1ListMeta(_continue=self.continuations.get(kind)))

    def _read(self, kind, name, namespace, kwargs):
        """Read a fresh exact-name copy and apply independent Pod startup observations."""
        resource = self._find(kind, name, namespace)
        role = resource.metadata.labels[ROLE]
        self._record("read", kind, role, (name, namespace), kwargs)
        self._raise_fault("read", role)
        if kind == "pod":
            self.read_calls.append((name, namespace, kwargs))
        result = _redact(resource)
        if kind == "pod":
            state = self.startup.get(role, "running")
            if state == "pending":
                result.status = client.V1PodStatus(phase="Pending")
            elif state == "failed":
                result.status.phase = "Failed"
                result.status.conditions[0].status = "False"
                result.status.container_statuses[0].ready = False
                result.status.container_statuses[0].state = client.V1ContainerState(
                    terminated=client.V1ContainerStateTerminated(exit_code=1, finished_at=CREATED))
            elif state == "not-ready":
                result.status.conditions[0].status = "False"
                result.status.container_statuses[0].ready = False
            elif state == "image":
                result.status.phase = "Pending"
                result.status.container_statuses[0].state = client.V1ContainerState(
                    waiting=client.V1ContainerStateWaiting(reason="ErrImageNeverPull"))
        return result

    def _create(self, kind, namespace, kwargs):
        """Assign distinct canonical UIDs, optionally commit once, and then deliver or lose the POST response."""
        body = _redact(kwargs["body"])
        role = body.metadata.labels[ROLE]
        self._record("create", kind, role, (namespace,), {**kwargs, "body": body})
        if any(item.metadata.name == body.metadata.name for item in self.objects(kind)):
            raise client.ApiException(status=409)
        if kind == "pod":
            self.create_calls.append((namespace, {**kwargs, "body": body}))
        if role not in self.no_commit:
            body.metadata.uid = ROLE_UIDS[role]
            body.metadata.resource_version = str(100 + list(ROLE_UIDS).index(role) * 100)
            if kind == "service":
                # Reuse the established Service fake's allocation behavior. Its
                # independent controls stay available to old triple helpers.
                body = FakeServices.create_namespaced_service(self.services, namespace, **kwargs)
            if kind == "pod":
                body.spec.node_name = "test-worker"
                name, image = body.spec.containers[0].name, body.spec.containers[0].image
                body.status = client.V1PodStatus(phase="Running", pod_ip="10.244.0.12",
                    conditions=[client.V1PodCondition(type="Ready", status="True")],
                    container_statuses=[client.V1ContainerStatus(name=name, image=image,
                        image_id="sha256:test", ready=True, restart_count=0,
                        state=client.V1ContainerState(running=client.V1ContainerStateRunning(started_at=CREATED)))])
            self._store(kind, body)
        self._raise_fault("create", role)
        return _redact(body)

    def _preconditions(self, resource, kwargs):
        """Reject absent or stale DELETE UID/version guards exactly as an API conflict."""
        guard = kwargs["body"].preconditions
        if guard is None or (guard.uid, guard.resource_version) != (
            resource.metadata.uid, resource.metadata.resource_version,
        ):
            raise client.ApiException(status=409)

    def _delete(self, kind, name, namespace, kwargs):
        """Check fresh preconditions, optionally retain/replace the resource, and never retry a mutation."""
        resource = self._find(kind, name, namespace)
        role = resource.metadata.labels[ROLE]
        self._record("delete", kind, role, (name, namespace), kwargs)
        self._preconditions(resource, kwargs)
        if kind == "pod":
            self.delete_calls.append((name, namespace, kwargs))
        if role not in self.stuck:
            self._store(kind, None, role=role)
        if role in self.replacement:
            self._store(kind, _redact(self.replacement[role]), role=role)
        self._raise_fault("delete", role)
        return client.V1Status(status="Success")

    def _patch(self, kind, name, namespace, kwargs):
        """Implement metadata annotation JSON Patch requiring UID and resourceVersion tests before writes."""
        resource = self._find(kind, name, namespace)
        role = resource.metadata.labels[ROLE]
        self._record("patch", kind, role, (name, namespace), kwargs)
        self._raise_fault("patch", role)
        operations = kwargs["body"]
        guards = {operation["path"]: operation.get("value") for operation in operations if operation["op"] == "test"}
        if guards.get("/metadata/uid") != resource.metadata.uid or guards.get(
            "/metadata/resourceVersion",
        ) != resource.metadata.resource_version:
            raise client.ApiException(status=409)
        seen = set()
        for operation in operations:
            path, verb = operation["path"], operation["op"]
            if verb == "test":
                seen.add(path)
                continue
            if seen != {"/metadata/uid", "/metadata/resourceVersion"}:
                raise AssertionError("annotation writes must follow both identity guards")
            if verb not in {"add", "replace"} or not path.startswith("/metadata/annotations"):
                raise AssertionError("only metadata annotation updates belong in lifecycle patches")
            if path == "/metadata/annotations":
                resource.metadata.annotations = copy.deepcopy(operation["value"])
            else:
                key = path.removeprefix("/metadata/annotations/").replace("~1", "/").replace("~0", "~")
                if resource.metadata.annotations is None:
                    resource.metadata.annotations = {}
                resource.metadata.annotations[key] = copy.deepcopy(operation["value"])
        resource.metadata.resource_version = str(int(resource.metadata.resource_version) + 1)
        return _redact(resource)

    def list_namespaced_pod(self, namespace, **kwargs):
        """List Agent and proxy Pods with complete same-sandbox selector semantics."""
        return self._list("pod", namespace, kwargs)

    def read_namespaced_pod(self, name, namespace, **kwargs):
        """Read an exact Agent or proxy name without conflating their startup status."""
        return self._read("pod", name, namespace, kwargs)

    def create_namespaced_pod(self, namespace, **kwargs):
        """Create an Agent or proxy Pod through the one worker-owned transport."""
        return self._create("pod", namespace, kwargs)

    def delete_namespaced_pod(self, name, namespace, **kwargs):
        """Apply exact original-UID and version guards to either Pod."""
        return self._delete("pod", name, namespace, kwargs)

    def patch_namespaced_pod(self, name, namespace, **kwargs):
        """Publish guarded lifecycle annotations on either Pod."""
        return self._patch("pod", name, namespace, kwargs)

    def list_namespaced_secret(self, namespace, **kwargs):
        """List only matching credentials with redacted diagnostic representation."""
        return self._list("secret", namespace, kwargs)

    def read_namespaced_secret(self, name, namespace, **kwargs):
        """Read an exact Secret without exposing token values in retained arguments."""
        return self._read("secret", name, namespace, kwargs)

    def create_namespaced_secret(self, namespace, **kwargs):
        """Accept one immutable credential resource with a distinct original UID."""
        return self._create("secret", namespace, kwargs)

    def delete_namespaced_secret(self, name, namespace, **kwargs):
        """Delete only the original credential under fresh UID/version guards."""
        return self._delete("secret", name, namespace, kwargs)

    def patch_namespaced_secret(self, name, namespace, **kwargs):
        """Publish credential-free lifecycle metadata on the immutable Secret."""
        return self._patch("secret", name, namespace, kwargs)

    def patch_namespaced_service(self, name, namespace, **kwargs):
        """Forward guarded Service annotations through the established Service facade."""
        return self.services.patch_namespaced_service(name, namespace, **kwargs)
