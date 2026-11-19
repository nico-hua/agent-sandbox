"""In-memory Docker and Agent transports used by runtime tests."""

from __future__ import annotations

import asyncio
import json
import threading
from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

import httpx
from docker.errors import (
    APIError,
    NotFound,
)

from control_plane.domain.models import (
    ResourceKind,
    SandboxNetwork,
    SandboxSpec,
)


ROLES: tuple[ResourceKind, ...] = (
    "proxy", "container", "ingress", "network", "workspace"
)


class DeletionState:
    """Represent daemon state shared by fresh independent runtime clients."""

    def __init__(self, roles: tuple[ResourceKind, ...] = ROLES) -> None:
        """Seed exactly labelled resources without endpoint or health topology."""
        self.lock = threading.RLock()
        self.events: list[tuple[str, str]] = []
        self.resources: dict[str, dict[str, DeletionResource]] = {
            "containers": {}, "networks": {}, "volumes": {}
        }
        self.original: dict[str, DeletionResource] = {}
        for role in roles:
            self.add(role)

    def add(self, role: ResourceKind, suffix: str = "") -> "DeletionResource":
        """Create one resource with a stable exact identifier and complete labels."""
        resource = DeletionResource(self, role, f"{role}-id{suffix}")
        self.resources[resource.group][resource.key] = resource
        if not suffix:
            self.original[role] = resource
        return resource

    def remaining(self) -> set[str]:
        """Return the remaining roles after observing daemon state under its lock."""
        with self.lock:
            return {r.role for resources in self.resources.values() for r in resources.values()}

    def mutations(self) -> list[tuple[str, str]]:
        """Return only destructive operations from the shared operation log."""
        return [event for event in self.events if event[0] in {"stop", "kill", "remove"}]


SANDBOX_ID = "sbx_0123456789abcdef0123456789abcdef"


TRANSACTION_ID = "txn_0123456789abcdef0123456789abcdef"


def make_spec() -> SandboxSpec:
    """Build one deterministic sandbox specification for runtime tests."""
    return SandboxSpec(
        sandbox_id=SANDBOX_ID,
        container_name=f"agent-sandbox-{SANDBOX_ID}",
        network_name=f"agent-sandbox-{SANDBOX_ID}-internal",
        volume_name=f"agent-sandbox-{SANDBOX_ID}-workspace",
        transaction_id=TRANSACTION_ID,
        access_token="a" * 43,
    )


DELETE_SPEC = make_spec()


class DeletionResource:
    """Model Docker resource reload and lifecycle operations with race hooks."""

    def __init__(self, state: DeletionState, role: ResourceKind, key: str) -> None:
        """Store stable lookup identity separately from mutable inspected identity."""
        self.state, self.role, self.key, self.id = state, role, key, key
        self.group = "containers" if role in {"proxy", "container", "egress"} else (
            "volumes" if role == "workspace" else "networks"
        )
        labels = DELETE_SPEC.labels_for(role)
        self.attrs: dict[str, Any] = (
            {"Config": {"Labels": labels}, "State": {"Status": "running"}}
            if self.group == "containers" else {"Labels": labels, "Name": key}
        )
        self.name = key
        self.reload_calls = 0
        self.reload_hook: Callable[["DeletionResource"], None] | None = None
        self.stop_hook: Callable[[], None] | None = None
        self.stop_error: Exception | None = None
        self.remove_error: Exception | None = None
        self.remove_hook: Callable[[], None] | None = None
        self.remove_keeps_resource = False
        self.stop_calls: list[dict[str, Any]] = []
        self.remove_calls: list[dict[str, Any]] = []

    @property
    def labels(self) -> dict[str, str]:
        """Expose labels using the container and non-container inspect shapes."""
        return self.attrs["Config"]["Labels"] if self.group == "containers" else self.attrs["Labels"]

    def reload(self) -> None:
        """Refresh inspect facts and optionally change ownership during a race."""
        with self.state.lock:
            self.state.events.append(("reload", self.key))
            if self.key not in self.state.resources[self.group]:
                raise NotFound("resource disappeared")
            self.reload_calls += 1
            if self.reload_hook is not None:
                self.reload_hook(self)

    def stop(self, **kwargs: Any) -> None:
        """Record a bounded stop and expose stop failures and ownership races."""
        self.state.events.append(("stop", self.key))
        self.stop_calls.append(kwargs)
        if self.stop_hook is not None:
            self.stop_hook()
        if self.stop_error is not None:
            raise self.stop_error
        self.attrs["State"]["Status"] = "exited"

    def kill(self) -> None:
        """Record a forceful stop if the implementation chooses an explicit kill."""
        self.state.events.append(("kill", self.key))
        self.attrs["State"]["Status"] = "exited"

    def remove(self, **kwargs: Any) -> None:
        """Delete only this exact resource and preserve occupied-resource failures."""
        with self.state.lock:
            self.state.events.append(("remove", self.key))
            self.remove_calls.append(kwargs)
            if self.remove_hook is not None:
                self.remove_hook()
            if self.remove_error is not None:
                raise self.remove_error
            if self.key not in self.state.resources[self.group]:
                raise NotFound("resource disappeared")
            if not self.remove_keeps_resource:
                del self.state.resources[self.group][self.key]


class DeletionManager:
    """Provide real label filtering and exact-ID retrieval against fake daemon state."""

    def __init__(self, state: DeletionState, group: str) -> None:
        """Store daemon collection and optional discovery/lookup hooks."""
        self.state, self.group = state, group
        self.list_calls: list[dict[str, Any]] = []
        self.get_calls: list[str] = []
        self.list_error: Exception | None = None
        self.get_hook: Callable[[str], None] | None = None

    def list(self, **kwargs: Any) -> list[DeletionResource]:
        """Honor Docker label filters instead of returning unfiltered fixtures."""
        with self.state.lock:
            self.list_calls.append(deepcopy(kwargs))
            if self.list_error is not None:
                raise self.list_error
            filters = kwargs["filters"]["label"]
            assert isinstance(filters, list)
            expected = dict(item.split("=", 1) for item in filters)
            assert expected["io.agent-sandbox.managed"] == "true"
            assert expected["io.agent-sandbox.project"] == "agent-sandbox"
            assert expected["io.agent-sandbox.sandbox-id"] == SANDBOX_ID
            assert expected["io.agent-sandbox.resource"] in (*ROLES, "egress")
            return [
                resource for resource in self.state.resources[self.group].values()
                if all(resource.labels.get(key) == value for key, value in expected.items())
            ]

    def get(self, resource_id: str) -> DeletionResource:
        """Read an exact recorded identifier and raise Docker NotFound if absent."""
        with self.state.lock:
            self.get_calls.append(resource_id)
            self.state.events.append(("get", resource_id))
            if self.get_hook is not None:
                self.get_hook(resource_id)
            try:
                return self.state.resources[self.group][resource_id]
            except KeyError as error:
                raise NotFound("resource disappeared") from error


class DeletionClient:
    """Expose only deletion-related SDK collections and observable close hooks."""

    def __init__(self, state: DeletionState | None = None) -> None:
        """Open one new transport facade over optional shared daemon resources."""
        self.state = state if state is not None else DeletionState()
        self.containers = DeletionManager(self.state, "containers")
        self.networks = DeletionManager(self.state, "networks")
        self.volumes = DeletionManager(self.state, "volumes")
        self.close_calls = 0
        self.closed = threading.Event()
        self.close_hook: Callable[[], None] | None = None

    def close(self) -> None:
        """Observe client cleanup and optionally block its final transport close."""
        self.close_calls += 1
        if self.close_hook is not None:
            self.close_hook()
        self.closed.set()


ALL_ROLES = ("egress", *ROLES)


DOMAINS = "io.agent-sandbox.allow-domains"


VERSION = "io.agent-sandbox.network-version"


def egress_spec(domains: tuple[str, ...] = ("EXAMPLE.COM.", "api.example.com")) -> SandboxSpec:
    """Build a canonical nonempty policy independently for each creation attempt."""
    return replace(make_spec(), network=SandboxNetwork(domains))


def configured_inventory(domains: tuple[str, ...] | None = ("example.com",)) -> DeletionClient:
    """Build an inspectable six-resource topology, or a five-resource legacy inventory."""
    spec = egress_spec(domains or ())
    client = DeletionClient(DeletionState(ALL_ROLES if domains else ROLES))
    for role, target in client.state.original.items():
        if domains is not None:
            target.labels.update({VERSION: "1", DOMAINS: json.dumps(list(spec.network.allow_domains), separators=(",", ":"))})
        if target.group == "containers":
            target.attrs["State"] = {"Status": "running", "Health": {"Status": "healthy"}}
            networks = {spec.network_name: {"NetworkID": "network-id", "Aliases": ["sandbox" if role == "container" else "egress"]}}
            if role in {"proxy", "egress"}:
                networks[spec.ingress_network_name] = {"NetworkID": "ingress-id"}
            target.attrs["NetworkSettings"] = {"Networks": networks, "Ports": {}}
            if role == "proxy":
                target.attrs["NetworkSettings"]["Ports"] = {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "49153"}]}
        elif role in {"network", "ingress"}:
            target.attrs.update(Name=spec.network_name if role == "network" else spec.ingress_network_name, Driver="bridge", Internal=role == "network")
    return client


def pause_inventory(domains: Any = ("example.com",)) -> Any:
    """Add observable pause/unpause semantics to the existing realistic inventory."""
    client = configured_inventory(domains)
    for role in ("container", "egress", "proxy"):
        if role not in client.state.original:
            continue
        resource = client.state.original[role]

        def change(paused: bool, resource: Any = resource) -> None:
            """Change actual inspect facts while retaining operation order."""
            client.state.events.append(("pause" if paused else "unpause", resource.key))
            resource.attrs["State"].update(Status="running", Paused=paused)

        resource.pause = lambda change=change: change(True)
        resource.unpause = lambda change=change: change(False)
    return client


ENDPOINT_SPEC = make_spec()


class EndpointManager:
    """Expose only read-only Docker list/get operations for inspect fixtures."""

    def __init__(self, resources: dict[str, Any]) -> None:
        """Store resources and optional read failures."""
        self.resources = resources
        self.error: Exception | None = None

    def list(self, **kwargs: Any) -> list[Any]:
        """Select candidates using Docker label filters without mutation."""
        if self.error:
            raise self.error
        filters = kwargs["filters"]["label"]
        return [r for r in self.resources.values() if all(
            r.attrs.get("Config", {}).get("Labels", r.attrs.get("Labels", {})).get(k) == v
            for k, v in (f.split("=", 1) for f in filters)
        )]

    def get(self, resource_id: str) -> Any:
        """Return only the requested exact ID."""
        return self.resources[resource_id]


def resource(resource_id: str, attrs: dict[str, Any]) -> Any:
    """Create an inspect-only resource with a no-op reload."""
    return SimpleNamespace(id=resource_id, attrs=attrs, reload=lambda: None)


class EndpointClient:
    """Build one healthy sandbox/proxy pair and two owned bridge networks."""

    def __init__(self) -> None:
        """Initialize an actual published port independent of HostConfig."""
        self.sandbox = resource("sandbox-id", {
            "Config": {"Labels": ENDPOINT_SPEC.labels_for("container")},
            "State": {"Status": "running", "Health": {"Status": "healthy"}},
            "NetworkSettings": {"Ports": {}, "Networks": {
                ENDPOINT_SPEC.network_name: {"NetworkID": "internal-id", "Aliases": ["sandbox"]},
            }},
        })
        self.proxy = resource("proxy-id", {
            "Config": {"Labels": ENDPOINT_SPEC.labels_for("proxy")},
            "State": {"Status": "running", "Health": {"Status": "healthy"}},
            "HostConfig": {"PortBindings": {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "9999"}]}},
            "NetworkSettings": {"Ports": {"8080/tcp": [{"HostIp": "127.0.0.1", "HostPort": "49153"}]}, "Networks": {
                ENDPOINT_SPEC.network_name: {"NetworkID": "internal-id"},
                ENDPOINT_SPEC.ingress_network_name: {"NetworkID": "ingress-id"},
            }},
        })
        self.containers = EndpointManager({"sandbox-id": self.sandbox, "proxy-id": self.proxy})
        self.networks = EndpointManager({
            "internal-id": resource("internal-id", {"Name": ENDPOINT_SPEC.network_name, "Driver": "bridge", "Internal": True, "Labels": ENDPOINT_SPEC.labels_for("network")}),
            "ingress-id": resource("ingress-id", {"Name": ENDPOINT_SPEC.ingress_network_name, "Driver": "bridge", "Internal": False, "Labels": ENDPOINT_SPEC.labels_for("ingress")}),
        })
        self.volumes = EndpointManager({})
        self.close_calls = 0

    def close(self) -> None:
        """Record transport cleanup."""
        self.close_calls += 1


def creation_labels(spec: SandboxSpec, role: str) -> dict[str, str]:
    """Build creation metadata independently of the runtime's label serializer."""
    return {
        **spec.labels_for(role),
        "io.agent-sandbox.endpoint-auth-version": "1",
        "io.agent-sandbox.endpoint-services-version": "1",
        "io.agent-sandbox.network-version": "1",
        "io.agent-sandbox.allow-domains": json.dumps(list(spec.network.allow_domains), separators=(",", ":")),
    }


def make_api_error(status_code: int, message: str = "Docker API failed") -> APIError:
    """Build a Docker API error with a stable fake HTTP status."""
    response = SimpleNamespace(
        status_code=status_code,
        url="docker://test",
        reason="Conflict" if status_code == 409 else "Server Error",
    )
    return APIError(message, response=response)


class RecordingResource:
    """Represent one Docker resource and record lifecycle operations."""

    def __init__(
        self,
        kind: str,
        resource_id: str,
        labels: dict[str, str],
        events: list[str],
    ) -> None:
        """Initialize a resource with Docker-like ID and attributes."""
        self.kind = kind
        self.id = resource_id
        self.attrs = {"Labels": labels, "Config": {"Labels": labels}}
        self._events = events
        self.start_error: Exception | None = None
        self.reload_error: Exception | None = None
        self.remove_error: Exception | None = None
        self.start_mutation: Callable[[], None] | None = None
        self.remove_calls: list[dict[str, Any]] = []
        self.removed = threading.Event()
        self.start_entered: threading.Event | None = None
        self.start_release: threading.Event | None = None

    def start(self) -> None:
        """Record that the container start operation was requested."""
        self._events.append(f"{self.kind}.start")
        if self.start_entered is not None:
            self.start_entered.set()
        if self.start_release is not None and not self.start_release.wait(timeout=1):
            raise TimeoutError("test did not release container start")
        if self.start_mutation is not None:
            self.start_mutation()
        if self.start_error is not None:
            raise self.start_error

    def reload(self) -> None:
        """Record an ownership refresh and optionally fail it."""
        self._events.append(f"{self.kind}.reload")
        if self.reload_error is not None:
            raise self.reload_error

    def remove(self, **kwargs: Any) -> None:
        """Record resource deletion arguments and optionally fail it."""
        self._events.append(f"{self.kind}.remove")
        self.remove_calls.append(kwargs)
        if self.remove_error is not None:
            raise self.remove_error
        self.removed.set()


class RecordingImages:
    """Record fixed-image lookup operations."""

    def __init__(self, events: list[str], error: Exception | None = None) -> None:
        """Store the shared operation log and an optional lookup error."""
        self._events = events
        self.error = error

    def get(self, image: str) -> object:
        """Record one image lookup and return its resolved immutable image ID."""
        self._events.append(f"images.get:{image}")
        if self.error is not None:
            raise self.error
        return SimpleNamespace(id="sha256:" + "a" * 64, attrs={"Config": {"Labels": {"io.agent-sandbox.proxy-auth-version": "1", "io.agent-sandbox.proxy-services-version": "1"}}})


class RecordingManager:
    """Record one resource manager's create calls."""

    def __init__(
        self,
        kind: str,
        events: list[str],
        resource_factory: Callable[[dict[str, Any]], RecordingResource],
    ) -> None:
        """Store resource kind, shared events, and a fake object factory."""
        self.kind = kind
        self._events = events
        self._resource_factory = resource_factory
        self.create_calls: list[dict[str, Any]] = []
        self.error: Exception | None = None
        self.get_calls: list[str] = []
        self.create_entered: threading.Event | None = None
        self.create_release: threading.Event | None = None

    def create(self, **kwargs: Any) -> RecordingResource:
        """Record keyword arguments and return one Docker-like resource."""
        self._events.append(f"{self.kind}.create")
        self.create_calls.append(kwargs)
        if self.create_entered is not None:
            self.create_entered.set()
        if self.create_release is not None and not self.create_release.wait(timeout=1):
            raise TimeoutError(f"test did not release {self.kind}.create")
        if self.error is not None:
            raise self.error
        return self._resource_factory(kwargs)

    def get(self, resource_id: str) -> RecordingResource:
        """Record an exact-ID lookup for late cancellation cleanup."""
        self.get_calls.append(resource_id)
        resource = self._resource_factory({"resource_id": resource_id})
        if resource.removed.is_set():
            raise NotFound("resource was removed")
        return resource


class RecordingAPI:
    """Adapt low-level Docker create calls to the existing recording managers."""

    def __init__(self, client: RecordingClient) -> None:
        """Store the recording client and a Docker API version marker."""
        self._client = client
        self._version = "1.45"

    def create_network(self, name: str, **kwargs: Any) -> dict[str, str]:
        """Create a fake network and return its confirmed ID."""
        resource = self._client.networks.create(name=name, **kwargs)
        return {"Id": resource.id}

    def create_volume(self, name: str, **kwargs: Any) -> dict[str, Any]:
        """Create a fake volume and return its name and actual labels."""
        resource = self._client.volumes.create(name=name, **kwargs)
        return {
            "Name": resource.id,
            "Labels": resource.attrs["Labels"],
        }

    def create_host_config(self, **kwargs: Any) -> dict[str, Any]:
        """Return inspectable host configuration arguments."""
        return kwargs

    def create_container(self, **kwargs: Any) -> dict[str, str]:
        """Create a fake container and return its confirmed ID."""
        resource = self._client.containers.create(**kwargs)
        return {"Id": resource.id}


    def connect_container_to_network(self, container: str, network: str) -> None:
        """Record proxy attachment to its dedicated internal bridge."""
        assert container == "proxy-id"
        assert network == "network-id"
        self._client.events.append("proxy.connect")


class RecordingClient:
    """Provide Docker-like managers while recording all operations."""

    def __init__(self) -> None:
        """Initialize managers, resources, and shared operation history."""
        self.events: list[str] = []
        self.close_calls = 0
        self.closed = threading.Event()
        self.close_entered: threading.Event | None = None
        self.close_release: threading.Event | None = None
        self.network = RecordingResource(
            "network",
            "network-id",
            creation_labels(make_spec(), "network"),
            self.events,
        )
        self.volume = RecordingResource(
            "workspace",
            "volume-id",
            creation_labels(make_spec(), "workspace"),
            self.events,
        )
        self.container = RecordingResource(
            "container",
            "container-id",
            creation_labels(make_spec(), "container"),
            self.events,
        )
        self.ingress = RecordingResource(
            "ingress", "ingress-id", creation_labels(make_spec(), "ingress"), self.events
        )
        self.proxy = RecordingResource(
            "proxy", "proxy-id", creation_labels(make_spec(), "proxy"), self.events
        )
        self.images = RecordingImages(self.events)
        self.networks = RecordingManager(
            "networks",
            self.events,
            lambda kwargs: self.ingress if kwargs.get("resource_id") == "ingress-id" or kwargs.get("name") == make_spec().ingress_network_name else self.network,
        )
        self.volumes = RecordingManager(
            "volumes",
            self.events,
            lambda _kwargs: self.volume,
        )
        self.containers = RecordingManager(
            "containers",
            self.events,
            lambda kwargs: self.proxy if kwargs.get("resource_id") == "proxy-id" or kwargs.get("labels", {}).get("io.agent-sandbox.resource") == "proxy" else self.container,
        )
        self.api = RecordingAPI(self)

    def close(self) -> None:
        """Record one Docker client close."""
        self.close_calls += 1
        self.events.append("client.close")
        if self.close_calls == 1 and self.close_entered is not None:
            self.close_entered.set()
        if (
            self.close_calls == 1
            and self.close_release is not None
            and not self.close_release.wait(timeout=1)
        ):
            raise TimeoutError("test did not release Docker client close")
        self.closed.set()


TOKEN = "aB_" + "x" * 40


AUTH_LABEL = "io.agent-sandbox.endpoint-auth-version"


IMAGE_LABEL = "io.agent-sandbox.proxy-auth-version"


def authenticated_client() -> EndpointClient:
    """Upgrade an inspect fixture with consistent v1 identity and configured proxy token."""
    client = EndpointClient()
    for resource in [client.sandbox, client.proxy, *client.networks.resources.values()]:
        resource.attrs.get("Config", {}).get("Labels", resource.attrs.get("Labels", {}))[AUTH_LABEL] = "1"
    for resource in [client.sandbox, client.proxy]:
        resource.attrs["Config"]["Labels"].update({
            "io.agent-sandbox.created-at": "2026-10-19T08:00:00Z",
            "io.agent-sandbox.expires-at": "2026-10-19T08:30:00Z",
        })
    client.proxy.attrs["Config"]["Env"] = [f"SANDBOX_ACCESS_TOKEN={TOKEN}", "SANDBOX_PROXY_AUTH_VERSION=1"]
    client.proxy.attrs["Image"] = "sha256:" + "a" * 64
    client.images = SimpleNamespace(get=lambda _: SimpleNamespace(
        id=client.proxy.attrs["Image"], attrs={"Config": {"Labels": {IMAGE_LABEL: "1"}}},
    ))
    return client


class ProbeStream(httpx.AsyncByteStream):
    """Expose raw body chunks, read boundaries and controllable cancellation cleanup."""

    def __init__(self, chunks=(b'{"status":"ok"}',), *, block=False, gate_close=False):
        """Keep byte chunks independent of the probe and record read/close effects."""
        self.chunks = chunks
        self.block = block
        self.gate_close = gate_close
        self.reads = 0
        self.closed = False
        self.cancelled = False
        self.entered = asyncio.Event()
        self.closing = asyncio.Event()
        self.release_close = asyncio.Event()

    async def __aiter__(self):
        """Yield one raw chunk at a time or pause to expose network cancellation."""
        self.entered.set()
        if self.block:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
        for chunk in self.chunks:
            self.reads += 1
            yield chunk

    async def aclose(self):
        """Make close observable and optionally gate the final connection release."""
        self.closing.set()
        if self.gate_close:
            await self.release_close.wait()
        self.closed = True


class ProbeTransport(httpx.AsyncBaseTransport):
    """Respond without external networking while retaining actual HTTPX stream handling."""

    def __init__(self, stream=None, *, status=200, headers=None, error=None):
        """Configure a single response and count every transport request and shutdown."""
        self.stream = stream if stream is not None else ProbeStream()
        self.status = status
        self.headers = headers or {}
        self.error = error
        self.requests = []
        self.closed = False

    async def handle_async_request(self, request):
        """Return a streamed response or a transport failure without automatic retries."""
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return httpx.Response(self.status, headers=self.headers, stream=self.stream)

    async def aclose(self):
        """Record that the request-scoped HTTP client released its transport."""
        self.closed = True


EGRESS_ID = "e" * 64


EGRESS_IMAGE = "sha256:" + "b" * 64


PROXY_ENV = {
    "HTTP_PROXY": "http://egress:18084", "HTTPS_PROXY": "http://egress:18084",
    "http_proxy": "http://egress:18084", "https_proxy": "http://egress:18084",
    "NO_PROXY": "localhost,127.0.0.1,::1", "no_proxy": "localhost,127.0.0.1,::1",
}


class EgressCreationClient(RecordingClient):
    """Extend the existing transaction fake with one separately identified egress container."""

    def __init__(self, spec: SandboxSpec) -> None:
        """Seed all actual creation labels and track exact container attachment options."""
        super().__init__()
        self.egress = RecordingResource("egress", EGRESS_ID, creation_labels(spec, "egress"), self.events)
        for role in ("network", "ingress", "container", "proxy", "workspace"):
            target = self.volume if role == "workspace" else getattr(self, role)
            labels = creation_labels(spec, role)
            target.attrs["Labels"] = target.attrs["Config"]["Labels"] = labels
        self.containers = RecordingManager("containers", self.events, self.container_for)
        self.attachments: list[tuple[str, str, dict[str, Any]]] = []
        self.images.get = self.image_get
        self.api.connect_container_to_network = self.connect

    def image_get(self, image: str) -> Any:
        """Return a distinct immutable egress image ID and retain all image preflight calls."""
        self.events.append(f"images.get:{image}")
        return SimpleNamespace(
            id=EGRESS_IMAGE if image == "agent-sandbox-egress:dev" else "sha256:" + "a" * 64,
            attrs={"Config": {"Labels": {"io.agent-sandbox.proxy-auth-version": "1", "io.agent-sandbox.proxy-services-version": "1"}}},
        )

    def container_for(self, options: dict[str, Any]) -> RecordingResource:
        """Select by the complete role or exact ID rather than mutable image tags."""
        role = options.get("labels", {}).get("io.agent-sandbox.resource")
        identifier = options.get("resource_id")
        if role == "egress" or identifier == EGRESS_ID:
            return self.egress
        if role == "proxy" or identifier == "proxy-id":
            return self.proxy
        return self.container

    def connect(self, container: str, network: str, **kwargs: Any) -> None:
        """Record the attached exact ID, internal network and egress DNS aliases."""
        self.attachments.append((container, network, kwargs))
        self.events.append("egress.connect" if container == EGRESS_ID else "proxy.connect")
