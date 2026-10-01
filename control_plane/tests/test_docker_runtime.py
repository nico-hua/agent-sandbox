"""Unit tests for read-only Docker runtime readiness."""

import pytest

from control_plane.runtime.docker import docker_runtime
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.core.errors import DockerUnavailable


class RecordingClient:
    """Record Docker client operations for a single probe."""

    def __init__(self, ping_error: Exception | None = None) -> None:
        """Initialize operation counters and an optional ping failure."""
        self.ping_error = ping_error
        self.ping_calls = 0
        self.close_calls = 0

    def ping(self) -> None:
        """Record a ping and optionally raise its configured error."""
        self.ping_calls += 1
        if self.ping_error is not None:
            raise self.ping_error

    def close(self) -> None:
        """Record one client close."""
        self.close_calls += 1


class RecordingFactory:
    """Return one fake Docker client and record factory calls."""

    def __init__(self, client: RecordingClient) -> None:
        """Store the client returned by the factory."""
        self.client = client
        self.calls = 0

    def __call__(self) -> RecordingClient:
        """Return the configured client and record one creation."""
        self.calls += 1
        return self.client


class FailingFactory:
    """Raise before a Docker client can be created."""

    def __init__(self, error: Exception) -> None:
        """Store the factory error and initialize its call count."""
        self.error = error
        self.calls = 0

    def __call__(self) -> RecordingClient:
        """Raise the configured client creation error."""
        self.calls += 1
        raise self.error


def test_probe_construction_is_lazy() -> None:
    """Constructing the probe must not create a Docker client."""
    factory = RecordingFactory(RecordingClient())

    DockerSandboxRuntime(ping_client_factory=factory)

    assert factory.calls == 0


@pytest.mark.anyio
@pytest.mark.parametrize("ping_error", [None, RuntimeError("ping failed")])
async def test_default_probe_factory_keeps_short_timeout_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
    ping_error: Exception | None,
) -> None:
    """Default ping must retain its 0.5s timeout and close on either outcome."""
    client = RecordingClient(ping_error)
    factory_calls: list[dict[str, object]] = []

    def from_env(**kwargs: object) -> RecordingClient:
        """Record transport arguments without connecting to the real daemon."""
        factory_calls.append(kwargs)
        return client

    monkeypatch.setattr(docker_runtime.docker, "from_env", from_env)
    probe = DockerSandboxRuntime()
    assert factory_calls == []
    if ping_error is None:
        await probe.ping()
    else:
        with pytest.raises(DockerUnavailable) as captured:
            await probe.ping()
        assert captured.value.__cause__ is ping_error
    assert factory_calls == [{"timeout": 0.5}]
    assert client.ping_calls == 1
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_ping_uses_only_ping_and_closes_client() -> None:
    """A successful probe must ping and close exactly once."""
    client = RecordingClient()
    factory = RecordingFactory(client)
    probe = DockerSandboxRuntime(ping_client_factory=factory)

    await probe.ping()

    assert factory.calls == 1
    assert client.ping_calls == 1
    assert client.close_calls == 1
    assert not hasattr(probe, "containers")
    assert not hasattr(probe, "networks")
    assert not hasattr(probe, "volumes")


@pytest.mark.anyio
async def test_ping_failure_closes_client_and_maps_domain_error() -> None:
    """A ping failure must become a domain error after the client closes."""
    error = RuntimeError("ping failed")
    client = RecordingClient(ping_error=error)
    probe = DockerSandboxRuntime(ping_client_factory=RecordingFactory(client))

    with pytest.raises(DockerUnavailable) as captured:
        await probe.ping()

    assert captured.value.__cause__ is error
    assert client.ping_calls == 1
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_factory_failure_maps_domain_error_without_close_attempt() -> None:
    """A factory failure must become a domain error without a close attempt."""
    error = RuntimeError("factory failed")
    factory = FailingFactory(error)
    probe = DockerSandboxRuntime(ping_client_factory=factory)

    with pytest.raises(DockerUnavailable) as captured:
        await probe.ping()

    assert captured.value.__cause__ is error
    assert factory.calls == 1
