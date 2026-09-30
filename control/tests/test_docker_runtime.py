"""Unit tests for the read-only Docker runtime probe."""

import pytest

from control.docker_runtime import DockerRuntimeProbe


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

    DockerRuntimeProbe(factory)

    assert factory.calls == 0


@pytest.mark.anyio
async def test_ping_uses_only_ping_and_closes_client() -> None:
    """A successful probe must ping and close exactly once."""
    client = RecordingClient()
    factory = RecordingFactory(client)
    probe = DockerRuntimeProbe(factory)

    await probe.ping()

    assert factory.calls == 1
    assert client.ping_calls == 1
    assert client.close_calls == 1
    assert not hasattr(probe, "containers")
    assert not hasattr(probe, "networks")
    assert not hasattr(probe, "volumes")


@pytest.mark.anyio
async def test_ping_failure_closes_client_and_preserves_error() -> None:
    """A ping error must propagate after the created client closes."""
    error = RuntimeError("ping failed")
    client = RecordingClient(ping_error=error)
    probe = DockerRuntimeProbe(RecordingFactory(client))

    with pytest.raises(RuntimeError) as captured:
        await probe.ping()

    assert captured.value is error
    assert client.ping_calls == 1
    assert client.close_calls == 1


@pytest.mark.anyio
async def test_factory_failure_propagates_without_close_attempt() -> None:
    """A factory error must propagate when no client exists to close."""
    error = RuntimeError("factory failed")
    factory = FailingFactory(error)
    probe = DockerRuntimeProbe(factory)

    with pytest.raises(RuntimeError) as captured:
        await probe.ping()

    assert captured.value is error
    assert factory.calls == 1
