"""Host-side application creation, dependency assembly and route registration."""

from fastapi import FastAPI

from control_plane.api.health import health_router
from control_plane.api.sandboxes import sandbox_router
from control_plane.core.service import SandboxService
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.runtime.sandbox_runtime import SandboxRuntime


def create_app(
    runtime: SandboxRuntime | None = None,
    readiness_timeout: float = 1.0,
    sandbox_query_timeout: float = 1.0,
) -> FastAPI:
    """Assemble one runtime and shared service without connecting to Docker."""
    application = FastAPI()
    backend = runtime if runtime is not None else DockerSandboxRuntime()
    service = SandboxService(backend)

    application.include_router(health_router(service, readiness_timeout))
    application.include_router(sandbox_router(service, sandbox_query_timeout))
    return application


app = create_app()
