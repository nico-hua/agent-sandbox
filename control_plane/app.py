"""Host-side application creation, dependency assembly and route registration."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from control_plane.api.health import health_router
from control_plane.api.sandboxes import sandbox_router
from control_plane.core.service import SandboxService
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.storage.lifecycle_store import LifecycleStore, SQLiteLifecycleStore


DEFAULT_DATABASE_PATH = Path(__file__).resolve().parent / ".local" / "lifecycle.sqlite3"


def create_app(
    runtime: SandboxRuntime | None = None,
    readiness_timeout: float = 1.0,
    sandbox_query_timeout: float = 1.0,
    *,
    store: LifecycleStore | None = None,
    database_path: str | Path | None = None,
) -> FastAPI:
    """Assemble lazy dependencies and explicitly initialize persistence at startup."""
    backend = runtime if runtime is not None else DockerSandboxRuntime()
    lifecycle_store = store if store is not None else SQLiteLifecycleStore(
        database_path if database_path is not None else DEFAULT_DATABASE_PATH
    )
    service = SandboxService(backend, lifecycle_store)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        """Require persistent metadata initialization before accepting requests."""
        await lifecycle_store.initialize()
        yield

    application = FastAPI(lifespan=lifespan)
    application.state.runtime = backend
    application.state.lifecycle_store = lifecycle_store
    application.state.service = service

    application.include_router(health_router(service, readiness_timeout))
    application.include_router(sandbox_router(service, sandbox_query_timeout))
    return application


app = create_app()
