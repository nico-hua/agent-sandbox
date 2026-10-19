"""Host-side application creation, dependency assembly and route registration."""

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from control_plane.api.health import health_router
from control_plane.api.sandboxes import sandbox_router
from control_plane.core.reaper import (
    DEFAULT_REAPER_BATCH_LIMIT,
    DEFAULT_REAPER_INTERVAL_SECONDS,
    run_reaper,
    stop_reaper,
    validate_reaper_config,
)
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
    reaper_enabled: bool = True,
    reaper_interval_seconds: float = DEFAULT_REAPER_INTERVAL_SECONDS,
    reaper_batch_limit: int = DEFAULT_REAPER_BATCH_LIMIT,
) -> FastAPI:
    """Assemble lazy dependencies and validate the application-owned reaper configuration."""
    validate_reaper_config(reaper_enabled, reaper_interval_seconds, reaper_batch_limit)
    backend = runtime if runtime is not None else DockerSandboxRuntime()
    lifecycle_store = store if store is not None else SQLiteLifecycleStore(
        database_path if database_path is not None else DEFAULT_DATABASE_PATH
    )
    service = SandboxService(backend, lifecycle_store)
    lifespan_running = False

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        """Initialize storage, own at most one reaper and await safe shutdown cleanup."""
        nonlocal lifespan_running
        if lifespan_running:
            raise RuntimeError("application lifespan is already running")
        lifespan_running = True
        task: asyncio.Task[None] | None = None
        try:
            await lifecycle_store.initialize()
            if reaper_enabled:
                task = asyncio.create_task(
                    run_reaper(
                        service, interval_seconds=reaper_interval_seconds,
                        batch_limit=reaper_batch_limit,
                    ),
                    name="sandbox-expiration-reaper",
                )
                application.state.reaper_task = task
            # Scheduling the first round must not delay HTTP startup until the
            # entire batch completes. The task runs after this context yields.
            yield
        finally:
            try:
                if task is not None:
                    await stop_reaper(task)
            finally:
                application.state.reaper_task = None
                lifespan_running = False

    application = FastAPI(lifespan=lifespan)
    application.state.runtime = backend
    application.state.lifecycle_store = lifecycle_store
    application.state.service = service
    application.state.reaper_task = None

    application.include_router(health_router(service, readiness_timeout))
    application.include_router(sandbox_router(service, sandbox_query_timeout))
    return application


app = create_app()
