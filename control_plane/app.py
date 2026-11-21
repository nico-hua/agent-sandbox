"""Host-side application creation, dependency assembly and route registration."""

import asyncio
import logging
from collections.abc import AsyncIterator, Collection
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException

from control_plane.api.auth import AuthenticationMiddleware, auth_settings
from control_plane.api.health_router import health_router
from control_plane.api.response import (
    http_exception_response,
    internal_error_response,
    validation_error_response,
)
from control_plane.api.sandbox_router import sandbox_router
from control_plane.api.workspace_router import workspace_router
from control_plane.common.async_tasks import settle_task
from control_plane.config.settings import ControlPlaneSettings
from control_plane.domain.maintenance import SandboxPendingRecoveryResult
from control_plane.runtime.agent_probe import HTTPAgentHealthProbe
from control_plane.runtime.base_runtime import AgentHealthProbe, SandboxRuntime
from control_plane.runtime.docker.docker_runtime import DockerRuntime
from control_plane.services.sandbox_maintenance import run_reaper, stop_reaper
from control_plane.services.sandbox_service import SandboxService
from control_plane.storage.base import LifecycleStore, LifecycleStoreError
from control_plane.storage.sqlite.run_lock import SQLiteRunLock
from control_plane.storage.sqlite.sqlite_store import SQLiteLifecycleStore

_LOGGER = logging.getLogger(__name__)


def _log_pending_recovery(result: SandboxPendingRecoveryResult) -> None:
    """Log bounded startup counts and fixed outage codes without runtime or database details."""
    reason = (
        result.error if result.error in ("docker_unavailable", "sandbox_metadata_unavailable")
        else "none" if result.error is None else "pending_recovery_failed"
    )
    _LOGGER.log(
        logging.WARNING if result.error is not None or result.failed else logging.INFO,
        "pending_recovery_round recovered=%s skipped=%s failed=%s error=%s",
        result.recovered, result.skipped, result.failed, reason,
    )


def create_app() -> FastAPI:
    """Read local settings and construct the control plane's lazy dependencies."""
    settings = ControlPlaneSettings.from_env()
    backend = DockerRuntime(additional_environments=settings.additional_environments)
    lifecycle_store = SQLiteLifecycleStore(settings.database_path)
    agent_probe = HTTPAgentHealthProbe()
    return _assemble_application(settings, backend, lifecycle_store, agent_probe, frozenset(backend.environment_images))


def _assemble_application(
    settings: ControlPlaneSettings,
    backend: SandboxRuntime,
    lifecycle_store: LifecycleStore,
    agent_probe: AgentHealthProbe,
    environment_names: Collection[str],
) -> FastAPI:
    """Bind dependencies to HTTP routes and the application lifecycle."""
    authentication = auth_settings(settings.api_key) if settings.auth_enabled else None
    if settings.pending_recovery_enabled and not isinstance(lifecycle_store, SQLiteLifecycleStore):
        raise ValueError("startup pending recovery requires SQLite application ownership")
    service = SandboxService(
        backend, lifecycle_store,
        agent_probe=agent_probe,
        max_concurrent_creates=settings.max_concurrent_creates,
        max_instances=settings.max_instances,
        environment_names=environment_names,
    )
    lifespan_running = False

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        """Hold SQLite ownership until initialization, pending recovery and the reaper settle."""
        nonlocal lifespan_running
        # This must precede lock-file I/O, SQLite initialization and all tasks.
        if authentication is not None:
            authentication.require_configured()
        if lifespan_running:
            raise RuntimeError("application lifespan is already running")
        lifespan_running = True
        application.state.pending_recovery_result = None
        task: asyncio.Task[None] | None = None
        run_lock: SQLiteRunLock | None = None
        try:
            if isinstance(lifecycle_store, SQLiteLifecycleStore):
                # Use the Store's actual database path to acquire the lock.
                # Acquisition is nonblocking and happens only during application startup.
                run_lock = SQLiteRunLock(lifecycle_store.database_path)
                run_lock.acquire()
            initialization = asyncio.create_task(lifecycle_store.initialize())
            # Cancelling this wait cannot stop SQLite's worker thread. Keep the
            # process lock until its commit/failure and connection cleanup are known.
            await settle_task(initialization, propagate_cancellation=True)
            if settings.pending_recovery_enabled:
                # Await directly: Service settles only the current candidate on
                # cancellation and stops the batch before this lifespan releases ownership.
                result = await service.recover_pending_once(limit=settings.pending_recovery_batch_limit)
                application.state.pending_recovery_result = result
                _log_pending_recovery(result)
                if result.error not in {None, "docker_unavailable"}:
                    raise LifecycleStoreError()
            if settings.reaper_enabled:
                task = asyncio.create_task(
                    run_reaper(
                        service, interval_seconds=settings.reaper_interval_seconds,
                        batch_limit=settings.reaper_batch_limit,
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
                try:
                    if run_lock is not None:
                        run_lock.release()
                finally:
                    lifespan_running = False

    application = FastAPI(
        lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None, redirect_slashes=False,
    )
    application.add_exception_handler(HTTPException, http_exception_response)
    application.add_exception_handler(RequestValidationError, validation_error_response)
    application.add_exception_handler(Exception, internal_error_response)
    if authentication is not None:
        application.add_middleware(AuthenticationMiddleware, settings=authentication)
    application.state.runtime = backend
    application.state.lifecycle_store = lifecycle_store
    application.state.service = service
    application.state.reaper_task = None
    application.state.pending_recovery_result = None

    application.include_router(health_router(service, settings.readiness_timeout))
    application.include_router(sandbox_router(service, settings.sandbox_query_timeout))
    application.include_router(workspace_router(service, settings.sandbox_query_timeout))
    return application


app = create_app()
