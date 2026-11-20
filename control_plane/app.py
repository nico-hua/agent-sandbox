"""Host-side application creation, dependency assembly and route registration."""

import asyncio
import logging
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException

from control_plane.api.auth import (
    UNSET_API_KEY,
    AuthenticationMiddleware,
    auth_settings,
)
from control_plane.api.health import health_router
from control_plane.api.responses import (
    http_exception_response,
    internal_error_response,
    validation_error_response,
)
from control_plane.api.sandboxes import sandbox_router
from control_plane.api.workspaces import workspace_router
from control_plane.common.async_tasks import settle_task
from control_plane.config.settings import (
    DEFAULT_DATABASE_PATH,
    configured_database_path,
)
from control_plane.domain.models import SandboxPendingRecoveryResult
from control_plane.runtime.agent_probe import HTTPAgentHealthProbe
from control_plane.runtime.base import AgentHealthProbe, SandboxRuntime
from control_plane.runtime.docker_runtime import DockerRuntime
from control_plane.runtime.images import build_environment_images
from control_plane.services.maintenance import (
    DEFAULT_REAPER_BATCH_LIMIT,
    DEFAULT_REAPER_INTERVAL_SECONDS,
    run_reaper,
    stop_reaper,
    validate_reaper_config,
)
from control_plane.services.sandbox_service import (
    DEFAULT_MAX_CONCURRENT_CREATES,
    DEFAULT_MAX_INSTANCES,
    SandboxService,
)
from control_plane.storage.base import LifecycleStore, LifecycleStoreError
from control_plane.storage.run_lock import SQLiteRunLock
from control_plane.storage.sqlite_store import SQLiteLifecycleStore

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
    pending_recovery_enabled: bool = False,
    pending_recovery_batch_limit: int = 100,
    agent_probe: AgentHealthProbe | None = None,
    max_concurrent_creates: int = DEFAULT_MAX_CONCURRENT_CREATES,
    max_instances: int = DEFAULT_MAX_INSTANCES,
    additional_environments: Mapping[str, str] | None = None,
    api_key: object = UNSET_API_KEY,
    auth_enabled: bool = False,
) -> FastAPI:
    """Assemble lazy lifecycle dependencies and opt into administrator authentication."""
    if type(auth_enabled) is not bool:
        raise ValueError("auth_enabled must be a boolean")
    authentication = auth_settings(api_key) if auth_enabled else None
    validate_reaper_config(reaper_enabled, reaper_interval_seconds, reaper_batch_limit)
    if type(pending_recovery_enabled) is not bool:
        raise ValueError("pending recovery enabled must be a boolean")
    if (
        type(pending_recovery_batch_limit) is not int
        or not 1 <= pending_recovery_batch_limit <= 2**63 - 1
    ):
        raise ValueError("pending recovery batch limit must be a positive SQLite integer")
    backend = runtime if runtime is not None else DockerRuntime(
        additional_environments=additional_environments,
    )
    if isinstance(backend, DockerRuntime):
        if additional_environments is not None and build_environment_images(additional_environments) != backend.environment_images:
            raise ValueError("application and injected Docker environment configurations differ")
        environment_names = frozenset(backend.environment_images)
    else:
        environment_names = frozenset(build_environment_images(additional_environments))
    selected_database_path = database_path
    if store is None and selected_database_path is None:
        selected_database_path = configured_database_path()
    lifecycle_store = store if store is not None else SQLiteLifecycleStore(
        DEFAULT_DATABASE_PATH if selected_database_path is None else selected_database_path
    )
    if pending_recovery_enabled and not isinstance(lifecycle_store, SQLiteLifecycleStore):
        raise ValueError("startup pending recovery requires SQLite application ownership")
    service = SandboxService(
        backend, lifecycle_store,
        agent_probe=agent_probe if agent_probe is not None else HTTPAgentHealthProbe(),
        max_concurrent_creates=max_concurrent_creates,
        max_instances=max_instances,
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
                # Use the injected Store's actual path, even if database_path differs.
                # Acquisition is nonblocking and happens only during application startup.
                run_lock = SQLiteRunLock(lifecycle_store.database_path)
                run_lock.acquire()
            initialization = asyncio.create_task(lifecycle_store.initialize())
            # Cancelling this wait cannot stop SQLite's worker thread. Keep the
            # process lock until its commit/failure and connection cleanup are known.
            await settle_task(initialization, propagate_cancellation=True)
            if pending_recovery_enabled:
                # Await directly: Service settles only the current candidate on
                # cancellation and stops the batch before this lifespan releases ownership.
                result = await service.recover_pending_once(limit=pending_recovery_batch_limit)
                application.state.pending_recovery_result = result
                _log_pending_recovery(result)
                if result.error not in {None, "docker_unavailable"}:
                    raise LifecycleStoreError()
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

    application.include_router(health_router(service, readiness_timeout))
    application.include_router(sandbox_router(service, sandbox_query_timeout))
    application.include_router(workspace_router(service, sandbox_query_timeout))
    return application


app = create_app(auth_enabled=True)
