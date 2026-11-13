"""Host-side application creation, dependency assembly and route registration."""

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from control_plane.api.auth import AuthenticationMiddleware, UNSET_API_KEY, auth_settings
from control_plane.api.health import health_router
from control_plane.api.sandboxes import sandbox_router
from control_plane.api.workspaces import workspace_router
from control_plane.core.reaper import (
    DEFAULT_REAPER_BATCH_LIMIT,
    DEFAULT_REAPER_INTERVAL_SECONDS,
    run_reaper,
    stop_reaper,
    validate_reaper_config,
)
from control_plane.core.models import SandboxPendingRecoveryResult
from control_plane.core.service import DEFAULT_MAX_CONCURRENT_CREATES, DEFAULT_MAX_INSTANCES, AgentHealthProbe, SandboxService
from control_plane.core.tasks import settle_task, wait_for_completion
from control_plane.runtime.agent_http_probe import HTTPAgentHealthProbe
from control_plane.runtime.configuration import build_runtime, configured_database_path, load_backend_configuration
from control_plane.runtime.docker.docker_runtime import DockerSandboxRuntime
from control_plane.runtime.docker.transaction import build_environment_images
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime
from control_plane.runtime.kubernetes.local_entry import LocalEntryError
from control_plane.runtime.kubernetes.local_entry_manager import KubernetesLocalEntryManager
from control_plane.runtime.sandbox_runtime import SandboxRuntime
from control_plane.storage.lifecycle_store import LifecycleStore, LifecycleStoreError, SQLiteLifecycleStore
from control_plane.storage.run_lock import SQLiteRunLock

DEFAULT_DATABASE_PATH = Path(__file__).resolve().parent / ".local" / "lifecycle.sqlite3"
DEFAULT_KUBERNETES_DATABASE_PATH = DEFAULT_DATABASE_PATH.with_name("kubernetes-lifecycle.sqlite3")
_LOGGER = logging.getLogger(__name__)


def _log_pending_recovery(result: SandboxPendingRecoveryResult) -> None:
    """Log bounded startup counts and fixed outage codes without runtime or database details."""
    reason = (
        result.error if result.error in ("docker_unavailable", "kubernetes_unavailable", "sandbox_metadata_unavailable")
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
    allow_unauthenticated: bool = False,
    kubernetes_entry_manager: KubernetesLocalEntryManager | None = None,
    kubernetes_initial_sandbox_ids: tuple[str, ...] | list[str] = (),
) -> FastAPI:
    """Freeze trusted environment configuration and assemble lazy lifecycle dependencies."""
    authentication = auth_settings(api_key, allow_unauthenticated=allow_unauthenticated)
    if type(kubernetes_initial_sandbox_ids) not in (tuple, list) or len(kubernetes_initial_sandbox_ids) > 8:
        raise ValueError("initial Kubernetes sandbox IDs must be a list or tuple of at most eight IDs")
    initial_sandbox_ids = tuple(kubernetes_initial_sandbox_ids)
    if any(type(value) is not str or re.fullmatch(r"sbx_[0-9a-f]{32}", value) is None
           for value in initial_sandbox_ids) or len(set(initial_sandbox_ids)) != len(initial_sandbox_ids):
        raise ValueError("initial Kubernetes sandbox IDs must be unique canonical sandbox IDs")
    validate_reaper_config(reaper_enabled, reaper_interval_seconds, reaper_batch_limit)
    if type(pending_recovery_enabled) is not bool:
        raise ValueError("pending recovery enabled must be a boolean")
    if (
        type(pending_recovery_batch_limit) is not int
        or not 1 <= pending_recovery_batch_limit <= 2**63 - 1
    ):
        raise ValueError("pending recovery batch limit must be a positive SQLite integer")
    if runtime is None:
        backend = build_runtime(load_backend_configuration(), additional_environments=additional_environments)
    else:
        backend = runtime
    if isinstance(backend, KubernetesSandboxRuntime):
        if additional_environments is not None:
            raise ValueError("Kubernetes custom environments are not supported")
        environment_names = frozenset(backend.environment_images)
    elif isinstance(backend, DockerSandboxRuntime):
        if additional_environments is not None and build_environment_images(additional_environments) != backend.environment_images:
            raise ValueError("application and injected Docker environment configurations differ")
        environment_names = frozenset(backend.environment_images)
    else:
        # Preserve existing injected test/custom Runtime registration behavior;
        # Kubernetes never passes through the Docker image-map branch.
        environment_names = frozenset(build_environment_images(additional_environments))
    selected_database_path = database_path
    if store is None and selected_database_path is None:
        selected_database_path = configured_database_path()
    if selected_database_path is None:
        selected_database_path = (
            DEFAULT_KUBERNETES_DATABASE_PATH if isinstance(backend, KubernetesSandboxRuntime) else DEFAULT_DATABASE_PATH
        )
    lifecycle_store = store if store is not None else SQLiteLifecycleStore(
        selected_database_path
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
    entry_manager = kubernetes_entry_manager
    if isinstance(backend, KubernetesSandboxRuntime):
        if entry_manager is None:
            if backend.local_entry_manager is not None:
                raise ValueError("an existing Kubernetes entry manager requires explicit application ownership")
            if initial_sandbox_ids or backend.prepare_entry_on_create:
                entry_manager = KubernetesLocalEntryManager(backend)
        elif entry_manager.runtime is not backend:
            raise ValueError("Kubernetes entry manager belongs to a different runtime")
        if entry_manager is not None:
            backend.bind_local_entry_manager(entry_manager)
    elif entry_manager is not None or initial_sandbox_ids:
        raise ValueError("local entry configuration requires a Kubernetes runtime")
    lifespan_running = False

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        """Hold SQLite ownership until initialization, reaper and owned local entries settle."""
        nonlocal lifespan_running
        # This must precede lock-file I/O, SQLite initialization and all tasks.
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
                application.state.kubernetes_entry_run_lock = run_lock if entry_manager is not None else None
            initialization = asyncio.create_task(lifecycle_store.initialize())
            # Cancelling this wait cannot stop SQLite's worker thread. Keep the
            # process lock until its commit/failure and connection cleanup are known.
            await settle_task(initialization, propagate_cancellation=True)
            if entry_manager is not None:
                # Endpoint lookup is deliberately read-only. Prepare only the
                # explicit entries before recovery can validate their availability.
                for sandbox_id in initial_sandbox_ids:
                    await entry_manager.open(sandbox_id)
            if pending_recovery_enabled:
                # Await directly: Service settles only the current candidate on
                # cancellation and stops the batch before this lifespan releases ownership.
                result = await service.recover_pending_once(limit=pending_recovery_batch_limit)
                application.state.pending_recovery_result = result
                _log_pending_recovery(result)
                if result.error is not None and result.error not in {"docker_unavailable", "kubernetes_unavailable"}:
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
                entries_closed = entry_manager is None
                try:
                    if entry_manager is not None:
                        # Keep cleanup independent of repeated cancellation of the
                        # lifespan owner, then observe its actual outcome before unlocking.
                        cleanup = asyncio.create_task(entry_manager.close_all())
                        cancellation = await wait_for_completion(cleanup)
                        cleanup.result()
                        if not entry_manager.is_closed:
                            raise LocalEntryError("local_entry_cleanup_failed")
                        entries_closed = True
                        if cancellation is not None:
                            raise cancellation
                finally:
                    application.state.reaper_task = None
                    # Failed cleanup retains the manager and the process lock for
                    # diagnosis; another lifespan must not inherit uncertain ownership.
                    if entries_closed:
                        try:
                            if run_lock is not None:
                                run_lock.release()
                        finally:
                            application.state.kubernetes_entry_run_lock = None
                            lifespan_running = False

    application = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)
    if entry_manager is not None:
        entry_manager.claim_application(application)
    application.add_middleware(AuthenticationMiddleware, settings=authentication)
    application.state.runtime = backend
    application.state.lifecycle_store = lifecycle_store
    application.state.service = service
    application.state.reaper_task = None
    application.state.pending_recovery_result = None
    application.state.kubernetes_entry_manager = entry_manager
    application.state.kubernetes_entry_run_lock = None

    # Select only fixed public text here; health routes know neither SDK nor
    # concrete Runtime types and always enter the shared Service.
    readiness_error = (
        ("kubernetes_unavailable", "Kubernetes runtime is unavailable")
        if isinstance(backend, KubernetesSandboxRuntime) else None
    )
    application.include_router(health_router(service, readiness_timeout, unavailable_error=readiness_error))
    application.include_router(sandbox_router(service, sandbox_query_timeout))
    application.include_router(workspace_router(service, sandbox_query_timeout))
    return application


app = create_app()
