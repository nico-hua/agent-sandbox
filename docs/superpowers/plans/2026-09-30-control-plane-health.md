# Python Control Plane Health Checks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a minimal FastAPI control-plane process on the WSL host with independent process health and read-only Docker readiness endpoints.

**Architecture:** A FastAPI application owns only HTTP routing, response mapping, and the one-second readiness deadline. A separately injected asynchronous runtime probe adapts the synchronous Python Docker SDK `ping()` call through a worker thread, creates clients lazily, and always closes them; Docker failures never prevent application startup.

**Tech Stack:** Python 3.12, uv, FastAPI, Uvicorn, Python Docker SDK, pytest, httpx, pytest-timeout.

**Spec:** `docs/superpowers/specs/2026-09-30-control-plane-health-design.md`

## Global Constraints

- Add only the independent root-level `control/` project and the requested README/progress documentation changes; do not modify `agent/` or its Compose topology.
- Bind manual validation only to `127.0.0.1:18083`; do not expose the unauthenticated service to untrusted networks.
- `/healthz` must never call Docker and must remain HTTP 200 while Docker is unavailable.
- `/readyz` may only issue Docker `ping()` and must return HTTP 503 for connection errors, permission errors, or timeout without exposing exception text, environment values, or socket paths.
- Docker clients must be created lazily during readiness checks, use a short SDK timeout, and be closed on both success and failure.
- Do not create, inspect in detail, start, stop, or delete sandbox resources as part of the service. Manual validation may list Docker resource identifiers before and after solely to prove no mutation occurred.
- Every named Python function, including test functions, must have a concise function-level docstring per the repository coding standard.
- Follow test-driven development: observe each new behavioral test fail for the expected missing behavior before adding the minimal production implementation.

## Review Focus

- A readiness probe that outlives the HTTP deadline must not expose a late Docker exception or change the stable timeout response; cover the HTTP timeout boundary in Task 2.
- A Docker client factory that raises before returning a client must still map to stable HTTP 503 without attempting to close an unbound client; cover this in Tasks 2 and 3.
- A Docker `ping()` that raises must still close the created client exactly once; cover this in Task 3.
- Constructing or importing the default application while Docker is unavailable must not connect to Docker; cover lazy construction in Task 3.
- Unsupported HTTP methods must keep FastAPI's normal 405 response and must not call the probe; cover this in Task 1.

---

### Task 1: Project scaffold, dependency injection, and successful health responses

**Files:**
- Create: `control/pyproject.toml`
- Create: `control/uv.lock`
- Create: `control/control/__init__.py`
- Create: `control/control/app.py`
- Create: `control/control/docker_runtime.py`
- Create: `control/tests/test_app.py`

**Interfaces:**
- Produces: `RuntimeProbe` protocol with `async def ping(self) -> None` in `control/docker_runtime.py`.
- Produces: the intermediate injectable `create_app(probe: RuntimeProbe, readiness_timeout: float = 1.0) -> FastAPI` in `control/app.py`; Task 3 adds the real default and module-level `app` after the adapter exists.
- Produces: `GET /healthz -> {"status": "ok"}` and successful `GET /readyz -> {"status": "ready"}`.

- [ ] **Step 1: Create the dependency-only project configuration**

Create a Python 3.12 `pyproject.toml` for package `agent-sandbox-control`. Add runtime dependencies with `uv add fastapi uvicorn docker` and development dependencies with `uv add --dev pytest httpx pytest-timeout`; commit the generated `uv.lock`. Configure pytest to discover `tests/`. This configuration scaffold contains no production behavior and is required before executing tests.

- [ ] **Step 2: Write failing application tests**

In `tests/test_app.py`, define a recording successful fake implementing `async ping()`. Add tests with docstrings that assert:

- `GET /healthz` returns exactly HTTP 200 and `{"status": "ok"}` without calling the fake.
- `GET /readyz` returns exactly HTTP 200 and `{"status": "ready"}` and calls the fake once.
- `POST /healthz` and `POST /readyz` return 405; the readiness POST does not call the fake.

Name the production change that makes these tests pass: creation of `RuntimeProbe`, injectable `create_app`, and the two GET routes.

- [ ] **Step 3: Run the tests and verify RED**

Run: `cd control && uv run pytest tests/test_app.py -v`

Expected: FAIL during import because `control.app` and `create_app` do not exist. Fix only test syntax or configuration errors until this is the reason for failure.

- [ ] **Step 4: Implement the minimum successful application path**

Create `RuntimeProbe` as an async `Protocol`. Implement the intermediate `create_app` requiring an injected probe, with GET-only `/healthz` and `/readyz`; successful readiness awaits that probe. Add concise docstrings to every named function. Do not add a fake production default or module-level application before the real Docker adapter exists.

- [ ] **Step 5: Run the focused and full test suites and verify GREEN**

Run: `cd control && uv run pytest tests/test_app.py -v`

Expected: all Task 1 tests PASS.

Run: `cd control && uv run pytest`

Expected: the complete control test suite PASS.

- [ ] **Step 6: Commit Task 1**

```bash
git add control/pyproject.toml control/uv.lock control/control control/tests/test_app.py
git commit -m "feat(control): add health endpoints"
```

### Task 2: Stable Docker failure and timeout responses

**Files:**
- Modify: `control/control/app.py`
- Modify: `control/tests/test_app.py`

**Interfaces:**
- Consumes: `RuntimeProbe.ping()` and `create_app(...)` from Task 1.
- Produces: HTTP 503 readiness response `{"status":"unavailable","error":{"code":"docker_unavailable","message":"Docker runtime is unavailable"}}` for every probe exception and timeout.

- [ ] **Step 1: Add failing readiness failure tests**

Add fake probes whose `ping()` either raises an exception containing a sentinel socket path/environment value or waits forever on an unset async event. Add tests that assert:

- A probe exception returns HTTP 503 with exactly the stable response body.
- A probe that exceeds a short injected `readiness_timeout` returns the same HTTP 503 response.
- `/healthz` remains HTTP 200 with either fake and never calls it.
- Neither response text contains the sentinel exception, `/var/run/docker.sock`, `DOCKER_HOST`, or a fake environment value.
- A probe factory-style failure raised before a client exists is represented by the same stable response.

Mark the timeout test with a short `pytest-timeout` safety ceiling; synchronize with an async event rather than using fixed sleep as the assertion mechanism.

- [ ] **Step 2: Run the new tests and verify RED**

Run: `cd control && uv run pytest tests/test_app.py -v`

Expected: failure because exceptions currently escape and no readiness deadline or stable 503 mapping exists.

- [ ] **Step 3: Implement the readiness deadline and stable mapping**

In `create_app`, wrap `probe.ping()` with `asyncio.wait_for(..., timeout=readiness_timeout)`. Catch timeout and ordinary probe exceptions at the endpoint boundary and return the exact stable JSON body with HTTP 503. Do not log or serialize exception details. Preserve FastAPI cancellation behavior for cancellation of the request task itself rather than broadly catching `BaseException`.

- [ ] **Step 4: Run focused and full tests and verify GREEN**

Run: `cd control && uv run pytest tests/test_app.py -v`

Expected: all application tests PASS without warning output.

Run: `cd control && uv run pytest`

Expected: the complete control suite PASS.

- [ ] **Step 5: Commit Task 2**

```bash
git add control/control/app.py control/tests/test_app.py
git commit -m "feat(control): report Docker readiness failures"
```

### Task 3: Lazy, read-only Docker SDK probe

**Files:**
- Modify: `control/control/docker_runtime.py`
- Modify: `control/control/app.py`
- Modify: `control/tests/test_app.py`
- Create: `control/tests/test_docker_runtime.py`

**Interfaces:**
- Consumes: `RuntimeProbe.ping()` protocol from Task 1.
- Produces: `DockerRuntimeProbe(client_factory: Callable[[], DockerClient] | None = None)` implementing async `ping() -> None`.
- Produces: a default Docker client factory using `docker.from_env(timeout=0.5)` without connecting until `ping()` is awaited.
- Produces: final `create_app(probe: RuntimeProbe | None = None, readiness_timeout: float = 1.0) -> FastAPI` and module-level `app` for Uvicorn.

- [ ] **Step 1: Write failing Docker adapter tests**

Using a recording fake Docker client and injected client factory, add documented tests that assert:

- Constructing `DockerRuntimeProbe` does not call the factory.
- Awaiting `ping()` creates one client, invokes only its `ping()` method, and closes it exactly once.
- If `ping()` raises, the same exception propagates to the HTTP boundary and the client still closes exactly once.
- If the client factory raises before returning a client, that exception propagates without a close attempt.
- No adapter method for container, network, or volume mutation is exposed or invoked.

Also add application tests that assert `create_app()` can construct the default application without invoking `docker.from_env`, and that the module exports a FastAPI `app` for Uvicorn.

- [ ] **Step 2: Run the adapter and default-application tests and verify RED**

Run: `cd control && uv run pytest tests/test_docker_runtime.py tests/test_app.py -v`

Expected: FAIL because the concrete adapter, lazy factory behavior, optional application default, and module-level `app` are absent.

- [ ] **Step 3: Implement the minimum Docker adapter**

Implement `DockerRuntimeProbe.ping()` using `asyncio.to_thread` around a private synchronous operation. The operation creates the client lazily, calls only `client.ping()`, and closes an obtained client in `finally`. The default factory calls `docker.from_env(timeout=0.5)`. Wire `create_app()`'s default to a `DockerRuntimeProbe` instance without triggering client creation.

- [ ] **Step 4: Run focused, application, and full tests and verify GREEN**

Run: `cd control && uv run pytest tests/test_docker_runtime.py -v`

Expected: all adapter tests PASS.

Run: `cd control && uv run pytest tests/test_app.py -v`

Expected: all HTTP tests still PASS.

Run: `cd control && uv run pytest`

Expected: the complete control suite PASS without warnings or leaked task errors.

- [ ] **Step 5: Commit Task 3**

```bash
git add control/control/docker_runtime.py control/control/app.py control/tests/test_app.py control/tests/test_docker_runtime.py
git commit -m "feat(control): add Docker readiness probe"
```

### Task 4: Documentation and host-level acceptance

**Files:**
- Modify: `README.md`
- Modify: `docs/PROJECT_PROGRESS.md`

**Interfaces:**
- Consumes: the final control-plane commands and HTTP contracts from Tasks 1–3.
- Produces: documented local startup and validation flow with explicit current limitations.

- [ ] **Step 1: Update user documentation**

Add a concise root README section covering:

- Control plane versus Go Agent responsibilities.
- `cd control`, `uv sync`, and the exact Uvicorn command binding `127.0.0.1:18083`.
- `/healthz` and `/readyz` response semantics.
- Docker failures do not prevent startup.
- The Docker socket remains host-side and must never be mounted into sandbox or proxy containers.
- The service is unauthenticated, local-development-only, and has no sandbox lifecycle endpoints.

Update `docs/PROJECT_PROGRESS.md` under `2026-09-30`; remove only the completed control-plane skeleton item from future work while retaining creation, query, deletion, and broader lifecycle work as pending.

- [ ] **Step 2: Run automated verification**

Run:

```bash
cd control
uv sync
uv run pytest
uv run python -m compileall -q control tests
cd ..
git diff --check
```

Expected: dependency sync succeeds; all tests pass; compilation emits no errors; diff check exits 0.

- [ ] **Step 3: Snapshot existing Docker resources without mutation**

Capture sorted identifiers before starting the control plane:

```bash
docker ps -aq | sort > /tmp/agent-sandbox-control-containers.before
docker network ls -q | sort > /tmp/agent-sandbox-control-networks.before
docker volume ls -q | sort > /tmp/agent-sandbox-control-volumes.before
```

These commands are read-only. If Docker access is unavailable in the execution environment, report this validation as blocked rather than changing permissions or resources.

- [ ] **Step 4: Validate Docker-available startup, endpoints, binding, and graceful stop**

Start `uv run uvicorn control.app:app --host 127.0.0.1 --port 18083` from `control/` in a managed background session. Poll `/healthz` with a bounded retry loop, then assert:

- `curl -i http://127.0.0.1:18083/healthz` returns HTTP 200 and `{"status":"ok"}`.
- `curl -i http://127.0.0.1:18083/readyz` returns HTTP 200 and `{"status":"ready"}`.
- `ss -ltnp` shows only `127.0.0.1:18083`, not `0.0.0.0:18083` or `[::]:18083`.
- Sending SIGTERM makes Uvicorn exit within a bounded wait with no process left listening on port 18083.

If port 18083 is occupied, record the owning listener and stop this validation without terminating it.

- [ ] **Step 5: Validate simulated Docker unavailability**

Start the same Uvicorn command with `DOCKER_HOST=unix:///tmp/agent-sandbox-control-missing.sock`, again only on `127.0.0.1:18083`. Assert:

- The process starts successfully.
- `/healthz` remains HTTP 200 with `{"status":"ok"}`.
- `/readyz` returns HTTP 503 with the exact stable `docker_unavailable` response.
- The response does not contain the simulated socket path, `DOCKER_HOST`, or an SDK exception.
- SIGTERM stops the process within a bounded wait.

- [ ] **Step 6: Prove Docker resource identifiers are unchanged**

Capture the same three sorted ID lists after validation and compare each with `cmp`. Expected: all comparisons exit 0. Do not remove or alter any pre-existing container, network, or volume.

- [ ] **Step 7: Run final repository verification**

Run:

```bash
cd control
uv run pytest
uv run python -m compileall -q control tests
cd ..
git diff --check
git status --short
```

Expected: tests and compilation pass, diff check exits 0, and status lists only intentional control-plane and documentation changes.

- [ ] **Step 8: Commit Task 4**

```bash
git add README.md docs/PROJECT_PROGRESS.md
git commit -m "docs: document local control plane"
```
