# Python 控制面健康检查实施计划

> **给智能体执行者：** 必须使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans` 子技能，逐项执行本计划。步骤使用复选框（`- [ ]`）跟踪进度。

**目标：** 在 WSL 宿主侧构建最小 FastAPI 控制面进程，提供独立的进程健康检查和只读 Docker 就绪检查。

**架构：** FastAPI 应用只负责 HTTP 路由、响应映射和 1 秒就绪超时。独立注入的异步运行时探测器通过工作线程适配 Python Docker SDK 的同步 `ping()` 调用，延迟创建并始终关闭客户端；Docker 故障不阻止应用启动。

**技术栈：** Python 3.12、uv、FastAPI、Uvicorn、Python Docker SDK、pytest、httpx、pytest-timeout。

**设计文档：** `docs/superpowers/specs/2026-09-30-control-plane-health-design.md`

## 全局约束

- 只新增根目录下独立的 `control/` 项目，并修改要求的 README 和进度文档；不修改 `agent/` 或其 Compose 拓扑。
- 手动验收只绑定 `127.0.0.1:18083`；不得将未认证服务暴露到不可信网络。
- `/healthz` 绝不能调用 Docker；Docker 不可用时仍须保持 HTTP 200。
- `/readyz` 只能执行 Docker `ping()`；连接错误、权限错误或超时时须返回 HTTP 503，且不得暴露异常文本、环境变量值或 socket 路径。
- Docker client 必须在就绪检查期间延迟创建，使用较短的 SDK 超时，并在成功和失败路径都关闭。
- 服务不得创建、详细检查、启动、停止或删除 sandbox 资源。手动验收只能在前后列出 Docker 资源标识，用于证明没有发生修改。
- 按仓库代码规范，每个具名 Python 函数（包括测试函数）都必须有简洁的函数级 docstring。
- 遵循测试驱动开发：每个新行为先观察测试因缺少预期实现而失败，再加入最小生产实现。

## 审查重点

- 就绪探测超过 HTTP 截止时间后，不得泄露迟到的 Docker 异常或改变已经生成的稳定超时响应；任务 2 覆盖 HTTP 超时边界。
- Docker client 工厂在返回 client 前抛出异常时，仍须映射为稳定的 HTTP 503，且不得尝试关闭尚未绑定的 client；任务 2 和任务 3 覆盖此行为。
- Docker `ping()` 抛出异常时，仍须恰好关闭一次已经创建的 client；任务 3 覆盖此行为。
- Docker 不可用时，构造或导入默认应用不得连接 Docker；任务 3 覆盖延迟创建行为。
- 不支持的 HTTP 方法应保持 FastAPI 的正常 405 响应，且不得调用探测器；任务 1 覆盖此行为。

---

### 任务 1：项目骨架、依赖注入和成功的健康响应

**文件：**
- 新建：`control/pyproject.toml`
- 新建：`control/uv.lock`
- 新建：`control/control/__init__.py`
- 新建：`control/control/app.py`
- 新建：`control/control/docker_runtime.py`
- 新建：`control/tests/test_app.py`

**接口：**
- 产出：`control/docker_runtime.py` 中包含 `async def ping(self) -> None` 的 `RuntimeProbe` 协议。
- 产出：`control/app.py` 中间阶段的可注入函数 `create_app(probe: RuntimeProbe, readiness_timeout: float = 1.0) -> FastAPI`；任务 3 在适配器存在后加入真实默认值和模块级 `app`。
- 产出：`GET /healthz -> {"status": "ok"}`，以及成功时的 `GET /readyz -> {"status": "ready"}`。

- [ ] **步骤 1：创建仅包含依赖的项目配置**

为 `agent-sandbox-control` 包创建 Python 3.12 `pyproject.toml`。使用 `uv add fastapi uvicorn docker` 添加运行依赖，使用 `uv add --dev pytest httpx pytest-timeout` 添加开发依赖，并提交生成的 `uv.lock`。配置 pytest 搜索 `tests/`。该配置骨架不包含生产行为，是执行测试前的必要准备。

- [ ] **步骤 2：编写失败的应用测试**

在 `tests/test_app.py` 中定义一个记录调用且始终成功的 fake，实现 `async ping()`。增加带 docstring 的测试并断言：

- `GET /healthz` 精确返回 HTTP 200 和 `{"status": "ok"}`，且不调用 fake。
- `GET /readyz` 精确返回 HTTP 200 和 `{"status": "ready"}`，并调用 fake 一次。
- `POST /healthz` 和 `POST /readyz` 返回 405；针对就绪接口的 POST 不调用 fake。

明确使这些测试通过的生产改动：创建 `RuntimeProbe`、可注入的 `create_app` 和两个 GET 路由。

- [ ] **步骤 3：运行测试并确认 RED**

运行：`cd control && uv run pytest tests/test_app.py -v`

预期：导入阶段失败，因为 `control.app` 和 `create_app` 尚不存在。只修复测试语法或配置错误，直到测试确实因为缺少这些实现而失败。

- [ ] **步骤 4：实现最小成功应用路径**

将 `RuntimeProbe` 创建为异步 `Protocol`。实现中间阶段的 `create_app`，要求显式注入探测器，并提供仅接受 GET 的 `/healthz` 和 `/readyz`；就绪成功路径等待该探测器。为每个具名函数添加简洁 docstring。在真实 Docker 适配器存在之前，不添加虚假的生产默认值或模块级应用。

- [ ] **步骤 5：运行聚焦测试和完整测试并确认 GREEN**

运行：`cd control && uv run pytest tests/test_app.py -v`

预期：任务 1 的所有测试通过。

运行：`cd control && uv run pytest`

预期：控制面的完整测试套件通过。

- [ ] **步骤 6：提交任务 1**

```bash
git add control/pyproject.toml control/uv.lock control/control control/tests/test_app.py
git commit -m "feat(control): add health endpoints"
```

### 任务 2：稳定的 Docker 失败与超时响应

**文件：**
- 修改：`control/control/app.py`
- 修改：`control/tests/test_app.py`

**接口：**
- 使用：任务 1 的 `RuntimeProbe.ping()` 和 `create_app(...)`。
- 产出：所有探测异常和超时统一返回 HTTP 503 就绪响应 `{"status":"unavailable","error":{"code":"docker_unavailable","message":"Docker runtime is unavailable"}}`。

- [ ] **步骤 1：增加失败的就绪异常测试**

增加 fake 探测器：其 `ping()` 要么抛出包含哨兵 socket 路径或环境变量值的异常，要么永久等待一个未设置的异步事件。增加测试并断言：

- 探测器异常返回 HTTP 503 和完全一致的稳定响应体。
- 探测超过注入的短 `readiness_timeout` 时返回相同的 HTTP 503 响应。
- 使用任一 fake 时 `/healthz` 都保持 HTTP 200，且从不调用 fake。
- 响应文本不包含哨兵异常、`/var/run/docker.sock`、`DOCKER_HOST` 或伪造的环境变量值。
- 在 client 存在前抛出的工厂式探测失败也使用同一个稳定响应表示。

用较短的 `pytest-timeout` 兜底限制标记超时测试；使用异步事件同步，不使用固定 sleep 作为断言机制。

- [ ] **步骤 2：运行新增测试并确认 RED**

运行：`cd control && uv run pytest tests/test_app.py -v`

预期：测试失败，因为异常当前会向外传播，且尚无就绪超时和稳定的 503 映射。

- [ ] **步骤 3：实现就绪超时和稳定映射**

在 `create_app` 中使用 `asyncio.wait_for(..., timeout=readiness_timeout)` 包装 `probe.ping()`。在 endpoint 边界捕获超时和普通探测异常，返回精确、稳定的 JSON 响应体和 HTTP 503。不要记录或序列化异常细节。不要宽泛捕获 `BaseException`，以保留请求任务自身被取消时的 FastAPI 取消语义。

- [ ] **步骤 4：运行聚焦测试和完整测试并确认 GREEN**

运行：`cd control && uv run pytest tests/test_app.py -v`

预期：所有应用测试通过，且没有警告输出。

运行：`cd control && uv run pytest`

预期：控制面的完整测试套件通过。

- [ ] **步骤 5：提交任务 2**

```bash
git add control/control/app.py control/tests/test_app.py
git commit -m "feat(control): report Docker readiness failures"
```

### 任务 3：延迟创建的只读 Docker SDK 探测器

**文件：**
- 修改：`control/control/docker_runtime.py`
- 修改：`control/control/app.py`
- 修改：`control/tests/test_app.py`
- 新建：`control/tests/test_docker_runtime.py`

**接口：**
- 使用：任务 1 的 `RuntimeProbe.ping()` 协议。
- 产出：实现异步 `ping() -> None` 的 `DockerRuntimeProbe(client_factory: Callable[[], DockerClient] | None = None)`。
- 产出：使用 `docker.from_env(timeout=0.5)` 的默认 Docker client 工厂；只有等待 `ping()` 时才连接。
- 产出：最终的 `create_app(probe: RuntimeProbe | None = None, readiness_timeout: float = 1.0) -> FastAPI`，以及供 Uvicorn 使用的模块级 `app`。

- [ ] **步骤 1：编写失败的 Docker 适配器测试**

使用记录调用的 fake Docker client 和注入的 client 工厂，增加带 docstring 的测试并断言：

- 构造 `DockerRuntimeProbe` 不调用工厂。
- 等待 `ping()` 会创建一个 client，只调用其 `ping()` 方法，并恰好关闭一次。
- `ping()` 抛出异常时，同一个异常传播到 HTTP 边界，且 client 仍恰好关闭一次。
- client 工厂在返回 client 前抛出异常时，异常向 HTTP 边界传播且不会尝试关闭。
- 适配器不暴露或调用容器、网络或 volume 修改方法。

同时增加应用测试，断言 `create_app()` 可以在不调用 `docker.from_env` 的情况下构造默认应用，并且模块导出供 Uvicorn 使用的 FastAPI `app`。

- [ ] **步骤 2：运行适配器和默认应用测试并确认 RED**

运行：`cd control && uv run pytest tests/test_docker_runtime.py tests/test_app.py -v`

预期：测试失败，因为具体适配器、延迟工厂行为、可选应用默认值和模块级 `app` 尚不存在。

- [ ] **步骤 3：实现最小 Docker 适配器**

使用 `asyncio.to_thread` 包装私有同步操作，实现 `DockerRuntimeProbe.ping()`。该同步操作延迟创建 client，只调用 `client.ping()`，并在 `finally` 中关闭已取得的 client。默认工厂调用 `docker.from_env(timeout=0.5)`。将 `create_app()` 的默认探测器连接到 `DockerRuntimeProbe` 实例，且不得触发 client 创建。

- [ ] **步骤 4：运行聚焦测试、应用测试和完整测试并确认 GREEN**

运行：`cd control && uv run pytest tests/test_docker_runtime.py -v`

预期：所有适配器测试通过。

运行：`cd control && uv run pytest tests/test_app.py -v`

预期：所有 HTTP 测试仍然通过。

运行：`cd control && uv run pytest`

预期：控制面的完整测试套件通过，且没有警告或泄漏的任务异常。

- [ ] **步骤 5：提交任务 3**

```bash
git add control/control/docker_runtime.py control/control/app.py control/tests/test_app.py control/tests/test_docker_runtime.py
git commit -m "feat(control): add Docker readiness probe"
```

### 任务 4：文档和宿主侧验收

**文件：**
- 修改：`README.md`
- 修改：`docs/PROJECT_PROGRESS.md`

**接口：**
- 使用：任务 1–3 产出的最终控制面命令和 HTTP 契约。
- 产出：记录本地启动、验证流程和当前明确限制的文档。

- [ ] **步骤 1：更新用户文档**

在根 README 中增加简洁章节，覆盖：

- 控制面与 Go Agent 的职责边界。
- `cd control`、`uv sync`，以及精确绑定 `127.0.0.1:18083` 的 Uvicorn 命令。
- `/healthz` 和 `/readyz` 的响应语义。
- Docker 故障不阻止进程启动。
- Docker socket 始终位于宿主侧，绝不能挂载到 sandbox 或代理容器。
- 服务未认证，仅用于本地开发，且没有 sandbox 生命周期接口。

更新 `docs/PROJECT_PROGRESS.md` 的 `2026-09-30` 记录；只从待开发列表中移除已经完成的控制面骨架，创建、查询、删除和更完整生命周期功能仍保留为待开发。

- [ ] **步骤 2：运行自动验证**

运行：

```bash
cd control
uv sync
uv run pytest
uv run python -m compileall -q control tests
cd ..
git diff --check
```

预期：依赖同步成功；所有测试通过；编译检查无错误；diff 检查以 0 退出。

- [ ] **步骤 3：在不修改资源的前提下记录现有 Docker 资源快照**

在启动控制面之前，记录排序后的资源标识：

```bash
docker ps -aq | sort > /tmp/agent-sandbox-control-containers.before
docker network ls -q | sort > /tmp/agent-sandbox-control-networks.before
docker volume ls -q | sort > /tmp/agent-sandbox-control-volumes.before
```

这些命令均为只读。如果执行环境无法访问 Docker，应将此项报告为受阻，不得修改权限或资源。

- [ ] **步骤 4：验证 Docker 可用时的启动、接口、绑定和优雅停止**

在 `control/` 中以可管理的后台会话启动 `uv run uvicorn control.app:app --host 127.0.0.1 --port 18083`。使用有明确上限的循环轮询 `/healthz`，然后断言：

- `curl -i http://127.0.0.1:18083/healthz` 返回 HTTP 200 和 `{"status":"ok"}`。
- `curl -i http://127.0.0.1:18083/readyz` 返回 HTTP 200 和 `{"status":"ready"}`。
- `ss -ltnp` 只显示 `127.0.0.1:18083`，不显示 `0.0.0.0:18083` 或 `[::]:18083`。
- 发送 SIGTERM 后，Uvicorn 在有限等待时间内退出，且 18083 端口不再有监听进程。

如果 18083 端口已被占用，记录占用者并停止此项验收，不终止该进程。

- [ ] **步骤 5：验证模拟 Docker 不可用场景**

使用 `DOCKER_HOST=unix:///tmp/agent-sandbox-control-missing.sock` 启动相同的 Uvicorn 命令，仍然只绑定 `127.0.0.1:18083`。断言：

- 进程成功启动。
- `/healthz` 保持 HTTP 200 和 `{"status":"ok"}`。
- `/readyz` 返回 HTTP 503 和精确、稳定的 `docker_unavailable` 响应。
- 响应不包含模拟 socket 路径、`DOCKER_HOST` 或 SDK 异常。
- SIGTERM 在有限等待时间内停止进程。

- [ ] **步骤 6：证明 Docker 资源标识未变化**

验收后记录相同的三份排序 ID 清单，并逐一使用 `cmp` 比较。预期所有比较均以 0 退出。不得删除或修改任何已有容器、网络或 volume。

- [ ] **步骤 7：运行最终仓库验证**

运行：

```bash
cd control
uv run pytest
uv run python -m compileall -q control tests
cd ..
git diff --check
git status --short
```

预期：测试和编译检查通过，diff 检查以 0 退出，状态只列出预期的控制面和文档修改。

- [ ] **步骤 8：提交任务 4**

```bash
git add README.md docs/PROJECT_PROGRESS.md
git commit -m "docs: document local control plane"
```
