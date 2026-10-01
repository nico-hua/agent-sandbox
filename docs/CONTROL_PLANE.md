# Control 控制面设计与启动

## 1. 当前定位

`control/` 是运行在 WSL 宿主侧的独立 Python/FastAPI 进程，与容器中的 Go Agent 分离。当前版本提供：

- `GET /healthz`：检查 Control HTTP 进程自身是否正常。
- `GET /readyz`：通过只读 Docker `ping()` 检查 Docker daemon 是否可用。
- `POST /v1/sandboxes`：以固定安全策略创建一个独立 Docker sandbox。

当前 Control 不提供 sandbox 查询、Agent 就绪检查、动态入口或删除 API，也不接管现有 Agent、Compose 容器、`agent-sandbox-internal` 网络和 `agent-sandbox-workspace` volume。

## 2. 进程与信任边界

```text
本机调用方
    |
    | HTTP 127.0.0.1:18083
    v
Python/FastAPI Control（WSL 宿主侧）
    |
    | Docker SDK：ping 或固定策略创建
    v
Docker daemon
    |
    | 专属 internal bridge
    v
受管 Agent sandbox（无宿主端口）

现有 Compose Go Agent（独立开发实例，不属于 Control 管理）
```

只有宿主侧 Control 可以访问 Docker daemon。Docker socket 不得挂载到 sandbox 或入口代理中。

Control 当前没有认证，只适合作为本机开发入口。启动时必须绑定 `127.0.0.1`，不得将其直接暴露到不可信网络。

## 3. 目录与组件职责

```text
control/
  AGENTS.md
  pyproject.toml
  uv.lock
  control/
    __init__.py
    app.py
    docker_runtime.py
    sandbox_runtime.py
    sandbox_service.py
  tests/
    test_app.py
    test_docker_runtime.py
    test_sandbox_runtime.py
    test_sandbox_service.py
    test_sandbox_docker_integration.py
```

### `control/app.py`

负责创建 FastAPI 应用和定义 HTTP 路由：

- `create_app(probe=None, sandbox_creator=None, readiness_timeout=1.0)` 支持分别注入探测器和 sandbox creator。
- `/healthz` 不访问 Docker。
- `/readyz` 为探测操作设置整体超时，并将连接失败、权限错误和超时统一映射为稳定的 HTTP 503 响应。
- `/v1/sandboxes` 只接受空请求，将创建结果与领域错误映射为稳定 HTTP 响应。
- 模块级 `app` 供 Uvicorn 使用。

HTTP 响应不会包含底层异常、Docker socket 路径、环境变量或调用栈。

### `control/docker_runtime.py`

负责封装 Docker SDK：

- `RuntimeProbe` 定义 HTTP 层依赖的异步 `ping()` 协议。
- `DockerRuntimeProbe` 使用 `asyncio.to_thread()` 在线程中调用同步 Docker SDK，避免阻塞 ASGI 事件循环。
- Docker client 在收到 `/readyz` 请求时才创建，应用导入和启动阶段不会连接 Docker。
- 默认 client 使用 `docker.from_env(timeout=0.5)` 创建。
- 已创建的 client 在 `ping()` 成功或失败后都会关闭。

探测器只调用 `ping()`，不执行任何 Docker 资源变更操作。

### `control/sandbox_service.py`

负责生成不可预测的 `sbx_` 加 128 位小写十六进制 ID，并据此构造容器、网络和 volume 名称及完整归属标签。service 不直接调用 Docker，只委托注入的 runtime，并仅在 runtime 成功后返回 `status="started"`。

所有资源包含以下标签：

- `io.agent-sandbox.managed=true`
- `io.agent-sandbox.project=agent-sandbox`
- `io.agent-sandbox.sandbox-id=<sandbox_id>`
- `io.agent-sandbox.resource=container|network|workspace`

### `control/sandbox_runtime.py`

负责在线程中执行同步 Docker SDK 调用。runtime 固定使用本地已有的 `agent-sandbox:dev` 镜像，不 pull、不 build，也不接受 HTTP 请求传入的 Docker 配置。它按镜像检查、internal bridge、workspace volume、容器创建、容器启动的顺序执行事务。

每个容器固定使用非 root `sandbox` 用户、`/workspace` 工作目录、1 CPU、256 MiB 内存、无额外 swap、64 PID、只读根文件系统、32 MiB `/tmp` tmpfs、drop ALL capabilities 和 no-new-privileges。容器只连接专属 internal bridge，不发布宿主机端口。

失败或请求取消时，runtime 逆序处理本次事务记录的资源。每次删除前重新通过精确资源 ID 加载，并核对 ID 与完整标签；不会按名称、前缀或全局扫描接管其他资源。清理不完整时返回包含 sandbox ID 的独立领域错误。

## 4. 请求流程

### `/healthz`

1. 请求进入 FastAPI。
2. 路由直接返回进程健康状态。
3. 整个过程不创建 Docker client，也不访问 Docker daemon。

成功响应为 HTTP 200：

```json
{"status":"ok"}
```

### `/readyz`

1. 请求进入 FastAPI。
2. `DockerRuntimeProbe` 在线程中延迟创建 Docker client。
3. client 执行一次只读 `ping()`，然后关闭。
4. 探测在 1 秒内成功时返回 HTTP 200。
5. Docker 不可用、权限不足、SDK 抛出异常或整体探测超时时返回 HTTP 503。

成功响应：

```json
{"status":"ready"}
```

失败响应：

```json
{
  "status": "unavailable",
  "error": {
    "code": "docker_unavailable",
    "message": "Docker runtime is unavailable"
  }
}
```

Docker 暂时不可用不会阻止 Control 进程启动，且不会影响 `/healthz` 返回 200。

### `/v1/sandboxes`

1. HTTP 层确认请求体完全为空；`{}`、空白字符或其他任意字节都返回 HTTP 400。
2. `SandboxService` 生成不可预测的 ID、资源名和标签。
3. `DockerSandboxRuntime` 在工作线程中执行固定创建事务。
4. 创建并启动成功后返回 HTTP 201；这不包含 Agent 就绪探测。
5. 任一步骤失败时，runtime 只回滚本次已记录且归属核对通过的资源，再由 HTTP 层返回稳定错误。

成功响应示例：

```json
{
  "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
  "status": "started"
}
```

稳定错误映射：

| HTTP | code | 含义 |
| --- | --- | --- |
| 400 | `invalid_request` | 请求体非空 |
| 503 | `sandbox_image_unavailable` | 固定镜像不存在 |
| 503 | `docker_unavailable` | Docker daemon 不可用 |
| 409 | `sandbox_resource_conflict` | 生成的资源名冲突 |
| 500 | `sandbox_create_failed` | 创建失败且已完成回滚 |
| 500 | `sandbox_cleanup_failed` | 创建失败且回滚不完整；错误对象额外包含 sandbox ID |

错误体不会返回 Docker 异常、socket 路径、环境变量或调用栈。

## 5. 环境要求

- WSL Ubuntu。
- Python 3.12 或更高版本。
- 已安装 `uv`。
- 验证 `/readyz` 成功时，当前 WSL 用户需要能够访问 Docker daemon。

检查工具版本：

```bash
python3 --version
uv --version
docker version
```

## 6. 安装依赖

进入 Control 目录并严格按照锁文件同步环境：

```bash
cd ~/agent-sandbox/control
uv sync --locked
```

运行依赖包括 FastAPI、Uvicorn 和 Python Docker SDK；测试依赖包括 pytest、httpx 和 pytest-timeout。

## 7. 启动与停止

仅监听本机 `127.0.0.1:18083`：

```bash
cd ~/agent-sandbox/control
uv run uvicorn control.app:app --host 127.0.0.1 --port 18083
```

正常启动后，Uvicorn 会输出监听地址。使用以下命令确认没有监听全部网络接口：

```bash
ss -ltnp 'sport = :18083'
```

输出中的本地地址应为 `127.0.0.1:18083`，不应为 `0.0.0.0:18083` 或 `[::]:18083`。

在前台运行时按 `Ctrl+C` 即可停止。Uvicorn 负责处理 SIGINT/SIGTERM 并完成正常退出。

如果端口已经被占用，启动会失败并返回非零退出状态。不要停止或替换占用端口的其他项目；应先确认占用者，再选择未被占用的本机端口进行临时测试。

## 8. 手动验证

### 检查进程健康

```bash
curl -i http://127.0.0.1:18083/healthz
```

预期状态码为 `200 OK`，响应体为：

```json
{"status":"ok"}
```

### 检查 Docker 就绪状态

```bash
curl -i http://127.0.0.1:18083/readyz
```

Docker 可用时预期返回 `200 OK` 和：

```json
{"status":"ready"}
```

### 模拟 Docker 不可用

先停止当前 Control，再使用不存在的 socket 启动另一个进程：

```bash
cd ~/agent-sandbox/control
DOCKER_HOST=unix:///tmp/agent-sandbox-control-missing.sock \
  uv run uvicorn control.app:app --host 127.0.0.1 --port 18083
```

此时验证：

```bash
curl -i http://127.0.0.1:18083/healthz
curl -i http://127.0.0.1:18083/readyz
```

预期 `/healthz` 仍返回 HTTP 200，`/readyz` 返回 HTTP 503 和稳定的 `docker_unavailable` 响应。响应中不应出现测试 socket 路径或底层 Docker 异常。

### 创建 sandbox

先确认本地固定镜像已经由 Agent 目录构建：

```bash
docker image inspect agent-sandbox:dev
```

发送没有请求体的 POST：

```bash
curl -i -X POST http://127.0.0.1:18083/v1/sandboxes
```

HTTP 201 响应中的 `started` 只表示容器已启动。新容器无宿主端口，当前不能从 WSL 直接访问其 Agent；当前也没有查询或公开删除 API。手动验收后若需清理，必须记录响应 ID，并逐个核对完整项目标签与资源类型，不能按名称前缀批量删除。

## 9. 自动验证

在 `control/` 目录执行：

```bash
uv sync --locked
uv run pytest
uv run python -m compileall -q control tests
```

测试覆盖：

- `/healthz` 与 Docker 状态相互独立。
- `/readyz` 成功、异常和超时响应。
- 不支持的 HTTP 方法返回 405。
- 稳定错误体不泄露宿主机信息。
- Docker client 延迟创建。
- `ping()` 成功、失败和 HTTP 已超时后的迟到失败均能关闭 client。
- 探测器不暴露容器、网络或 volume 管理操作。
- 创建 API 的空请求契约、成功响应和稳定错误映射。
- 固定资源名称、标签、安全参数、调用顺序、失败回滚和取消清理。
- Docker 集成测试默认跳过，不会修改 Docker 资源。

真实 Docker 集成测试必须显式启用：

```bash
RUN_DOCKER_INTEGRATION=1 uv run pytest -m docker_integration -v
```

该测试会创建一个唯一 sandbox，检查配置后仅按已记录资源 ID 和完整标签清理。运行前仍应记录现有容器、网络和 volume 快照，并确认 `agent-sandbox:dev` 已存在；测试不会 pull 或 build 镜像。

在仓库根目录额外执行：

```bash
git diff --check
```

## 10. 当前限制

- 没有认证、授权和审计能力。
- 只有最小 sandbox 创建接口，没有查询、Agent 就绪、动态入口、续期、暂停、恢复和删除接口。
- 没有状态持久化、后台任务、重试或运行时资源管理。
- `/readyz` 只说明 Docker daemon 能否响应 ping，不代表任意 sandbox 已经就绪。
- 创建结果 `started` 也不表示容器内 Agent 已经就绪。
- 新 sandbox 无宿主端口且仅连接专属 internal bridge，当前没有从宿主机动态访问它的入口。
- 当前实现不是完整的控制面，也不是安全隔离能力。

后续增加生命周期操作时，应继续保持 HTTP 层与 Docker 访问层分离，并为资源创建失败、部分成功、清理和恢复定义明确语义。

## 11. 常见问题

### `/healthz` 正常但 `/readyz` 返回 503

这表示 Control HTTP 进程正常，但 Docker daemon 当前不可访问。检查：

```bash
docker version
docker info
```

不要为了让探测通过而放宽 sandbox 权限或将 Docker socket 挂载进 sandbox。

### 无法启动，提示端口已被占用

检查监听进程：

```bash
ss -ltnp 'sport = :18083'
```

不要自动终止占用者。开发验证时可以显式改用另一个仅绑定 `127.0.0.1` 的端口。

### 修改代码后 Uvicorn 没有自动重载

当前启动命令不启用 `--reload`，这是预期行为。停止并重新启动进程即可加载修改。
