# Python 控制面健康检查设计

## 目标与范围

在 WSL 宿主侧增加一个独立于 Go Agent 的最小 Python/FastAPI 控制面。控制面提供自身健康检查和 Docker 运行时可用性检查，为后续 sandbox 生命周期控制打基础。

本阶段只实现：

- `GET /healthz`：检查控制面 HTTP 服务自身。
- `GET /readyz`：通过只读 Docker ping 检查 Docker daemon 是否可用。
- 本机启动、SIGINT/SIGTERM 优雅停止，以及 Docker 可用和不可用场景的验证。

本阶段不实现 sandbox 创建、查询、删除或其他生命周期操作，不修改 Go Agent、现有 Compose 拓扑、容器、网络或 workspace volume。

## 进程与信任边界

控制面运行在 WSL 宿主侧，是与容器内 Go Agent 分离的 Python 进程。只有控制面进程访问宿主 Docker socket；Docker socket 不挂载到 sandbox 或入口代理中。

控制面当前没有认证，只能作为本机开发入口。默认监听 `127.0.0.1:18083`，避开本地代理使用的 `18081` 和 README 中 Go Agent 示例使用的 `18082`。端口被占用时，Uvicorn 报错并以非零状态退出，不停止或替换其他进程。

## 项目结构与依赖

新增独立的 `control/` Python 项目：

```text
control/
  pyproject.toml
  uv.lock
  control/
    __init__.py
    app.py
    docker_runtime.py
  tests/
    test_app.py
    test_docker_runtime.py
```

使用现有 `uv` 管理依赖和锁文件。运行依赖为 FastAPI、Uvicorn 和 Python Docker SDK；测试依赖为 pytest 和 httpx。目标环境采用当前 WSL 的 Python 3.12。

启动命令为：

```bash
cd ~/agent-sandbox/control
uv sync
uv run uvicorn control.app:app --host 127.0.0.1 --port 18083
```

Uvicorn 负责监听、SIGINT/SIGTERM 和进程退出。本阶段没有后台任务或持久连接需要额外清理。

## 组件职责

### HTTP 应用

`control.app:create_app(probe=None)` 创建 FastAPI 应用。未提供探测器时使用真实 Docker 探测器；测试传入 fake 探测器，避免访问真实 Docker。

HTTP 层负责：

- 注册 `/healthz` 和 `/readyz`。
- 为 `/readyz` 施加固定 1 秒的整体探测上限。
- 将所有 Docker 连接失败、权限错误和超时映射为相同的稳定 503 响应。
- 不向客户端暴露底层异常、环境变量、socket 路径或调用栈。

HTTP 层不直接创建、启动、停止或删除 Docker 资源，也不散落 Docker SDK 调用。

### Docker 探测器

`control.docker_runtime` 定义一个小型异步探测协议和真实实现。真实实现仅执行 Docker SDK 的 `ping()`：

1. 在 `/readyz` 请求期间根据当前环境创建 Docker client。
2. 为 SDK client 设置短请求超时。
3. 在线程中执行同步 `ping()`，避免阻塞 ASGI 事件循环。
4. 无论成功或失败都关闭 client。

外层 1 秒上限确保 HTTP 请求及时返回；SDK 自身超时限制超时后同步工作线程继续存活的时间。Docker client 不在应用导入或启动阶段连接 daemon，因此 Docker 不可用不会阻止控制面启动。

## HTTP 契约

### `GET /healthz`

不访问 Docker。成功返回 HTTP 200：

```json
{"status":"ok"}
```

### `GET /readyz`

Docker ping 成功时返回 HTTP 200：

```json
{"status":"ready"}
```

Docker daemon 不可用、连接失败、权限不足或探测超时时统一返回 HTTP 503：

```json
{
  "status": "unavailable",
  "error": {
    "code": "docker_unavailable",
    "message": "Docker runtime is unavailable"
  }
}
```

失败原因只记录为接口可用性状态；本阶段不把宿主机诊断细节返回给客户端。FastAPI 对其他 HTTP 方法返回 405。

## 失败处理

- Docker 暂时不可用：控制面继续运行，`/healthz` 保持 200，`/readyz` 返回 503。
- Docker 探测超过 1 秒：取消 HTTP 等待并返回与其他不可用情况相同的 503 响应。
- Docker SDK 抛出异常：不传播异常文本到 HTTP 响应，仍返回稳定 503。
- 监听端口被占用：Uvicorn 启动失败并返回非零退出状态，不干预占用端口的进程。
- SIGINT/SIGTERM：由 Uvicorn 正常停止服务。

## 测试与验收

实现遵循 TDD，先写失败测试，再写最小实现。

单元测试覆盖：

- fake 成功探测器下 `/healthz` 和 `/readyz` 均返回 200。
- fake 连接失败时 `/healthz` 仍为 200，`/readyz` 返回 503。
- fake 阻塞探测器超过测试配置的短上限时 `/readyz` 返回 503，不使用固定 sleep 判断状态。
- 失败和超时响应采用稳定结构，且不包含 socket 路径、环境变量值或底层异常文本。
- 真实 Docker 探测封装调用 `ping()`，成功和失败路径都关闭 client。
- Docker 探测不包含创建、启动、停止或删除资源的调用。

WSL 验收覆盖：

- `uv sync`、`uv run pytest` 和 Python 编译检查通过。
- 正常 Docker 环境中启动服务，`/healthz` 和 `/readyz` 返回 200。
- 使用无效 `DOCKER_HOST` 模拟 Docker 不可用，服务仍能启动；`/healthz` 返回 200，`/readyz` 返回 503 且响应不泄露配置。
- 检查监听地址仅为 `127.0.0.1:18083`。
- 验收前后比较 Docker 容器、网络和 volume ID 清单，确认只读 ping 没有改变现有资源。
- 运行 `git diff --check`。

## 文档更新

根 README 增加控制面与 Go Agent 的职责边界、依赖安装、启动命令、健康接口和安全限制。`docs/PROJECT_PROGRESS.md` 记录最小控制面骨架已完成，同时明确 sandbox 生命周期操作仍待开发。

文档不得把 Docker ping 描述为 sandbox 管理能力，也不得暗示当前控制面可安全暴露到不可信网络。
