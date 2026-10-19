# Control 控制面设计与启动

## 1. 当前定位

`control_plane/` 是运行在 WSL 宿主侧的独立 Python/FastAPI 进程，与容器中的 Go Agent 分离。当前版本提供：

- `GET /healthz`：检查 Control HTTP 进程自身是否正常。
- `GET /readyz`：通过只读 Docker `ping()` 检查 Docker daemon 是否可用。
- `POST /v1/sandboxes`：以固定安全策略创建一个独立 Docker sandbox，记录创建和初始计划过期时间。
- `GET /v1/sandboxes/{sandbox_id}`：读取 Docker 容器、Agent 健康状态和初始时间，控制面重启后仍可查询。
- `GET /v1/sandboxes/{sandbox_id}/endpoint`：核验专属代理入口并返回实际本机地址，重启后仍可恢复。
- `DELETE /v1/sandboxes/{sandbox_id}`：回收专属容器、代理和两网，默认保留 workspace；`delete_workspace=true` 显式删除卷。

当前 Control 支持“创建 → 使用 → 删除”的手动闭环；不提供主动 Agent 就绪探测，也不接管现有 Agent、Compose 容器、`agent-sandbox-internal` 网络和 `agent-sandbox-workspace` volume。

## 2. 进程与信任边界

```text
本机调用方
    |
    | HTTP 127.0.0.1:18083
    v
Python/FastAPI Control（WSL 宿主侧）
    |
    | Docker SDK：ping、固定策略创建、只读查询或归属核验后的删除
    v
Docker daemon
    |
    | 专属 internal bridge + 专属 ingress bridge
    v
专属 proxy（仅发布动态 127.0.0.1 端口，固定上游 sandbox:8080）
    | internal bridge
    v
受管 Agent sandbox（仅 internal，无宿主端口）

现有 Compose Go Agent（独立开发实例，不属于 Control 管理）
```

只有宿主侧 Control 可以访问 Docker daemon。Docker socket 不得挂载到 sandbox 或入口代理中。

Control 当前没有认证，只适合作为本机开发入口。启动时必须绑定 `127.0.0.1`，不得将其直接暴露到不可信网络。

## 3. 目录与组件职责

```text
control_plane/
  app.py
  pyproject.toml
  uv.lock
  api/
    __init__.py
    health.py
    sandboxes.py
  core/
    __init__.py
    models.py
    errors.py
    service.py
  runtime/
    __init__.py
    sandbox_runtime.py
    docker/
      __init__.py
      docker_runtime.py
      transaction.py
  tests/
```

控制面顶层只有 `app.py` 一个 Python 源文件，使用 namespace package；项目配置、锁文件和测试保留在 `control_plane/`。协作约束统一放在仓库根 `AGENTS.md`，不创建同名嵌套目录或兼容包装层。

### `control_plane/app.py` 与 `api/`

`app.py` 只负责创建 FastAPI 应用、装配依赖和注册路由，保留模块级 `app`。`create_app(runtime=None, readiness_timeout=1.0, sandbox_query_timeout=1.0)` 支持注入自定义 `SandboxRuntime`。默认只创建一个 `DockerSandboxRuntime`，再用它创建一个 `SandboxService`，健康与 sandbox 路由共享同一个 Service；测试使用完整实现抽象契约的 FakeRuntime。

`api/health.py` 定义 `/healthz` 和 `/readyz`；`api/sandboxes.py` 定义创建、状态/入口查询、删除和 HTTP 错误映射。HTTP 层统一依赖 `SandboxService`，不导入 Runtime 基类、Docker 实现或 Docker SDK。所有涉及运行时的请求均经过 `API → SandboxService → SandboxRuntime`。`/healthz` 不访问 Docker，`/readyz` 和状态查询分别使用自己的整体超时；创建只接受空请求体，删除不套用查询的 1 秒等待超时。错误响应不包含底层异常、Docker socket 路径、环境变量或调用栈。

### `control_plane/core/`

`models.py` 保存 `SandboxSpec`、资源类型、状态与结果，`errors.py` 保存领域异常。`service.py` 的 `SandboxService` 仅依赖公共 `runtime/sandbox_runtime.py` 契约：`ping()` 直接委托运行时并传播领域异常；`create()` 生成不可预测的 sandbox ID、资源命名和事务标签，只有运行时创建成功才组装 `status="started"` 结果；`get()`、`get_endpoint()` 和 `delete()` 在调用运行时前校验 ID，删除还传递默认关闭的 `delete_workspace` 参数。core 不依赖 FastAPI 或具体 Docker 实现。

Service 支持注入时钟。每次创建在调用 Runtime 前只读取一次带时区的 UTC 时间，作为 `created_at`，按固定默认 TTL 1800 秒计算 `expires_at`。公共创建规格与创建、查询结果携带这两个时间；Runtime 接收已计算值，不自行取时。对外统一使用 RFC 3339 UTC 格式，例如 `2026-10-19T08:00:00Z`。读取旧资源时不通过模型默认值生成时间。

所有资源包含以下标签：

- `io.agent-sandbox.managed=true`
- `io.agent-sandbox.project=agent-sandbox`
- `io.agent-sandbox.sandbox-id=<sandbox_id>`
- `io.agent-sandbox.transaction-id=<transaction_id>`
- `io.agent-sandbox.resource=container|network|workspace|proxy|ingress`

新创建的五项资源还带有一致的初始生命周期时间标签：

- `io.agent-sandbox.created-at=<RFC 3339 UTC created_at>`
- `io.agent-sandbox.expires-at=<RFC 3339 UTC expires_at>`

时间标签不替代上述完整归属标签。它们保存初始生命周期信息，后续续期需要可更新的持久化元数据，当前不使用 `container.update` 修改标签。

### `control_plane/runtime/`

- `sandbox_runtime.py` 使用 `ABC` 和 `abstractmethod` 定义异步 `ping() -> None`、`create(spec: SandboxSpec) -> None`、`get(sandbox_id: str) -> SandboxQueryResult`、`get_endpoint(sandbox_id: str) -> SandboxEndpoint` 和 `delete(sandbox_id: str, *, delete_workspace: bool = False) -> None`，只依赖公共模型，不包含 Docker SDK、线程或回滚代码。
- `docker/docker_runtime.py` 的 `DockerSandboxRuntime` 完整实现五项异步操作，集中只读 ping、查询和状态映射、线程调度与取消后的清理等待；异步 `delete()` 委托同步删除实现。ping 失败映射为领域异常 `DockerUnavailable`，HTTP 超时及错误响应仍由 API 处理。
- `docker/transaction.py` 集中同步 Docker 资源生命周期操作与归属工具，保存生命周期 client 协议与延迟工厂、固定配置创建事务、资源记录、归属校验、错误分类和创建失败逆序回滚；正式删除独立执行标签清单恢复、完整归属、有效事务、跨资源一致性与角色唯一性预检，以及每步精确标识重读、回收和结果确认。查询复用相同的标签读取与核验逻辑。

`runtime/__init__.py` 不自动加载具体后端。独立探测类和创建/查询转发接口已经移除；Docker 后端业务代码集中在上述两个文件。只读 ping transport timeout 保持 0.5 秒，生命周期创建、查询与删除保持 5 秒。`DockerSandboxRuntime` 可分别注入生命周期 `client_factory` 和只读 `ping_client_factory`，两者均延迟调用，避免合并入口后混用传输超时。

应用导入和调用 `create_app()` 均不会连接 Docker。每次操作延迟创建独立 client 并负责关闭。runtime 创建前检查本地已有的 `agent-sandbox:dev` 和 `agent-sandbox-proxy:dev` 镜像，不 pull、不 build，也不接受 HTTP 请求传入的 Docker 配置；创建顺序为两镜像检查、internal bridge、ingress bridge、workspace volume、sandbox 创建/启动、proxy 创建、连接 internal、proxy 启动。

每个 sandbox 容器固定使用非 root `sandbox` 用户、`/workspace` 工作目录、1 CPU、256 MiB 内存、无额外 swap、64 PID、只读根文件系统、32 MiB `/tmp` tmpfs、drop ALL capabilities 和 no-new-privileges。sandbox 只连接专属 internal bridge，DNS 别名为 `sandbox`，不发布宿主机端口。专属 proxy 使用打包现有 `proxy.conf` 的代理镜像，直接以 nginx 用户运行；连接专属普通 ingress 和 internal，仅发布动态 `127.0.0.1` 端口。不挂载 workspace、socket 或宿主目录；使用 0.5 CPU、64 MiB 内存及同等 swap 上限、32 PID、只读根、16 MiB `/tmp`、drop ALL 和 no-new-privileges。

每次创建尝试还会生成独立、不可预测的事务 ID，用于区分同一 sandbox ID 下的既有资源。volume 创建采用 Docker 的幂等 API 后，必须先核对响应中的事务标签；标签不符时按资源冲突处理，不挂载或删除该 volume。

失败或请求取消时，runtime 逆序处理本次事务记录的资源。低层 Docker create 响应一旦返回，就先记录资源 ID，再执行可能失败的模型 inspect；每次删除前重新通过精确资源 ID 加载，并核对 ID 与完整标签。重复取消也不能中断清理等待。runtime 不会按名称、前缀或全局扫描接管其他资源，清理不完整时返回包含 sandbox ID 的独立领域错误。

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
2. 路由在整体超时内调用 `SandboxService.ping()`，Service 委托 `DockerSandboxRuntime.ping()` 在线程中延迟创建 Docker client。
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
2. `SandboxService` 生成不可预测的 ID、资源名和标签，并一次取时生成 `created_at` 与 1800 秒后的 `expires_at`。
3. `DockerSandboxRuntime` 在工作线程中执行固定创建事务，将相同的初始时间标签写入五项资源。
4. 创建并启动成功后返回 HTTP 201；这不包含 Agent 就绪探测。
5. 任一步骤失败时，runtime 只回滚本次已记录且归属核对通过的资源，再由 HTTP 层返回稳定错误。

成功响应示例：

```json
{
  "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
  "status": "started",
  "created_at": "2026-10-19T08:00:00Z",
  "expires_at": "2026-10-19T08:30:00Z"
}
```

创建错误映射（查询状态与错误契约见 [控制面 API](CONTROL_API.md)）：

| HTTP | code                          | 含义                                              |
| ---- | ----------------------------- | ------------------------------------------------- |
| 400  | `invalid_request`           | 请求体非空                                        |
| 503  | `sandbox_image_unavailable` | 固定镜像不存在                                    |
| 503  | `docker_unavailable`        | Docker daemon 不可用                              |
| 409  | `sandbox_resource_conflict` | 生成的资源名冲突                                  |
| 500  | `sandbox_create_failed`     | 创建失败且已完成回滚                              |
| 500  | `sandbox_cleanup_failed`    | 创建失败且回滚不完整；错误对象额外包含 sandbox ID |

错误体不会返回 Docker 异常、socket 路径、环境变量或调用栈。

### `/v1/sandboxes/{sandbox_id}`

1. HTTP 路由在查询整体超时内调用共享的 `SandboxService.get(sandbox_id)`。
2. Service 校验 ID；格式无效时抛出 `SandboxNotFound`，不调用运行时。
3. `DockerSandboxRuntime.get()` 只读加载候选容器，重新按精确 ID 检查完整归属标签，从时间标签恢复 `created_at`、`expires_at`，并映射 Docker 与 health 状态。新建 Runtime/Service 或控制面重启后仍恢复原值，不重新取时或推迟过期时间。
4. 两项时间标签均不存在时，旧实例的两个时间字段返回 `null`，不补写或接管 TTL；仅缺一项、格式错误或过期时间早于创建时间时，返回稳定的 HTTP 500 `sandbox_query_failed`，不返回原始解析异常。
5. API 保持现有 404/503/504/500 错误映射与脱敏响应，查询不修改或清理 Docker 资源。旧实例的查询、endpoint 与删除能力保留。

`expires_at` 仅表示计划过期时间。本阶段没有过期扫描、自动删除或续期；时间已过仍返回原值，状态、endpoint 和命令执行沿用现有语义。`ready` 继续表示 Agent 健康，不增加 `expired` 状态。

### `DELETE /v1/sandboxes/{sandbox_id}`

1. HTTP 层解析 `delete_workspace`，默认为 false；Service 在访问 Docker 前拒绝格式不符合 `sbx_[0-9a-f]{32}` 的 ID。
2. Docker 工作线程从项目、受管标记、sandbox ID 和角色标签发现五类候选，即使只剩网络或 workspace 也可恢复清单。全量预检要求完整标签、有效且跨资源一致的事务 ID、各角色唯一；归属冲突先拒绝，不任意选择资源。
3. 按 proxy、sandbox、ingress、internal、可选 workspace 的顺序回收。每次 stop/remove 前按记录的精确 ID 或经核验的卷标识重新加载并检查归属；容器 stop 使用 2 秒宽限，失败后重验再强制移除该容器，不附带删卷。网络和卷不强制移除，也不强制断开其他容器。
4. 每项移除后确认不存在，资源已经不存在视为完成。清理不完整必须报错，后续请求重新发现残留继续清理；已经删除的项不重建。
5. 删除启动后的取消和重复取消都等待工作线程收尾并关闭 client 后再传播，不将 asyncio 取消视为 SDK 已停止。

成功返回 HTTP 204 且无响应体。格式合法但没有目标受管资源的 ID、默认删除后仅剩保留卷的情况也返回 204。默认保留卷及其中数据，保留卷持续占用磁盘；`?delete_workspace=true` 会在最后不可恢复地删除该实例专属卷。创建失败回滚仍清理本次 workspace，不采用正式删除的保留策略。

| HTTP | code                        | message / 含义                                                          |
| ---- | --------------------------- | ----------------------------------------------------------------------- |
| 404  | `sandbox_not_found`       | ID 格式无效，未调用 Docker                                              |
| 409  | `sandbox_delete_conflict` | `sandbox resource ownership conflict`，归属歧义、事务不一致或角色重复 |
| 503  | `docker_unavailable`      | 后端不可用                                                              |
| 500  | `sandbox_delete_failed`   | `sandbox deletion was incomplete`，错误对象包含 `sandbox_id`        |

删除不以容器健康、endpoint 可访问或拓扑完整为前提，不使用数据库、墓碑或创建时的内存记录。详见 [删除设计](docker/DELETION.md)。

## 5. 环境要求

- WSL Ubuntu。
- Python 3.12（本阶段验收环境）。
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
cd ~/agent-sandbox/control_plane
uv sync --locked
```

旧 `control/.venv` 保留在原处，不迁移或删除；新项目目录通过上述命令重建环境。

运行依赖包括 FastAPI、Uvicorn 和 Python Docker SDK；测试依赖包括 pytest、httpx 和 pytest-timeout。

## 7. 启动与停止

仅监听本机 `127.0.0.1:18083`：

```bash
cd ~/agent-sandbox/control_plane
uv run uvicorn control_plane.app:app --app-dir .. --host 127.0.0.1 --port 18083
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
cd ~/agent-sandbox/control_plane
DOCKER_HOST=unix:///tmp/agent-sandbox-control-missing.sock \
  uv run uvicorn control_plane.app:app --app-dir .. --host 127.0.0.1 --port 18083
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
docker image inspect agent-sandbox:dev agent-sandbox-proxy:dev
```

发送没有请求体的 POST：

```bash
curl -i -X POST http://127.0.0.1:18083/v1/sandboxes
```

HTTP 201 响应中的 `started` 只表示容器已启动，同时返回相差 1800 秒的 `created_at`、`expires_at`。sandbox 无宿主端口，入口由专属 proxy 发布；通过 `GET /v1/sandboxes/{sandbox_id}/endpoint` 获取实际本机地址，503 时可在有限期限内重试。可通过 `GET /v1/sandboxes/{sandbox_id}` 查询 Docker、健康事实及相同的初始时间；重启控制面后再次查询，两个时间应保持不变。

### 删除 sandbox

只使用本次 POST 响应记录的 `SANDBOX_ID`。默认删除运行资源并保留 workspace；成功返回 204 空体，状态和入口查询改为 404，旧 endpoint 不再可访问：

```bash
curl -i -X DELETE "http://127.0.0.1:18083/v1/sandboxes/$SANDBOX_ID"
```

保留卷会持续占用磁盘。下面的显式参数会不可恢复地删除该卷及全部文件；可在默认删除后对同一 ID 执行：

```bash
curl -i -X DELETE "http://127.0.0.1:18083/v1/sandboxes/$SANDBOX_ID?delete_workspace=true"
```

DELETE 支持幂等重试，控制面重启后仍可从 Docker 标签执行；清理失败时对同一 ID 重试，不使用名称前缀批量删除或 `docker prune`。完整双实例手动验收步骤见 [curl 手动测试](CURL_TESTS.md#动态实例的手动删除)。

## 9. 自动验证

在 `control_plane/` 目录执行：

```bash
uv sync --locked
uv run --locked pytest
uv run python -m compileall -q app.py api core runtime tests
```

测试覆盖：

- `/healthz` 与 Docker 状态相互独立。
- `/readyz` 成功、异常和超时响应。
- 不支持的 HTTP 方法返回 405。
- 稳定错误体不泄露宿主机信息。
- 新入口首次导入、应用装配和 Docker client 延迟创建。
- `ping()` 成功、失败和 HTTP 已超时后的迟到失败均能关闭 client。
- 探测器不暴露容器、网络或 volume 管理操作。
- 创建 API 的空请求契约、成功响应和稳定错误映射。
- 固定时钟、单次取时、精确 1800 秒 TTL 和 UTC 序列化，五项资源的时间标签一致。
- 创建与查询时间一致、新 Runtime/Service 恢复原值、旧实例双 `null`、时间标签缺项/格式/顺序错误，以及超过计划过期时间后仍保留现有状态和入口语义。
- 固定资源名称、标签、安全参数、调用顺序、精确逆序回滚、取消与重复取消清理。
- Docker/health 状态映射、查询超时、错误脱敏和控制面重启后查询。
- 删除默认保留与显式删卷、未知/无效 ID、重复/并发请求、残留发现、归属/事务/角色预检、停止失败、占用失败后重试，以及取消/重复取消后的线程和 client 收尾。
- Docker 集成测试默认跳过，不会修改 Docker 资源。

真实 Docker 集成测试必须显式启用：

```bash
RUN_DOCKER_INTEGRATION=1 uv run --locked pytest -m docker_integration -v
```

测试验证固定创建配置、真实五项资源的初始时间标签，以及两个动态实例的不同端口、正确路由、独立工作区、HTTP/文件/SSE、重启恢复和普通 bridge 公网出口对照；新 Runtime/Service 查询应返回原始创建与过期时间。删除测试由新 app/runtime 删除 A，核对查询 404、旧入口失效、默认保留卷和文件、再显式删卷，并确认 B 继续可用；最终只按本次记录的精确资源 ID 和完整标签清理。运行前仍应记录现有容器、网络和 volume 快照，并确认两个固定镜像均已存在；测试不会 pull 或 build 镜像。

在仓库根目录额外执行：

```bash
git diff --check
```

## 10. 动态入口查询

入口查询只读加载 sandbox、proxy 和两网，核验完整归属、相同事务 ID、网络角色及 internal 属性、实际连接、DNS 别名、代理 running/healthy。仅从实际 `NetworkSettings.Ports` 接受唯一 loopback 有效端口。旧实例无代理或入口任何核验失败为稳定 503，未知/无效 ID 为 404，整体与 transport 超时为稳定 504。状态 ready 保持内部 Agent 健康语义。

详细契约见 [CONTROL_API.md](CONTROL_API.md)，设计与验证边界见 [DYNAMIC_ENDPOINT.md](DYNAMIC_ENDPOINT.md)。入口是本机 WSL 地址，不承诺远程能力；查询后的可用性仍可能随资源退出而变化。

Docker 后端的创建事务、归属核验、取消回滚、状态/入口查询和正式删除实现，见 [Docker Runtime 设计与实现说明](docker/README.md)；删除边界见 [删除设计](docker/DELETION.md)。

## 11. 当前限制

- 没有认证、授权和审计能力。
- 当前支持创建、基于 Docker 事实的状态/入口查询和手动删除，没有主动 Agent 就绪探测、续期、暂停或恢复接口。
- TTL 仅记录初始时间，没有过期扫描、自动删除或续期；没有数据库、持久化删除状态或后台回收。部分删除失败需要调用者重试，保留 workspace 的磁盘占用和后续卷回收策略仍需单独管理。
- 当前 Docker 标签保存初始生命周期信息，后续续期需要可更新的持久化元数据。
- `/readyz` 只说明 Docker daemon 能否响应 ping，不代表任意 sandbox 已经就绪。
- 创建结果 `started` 也不表示容器内 Agent 已经就绪。
- endpoint 只支持本机 WSL 开发；无 proxy 的旧实例返回入口不可用，不自动迁移。
- 当前实现不是完整的控制面，也不是安全隔离能力。

后续增加生命周期操作时，应继续保持 HTTP 层与 Docker 访问层分离，并为资源创建失败、部分成功、清理和恢复定义明确语义。

## 12. 常见问题

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
