# Control 控制面设计与启动

## 1. 当前定位

`control_plane/` 是运行在 WSL 宿主侧的独立 Python/FastAPI 进程，与容器中的 Go Agent 分离。当前版本提供：

- `GET /healthz`：检查 Control HTTP 进程自身是否正常。
- `GET /readyz`：通过只读 Docker `ping()` 检查 Docker daemon 是否可用。
- `POST /v1/sandboxes`：以固定安全策略创建一个独立 Docker sandbox，记录创建和初始计划过期时间。
- `GET /v1/sandboxes/{sandbox_id}`：读取 Docker 容器、Agent 健康状态，并合并身份匹配的 SQLite 生命周期时间，控制面重启后仍可查询。
- `GET /v1/sandboxes/{sandbox_id}/endpoint`：核验专属代理入口并返回实际本机地址，重启后仍可恢复。
- `POST /v1/sandboxes/{sandbox_id}/renew`：延长未到期、身份匹配的 `active` 实例期限，不修改 Docker 初始时间标签。
- `DELETE /v1/sandboxes/{sandbox_id}`：回收专属容器、代理和两网，默认保留 workspace；`delete_workspace=true` 显式删除卷。

当前 Control 支持创建、使用、显式续期、手动删除和到期自动回收；不提供主动 Agent 就绪探测，也不接管现有 Agent、Compose 容器、`agent-sandbox-internal` 网络和 `agent-sandbox-workspace` volume。后台回收默认启用，以单进程、单 worker 为运行前提。

## 2. 进程与信任边界

```text
本机调用方
    |
    | HTTP 127.0.0.1:18083
    v
Python/FastAPI Control（WSL 宿主侧）
    |-- SQLite：control_plane/.local/lifecycle.sqlite3
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
    reaper.py
  runtime/
    __init__.py
    sandbox_runtime.py
    docker/
      __init__.py
      docker_runtime.py
      transaction.py
  storage/
    __init__.py
    lifecycle_store.py
    AGENTS.md
  tests/
```

控制面顶层只有 `app.py` 一个 Python 源文件，使用 namespace package；项目配置、锁文件和测试保留在 `control_plane/`。协作约束统一放在仓库根 `AGENTS.md`，不创建同名嵌套目录或兼容包装层。

### `control_plane/app.py` 与 `api/`

`app.py` 负责创建 FastAPI 应用、装配 Runtime、Store 和 Service，并注册路由，保留模块级 `app`。`create_app()` 支持注入 `runtime`、`store` 或测试 `database_path`，保留 `readiness_timeout=1.0`、`sandbox_query_timeout=1.0`；后台回收参数见下文。默认创建一个 `DockerSandboxRuntime` 和一个 `SQLiteLifecycleStore`，注入 `SandboxService(runtime, store)`，健康与 sandbox 路由共享同一个 Service；测试显式使用临时数据库或 Fake Store。

默认路径通过 `app.py` 的文件位置定位到 `control_plane/.local/lifecycle.sqlite3`，不随启动目录变化。FastAPI lifespan 显式调用 `Store.initialize()`，初始化失败使应用启动失败，也不会启动回收任务，不切换到内存或临时存储。导入模块、构造 Store 和调用 `create_app()` 不创建数据库、目录、连接、Docker client 或后台任务。

`api/health.py` 定义 `/healthz` 和 `/readyz`；`api/sandboxes.py` 定义创建、状态/入口查询、续期和删除路由，`api/errors.py` 集中固定 HTTP 错误映射及 endpoint 的不可用归并策略。映射支持已知异常的子类，不读取异常消息生成公开文案；未知错误继续交由上层诊断。HTTP 层统一依赖 `SandboxService`，不导入 Runtime 基类、Docker 实现或 Docker SDK。所有涉及运行时的请求均经过 `API → SandboxService → SandboxRuntime`。`/healthz` 不访问 Docker，`/readyz` 和状态查询分别使用自己的整体超时；创建只接受空请求体，续期和删除不套用查询的 1 秒等待超时。错误响应不包含底层异常、Docker socket 路径、环境变量或调用栈。

### `control_plane/core/`

`models.py` 保存 `SandboxSpec`、`SandboxLifecycleRecord`、资源类型、状态与结果，`errors.py` 保存领域异常。`SandboxService` 依赖公共 `SandboxRuntime` 和最小 `LifecycleStore` 协议，不依赖 FastAPI、SQLite 连接或具体 Docker 实现。`ping()` 和 `get_endpoint()` 继续委托 Runtime；创建、状态查询、续期和删除由 Service 协调 Runtime 与 Store，详细流程见下一节。实例 ID 在查询、endpoint、续期与删除访问后端前校验。

`core/tasks.py` 集中不向 worker 转发取消的等待、穿过重复取消的收尾和最终结果读取。Service、Runtime 与 reaper 复用这些基础动作，但分别决定创建补偿、删除收尾和续期提交后的取消策略。该机制使用 `asyncio.wait()`，避免 Python 3.14 中取消 shield 后对迟到 worker 异常的额外报告。

创建领域异常可携带不可变的 `CreationFailureInfo`，区分阶段、`unchanged/rolled_back/remaining/unknown` 资源状态和脱敏 `CleanupIssue`。Service 仅在前两种状态下补偿元数据，不再维护异常类型白名单；未知结果继续保留诊断记录。原有具体异常与错误响应保留，旧 Runtime 的默认恢复语义见 `SandboxRuntime.create()`。详细边界见 [异常处理设计](CONTROL_PLANE_ERRORS.md)。

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

时间标签不替代上述完整归属标签，只保存初始生命周期信息。SQLite 记录可更新的过期时间；修改记录不改写 Docker 标签，当前不使用 `container.update` 修改标签，也不自动接管没有数据库记录的旧实例。

### `control_plane/storage/`

`lifecycle_store.py` 集中定义最小 `LifecycleStore` 协议、稳定存储异常和标准库 `sqlite3` 实现，没有 ORM、连接池或新依赖。记录包含不可变的 sandbox ID、事务 ID、创建时间，以及可更新的 `expires_at`、`management_state` 和从 0 开始的版本。过期时间更新、状态迁移和删除通过事务 ID 与预期版本保护；成功修改递增版本，旧版本不能覆盖新值。

管理状态按 `pending → active → deleting → 移除记录` 推进：`pending` 表示创建尚未确认或结果不明确，`active` 表示 Runtime 与元数据确认均已完成，`deleting` 表示已进入删除流程。手动删除允许处理三种状态；`deleting` 禁止更新过期时间。这些内部状态不公开为 HTTP `status`，也不表示 Agent 健康。当前 schema 为 2，只初始化新库或经过核验的同版本数据库，不支持旧版本迁移；测试使用全新临时数据库，不修改现有开发数据库。

时间以带时区 UTC 表达，持久化和比较使用整数微秒；到期筛选使用调用方传入的 `now`，包含恰好到期记录，以过期时间、sandbox ID 稳定排序并限制条数。所有同步操作在线程中创建、提交或回滚并关闭自己的连接，使用参数化 SQL 和有限锁等待，不跨线程共享连接。Store 只操作元数据，不访问 Docker；Runtime 不访问 SQLite。

### `control_plane/runtime/`

- `sandbox_runtime.py` 使用 `ABC` 和 `abstractmethod` 定义异步 `ping()`、`create(spec)`、`get(sandbox_id)`、`get_endpoint(sandbox_id)` 和 `delete(sandbox_id, *, delete_workspace=False, expected_transaction_id=None)`，只依赖公共模型，不包含 Docker SDK、线程或回滚代码。查询结果携带内部 `transaction_id`；删除可接收预期事务 ID，两者不新增为 HTTP 字段或参数。
- `docker/docker_runtime.py` 的 `DockerSandboxRuntime` 完整实现五项异步操作，集中只读 ping、查询和状态映射、线程调度与取消后的清理等待；异步 `delete()` 委托同步删除实现。ping 失败映射为领域异常 `DockerUnavailable`，HTTP 超时及错误响应仍由 API 处理。
- `docker/transaction.py` 集中同步 Docker 资源生命周期操作与归属工具，保存生命周期 client 协议与延迟工厂、固定配置创建事务、资源记录、归属校验、错误分类和创建失败逆序回滚；正式删除独立执行标签清单恢复、完整归属、有效事务、跨资源一致性与角色唯一性预检，以及每步精确标识重读、回收和结果确认。查询复用相同的标签读取与核验逻辑。

`runtime/__init__.py` 不自动加载具体后端。独立探测类和创建/查询转发接口已经移除；Docker 后端业务代码集中在上述两个文件。只读 ping transport timeout 保持 0.5 秒，生命周期创建、查询与删除保持 5 秒。`DockerSandboxRuntime` 可分别注入生命周期 `client_factory` 和只读 `ping_client_factory`，两者均延迟调用，避免合并入口后混用传输超时。

应用导入和调用 `create_app()` 均不会连接 Docker。每次操作延迟创建独立 client 并负责关闭。runtime 创建前检查本地已有的 `agent-sandbox:dev` 和 `agent-sandbox-proxy:dev` 镜像，不 pull、不 build，也不接受 HTTP 请求传入的 Docker 配置；创建顺序为两镜像检查、internal bridge、ingress bridge、workspace volume、sandbox 创建/启动、proxy 创建、连接 internal、proxy 启动。

每个 sandbox 容器固定使用非 root `sandbox` 用户、`/workspace` 工作目录、1 CPU、256 MiB 内存、无额外 swap、64 PID、只读根文件系统、32 MiB `/tmp` tmpfs、drop ALL capabilities 和 no-new-privileges。sandbox 只连接专属 internal bridge，DNS 别名为 `sandbox`，不发布宿主机端口。专属 proxy 使用打包现有 `proxy.conf` 的代理镜像，直接以 nginx 用户运行；连接专属普通 ingress 和 internal，仅发布动态 `127.0.0.1` 端口。不挂载 workspace、socket 或宿主目录；使用 0.5 CPU、64 MiB 内存及同等 swap 上限、32 PID、只读根、16 MiB `/tmp`、drop ALL 和 no-new-privileges。

每次创建尝试还会生成独立、不可预测的事务 ID，用于区分同一 sandbox ID 下的既有资源。volume 创建采用 Docker 的幂等 API 后，必须先核对响应中的事务标签；标签不符时按资源冲突处理，不挂载或删除该 volume。

失败或请求取消时，runtime 逆序处理本次事务记录的资源。低层 Docker create 响应一旦返回，就先记录资源 ID，再执行可能失败的模型 inspect；每次删除前重新通过精确资源 ID 加载，并核对 ID 与完整标签。重复取消也不能中断清理等待。runtime 不会按名称、前缀或全局扫描接管其他资源，清理不完整时返回包含 sandbox ID 的独立领域错误。

回滚接受精确 ID 的 `NotFound`，并在删除响应后复查资源是否消失；检查失败和确认残留分别记录为未知或有残留。已登记资源清理成功不能证明丢失创建响应的资源不存在。状态和入口查询的主错误不被 client 关闭失败覆盖；独立关闭失败转换为稳定查询错误或超时，日志只包含操作和异常类型。

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
3. Service 先将 `pending`、版本为 0 的生命周期记录写入 Store。插入失败或 ID 冲突不调用 Runtime，也不创建 Docker 资源。
4. `DockerSandboxRuntime` 在工作线程中执行固定创建事务，将相同的初始时间标签写入五项资源。
5. Runtime 创建成功后，Service 将同一事务和预期版本的记录原子激活为 `active`，版本递增。只有激活也成功，才返回 HTTP 201；这不包含 Agent 就绪探测。激活失败或结果不明确时保留可追踪记录，不报告创建成功。
6. Runtime 明确完成失败回滚后，Service 按原事务 ID 和版本清理预写记录；回滚不完整或结果不明确时保留记录，返回包含 sandbox ID 的稳定错误。

SQLite 与 Docker 没有跨系统原子事务，SQL 回滚不能撤销 Docker 操作。数据库有记录不表示实例创建成功或仍存在。SQLite 写入期间取消，Service 必须等待最终结果；若已提交且尚未启动 Runtime，则按原事务 ID 和版本补偿。Runtime 创建期间取消仍由 Runtime 等待精确回滚，Service 确认完成后再清理元数据；首次及重复取消都等待关键收尾并读取后台异常，补偿失败保留 sandbox ID 诊断依据。

已确认创建的资源在取消补偿前先进入 `deleting`，清理失败可由下一轮重试。若手动删除已移除或正在移除记录，晚到的创建成功仍须清理原事务资源；元数据冲突时不刷新版本、不删除替代记录，也不重建已移除的记录。

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
| 503  | `sandbox_metadata_unavailable` | 预写或激活失败，含 sandbox ID；预写失败不调用 Runtime |
| 409  | `sandbox_metadata_conflict` | 预写或激活身份、版本冲突，含 sandbox ID |
| 409  | `sandbox_resource_conflict` | 生成的资源名冲突                                  |
| 500  | `sandbox_create_failed`     | 创建失败且已完成回滚                              |
| 500  | `sandbox_cleanup_failed`    | 创建失败且回滚不完整；错误对象额外包含 sandbox ID |
| 500  | `sandbox_create_uncertain` | Runtime 结果不明确，保留记录，含 sandbox ID |
| 500  | `sandbox_metadata_cleanup_failed` | 资源回滚或写入取消后的元数据补偿失败，含 sandbox ID |

错误体不会返回 Docker 异常、socket 路径、环境变量或调用栈。

### `/v1/sandboxes/{sandbox_id}`

1. HTTP 路由在查询整体超时内调用共享的 `SandboxService.get(sandbox_id)`。
2. Service 校验 ID；格式无效时抛出 `SandboxNotFound`，不调用运行时。
3. `DockerSandboxRuntime.get()` 只读加载候选容器，按精确 ID 检查完整归属标签，恢复初始时间和内部事务 ID，并映射 Docker 与 health 状态；实际实例不存在仍返回 404，不因数据库有记录而报告实例存在。
4. Service 读取 Store，核对记录的 sandbox ID、事务 ID 和创建时间。匹配时使用数据库当前的 `expires_at`；状态、reason 与 message 来自 Runtime，内部事务 ID 不公开。重启后仍读取同一个数据库，不重新取时或推迟过期时间。
5. 没有记录时保留 Docker 初始时间，旧实例两个时间标签均不存在时返回两个 `null`，不补写记录或接管 TTL。标签仅缺一项、格式错误或过期早于创建时仍返回 `500 sandbox_query_failed`。
6. Store 不可用返回 `503 sandbox_metadata_unavailable`，身份或创建时间不匹配返回 `409 sandbox_metadata_conflict`，均包含 sandbox ID，不静默回退到可能过时的标签值。查询不修改数据库或 Docker 资源。

查询仅返回计划过期时间，不触发删除。后台周期循环调用单轮回收处理到期实例；实际删除前仍沿用现有状态、endpoint 和命令语义。`ready` 继续表示 Agent 健康，不增加 `expired` 状态。

### `POST /v1/sandboxes/{sandbox_id}/renew`

1. HTTP 层只接受 `{"ttl_seconds": 1800}` 形式的 JSON 对象；TTL 为必填的真正整数，范围 1～86400，拒绝布尔值、浮点数、字符串、`null` 和额外字段。输入无效返回统一 `400 invalid_request`，不调用后端。
2. `SandboxService.renew()` 校验 ID，并只读调用 Runtime 确认实际实例，再读取 Store 核对实例 ID、事务 ID 和创建时间；无效或不存在为 404，身份不匹配为 `409 sandbox_metadata_conflict`。
3. 本次操作只读取一次 UTC 时钟。仅有记录、`active` 且 `expires_at > now` 时可续期，目标为 `max(current_expires_at, now + ttl_seconds)`；不累加、不缩短，恰好到期也拒绝。无记录、状态不允许或已到期返回 `409 sandbox_renew_conflict`。
4. 使用事务 ID 与预期版本调用 `update_expiration()`。续期先提交会使旧回收认领失效；回收或手动删除先进入 `deleting` 后拒绝续期。并发续期版本冲突返回 `sandbox_renew_conflict`，不自动刷新重试。续期成功不阻止随后发起的显式手动删除。
5. 成功返回 HTTP 200 和实际保存的 `sandbox_id`、RFC 3339 UTC `expires_at`。新期限持久化供查询及后台回收使用，重启后仍有效；不修改 Docker 标签，不新增 Runtime 续期方法，也不表示 Agent 已就绪。

续期不套用只读查询超时。SQLite 写入取消时，首次及重复取消都等待最终结果并读取异常；已经提交的期限不做反向补偿，避免覆盖后续更新。未收到响应的调用方需查询确认。Store 或 Runtime 故障沿用稳定领域错误，完整契约见 [续期 API](CONTROL_API.md#7-续期-sandbox)。

### `DELETE /v1/sandboxes/{sandbox_id}`

1. HTTP 层解析 `delete_workspace`，默认为 false；Service 在访问 Docker 前拒绝格式不符合 `sbx_[0-9a-f]{32}` 的 ID。
2. Service 先读取 Store；有记录时按预期事务与版本将 `pending` 或 `active` 转为 `deleting` 并递增版本，已有 `deleting` 可重试。再把认领记录的预期事务 ID 交给 Runtime；无记录旧实例按原规则删除。Store 读取或认领失败不开始 Docker 删除。
3. Docker 工作线程从项目、受管标记、sandbox ID 和角色标签发现五类候选，即使只剩网络或 workspace 也可恢复清单。全量预检要求完整标签、有效且跨资源一致的事务 ID、各角色唯一，有预期事务 ID 时所有存留角色都必须匹配；归属冲突先拒绝，不任意选择资源。不依赖 `Runtime.get()` 成功。
4. 按 proxy、sandbox、ingress、internal、可选 workspace 的顺序回收。每次 stop/remove 前按记录的精确 ID 或经核验的卷标识重新加载并检查归属；容器 stop 使用 2 秒宽限，失败后重验再强制移除该容器，不附带删卷。网络和卷不强制移除，也不强制断开其他容器。
5. 每项移除后确认不存在，资源已经不存在视为完成。Docker 清理失败保留 `deleting`，后续请求重新发现残留继续清理；已经删除的项不重建。
6. Runtime 确认删除目标完成后，Service 才按认领记录的事务 ID 和版本删除元数据；默认保留 workspace 也清理已结束实例记录，残卷由 Docker 标签管理。元数据清理失败或版本冲突返回稳定失败和 sandbox ID，不盲目刷新版本后删除，保留重试依据。
7. 首次和重复取消都等待 Runtime 删除及后续数据库关键收尾，再传播取消或可追踪失败，并读取后台异常，不将 asyncio 取消视为工作线程已停止。

成功返回 HTTP 204 且无响应体。格式合法但没有目标受管资源的 ID、默认删除后仅剩保留卷的情况也返回 204。默认保留卷及其中数据，保留卷持续占用磁盘；`?delete_workspace=true` 会在最后不可恢复地删除该实例专属卷。创建失败回滚仍清理本次 workspace，不采用正式删除的保留策略。

| HTTP | code                        | message / 含义                                                          |
| ---- | --------------------------- | ----------------------------------------------------------------------- |
| 404  | `sandbox_not_found`       | ID 格式无效，未调用 Docker                                              |
| 409  | `sandbox_delete_conflict` | `sandbox resource ownership conflict`，归属歧义、事务不一致或角色重复 |
| 503  | `docker_unavailable`      | 后端不可用                                                              |
| 503  | `sandbox_metadata_unavailable` | 读取或删除认领失败，未调用 Runtime，含 sandbox ID |
| 409  | `sandbox_metadata_conflict` | 生命周期记录身份冲突，含 sandbox ID |
| 500  | `sandbox_delete_failed`   | `sandbox deletion was incomplete`，错误对象包含 `sandbox_id`        |
| 500  | `sandbox_metadata_cleanup_failed` | Docker 已完成，但元数据清理失败或版本冲突，含 sandbox ID |

删除不以容器健康、endpoint 可访问或拓扑完整为前提，不依赖创建时的内存记录。持久化 `deleting` 支持后台下轮或手动重试；没有外部调度框架或墓碑。版本保护和删除状态协调续期与删除认领，不承诺续期成功后阻止显式手动删除。Docker 删除细节见 [删除设计](docker/DELETION.md)。

### 显式单轮过期回收

内部调用 `await service.reap_expired_once(limit=100)` 执行一轮回收，没有对应 HTTP API。单轮方法本身不创建调度任务，由应用后台循环周期调用。每轮只读取一次带时区 UTC 时钟，有界获取到期 `active` 与待重试 `deleting`，按过期时间和实例 ID 稳定排序；`pending` 不作为候选。

对 `active` 候选，Store 在同一 SQLite 事务中核对实例 ID、事务 ID、预期版本、状态和 `expires_at <= now`，成功后转为 `deleting`、递增版本并返回认领记录。候选已被续改、删除或不再到期则跳过，不调用 Runtime。过期时间更新先成功会使旧认领失效；认领先成功则拒绝后续过期时间更新。不在 SQLite 事务中等待 Docker。

Service 使用认领记录的事务和版本复用受保护删除流程，默认 `delete_workspace=False`。Runtime 达到删除目标后才清理元数据；Runtime 或元数据清理失败保留 `deleting`，下轮可重试。单项失败不妨碍本批其他候选，每项本轮最多尝试一次；数据库整体不可用则停止本轮，不降级为盲目删除。取消停止处理后续候选，等待已开始操作的关键收尾，重复取消也不会跳过等待。

结果包含本轮 `now`、`items`、`error` 及 `succeeded`、`skipped`、`failed` 计数。每项提供 `sandbox_id`、`outcome` 和稳定 `reason`：成功为 `deleted`，认领条件失效为 `candidate_changed`；单项 Runtime 或清理失败使用稳定领域错误码。数据库整体不可用时 `error` 为 `sandbox_metadata_unavailable`。本次不承诺跨进程恰好执行一次，也不提供分布式锁或租约。

### 后台周期回收

调用链为 `FastAPI lifespan → core/reaper.py → SandboxService.reap_expired_once()`。循环只负责调度，不访问 SQLite、Docker 或重新判断过期条件。

`create_app()` 提供以下参数：

| 参数 | 默认值 | 含义 |
| --- | --- | --- |
| `reaper_enabled` | `True` | 是否在应用 lifespan 启动回收任务 |
| `reaper_interval_seconds` | `60.0` | 每轮完成后等待的秒数，必须为有限正数 |
| `reaper_batch_limit` | `100` | 每轮候选上限，必须为 SQLite 支持的正整数 |

启用开关必须为布尔值；间隔和批次不接受布尔值或无效类型。配置在应用装配时校验。数据库初始化成功后，lifespan 创建唯一后台任务，第一轮在后台立即执行，不等待整轮完成再提供 HTTP 服务。同一应用实例的各轮串行，无候选、满批次和普通失败后均等待完整间隔，避免忙循环。无关测试显式传 `reaper_enabled=False`，专门验证自动回收的测试显式开启。

循环记录轮次处理数量、失败数量和稳定原因，同时检查单轮结果的 `error`、`items` 以及抛出的普通异常；失败不会永久停止后续轮次，日志不包含原始数据库或 Docker 异常。应用关闭时取消并等待任务；取消按单轮语义等待当前候选关键收尾，不再处理本批余下候选，也不启动下一轮。重复取消不会跳过等待，不用短超时假定工作线程已经退出。

当前仅支持单进程、单 worker，无分布式互斥或 exactly-once 保证。扫描间隔、单轮耗时及失败重试可能延迟删除，不承诺到期瞬间回收。强制终止后，下次启动仍读取持久化的到期 `active` 和 `deleting` 重试；`pending` 恢复、无记录旧资源接管、自动续期客户端和保留 workspace 的后续清理尚未实现。

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

启动时 lifespan 初始化 schema 2 数据库并保留同版本已有记录；不兼容旧库或初始化失败时启动失败，不迁移或降级存储。正常启动后，Uvicorn 会输出监听地址。使用以下命令确认没有监听全部网络接口：

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

HTTP 201 响应中的 `started` 表示容器已启动、生命周期记录已确认 `active`，同时返回相差 1800 秒的 `created_at`、`expires_at`。sandbox 无宿主端口，入口由专属 proxy 发布；通过 `GET /v1/sandboxes/{sandbox_id}/endpoint` 获取实际本机地址，503 时可在有限期限内重试。状态查询合并 Docker 事实与数据库当前过期时间；重启控制面并使用同一数据库后再次查询，时间不会因重启改变。

### 续期 sandbox

对尚未到期且未进入删除流程的受管实例续期：

```bash
curl -i -H 'Content-Type: application/json' \
  -d '{"ttl_seconds":1800}' \
  "http://127.0.0.1:18083/v1/sandboxes/$SANDBOX_ID/renew"
```

HTTP 200 返回实际期限；`ttl_seconds` 从本次校验时刻计算，不在原期限上累加。重复调用不会缩短期限，取消后应通过状态查询确认是否已提交。

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
uv run python -m compileall -q app.py api core runtime storage tests
```

测试覆盖：

- `/healthz` 与 Docker 状态相互独立。
- `/readyz` 成功、异常和超时响应。
- 不支持的 HTTP 方法返回 405。
- 稳定错误体不泄露宿主机信息。
- 新入口首次导入、应用装配和 Docker client 延迟创建。
- 默认数据库路径不随启动目录变化，构造无数据库副作用，lifespan 显式初始化与启动失败。
- `ping()` 成功、失败和 HTTP 已超时后的迟到失败均能关闭 client。
- 探测器不暴露容器、网络或 volume 管理操作。
- 创建 API 的空请求契约、成功响应和稳定错误映射。
- 固定时钟、单次取时、精确 1800 秒 TTL 和 UTC 序列化，五项资源的时间标签一致。
- 创建与查询时间一致、新 Runtime/Service 恢复原值、旧实例双 `null`、时间标签缺项/格式/顺序错误，以及超过计划过期时间后仍保留现有状态和入口语义。
- 真实临时 SQLite 与 FakeRuntime 的预写、重复 ID、更新后查询、身份匹配、无记录旧实例不接管、运行时不存在仍 404、跨 Store/Service 恢复，以及 SQLite 写入期间取消、重复取消、回滚不完整和补偿失败。
- 固定资源名称、标签、安全参数、调用顺序、精确逆序回滚、取消与重复取消清理。
- Docker/health 状态映射、查询超时、错误脱敏和控制面重启后查询。
- 删除默认保留与显式删卷、未知/无效 ID、重复/并发请求、残留发现、归属/事务/角色预检、停止失败、占用失败后重试，以及取消/重复取消后的线程和 client 收尾。
- 删除按预期事务核验全部残留角色、Docker 失败保留记录、成功后按原版本清理、默认留卷移除实例记录，以及元数据清理失败和冲突后的同 ID 重试。
- 创建确认、管理状态与版本变化、到期认领和过期时间更新竞争、单轮回收边界、批次上限、失败重试及取消收尾。数据库测试均新建临时文件，旧版本明确拒绝，不修改现有开发库。
- 后台首轮与固定间隔、串行执行、空批次和失败后的等待、日志摘要、关闭取消，以及重启后处理持久化候选；普通测试显式关闭周期回收。
- 续期输入和拒绝条件、期限持久化和单次时钟、与回收和手动删除的版本竞争，以及写入期间首次或重复取消后的提交确认。
- Docker 集成测试默认跳过，不会修改 Docker 资源。

真实 Docker 集成测试必须显式启用：

```bash
RUN_DOCKER_INTEGRATION=1 uv run --locked pytest -m docker_integration -v
```

测试使用临时 SQLite，验证固定创建配置、生命周期记录、真实五项资源的初始时间标签，以及两个动态实例的不同端口、正确路由、独立工作区、HTTP/文件/SSE、重启恢复和普通 bridge 公网出口对照。删除测试覆盖默认留卷和显式删卷。单轮及 lifespan 后台回收确认到期 A 被回收并留卷、未到期 B 可用。HTTP 续期测试使用可控时间和重启后的应用，确认实例在原期限仍可用、标签未改，新期限到达后由后台回收并留卷；不手动调用单轮方法。最终只按本次记录的精确资源 ID 和完整标签清理。运行前记录现有容器、网络和 volume 快照，并确认两个固定镜像均已存在；测试不会 pull 或 build 镜像。

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
- 当前支持创建、基于 Docker 事实的状态/入口查询、显式续期和手动删除，没有主动 Agent 就绪探测、暂停或恢复接口。
- 已支持周期及显式单轮过期回收，没有公开回收 API、自动续期客户端或自定义创建 TTL。`pending` 不自动回收，部分失败保留 `deleting` 供重试，保留 workspace 的磁盘占用和后续卷回收策略仍需单独管理。
- 周期回收仅支持单进程、单 worker，无多 worker 协调；到期与实际删除之间存在扫描间隔、处理耗时及重试延迟。
- Docker 标签继续保存初始生命周期信息，不随数据库更新而改变；旧实例或数据库丢失前的实例不自动接管。
- SQLite 与 Docker 不具备跨系统原子事务，进程崩溃不会自动恢复或回滚未完成创建。续期与删除认领由版本和状态保护，不承诺跨进程只执行一次，续期成功也不阻止后续显式删除。
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
