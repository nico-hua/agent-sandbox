# Control Plane 设计与运行

本文说明宿主侧 Control 的职责、生命周期一致性、失败恢复和运行边界。对外请求、响应、状态与错误码以 [HTTP API](API.md) 为准；Docker 资源拓扑和固定安全参数见 [Docker Runtime 设计](docker/DESIGN.md)，构建、运行和真实 Docker 验证见 [Docker Runtime 实现与验证](docker/IMPLEMENTATION.md)。开发记录与待办见 [项目进度](../PROJECT_PROGRESS.md)。

## 当前定位

`control_plane/` 是运行在 WSL 宿主侧的独立 Python/FastAPI 进程，与容器中的 Go Agent 分离。当前提供进程健康、只读 Docker 就绪探测、固定配置创建、状态和动态入口查询、显式续期、手动删除及到期自动回收。命令、文件和 SSE 由 [Sandbox Agent API](../sandbox_agent/API.md) 提供，Control 不转发这些调用，也不接受任意用户服务端口。

Control 不接管现有 Compose Agent、`agent-sandbox-internal` 网络或 `agent-sandbox-workspace` volume。当前运行前提是单进程、单 worker，SQLite 控制面通过系统文件锁限制同一数据库只能运行一个应用实例；没有认证、授权、审计、暂停、恢复、公开回收 API、自动续期客户端或自定义创建 TTL。它是本机开发控制面，不能据此宣称具有完整平台能力或已经验证的安全隔离能力。

## 进程与信任边界

```text
本机调用方
    |
    | HTTP 127.0.0.1:18083
    v
Python/FastAPI Control（WSL 宿主侧）
    |-- SQLite：control_plane/.local/lifecycle.sqlite3
    |
    | Docker SDK：只读探测、固定策略创建、查询、归属核验后的删除
    v
Docker daemon
    |
    | 专属 ingress bridge + 专属 internal bridge
    v
专属 proxy（动态发布 127.0.0.1 端口，固定上游 sandbox:8080）
    | internal bridge
    v
受管 Agent sandbox（仅 internal，无宿主端口）

现有 Compose Go Agent（独立开发实例，不属于 Control 管理）
```

Docker socket 只供宿主侧 Control 使用，不得挂载进 sandbox 或入口代理。Control 必须绑定 `127.0.0.1`，不得直接暴露到不可信网络。sandbox 固定采用非 root 用户、有限 CPU/内存/PID、只读根文件系统、有限 `/tmp`、drop ALL capabilities 和 no-new-privileges；workspace 仅挂载给 sandbox。proxy 不挂载 workspace、socket 或宿主目录，采用独立资源上限，仅发布本机入口。完整参数和网络边界集中在 [Docker Runtime 设计](docker/DESIGN.md)。

## 组件职责与依赖

控制面顶层 Python 源文件只有 `app.py`，使用 namespace package；项目配置、`uv.lock` 和测试保留在 `control_plane/`。源码统一使用 `control_plane.api`、`control_plane.core`、`control_plane.runtime` 和 `control_plane.storage` 导入，不修改 `sys.path`。

| 组件 | 职责与边界 |
| --- | --- |
| [`app.py`](../../control_plane/app.py) | 创建应用、装配一个 Runtime、Store 和共享的 `SandboxService`，注册路由，管理 lifespan、SQLite 运行锁与唯一后台回收任务；保留模块级 `app`。 |
| [`api/health.py`](../../control_plane/api/health.py) 与 [`api/sandboxes.py`](../../control_plane/api/sandboxes.py) | 处理 HTTP 输入、只读请求整体超时与成功响应；运行时请求统一经过 Service，不直接调用 Docker SDK。 |
| [`api/errors.py`](../../control_plane/api/errors.py) | 集中已知领域异常的固定状态码、错误码、公开文案和诊断 ID 策略；处理 endpoint 的不可用归并。支持已知异常的子类，未知异常继续交由上层诊断。 |
| [`core/models.py`](../../control_plane/core/models.py) 与 [`core/errors.py`](../../control_plane/core/errors.py) | 定义规格、生命周期记录、运行状态、结果和稳定领域异常；不依赖 HTTP 或 Docker SDK。 |
| [`core/service.py`](../../control_plane/core/service.py) | 生成身份与时间，协调 Runtime 和 Store 的创建、查询、续期、删除及 pending 只读核验；通过公共 `SandboxRuntime` 和最小 `LifecycleStore` 协议工作。 |
| [`core/tasks.py`](../../control_plane/core/tasks.py) | 提供不向 worker 转发取消的等待、穿过重复取消的收尾和最终结果读取；业务层决定补偿与错误优先级。 |
| [`core/reaper.py`](../../control_plane/core/reaper.py) | 串行调度 Service 的有界单轮回收，记录脱敏摘要并等待关闭收尾；不直接访问 SQLite 或 Docker。 |
| [`storage/lifecycle_store.py`](../../control_plane/storage/lifecycle_store.py) | 定义最小 Store 协议、稳定存储异常和标准库 `sqlite3` 实现；只管理元数据，不调用 Docker。该模块的约束见其 [`AGENTS.md`](../../control_plane/storage/AGENTS.md)。 |
| [`storage/run_lock.py`](../../control_plane/storage/run_lock.py) | 通过 `SQLiteRunLock` 管理同库系统独占文件锁；区分锁竞争、文件错误和平台不支持，不管理 SQLite 事务或 Docker 资源。 |
| [`runtime/sandbox_runtime.py`](../../control_plane/runtime/sandbox_runtime.py) | 用 `ABC` 定义异步 `ping()`、`create(spec)`、`get(id)`、`get_endpoint(id)`、`inspect_resources(id, *, expected_transaction_id)`、`delete(id, *, delete_workspace=False, expected_transaction_id=None)`；不包含 SDK、线程或具体回滚实现。 |
| [`runtime/docker/docker_runtime.py`](../../control_plane/runtime/docker/docker_runtime.py) | 实现异步 Runtime、线程适配、只读状态/入口检查与取消等待。 |
| [`runtime/docker/transaction.py`](../../control_plane/runtime/docker/transaction.py) | 实现同步 Docker 创建事务、精确资源记录、归属工具、错误分类、逆序回滚和正式删除。 |

`create_app()` 可以注入 `runtime`、`store` 或测试 `database_path`；默认装配 `DockerSandboxRuntime`、`SQLiteLifecycleStore` 和 `SandboxService(runtime, store)`。默认数据库路径根据 `app.py` 位置确定，不随启动目录变化。导入模块、构造 Store 和调用 `create_app()` 不创建数据库、目录、连接、锁文件、Docker client 或后台任务；`runtime/__init__.py` 不自动加载具体后端。

FastAPI lifespan 先取得 SQLite 运行锁，再显式调用 `Store.initialize()`，成功后才启动回收任务并提供服务。取锁或初始化失败均阻止应用启动，不切换到内存或临时数据库。Docker 暂时不可用不阻止启动：`/healthz` 不访问 Docker，`/readyz` 只通过 `Service.ping()` 执行一次只读 `ping()`。

HTTP 就绪探测的整体超时默认 1 秒，状态与 endpoint 查询共享独立的 `sandbox_query_timeout=1.0` 参数；创建、续期和删除不套用只读查询超时。Docker client 每次操作延迟创建并负责关闭；只读 ping 的 transport timeout 为 0.5 秒，生命周期创建、查询和删除为 5 秒。两个 client factory 分别注入，避免混用超时。请求等待取消不表示同步 SDK 或 SQLite 操作已经停止。

### SQLite 单实例运行保护

运行锁身份取自实际注入的 `SQLiteLifecycleStore` 所使用的数据库规范路径，而不是仅依据 `create_app(database_path=...)`。伴随锁文件位于数据库旁，例如 `lifecycle.sqlite3.lock`。同一数据库的不同应用对象及不同进程互斥，不同数据库可分别启动。SQLite 没有关闭该保护的开关；纯内存 Fake Store 不使用数据库运行锁。

Linux/WSL 使用标准库 `fcntl` 系统锁，Windows 使用 `msvcrt` 系统锁，按平台加载；不支持的平台明确拒绝启动。获取为非阻塞尝试：锁已被持有时立即返回稳定启动错误，不初始化数据库、调用 Docker 或启动回收任务。权限、路径和文件系统错误与锁竞争分别报告，错误与日志不包含完整路径、凭据或底层异常文本。

锁由独占文件句柄持有，句柄不传给无关子进程。锁文件存在、PID 或文件内容都不能证明锁仍被持有；锁文件可以在无持有者时保留。正常关闭在服务请求收尾和回收任务结束后显式解锁、关闭句柄，不删除锁文件，也不提供强制删除锁文件或终止占用进程的解锁方式。进程异常退出后由操作系统释放锁，后续实例可重新获取。

初始化失败和启动取消也必须释放锁，但已经开始的初始化或回收关闭工作须先通过共用关键任务等待工具完成，首次和重复取消均不能提前放锁。不持有长期 SQLite 写事务来实现保护，因此运行锁不阻塞 Store 正常的独立连接与短事务。

保护限于本机、本地文件系统及统一数据库路径，要求控制面遵守同一锁协议；不承诺网络文件系统、硬链接别名或 Windows/WSL 跨系统共享锁语义，也不能阻止外部程序直接操作数据库或 Docker。它不是分布式租约、多 worker 协调或 exactly-once 保证。取得锁不证明旧 Docker 请求已经结束，也不触发 pending 自动恢复、激活或删除。

## 身份、时间与持久化

### 资源身份与初始标签

每次创建生成不可预测的 `sbx_<32 位小写十六进制>` ID 和独立的 `txn_<32 位小写十六进制>` 事务 ID。事务 ID 用于区分同一 sandbox ID 下不同创建尝试；名称不能替代身份核验。五项资源都有完整归属标签：

```text
io.agent-sandbox.managed=true
io.agent-sandbox.project=agent-sandbox
io.agent-sandbox.sandbox-id=<sandbox_id>
io.agent-sandbox.transaction-id=<transaction_id>
io.agent-sandbox.resource=container|network|workspace|proxy|ingress
```

Service 支持注入时钟，每次创建在调用 Runtime 前只读取一次带时区时间并归一化为 UTC。`created_at` 为该时间，`expires_at = created_at + 1800 秒`。Runtime 接收已计算值，不自行取时，将同一对初始时间标签写入五项资源：

```text
io.agent-sandbox.created-at=<RFC 3339 UTC created_at>
io.agent-sandbox.expires-at=<RFC 3339 UTC expires_at>
```

对外时间使用 RFC 3339 UTC、以 `Z` 结尾并保留微秒，例如 `2026-10-19T08:00:00.123456Z`。Docker 标签保存初始生命周期信息，不替代完整归属标签；SQLite 保存可更新的期限。续期不修改标签，也不调用 `container.update`。

### SQLite schema 与状态

默认存储为 `control_plane/.local/lifecycle.sqlite3`。当前 schema 版本为 **2**，仅初始化新库或核验同版本已有库；核验包含表、约束、索引及不存在额外 trigger，不支持版本 1 迁移或自动修复。数据库、journal、WAL、共享内存和伴随运行锁文件不得提交版本库。验证使用全新临时数据库，不删除或重建现有开发库。

记录包含不可变的 `sandbox_id`、`transaction_id`、`created_at`，以及可更新的 `expires_at`、`management_state` 和从 0 开始的 `version`。时间存储和比较使用整数 UTC 微秒，避免浮点时间戳或字符串排序损失精度。更新期限、状态迁移和删除均核对事务 ID 与预期版本；成功更新或状态迁移递增版本，已经处于 `deleting` 的精确版本重试保持原版本。旧版本不能覆盖新值，也不能通过刷新版本接管替代记录。

```text
pending（version 0） → active → deleting → 移除记录
```

`pending` 表示创建尚未确认或结果不明确；`active` 表示 Runtime 创建与元数据确认均完成；`deleting` 表示已经进入删除流程。手动删除可处理三种状态，`deleting` 禁止更新期限。这些是内部管理状态，不公开为 HTTP `status`，也不表示 Agent 健康。创建响应的 `started` 表示启动调用已完成，查询的 `ready` 仅表示 Docker health 为 `healthy`；入口可用性另由 endpoint 查询核验。

每次同步存储操作在线程内创建、提交或回滚并关闭自己的连接，不跨线程共享连接；使用参数化 SQL 和有限锁等待，默认 0.5 秒。Store 的 `get()` 缺失返回 `None`，更新缺失与版本冲突各有稳定异常，精确身份的幂等删除接受记录已不存在；这些语义不合并。SQLite 事务中不等待 Docker。

## 生命周期流程

### 创建与确认

1. HTTP 层要求请求体完全为空；Service 生成身份与固定 1800 秒 TTL。
2. Service 先插入 `pending`、版本 0 的记录。插入失败或 ID 冲突不调用 Runtime，也不创建 Docker 资源。
3. Runtime 检查本地 `agent-sandbox:dev` 和 `agent-sandbox-proxy:dev` 镜像，不 pull/build；依次创建 internal bridge、ingress bridge、workspace、sandbox 并启动、proxy 并连接 internal 后启动。固定创建配置不接受 HTTP 覆盖。
4. Runtime 成功后，Service 按原事务 ID 和版本原子激活记录。只有激活也成功才返回 HTTP 201 与 `started`；这不等待 Agent 或 proxy 健康检查。
5. Runtime 明确证明未修改资源或已完成失败回滚时，Service 按原事务 ID 和版本补偿预写记录。有残留或结果未知时保留记录和 sandbox ID，不报告成功。

SQLite 与 Docker 没有跨系统原子事务。SQL 回滚不能撤销 Docker 操作，数据库有记录不证明实例已经创建成功或仍存在。激活失败可能发生在 durable commit 之后，版本结果未知时保留记录与资源用于诊断。

### Pending 创建记录只读核验

内部 `await service.inspect_pending_once(limit=100)` 显式核验一批 `pending` 记录，没有 HTTP 接口，也不接入应用启动或周期回收。Store 直接使用有界 SQL，按 `created_at`、`sandbox_id` 稳定排序；`limit` 必须为 1～2⁶³−1 的真正整数。方法不初始化或修复数据库，不自动恢复、激活或删除记录与资源。

Service 保存候选身份和版本，调用 `Runtime.inspect_resources(sandbox_id, expected_transaction_id=...)`，随后重新读取记录。记录消失、身份或版本变化，或已经离开 `pending`，均返回 `changed`，避免将旧观察当作当前记录结论。整体存储失败通过结果的 `error` 明确报告，不伪装成空列表；单项资源冲突或后端不可用不阻止本批其他项。

每项包含 `sandbox_id`、`classification`、稳定 `reason`，以及能够可靠确认的 `found_resources` 和 `missing_resources`。分类如下：

| 分类 | 含义 |
| --- | --- |
| `all_present` | 五类资源均存在且通过完整归属核验。 |
| `partial` | 仅发现部分匹配资源；主容器缺失仍继续核验其他角色。 |
| `not_found` | 没有发现匹配的受管资源。 |
| `conflict` | 角色重复、事务不符或其他归属冲突，不能安全归并。 |
| `unavailable` | 后端不可用，无法确认资源情况。 |
| `changed` | 核验期间生命周期记录已经变化或消失。 |

Runtime 只读检查 `container`、`proxy`、`network`、`ingress`、`workspace`，复用资源发现和完整归属校验；不通过仅筛选预期事务隐藏同一实例的其他事务。检查不改变资源、标签、期限、版本、管理状态或正常进行中的创建任务。

结果仅覆盖本批，且不是 SQLite 与 Docker 的跨系统原子快照。`all_present` 不证明创建成功、拓扑正确或 Agent 就绪；`not_found` 不排除标签缺失或无法识别的遗留资源。`pending` 状态和记录年龄也不能证明原创建任务已经停止。自动恢复、清理、调度及创建任务存活判定尚未实现。

### 状态与入口查询

Service 在查询、endpoint、续期和删除访问后端前校验 ID。状态查询先由 Runtime 只读加载唯一候选容器，重读精确 ID、核对完整归属、恢复初始时间与内部事务 ID，再映射 Docker 容器和 health 状态。实际容器不存在仍返回 404，即使数据库有记录。

Service 随后读取 Store，核对 sandbox ID、事务 ID 和创建时间；身份匹配才使用数据库当前 `expires_at`，运行状态、reason 和 message 仍来自 Runtime。数据库不可用或身份冲突返回稳定错误，不静默回退到旧标签。重启后从同一数据库恢复期限，不重新取时，也不自动延长。

没有记录时保留 Docker 初始时间，不补写或接管 TTL。旧实例同时缺少两个时间标签时返回双 `null`；只缺一个、格式错误或过期早于创建均为查询失败，不猜测时间。无记录旧实例仍可查询状态、endpoint 和删除，但不能续期，也不纳入数据库到期回收。

状态查询不修改资源、不触发删除或修复；超过计划期限不新增 `expired` 状态，到实际回收前仍保持原状态、endpoint 和命令语义。Control 不直接访问容器 IP；Agent 镜像在容器内部请求 `/healthz` 执行 HEALTHCHECK，只有 Docker health 明确为 `healthy` 才返回 `ready`。没有 health check 的旧运行容器仍为 `running`；健康失败不自动删除、重启或清理 workspace，重建镜像也不会更新旧容器。

endpoint 查询通过 Service 直接委托 Runtime，不依赖进程内创建记录或 SQLite。Runtime 核验 sandbox/proxy 的完整归属与相同事务、专属两网角色/internal 属性、实际连接、sandbox DNS 别名及 proxy running/healthy；只从实际 `NetworkSettings.Ports` 接受唯一 `127.0.0.1` 有效端口，不以 `HostConfig.PortBindings` 推断发布成功。无 proxy 的旧实例返回不可用，不自动迁移；查询成功后的可用性仍可能随资源退出变化。具体 404、503 与 504 契约见 [入口 API](API.md#6-查询本机入口)。

### 续期与并发认领

续期只接受 1～86400 的真正整数 TTL。Service 使用一次 UTC 时钟快照，先只读核验 Runtime 实例，再核对 Store 的 sandbox ID、事务 ID 和创建时间；仅存在记录、`active` 且 `current_expires_at > now` 时允许续期，恰好到期也拒绝。目标为：

```text
new_expires_at = max(current_expires_at, now + ttl_seconds)
```

TTL 从本次校验时刻计算，不在原期限上累加，也不缩短。即使目标期限与当前值相同，仍执行有事务 ID 和预期版本保护的提交，使先完成的删除认领生效；不能因为“无需延长”而绕过竞争检查。续期先提交使旧回收候选认领失效，删除或回收先进入 `deleting` 则不能续活；并发版本冲突返回冲突，不刷新版本强行覆盖。续期成功不阻止随后显式手动删除。

续期不新增 Runtime 方法、不重建资源、不修改 Docker 标签、入口或运行状态。SQLite 写入取消须等待最终结果并读取异常；已提交期限不反向补偿，以免覆盖后续更新。调用方没有收到响应时可能已经提交，应通过状态查询确认实际期限。完整协议见 [续期 API](API.md#7-续期-sandbox)。

### 删除与元数据收尾

Service 先读 Store；有记录时按原事务和版本将 `pending` 或 `active` 认领为 `deleting`，已有 `deleting` 允许精确版本重试。读取或认领失败不开始 Docker 删除。随后将预期事务 ID 传给 Runtime；无记录旧实例仍按 Docker 完整归属删除，不要求容器健康、endpoint 可用或 `Runtime.get()` 成功。

Runtime 从标签恢复五类存留资源，即使只剩网络或 workspace 也可处理。破坏性操作前核验完整标签、有效且跨角色一致的事务、角色唯一性以及预期事务；默认保留的 workspace 也要核验。每次 stop/remove 前重读精确容器/网络 ID 或经核验的卷 Name，归属冲突拒绝删除，不按名称、前缀或全局扫描接管资源。

正式删除依次处理 proxy、sandbox、ingress、internal，以及显式请求时的 workspace。容器使用 2 秒停止宽限，失败后重新核验再强制移除本实例容器；网络和卷不强制删除，不强制断开其他容器。已经消失的资源视为完成，每次删除后确认不存在；部分失败不会重建已删除项，对同一 ID 重试继续收敛。Docker 细节见 [删除边界](docker/DESIGN.md) 和 [实现与验证](docker/IMPLEMENTATION.md)。

Runtime 达到删除目标后，Service 才按认领事务和版本删除元数据。默认保留 workspace 时同样移除已结束实例记录；残卷继续按完整 Docker 标签和显式 `delete_workspace=true` 管理。Docker 失败保留 `deleting`；资源已删除但元数据收尾失败或版本冲突时保留 sandbox ID，允许同 ID 重试，不盲目刷新版本。合法 ID 无目标、重复删除或默认删除只剩保留卷均可达到 204 空体目标。

默认留卷会持续占用磁盘；单轮与后台过期回收也保留 workspace，尚无自动卷清理。创建失败的逆序回滚会清理本次 workspace，不采用正式删除的留卷策略。

## 异常、取消与恢复证据

### 异常转换边界

Runtime/Store 将依赖异常转换为稳定领域或存储异常并保留异常链，事务边界兜底清理；Service 协调元数据与资源结果；API 通过 [`api/errors.py`](../../control_plane/api/errors.py) 集中固定公开响应。endpoint 将不可核验的入口和后端不可用归并为稳定 503，操作超时另行处理。映射不读取异常消息生成公开文案，未知异常不伪造为已知业务错误。完整错误表、固定文案和响应示例保留在 [HTTP API](API.md)。

创建异常可附带不可变的 `CreationFailureInfo(phase, resource_state, cleanup_issues)`；`CleanupIssue` 只保存固定资源角色、阶段和异常类型类别，不保存原始 SQL、路径、凭据、后端响应或资源身份。Service 根据明确证据决定是否移除元数据：

| `resource_state` | 语义 | Service 行为 |
| --- | --- | --- |
| `unchanged` | 已确认没有资源修改。 | 可补偿原事务与版本的元数据。 |
| `rolled_back` | 已确认本次资源回滚完成。 | 可补偿原事务与版本的元数据。 |
| `remaining` | 确认仍有资源残留或归属变化使其不能删除。 | 保留记录和诊断 ID。 |
| `unknown` | 无法确认创建或清理的最终结果。 | 保留记录和诊断 ID。 |

Service 不维护“异常类型等于已回滚”的白名单。为兼容旧 Runtime，`SandboxCreateFailed`、`SandboxResourceConflict` 默认携带 `rolled_back`，`SandboxImageUnavailable` 默认携带 `unchanged`；实现若不具备这些保证，必须附上实际证据或改用保留恢复记录的异常。`Runtime.create()` 仅在精确回滚已确认时传播 `CancelledError`；不完整或未知清理必须抛出带 sandbox ID 的错误。

### 精确回滚与主错误

Docker 低层 create 响应返回后，先记录资源精确 ID，再进行可能失败的 inspect/start；volume create 可能复用同名卷，响应标签与本事务不匹配时不能登记、挂载或删除该卷。创建失败按本次登记清单逆序处理，单项失败后仍继续清理其他项。精确 ID 的 `NotFound` 表示已经消失，删除响应成功或失败后均复查是否仍存在；归属变化绝不删除，检查失败记为未知，确认残留记为有残留。

已登记资源清理成功不能证明丢失创建响应的其他资源不存在；通信不确定性仍保守标记为未知。创建完成或提交后回滚结束时独立 close 失败也保留未知结果与诊断；已有创建主错误时 close 问题附入恢复证据，不覆盖主错误及其资源状态。

状态与 endpoint 查询的主错误优先于 client close 错误。读取已经失败时仅记录固定操作与异常类型；读取成功后独立关闭失败才转换为该查询的稳定领域错误或超时。错误响应和默认日志不输出原始 Docker 异常、socket/数据库路径、SQL、环境变量或调用栈。

### 取消与迟到提交

[`core/tasks.py`](../../control_plane/core/tasks.py) 使用 `asyncio.wait()` 等待任务，避免将等待者取消直接转发给 worker，也避免 Python 3.14 取消 shield 后对迟到异常的额外报告。`wait_for_task()` 保留可收尾的 worker，`wait_for_completion()` 穿过重复取消并返回首次取消信号，`settle_task()` 读取最终结果后按调用方策略传播取消。Service、Runtime 与 reaper 共用等待动作，但保留各自业务补偿和错误优先级。

- 预写期间取消：等待 SQLite 最终结果；已提交但 Runtime 尚未启动时，按原事务和版本删除元数据。冲突或补偿失败不能删除替代记录。
- Runtime 创建期间取消：向创建协程发出取消，由 Runtime 在线程步骤间设置取消标志，等待精确回滚；创建赢得竞争而迟到成功时，显式补偿本事务资源。只有确认清理完成才清理元数据并传播取消。
- 激活期间取消：等待激活最终结果，已知提交版本才执行补偿；结果未知则保留资源和记录。已确认创建的补偿先持久化 `deleting`，失败留待重试。手动删除已移除记录或认领较新版本时，迟到创建仍清理原事务资源，不重建记录、不刷新版本，也不删除替代元数据。
- 删除期间取消：Runtime 删除与其后元数据收尾构成关键操作，首次及重复取消都等待最终结果并读取后台异常。调用方未收到响应时按同一 ID 重试，以资源事实确认结果。
- 续期期间取消：等待提交结果，已提交期限不撤销。回收取消则停止后续候选，先完成当前候选从认领到清理的关键收尾。

## 后台周期回收

内部 `await service.reap_expired_once(limit=100)` 执行一轮回收，没有对应 HTTP API。每轮只读取一次 UTC 时钟，有界获取到期 `active` 与待重试 `deleting`，按过期时间、sandbox ID 稳定排序；`pending` 不作为候选。

对 `active` 候选，Store 在同一 SQLite 事务中核对 sandbox ID、事务 ID、预期版本、状态和 `expires_at <= now`，成功后转为 `deleting` 并递增版本。续期、删除或候选变化使认领条件失效时跳过，不调用 Runtime；`deleting` 重试同样核验原身份与版本。随后复用受保护删除，默认 `delete_workspace=False`，成功后才清理元数据。

单项 Runtime 或收尾失败保留 `deleting`，不妨碍本批其他候选，每项本轮最多尝试一次；数据库整体不可用停止本轮，不降级为盲目删除。取消停止本批后续候选，等待已开始操作收尾。结果包含 `now`、`items`、`error` 及计算得到的 `succeeded/skipped/failed` 计数；每项含 `sandbox_id`、`outcome` 和稳定 `reason`。成功为 `deleted`，候选失效为 `candidate_changed`，其他失败使用稳定领域错误码；整体存储失败为 `sandbox_metadata_unavailable`。

后台调用链为 `FastAPI lifespan → core/reaper.py → SandboxService.reap_expired_once()`。循环只调度，不重复判断过期条件。`create_app()` 参数如下：

| 参数 | 默认值 | 约束与语义 |
| --- | --- | --- |
| `reaper_enabled` | `True` | 必须为布尔值；控制是否在 lifespan 启动任务。 |
| `reaper_interval_seconds` | `60.0` | 有限正数，拒绝布尔值；每轮完成后等待的秒数。 |
| `reaper_batch_limit` | `100` | 1～2⁶³−1 的真正整数；每轮候选上限。 |

配置在装配时校验。数据库初始化成功后启动唯一后台任务，第一轮在后台立即执行，不等整轮结束才提供 HTTP 服务。同一应用实例的各轮串行；无候选、满批次和普通失败后均等待完整间隔，避免忙循环。普通测试显式关闭回收，专门验证该行为时才开启。

循环检查单轮 `error`、`items` 及普通异常，记录处理计数和稳定失败原因，失败不会永久停止后续轮次。应用关闭时取消并等待任务，不再启动下一轮；重复取消不能跳过当前关键收尾，也不使用短超时假定工作线程已经退出。

当前仅支持单进程、单 worker，同库运行锁不提供分布式互斥、租约或 exactly-once 保证。扫描间隔、单轮耗时和失败重试可能延迟删除，不承诺到期瞬间回收。强制终止后，下次启动继续读取持久化的到期 `active` 和 `deleting`；未完成 `pending` 创建不会自动恢复或回滚，无记录旧资源不会自动接管，保留 workspace 不自动清理。

## 环境、依赖与启动

本地运行目标为 WSL Ubuntu；`pyproject.toml` 要求 Python **>=3.12**，使用 `uv` 和锁文件管理依赖。运行依赖为 FastAPI、Uvicorn、Python Docker SDK，测试依赖为 pytest、httpx 和 pytest-timeout。默认 Fake/临时 SQLite 测试也可在受支持的 Windows Python 环境验证，但 Docker、网络隔离和真实资源行为必须按实际 Linux/WSL 环境验收。

检查环境并安装依赖：

```bash
python3 --version
uv --version
docker version
cd ~/agent-sandbox/control_plane
uv sync --locked
```

`/readyz` 成功要求当前 WSL 用户可访问 Docker daemon；创建还要求两个固定镜像已存在。镜像构建与现有 Compose 开发实例的关系、代理镜像和验证准备见 [Docker 实现与验证](docker/IMPLEMENTATION.md)，镜像构建上下文仅为 `sandbox_agent/`，不自动重建既有开发资源。

前台启动且仅绑定本机：

```bash
cd ~/agent-sandbox/control_plane
uv run uvicorn control_plane.app:app --app-dir .. --host 127.0.0.1 --port 18083
```

启动时先取得同库独占运行锁，再显式初始化并核验 schema 2，同版本记录保留；锁被占用、不兼容或初始化失败时进程启动失败。更换 HTTP 端口不能绕过同库互斥。确认监听地址：

```bash
ss -ltnp 'sport = :18083'
```

本地地址应为 `127.0.0.1:18083`。前台按 `Ctrl+C` 停止，Uvicorn 处理 SIGINT/SIGTERM 并等待 lifespan 收尾。命令不启用 `--reload`，修改后需停止并重启。端口冲突时先确认占用者，可选择另一本机端口验证，不自动终止其他项目。

## 验证边界

在 `control_plane/` 执行默认验证：

```bash
uv sync --locked
uv run --locked pytest
uv run python -m compileall -q app.py api core runtime storage tests
```

在仓库根执行：

```bash
git diff --check
```

默认测试使用 Fake Runtime、Fake Docker client 和临时文件 SQLite，不修改真实 Docker 资源。关键验收包括：

- 健康/就绪独立、只读超时、405、固定错误与派生异常映射、endpoint 特例和脱敏；导入/装配无 I/O，client 延迟创建和成功/失败关闭。
- 单次 UTC 时钟、1800 秒初始 TTL、微秒精度、五项资源标签一致、状态/health 映射、入口拓扑与实际发布端口；旧实例双 `null`、错误时间标签、不隐式接管。
- 临时文件数据库的预写、激活、状态/版本、跨 Store/Service 恢复、查询身份合并、缺失和冲突区别；拒绝旧 schema，不修改开发库。
- 固定创建配置、精确逆序回滚、NotFound、归属变化、丢失删除响应、删除成功但资源残留，以及未登记创建结果不能被成功清理掩盖。
- 创建、激活、续期、删除与回收的首次/重复取消、迟到提交、原事务/版本补偿、后台异常读取；查询主错误叠加 close 失败、独立 close 失败和结构化恢复证据。
- 默认留卷/显式删卷、残留发现与事务/角色预检、占用失败重试、元数据收尾；续期与手动删除/回收的版本竞争，包含目标期限不变时的保护提交。
- 单轮边界、批次上限、稳定排序、失败重试、候选失效、取消收尾；后台首轮、固定间隔、串行执行、空/满批与失败后等待、日志摘要及重启后的持久化候选。
- pending 有界只读核验、五类资源缺失与冲突、后端不可用、记录变化及存储失败；核验前后资源和元数据保持不变。
- 同库应用对象和独立子进程互斥、不同数据库独立、正常关闭及进程异常退出后重新获取；初始化失败与取消、回收关闭收尾期间保持锁，以及无锁持有者的既有锁文件和脱敏文件错误。

运行锁测试仅使用临时目录、临时 SQLite 和本次创建的子进程，有界等待并清理测试进程，无需真实 Docker。Windows 文件锁须在原生 Windows Python 环境单独验证，WSL 结果不代表 Windows 验收。

真实 Docker 集成默认跳过，须在准备好的环境显式启用：

```bash
RUN_DOCKER_INTEGRATION=1 uv run --locked pytest -m docker_integration -v
```

集成测试使用临时数据库，覆盖固定资源配置与初始标签、双实例不同端口/正确路由/独立工作区、HTTP/文件/SSE、重启恢复、网络出口对照、留卷/删卷、单轮和 lifespan 到期回收、续期后在新期限回收，以及完整和部分资源的 pending 只读核验。运行前记录既有容器、网络和卷快照，确认固定镜像已存在；测试不 pull/build，只清理本次记录且精确 ID、完整标签核验通过的资源。Linux 网络限制不能以 Windows Fake 测试结果代替。操作步骤见 [Docker 实现与验证](docker/IMPLEMENTATION.md)；报告验证结果时注明平台和未执行的检查。

## 常见问题与恢复边界

### 启动提示同库实例正在运行

同一数据库只能由一个遵守锁协议的控制面实例运行。正常停止已知实例并等待服务与回收收尾后重试，或为独立实例配置另一数据库；更换端口不能解除冲突。不要删除伴随锁文件或自动终止其他进程。锁文件存在但没有持有者不会阻止启动；权限和文件系统错误应按对应的稳定启动错误排查。

### 进程健康但 Docker 不可用

`/healthz` 正常而 `/readyz` 为 503 表示进程能处理请求，Docker 连接当前不可用。用 `docker version`、`docker info` 检查 daemon 与当前用户访问权限，不通过放宽 sandbox 权限或挂载 socket 修复。可在单独启动的测试进程设置不存在的 `DOCKER_HOST`，验证健康仍为 200、就绪为脱敏 503；不要停止 Docker 或改动既有资源。

### 元数据初始化失败或查询冲突

核对配置的数据库路径、可写性、schema 兼容性和所用数据库是否与该批实例一致。不兼容旧库不自动迁移，也不通过删除或重建现有开发库绕过错误。查询身份冲突不应通过覆盖记录或回退标签隐藏；按响应中的 sandbox ID 核验实例、事务和创建时间。

### 创建、取消或删除没有收到最终响应

取消、连接断开和超时不能证明写入或资源操作没有发生。创建可能保留 `pending` 或未知结果，须按 sandbox ID 核验；无自动 `pending` 恢复。删除对同一 ID 重试，并遵守精确身份和完整归属，不使用名称前缀批量删除或 `docker prune`。续期则查询实际 `expires_at`，不根据响应缺失反向撤销。

### Agent ready 但入口不可用，或到期后仍可查询

`ready` 只描述容器内部 Agent 健康；endpoint 独立核验 proxy、两网和实际端口。入口暂不可用可在有限期限内重试并按 Docker 实现文档排查，不自动迁移旧实例。计划期限与实际删除之间存在扫描、处理和重试延迟；检查回收稳定日志、持久化管理状态及是否以单 worker 运行。默认删除和到期回收留卷，应记录 sandbox ID 并单独决定数据保留时长。
