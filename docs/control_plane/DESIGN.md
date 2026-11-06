# Control Plane 设计与运行

本文说明宿主侧 Control 的职责、生命周期一致性、失败恢复和运行边界。对外请求、响应、状态与错误码以 [HTTP API](API.md) 为准；Docker 资源拓扑和固定安全参数见 [Docker Runtime 设计](docker/DESIGN.md)，构建、运行和真实 Docker 验证见 [Docker Runtime 实现与验证](docker/IMPLEMENTATION.md)。开发记录与待办见 [项目进度](../PROJECT_PROGRESS.md)。

## 当前定位

`control_plane/` 是运行在 WSL 宿主侧的独立 Python/FastAPI 进程，与容器中的 Go Agent 分离。当前提供进程健康、只读 Docker 就绪探测、受约束规格及可选精确域名出口创建、状态和动态入口查询、受控 HTTP 服务入口交付、实例主动就绪检查、显式续期、手动删除及到期自动回收。命令、文件和 SSE 由 [Sandbox Agent API](../sandbox_agent/API.md) 提供；Control 只核验并交付入口，不转发这些调用或应用 HTTP 数据。

Control 不接管现有 Compose Agent、`agent-sandbox-internal` 网络或 `agent-sandbox-workspace` volume。当前运行前提是单进程、单 worker，SQLite 控制面通过系统文件锁限制同一数据库只能运行一个应用实例；默认要求单管理员 Bearer key，没有多租户授权、审计、公开回收或 pending 恢复 API、自动续期客户端、整机资源预算或容量准入保证。完整 pending 实例可受控确认，默认关闭启动恢复。它是本机开发控制面，不能据此宣称具有完整平台能力或已经验证的安全隔离能力。

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
专属 proxy（动态发布 127.0.0.1 端口，Agent 上游 sandbox:8080；服务上游 sandbox:<受约束端口>）
    | internal bridge
    v
受管 Agent sandbox（仅 internal，无宿主端口）

现有 Compose Go Agent（独立开发实例，不属于 Control 管理）
```

Docker socket 只供宿主侧 Control 使用，不得挂载进 sandbox 或入口代理。Control 必须绑定 `127.0.0.1`，不得直接暴露到不可信网络。sandbox 固定采用非 root 用户、有限 CPU/内存/PID、只读根文件系统、有限 `/tmp`、drop ALL capabilities 和 no-new-privileges；workspace 仅挂载给 sandbox。proxy 不挂载 workspace、socket 或宿主目录，采用独立资源上限，仅发布本机入口。完整参数和网络边界集中在 [Docker Runtime 设计](docker/DESIGN.md)。

## 控制面认证与启动配置

默认应用在装配时捕获 `AGENT_SANDBOX_API_KEY`；显式 `create_app(api_key=key)` 优先于环境变量。key 严格匹配 1～128 个 ASCII `[A-Za-z0-9_-]` 字符，不裁剪或转换类型，不接受显式 `None`。建议通过 `secrets.token_urlsafe(32)` 生成，启动命令使用 shell substitution 导出，避免输出 key；调用终端必须使用相同值。

未配置环境变量时，模块导入与 `create_app()` 保持无 I/O 并可成功装配，除精确 `GET /healthz` 外请求均拒绝；正常 lifespan 在取得运行锁、初始化 SQLite、pending 恢复或启动回收前以固定错误拒绝启动。非法显式配置或非法环境值在装配时抛出固定 `ValueError`，错误与配置 repr 不包含输入 key。配置在装配后不再随进程环境变化，不持久化到 SQLite、Docker 标签、容器环境、Service、Store 或 Runtime。

纯 ASGI 认证 middleware 只接受单个严格 Bearer header，用常量时间比较核验凭据；未授权请求在读取 body 或业务处理之前返回固定 401。只有精确 `GET /healthz` 匿名；公开协议见 [API 认证](API.md#认证)。默认与开发模式均关闭 `/docs`、`/redoc` 和 `/openapi.json`。

测试或受信本机开发可在装配时**显式**使用 `create_app(allow_unauthenticated=True)`；没有环境变量可隐式开启该模式。参数只接受真正的 bool，同时提供有效 key（包括从环境捕获）与 `True` 会因配置冲突拒绝，须移除 key 后明确选择开发模式。该模式不是生产部署方式；示例与真实 SDK 验收均使用正常认证。

默认测试隔离开发终端的 `AGENT_SANDBOX_API_KEY`，每个应用工厂明确选择测试 key 或开发模式；认证测试自行配置真实认证路径，不替换 middleware 或放宽生产校验。

管理员 key 只用于控制面；新动态入口另用每实例随机 Token。SDK 显式区分请求角色，即使同源也不向 Agent 发送管理员 key。主动健康探测使用匿名精确 GET /healthz。当前仍限可信本机，没有多租户授权、轮换、TLS 或审计，也不保护容器内部 Agent 访问。

## 可选出口策略

创建规格的 `SandboxNetwork` 保存规范化、排序且不可变的精确域名 tuple；默认空白名单保持原五资源链路，非空白名单增加一个受管 `egress` 容器，复用本实例两网。HTTP 严格校验、core 直接调用校验及 Go Policy 规范化保持一致，不引入任意镜像、代理地址或网络参数。Service 将同一网络规格交给 Runtime，不执行代理协议。

网络策略通过本次所有 Docker 资源上的版本化不可变标签持久化，不修改 SQLite schema。查询、pending 核验和启动恢复通过资源事实判断策略及预期角色；缺失或不一致策略不降级放行。旧实例两项策略标签均不存在时按禁用出口的五资源兼容处理，不修改既有容器。状态 ready 对启用实例还要求 egress 运行、Docker health 健康且双网连接正确；这些检查不发起公网请求。入口查询保持入口语义，实例 readyz 先通过状态检查再访问 Agent。

出口容器与其资源记录纳入原创建事务、重复取消等待、逆序回滚、正式删除和到期回收。删除授权仍依据精确 ID 和完整归属；策略元数据损坏不能阻止回收其他合法存留资源，也不能授权操作不属于本事务的对象。详细安全配置和兼容规则见 [Docker 网络策略](docker/DESIGN.md#network-policy)。

## 组件职责与依赖

控制面顶层 Python 源文件只有 `app.py`，使用 namespace package；项目配置、`uv.lock` 和测试保留在 `control_plane/`。源码统一使用 `control_plane.api`、`control_plane.core`、`control_plane.runtime` 和 `control_plane.storage` 导入，不修改 `sys.path`。

| 组件                                                                                                                 | 职责与边界                                                                                                                                                                                                                                           |
| -------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`app.py`](../../control_plane/app.py)                                                                              | 创建应用、装配一个 Runtime、Store 和共享的`SandboxService`，注册路由，管理 lifespan、SQLite 运行锁与唯一后台回收任务；保留模块级 `app`。                                                                                                         |
| [`api/auth.py`](../../control_plane/api/auth.py) | 装配不可变且脱敏的认证设置，以纯 ASGI middleware 在路由与请求体读取前校验管理员 Bearer key；不向 core 或 runtime 传递凭据。 |
| [`api/health.py`](../../control_plane/api/health.py) 与 [`api/sandboxes.py`](../../control_plane/api/sandboxes.py) | 处理 HTTP 输入、只读请求整体超时与成功响应；运行时请求统一经过 Service，不直接调用 Docker SDK。                                                                                                                                                      |
| [`api/errors.py`](../../control_plane/api/errors.py)                                                                | 集中已知领域异常的固定状态码、错误码、公开文案和诊断 ID 策略；处理 endpoint 的不可用归并。支持已知异常的子类，未知异常继续交由上层诊断。                                                                                                             |
| [`core/models.py`](../../control_plane/core/models.py) 与 [`core/errors.py`](../../control_plane/core/errors.py)   | 定义规格、生命周期记录、运行状态、结果和稳定领域异常；不依赖 HTTP 或 Docker SDK。                                                                                                                                                                    |
| [`core/service.py`](../../control_plane/core/service.py)                                                            | 生成身份与时间，协调 Runtime 和 Store 的创建、查询、暂停、恢复、续期、删除、pending 只读核验及确认恢复；通过公共`SandboxRuntime` 和最小 `LifecycleStore` 协议工作。                                                                                          |
| [`core/tasks.py`](../../control_plane/core/tasks.py)                                                                | 提供不向 worker 转发取消的等待、穿过重复取消的收尾和最终结果读取；业务层决定补偿与错误优先级。                                                                                                                                                       |
| [`core/reaper.py`](../../control_plane/core/reaper.py)                                                              | 串行调度 Service 的有界单轮回收，记录脱敏摘要并等待关闭收尾；不直接访问 SQLite 或 Docker。                                                                                                                                                           |
| [`storage/lifecycle_store.py`](../../control_plane/storage/lifecycle_store.py)                                      | 定义最小 Store 协议、稳定存储异常和标准库`sqlite3` 实现；只管理元数据，不调用 Docker。该模块的约束见其 [`AGENTS.md`](../../control_plane/storage/AGENTS.md)。                                                                                     |
| [`storage/run_lock.py`](../../control_plane/storage/run_lock.py)                                                    | 通过`SQLiteRunLock` 管理同库系统独占文件锁；区分锁竞争、文件错误和平台不支持，不管理 SQLite 事务或 Docker 资源。                                                                                                                                   |
| [`runtime/sandbox_runtime.py`](../../control_plane/runtime/sandbox_runtime.py)                                      | 用`ABC` 定义异步 `ping()`、`create(spec)`、`get(id)`、`get_endpoint(id)`、`pause(id, *, expected_transaction_id)`、`resume(id, *, expected_transaction_id)`、`inspect_resources(id, *, expected_transaction_id)`、`delete(id, *, delete_workspace=False, expected_transaction_id=None)`；不包含 SDK、线程或具体回滚实现。 |
| [`runtime/agent_http_probe.py`](../../control_plane/runtime/agent_http_probe.py)                                    | 实现可注入的固定 Agent HTTP 健康探针；限定本机入口、超时和响应大小，不访问 SQLite 或 Docker。                                                                                                                                                        |
| [`runtime/docker/docker_runtime.py`](../../control_plane/runtime/docker/docker_runtime.py)                          | 实现异步 Runtime、线程适配、只读状态/入口检查与取消等待。                                                                                                                                                                                            |
| [`runtime/docker/transaction.py`](../../control_plane/runtime/docker/transaction.py)                                | 实现同步 Docker 创建事务、精确资源记录、归属工具、错误分类、逆序回滚和正式删除。                                                                                                                                                                     |
| [`runtime/kubernetes/kubernetes_runtime.py`](../../control_plane/runtime/kubernetes/kubernetes_runtime.py) | 可显式构造的 Kubernetes Runtime，目前仅实现只读 namespace `ping()`，其他能力明确不支持。 |

`create_app()` 可以注入 `runtime`、`store`、测试 `database_path` 和 `agent_probe`；默认装配 `DockerSandboxRuntime`、`SQLiteLifecycleStore`、`HTTPAgentHealthProbe` 和共享 Service。Core 仅依赖最小 `AgentHealthProbe.check(endpoint)` 协议，不依赖 HTTPX。默认数据库路径根据 `app.py` 位置确定，不随启动目录变化。导入模块、构造依赖和调用 `create_app()` 不创建数据库、目录、连接、锁文件、Docker/HTTP client 或后台任务；`runtime/__init__.py` 不自动加载具体后端。

`create_app(max_concurrent_creates=2)` 将创建并发配置传给共享 Service；Service 直接装配也接受同名参数。默认 2，严格要求实际整数 1～16，拒绝布尔值和类型转换。非法配置在装配期间抛出 `ValueError`，不等待 lifespan。该参数不属于公开创建请求。

`create_app(max_instances=10)` 同时向 Service 传入同库受管实例总量上限，默认 10，只接受 1～1024 的真正整数；非法值在装配时拒绝，不触发数据库或 Docker I/O。Service 直接构造遵循相同规则，客户端不能指定。

FastAPI lifespan 按「检查认证配置 → 取得 SQLite 运行锁 → `Store.initialize()` → 可选的一轮 pending 恢复 → 启动回收任务 → 提供服务」执行。默认不恢复 pending。取锁、初始化或恢复中的整体存储失败阻止启动，不切换到内存或临时数据库。Docker 暂时不可用不阻止启动：恢复结束本轮并告警，`/healthz` 不访问 Docker，`/readyz` 只通过 `Service.ping()` 执行一次只读 `ping()`。

根就绪探测的整体超时默认 1 秒；实例列表、状态、Agent/服务 endpoint 与实例主动就绪检查使用独立的 `sandbox_query_timeout=1.0` 参数。创建、续期和删除不套用只读查询超时。Docker client 每次操作延迟创建并负责关闭；只读 ping 的 transport timeout 为 0.5 秒，生命周期创建、查询和删除为 5 秒。两个 client factory 分别注入，避免混用超时。请求等待取消不表示同步 SDK 或 SQLite 操作已经停止。

### Kubernetes 只读探测

新增 `KubernetesSandboxRuntime` 继承同一 `SandboxRuntime`，当前只实现 `ping()`。`app.py` 默认仍装配 Docker，不新增后端选择 API，不把此探测实现作为可创建 Kubernetes sandbox 的后端。`SandboxService.ping()` 可通过现有契约调用它，Service、API 和 Store 均不依赖 Kubernetes SDK。

Runtime 必须显式提供 kubeconfig 文件路径、context 和 namespace；导入、构造及工厂保存阶段不读取配置、不创建客户端或访问集群。每次 ping 在工作线程中读取指定文件，以指定 context 加载一份独立配置，不依赖 `current-context` 或 `KUBECONFIG` 自动选择、不合并其他配置，也不在失败后切换集群。首版支持本机 Minikube 静态客户端证书及 CA（文件引用或内嵌证书数据），保留 TLS 校验；拒绝 token、exec/auth-provider 插件、隐式集群内认证、跳过 TLS 验证及 kubeconfig 上游代理。

工作线程仅调用一次 `list_namespaced_pod(namespace, limit=1, watch=False, _request_timeout=(0.5, 0.5))`，空列表成功，不读取下一页；关闭 urllib3 和 SDK 额外重试，并通过私有配置禁用环境 HTTP 代理，不改全局环境或 SDK 默认配置。0.5 秒分别约束连接和读取阶段，不是含文件加载、DNS、全部读取与关闭在内的严格总 deadline。成功仅证明该 API 可达且具有此 namespace 的 Pod 列表权限，不证明有写权限、全部节点健康或 Agent 已就绪。

客户端由原工作线程在 finally 中关闭；首次及重复异步取消均等待已开始的线程和关闭收尾，读取最终异常，不从其他线程提前关连接。主操作失败优先于关闭异常，只有关闭失败也不返回成功。固定领域错误分别为 `KubernetesConfigurationError`、`KubernetesConnectionFailed`、`KubernetesTimeout`、`KubernetesPermissionDenied`，不携带原始配置、API 响应或凭据。其余 Runtime 方法（包括工作区）抛出 `KubernetesOperationUnsupported`；create 的失败证据为 `UNCHANGED`，不创建资源、不伪造查询结果、不回退 Docker。HTTP 错误映射与公开接口本轮保持不变。

使用官方 Python 客户端 `kubernetes>=37.0.1,<38`，锁文件固定 37.0.1；按[官方兼容矩阵](https://github.com/kubernetes-client/python#compatibility)，37.x 对应 Kubernetes 1.37。当前只在 WSL Ubuntu-24.04、Minikube v1.39.0 的现有 `agent-sandbox-k8s` context、Kubernetes v1.37.0 / containerd / Calico 三节点环境验证此项读取。namespace 为现有 `agent-sandbox-test`，不使用或修改存储类，不创建 Pod、Secret、RBAC 或 namespace，不切换 kubectl 当前 context。

在仓库根可直接执行只读检查；不需要启动 HTTP 应用或初始化数据库：

```python
import asyncio
from pathlib import Path
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime

runtime = KubernetesSandboxRuntime(
    kubeconfig_path=Path.home() / ".kube" / "config",
    context="agent-sandbox-k8s",
    namespace="agent-sandbox-test",
)
asyncio.run(runtime.ping())
```

测试工厂通过 `client_factory` 注入，只在工作线程接收 `kubeconfig_path`、`context` 两个关键字，返回具备 `list_namespaced_pod()` 和 `api_client.close()` 的对象；默认测试使用替身，不访问真实配置或集群。以下在 `control_plane/` 执行，集成必须同时显式启用并提供 context、namespace，否则在读取 kubeconfig 和创建客户端之前跳过。非默认配置文件可另设 `KUBERNETES_TEST_KUBECONFIG`，缺省使用明确的 `~/.kube/config` 文件：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_runtime.py
kubectl --context=agent-sandbox-k8s get nodes
kubectl --context=agent-sandbox-k8s get namespace agent-sandbox-test
RUN_KUBERNETES_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_runtime_integration.py
```

尚未实现 Kubernetes 资源创建、入口、工作区、暂停/恢复、删除与 pending 核验，不把只读 ping 通过描述为 Kubernetes 生命周期接入完成。

### SQLite 单实例运行保护

运行锁身份取自实际注入的 `SQLiteLifecycleStore` 所使用的数据库规范路径，而不是仅依据 `create_app(database_path=...)`。伴随锁文件位于数据库旁，例如 `lifecycle.sqlite3.lock`。同一数据库的不同应用对象及不同进程互斥，不同数据库可分别启动。SQLite 没有关闭该保护的开关；纯内存 Fake Store 不使用数据库运行锁。

Linux/WSL 使用标准库 `fcntl` 系统锁，Windows 使用 `msvcrt` 系统锁，按平台加载；不支持的平台明确拒绝启动。获取为非阻塞尝试：锁已被持有时立即返回稳定启动错误，不初始化数据库、调用 Docker 或启动回收任务。权限、路径和文件系统错误与锁竞争分别报告，错误与日志不包含完整路径、凭据或底层异常文本。

锁由独占文件句柄持有，句柄不传给无关子进程。锁文件存在、PID 或文件内容都不能证明锁仍被持有；锁文件可以在无持有者时保留。正常关闭在服务请求收尾和回收任务结束后显式解锁、关闭句柄，不删除锁文件，也不提供强制删除锁文件或终止占用进程的解锁方式。进程异常退出后由操作系统释放锁，后续实例可重新获取。

初始化失败和启动取消也必须释放锁，但已经开始的初始化或回收关闭工作须先通过共用关键任务等待工具完成，首次和重复取消均不能提前放锁。不持有长期 SQLite 写事务来实现保护，因此运行锁不阻塞 Store 正常的独立连接与短事务。

保护限于本机、本地文件系统及统一数据库路径，要求控制面遵守同一锁协议；不承诺网络文件系统、硬链接别名或 Windows/WSL 跨系统共享锁语义，也不能阻止外部程序直接操作数据库或 Docker。它不是分布式租约、多 worker 协调或 exactly-once 保证。取得锁不证明旧 Docker 请求已经结束，也不自动激活或删除 pending；开启确认恢复仍须满足下述独立准入条件。

## 身份、时间与持久化

### 资源身份与初始标签

每次创建生成不可预测的 `sbx_<32 位小写十六进制>` ID 和独立的 `txn_<32 位小写十六进制>` 事务 ID。事务 ID 用于区分同一 sandbox ID 下不同创建尝试；名称不能替代身份核验。本次实例自有资源都有完整归属标签；引用的独立工作区使用单独归属：

```text
io.agent-sandbox.managed=true
io.agent-sandbox.project=agent-sandbox
io.agent-sandbox.sandbox-id=<sandbox_id>
io.agent-sandbox.transaction-id=<transaction_id>
io.agent-sandbox.resource=container|network|workspace|proxy|ingress|egress
```

Service 支持注入时钟，每次创建在写 pending、调用 Runtime 前只读取一次带时区时间并归一化为 UTC。`created_at` 为该时间，`expires_at = created_at + ttl_seconds`，默认 TTL 为 1800 秒。Runtime 接收已计算值，不自行取时，将同一对初始时间标签写入本次实例自有资源：

```text
io.agent-sandbox.created-at=<RFC 3339 UTC created_at>
io.agent-sandbox.expires-at=<RFC 3339 UTC expires_at>
```

对外时间使用 RFC 3339 UTC、以 `Z` 结尾并保留微秒，例如 `2026-10-19T08:00:00.123456Z`。Docker 标签保存初始生命周期信息，不替代完整归属标签；SQLite 保存可更新的期限。续期不修改标签，也不调用 `container.update`。

### SQLite schema 与状态

默认存储为 `control_plane/.local/lifecycle.sqlite3`。当前 schema 版本为 **4**，仅初始化新库或核验同版本已有库；核验包含表、约束、索引及不存在额外 trigger。schema 2/3 必须在停服且持有 SQLiteRunLock 时显式调用 Store.upgrade()，启动不会自动升级；不支持版本 1 迁移或自动修复。数据库、journal、WAL、共享内存和伴随运行锁文件不得提交版本库。验证使用全新临时数据库，不删除或重建现有开发库。

记录包含不可变的 `sandbox_id`、`transaction_id`、`created_at`，以及可更新的 `expires_at`、`management_state` 和从 0 开始的 `version`。时间存储和比较使用整数 UTC 微秒，避免浮点时间戳或字符串排序损失精度。更新期限、状态迁移和删除均核对事务 ID 与预期版本；成功更新或状态迁移递增版本，已经处于 `deleting` 的精确版本重试保持原版本。旧版本不能覆盖新值，也不能通过刷新版本接管替代记录。

```text
pending（version 0） → active → deleting → 移除记录
```

`pending` 表示创建尚未确认或结果不明确；`active` 表示 Runtime 创建与元数据确认均完成；`deleting` 表示已经进入删除流程。手动删除可处理三种状态，`deleting` 禁止更新期限。这些是内部管理状态，不公开为 HTTP `status`，也不表示 Agent 健康。创建响应的 `started` 表示启动调用已完成，查询的 `ready` 要求 Agent Docker health 为 `healthy`，启用策略时还要求出口健康及拓扑正确；入口可用性另由 endpoint 查询核验。

每次同步存储操作在线程内创建、提交或回滚并关闭自己的连接，不跨线程共享连接；使用参数化 SQL 和有限锁等待，默认 0.5 秒。Store 的 `get()` 缺失返回 `None`，更新缺失与版本冲突各有稳定异常，精确身份的幂等删除接受记录已不存在；这些语义不合并。SQLite 事务中不等待 Docker。

## 生命周期流程

### 创建与确认

1. HTTP 层按实际字节有界读取最多 4 KiB，允许空请求或受约束 JSON；检查结构和字段，交由不可变 `SandboxCreateOptions`、`SandboxResources`、`SandboxNetwork` 严格校验数值和域名策略。Service 的无参数 `create()` 保持默认配置，接受选项时再次校验，并确认环境名在本 Service 的不可变注册集合中，然后非阻塞申请创建额度。满额抛出 `SandboxCreateCapacityExceeded`，HTTP 返回固定 429 `sandbox_create_capacity_exceeded`；只有准入成功后才生成身份、采样初始时间。
2. Service 用 `secrets.token_urlsafe(32)` 生成独立 Token；Store 在 `BEGIN IMMEDIATE` 内检查全部生命周期记录数量，再与 `pending`、版本 0 的记录及可选工作区占用原子写入。达到 `max_instances` 时不写入任何一项，Service 返回独立的实例总量 429。插入失败或 ID 冲突不调用 Runtime，也不创建 Docker 资源。
3. Service 将环境名、资源值与网络策略放入现有 `SandboxSpec`，Runtime.create(spec) 契约不变。Docker 后端维护内置与可信装配追加环境的不可变镜像映射，预检所选主镜像、固定入口镜像，仅非空策略时额外预检固定出口镜像；不 pull/build/fallback。主镜像和出口镜像预检后按本轮 ID 创建，避免标签变化。主容器转换 CPU 和内存参数，默认创建五项资源，启用策略时增加专属出口容器并注入固定代理变量；不允许覆盖任意镜像、代理额度、实际 Docker 网络、挂载、权限或其他安全配置。
4. Runtime 成功后，Service 按原事务 ID 和版本原子激活记录。只有激活也成功才返回 HTTP 201 与 `started`；这不等待 Agent 或 proxy 健康检查。
5. Runtime 明确证明未修改资源或已完成失败回滚时，Service 按原事务 ID 和版本补偿预写记录。有残留或结果未知时保留记录和 sandbox ID，不报告成功。

SQLite 与 Docker 没有跨系统原子事务。SQL 回滚不能撤销 Docker 操作，数据库有记录不证明实例已经创建成功或仍存在。激活失败可能发生在 durable commit 之后，版本结果未知时保留记录与资源用于诊断。

创建额度由每个 Service 自己持有，检查与占用之间没有异步让出点，当前事件循环内的竞争调用不会突破上限；满额无等待队列。额度从准入持续到本次操作及必要收尾结束，包含预写、创建、确认、晚到成功后的补偿，以及首次和重复取消时的关键任务等待，再由 `finally` 释放一次。不通过另一个无人等待的后台创建任务缩短额度生命周期。明确报告不确定结果并完成本次收尾后释放额度，残留 pending 及诊断依据继续保留，不永久占用创建额度。

查询、健康、endpoint、续期、删除和回收不受创建额度限制。生产装配为一个应用持有一个 Service，不使用全局计数、跨线程锁或分布式协调；不同 Service 的额度独立。创建并发准入不限制已创建实例数、不预留资源、不限制请求速率，也不提供整机容量保证或跨应用、进程、worker 的总量约束。

创建参数的默认值、范围和严格类型规则集中在 core 模型：环境名默认 basic，必须是匹配 `[a-z][a-z0-9-]{0,31}` 的精确 ASCII 字符串；模型只检查语法，Service 在创建准入前检查实际注册集合，core 不包含镜像地址；TTL 默认 1800、范围 1～86400 秒；CPU 默认 1000、范围 100～2000 毫核；内存默认 256、范围 128～2048 MiB。Service 直接调用同样校验。API 不接受任意 Docker 参数，非法配置返回 400，实际体积超限返回 413，两者均无生命周期写入或 Runtime 调用。环境和资源配置只用于创建传递，不增加 SQLite 字段或创建/查询/endpoint 响应字段；续期不调额或换环境，pending 确认恢复不重置初始期限。CPU/内存限制是单实例上限，不是预留或全局容量保证，尚无整机预算或动态调额。

### 受管实例总量

实例名额直接由 `lifecycle_records` 表示，没有额外计数器表或内存总量计数。普通和显式绑定创建分别通过 `Store.create(..., max_instances=...)`、`create_with_workspace(..., max_instances=...)`；两者在同一写事务内执行数量检查及插入，工作区占用、版本和凭据一起提交或回滚，不在 SQL 事务内访问 Docker。独立连接竞争最后一个名额时由 SQLite 串行决定，不能先在 Service 查询数量再插入。

所有 pending/active/deleting、旧记录、暂停、停止和到期未清理实例均占名额。成功创建只释放本次创建并发额度，不释放实例名额；结果不明或清理失败同样保留名额。只有原事务／版本保护的生命周期删除成功才释放，凭据和工作区占用沿原事务清理。取消仍等待已开始的 SQLite/Runtime 操作与关键收尾；迟到提交或确认丢失不靠内存计数猜测结果。

Store 以独立 `LifecycleInstanceLimitExceeded` 表示预期的满额结果，Service 映射为 `SandboxInstanceLimitExceeded`，HTTP 固定 429 `sandbox_instance_limit_exceeded`；不被元数据不可用或工作区冲突分支吞掉。SQLite busy、损坏和 I/O 错误保留原元数据错误。被取消的等待若最终确认未获名额，则按原取消语义结束，不清理其他事务。

重建应用从已有记录自然恢复；降低上限只阻止创建，不驱逐实例，也不限制查询、续期、暂停／恢复、删除和回收。独立工作区、保留卷、无记录遗留和其他数据库不在计数范围；仍无整机 CPU/内存预算、磁盘配额、租户配额或分布式调度。schema 保持 4，不迁移开发库。

### 可信自定义环境注册

`create_app(*, additional_environments=None)` 接受维护者提供的名称到镜像引用 `Mapping[str, str]`，默认仍为 basic/python/node。最多追加 32 项，不得覆盖内置名称；名称遵循创建规格语法，镜像引用采用受限 Docker reference 语法：ASCII 总长不超过 512，小写仓库路径不超过 255；可带合法 tag、DNS/localhost registry 与 1～65535 端口、`sha256` 摘要。拒绝 URL、凭据、空白/控制字符、Unicode、IPv6 registry 和大写仓库名；完整格式由 Docker 后端本地校验，不访问 registry。装配立即校验、复制并冻结配置，不访问 Docker、镜像或数据库，不提供动态注册、持久化或 HTTP 配置入口。

默认装配将同一映射传给 `DockerSandboxRuntime`，并从 Runtime 实际只读 `environment_images` 派生 Service 的名称集合；注入已有 Docker Runtime 时也以其实际映射为准。如同时显式传入应用映射，两者必须一致，否则装配抛出 `ValueError`，避免 HTTP 接受而运行时拒绝。独立构造 Service 可传入完整 `environment_names` 集合，省略时仍只接受三种内置名。

注册只表示维护者允许选择该镜像，不代表控制面审计镜像内容或保证其隔离强度。镜像须预装约定路径的 Agent、sandbox 用户、工作区及健康检查依赖；运行时固定 Agent 入口、参数和安全配置，不运行镜像自带业务入口，不注入 Agent 或联网安装软件。预检同时拒绝 `/workspace` 之外的镜像 VOLUME 声明，避免产生未受管的匿名卷。只预检所选镜像并固定本轮 ID；去掉注册项后，旧实例仍按实际资源、完整归属和原生命周期记录管理，无 schema 或响应字段变化。完整镜像契约及可运行例子见 [Docker 实现](docker/IMPLEMENTATION.md#custom-environments)。

### Pending 创建记录只读核验

内部 `await service.inspect_pending_once(limit=100)` 显式核验一批 `pending` 记录，没有 HTTP 接口，也不接入应用启动或周期回收。Store 直接使用有界 SQL，按 `created_at`、`sandbox_id` 稳定排序；`limit` 必须为 1～2⁶³−1 的真正整数。方法不初始化或修复数据库，不自动恢复、激活或删除记录与资源。

Service 保存候选身份和版本，调用 `Runtime.inspect_resources(sandbox_id, expected_transaction_id=...)`，随后重新读取记录。记录消失、身份或版本变化，或已经离开 `pending`，均返回 `changed`，避免将旧观察当作当前记录结论。整体存储失败通过结果的 `error` 明确报告，不伪装成空列表；单项资源冲突或后端不可用不阻止本批其他项。

每项包含 `sandbox_id`、`classification`、稳定 `reason`，以及能够可靠确认的 `found_resources` 和 `missing_resources`。分类如下：

| 分类            | 含义                                               |
| --------------- | -------------------------------------------------- |
| `all_present` | 策略决定的五类或六类资源存在，归属及策略一致。                 |
| `partial`     | 仅发现部分匹配资源；主容器缺失仍继续核验其他角色。 |
| `not_found`   | 没有发现匹配的受管资源。                           |
| `conflict`    | 角色重复、事务不符或其他归属冲突，不能安全归并。   |
| `unavailable` | 后端不可用，无法确认资源情况。                     |
| `changed`     | 核验期间生命周期记录已经变化或消失。               |

Runtime 只读检查 `container`、`proxy`、`egress`、`network`、`ingress`、`workspace`，复用资源发现和完整归属校验；不通过仅筛选预期事务隐藏同一实例的其他事务。检查不改变资源、标签、期限、版本、管理状态或正常进行中的创建任务。

结果仅覆盖本批，且不是 SQLite 与 Docker 的跨系统原子快照。`all_present` 不证明创建成功、拓扑正确或 Agent 就绪；`not_found` 不排除标签缺失或无法识别的遗留资源。`pending` 状态和记录年龄也不能证明原创建任务已经停止。只读核验不负责激活或清理，受控确认恢复见下一节。

### 完整 pending 实例确认恢复

`await service.recover_pending_once(limit=100)` 显式处理一批按创建时间、sandbox ID 稳定排序的 pending 记录，复用 SQL 层的有界选择。调用方须拥有运行所有权并排除并发创建；不能仅按记录年龄判断原创建已停止，也不应在接收创建请求的应用上并行执行恢复。启动接入在持锁且尚未提供 HTTP 服务的阶段满足这一约束；运行锁不能撤销旧 Docker 请求。

新凭据实例额外要求代理配置与持久化凭据一致。确认依次要求：`inspect_resources()` 按策略要求五类或六类资源齐全且归属与策略匹配，`Runtime.get()` 的 sandbox ID、事务 ID 和 created_at 与原记录一致，状态为 ready，现有 `Runtime.get_endpoint()` 核验通过。运行时检查结束后重读完整记录，变化或消失即跳过；在每个候选的提交校验时读取一次 UTC 时间，要求 `expires_at > now`。再按原事务、原版本及 pending 状态调用 `Store.activate()`，成功仅递增版本并转为 active，冲突不刷新版本重试。

该方法不初始化数据库，不调用 Runtime 创建或删除，不修改资源、标签、created_at 或 expires_at。ready 只是本阶段恢复的保守准入条件，通常的 active 仍不表示 Agent ready。检查与提交不是跨系统原子快照；确认成功后的运行状态仍以 Runtime 查询为准。

结果为 `items`、`error` 及 recovered/skipped/failed 计数，每项含 sandbox_id、outcome、稳定 reason。资源部分存在、未发现、冲突、身份不符、尚未 ready、入口不符、恰好到期及记录变化均保留记录并跳过。读写失败报告 failed；整体存储故障以 `sandbox_metadata_unavailable` 结束本轮，后端不可用以 `docker_unavailable` 结束本轮。失败计数不证明写入没有提交，已提交的 active 保留，调用方可通过 `Store.get()` 核对实际管理状态；HTTP 查询仍只公开运行时状态和当前期限。

`create_app()` 提供以下可选参数，装配时严格校验，不产生外部 I/O：

| 参数                             | 默认值    | 约束                                                                          |
| -------------------------------- | --------- | ----------------------------------------------------------------------------- |
| `pending_recovery_enabled`     | `False` | 必须为布尔值；开启时要求实际 Store 具备当前 SQLite 运行锁保护，否则拒绝装配。 |
| `pending_recovery_batch_limit` | `100`   | 1～2⁶³−1 的真正整数；只处理一个有界批次，不扫描至全库结束。                |

开启后恢复完成才启动回收任务和提供 HTTP，可能增加启动耗时。后端不可用记录脱敏告警后允许服务启动，整体存储失败阻止启动。摘要可通过 `application.state.pending_recovery_result` 查看；关闭开关时为 None。启动恢复不等待实例变为 ready，不循环重试，跳过项不会自动周期重试，也没有公开恢复 API。

取消时等待初次 SQLite 查询或当前候选的关键操作收尾，停止后续候选，收尾前保持运行锁。恢复独立于新建补偿，不删除资源，不把已提交 active 改回 pending；提交成功但响应丢失时同样保留实际记录。partial/缺失资源修补或清理、周期恢复及更完整的创建任务与故障协调尚未实现。

### 受管实例发现与分页

`GET /v1/sandboxes` 经管理员认证后，沿 API → `SandboxService.list_sandboxes()` → `LifecycleStore.list_sandboxes()` 读取公开管理摘要。列表不访问 Runtime、凭据表或工作区详情，不探测资源，不改变状态、版本与期限。返回的 `management_state` 来自数据库，不能解释为运行或健康状态；所有生命周期记录都可见，计数与实例总量准入一致。

Store 在线程内以独立连接开始短暂 `BEGIN` 读事务，先统计全部记录，再按 `(created_at_us, sandbox_id)` 做排他位置查询，SQL `LIMIT limit+1`；两次查询共享快照，不在事务内等待网络。摘要 SELECT 只包含公开 ID、状态、时间和可空 `workspace_id`，不读取事务 ID、Token 或内部资源标识。Service 添加当前 `_max_instances`，已有记录超过上限仍是合法结果。不新增表、索引或 schema 升级。

默认每页 20，上限 100。游标最多 256 ASCII 字符，采用 `v1.` 与无 padding 的 URL-safe Base64，紧凑 JSON 保存 `[整数 epoch 微秒, sandbox_id]`；严格校验版本、规范编码、类型和 datetime 范围后，将位置作为 SQL 参数。无后页返回 null；锚点消失不影响继续。分页没有持久会话或签名，多次翻页不保证同一快照。

API 拒绝重复、未知和非法参数，所有已认证列表响应设置 `no-store`。存储异常转换为固定 `503 sandbox_list_unavailable`，只读整体 deadline 返回 `504 sandbox_list_timeout`，不能伪装成空列表。Python SDK 只取一页，用独立不可变 `SandboxManagementSummary` 和 `SandboxListPage` 检查字段、计数、顺序与游标，不复用 `SandboxInfo`，只发送控制面管理员凭据。

### 状态与入口查询

Service 在查询、endpoint、续期和删除访问后端前校验 ID。状态查询先由 Runtime 只读加载唯一候选容器，重读精确 ID、核对完整归属、恢复初始时间与内部事务 ID，再映射 Docker 容器和 health 状态。实际容器不存在仍返回 404，即使数据库有记录。

Service 随后读取 Store，核对 sandbox ID、事务 ID 和创建时间；身份匹配才使用数据库当前 `expires_at`，运行状态、reason 和 message 仍来自 Runtime。数据库不可用或身份冲突返回稳定错误，不静默回退到旧标签。重启后从同一数据库恢复期限，不重新取时，也不自动延长。

没有记录时保留 Docker 初始时间，不补写或接管 TTL。旧实例同时缺少两个时间标签时返回双 `null`；只缺一个、格式错误或过期早于创建均为查询失败，不猜测时间。无记录旧实例仍可查询状态和删除，但拒绝交付旧无认证 endpoint，不能续期，也不纳入数据库到期回收。

状态查询不修改资源、不触发删除或修复；超过计划期限不新增 `expired` 状态，到实际回收前仍保持原状态、endpoint 和命令语义。Control 不直接访问容器 IP；新容器由 Runtime 固定健康检查，在容器内部请求 `/healthz`，只有 Docker health 明确为 `healthy` 才返回 `ready`。没有 health check 的旧运行容器仍为 `running`；健康失败不自动删除、重启或清理 workspace，重建镜像也不会更新旧容器。

endpoint 查询由 Service 合并 Runtime 入口事实与 SQLite 的同实例／同事务凭据，并核对创建时间、认证版本及代理配置 Token 摘要；摘要仅用于可信侧比对，不写 Docker labels，也不是加密存储。Runtime 核验 sandbox/proxy 的完整归属与相同事务、专属两网角色/internal 属性、实际连接、sandbox DNS 别名及 proxy running/healthy；只从实际 `NetworkSettings.Ports` 接受唯一 `127.0.0.1` 有效端口，不以 `HostConfig.PortBindings` 推断发布成功。无 proxy 的旧实例返回不可用，不自动迁移；查询成功后的可用性仍可能随资源退出变化。具体 404、409、503 与 504 契约见 [入口 API](API.md#6-查询本机入口)。

### 实例执行入口凭据

`endpoint_credentials` 独立表保存 sandbox ID、事务 ID 和随机 Access Token，与 pending 和可选工作区占用原子提交。CAS 删除生命周期记录时，同一事务级联删除凭据；创建／补偿／删除结果不明时沿现有规则保留恢复依据。普通记录、模型 repr、日志和诊断不含明文。

数据库含本地明文秘密，不是加密存储。新库限制为宿主用户可读写（Unix 0600，新目录 0700）；备份也需同等保护，不提交版本库。可信宿主／Docker 管理员仍可读取代理配置，Token 不进入主容器、出口容器、workspace 或 Docker labels。

动态入口使用独立 `agent-sandbox-proxy:dev` 镜像，并预检认证协议版本、固定实际镜像 ID。入口容器在私有 tmpfs 中以受限权限生成配置，只替换指定 Token 变量；缺失或非法配置拒绝启动。Nginx 以大小写敏感的有界规则匹配，不能声称常量时间比较。所有执行路径验证后清除认证头，只精确 GET /healthz 匿名。

旧代理不被改造，默认拒绝旧 endpoint 交付并提示重建，原地址可能继续无认证可用；固定 Compose 行为保持原状。Token 不实现即时撤销、逐请求 TTL 检查或暂停联动，删除／回收生效前仍可能有效。这不等于资源级授权或完整隔离。

### 受控 HTTP 服务入口

新动态入口在选择上游前统一拒绝路径点段、重复分隔符、反斜杠、编码分隔符、编码点和双重编码等歧义原始路径，避免服务前缀经归一化消失后进入 Agent 兜底。query 不参与这项路径限制，原 Agent 文件参数继续按 query 编码。规范 Agent 路由保持不变。

`GET /v1/sandboxes/{id}/services/{port}/endpoint` 沿 API → `SandboxService.get_service_endpoint()` → 原 `get_endpoint()`/Runtime/Store 读取链路，先校验真正整数端口 1024～65535 且排除 8080，再复用实例归属、事务、创建时间、loopback 发布、网络和凭据核验。HTTP 层校验规范十进制路径字段并设置 `Cache-Control: no-store`；core 返回独立 `SandboxServiceEndpoint`，包含请求实例 ID、应用端口、固定服务 URL、`auth_required=true` 和隐藏 repr 的原实例 Token。不新增 Runtime 方法、数据库字段、应用探针或数据 HTTP 转发逻辑。

动态镜像继续使用 `agent-sandbox-proxy:dev`，预检同时要求 `io.agent-sandbox.proxy-auth-version=1` 与 `io.agent-sandbox.proxy-services-version=1`，固定本轮实际镜像 ID 创建。新实例自有资源写入 `io.agent-sandbox.endpoint-services-version=1`，独立复用卷不修改；入口恢复从存留自有角色和代理实际 Image ID 核验一致性。资源与镜像均缺少服务标记表示旧能力；仍支持原认证命令、文件接口，但新服务接口返回 409 `sandbox_service_unsupported`，固定提示 `proxy does not support service forwarding; recreate sandbox`。未知或矛盾能力标记不作为旧版本降级放行。能力标签不是删除授权，清理仍以精确资源 ID 和完整归属为准。

调用方直接访问 `http://127.0.0.1:<入口端口>/services/<应用端口>/`，入口将原始 URI 的前缀剥离后转发固定 `sandbox:<port>`。端口只能来自规范受限路径，不能指定 host、协议、其他实例或任意代理目标；点段、编码路径分隔符、反斜杠、绝对形式与 Upgrade 被拒绝。所有服务路径包括应用 `/healthz` 均要求实例 Token。代理移除实例认证和 Forwarded 等平台控制头，忽略并隐藏上游 X-Accel 控制响应，禁用请求重试和重定向改写，限制每实例应用并发 8、请求体 10 MiB、请求头缓冲 32 KiB、建连 2 秒、上游读写空闲 30 秒、下行空闲 40 秒。完整方法、失败响应与传输限制见 [服务入口 API](API.md#查询受控-http-服务入口)。

返回连接只证明原入口及能力核验通过，不证明应用正在监听或健康；实例 readyz 仍只检查 Agent。应用需监听容器内 `0.0.0.0` 并自行兼容公开路径前缀、重定向和 Cookie，普通 HTTP 转发不保证任意网页可用，也不提供 WebSocket 或原始 TCP。Token 仍能授权同实例 Agent 执行，不提供按端口授权；管理员 key 不发送到应用。Python SDK 以独立 `ServiceConnection` 交付凭据，不扩大 `EndpointConnection` 或新增应用请求封装。

该能力不改变 Agent 命令最长 30 秒、取消即停止的约束，不提供长期服务托管。应用受实例暂停、TTL 和删除约束，代理不逐请求查询 TTL，也不在暂停时瞬时撤销 Token。更新同名镜像只影响后续创建，不改固定 Compose、schema 或已有实例；需要新能力时显式重建，不能按新标签推断旧容器已更新。

### 实例主动就绪检查

`GET /v1/sandboxes/{id}/readyz` 经 `Service.check_ready()` 依次调用 `Service.get()`、现有入口核验和可注入的 Agent HTTP 探针。状态查询包含实际 Runtime 与 SQLite 身份核对；状态不是 ready 时停止，不查询入口或发起 HTTP。入口返回的 sandbox ID 必须匹配请求，地址还须通过探针的严格校验。该流程不新增 Runtime 方法、不修改管理状态、期限或资源，也不改变 pending 恢复的准入条件。

`HTTPAgentHealthProbe` 每次请求延迟创建并关闭 HTTPX client，只允许 `http://127.0.0.1:<有效端口>`（可带根路径 `/`），固定 GET `/healthz`。拒绝其他主机、用户信息、异常路径、query 和 fragment，禁用环境代理、重定向与重试；要求 HTTP 200 和健康 JSON `{"status":"ok"}`。以 raw stream 有界读取最多 4 KiB，拒绝非 identity 编码，不触发自动解压。

API 的 `sandbox_query_timeout`（默认 1 秒）覆盖状态、Store、入口和 HTTP；探针另设默认 0.5 秒的网络及整体探测超时，防止慢速分块无限延长读取。探针失败为 503 `sandbox_agent_unavailable`，API deadline 为 504 `sandbox_readiness_timeout`，未 ready 为 503 `sandbox_not_ready`；领域错误继续集中映射。取消直接传播到 HTTP，只对响应及连接关闭使用共用关键任务等待，重复取消也须读取收尾结果。底层异常和响应体不进入公开错误。

根 `/readyz` 仍只读 ping Docker；状态的 ready 仍是 Docker health 观察，主动检查额外确认当次入口链路，不保证后续命令成功、可用额度或安全隔离。没有轮询等待、后台探测或自动续期，成功也不是跨系统原子快照。请求与错误示例见 [主动就绪 API](API.md#9-主动就绪检查)。

### 暂停、恢复与同实例协调

Service 先在同一实例的操作锁内核验 Runtime 与 Store 身份，要求管理状态 `active` 且 `expires_at > now`，随后把预期事务交给 Runtime。无记录、pending、deleting、已到期或运行状态不允许时返回暂停/恢复冲突；资源或存储身份冲突不自动接管。目标状态已经达到的重复请求仍须完整准入与资源核验。

暂停只冻结现有主容器，非空出口策略还冻结本实例 egress；先冻结主容器再出口，恢复则先出口再主容器。入口 proxy 保持运行，所有资源 ID、端口、标签、workspace 与进程内存保留。暂停事实从 Docker inspect 得到，查询公开 paused，主动 readyz 在状态阶段拒绝。SQLite schema 不变，management_state 仍为 active；控制面重新装配后继续观察原容器。

TTL 不随暂停停止，paused 仍可在到期前续期，默认留卷删除和到期回收同样适用。同一 `SandboxService` 以每实例操作锁协调暂停、恢复、手动删除、回收认领及创建失败后的资源补偿，锁覆盖已开始操作的取消收尾；不同实例可并行。此锁不提供分布式互斥，不约束另一个 Service、外部 Docker 操作或已经发出的旧 Docker 请求。

Docker 操作经有限 transport timeout 的工作线程执行。取消等待已开始步骤的结果，重复取消不能提前释放协调锁；失败或响应丢失时以查询事实和同目标重试收敛。多容器部分完成不作跨系统原子性承诺，不自动重建或补写管理状态。前置失败不得产生 pause/unpause；每次写操作重读精确 ID 和完整归属，确认结果后再成功返回。

冻结保留内存而不释放资源，不是 checkpoint 或宿主重启恢复。TCP/SSE/CONNECT 可能因外部期限失效；任务 timeout 和终态保留期不保证随冻结停止，恢复后可能立即触发。此项不修改 Agent、镜像、执行协议或连接期限。HTTP 契约见[暂停与恢复 API](API.md#10-暂停与恢复)。

### 续期与并发认领

续期只接受 1～86400 的真正整数 TTL。Service 使用一次 UTC 时钟快照，先只读核验 Runtime 实例，再核对 Store 的 sandbox ID、事务 ID 和创建时间；仅存在记录、`active` 且 `current_expires_at > now` 时允许续期，恰好到期也拒绝。目标为：

```text
new_expires_at = max(current_expires_at, now + ttl_seconds)
```

TTL 从本次校验时刻计算，不在原期限上累加，也不缩短。即使目标期限与当前值相同，仍执行有事务 ID 和预期版本保护的提交，使先完成的删除认领生效；不能因为“无需延长”而绕过竞争检查。续期先提交使旧回收候选认领失效，删除或回收先进入 `deleting` 则不能续活；并发版本冲突返回冲突，不刷新版本强行覆盖。续期成功不阻止随后显式手动删除。

续期不新增 Runtime 方法、不重建资源、不修改 Docker 标签、入口或运行状态。SQLite 写入取消须等待最终结果并读取异常；已提交期限不反向补偿，以免覆盖后续更新。调用方没有收到响应时可能已经提交，应通过状态查询确认实际期限。完整协议见 [续期 API](API.md#7-续期-sandbox)。

### 删除与元数据收尾

Service 先读 Store；有记录时按原事务和版本将 `pending` 或 `active` 认领为 `deleting`，已有 `deleting` 允许精确版本重试。读取或认领失败不开始 Docker 删除。随后将预期事务 ID 传给 Runtime；无记录旧实例仍按 Docker 完整归属删除，不要求容器健康、endpoint 可用或 `Runtime.get()` 成功。

Runtime 从标签恢复六类可能的存留资源，即使只剩网络或 workspace 也可处理。破坏性操作前核验完整标签、有效且跨角色一致的事务、角色唯一性以及预期事务；默认保留的 workspace 也要核验。每次 stop/remove 前重读精确容器/网络 ID 或经核验的卷 Name，归属冲突拒绝删除，不按名称、前缀或全局扫描接管资源。

正式删除依次处理 proxy、sandbox、ingress、internal，以及显式请求时的 workspace。容器使用 2 秒停止宽限，失败后重新核验再强制移除本实例容器；网络和卷不强制删除，不强制断开其他容器。已经消失的资源视为完成，每次删除后确认不存在；部分失败不会重建已删除项，对同一 ID 重试继续收敛。Docker 细节见 [删除边界](docker/DESIGN.md) 和 [实现与验证](docker/IMPLEMENTATION.md)。

Runtime 达到删除目标后，Service 才按认领事务和版本删除元数据。默认保留 workspace 时同样移除已结束实例记录；残卷继续按完整 Docker 标签和显式 `delete_workspace=true` 管理。Docker 失败保留 `deleting`；资源已删除但元数据收尾失败或版本冲突时保留 sandbox ID，允许同 ID 重试，不盲目刷新版本。合法 ID 无目标、重复删除或默认删除只剩保留卷均可达到 204 空体目标。

默认留卷会持续占用磁盘；单轮与后台过期回收也保留 workspace，尚无自动卷清理。创建失败的逆序回滚只清理本次创建的私有 workspace，引用的独立工作区不进入回滚清单。

## 异常、取消与恢复证据

### 异常转换边界

Runtime/Store 将依赖异常转换为稳定领域或存储异常并保留异常链，事务边界兜底清理；Service 协调元数据与资源结果；API 通过 [`api/errors.py`](../../control_plane/api/errors.py) 集中固定公开响应。endpoint 将不可核验的入口和后端不可用归并为稳定 503，操作超时另行处理。映射不读取异常消息生成公开文案，未知异常不伪造为已知业务错误。完整错误表、固定文案和响应示例保留在 [HTTP API](API.md)。

创建异常可附带不可变的 `CreationFailureInfo(phase, resource_state, cleanup_issues)`；`CleanupIssue` 只保存固定资源角色、阶段和异常类型类别，不保存原始 SQL、路径、凭据、后端响应或资源身份。Service 根据明确证据决定是否移除元数据：

| `resource_state` | 语义                                     | Service 行为                 |
| ------------------ | ---------------------------------------- | ---------------------------- |
| `unchanged`      | 已确认没有资源修改。                     | 可补偿原事务与版本的元数据。 |
| `rolled_back`    | 已确认本次资源回滚完成。                 | 可补偿原事务与版本的元数据。 |
| `remaining`      | 确认仍有资源残留或归属变化使其不能删除。 | 保留记录和诊断 ID。          |
| `unknown`        | 无法确认创建或清理的最终结果。           | 保留记录和诊断 ID。          |

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

| 参数                        | 默认值   | 约束与语义                                   |
| --------------------------- | -------- | -------------------------------------------- |
| `reaper_enabled`          | `True` | 必须为布尔值；控制是否在 lifespan 启动任务。 |
| `reaper_interval_seconds` | `60.0` | 有限正数，拒绝布尔值；每轮完成后等待的秒数。 |
| `reaper_batch_limit`      | `100`  | 1～2⁶³−1 的真正整数；每轮候选上限。       |

配置在装配时校验。数据库初始化成功后启动唯一后台任务，第一轮在后台立即执行，不等整轮结束才提供 HTTP 服务。同一应用实例的各轮串行；无候选、满批次和普通失败后均等待完整间隔，避免忙循环。普通测试显式关闭回收，专门验证该行为时才开启。

循环检查单轮 `error`、`items` 及普通异常，记录处理计数和稳定失败原因，失败不会永久停止后续轮次。应用关闭时取消并等待任务，不再启动下一轮；重复取消不能跳过当前关键收尾，也不使用短超时假定工作线程已经退出。

当前仅支持单进程、单 worker，同库运行锁不提供分布式互斥、租约或 exactly-once 保证。扫描间隔、单轮耗时和失败重试可能延迟删除，不承诺到期瞬间回收。强制终止后，下次启动继续读取持久化的到期 `active` 和 `deleting`；pending 只在显式开启的一轮启动恢复中按严格条件确认，不自动回滚或清理，无记录旧资源不会自动接管，保留 workspace 不自动清理。

## 环境、依赖与启动

本地运行目标为 WSL Ubuntu；`pyproject.toml` 要求 Python **>=3.12**，使用 `uv` 和锁文件管理依赖。运行依赖为 FastAPI、Uvicorn、Python Docker SDK 和 HTTPX，测试依赖为 pytest 和 pytest-timeout。默认 Fake/临时 SQLite 测试也可在受支持的 Windows Python 环境验证，但 Docker、网络隔离和真实资源行为必须按实际 Linux/WSL 环境验收。

检查环境并安装依赖：

```bash
python3 --version
uv --version
docker version
cd ~/agent-sandbox/control_plane
uv sync --locked
```

`/readyz` 成功要求当前 WSL 用户可访问 Docker daemon；创建还要求所选环境的主镜像与固定代理镜像已存在，不要求其他环境镜像。镜像构建与现有 Compose 开发实例的关系、代理镜像和验证准备见 [Docker 实现与验证](docker/IMPLEMENTATION.md)，Agent 镜像构建上下文仅为 `sandbox_agent/`，不自动重建既有开发资源。

前台启动且仅绑定本机：

```bash
cd ~/agent-sandbox/control_plane
export AGENT_SANDBOX_API_KEY="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
uv run uvicorn control_plane.app:app --app-dir .. --host 127.0.0.1 --port 18083
```

启动时先检查认证配置，再取得同库独占运行锁并显式初始化、核验 schema 4，同版本记录保留；schema 2/3 必须先按下文显式升级。锁被占用、不兼容或初始化失败时进程启动失败。更换 HTTP 端口不能绕过同库互斥。确认监听地址：

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
- 实例主动就绪的状态与身份前置校验、严格入口地址、固定健康响应、4 KiB 原始字节上限、压缩与重定向拒绝、环境代理隔离、两层超时与重复取消清理。
- 单次 UTC 时钟、1800 秒初始 TTL、微秒精度、全部资源标签一致、状态/health 映射、入口拓扑与实际发布端口；旧实例双 `null`、错误时间标签、不隐式接管。
- 临时文件数据库的预写、激活、状态/版本、跨 Store/Service 恢复、查询身份合并、缺失和冲突区别；拒绝旧 schema，不修改开发库。
- 固定创建配置、精确逆序回滚、NotFound、归属变化、丢失删除响应、删除成功但资源残留，以及未登记创建结果不能被成功清理掩盖。
- 创建、激活、续期、删除与回收的首次/重复取消、迟到提交、原事务/版本补偿、后台异常读取；查询主错误叠加 close 失败、独立 close 失败和结构化恢复证据。
- 暂停/恢复的完整归属、状态与期限准入、幂等目标、部分状态、按实例协调及取消收尾；五/六角色同进程冻结、控制面重装后继续、暂停期间删除与过期回收。
- 默认留卷/显式删卷、残留发现与事务/角色预检、占用失败重试、元数据收尾；续期与手动删除/回收的版本竞争，包含目标期限不变时的保护提交。
- 单轮边界、批次上限、稳定排序、失败重试、候选失效、取消收尾；后台首轮、固定间隔、串行执行、空/满批与失败后等待、日志摘要及重启后的持久化候选。
- pending 有界只读核验、五类或六类资源缺失与冲突、后端不可用、记录变化及存储失败；核验前后资源和元数据保持不变。
- 完整 pending 的身份、ready、入口及原 TTL 准入、原版本激活、竞态和提交确认丢失；默认关闭、启动顺序、后端与存储故障策略，以及候选选择和激活取消期间的持锁收尾。
- 同库应用对象和独立子进程互斥、不同数据库独立、正常关闭及进程异常退出后重新获取；初始化失败与取消、回收关闭收尾期间保持锁，以及无锁持有者的既有锁文件和脱敏文件错误。

运行锁测试仅使用临时目录、临时 SQLite 和本次创建的子进程，有界等待并清理测试进程，无需真实 Docker。Windows 文件锁须在原生 Windows Python 环境单独验证，WSL 结果不代表 Windows 验收。

真实 Docker 集成默认跳过，须在准备好的环境显式启用：

```bash
RUN_DOCKER_INTEGRATION=1 uv run --locked pytest -m docker_integration -v
```

集成测试使用临时数据库，覆盖固定资源配置与初始标签、双实例不同端口/正确路由/独立工作区、HTTP/文件/SSE、重启恢复、网络出口对照、留卷/删卷、单轮和 lifespan 到期回收、续期后在新期限回收、完整和部分资源的 pending 只读核验，以及完整 pending 启动确认后的查询、续期与回收；暂停集成另覆盖五/六角色的内存 token、同进程计数器冻结/继续、控制面重装、入口/文件保持、暂停续期与删除/过期留卷。运行前记录既有容器、网络和卷快照，确认固定镜像已存在；测试不 pull/build，只清理本次记录且精确 ID、完整标签核验通过的资源。Linux 网络限制不能以 Windows Fake 测试结果代替。操作步骤见 [Docker 实现与验证](docker/IMPLEMENTATION.md)；报告验证结果时注明平台和未执行的检查。

## 常见问题与恢复边界

### 启动提示同库实例正在运行

同一数据库只能由一个遵守锁协议的控制面实例运行。正常停止已知实例并等待服务与回收收尾后重试，或为独立实例配置另一数据库；更换端口不能解除冲突。不要删除伴随锁文件或自动终止其他进程。锁文件存在但没有持有者不会阻止启动；权限和文件系统错误应按对应的稳定启动错误排查。

### 进程健康但 Docker 不可用

`/healthz` 正常而 `/readyz` 为 503 表示进程能处理请求，Docker 连接当前不可用。用 `docker version`、`docker info` 检查 daemon 与当前用户访问权限，不通过放宽 sandbox 权限或挂载 socket 修复。可在单独启动的测试进程设置不存在的 `DOCKER_HOST`，验证健康仍为 200、就绪为脱敏 503；不要停止 Docker 或改动既有资源。

### 元数据初始化失败或查询冲突

核对配置的数据库路径、可写性、schema 兼容性和所用数据库是否与该批实例一致。不兼容旧库不自动迁移，也不通过删除或重建现有开发库绕过错误。查询身份冲突不应通过覆盖记录或回退标签隐藏；按响应中的 sandbox ID 核验实例、事务和创建时间。

### 创建、取消或删除没有收到最终响应

取消、连接断开和超时不能证明写入或资源操作没有发生。创建可能保留 `pending` 或未知结果，须按 sandbox ID 核验；默认不恢复 pending，可在受控启动阶段开启完整实例确认，跳过项仍需后续判断。删除对同一 ID 重试，并遵守精确身份和完整归属，不使用名称前缀批量删除或 `docker prune`。续期及确认恢复则通过后续查询核对实际记录，不根据响应缺失反向撤销。

### Agent ready 但入口不可用，或到期后仍可查询

`ready` 只描述容器内部 Agent 健康；endpoint 独立核验 proxy、两网和实际端口。入口暂不可用可在有限期限内重试并按 Docker 实现文档排查，不自动迁移旧实例。计划期限与实际删除之间存在扫描、处理和重试延迟；检查回收稳定日志、持久化管理状态及是否以单 worker 运行。默认删除和到期回收留卷，应记录 sandbox ID 并单独决定数据保留时长。

### 已取得服务 URL，但应用请求失败

服务连接查询不探测应用。先核对应用监听 `0.0.0.0`、端口与返回前缀相同、进程仍在运行，再检查实例暂停、停止和 TTL 清理状态；Agent ready 不代表应用 ready。401 检查是否使用对应实例 Token，不能以管理员 key 替代；400 检查端口、路径编码或 Upgrade；429 为应用并发上限；502/504 可能是未监听、进程退出或有限代理超时，不应盲目重放有副作用的请求。409 `sandbox_service_unsupported` 需要显式重建实例，同名镜像重建不能更新既有容器。页面资源、跳转或 Cookie 路径错误需由应用兼容公开前缀，详见 [Docker 排查](docker/IMPLEMENTATION.md#受控-http-服务入口)。


## 显式工作区及持久化独占

显式工作区是独立对象，不属于实例创建事务自有资源。Service 经 Runtime 固定所选环境镜像 ID，写入 pending 工作区身份，再初始化卷并确认 available；失败与取消结果不明时保留身份用于诊断，不猜测或接管未知卷。创建令牌、精确卷名和镜像 ID 持久化，公开 API 只提供 ID、状态、持有者和创建时间。

schema 4 保留 schema 3 引入的 workspaces、workspace_bindings 与可空 workspace_id，保存独立身份及唯一占用。绑定、pending 实例写入和 workspace 版本递增在单一 BEGIN IMMEDIATE 中完成；解除占用与带事务/版本保护的实例记录删除也在同一事务完成。迟到的旧持有者不能释放新占用。创建失败、取消、删除失败、pending 核验和控制面重启都必须保留原绑定身份，直到实际实例清理与元数据提交完成。

同一 Service 的每实例锁覆盖显式绑定的创建关键阶段与删除，避免 pending 创建仍在进行时被误当作空实例回收。Runtime 的 expected_workspace 独立于生命周期版本，绑定即使主容器已不存在也必须核验。查询、续期、暂停/恢复、pending 核验及恢复比对数据库绑定与实际挂载，不能只凭卷名或容器健康判断可用。

Runtime 查询携带内部不可变绑定，Service 将实际创建令牌、镜像、卷名和持有事务与数据库比较，相关字段不新增为 HTTP 响应。资源完整性与删除归属分开：工作区缺失时查询和恢复不能宣称正常，但实例删除可在确认准确卷名不存在后继续清理合法残留；存在同名外来卷或标签冲突时仍拒绝操作，不重建缺失卷。

工作区删除先持久化认领为 deleting，再核验精确身份、空占用及实际无容器引用，普通删除并确认消失后才清理元数据。失败保留 deleting 可重试；pending 不自动接管。显式绑定的实例不能使用 delete_workspace=true，默认删除及回收只释放占用，数据永久删除必须独立调用工作区接口。私有卷路径与历史残卷保持原策略，不自动导入、迁移内容或复用。

首次创建固定主镜像 ID；后续环境解析必须得到相同 ID。权限与初始文件只初始化一次，重绑定不复制、不 chown、不清空内容。该能力保存文件数据，不保存进程、系统包、网络会话或进程内存，仍以本机单进程和可信 Docker 管理者为前提。

### 显式 schema 升级

先停止所有使用目标数据库的 Control，备份该库及相应 SQLite 状态文件，并持有同一路径的运行锁。仅对完整核验通过的本项目 schema 2 或 3 显式升级到 4；保留原生命周期、工作区和占用身份、时间及版本，不为旧记录生成凭据或导入私有卷。schema 4 的重复 upgrade 只核验；不接受未知 schema 或额外对象。验证使用临时数据库，本轮不升级真实开发库。

```python
import asyncio
from pathlib import Path
from control_plane.storage.lifecycle_store import SQLiteLifecycleStore
from control_plane.storage.run_lock import SQLiteRunLock
from control_plane.core.tasks import settle_task

async def upgrade_database(database_path: Path) -> None:
    """Upgrade one stopped, explicitly selected database while holding its process lock."""
    lock = SQLiteRunLock(database_path)
    lock.acquire()
    try:
        upgrade = asyncio.create_task(SQLiteLifecycleStore(database_path).upgrade())
        await settle_task(upgrade, propagate_cancellation=True)
    finally:
        lock.release()

# Replace only after stopping Control and backing up the selected database.
# asyncio.run(upgrade_database(Path("/absolute/path/to/lifecycle.sqlite3")))
```
