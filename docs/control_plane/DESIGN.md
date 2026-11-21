# Control Plane 设计与运行

本文说明 Docker 控制面、资源拓扑、存储和失败恢复。Docker 后端的模块职责与资源操作见 [Docker Runtime 设计与实现](DOCKER_RUNTIME.md)。对外协议见 [HTTP API](API.md)，启动、迁移及验收见 [运行手册](RUNBOOK.md)。

## 当前定位

`control_plane/` 是运行在 WSL 宿主侧的独立 Python/FastAPI 进程，与容器中的 Go Agent 分离。当前提供 Docker 生命周期与就绪探测、受约束规格及可选精确域名出口创建、状态和动态入口查询、受控 HTTP 服务入口交付、实例主动就绪检查、显式续期、手动删除及到期自动回收。命令、文件和 SSE 由 [Sandbox Agent API](../sandbox_agent/API.md) 提供；Control 只核验并交付入口，不转发这些调用或应用 HTTP 数据。

Control 不接管现有 Compose Agent、`agent-sandbox-internal` 网络或 `agent-sandbox-workspace` volume。当前运行前提是单进程、单 worker，SQLite 控制面通过系统文件锁限制同一数据库只能运行一个应用实例。单管理员 Bearer 认证可在 `.env` 中启用。控制面没有多租户授权、审计、公开回收或 pending 恢复 API、自动续期客户端、整机资源预算或容量准入保证。完整 pending 实例可受控确认，默认关闭启动恢复。它是本机开发控制面，不能据此宣称具有完整平台能力或已经验证的安全隔离能力。

## 进程与信任边界

控制面绑定 `127.0.0.1`。Docker daemon 运行在可信宿主，Docker socket 仅向控制面开放。入口代理连接实例的两张网络，动态发布一个 loopback 端口，向 Agent 或受约束应用端口转发。Agent 与用户进程共用容器身份。工作区是显式可写挂载，资源标签用于归属判断。系统信任边界见 [系统架构](../ARCHITECTURE.md)。

## 控制面认证与启动配置

`create_app()` 通过 `ControlPlaneSettings` 从固定的 `control_plane/.env` 读取启动配置，缺项或空值使用默认值。读取时不导出文件内容到进程环境。配置格式和默认值见 [环境文件模板](../../control_plane/.env.example)。

标准入口 `control_plane.app:app` 和直接调用 `create_app()` 都默认关闭管理员认证。在 `.env` 设置 `AGENT_SANDBOX_AUTH_ENABLED=true` 即可开启认证。开启时读取 `AGENT_SANDBOX_API_KEY`，值必须为 1～128 个 ASCII `[A-Za-z0-9_-]` 字符。调用终端使用相同值。

启用认证但缺少 key 时，lifespan 在取得运行锁、初始化 SQLite、pending 恢复或启动回收前拒绝启动。非法配置在装配时抛出固定错误，错误与配置 `repr` 会隐藏 key。认证配置在装配时固定，key 只用于 HTTP 认证。

纯 ASGI 认证中间件只接受单个严格 Bearer header，用常量时间比较核验凭据。未授权请求在读取 body 或业务处理之前返回固定 401，只有精确 `GET /healthz` 可匿名访问。两种认证设置下均关闭 `/docs`、`/redoc` 和 `/openapi.json`。公开协议见 [API 认证](API.md#认证)。

默认测试隔离开发用 `.env`。认证测试显式启用认证，并覆盖缺失及无效 key 的场景。

管理员 key 只用于控制面；新动态入口另用每实例随机 Token。SDK 显式区分请求角色，即使同源也不向 Agent 发送管理员 key。主动健康探测使用匿名精确 GET /healthz。当前仍限可信本机，没有多租户授权、轮换、TLS 或审计，也不保护容器内部 Agent 访问。

## 分层与装配

`create_app()` 在读取配置后创建 `DockerRuntime`、`SQLiteLifecycleStore` 和 `HTTPAgentHealthProbe`。目前 `AGENT_SANDBOX_RUNTIME` 只接受 `docker`。导入和装配不打开 Docker 或 SQLite。默认数据库为 `control_plane/.local/lifecycle.sqlite3`，`.env` 中的相对数据库路径以 `control_plane/` 为基准。启动配置见 [运行手册](RUNBOOK.md)。

HTTP 层负责请求解析、认证、响应和错误映射，通过 `SandboxService` 进入业务流程。服务依赖 Runtime 和 Store 契约，具体实现由应用装配。领域模型依赖标准库和同层模块。配置独立读取与校验，由应用传给服务和适配器。完整文件映射见 [目录与职责](../REPOSITORY_LAYOUT.md)。

`SandboxService` 持有创建准入和每实例操作锁，编排暂停、恢复、续期和删除。`SandboxCreation` 预写原事务的 pending 记录、入口凭据和可选工作区占用，再创建 Docker 资源并确认 active。创建失败后依资源状态选择补偿或保留恢复依据，准入在收尾完成后释放。绑定独立工作区时，从绑定核验到补偿都由同一实例锁保护。手动删除、回收及创建补偿共用原事务与版本的元数据守卫。

`SandboxQueries` 核对运行时观察与 SQLite 元数据的身份，并合并期限或探测 Agent 就绪。`EndpointAccess` 在核验原事务和代理凭据摘要后交付入口。`WorkspaceService` 编排独立卷操作，SQLite Store 在同一连接和事务内更新实例、凭据及占用。`SandboxMaintenance` 调度回收与启动时受控 pending 恢复，过期删除通过 `SandboxService` 的实例锁和认领流程执行。启动恢复在接收请求前运行，通过原版本 CAS 确认 pending。

启动顺序为管理员 key 核验、取得 RunLock、初始化 v7、可选的单批 pending 恢复、启动回收、接收 HTTP。关闭时等待回收的当前候选收尾，再释放锁。根 `/readyz` 只执行只读 Docker `ping()`；ping transport timeout 为 0.5 秒，其他 Docker 操作为 5 秒。根就绪与实例只读查询配置各自独立的默认 1 秒等待预算。同步客户端在各自线程内创建与关闭，等待预算可能包含收尾耗时。

### SQLite 单实例运行保护

运行锁身份取自 `SQLiteLifecycleStore` 使用的实际数据库规范路径。伴随锁文件位于数据库旁，例如 `lifecycle.sqlite3.lock`。同一数据库的不同应用对象及不同进程互斥，不同数据库可分别启动。SQLite 没有关闭该保护的开关。

Linux/WSL 使用标准库 `fcntl` 系统锁，Windows 使用 `msvcrt` 系统锁，按平台加载；不支持的平台明确拒绝启动。获取为非阻塞尝试：锁已被持有时立即返回稳定启动错误，不初始化数据库、调用 Docker 或启动回收任务。权限、路径和文件系统错误与锁竞争分别报告，错误与日志不包含完整路径、凭据或底层异常文本。

锁由独占文件句柄持有，句柄不传给无关子进程。锁文件存在、PID 或文件内容都不能证明锁仍被持有；锁文件可以在无持有者时保留。正常关闭在服务请求收尾和回收任务结束后显式解锁、关闭句柄，不删除锁文件，也不提供强制删除锁文件或终止占用进程的解锁方式。进程异常退出后由操作系统释放锁，后续实例可重新获取。

初始化失败和启动取消也必须释放锁，但已经开始的初始化或回收关闭工作须先通过共用关键任务等待工具完成，首次和重复取消均不能提前放锁。不持有长期 SQLite 写事务来实现保护，因此运行锁不阻塞 Store 正常的独立连接与短事务。

保护限于本机、本地文件系统及统一数据库路径，要求控制面遵守同一锁协议；不承诺网络文件系统、硬链接别名或 Windows/WSL 跨系统共享锁语义，也不能阻止外部程序直接操作数据库或 Docker。它不是分布式租约、多 worker 协调或 exactly-once 保证。取得锁不证明旧 Docker 请求已经结束，也不自动激活或删除 pending；开启确认恢复仍须满足下述独立准入条件。

<a id="topology"></a>

## 独占资源与两网拓扑

默认动态 sandbox 独占下列五项资源，启用非空出口白名单时增加第六项 `egress` 容器。实例 ID 为 `sbx_` 加 32 位小写十六进制字符；名称用于固定拓扑和诊断，资源归属须由完整标签确认。

| 角色标签 | Docker 类型 | 名称 |
| --- | --- | --- |
| `network` | internal bridge | `agent-sandbox-<sandbox_id>-internal` |
| `ingress` | 普通 bridge | `agent-sandbox-<sandbox_id>-ingress` |
| `workspace` | 命名 volume | `agent-sandbox-<sandbox_id>-workspace` |
| `container` | sandbox 容器 | `agent-sandbox-<sandbox_id>` |
| `proxy` | 入口代理容器 | `agent-sandbox-<sandbox_id>-proxy` |
| `egress`（可选） | 出口代理容器 | `agent-sandbox-<sandbox_id>-egress` |

`container/network/workspace` 保留原角色含义，以兼容旧实例；`proxy/ingress` 用于专属动态入口。

sandbox 只连接专属 internal 网络，DNS 别名固定为 `sandbox`，不发布宿主端口。proxy 创建时连接专属 ingress，随后按精确网络 ID 接入 internal，两网连接完成后才启动。实例之间不共享 ingress；workspace 只挂载给本实例 sandbox。

proxy 通过 `port_bindings={8080: ("127.0.0.1", None)}` 请求 Docker 分配动态端口，避免应用先探测再绑定的竞争。创建时不预占本机端口；入口查询从启动后的实际发布结果恢复地址。该地址是本机 WSL 开发地址，不承诺远程访问或任意用户服务端口暴露。

### 资源上限与固定安全配置

创建 API 只允许服务端注册的环境（默认 basic/python/node）、受约束的 CPU、内存、初始 TTL 和精确域名出口白名单，不允许指定任意镜像、命令、环境变量、挂载、网络或其他 Docker 参数。要求所选环境的主镜像和固定入口镜像预先存在，仅非空白名单额外要求出口镜像，Runtime 不自动 pull/build 或回退；预检后使用主镜像 ID 创建，避免标签变化。

| 配置 | sandbox | proxy |
| --- | --- | --- |
| 镜像 | 默认 basic: `agent-sandbox-basic:dev`；python: `agent-sandbox-python:dev`；node: `agent-sandbox-node:dev`；可信装配可追加映射 | `agent-sandbox-proxy:dev` |
| 用户 | `sandbox` | `nginx` |
| CPU 上限 | 默认 1 CPU，允许 0.1～2 CPU | 固定 0.5 CPU |
| 内存及内存加 swap 上限 | 默认均为 256 MiB，允许 128～2048 MiB | 固定均为 64 MiB |
| PID 上限 | 64 | 32 |
| 根文件系统 | 只读 | 只读 |
| `/tmp` tmpfs | 32 MiB | 16 MiB |
| capabilities | drop ALL | drop ALL |
| security option | `no-new-privileges:true` | `no-new-privileges:true` |
| init | 开启 | 开启 |
| workspace | 可写挂载到 `/workspace` | 无挂载 |
| 宿主端口 | 不发布 | loopback 动态发布 8080 |

`memswap_limit` 与 `mem_limit` 相同，表示不提供额外 swap 额度。两容器 `/tmp` 都使用 `rw,nosuid,nodev,mode=1777`，并设置上述大小上限。

领域模型使用整数 `cpu_millis` 和 `memory_mib`，仅在 Docker 后端映射为 `nano_cpus = cpu_millis × 1_000_000` 及 `mem_limit = memswap_limit = memory_mib × 1024 × 1024` 字节。默认实际限制保持 1 CPU、256 MiB、无额外 swap。资源上限不表示预留或全局容量保证；不提供整机预算、容量准入或运行中资源调整。SQLite 不新增资源字段，查询响应不返回资源配置。

[Agent Dockerfile](../../sandbox_agent/Dockerfile) 仅从 `sandbox_agent/` 构建上下文复制 Go 服务，使用非 root 用户和镜像 HEALTHCHECK。[Dockerfile.proxy](../../deploy/local/Dockerfile.proxy) 打包动态入口认证模板和启动脚本，以 nginx 用户在私有 tmpfs 生成受限配置；不修改只读根文件系统。固定 Compose 继续直接挂载原 [proxy.conf](../../deploy/local/proxy.conf)，不会获得本轮认证。健康检查通过精确匿名 GET /healthz 访问内部 Agent。

维护者通过 `RUNTIME_BASE_IMAGE` 构建三个独立镜像，默认 basic 使用 Alpine 3.22，Python 与 Node 使用固定版本的 Alpine 3.23 语言基础镜像；共用同一 Agent 构建和运行配置，覆盖继承的 ENTRYPOINT/CMD。此参数不能从 API 传入。标准库、内置模块及自带 pip/npm 经过现有执行接口使用，不在启动时联网安装包；Alpine/musl 不保证任意 glibc 二进制或第三方依赖兼容。HTTP 任意镜像和动态 Agent 注入仍为后续方向，具体构建版本见[预制镜像说明](RUNBOOK.md#preset-images)。

维护者可在应用装配时追加最多 32 个自定义环境，保留内置映射；Runtime 复制、冻结并公开只读 `environment_images`，创建仅按该实例所选名查找镜像并预检后固定镜像 ID。映射不会保存到 SQLite，移除映射不影响已存在资源的生命周期管理。

自定义镜像属于可信运维输入：须预装可执行 `/usr/local/bin/agent`、`sandbox` 用户、可用 `/workspace`、8080 `/healthz` 服务及 wget，不得声明 `/workspace` 之外的 VOLUME。Runtime 为所有主容器显式设置 Entrypoint `["/usr/local/bin/agent"]`、Cmd `["-listen", "0.0.0.0:8080"]`，继续固定用户、工作目录、挂载和安全上限，并显式设置 wget 健康检查，覆盖镜像原业务启动与健康配置。注册并不校验镜像内全部文件、依赖或威胁模型；动态复制 Agent、启动安装包以及任意基础镜像兼容性均未实现。示例从既有 Agent 镜像派生并仅复制标记文件，见 [自定义环境](RUNBOOK.md#custom-environments)。

代理上游固定为 `sandbox:8080`，请求不能指定上游。`/v1/files` 关闭请求与响应缓冲，并将代理并发连接限制为 2；代理容量拒绝返回固定的 429 JSON。`/v1/commands:stream` 关闭响应缓冲，保留实时 SSE；代理读写超时为 40 秒，请求体上限为 11 MiB，临时目录和 PID 文件位于 `/tmp`。Agent 自身的命令、文件限制和协议见 [Agent API](../sandbox_agent/API.md)。

### 与固定 Compose 开发链路的关系

[compose.yml](../../deploy/local/compose.yml) 是已有的固定开发链路：入口为 `127.0.0.1:18081`，使用固定容器名、外部 internal 网络与外部 workspace，proxy 从 `nginx:alpine` 运行并只读挂载配置。动态 Runtime 使用独立五资源或六资源、打包的代理镜像和动态端口；不接管或重建这些固定开发资源。Compose proxy 未设置与 Runtime 相同的 `memswap_limit`，不能将上表直接当作 Compose 配置清单。开发脚本从自身位置定位 Compose 文件，不依赖调用者当前目录。

<a id="network-policy"></a>

## 可选出口与不可变策略标签

`network.allow_domains` 省略或为空时不创建出口、不预检出口镜像、不注入代理环境变量。非空时固定使用 `agent-sandbox-egress:dev`，预检后保存镜像 ID，不 pull/build、不接受任意镜像或代理地址。策略和规范化规则见 [API](API.md#4-创建-sandbox) 与 [出口设计](../egress_proxy/DESIGN.md)。

出口配置固定为用户 `10001:10001`、0.5 CPU、128 MiB 内存及同值 memory+swap、32 PID、只读根文件系统、drop ALL、`no-new-privileges:true`、无挂载和宿主端口。它只接本实例已有的 ingress 与 internal，internal 别名固定 `egress`；启动参数为逐项重复的 `-allow-domain`。仅 sandbox 注入大小写两组 HTTP(S)_PROXY=`http://egress:18084` 及 NO_PROXY=`localhost,127.0.0.1,::1`；入口和出口代理自身不注入这些变量。sandbox 的单 internal 连接、资源上限、workspace 及入口发布规则不变；本地健康检查显式禁用环境代理，兼容说明见[实现文档](RUNBOOK.md)。

所有新建实例自有资源，包括禁用出口的实例，都保存以下相同策略标签；引用的独立卷保持原独立身份：

```text
io.agent-sandbox.network-version=1
io.agent-sandbox.allow-domains=["example.com","iana.org"]
```

数组必须是无空白的紧凑 JSON，内容为已规范化、去重、排序的域名；禁用时为 `[]`。这些标签不属于五项归属标签，不在续期或恢复时修改。两项均不存在是旧五资源实例；仅缺一项、未知版本、非法或非规范 JSON、跨资源不一致、旧实例出现 egress 或空策略出现 egress 均不能降级为禁用出口。状态和只读资源核验遇到异常闭合失败，完整 pending 只在预期角色齐全时才可确认。

启用策略的状态 ready 还要求唯一且完整归属的 egress 运行、Docker health 为 healthy，并连接本实例精确两网及内部别名；缺失、停止、不健康分别返回 `failed` 和 `egress_missing`、`egress_stopped`、`egress_unhealthy`；health 正在启动返回 `starting/egress_starting`，策略、归属或拓扑异常则查询失败。检查不访问 DNS 或公网，healthy 不保证白名单目标可达。endpoint 同样核验已发现资源的策略一致性，但缺失或停止的 egress 不单独阻止入口可用；readyz 先核验状态，因此出口故障时返回未就绪。没有自动重建、网络切换或直连回退。

正式删除发现全部六个可能角色，回收授权仍只依据精确 ID 和完整归属。策略标签缺失或损坏不会阻止回收合法残留，跨事务、重复角色和归属变化继续拒绝；默认保留 workspace，显式删卷和重复删除保持原语义。出口策略仅约束代理自身发起的 IPv4 TCP 80/443，不覆盖 DNS、UDP、IPv6、宿主机访问或允许站点的二次转发，不能据此宣称完整安全隔离。

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

默认存储为 `control_plane/.local/lifecycle.sqlite3`。schema **7** 包含生命周期、凭据、独立工作区及其唯一占用。初始化只支持空新库与经结构核验的同版本库。

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
4. Runtime 成功后，Service 按原事务 ID 和版本原子激活记录。只有激活也成功才返回 HTTP 200 与 `started`；这不等待 Agent 或 proxy 健康检查。
5. Runtime 明确证明未修改资源或已完成失败回滚时，Service 按原事务 ID 和版本补偿预写记录。有残留或结果未知时保留记录和 sandbox ID，不报告成功。

SQLite 与 Docker 没有跨系统原子事务。SQL 回滚不能撤销 Docker 操作，数据库有记录不证明实例已经创建成功或仍存在。激活失败可能发生在 durable commit 之后，版本结果未知时保留记录与资源用于诊断。

创建额度由每个 Service 自己持有，检查与占用之间没有异步让出点，当前事件循环内的竞争调用不会突破上限；满额无等待队列。额度从准入持续到本次操作及必要收尾结束，包含预写、创建、确认、晚到成功后的补偿，以及首次和重复取消时的关键任务等待，再由 `finally` 释放一次。不通过另一个无人等待的后台创建任务缩短额度生命周期。明确报告不确定结果并完成本次收尾后释放额度，残留 pending 及诊断依据继续保留，不永久占用创建额度。

查询、健康、endpoint、续期、删除和回收不受创建额度限制。生产装配为一个应用持有一个 Service，不使用全局计数、跨线程锁或分布式协调；不同 Service 的额度独立。创建并发准入不限制已创建实例数、不预留资源、不限制请求速率，也不提供整机容量保证或跨应用、进程、worker 的总量约束。

创建参数的默认值、范围和严格类型规则集中在 领域模型：环境名默认 basic，必须是匹配 `[a-z][a-z0-9-]{0,31}` 的精确 ASCII 字符串；模型只检查语法，Service 在创建准入前检查实际注册集合，领域模型不包含镜像地址；TTL 默认 1800、范围 1～86400 秒；CPU 默认 1000、范围 100～2000 毫核；内存默认 256、范围 128～2048 MiB。Service 直接调用同样校验。API 不接受任意 Docker 参数，非法配置返回 400，实际体积超限返回 413，两者均无生命周期写入或 Runtime 调用。环境和资源配置只用于创建传递，不增加 SQLite 字段或创建/查询/endpoint 响应字段；续期不调额或换环境，pending 确认恢复不重置初始期限。CPU/内存限制是单实例上限，不是预留或全局容量保证，尚无整机预算或动态调额。

### 受管实例总量

实例名额直接由 `lifecycle_records` 表示，没有额外计数器表或内存总量计数。普通和显式绑定创建分别通过 `Store.create(..., max_instances=...)`、`create_with_workspace(..., max_instances=...)`；两者在同一写事务内执行数量检查及插入，工作区占用、版本和凭据一起提交或回滚，不在 SQL 事务内访问 Docker。独立连接竞争最后一个名额时由 SQLite 串行决定，不能先在 Service 查询数量再插入。

所有 pending/active/deleting、旧记录、暂停、停止和到期未清理实例均占名额。成功创建只释放本次创建并发额度，不释放实例名额；结果不明或清理失败同样保留名额。只有原事务／版本保护的生命周期删除成功才释放，凭据和工作区占用沿原事务清理。取消仍等待已开始的 SQLite/Runtime 操作与关键收尾；迟到提交或确认丢失不靠内存计数猜测结果。

Store 以独立 `LifecycleInstanceLimitExceeded` 表示预期的满额结果，Service 映射为 `SandboxInstanceLimitExceeded`，HTTP 固定 429 `sandbox_instance_limit_exceeded`；不被元数据不可用或工作区冲突分支吞掉。SQLite busy、损坏和 I/O 错误保留原元数据错误。被取消的等待若最终确认未获名额，则按原取消语义结束，不清理其他事务。

重建应用从已有记录自然恢复；降低上限只阻止创建，不驱逐实例，也不限制查询、续期、暂停／恢复、删除和回收。独立工作区、保留卷、无记录遗留和其他数据库不在计数范围；仍无整机 CPU/内存预算、磁盘配额、租户配额或分布式调度。实例总量按现有生命周期记录计数。

### 可信自定义环境注册

配置项 `AGENT_SANDBOX_ADDITIONAL_ENVIRONMENTS` 接受维护者提供的名称到镜像引用的 JSON 对象，默认注册 basic/python/node。最多追加 32 项，不得覆盖内置名称；名称遵循创建规格语法，镜像引用采用受限 Docker reference 语法：ASCII 总长不超过 512，小写仓库路径不超过 255；可带合法 tag、DNS/localhost registry 与 1～65535 端口、`sha256` 摘要。拒绝 URL、凭据、空白/控制字符、Unicode、IPv6 registry 和大写仓库名；完整格式由 Docker 后端本地校验，不访问 registry。装配立即校验、复制并冻结配置，不访问 Docker、镜像或数据库，不提供动态注册、持久化或 HTTP 配置入口。

Docker 装配从 Runtime 实际只读 `environment_images` 派生 Service 的名称集合；注入已有 Docker Runtime 时也以其实际映射为准。如同时显式传入应用映射，两者必须一致，否则装配抛出 `ValueError`，避免 HTTP 接受而运行时拒绝。独立构造 Service 可传入合法非空的有界 `environment_names` 子集，最多 35 条输入，复制后冻结；不再要求全部内置名，省略时仍只接受三种内置名。未注册名称在创建并发准入、身份分配和 pending 写入前拒绝。

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

启动恢复使用以下 `.env` 配置项。

| 参数                             | 默认值    | 约束                                                                          |
| -------------------------------- | --------- | ----------------------------------------------------------------------------- |
| `AGENT_SANDBOX_PENDING_RECOVERY_ENABLED` | `false` | 接受 `true` 或 `false`。开启时持有 SQLite 运行锁。 |
| `AGENT_SANDBOX_PENDING_RECOVERY_BATCH_LIMIT` | `100` | 1～2⁶³−1 的整数。只处理一个有界批次。 |

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

`GET /v1/sandboxes/{id}/services/{port}/endpoint` 沿 API → `SandboxService.get_service_endpoint()` → 原 `get_endpoint()`/Runtime/Store 读取链路，先校验真正整数端口 1024～65535 且排除 8080，再复用实例归属、事务、创建时间、loopback 发布、网络和凭据核验。HTTP 层校验规范十进制路径字段并设置 `Cache-Control: no-store`；Service 返回独立 `SandboxServiceEndpoint`，包含请求实例 ID、应用端口、固定服务 URL、`auth_required=true` 和隐藏 repr 的原实例 Token。不新增 Runtime 方法、数据库字段、应用探针或数据 HTTP 转发逻辑。

动态镜像继续使用 `agent-sandbox-proxy:dev`，预检同时要求 `io.agent-sandbox.proxy-auth-version=1` 与 `io.agent-sandbox.proxy-services-version=1`，固定本轮实际镜像 ID 创建。新实例自有资源写入 `io.agent-sandbox.endpoint-services-version=1`，独立复用卷不修改；入口恢复从存留自有角色和代理实际 Image ID 核验一致性。资源与镜像均缺少服务标记表示旧能力；仍支持原认证命令、文件接口，但新服务接口返回 409 `sandbox_service_unsupported`，固定提示 `proxy does not support service forwarding; recreate sandbox`。未知或矛盾能力标记不作为旧版本降级放行。能力标签不是删除授权，清理仍以精确资源 ID 和完整归属为准。

调用方直接访问 `http://127.0.0.1:<入口端口>/services/<应用端口>/`，入口将原始 URI 的前缀剥离后转发固定 `sandbox:<port>`。端口只能来自规范受限路径，不能指定 host、协议、其他实例或任意代理目标；点段、编码路径分隔符、反斜杠、绝对形式与 Upgrade 被拒绝。所有服务路径包括应用 `/healthz` 均要求实例 Token。代理移除实例认证和 Forwarded 等平台控制头，忽略并隐藏上游 X-Accel 控制响应，禁用请求重试和重定向改写，限制每实例应用并发 8、请求体 10 MiB、请求头缓冲 32 KiB、建连 2 秒、上游读写空闲 30 秒、下行空闲 40 秒。完整方法、失败响应与传输限制见 [服务入口 API](API.md#查询受控-http-服务入口)。

返回连接只证明原入口及能力核验通过，不证明应用正在监听或健康；实例 readyz 仍只检查 Agent。应用需监听容器内 `0.0.0.0` 并自行兼容公开路径前缀、重定向和 Cookie，普通 HTTP 转发不保证任意网页可用，也不提供 WebSocket 或原始 TCP。Token 仍能授权同实例 Agent 执行，不提供按端口授权；管理员 key 不发送到应用。Python SDK 以独立 `ServiceConnection` 交付凭据，不扩大 `EndpointConnection` 或新增应用请求封装。

同步命令和前台 SSE 最长 30 秒。后台任务可显式设置最长 24 小时，其有效期仍受实例生命周期约束。应用受实例暂停、TTL 和删除约束，代理不逐请求查询 TTL，也不在暂停时瞬时撤销 Token。更新同名镜像只影响后续创建，不改固定 Compose、schema 或已有实例；需要新能力时显式重建，不能按新标签推断旧容器已更新。

### 实例主动就绪检查

`GET /v1/sandboxes/{id}/readyz` 经 `Service.check_ready()` 依次调用 `Service.get()`、现有入口核验和可注入的 Agent HTTP 探针。状态查询包含实际 Runtime 与 SQLite 身份核对；状态不是 ready 时停止，不查询入口或发起 HTTP。入口返回的 sandbox ID 必须匹配请求，地址还须通过探针的严格校验。该流程不新增 Runtime 方法、不修改管理状态、期限或资源，也不改变 pending 恢复的准入条件。

`HTTPAgentHealthProbe` 每次请求延迟创建并关闭 HTTPX client，只允许 `http://127.0.0.1:<有效端口>`（可带根路径 `/`），固定 GET `/healthz`。拒绝其他主机、用户信息、异常路径、query 和 fragment，禁用环境代理、重定向与重试；要求 HTTP 200 和健康 JSON `{"code":200,"message":"ok","data":{"status":"ok"}}`。以 raw stream 有界读取最多 4 KiB，拒绝非 identity 编码，不触发自动解压。

API 的 `sandbox_query_timeout`（默认 1 秒）覆盖状态、Store、入口和 HTTP；探针另设默认 0.5 秒的网络及整体探测超时，防止慢速分块无限延长读取。探针失败为 503，`data.reason` 为 `sandbox_agent_unavailable`。API deadline 为 504，原因为 `sandbox_readiness_timeout`。尚未就绪为 503，原因为 `sandbox_not_ready`；领域错误继续集中映射。取消直接传播到 HTTP，只对响应及连接关闭使用共用关键任务等待，重复取消也须读取收尾结果。底层异常和响应体不进入公开错误。

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

Runtime 达到删除目标后，Service 才按认领事务和版本删除元数据。默认保留 workspace 时同样移除已结束实例记录；残卷继续按完整 Docker 标签和显式 `delete_workspace=true` 管理。Docker 失败保留 `deleting`；资源已删除但元数据收尾失败或版本冲突时保留 sandbox ID，允许同 ID 重试，不盲目刷新版本。合法 ID 无目标、重复删除或默认删除只剩保留卷均可返回 HTTP 200，`data` 为 `null`。

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

[`common/async_tasks.py`](../../control_plane/common/async_tasks.py) 使用 `asyncio.wait()` 等待任务，避免将等待者取消直接转发给 worker，也避免 Python 3.14 取消 shield 后对迟到异常的额外报告。`wait_for_task()` 保留可收尾的 worker，`wait_for_completion()` 穿过重复取消并返回首次取消信号，`settle_task()` 读取最终结果后按调用方策略传播取消。Service、Runtime 与 reaper 共用等待动作，但保留各自业务补偿和错误优先级。

- 预写期间取消：等待 SQLite 最终结果；已提交但 Runtime 尚未启动时，按原事务和版本删除元数据。冲突或补偿失败不能删除替代记录。
- Runtime 创建期间取消：向创建协程发出取消，由 Runtime 在线程步骤间设置取消标志，等待精确回滚；创建赢得竞争而迟到成功时，显式补偿本事务资源。只有确认清理完成才清理元数据并传播取消。
- 激活期间取消：等待激活最终结果，已知提交版本才执行补偿；结果未知则保留资源和记录。已确认创建的补偿先持久化 `deleting`，失败留待重试。手动删除已移除记录或认领较新版本时，迟到创建仍清理原事务资源，不重建记录、不刷新版本，也不删除替代元数据。
- 删除期间取消：Runtime 删除与其后元数据收尾构成关键操作，首次及重复取消都等待最终结果并读取后台异常。调用方未收到响应时按同一 ID 重试，以资源事实确认结果。
- 续期期间取消：等待提交结果，已提交期限不撤销。回收取消则停止后续候选，先完成当前候选从认领到清理的关键收尾。

## 后台周期回收

内部 `await service.reap_expired_once(limit=100)` 执行一轮回收，没有对应 HTTP API。每轮只读取一次 UTC 时钟，有界获取到期 `active` 与待重试 `deleting`，按过期时间、sandbox ID 稳定排序；`pending` 不作为候选。

对 `active` 候选，Store 在同一 SQLite 事务中核对 sandbox ID、事务 ID、预期版本、状态和 `expires_at <= now`，成功后转为 `deleting` 并递增版本。续期、删除或候选变化使认领条件失效时跳过，不调用 Runtime；`deleting` 重试同样核验原身份与版本。随后复用受保护删除，默认 `delete_workspace=False`，成功后才清理元数据。

单项 Runtime 或收尾失败保留 `deleting`，不妨碍本批其他候选，每项本轮最多尝试一次；数据库整体不可用停止本轮，不降级为盲目删除。取消停止本批后续候选，等待已开始操作收尾。结果包含 `now`、`items`、`error` 及计算得到的 `succeeded/skipped/failed` 计数；每项含 `sandbox_id`、`outcome` 和稳定 `reason`。成功为 `deleted`，候选失效为 `candidate_changed`，其他失败使用稳定领域错误码；整体存储失败为 `sandbox_metadata_unavailable`。

后台调用链为 `FastAPI lifespan → services/sandbox_maintenance.py → SandboxService.reap_expired_once()`。循环只调度，不重复判断过期条件。回收循环使用以下 `.env` 配置项。

| 参数                        | 默认值   | 约束与语义                                   |
| --------------------------- | -------- | -------------------------------------------- |
| `AGENT_SANDBOX_REAPER_ENABLED` | `true` | 接受 `true` 或 `false`。控制是否在 lifespan 启动任务。 |
| `AGENT_SANDBOX_REAPER_INTERVAL_SECONDS` | `60.0` | 有限正数。每轮完成后等待的秒数。 |
| `AGENT_SANDBOX_REAPER_BATCH_LIMIT` | `100` | 1～2⁶³−1 的整数。每轮候选上限。 |

配置在装配时校验。数据库初始化成功后启动唯一后台任务，第一轮在后台立即执行，不等整轮结束才提供 HTTP 服务。同一应用实例的各轮串行；无候选、满批次和普通失败后均等待完整间隔，避免忙循环。普通测试显式关闭回收，专门验证该行为时才开启。

循环检查单轮 `error`、`items` 及普通异常，记录处理计数和稳定失败原因，失败不会永久停止后续轮次。应用关闭时取消并等待任务，不再启动下一轮；重复取消不能跳过当前关键收尾，也不使用短超时假定工作线程已经退出。

当前仅支持单进程、单 worker，同库运行锁不提供分布式互斥、租约或 exactly-once 保证。扫描间隔、单轮耗时和失败重试可能延迟删除，不承诺到期瞬间回收。强制终止后，下次启动继续读取持久化的到期 `active` 和 `deleting`；pending 只在显式开启的一轮启动恢复中按严格条件确认，不自动回滚或清理，无记录旧资源不会自动接管，保留 workspace 不自动清理。

## 运行与验证

镜像、启动、数据库升级、验证命令和环境限制见 [运行手册](RUNBOOK.md)。

## 常见问题与恢复边界

### 启动提示同库实例正在运行

同一数据库只能由一个遵守锁协议的控制面实例运行。正常停止已知实例并等待服务与回收收尾后重试，或为独立实例配置另一数据库；更换端口不能解除冲突。不要删除伴随锁文件或自动终止其他进程。锁文件存在但没有持有者不会阻止启动；权限和文件系统错误应按对应的稳定启动错误排查。

### 进程健康但 Docker 不可用

`/healthz` 正常而 `/readyz` 为 503 表示进程能处理请求，Docker 连接当前不可用。用 `docker version`、`docker info` 检查 daemon 与当前用户访问权限，不通过放宽 sandbox 权限或挂载 socket 修复。可在单独启动的测试进程设置不存在的 `DOCKER_HOST`，验证健康仍为 200、就绪为脱敏 503；不要停止 Docker 或改动既有资源。

### 元数据初始化失败或查询冲突

核对配置的数据库路径、可写性、schema 兼容性和所用数据库是否与该批实例一致。不兼容的数据库会阻止启动并保留原内容。查询身份冲突不应通过覆盖记录或回退标签隐藏；按响应中的 sandbox ID 核验实例、事务和创建时间。

### 创建、取消或删除没有收到最终响应

取消、连接断开和超时不能证明写入或资源操作没有发生。创建可能保留 `pending` 或未知结果，须按 sandbox ID 核验；默认不恢复 pending，可在受控启动阶段开启完整实例确认，跳过项仍需后续判断。删除对同一 ID 重试，并遵守精确身份和完整归属，不使用名称前缀批量删除或 `docker prune`。续期及确认恢复则通过后续查询核对实际记录，不根据响应缺失反向撤销。

### Agent ready 但入口不可用，或到期后仍可查询

`ready` 只描述容器内部 Agent 健康；endpoint 独立核验 proxy、两网和实际端口。入口暂不可用可在有限期限内重试并按 Docker 实现文档排查，不自动迁移旧实例。计划期限与实际删除之间存在扫描、处理和重试延迟；检查回收稳定日志、持久化管理状态及是否以单 worker 运行。默认删除和到期回收留卷，应记录 sandbox ID 并单独决定数据保留时长。

### 已取得服务 URL，但应用请求失败

## 显式工作区及持久化独占

显式工作区是独立对象，不属于实例创建事务自有资源。Service 经 Runtime 固定所选环境镜像 ID，写入 pending 工作区身份，再初始化卷并确认 available；失败与取消结果不明时保留身份用于诊断，不猜测或接管未知卷。创建令牌、精确卷名和镜像 ID 持久化，公开 API 只提供 ID、状态、持有者和创建时间。

schema 7 通过 `lifecycle_records.workspace_id` 的可空外键和唯一约束表达独立工作区占用。在同一个读事务中关联 `workspaces` 重建 `WorkspaceBinding`。工作区身份和占用期间的版本由触发器保护。占用实例的 pending 写入与工作区版本递增共享一个 `BEGIN IMMEDIATE` 事务。删除时以原实例事务和版本核验持有者，并将删除实例记录与递增工作区版本一起提交。创建失败、取消、删除失败、pending 核验和控制面重启仍通过实际生命周期记录保留占用。

同一 Service 的每实例锁覆盖显式绑定的创建关键阶段与删除，避免 pending 创建仍在进行时被误当作空实例回收。Runtime 的 expected_workspace 独立于生命周期版本，绑定即使主容器已不存在也必须核验。查询、续期、暂停/恢复、pending 核验及恢复比对数据库绑定与实际挂载，不能只凭卷名或容器健康判断可用。

Runtime 查询携带内部不可变绑定，Service 将实际创建令牌、镜像、卷名和持有事务与数据库比较，相关字段不新增为 HTTP 响应。资源完整性与删除归属分开：工作区缺失时查询和恢复不能宣称正常，但实例删除可在确认准确卷名不存在后继续清理合法残留；存在同名外来卷或标签冲突时仍拒绝操作，不重建缺失卷。

工作区删除先持久化认领为 deleting，再核验精确身份、空占用及实际无容器引用，普通删除并确认消失后才清理元数据。失败保留 deleting 可重试；pending 不自动接管。显式绑定的实例不能使用 delete_workspace=true，默认删除及回收只释放占用，数据永久删除必须独立调用工作区接口。私有卷路径与历史残卷保持原策略，不自动导入、迁移内容或复用。

独立工作区使用固定 local driver 和空 Options，由隔离网络、有限额度的短生命周期初始化容器完成初始目录准备。首次创建固定主镜像 ID。绑定时禁用 Docker 的卷内容拷贝，后续环境解析必须得到相同 ID。权限与初始文件只初始化一次，重绑定不复制、不 chown、不清空内容。该能力保存文件数据，不保存进程、系统包、网络会话或进程内存，仍以本机单进程和可信 Docker 管理者为前提。
