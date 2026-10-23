# Control Plane HTTP API

本文档描述当前 WSL 宿主侧 Python/FastAPI Control 对外提供的 HTTP 接口。路由定义位于 [`api/health.py`](../../control_plane/api/health.py) 和 [`api/sandboxes.py`](../../control_plane/api/sandboxes.py)，已知领域异常的固定 HTTP 映射集中在 [`api/errors.py`](../../control_plane/api/errors.py)，应用装配入口为 [`app.py`](../../control_plane/app.py)。所有运行时调用统一经过 `SandboxService`；`/healthz` 直接返回进程健康，HTTP 层负责输入、状态码、只读超时和响应契约。架构、存储与运行说明见 [控制面设计](DESIGN.md)，Docker 资源行为见 [Docker Runtime 设计](docker/DESIGN.md)。

## 1. 服务边界

Control 与容器内的 Go Agent 是两个独立进程。Control 负责检查 Docker 可用性，以及创建、查询、暂停、恢复、续期和回收由本项目管理的 sandbox；命令、文件和 SSE 的协议见 [Sandbox Agent API](../sandbox_agent/API.md)。

本地开发默认使用：

```text
http://127.0.0.1:18083
```

Control 当前没有认证，只能监听本机地址，不得暴露到不可信网络。Docker socket 只供宿主侧 Control 使用，不能挂载进 sandbox 或入口代理。

除无响应体的请求外，接口响应使用 JSON。错误响应采用统一结构：

```json
{
  "error": {
    "code": "stable_error_code",
    "message": "stable error message"
  }
}
```

底层 Docker 异常、socket 路径、数据库路径、SQL、环境变量和调用栈不会写入错误响应。生命周期存储相关错误包含 `sandbox_id`，用于定位记录和重试。已知异常及其子类按固定映射响应，不读取异常消息生成公开文案；未知异常交由上层诊断，不保证转换为表中的某个已知错误。

默认持久化文件为 `control_plane/.local/lifecycle.sqlite3`，路径按项目文件位置确定。FastAPI lifespan 在启动时显式初始化 schema 2；不兼容旧库或初始化失败会阻止启动，不提供旧库迁移，也不回退到内存或临时数据库。导入应用、构造 Store 和 `create_app()` 不执行数据库或 Docker I/O。

## 2. 进程健康检查

### `GET /healthz`

检查 Control HTTP 进程自身是否能够处理请求。该接口不访问 Docker daemon。

请求示例：

```bash
curl -i http://127.0.0.1:18083/healthz
```

成功响应为 HTTP 200：

```json
{
  "status": "ok"
}
```

该接口返回成功不代表 Docker 可用，也不代表任何 sandbox 已就绪。

## 3. Docker 就绪检查

### `GET /readyz`

通过一次只读 Docker `ping()` 检查 Control 当前是否能够访问 Docker daemon。探测的 HTTP 整体超时默认 1 秒，Docker transport timeout 为 0.5 秒，不会创建、启动、停止或删除 Docker 资源。

请求示例：

```bash
curl -i http://127.0.0.1:18083/readyz
```

Docker 可用时返回 HTTP 200：

```json
{
  "status": "ready"
}
```

Docker 不可用、权限不足或探测超时时返回 HTTP 503：

```json
{
  "status": "unavailable",
  "error": {
    "code": "docker_unavailable",
    "message": "Docker runtime is unavailable"
  }
}
```

Docker 暂时不可用不会阻止 Control 进程启动，`GET /healthz` 仍可返回 HTTP 200。

## 4. 创建 sandbox

### `POST /v1/sandboxes`

使用固定的 Docker 安全基线创建一个由本项目管理的 sandbox。请求方只能通过 `network.allow_domains` 选择精确域名出口策略，不能指定镜像、命令、挂载、任意网络或其他 Docker 参数。

空请求体、`{}` 和 `{"resources":{}}` 均使用默认值；允许按需提供以下字段，省略的字段独立补齐：

| 字段                     | 默认值    | 允许范围与语义                                              |
| ------------------------ | --------- | ----------------------------------------------------------- |
| `environment`          | `basic` | 严格 ASCII 环境名；默认注册 `basic`、`python`、`node`，可选服务端额外注册名 |
| `ttl_seconds`          | 1800      | 整数 1～86400；从创建开始时刻计算初始期限                   |
| `resources.cpu_millis` | 1000      | 整数 100～2000；1000 表示 1 CPU 的使用上限                  |
| `resources.memory_mib` | 256       | 整数 128～2048；单位为 MiB，且不提供额外 swap               |
| `network.allow_domains` | `[]` | 最多 32 条 ASCII 域名输入；非空时启用专属出口代理 |

数值字段必须为真正的整数，拒绝 0、负数、`null`、布尔值、浮点数、字符串和越界值，不做类型转换。`environment` 必须匹配 `[a-z][a-z0-9-]{0,31}`，不接受空字符串、空白、`null`、大小写变化、Unicode 或非字符串，也不自动转换。语法合法但当前未注册的名称同样返回 400 `invalid_request`，固定文案为 `invalid sandbox creation options`；拒绝发生在创建额度、ID 分配和 pending 写入之前。顶层只允许 `environment`、`ttl_seconds`、`resources`、`network`；`resources` 必须为对象，且只允许 `cpu_millis`、`memory_mib`。未知字段、重复字段、非对象请求、非法 JSON、空白字符请求及 NaN/Infinity 均返回 400 `invalid_request`，不写入生命周期数据或调用 Runtime。

`network` 必须是对象且仅允许 `allow_domains`；省略 `network`、`network: {}` 或空数组均禁用出口代理。拒绝 `network: null`、`allow_domains: null`、非数组、非字符串成员和未知字段。域名与 Go Policy 使用相同规则：严格 ASCII DNS 标签、整体长度与标签长度受限，只允许字母数字及标签内部连字符；统一小写、允许单个末尾根点并去除，去重后排序。不 trim 空白、不做 Unicode/IDNA 转换，不接受 URL、端口、IP、通配符、空标签或下划线；白名单不包含子域名。32 条限制按规范化去重前的输入数量计算，非法请求在写 pending 前返回 400。

仅此创建接口有 4 KiB 请求体上限，按实际读取的字节计数，不依赖 Content-Length；超过上限返回 413 `invalid_request`。CPU 和内存是单实例使用上限，不是资源预留或整机容量保证。环境只能选择当前服务端注册的映射，不能指定镜像、基础镜像构建参数或 entrypoint；代理额度、实际 Docker 网络、权限及其他安全配置保持固定。

请求示例：

```bash
curl -i -X POST http://127.0.0.1:18083/v1/sandboxes

# Optional constrained settings; omitted fields retain their defaults.
curl -i -X POST -H 'Content-Type: application/json' \
  -d '{"environment":"node","ttl_seconds":600,"resources":{"cpu_millis":750,"memory_mib":384}}' \
  http://127.0.0.1:18083/v1/sandboxes

# Optional exact-domain egress; does not include www.example.com.
curl -i -X POST -H 'Content-Type: application/json' \
  -d '{"environment":"python","network":{"allow_domains":["example.com"]}}' \
  http://127.0.0.1:18083/v1/sandboxes
```

默认请求创建成功时返回 HTTP 201：

```json
{
  "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
  "status": "started",
  "created_at": "2026-10-19T08:00:00Z",
  "expires_at": "2026-10-19T08:30:00Z"
}
```

`sandbox_id` 使用 `sbx_` 前缀和 32 位小写十六进制随机值。`started` 只表示 Docker 已完成容器启动调用，不表示容器内 Agent 已通过健康检查，也不表示入口 proxy 已通过健康检查。

`created_at` 是 `SandboxService` 在写 pending、调用运行时创建前生成的带时区 UTC 时间，每次创建只读取一次时钟。`expires_at = created_at + ttl_seconds`，默认加 1800 秒；两者均使用 RFC 3339 UTC 格式，例如上述以 `Z` 结尾的时间，非零微秒会保留。创建成功后，创建响应与后续查询返回相同初始时间；查询随后采用身份匹配的 SQLite 记录中的当前过期时间。创建和查询响应不新增资源字段，SQLite 不保存资源配置；续期仅修改期限，不调整 CPU 或内存。

Service 先插入 `pending`、版本为 0 的 SQLite 生命周期记录，再调用 Runtime。数据库插入失败或 ID 冲突时不创建 Docker 资源；Runtime 成功后还需将同一事务原子确认为 `active`，版本递增，才能返回 201。确认失败或结果不明确时保留诊断记录，不返回创建成功。事务 ID、版本及 `management_state` 均为内部信息，不新增为 HTTP 响应字段。

创建规格校验通过后，Service 非阻塞申请创建额度。`create_app(max_concurrent_creates=2)` 配置单个 Service 同时处理的创建数量，默认 2，仅接受 1～16 的实际整数；这是服务端配置，创建请求不能指定。满额立即返回 429，不排队，不生成 sandbox ID、不读取创建时钟，也不写入 pending 或调用 Runtime：

```json
{
  "error": {
    "code": "sandbox_create_capacity_exceeded",
    "message": "sandbox creation capacity exceeded"
  }
}
```

额度覆盖 pending 写入、Runtime 创建、active 确认及失败或取消后的必要补偿；关键收尾完成后才释放，HTTP 等待取消不会提前释放。规格无效时仍优先返回原有 400/413；拒绝响应不含 `sandbox_id`。查询、健康、endpoint、续期、删除及回收不占创建额度。该限制不限制存量实例数、不预留 CPU/内存、不限制请求速率，也不协调其他 Service、应用、进程或 worker。

Docker 后端将 `basic`、`python`、`node` 分别映射到 `agent-sandbox:dev`、`agent-sandbox-python:dev`、`agent-sandbox-node:dev`。创建预检所选主镜像和 `agent-sandbox-proxy:dev`；仅在白名单非空时额外预检固定 `agent-sandbox-egress:dev`，并使用本轮取得的出口镜像 ID 创建，不要求其他环境镜像存在；主容器使用预检取得的镜像 ID，标签随后变化也不改变选择。缺失镜像返回 `sandbox_image_unavailable`，按原失败证据补偿 pending，不拉取、不构建、不回退。

维护者可在可信应用装配代码中调用 `create_app(additional_environments={"tools": "agent-sandbox-tools:dev"})` 追加最多 32 个名称到镜像引用的映射，不能覆盖 `basic/python/node`。配置被复制并冻结，不是 HTTP 参数、环境发现接口或任意镜像入口；镜像必须预先存在并满足 Agent 镜像契约，不得声明 `/workspace` 外的 VOLUME，预检发现不兼容挂载时返回 `sandbox_image_unavailable`。删除映射只阻止新的同名创建，不影响旧实例的查询、暂停、恢复、删除或到期回收。配置与示例见 [自定义环境](docker/IMPLEMENTATION.md#custom-environments)。

每次创建专属 internal bridge、ingress bridge、workspace volume、无宿主端口映射的 sandbox 和专属 proxy。sandbox 仅连 internal，以 `sandbox` 为 DNS 别名；proxy 连接两网，仅在 `127.0.0.1` 动态发布 8080，workspace 只挂载给 sandbox。资源带有完整的项目、sandbox ID、事务 ID、受管标记和资源类型标签。所有注册环境均以固定 `/usr/local/bin/agent -listen 0.0.0.0:8080` 启动，运行时分别设置 Entrypoint 和 Cmd，并使用 `sandbox` 用户、`/workspace` 工作目录与相同安全限制；Python 提供 python3/pip，Node 提供 node/npm，不在启动时联网安装包。镜像构建和 Alpine 兼容边界见 [Docker 实现](docker/IMPLEMENTATION.md#preset-images)。

启用出口时额外创建专属 `egress` 容器，连接本实例已有的 internal 与 ingress 网络，内部别名为 `egress`，不发布宿主端口且无挂载。仅 sandbox 注入 `HTTP_PROXY`/`http_proxy`、`HTTPS_PROXY`/`https_proxy` 为 `http://egress:18084`，`NO_PROXY`/`no_proxy` 为 `localhost,127.0.0.1,::1`。环境变量用于兼容客户端；调用方仍须显式配置不读取代理环境的客户端，HTTPS 由客户端正常校验证书，无透明代理或直连回退保证。

新资源都带版本化不可变策略标签；新旧实例、标签一致性和清理语义见 [Docker 设计](docker/DESIGN.md#network-policy)。创建、查询和 endpoint 响应不回显网络策略，SQLite 不增加策略字段；启用实例缺失、停止或不健康的 egress 不能返回状态 ready，实例 readyz 也不能成功。endpoint 仍只表示入口可用，不保证公网可达；出口故障不会触发自动修复或切换网络。

创建、状态与 endpoint 响应结构不增加环境字段，SQLite 不保存环境选择；后续操作按真实资源和归属管理，不能按当前镜像映射推断、重建或更换已有实例。续期与 pending 确认恢复不切换环境、不重置初始 TTL。

运行时直接使用 Service 计算好的时间，将同一组 `io.agent-sandbox.created-at`、`io.agent-sandbox.expires-at` 标签写入本次全部资源，不重新取时。时间标签属于初始生命周期元数据，不替代完整归属标签。

SQLite 与 Docker 没有跨系统原子事务。Runtime 明确完成失败回滚后，Service 按原事务 ID 和版本清理预写记录；回滚不完整或结果不明确时保留记录。数据库有记录不表示实例已经创建成功或仍存在。SQLite 写入期间取消会等待最终提交结果；已提交而尚未开始 Runtime 时先补偿元数据，不能因取消异步等待就假定写入已停止。创建与补偿的首次、重复取消都等待关键收尾并读取后台异常。

### 创建错误

| HTTP 状态码 | `error.code`                      | 含义                                                          |
| ----------- | ----------------------------------- | ------------------------------------------------------------- |
| 400         | `invalid_request`                 | JSON 结构、字段、类型或范围无效                               |
| 413         | `invalid_request`                 | 实际请求体超过 4096 字节                                      |
| 429         | `sandbox_create_capacity_exceeded` | 正在处理的创建数量满额，立即拒绝且未分配 sandbox ID            |
| 409         | `sandbox_resource_conflict`       | 生成的资源与已有资源冲突                                      |
| 503         | `sandbox_image_unavailable`       | 所选预制主镜像、入口镜像或已启用的出口镜像不存在                                |
| 503         | `sandbox_metadata_unavailable`    | 生命周期预写或激活失败，含 sandbox ID；预写失败不调用 Runtime |
| 409         | `sandbox_metadata_conflict`       | 预写或激活身份、版本冲突，含 sandbox ID                       |
| 500         | `sandbox_create_failed`           | 创建失败，但本次已记录资源已完成回滚                          |
| 500         | `sandbox_cleanup_failed`          | 创建失败且本次资源未能全部回滚                                |
| 500         | `sandbox_create_uncertain`        | Runtime 创建结果不明确，保留记录，含 sandbox ID               |
| 500         | `sandbox_metadata_cleanup_failed` | 创建回滚或写入取消后的元数据补偿失败，含 sandbox ID           |

普通创建错误示例：

```json
{
  "error": {
    "code": "sandbox_create_failed",
    "message": "sandbox creation failed"
  }
}
```

回滚不完整时，错误对象额外包含 sandbox ID，便于人工定位本项目管理的残留资源：

```json
{
  "error": {
    "code": "sandbox_cleanup_failed",
    "message": "sandbox creation failed and cleanup was incomplete",
    "sandbox_id": "sbx_0123456789abcdef0123456789abcdef"
  }
}
```

## 5. 查询 sandbox 状态

### `GET /v1/sandboxes/{sandbox_id}`

在默认 1 秒的 HTTP 整体查询超时内，读取指定 sandbox 当前的 Docker 容器状态、Docker health 状态和生命周期时间。Service 先查询 Runtime 中的实际实例，不存在仍返回 404，即使数据库中有记录；再读取 SQLite，核对实例 ID、内部事务 ID 和创建时间。匹配时采用数据库当前的 `expires_at`，运行状态继续来自 Docker。续期后，HTTP 查询返回新期限，Docker 初始标签保持原值。

新建 Runtime/Service 或重启 Control 后仍可从同一个数据库恢复记录；查询不重新取时、不自动延长过期时间。数据库不可用或身份不匹配时返回稳定错误，不静默回退到可能过时的 Docker 标签。内部事务 ID 仅用于核验，不加入公开响应。

路径参数必须符合以下格式：

```text
sbx_<32 位小写十六进制字符>
```

请求示例：

```bash
SANDBOX_ID=sbx_0123456789abcdef0123456789abcdef
curl -i "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}"
```

查询成功时返回 HTTP 200：

```json
{
  "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
  "status": "ready",
  "reason": "health_check_passed",
  "message": "Agent health check passed",
  "created_at": "2026-10-19T08:00:00Z",
  "expires_at": "2026-10-19T08:30:00Z"
}
```

`status` 是面向调用方的概要状态，`reason` 和 `message` 说明产生该状态的具体事实。

没有数据库记录时，继续返回 Docker 标签中的初始时间，不自动补写记录或接管数据库丢失前的实例。旧实例同时没有两个时间标签时，响应中的 `created_at`、`expires_at` 均为 `null`，控制面不补写标签，也不接管旧实例的 TTL。旧实例原有状态查询、endpoint 和删除能力保留。时间标签只缺少一项、格式错误或 `expires_at < created_at` 时，返回下述稳定的 `sandbox_query_failed`，不暴露原始解析异常，也不猜测时间。

`expires_at` 表示计划过期时间。查询本身不触发删除，超过该时间也不会增加 `expired` 状态，`ready` 表示 Agent 健康，启用出口时还要求出口健康及拓扑正确，不保证公网可达。应用启动后默认在后台周期回收，每轮结束后等待 60 秒、每轮最多 100 条；到实际删除前，状态、endpoint 与命令执行保持现有语义。扫描与失败重试可能延迟回收；没有公开回收 API，配置及单进程限制见 [控制面设计](DESIGN.md#后台周期回收)。

启用出口时，`egress_missing`、`egress_stopped`、`egress_unhealthy` 返回 `status: failed`，`egress_starting` 返回 `status: starting`；查询仍为 HTTP 200。策略标签损坏、不一致、归属或拓扑异常返回既有 500 `sandbox_query_failed`，不把它们当成可正常使用的禁用出口实例。无论新旧策略，查询都检查已发现角色的策略一致性，不因主容器标签缺失忽略其他资源的策略。

### 状态与原因

| Docker 事实                       | `status`   | `reason`                      | `message`                                                   |
| --------------------------------- | ------------ | ------------------------------- | ------------------------------------------------------------- |
| 容器已创建但尚未运行              | `starting` | `container_created`           | `container has been created but is not running`             |
| 容器正在重启                      | `starting` | `container_restarting`        | `container is restarting`                                   |
| 容器运行中，未配置 health check   | `running`  | `health_check_not_configured` | `container is running but no health check is configured`    |
| 容器运行中，health check 正在启动 | `starting` | `health_check_starting`       | `container is running and its health check is starting`     |
| health check 为`healthy`        | `ready`    | `health_check_passed`         | `Agent health check passed`                                 |
| health check 为`unhealthy`      | `failed`   | `health_check_failed`         | `Agent health check is failing`                             |
| 容器已暂停                        | `paused`  | `container_paused`            | `container is paused`                                       |
| 容器以退出码 0 结束               | `stopped`  | `container_exited`            | `container exited successfully`                             |
| 容器以非零退出码结束              | `failed`   | `container_exited_with_error` | `container exited with a non-zero status`                   |
| 容器因超过内存限制被终止          | `failed`   | `container_oom_killed`        | `container was terminated after exceeding its memory limit` |
| Docker 将容器标记为 dead          | `failed`   | `container_dead`              | `Docker reports that the container is dead`                 |

只有 Docker health 明确为 `healthy` 时才返回 `ready`。运行时为所有主容器配置固定 wget 健康检查，访问容器内部的 `http://127.0.0.1:8080/healthz`，覆盖镜像自带的健康检查，health starting 时返回 `starting`，unhealthy 时返回 `failed`。控制面只读取 Docker 状态，创建接口仍返回 `started` 而不等待检查通过。

`ready` 表示容器内部 Agent 健康，不代表宿主机动态入口或外部代理可用。镜像重建不会更新旧容器；没有健康检查的旧容器运行时仍返回 `running / health_check_not_configured`。健康检查失败不会触发容器删除、工作区清理或自动重启。

### 查询错误

| HTTP 状态码 | `error.code`                   | 含义                                                                                      |
| ----------- | -------------------------------- | ----------------------------------------------------------------------------------------- |
| 404         | `sandbox_not_found`            | ID 格式无效、受管容器不存在或归属标签不匹配                                               |
| 503         | `docker_unavailable`           | Docker daemon 不可用                                                                      |
| 503         | `sandbox_metadata_unavailable` | 生命周期存储不可用，含 sandbox ID，不回退到标签时间                                       |
| 409         | `sandbox_metadata_conflict`    | 记录与实际实例的 ID、事务 ID 或创建时间不匹配，含 sandbox ID                              |
| 504         | `sandbox_query_timeout`        | 查询未在有限时间内完成                                                                    |
| 500         | `sandbox_query_failed`         | Docker 返回重复、未知或无法可靠解析的受管状态，或时间标签单项缺失、格式错误、时间顺序错误 |

未知 sandbox 的响应不会说明同名非本项目容器是否存在：

```json
{
  "error": {
    "code": "sandbox_not_found",
    "message": "sandbox not found"
  }
}
```

## 6. 查询本机入口

### `GET /v1/sandboxes/{sandbox_id}/endpoint`

通过 `API → SandboxService → SandboxRuntime` 在默认 1 秒的 HTTP 整体查询超时内，只读查询专属 proxy 的实际地址；该查询不读取生命周期数据库，也不依赖进程内创建记录，控制面重启后仍可恢复。ID 格式与状态查询相同。

```bash
curl -i "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}/endpoint"
```

HTTP 200：

```json
{"sandbox_id":"sbx_0123456789abcdef0123456789abcdef","url":"http://127.0.0.1:49153"}
```

地址从代理实际 `NetworkSettings.Ports["8080/tcp"]` 读取，不根据 `HostConfig.PortBindings` 推断已发布；仅接受唯一 `127.0.0.1`、1–65535 端口。查询核验 sandbox/proxy 完整归属、相同事务 ID、专属网络标签与实际连接、sandbox DNS 别名和 proxy running/healthy。`ready` 继续表示内部 Agent 健康，入口可用性由本接口单独判断。

| HTTP 状态码 | `error.code`                   | `error.message`                    |
| ----------- | -------------------------------- | ------------------------------------ |
| 404         | `sandbox_not_found`            | `sandbox not found`                |
| 503         | `sandbox_endpoint_unavailable` | `sandbox endpoint is unavailable`  |
| 504         | `sandbox_endpoint_timeout`     | `sandbox endpoint query timed out` |

入口缺失、代理未就绪、归属或事务不匹配、网络错误、端口未实际发布、多个候选 proxy 或 Docker 不可用均返回稳定 503；HTTP 整体或 Docker transport 超时返回稳定 504。没有 proxy 的旧实例返回 503，不会修改既有实例。错误不暴露 Docker 细节。

此 URL 仅供本机 WSL 开发，不能作为远程访问承诺。调用者通过该 URL 访问 Agent `/healthz`、命令、文件和 SSE；控制面不转发这些调用。

## 7. 续期 sandbox

### `POST /v1/sandboxes/{sandbox_id}/renew`

延长尚未到期的受管实例期限。请求体必须为只包含 `ttl_seconds` 的 JSON 对象，该字段必填，且必须为 1～86400 的真正整数；拒绝布尔值、浮点数、字符串、`null`、未知字段、缺失字段和非法 JSON。

```bash
curl -i -X POST \
  -H 'Content-Type: application/json' \
  -d '{"ttl_seconds":1800}' \
  "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}/renew"
```

每次续期只读取一次带时区 UTC 时钟，按以下规则计算并持久化：

```text
new_expires_at = max(current_expires_at, now + ttl_seconds)
```

`ttl_seconds` 表示从本次续期校验时刻起希望至少保留的时长，不是在原期限上累加。目标时间较早时保留原期限，但仍进行事务与版本保护的提交；重复调用不会按调用次数累加 TTL，但校验时刻向后移动可能使期限延后。创建接口的初始 TTL 独立配置，默认仍为 1800 秒。

成功返回 HTTP 200，时间沿用 RFC 3339 UTC 格式：

```json
{
  "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
  "expires_at": "2026-10-19T08:45:00Z"
}
```

Service 先通过 Runtime 只读核验实际实例，再核对数据库记录的实例 ID、事务 ID 和创建时间。仅身份匹配、`management_state=active` 且 `current_expires_at > now` 时允许续期；恰好到期也拒绝。`pending`、`deleting`、已到期或没有生命周期记录的旧实例均返回续期冲突，不自动接管或补写。续期不重建容器、不修改 Docker 初始时间标签、不改变入口或运行状态；成功仅说明生命周期元数据已提交，不代表 Agent 就绪。

续期提交核对事务 ID 与预期版本。续期先提交会使旧回收候选的认领失效；回收或手动删除先认领为 `deleting` 后，续期不能重新续活。并发续期发生版本冲突时返回 409，不刷新版本强行覆盖。续期成功不阻止随后发起的显式手动删除；当前仍以单进程、单 worker 为前提。

续期不套用只读查询的整体超时，也不能将取消或超时解释为没有写入。SQLite 写入期间请求取消会等待最终结果并读取异常，已经提交的期限不反向补偿。调用方断开或没有收到响应时，可能已经提交，应通过状态查询确认实际 `expires_at`。

### 续期错误

| HTTP 状态码 | `error.code`                   | 含义                                                      |
| ----------- | -------------------------------- | --------------------------------------------------------- |
| 400         | `invalid_request`              | 请求体、TTL 类型、范围或字段无效；不调用 Runtime 或 Store |
| 404         | `sandbox_not_found`            | ID 无效或实际受管实例不存在，即使数据库仍有记录           |
| 409         | `sandbox_renew_conflict`       | 无记录、状态不允许、已到期或更新版本冲突，含 sandbox ID   |
| 409         | `sandbox_metadata_conflict`    | 记录与实际实例的身份或创建时间不匹配，含 sandbox ID       |
| 503         | `sandbox_metadata_unavailable` | 生命周期存储不可用，含 sandbox ID                         |
| 503         | `docker_unavailable`           | Docker daemon 不可用                                      |
| 500         | `sandbox_query_failed`         | Runtime 只读身份核验失败，例如时间标签异常                |

错误沿用统一的 `error` 结构，不包含底层 SQL、文件路径、Docker 响应或异常堆栈。

## 8. 删除 sandbox

### `DELETE /v1/sandboxes/{sandbox_id}`

通过 `API → SandboxService → SandboxRuntime` 回收受管资源。ID 格式与查询相同，无效 ID 在访问 Docker 前返回 404。删除不要求 sandbox 容器仍存在、健康或 endpoint 可用；控制面重启后从 Docker 标签恢复清单，也可删除仅剩代理、网络或 workspace 的实例。

Service 先读取生命周期记录，有记录时将 `pending` 或 `active` 原子认领为 `deleting` 并递增版本，已处于 `deleting` 的记录允许重试。随后向 Runtime 传入认领记录的预期事务 ID，Runtime 必须在破坏性操作前核对所有存留角色，包括默认保留的 workspace；不能用 `Runtime.get()` 成功代替删除归属核验。没有记录的旧实例继续按现有规则删除。

| 查询参数             | 类型    | 默认值    | 语义                                                         |
| -------------------- | ------- | --------- | ------------------------------------------------------------ |
| `delete_workspace` | boolean | `false` | `true` 时最后一并删除该实例专属 workspace 卷，数据不可恢复 |

默认删除 proxy、sandbox、ingress 网络和 internal 网络，保留 workspace 和文件：

```bash
# Use only the SANDBOX_ID recorded from this creation.
curl -i -X DELETE "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}"
```

明确需要删除数据时，显式传参；此操作也适用于运行资源已经删除、仅剩保留卷的情况：

```bash
# Permanently delete all data in this sandbox workspace.
curl -i -X DELETE "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}?delete_workspace=true"
```

达到请求的目标状态后返回 **HTTP 204，响应体为空**。格式合法但无目标受管资源、重复删除，以及默认删除时仅剩保留卷，也返回 204。成功删除运行资源后，实例状态与 endpoint 查询返回 404，旧入口不再可用；保留卷本身不构成可查询实例。

Runtime 确认删除目标完成后，Service 按认领记录的事务 ID 和版本删除元数据。默认保留 workspace 时同样移除已结束实例的生命周期记录，残卷继续按 Docker 完整归属标签和显式删卷请求管理。Docker 删除失败时保留 `deleting`；Docker 已完成但元数据清理失败时返回 `sandbox_metadata_cleanup_failed` 并保留 sandbox ID，允许对同一 ID 重试。版本冲突不能盲目刷新版本后删除，已进入删除流程的记录不能修改过期时间。

删除前核验所有候选的完整归属标签、有效且跨角色一致的事务 ID，以及角色唯一性；归属有歧义时先拒绝，不任选一个候选。操作只使用清单中的精确容器/网络 ID 或经核验的卷 Name，每次 stop/remove 前重新读取归属。固定 Compose 实例、共享资源及标签不匹配的资源不属于目标。

容器使用有限停止等待，必要时重新核验后强制终止本实例容器。网络被其他容器连接或卷仍被使用时会报告失败，不强制断开其他容器或强制删除卷。已经消失的资源视为完成；部分失败不会重建已删除项，可对同一 ID 重试继续回收。

### 删除错误

| HTTP 状态码 | `error.code`                      | `error.message`                             | 含义                                                     |
| ----------- | ----------------------------------- | --------------------------------------------- | -------------------------------------------------------- |
| 404         | `sandbox_not_found`               | `sandbox not found`                         | ID 格式无效                                              |
| 409         | `sandbox_delete_conflict`         | `sandbox resource ownership conflict`       | 候选重复、事务冲突、归属或精确标识变化                   |
| 503         | `docker_unavailable`              | `Docker runtime is unavailable`             | Docker 后端无法完成资源发现                              |
| 503         | `sandbox_metadata_unavailable`    | `sandbox lifecycle metadata is unavailable` | 读取或删除认领失败，尚未调用 Runtime，含 sandbox ID      |
| 409         | `sandbox_metadata_conflict`       | `sandbox lifecycle metadata conflict`       | 生命周期记录身份冲突，含 sandbox ID                      |
| 500         | `sandbox_delete_failed`           | `sandbox deletion was incomplete`           | 回收未达到目标状态，含`sandbox_id`                     |
| 500         | `sandbox_metadata_cleanup_failed` | `sandbox lifecycle metadata cleanup failed` | Docker 已完成，但元数据清理失败或版本冲突，含 sandbox ID |
| 422         | FastAPI 参数校验响应                | FastAPI 参数校验响应                          | `delete_workspace` 不是可接受的 boolean                |

部分清理失败的稳定响应示例：

```json
{
  "error": {
    "code": "sandbox_delete_failed",
    "message": "sandbox deletion was incomplete",
    "sandbox_id": "sbx_0123456789abcdef0123456789abcdef"
  }
}
```

DELETE 不套用状态/入口查询的 1 秒整体超时；SDK 调用仍有有限 transport timeout。请求取消不等于同步 Docker 或 SQLite 操作停止：Runtime 删除与后续元数据清理作为关键收尾，首次和重复取消均等待最终结果并读取后台异常，再传播取消或可追踪的失败。调用者未收到结果时应对同一 ID 重试，以当前资源事实确认结果。

默认保留卷会持续占用宿主磁盘。周期及显式单轮过期回收同样保留 workspace；尚无后台卷回收任务，需要调用者记录 sandbox ID 并显式决定数据回收时间。该保留策略不改变创建失败时的事务回滚。

## 9. 主动就绪检查

### `GET /v1/sandboxes/{sandbox_id}/readyz`

只读检查本次 Control → 实例入口代理 → Agent 的 HTTP 链路。先复用状态查询的实际实例及生命周期身份核验；状态不是 `ready` 时立即返回未就绪，不查询入口或请求 Agent。随后复用 endpoint 核验，核对返回的实例 ID，最后向该入口的固定 `/healthz` 请求一次，不轮询或重试。

```bash
curl -i "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}/readyz"
```

Agent 返回 HTTP 200 且 JSON 为 `{"status":"ok"}` 时，Control 返回 HTTP 200：

```json
{"sandbox_id":"sbx_0123456789abcdef0123456789abcdef","status":"ready"}
```

整次检查使用 `sandbox_query_timeout` 的整体 deadline，默认 1 秒，覆盖状态、元数据、入口和 HTTP 探测。HTTP 探针另有默认 0.5 秒的网络及整次探测超时。取消会传播到 HTTP 请求，并等待响应和连接关闭；检查不写入数据库、不续期或改变 Docker 资源。

目标只接受核验后的 `http://127.0.0.1:<1～65535>` 入口（可带根路径 `/`），不接受调用方 URL、任意主机、用户信息、其他路径、query 或 fragment。固定 GET `/healthz`，禁止重定向和环境代理，要求未压缩响应，按原始字节有界读取，最多 4 KiB；错误不包含原始响应或底层异常。

| HTTP 状态码 | `error.code`                   | 含义                                                                           |
| ----------- | -------------------------------- | ------------------------------------------------------------------------------ |
| 404         | `sandbox_not_found`            | ID 无效或实际受管实例不存在                                                    |
| 503         | `sandbox_not_ready`            | 实例状态尚不是`ready`                                                        |
| 503         | `sandbox_endpoint_unavailable` | 入口核验失败，或入口返回的实例 ID 不匹配                                       |
| 503         | `sandbox_agent_unavailable`    | 目标地址不允许、连接失败、探针超时、非 200、健康 JSON 无效、响应过大或压缩响应 |
| 504         | `sandbox_readiness_timeout`    | 整次检查未在 API deadline 内完成                                               |
| 503         | `docker_unavailable`           | 初始状态查询时 Docker 不可用                                                   |
| 500         | `sandbox_query_failed`         | 初始运行状态无法可靠确认                                                       |
| 503         | `sandbox_metadata_unavailable` | 生命周期存储不可用，含 sandbox ID                                              |
| 409         | `sandbox_metadata_conflict`    | 元数据与实际实例身份不匹配，含 sandbox ID                                      |

根 `/readyz` 仍只检查 Docker，状态查询中的 `ready` 仍只表示 Docker health 为 healthy；本接口额外确认本次实际入口链路。成功是当下观察，不保证下一条命令成功、剩余执行额度或更强的安全隔离，也不构成跨系统原子快照。没有等待就绪、周期探测或自动续期。

## 10. 暂停与恢复

### `POST /v1/sandboxes/{sandbox_id}/pause`
### `POST /v1/sandboxes/{sandbox_id}/resume`

请求无需 JSON body。成功均返回 **HTTP 204，响应体为空**；只在 Docker 确认目标状态后返回，不等待恢复后的 Agent health 变为 healthy。

```bash
curl -i -X POST "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}/pause"
curl -i "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}"
curl -i -X POST "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}/resume"
curl -i "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}/readyz"
```

仅允许实际资源与生命周期记录身份一致、管理状态为 `active` 且尚未到期的实例。无记录旧实例、`pending`、`deleting`、到期或状态不允许时返回对应操作冲突；恰好到期也拒绝。已达到本次目标的重复请求仍执行完整核验，满足准入时返回 204。主容器不存在或 ID 无效返回 404；存储故障、身份冲突与 Docker 不可用继续使用既有稳定错误。

暂停通过 Docker 冻结原容器内的 Agent 和用户进程，保留进程内存、容器 ID、网络、入口端口、标签及 workspace；恢复在同一进程上继续。非空出口策略下，暂停按 sandbox → egress，恢复按 egress → sandbox，入口 proxy 保持运行。查询公开 `status: paused / reason: container_paused`；主容器与运行中出口的冻结状态不一致时返回 `failed / pause_incomplete`，缺失或异常出口仍使用相应 `egress_*` 原因。实例 `/readyz` 在状态检查阶段返回 503 `sandbox_not_ready`，不访问被冻结的 Agent。endpoint 仍校验既有入口和 proxy health；暂停期间该健康检查可能过期而返回 503，但不更改发布端口或配置，也不代表可执行请求。

TTL 在暂停期间继续计算，未到期的暂停实例仍可续期；手动删除和到期回收可终止暂停实例，默认保留 workspace。SQLite schema 和 `pending/active/deleting` 管理状态不变，暂停事实从 Docker 恢复，控制面重启不会丢失该事实。

冻结不释放内存，不是 checkpoint、容器重建或宿主重启恢复。既有 TCP、SSE、CONNECT 连接可能因客户端、代理或远端期限而断开；Agent 命令 timeout 与终态保留期不保证随暂停冻结，恢复后可能立即超时或清理。调用方应在恢复后重新检查 readyz，并按现有接口查询后台任务结果。

| HTTP | `error.code` | 含义 |
| --- | --- | --- |
| 404 | `sandbox_not_found` | ID 无效或实际受管主容器不存在 |
| 409 | `sandbox_pause_conflict` / `sandbox_resume_conflict` | 管理状态、期限、运行状态或资源归属不允许操作 |
| 500 | `sandbox_pause_failed` / `sandbox_resume_failed` | 操作或结果确认失败 |
| 503 | `docker_unavailable` / `sandbox_metadata_unavailable` | Docker 或生命周期存储不可用 |
| 409 | `sandbox_metadata_conflict` | 记录与实际实例身份冲突 |
| 500 | `sandbox_query_failed` | 前置实际状态无法可靠读取 |

暂停与恢复使用有限 Docker transport timeout，不套用只读查询的整体超时。请求取消或响应丢失不证明操作未执行；首次及重复取消均等待已开始的关键操作收尾并读取结果，再按状态查询或同一目标重试。多容器操作没有跨 Docker 原子事务；失败后可能保留部分状态，重试依据实际资源继续收敛，不重建容器或修改期限。

## 11. HTTP 方法

当前各路径只接受本文档声明的方法。使用其他方法时，FastAPI 返回 HTTP 405 `Method Not Allowed`。

## 12. 当前限制

- 没有认证或授权，只能作为本机开发接口使用。
- 创建允许 basic/python/node 环境及受约束的 CPU、内存和初始 TTL，缺省 basic、1800 秒；支持显式续期及周期或内部单轮回收，没有自动续期客户端或公开回收接口。
- 预制环境基于 Alpine，不承诺任意 glibc 二进制、第三方依赖或完整开发工具链可用；任意镜像、动态 Agent 注入及其他发行版仍属后续方向。
- CPU/内存限制仅为单实例上限；创建并发准入不提供实例总量限制、整机资源预算、容量预留或运行中动态调额。
- Docker 标签保存初始生命周期信息，SQLite 保存可更新的生命周期记录；当前不通过 `container.update` 修改标签，不隐式同步或接管无记录旧实例。
- SQLite 与 Docker 没有跨系统原子事务或自动故障恢复；数据库记录不证明实例存在，未确认创建的 `pending` 仍需核验或手动删除。续期与删除认领通过版本和状态保护，不提供跨进程互斥或 exactly-once 保证。
- 入口只支持本机固定 Agent HTTP，不暴露任意用户服务端口。
- 状态查询合并 Docker 事实与身份匹配的数据库时间，不会主动启动、停止、修复、补写或清理 sandbox。
- Control 不直接访问容器 IP；状态查询根据 Docker health 报告 `ready`，实例级 `/readyz` 通过核验后的本机入口主动检查 Agent。
- 创建响应中的 `started` 与查询响应中的 `ready` 含义不同。
- Control API 不提供 Agent 的命令执行和文件传输能力。
