# 项目开发进度

本文档记录 Agent Sandbox 已完成的开发工作、后续功能和已知优化事项。开发进度按日期倒序排列，最新记录位于最前。

## 开发进度

### 2026-10-01

- 统一控制面运行时调用链为 `API → SandboxService → SandboxRuntime`：`core/service.py` 新增 `ping()`，创建继续生成 ID/规格并在运行时成功后组装结果，查询继续先校验 ID；API 仅依赖共享 Service。`create_app(runtime=None, readiness_timeout=1.0, sandbox_query_timeout=1.0)` 装配一个 Runtime 与一个 Service，支持 FakeRuntime，替代原独立 probe/creator/reader 注入。
- 公共运行时契约移至 `runtime/sandbox_runtime.py`，使用 ABC/abstractmethod 定义异步 ping/create/get；移除 `core/protocols.py` 中的独立探测和创建/查询转发接口。Docker 探测、异步适配、只读查询与状态映射合并至 `runtime/docker/docker_runtime.py`；创建事务、client 生命周期工厂、共用归属校验和逆序回滚合并至 `runtime/docker/transaction.py`，原细分模块移除。
- 保持 `/healthz` 不调用运行时，`/readyz` 的整体超时与脱敏响应留在 API；Docker ping 失败转换为领域异常后经 Service 传播。runtime 包入口不加载具体后端，导入、构造和应用装配均不连接 Docker。ping transport timeout 仍为 0.5 秒，创建和查询为 5 秒，资源配置与 HTTP 契约不变。
- 本次默认测试基线为 86 passed、1 skipped；调整后为 96 passed、1 skipped。FakeRuntime 覆盖 Service 的 ping/create/get 委托、领域异常传播与 ID 校验；新增共享 Service 调用链、无效 ID 在运行时前拒绝、纯契约/Service/API 无 Docker 后端导入、ABC 完整实现要求和默认查询 5 秒 transport timeout 测试。原创建失败、取消/重复取消、提交后回滚、精确 ID 与完整标签核验测试继续通过；AST 对照确认事务、归属核验、状态映射与异步 create/get 代码原样迁移，依赖配置与锁文件未变，新旧 OpenAPI 完全一致；本次独立只读复核未发现阻断或待优化问题。
- 本次 `uv run python -m compileall -q app.py api core runtime tests` 与 `git diff --check` 通过；指定 Uvicorn 入口在 `127.0.0.1:18083` 启动，实际 `/healthz`、`/readyz` 返回 200，无效 sandbox ID 查询返回脱敏 404，随后正常停止本次进程。默认 uv 缓存只读，使用 `/tmp` 缓存；受限环境线程测试停滞，默认回归在获准环境运行。未运行需显式修改真实资源的 Docker 集成测试，也未运行未改动的 Go、镜像构建与 Compose 验证；当前合并关系和设计见 [仓库目录与职责](REPOSITORY_LAYOUT.md)。

- 最终独立只读复核通过，无阻断或待优化发现；额外对比新旧 OpenAPI 和 `create_app` 参数名称、顺序、默认值，结果完全一致。AST 对照确认原 Python 测试函数体与断言仅 patch 目标迁移，Go 源码仅 import/module 变化，代理配置原样、Compose 安全策略仅构建路径变化。
- 完成按运行位置与职责重组：`agent/` → `sandbox_agent/`，`internal/command`、`internal/server` 直接迁为 `command/`、`server/`，Go module/import 更新为 `github.com/nico-hua/agent-sandbox/sandbox_agent`；Compose、代理与脚本迁为 `deploy/local/`，构建上下文固定为 `../../sandbox_agent`。
- `control/` 的配置、原样锁文件和测试迁为 `control_plane/`；原 `control/control/app.py` 拆为顶层应用装配与 `api/health.py`、`api/sandboxes.py`，`sandbox_service.py` 拆为 `core/models.py`、`errors.py`、`protocols.py`、`service.py`；原探测与 `sandbox_runtime.py` 拆为 `runtime/docker_client.py`、`docker_probe.py`、`docker_runtime.py`、`docker_create.py`、`docker_query.py`、`ownership.py`。顶层 Python 源文件只有 `app.py`，无同名嵌套目录、兼容包装层或控制面专属 AGENTS；原控制面约束合并到根 AGENTS。
- 保留 HTTP、命令/文件/SSE 协议、镜像标签、资源命名与安全配置，保持应用装配注入、client 延迟创建、ping 0.5 秒与生命周期 5 秒 transport timeout、完整创建记录及取消/重复取消后的精确逆序回滚。原测试断言全部保留，新增首次导入与装配不连接 Docker、默认 ping 超时和成功/失败关闭验证。
- 迁移前 Python 默认基线为 83 passed、1 skipped；Go test/race/vet 通过。迁移后 `uv sync --locked` 重建新环境，Python 默认测试为 86 passed、1 skipped，compileall 与 Go test/race/vet 均通过。锁文件和依赖版本保持不变；旧 `control/.venv` 留在原处。
- `docker compose -f deploy/local/compose.yml config --quiet`、镜像构建和本地脚本 shell 语法检查通过；从 `/tmp` 解析 Compose 验证构建与挂载绝对路径，使用隔离 Docker 测试替身验证两个脚本定位自身配置。真实 Docker 集成显式执行，1 passed、86 deselected；测试前后容器、网络、volume 的 ID/名称快照完全一致（5/8/19 项），既有开发资源未删除或重建。
- 新 Uvicorn 入口完成应用装配；18083 已由既有服务占用，按指定命令启动返回地址占用错误，改用 127.0.0.1:18084 验证 `/healthz` 返回 200 和 `{"status":"ok"}`，随后停止本次进程。受限执行环境对默认缓存、端口和 Docker socket 有限制，uv 同步/回归、Go HTTP 测试与 Docker 验证在允许访问的环境执行，Go 缓存使用 `/tmp`。未运行会重建现有容器的本地启动或完整 smoke；路径通过配置解析和脚本测试替身验证。`git diff --check` 通过，不自动提交。迁移设计见 [仓库目录与职责](REPOSITORY_LAYOUT.md)。
- Agent 镜像新增容器内 `/healthz` HEALTHCHECK，动态创建仍返回 `started`，查询在健康检查通过后返回 `ready`；ready 仅表示内部 Agent 健康，旧容器不会自动继承检查。新镜像显式 Docker 测试通过 HTTP 创建、有限轮询到 ready，核对健康状态和原安全基线后精确清理；Python 默认测试、编译检查及 Go test/race/vet 均通过。动态入口、删除与资源回收继续待开发。
- 控制面新增 `GET /v1/sandboxes/{id}`：按完整项目标签读取 Docker 容器事实，控制面重启后仍可查询；返回 `starting`、`running`、`ready`、`stopped` 或 `failed` 及稳定的 reason/message，只有 Docker health 为 `healthy` 时才标记 ready，未配置健康检查的运行容器明确保持 `running`。
- 查询接口对未知、格式无效或标签不匹配的 ID 统一返回 404，Docker 不可用与查询超时分别返回稳定的 503/504；查询只读且不启动、停止或清理资源。fake 测试覆盖状态映射、归属校验、异常和超时，显式 Docker 集成测试通过全新 runtime 实例查询本次创建的测试容器。
- 控制面新增最小 `POST /v1/sandboxes`：空请求会生成不可预测 ID，并以固定 `agent-sandbox:dev` 镜像创建专属 internal bridge、workspace volume 和无宿主端口的非 root 容器；返回的 `started` 仅表示容器已启动，不表示 Agent ready。
- 所有受管资源使用一致的项目、sandbox ID、独立事务 ID、受管标记和资源类型标签；同步 Docker SDK 在线程中执行，低层 create 成功后先记录 ID，失败或重复取消时仍按精确 ID 与完整标签逆序回滚，回滚不完整会返回 sandbox ID。
- fake 单元测试覆盖创建顺序、安全配置、错误分类、取消与精确回滚；显式 Docker 集成测试核对 internal 网络、空端口映射和资源限制，并在标签核对后清理。本阶段仍无动态入口和公开删除 API。

### 2026-09-30

- 新增独立的 WSL 宿主侧 Python/FastAPI 控制面骨架：`GET /healthz` 仅检查 HTTP 进程，`GET /readyz` 通过只读 Docker ping 检查运行时；Docker 失败或 1 秒超时统一返回不泄露宿主细节的 HTTP 503。
- Docker 探测采用依赖注入、延迟创建 client 和线程化同步 SDK 调用，成功及失败路径均关闭 client；Docker 不可用不影响控制面启动。
- 控制面默认仅监听 `127.0.0.1:18083`，Docker socket 保留在宿主侧；当前仍无认证和 sandbox 创建、查询、删除等生命周期能力。

### 2026-09-26

- 本地 Compose 入口改用 `127.0.0.1:18081`，同步 README 和 smoke 脚本默认端口；新增覆盖命令参数与文件 API 的 curl 手动测试文档。
- 增加 `/v1/files` 单文件二进制上传/下载：路径约束在 `/workspace`，拒绝同名覆盖、路径穿越及越界符号链接，单次上传和下载均限制为 10 MiB。
- 在备用本机端口 `18081` 验证“上传 → 命令处理 → 下载”、10 MiB 边界、超限拒绝和容器重建后的文件持久化；原定 `18080` 当时被其他项目容器占用，本次未通过该端口验收。
- 加固文件 API：上传写入工作区内临时文件后无覆盖发布，失败清理；上传/下载共享 2 个并发额度，代理也限制该路径并发并关闭请求和响应缓冲。在实际入口 `18081` 验证第 3 个并发请求返回 429、10 MiB 边界、文件闭环与公网出口对照；加入唯一文件名和实际代理配置核对后，连续两次本地 smoke 均通过。
- 新增前台 SSE 命令接口，与同步接口共用请求策略和并发额度；stdout/stderr 按 Base64 JSON 帧实时传输，完成或失败有终止事件。通过 `18081` 的 `curl -N` 观察到首帧比第二帧早约 2 秒，断线后容器内命令及后代进程退出；本地 smoke 复测同步命令、文件闭环与公网出口。

### 2026-09-24

- 新增最小多阶段 Docker 镜像：Agent 及基础命令工具在容器内以非 root 用户运行，默认工作目录为可写的 `/workspace`。
- 验证仅向宿主机 `127.0.0.1` 发布 HTTP 端口、命名卷跨容器重建保留文件，以及健康检查、同步命令和现有请求限制在容器中的行为。
- 增加本地启动脚本，为整个容器设置 1 核 CPU 配额、256 MiB 内存、无额外 swap 和 64 个 PID；通过 Docker 配置与 cgroup v2 文件验证生效。
- 本地容器启用只读根文件系统，以命名卷保留可写 `/workspace`，并为可写 `/tmp` 配置 32 MiB 临时文件系统；验证写入边界及重建后的文件生命周期。
- 收紧本地容器进程权限：移除全部 Linux capabilities、启用 no-new-privileges，并通过 HTTP 子进程的 `CapEff`、`CapBnd` 和 `NoNewPrivs` 验证生效。
- 以 Docker Compose 运行仅连接 internal bridge 的 sandbox 和双网络固定上游入口代理；仅代理向 WSL `127.0.0.1:18080` 发布端口。健康检查、命令执行、工作区持久化及同一公网 IP 的 bridge 对照测试均通过。本方案不是 OpenSandbox Docker 网络实现的原样复现。

### 2026-09-23

- 实现最小 Agent HTTP daemon：支持可配置监听地址、`GET /healthz`、HTTP 超时及 SIGINT/SIGTERM 优雅关闭。
- 实现同步命令接口 `POST /v1/commands:run`，通过 CommandRunner 直接执行 argv，并返回独立的 stdout、stderr 和退出码。
- 扩展同步命令请求：支持 Cwd、Env、stdin、自定义超时和每流输出限制，并设置服务端默认值、硬上限与参数校验。
- 增加服务级命令并发限制：默认最多同时执行 4 条命令，可通过启动参数配置为 1–1024；容量耗尽时立即返回 HTTP 429，不排队或调用 CommandRunner。
- CommandRunner 增加 stdin、Cwd 和 Env 支持，保持 argv 与标准流的字面量传递语义。
- 增加 context、请求级 Timeout 和 Linux 进程组清理，取消时终止仍在同一进程组中的后代进程。
- 增加 stdout/stderr 独立字节上限，超限时取消整个命令进程组并返回可识别错误。
- 完善上述行为的单元测试、race 验证、代码注释规范和项目进度文档。

### 2026-09-22

- 在 `agent/` 下创建独立的 Go 1.25 module。
- 实现最小 CommandRunner，直接通过 argv 启动程序，不隐式使用 Shell。
- 支持独立写出 stdout 和 stderr，并区分非零退出码与命令启动失败。
- 添加空 argv、输出流、退出码、启动错误和参数原样传递测试。

### 2026-09-17

- 初始化项目说明、协作约定和通用 `.gitignore`。
- 记录项目目标、安全边界和小步迭代原则。

## 待开发功能

- 后台命令。
- 命令 ID。
- 查询命令运行状态。
- 增量读取命令日志。
- 主动中断指定命令。
- 输出文件保留与定期清理。
- Bash 持久会话。
- PTY 交互式终端。
- WebSocket 输入输出。
- 指定 UID/GID 运行命令。
- Agent 作为 PID 1 或 subreaper 回收孤儿进程。
- SIGTERM 优雅退出和信号转发。
- 对主动创建新会话或逃离进程组的后代进程进行可靠清理。
- 总输出共享额度、输出行数限制、磁盘配额、日志轮转或运行时动态调整额度。
- 主动 Agent 就绪探测、动态入口、续期、暂停、恢复、删除和 TTL 等生命周期流程。
- workspace 磁盘配额、更严格的隔离运行时、网络策略、认证和审计。
- 如需学习并复现 OpenSandbox 的可配置出口策略，另行设计 egress sidecar 持有网络命名空间与端口映射、sandbox 共享该命名空间，并通过 nftables/DNS 实施策略；当前阶段不实现。

## 待优化问题

- `/workspace` 命名卷仍可持续增长，尚无空间配额或自动清理策略。
- 文件 API 尚无 workspace 空间配额，也未通过持续高并发压测证明代理和 Agent 的实际内存峰值；当前仅验证并发上限、10 MiB 边界及代理 16 MiB 临时目录配置。
- SSE 当前仅支持一次 POST 前台流；断线、写失败或过慢客户端无法保证收到终止帧，也没有重连、事件重放或后台日志存储。
- Docker 示例的资源上限仍为固定值，尚未验证运行中命令在容器停止时的完整优雅关闭语义。
- 当前 CommandRunner 直接在 Agent 所在环境启动本地进程，不提供 sandbox 隔离。
- context 取消和 Timeout 使用 Linux 进程组及 `SIGKILL` 清理进程；当前没有 SIGTERM 宽限期，也无法清理通过 `setsid` 等方式逃离进程组的后代进程。
- 输出限制当前按 stdout 和 stderr 分别统计原始字节，不解释文本编码，也不限制调用方 writer 自身的存储方式。
- 命令并发限制当前仅作用于单个 Agent 进程，不提供跨实例、按用户或按优先级的调度能力。
- CommandRunner 测试面向 Linux/WSL，并依赖 `sh`、`printf`、`pwd` 和 `cat` 等系统程序；尚未覆盖其他平台。
- `Request.Env` 尚不支持删除变量；其中的 `PATH` 也不改变 `Argv[0]` 的初始可执行文件查找规则。
- 项目尚未建立正式的威胁模型，也未验证资源、文件系统和网络隔离边界。
- 控制面查询直接以 Docker 资源标签和状态为来源，尚无宿主动态入口或公开删除 API；异常退出后的资源恢复与回收仍需人工处理。
