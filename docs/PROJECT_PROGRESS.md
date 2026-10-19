# 项目开发进度

本文档记录 Agent Sandbox 已完成的开发工作、后续功能和已知优化事项。开发进度按日期倒序排列，最新记录位于最前。

## 开发进度

### 2026-10-19

- 实现 TTL 第一步：`SandboxService` 支持时钟注入，在调用 Runtime 创建前只读取一次带时区的 UTC 时间，生成 `created_at` 并按固定 1800 秒计算 `expires_at`。公共创建规格、创建和查询结果携带时间，API 统一输出 RFC 3339 UTC；POST 继续只接受空请求，不增加自定义 TTL 参数。
- Docker 在本次 internal、ingress、workspace、sandbox 和 proxy 五项资源上保存相同的 `io.agent-sandbox.created-at`、`io.agent-sandbox.expires-at` 标签，保留原有完整归属标签和创建、取消、精确逆序回滚语义。查询从 Docker 标签恢复原始时间，新 Runtime/Service 或控制面重启不会重算时间或推迟过期时间。
- 两项时间标签均不存在的旧实例返回两个 `null`，不补写或接管 TTL，原有查询、endpoint 和删除能力保留；仅缺一项、格式错误或过期早于创建时，状态查询返回稳定的 `sandbox_query_failed`。即使超过 `expires_at`，也只返回原始时间，状态、endpoint 和命令执行继续沿用现有语义，ready 仍只表示 Agent 健康。
- 本阶段只记录初始生命周期信息，没有过期扫描、自动删除、自动续期或后台任务，不改变默认删除保留 workspace 的策略。Docker 标签不是后续续期的可更新存储；续期需要另行设计可更新的持久化元数据。验收范围保持 WSL/Python 3.12，不扩展 Windows 适配。
- 本次 Python 3.12.3 默认回归 `uv run --locked pytest` 为 317 passed、3 skipped，覆盖固定时钟与精确 TTL、五资源标签一致、旧实例和损坏标签、创建/查询一致及重启恢复；带时间标签的普通失败和重复取消均按精确归属逆序回滚，原 endpoint/删除回归保留。显式 `RUN_DOCKER_INTEGRATION=1 uv run --locked pytest -m docker_integration -v` 为 3 passed（53.83s），核对真实五资源标签、新 Runtime/Service 恢复原时间、已过期实例仍 ready/入口及命令可用，并通过原双实例隔离与删除验收；最终只清理本次账本中精确 ID 与完整归属核验通过的资源。
- `uv run python -m compileall -q app.py api core runtime tests` 与 `git diff --check` 通过。默认 uv 缓存只读，使用 `/tmp` 缓存；受限环境线程测试停滞，完整回归和真实 Docker 验收在获准环境运行。未手动启动常驻 Uvicorn，也未运行本次未改动的 Go、Compose 或镜像构建检查，集成验收使用既有固定镜像；未扩展 Windows 验证。
- 按当前模块边界将同步删除代码合并进 `runtime/docker/transaction.py`，移除独立实现文件；`docker_runtime.py` 保留异步 delete、线程调度和取消等待。创建与删除共用 `_ResourceRecord`，两种清理流程仍独立。AST 对照确认原创建/回滚代码和五项删除函数等价（仅归一化资源记录类型名），Runtime 仅调整导入；合并后 WSL 默认回归 281 passed、3 skipped，Windows 删除专项 100 passed，compileall 与 `git diff --check` 通过；另显式复测真实 Docker 双实例删除验收 1 passed（23.76s），覆盖保留文件、显式删卷、B 不受影响及本次资源精确清理。
- 实现 `DELETE /v1/sandboxes/{sandbox_id}`，沿用 `API → SandboxService → SandboxRuntime → DockerSandboxRuntime`。默认删除 proxy、sandbox、ingress 和 internal 网络并保留 workspace；显式 `delete_workspace=true` 最后不可恢复地删除卷。合法未知 ID、重复请求和默认仅剩卷均返回 204 空体；无效 ID 在运行时前返回 404，归属冲突 409、发现时后端不可用 503、清理不完整 500 且含 sandbox_id。
- `runtime/docker/transaction.py` 的同步删除流程恢复与核验五角色清单，不依赖创建进程记录、容器健康、endpoint 或完整拓扑。完整标签、有效事务、跨角色事务一致及角色唯一性在操作前核验；每次 stop/remove 前按精确容器/网络 ID 或经核验的卷 Name 重新读取归属。复用归属工具，并加固 malformed Config/Labels 的读取；创建失败回滚策略保持不变。
- proxy → sandbox → ingress → internal → 可选 workspace 顺序回收；容器使用 2 秒停止宽限，失败后重验归属再强制移除该精确容器，网络/卷均不强制。消失视为完成，部分失败继续可安全执行的后续项并允许重试；每项移除后确认不存在，失败项在末尾再按精确标识确认，避免并发请求已完成回收却误报 500，也避免把改标后的资源误判为不存在。
- DELETE 不套用 1 秒查询超时，仍使用延迟创建的 5 秒 SDK client。通过 `asyncio.wait()` 等待 worker，首次及重复取消都等待线程回收与 client 关闭后传播取消，消费线程异常，不重建已经删除的资源。未增加数据库、进程墓碑、持久化删除状态、TTL 或后台任务。
- 新增 100 项 Docker 删除专项测试，另补 Service/API/集成清理安全测试。WSL Ubuntu 24.04、Python 3.12.3 最终 `uv run --locked pytest` 为 **281 passed、3 skipped**；compileall 和 `git diff --check` 通过。独立审查发现并修复并发终态误报，补充了失败资源消失与改标的红/绿回归测试。
- 最终显式 `RUN_DOCKER_INTEGRATION=1 uv run --locked pytest -m docker_integration -v` 为 **3 passed，53.65s**。新 app/runtime 删除 A `sbx_88663c546eb736e7a6e54fa2ad128843` 后查询 404、旧入口失效；默认保留卷中含 `0x00/0xff` 的文件通过只读受限 helper 的 Base64 输出逐字节核验，再显式删除 workspace。B `sbx_97c149c38163d816ef55ea4774728ffd` 仍 ready，文件和命令正常；最终 A/B 与 helper 的 11 项本次记录资源均按精确标识确认不存在。
- 手动实际 HTTP 验收使用本次启动的 `127.0.0.1:18084`：指定 18083 已被既有 Windows Uvicorn 占用，未停止或重建它。A `sbx_e7f04b7de6497e9a222fd6d3c2b208a2` 的入口 `44422` 关闭、查询 404，四项运行资源消失，默认卷及二进制文件保留后再显式删卷；B `sbx_ae00262c939382dba1235e82c9939427` 的入口 `43488` 健康 200、命令退出码 0、文件保持独立。最终 11 项本次精确资源全不存在；既有容器/网络/卷快照前后相同（2/14/31 项）。本次 18084 控制面已关闭。
- 更新 API、curl 示例、README、控制面和 Docker 实现说明，[删除设计与实施计划](docker/DELETION.md) 记录契约、安全边界、失败收敛和验收。默认保留卷持续占用磁盘，调用者需保存 sandbox ID 并决定显式删除时间；卷配额、TTL 和后台回收仍待设计。
- 验证使用已存在的两个固定镜像，没有修改 Agent、Compose、Dockerfile 或依赖/锁文件；本次未重跑 Go test/race/vet、Compose config 或镜像构建。额外 Windows Python 3.14.3 全套结果为 **275 passed、6 failed、3 skipped**，删除专项 100 项全部通过；失败集中在既有创建路径的 shield 取消异常报告，WSL 同名测试均通过，未扩大本次范围修改创建取消适配。

Windows Python 3.14.3 失败项均位于 `control_plane/tests/test_sandbox_runtime.py`：

- `test_cancel_stops_before_next_create_step_and_waits_for_rollback`
- `test_cancel_during_start_removes_started_or_created_resources`
- `test_cancel_does_not_leave_unhandled_thread_exception`
- `test_cancel_with_incomplete_cleanup_raises_cleanup_failed`
- `test_cancel_during_proxy_start_reverses_all_resources`
- `test_cancel_during_proxy_connect_waits_for_complete_rollback`

### 2026-10-09

- 新增 [Docker Runtime 设计与实现说明](docker/README.md)，结合实际代码记录分层契约、独立网络与代理、固定资源限制、创建清单、精确逆序回滚、重复及提交后取消、内部状态和实际入口查询、错误映射与验证方式；明确线程取消、进程重启恢复和未持久化事务的边界。
- 为动态实例增加专属 proxy 和 ingress bridge：sandbox 仅连接 internal、设置 `sandbox` DNS 别名且不发布端口；proxy 双网络、只向 `127.0.0.1` 动态发布 Agent HTTP，workspace 仅挂载 sandbox。新增 `Dockerfile.proxy` 打包既有配置并直接以 nginx 用户启动，沿用非 root、只读根、drop ALL、no-new-privileges 和 CPU/内存/PID/tmp 限制。
- 创建前检查 Agent/代理镜像均存在，不自动 pull/build。新增 proxy/ingress 角色接入精确 ID 记录、完整标签读取和逆序回滚；覆盖创建、连接、启动失败与取消，保留提交后取消及重复取消等待。
- 新增 `GET /v1/sandboxes/{sandbox_id}/endpoint`，统一经 Service/Runtime；只读恢复实际 `NetworkSettings.Ports`，核验两容器与两网络归属、事务、连接、DNS 和代理健康。未知/无效 ID 返回 404，旧实例无代理或入口不可用返回稳定 503，超时返回稳定 504；ready 仍表示内部 Agent 健康。
- 已构建两个镜像；默认测试 150 passed、2 skipped。真实 Docker 两项集成测试通过（30.37s）：双实例端口 `44246`、`44232`，健康/命令/上传/下载分别返回 200/200/201/200，SSE 首帧约 4ms、2ms；新 app/runtime 可恢复两地址，工作区文件互不串用。
- 公网对照 `1.1.1.1:80`：普通 bridge TCP 成功（退出码 0），两个 internal sandbox 均被拒绝（退出码 1）。双实例测试的 11 项本次资源（含 bridge 对照容器）已按精确 ID 和完整归属核验清理，并确认不存在；既有开发实例未修改或重建。
- `uv sync --locked`、compileall、Go test/race/vet、Compose config 和 `git diff --check` 通过；控制面实际监听 `127.0.0.1:18083` 时 healthz/readyz 返回 200，无效 endpoint ID 返回 404，本次控制面进程已关闭。独立审查无阻断项，补充了归属及误发布端口测试，并修复 malformed 事务标签和 client 关闭异常的稳定错误映射。
- endpoint 只承诺本机 WSL 开发。删除、异常退出资源回收、TTL、续期、暂停/恢复、认证和任意用户服务端口仍待开发。

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
- TTL 过期扫描、自动资源回收与保留 workspace 的回收策略。
- 续期接口及可更新的持久化生命周期元数据；当前 Docker 标签仅保存初始时间。
- 主动 Agent 就绪探测、暂停和恢复等生命周期流程。
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
- 控制面查询和手动删除直接以 Docker 资源标签和状态为来源，已支持本机专属入口和受管残留删除；异常退出后的自动恢复与回收仍待实现，默认保留卷需显式删除。
- 初始 TTL 目前只记录计划过期时间，不自动执行；Docker 标签不适合作为后续续期的可更新持久化元数据，需单独设计存储、失败与恢复语义。
- Windows Python 3.14 下既有创建路径的 `asyncio.shield()` 取消异常报告与 WSL Python 3.12 不同，上述六项创建取消测试仍失败；当前正式开发与验收环境为 Linux/WSL Python 3.12。
