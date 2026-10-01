
# Agent Sandbox

一个从零开始探索 AI Agent sandbox 设计与实现的项目。

## 项目状态

项目当前处于早期实现阶段。`agent/` 是一个使用 Go 1.25 的独立 module，包含 HTTP daemon 和 CommandRunner。HTTP 服务提供健康检查及同步命令执行接口；CommandRunner 支持 argv、标准流、工作目录、环境变量、超时、进程组清理和单流输出限制。

CommandRunner 在 Agent 所在环境中创建进程。现在可以将 Agent 放在最小 Docker 容器中运行，但尚未建立可安全运行不可信代码的隔离边界。HTTP 服务仅使用 Go 标准库。

## 为什么做这个项目

AI Agent 可能需要执行命令、修改文件、运行代码、访问网络或启动用户服务。一个可用的 sandbox 不只是启动一个容器，还需要回答：

- 工作负载与宿主机之间的隔离边界在哪里？
- 如何限制 CPU、内存、进程数、磁盘和运行时间？
- 如何安全地执行命令并实时返回输出？
- 如何暴露服务 endpoint，同时避免未经授权的访问？
- 如何控制 sandbox 的出站网络？
- 创建失败、进程崩溃、服务重启和超时后如何清理或恢复？

本项目优先学习和验证这些基础问题，不以一开始复现完整商业平台为目标。

## 设计方向（尚未冻结）

后续实现可能围绕以下概念展开：

- 生命周期控制面：创建、查询、续期、暂停、恢复和删除 sandbox。
- sandbox 数据面：在隔离环境内执行命令、文件操作和 Agent 任务。
- 运行时适配：将统一的 sandbox 请求转换为具体的容器或其他隔离运行时。
- 网络和访问控制：限制出站网络，并对公开 endpoint 做认证。
- 可靠性：状态、TTL、超时、取消、回滚、清理和重启恢复。
- 可观察性：日志、指标、事件和可诊断的失败原因。

这些是学习方向，不是已经承诺的目录结构或技术选型。新增技术方案前，应先说明为什么需要它，以及它解决了哪个已验证的问题。

## 推荐的第一个垂直切片

建议先只实现一条最小、可测试的链路：

```text
启动一个受限的本地 sandbox
    -> sandbox 内运行一个执行 agent
    -> 通过 HTTP 执行一个命令
    -> 返回 stdout、stderr 和退出码
    -> 支持 timeout 和取消
    -> 删除 sandbox 并确认资源已清理
```

在这条链路稳定之前，暂不同时引入多种运行时、多语言 SDK、复杂网络代理、快照、资源池或微 VM。

目前已完成基础命令执行、取消、同进程组清理、同步 HTTP 命令接口、单容器运行验证，以及宿主侧控制面的健康检查和最小 sandbox 创建接口；尚未实现完整的 sandbox 生命周期闭环。

## 当前如何开始

当前实现和测试面向 Linux/WSL，需要安装 Go 1.25。开始参与前请先阅读：

- [AGENTS.md](AGENTS.md)：Codex 和贡献者的工作约定。
- [项目开发进度](docs/PROJECT_PROGRESS.md)：已完成工作、待开发功能和待优化问题。
- [curl 手动测试](docs/CURL_TESTS.md)：覆盖命令参数与文件上传、处理、下载的可复制命令。
- [Control 控制面设计与启动](docs/CONTROL_PLANE.md)：说明宿主侧控制面的边界、组件、启动方式和健康检查。

### 启动宿主侧控制面

`control/` 是运行在 WSL 宿主侧的独立 Python/FastAPI 进程，与容器内负责命令和文件操作的 Go Agent 分离。当前控制面提供自身健康检查、只读 Docker 可用性检查和固定策略的 sandbox 创建接口；尚不提供查询、Agent 就绪检查、动态入口或删除接口。

使用 Python 3.12 和 `uv` 安装依赖并仅监听本机 `127.0.0.1:18083`：

```bash
cd ~/agent-sandbox/control
uv sync
uv run uvicorn control.app:app --host 127.0.0.1 --port 18083
```

在另一个终端检查接口：

```bash
curl -i http://127.0.0.1:18083/healthz
curl -i http://127.0.0.1:18083/readyz
```

`GET /healthz` 只反映控制面 HTTP 进程自身，正常返回 HTTP 200 和 `{"status":"ok"}`。`GET /readyz` 只执行 Docker ping；Docker 可用时返回 HTTP 200 和 `{"status":"ready"}`，不可用或超时时返回 HTTP 503 和稳定的 `docker_unavailable` 错误，不返回 socket 路径或底层异常。Docker 不可用不会阻止控制面启动。

创建一个固定配置的 sandbox 时，请发送没有请求体的 POST：

```bash
curl -i -X POST http://127.0.0.1:18083/v1/sandboxes
```

成功返回 HTTP 201，例如：

```json
{"sandbox_id":"sbx_0123456789abcdef0123456789abcdef","status":"started"}
```

`started` 只表示 Docker 已完成容器启动调用，不表示容器内 Agent 已就绪。请求不能指定镜像、命令、挂载或 Docker 参数；任何非空请求体都会返回 HTTP 400。控制面固定使用已经存在的 `agent-sandbox:dev` 镜像，不自动 pull 或 build。每次创建使用不可预测的 sandbox ID，并创建专属 internal bridge、workspace volume 和容器；三类资源带有一致的项目、sandbox ID、受管标记和资源类型标签。容器沿用非 root、1 CPU、256 MiB 内存、无额外 swap、64 PID、只读根文件系统、32 MiB `/tmp`、drop ALL capabilities 与 no-new-privileges 基线。

新容器只连接自己的 internal 网络，且不发布任何宿主机端口，因此当前无法从 WSL 直接调用其 Agent。创建失败时，控制面只按本次事务记录的资源 ID 和完整标签逆序回滚；错误响应使用稳定的 `sandbox_image_unavailable`、`docker_unavailable`、`sandbox_resource_conflict`、`sandbox_create_failed` 或 `sandbox_cleanup_failed` code，不返回 Docker 异常细节。回滚不完整时响应包含 sandbox ID，供人工定位受管残留。

Docker socket 只供 WSL 宿主侧控制面访问，绝不能挂载进 sandbox 或入口代理。控制面当前没有认证，只能作为本机开发入口，不得暴露到不可信网络。当前也没有公开查询或删除 API；创建成功后需要人工按照完整标签核对资源，不能按名称前缀批量清理。

### 启动 Go Agent

在 `agent/` 目录运行验证：

```bash
go test ./...
go test -race ./...
go vet ./...
```

启动 Agent HTTP 服务：

```bash
go run .
```

服务默认监听 `127.0.0.1:8080`。可以使用 `-listen` 修改地址；使用下例时，请将后续请求端口同步改为 18082，避免与本地容器入口的 18081 冲突：

```bash
go run . -listen 127.0.0.1:18082
```

健康检查：

```bash
curl -i http://127.0.0.1:8080/healthz
```

`GET /healthz` 返回 `200 OK` 和 `{"status":"ok"}`；该路径的其他方法返回 `405 Method Not Allowed`。SIGINT 和 SIGTERM 会触发最长 5 秒的优雅关闭。

同步命令接口 `POST /v1/commands:run` 接收 argv，以及可选的 `cwd`、`env`、`stdin`、`timeout_ms` 和 `max_output_bytes_per_stream`。默认最多并发执行 4 条命令，可用 `-max-concurrent-commands` 配置为 1–1024；额度已满时返回 HTTP 429。

前台流式接口 `POST /v1/commands:stream` 使用同一 JSON 请求字段、校验、超时、每流输出上限和命令并发额度，响应为 `text/event-stream`；同步接口格式不变。可在容器启动后用下面的命令观察实时输出：

```bash
curl -N -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf out; sleep 1; printf err >&2"]}' \
  http://127.0.0.1:18081/v1/commands:stream
```

SSE 的 `stdout`、`stderr` 事件分别携带 `{"data_base64":"..."}`，每个数据块以 Base64 编码原始字节；块边界不保证对应字符或行。命令正常退出及用户程序非零退出均以 `complete` 事件结束，数据为 `{"exit_code":0}` 或真实非零退出码。启动失败、超时和输出超限以 `failure` 事件结束，数据为 `{"error":{"code":"...","message":"..."}}`。流开始前的无效请求或并发额度不足仍返回原有 JSON HTTP 错误；流开始后 HTTP 状态已固定为 200，客户端断开则无法保证收到终止事件。该接口需要 POST，不能直接使用浏览器原生 `EventSource` 调用。SSE 响应在请求解析后重新设置 35 秒写截止时间，长于最大 30 秒命令超时；本地代理读超时为 40 秒且关闭此路由的响应缓冲。

单文件 API 使用相对 `/workspace` 的 `path` 查询参数：`POST /v1/files?path=...` 上传原始字节并创建新文件（成功返回 201，同名返回 409）；`GET /v1/files?path=...` 下载原始字节（`application/octet-stream`，不存在返回 404）。上传和下载分别最多 10 MiB（10,485,760 字节），超过时返回 413。路径不得是绝对路径、包含 `..` 或通过符号链接逃出工作区；父目录必须已存在，接口不提供列目录、删除或覆盖功能。

上传先流式写入工作区内的随机临时文件，完整写入并关闭后才以不覆盖已有文件的方式发布；读取、写入和发布失败时会清理临时文件。文件上传和下载共用独立的 2 个并发额度，用尽时不排队，返回 HTTP 429 和 `file_capacity_exceeded`。本地代理对该路径也限制为 2 个并发请求，并关闭请求体及响应缓冲，避免多个 10 MiB 请求先堆积在代理的 16 MiB `/tmp` 中；sandbox 仍受 256 MiB 容器内存上限约束。这些限制不是 workspace 磁盘配额。

容器启动后，可用下面的例子完成“上传 → 命令处理 → 下载”：

```bash
printf 'hello' | curl -i --data-binary @- \
  'http://127.0.0.1:18081/v1/files?path=input.txt'
curl -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","tr a-z A-Z < input.txt > output.txt"]}' \
  http://127.0.0.1:18081/v1/commands:run
curl 'http://127.0.0.1:18081/v1/files?path=output.txt'
```

第二次上传同名文件会返回 409；重复试验需选用新文件名。文件 API 的 `/workspace` 路径约束**不限制**现有 CommandRunner：命令仍可访问 Agent 用户有权限访问的其他路径。本地入口没有认证，不能向不可信网络发布。
直接在宿主机使用 `go run .` 时也需要提供 `/workspace` 目录，否则文件接口会返回 workspace 不可用错误。

### 在 Docker 中运行

在 `agent/` 目录构建镜像，并用命名卷保存 `/workspace`：

```bash
docker build -t agent-sandbox:dev .
docker volume create agent-sandbox-workspace
./run-local.sh
curl -H 'Content-Type: application/json' \
  -d '{"argv":["pwd"]}' \
  http://127.0.0.1:18081/v1/commands:run
```

脚本通过 Docker Compose 管理 sandbox 和固定上游入口代理，重复运行会复用或更新这两个容器。sandbox 使用 `--init`、只读根文件系统、1 核 CPU 配额、256 MiB 内存、无额外 swap 和整个容器最多 64 个 PID。`/workspace` 是持久化的可写命名卷，`/tmp` 是不跨容器重建保留、上限为 32 MiB 的可写 tmpfs（同时计入容器内存使用量）。可检查 Docker 配置及当前环境的 cgroup v2 限制：

修改 `proxy.conf` 后，如果正在运行的代理仍使用旧的 bind mount 内容，可执行 `docker compose -f compose.local.yml up -d --force-recreate --no-deps proxy` 仅重建代理；`./smoke-local.sh` 会核对容器内实际配置与本地文件是否一致。

```bash
docker inspect agent-sandbox-dev \
  --format 'Memory={{.HostConfig.Memory}} MemorySwap={{.HostConfig.MemorySwap}} NanoCpus={{.HostConfig.NanoCpus}} PidsLimit={{.HostConfig.PidsLimit}}'
docker exec agent-sandbox-dev sh -c \
  'cat /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.swap.max /sys/fs/cgroup/pids.max /sys/fs/cgroup/cpu.max'
docker inspect agent-sandbox-dev \
  --format 'ReadonlyRootfs={{.HostConfig.ReadonlyRootfs}} Tmpfs={{json .HostConfig.Tmpfs}}'
docker inspect agent-sandbox-dev \
  --format 'CapDrop={{json .HostConfig.CapDrop}} SecurityOpt={{json .HostConfig.SecurityOpt}}'
```

本机 cgroup v2 的预期值依次为 `268435456`、`0`、`64` 和 `100000 100000`；其他环境应核对 CPU 配额与周期之比为 1。CPU 配额不绑定某个物理核心，PID 上限作用于整个容器。镜像中的 Agent 和命令以非 root 的 `sandbox` 用户运行，默认工作目录是可写的 `/workspace`。sandbox 仅连接 `agent-sandbox-internal` 内部 bridge，不发布宿主机端口；代理连接内部和普通 bridge，仅将 `127.0.0.1:18081` 转发到 Agent 的 `8080` 端口。停止并重建后，原有内部网络和 workspace 命名卷仍保留。此方案不等于完整的网络隔离或 OpenSandbox 的出口策略实现。

脚本还使用 `--cap-drop=ALL` 移除全部 Linux capabilities，并用 `--security-opt=no-new-privileges:true` 禁止执行新程序时获得额外权限。除核对 Docker 配置，还应通过 HTTP 检查实际启动的用户命令：

```bash
curl -s -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","grep -E \"^(CapEff|CapBnd|NoNewPrivs):\" /proc/self/status"]}' \
  http://127.0.0.1:18081/v1/commands:run
```

响应中的 `CapEff` 和 `CapBnd` 应均为全零，`NoNewPrivs` 应为 `1`。`CapBnd` 为零表示后续执行程序也不能从能力边界重新取得 capabilities；这仍不等于完整的 sandbox 安全隔离。

启动容器后，可验证可写区域；在确认 `/home/sandbox` 存在且归 `sandbox` 所有后，再验证只读根文件系统拒绝该目录的写入：

```bash
docker exec agent-sandbox-dev sh -c 'ls -ld /home/sandbox; id'
curl -s -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf workspace > /workspace/persist.txt; printf temporary > /tmp/temp.txt; cat /workspace/persist.txt /tmp/temp.txt"]}' \
  http://127.0.0.1:18081/v1/commands:run
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf blocked > /home/sandbox/rootfs-check.txt"]}' \
  http://127.0.0.1:18081/v1/commands:run
```

第二个请求的 HTTP 状态仍是 200，但命令退出码应非零，stderr 应提示只读文件系统。执行 `docker compose -f compose.local.yml down` 后再次运行 `./run-local.sh`，应仍能读取 `/workspace/persist.txt`，而 `/tmp/temp.txt` 应不存在。检查结束后再次执行 `docker compose -f compose.local.yml down`；外部内部网络和命名卷不会因此被删除。只读根文件系统限制写入位置，不限制命令读取其他可访问路径；命名卷本身也没有磁盘配额。

CommandRunner 当前提供以下最小接口：

```go
var stdout bytes.Buffer
result, err := command.Run(
	context.Background(),
	command.Request{
		Argv:                    []string{"printf", "%s", "hello"},
		Cwd:                     "/tmp",
		Env:                     map[string]string{"EXAMPLE": "literal value"},
		Timeout:                 5 * time.Second,
		MaxOutputBytesPerStream: 1024,
	},
	nil,
	&stdout,
	nil,
)
```

`Argv` 会直接传递给目标程序，不经过隐式 Shell。独立的 stdin 参数为 nil 时子进程读取到 EOF；`Cwd` 为空时继承当前工作目录；`Env` 为空时继承 Agent 环境，非空时在继承环境上覆盖或新增变量。`Timeout` 为零时只使用调用方 context，正数时创建请求级 deadline。`MaxOutputBytesPerStream` 分别限制 stdout 和 stderr，零值表示不限制。

程序正常结束或返回非零状态时，退出码通过 `Result` 返回且 `error` 为 nil；程序无法启动时返回退出码 `-1` 和非 nil error。调用方取消、超时或输出超限返回 `ExitCode=-1`，错误可以通过 `errors.Is` 区分。Linux/WSL 下取消会向命令的独立进程组发送 `SIGKILL`。

当前限制包括：

- HTTP 命令接口尚未提供认证、TLS 或后台任务；请勿对不可信网络开放。
- 命令取消仅清理仍在同一进程组中的进程；尚未实现 SIGTERM 宽限期、信号转发或逃逸进程清理。
- 输出限制按 stdout 和 stderr 分别统计原始字节，不提供共享额度、磁盘配额或日志轮转。
- 不支持环境变量删除语义或环境变量文件。
- `Request.Env` 中的 `PATH` 只影响子进程环境，不改变 `Argv[0]` 的初始查找规则。
- Docker 示例使用固定的 CPU、内存、swap 和 PID 上限，以及只读根文件系统；尚无 workspace 磁盘配额、认证或完整威胁模型，不能用于安全运行不可信代码。

## 安全声明

当前项目尚未达到可运行不可信代码的生产安全标准。不要把当前版本部署到生产环境，也不要把宿主机 Docker socket、云凭证、生产密钥或任意宿主机目录交给 sandbox 工作负载。

任何“安全隔离”结论都必须建立在明确的威胁模型、隔离机制说明和攻击性验证之上。普通容器、进程隔离、网络策略和微 VM 的安全边界不同，不能混为一谈。

## 使用 Codex 协作

建议将工作拆成可独立验收的小任务：

1. 先分析现有文件和目标，不预先假定技术栈。
2. 明确本次只实现的垂直切片和不实现的内容。
3. 先补测试或验收标准，再编写实现。
4. 运行与改动范围匹配的验证。
5. 在 README 或设计文档中记录已经确定的约束。

不要直接要求一次性“完整复现一个 sandbox 平台”。应优先让每个阶段都能运行、观察和解释。

## 许可证

项目尚未选择许可证，因此当前不包含 `LICENSE` 文件。正式公开发布前需要由项目维护者选择并添加合适的开源许可证。
