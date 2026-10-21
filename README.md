# Agent Sandbox

面向 AI Agent 工作负载的 sandbox 实验项目。通过宿主侧 Python 控制面管理 Docker 实例，在容器内使用 Go Agent 执行命令、传输文件和返回实时输出。

项目处于早期开发阶段，当前面向 Linux/WSL 本机开发，尚不具备运行不可信代码的生产安全保障。

## 当前功能

- **实例管理**：创建、状态查询、动态入口查询、主动就绪检查和手动删除，支持控制面重启后查询。
- **命令执行**：同步 HTTP 与前台 SSE，支持工作目录、环境变量、标准输入、超时、并发和输出限制。
- **文件传输**：workspace 内上传和下载，单次上限 10 MiB，拒绝覆盖和路径越界。
- **生命周期管理**：默认 1800 秒 TTL、显式续期、SQLite 持久化和后台周期过期回收，查询返回当前过期时间。
- **容器约束**：非 root、只读根文件系统、CPU/内存/进程数限制，以及独立网络和工作区。

控制面启动后自动回收到期实例，默认每轮结束后等待 60 秒，每轮最多处理 100 条，保留 workspace。回收可能因扫描间隔、处理耗时或失败重试而延迟；没有公开回收 API。

## 快速开始

运行需要 Linux/WSL、Docker、Python 3.12、[uv](https://docs.astral.sh/uv/) 和 curl。Docker daemon 需已启动，当前用户需有访问权限；开发 Go Agent 另需 Go 1.25。

### 1. 构建镜像

在仓库根目录执行，控制面不会自动构建或拉取镜像：

```bash
docker build -t agent-sandbox:dev ./sandbox_agent
docker build -t agent-sandbox-proxy:dev -f deploy/local/Dockerfile.proxy deploy/local
```

### 2. 启动控制面

```bash
cd control_plane
uv sync --locked
uv run uvicorn control_plane.app:app --app-dir .. --host 127.0.0.1 --port 18083
```

默认数据库为 `control_plane/.local/lifecycle.sqlite3`，启动时初始化，路径不随工作目录变化。当前使用 schema 2，仅支持新建或同版本数据库，不提供旧版本迁移。

### 3. 创建并使用 sandbox

在另一个终端执行以下命令。创建接口只接受空请求，不接受 `{}` 或自定义 Docker 参数。

```bash
CONTROL_URL=http://127.0.0.1:18083
curl -fsS "$CONTROL_URL/readyz"

SANDBOX_ID=$(curl -fsS -X POST "$CONTROL_URL/v1/sandboxes" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["sandbox_id"])')
echo "$SANDBOX_ID"
curl -fsS "$CONTROL_URL/v1/sandboxes/$SANDBOX_ID"
```

创建成功返回 `started` 和 `created_at`、`expires_at`。`ready` 表示内部 Agent 健康，入口需单独查询；下面的命令会在入口尚未就绪时有限重试：

```bash
ENDPOINT=$(curl -fsS --retry 10 --retry-delay 1 \
  "$CONTROL_URL/v1/sandboxes/$SANDBOX_ID/endpoint" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["url"])')

curl -fsS -H 'Content-Type: application/json' \
  -d '{"argv":["pwd"]}' "$ENDPOINT/v1/commands:run"
```

命令和文件请求发送到 `ENDPOINT`。命令响应包含 stdout、stderr 和 exit_code；更多用法见 [Sandbox Agent API 与 curl 示例](docs/sandbox_agent/API.md)。

尚未到期的受管实例可显式续期：

```bash
curl -fsS -H 'Content-Type: application/json' \
  -d '{"ttl_seconds":1800}' "$CONTROL_URL/v1/sandboxes/$SANDBOX_ID/renew"
```

`ttl_seconds` 必须为 1～86400 的整数，表示从本次校验时刻起至少保留的秒数；不会在原期限上累加，也不会缩短期限。已到期或进入删除流程时拒绝续期；取消请求后可能已提交，应查询确认结果。

### 4. 删除 sandbox

默认删除运行资源并保留 workspace，成功返回 204：

```bash
curl -i -X DELETE "$CONTROL_URL/v1/sandboxes/$SANDBOX_ID"
```

确认不再需要文件后，对同一个 ID 显式删卷；该操作不可恢复：

```bash
curl -i -X DELETE "$CONTROL_URL/v1/sandboxes/$SANDBOX_ID?delete_workspace=true"
```

保留的 workspace 不会自动回收，请保存 sandbox ID 以便后续删除。

## 项目结构

```text
control_plane/   Python/FastAPI 控制面与 SQLite 生命周期存储
sandbox_agent/   Go 命令、文件和 SSE 服务
deploy/local/    本地 Compose、代理与脚本
docs/            API、设计、使用示例和开发进度
```

## 开发与测试

在 `control_plane/` 执行：

```bash
uv run --locked pytest
uv run python -m compileall -q app.py api core runtime storage tests
```

在 `sandbox_agent/` 执行：

```bash
go test ./...
go test -race ./...
go vet ./...
```

默认测试不修改 Docker 资源。需要真实 Docker 验证时，在 `control_plane/` 显式执行：

```bash
RUN_DOCKER_INTEGRATION=1 uv run --locked pytest -m docker_integration -v
```

集成测试会创建临时实例，只清理本次创建且归属核验通过的资源。在仓库根目录执行 `git diff --check` 检查补丁格式。

## 使用限制

- HTTP 接口没有认证，仅限本机访问，不得暴露到不可信网络。
- Docker socket 仅供宿主侧控制面访问，不得将其或宿主凭证挂载进 sandbox。
- 容器限制尚未构成经过完整验证的安全隔离，workspace 也没有磁盘配额。
- Docker 与 SQLite 的部分失败仍可能需要重试或人工核验；暂停、恢复和后台命令尚未实现。
- 周期回收以单进程、单 worker 为前提，没有多 worker 协调；未确认创建的记录和无记录旧资源不自动接管。

## 文档

- [文档目录与阅读导航](docs/README.md)
- [Sandbox Agent 设计](docs/sandbox_agent/DESIGN.md)、[API 与 curl 示例](docs/sandbox_agent/API.md)
- [控制面启动与设计](docs/control_plane/DESIGN.md)
- [控制面 API](docs/control_plane/API.md)
- [Docker 设计](docs/control_plane/docker/DESIGN.md)、[实现与验收](docs/control_plane/docker/IMPLEMENTATION.md)
- [目录与职责](docs/REPOSITORY_LAYOUT.md)
- [开发进度与后续计划](docs/PROJECT_PROGRESS.md)
- [贡献与协作约定](AGENTS.md)

## 许可证

项目尚未选择许可证，当前没有 `LICENSE` 文件。
