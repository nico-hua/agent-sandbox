# Agent Sandbox

面向 AI Agent 工作负载的 sandbox 实验项目。通过宿主侧 Python 控制面管理实例，默认使用 Docker，可显式选择当前仅支持 basic 的 Kubernetes 后端；在容器内使用 Go Agent 执行命令、传输文件和返回实时输出。

项目处于早期开发阶段，当前面向 Linux/WSL 本机开发，尚不具备运行不可信代码的生产安全保障。

## 当前功能

- **控制面认证**：默认要求单管理员 Bearer API key，仅精确 `GET /healthz` 匿名；新动态实例入口使用独立 Access Token。
- **实例管理**：创建、状态查询、动态入口查询、主动就绪检查和手动删除，支持控制面重启后查询。
- **命令执行**：同步 HTTP 与前台 SSE，支持工作目录、环境变量、标准输入、超时、并发和输出限制。
- **文件传输**：workspace 内上传和下载，单次上限 10 MiB，拒绝覆盖和路径越界。
- **生命周期管理**：默认 1800 秒 TTL、显式续期、SQLite 持久化和后台周期过期回收，查询返回当前过期时间。
- **Python SDK**：独立同步客户端，封装实例生命周期、命令、文件与显式工作区顺序复用，见 [SDK 使用说明](docs/sdk/python.md)。
- **容器约束**：非 root、只读根文件系统、CPU/内存/进程数限制，以及独立网络和工作区。

控制面启动后自动回收到期实例，默认每轮结束后等待 60 秒，每轮最多处理 100 条，保留 workspace。回收可能因扫描间隔、处理耗时或失败重试而延迟；没有公开回收 API。

## 快速开始

运行需要 Linux/WSL、Docker、Python 3.12、[uv](https://docs.astral.sh/uv/) 和 curl。Docker daemon 需已启动，当前用户需有访问权限；开发 Go Agent 另需 Go 1.25。

### 1. 构建镜像

在仓库根目录执行，控制面不会自动构建或拉取镜像：

```bash
docker build -t agent-sandbox-basic:dev ./sandbox_agent
docker build -t agent-sandbox-proxy:dev -f deploy/local/Dockerfile.proxy deploy/local
```

默认环境为 basic；Python、Node 预制镜像的构建命令见[镜像说明](docs/control_plane/docker/IMPLEMENTATION.md#preset-images)，创建时通过 `environment` 选择。

### 2. 启动控制面

```bash
cd control_plane
uv sync --locked
export AGENT_SANDBOX_RUNTIME=docker
export AGENT_SANDBOX_DATABASE_PATH="$PWD/.local/lifecycle.sqlite3"
export AGENT_SANDBOX_API_KEY="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
uv run uvicorn control_plane.app:app --app-dir .. --host 127.0.0.1 --port 18083
```

此命令在环境变量中生成随机管理员 key，不输出其值。所有调用控制面的终端必须配置同一个 `AGENT_SANDBOX_API_KEY`；不要另行生成不同 key，也不要将它写入代码、日志或提交到仓库。缺少 key 会使正常启动失败；`/docs`、`/redoc` 和 `/openapi.json` 均关闭。key 配置与显式开发模式见[认证与启动](docs/control_plane/DESIGN.md#控制面认证与启动配置)。

默认数据库为 `control_plane/.local/lifecycle.sqlite3`，启动时初始化，路径不随工作目录变化。当前使用 schema 4；schema 2/3 必须停服、备份并持有运行锁后[显式升级](docs/control_plane/DESIGN.md#显式-schema-升级)，启动不会自动迁移。

### 3. 创建并使用 sandbox

在另一个终端执行以下命令。创建接口支持空请求或受约束的 CPU、内存和初始 TTL，详见[创建 API](docs/control_plane/API.md#4-创建-sandbox)；不接受其他 Docker 参数。

```bash
CONTROL_URL=http://127.0.0.1:18083
curl -H "Authorization: Bearer $AGENT_SANDBOX_API_KEY" -fsS "$CONTROL_URL/readyz"

SANDBOX_ID=$(curl -H "Authorization: Bearer $AGENT_SANDBOX_API_KEY" -fsS -X POST "$CONTROL_URL/v1/sandboxes" \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["sandbox_id"])')
echo "$SANDBOX_ID"
curl -H "Authorization: Bearer $AGENT_SANDBOX_API_KEY" -fsS "$CONTROL_URL/v1/sandboxes/$SANDBOX_ID"
```

创建成功返回 `started` 和 `created_at`、`expires_at`。`ready` 表示内部 Agent 健康，入口需单独查询；下面的命令会在入口尚未就绪时有限重试：

```bash
CONNECTION=$(curl -H "Authorization: Bearer $AGENT_SANDBOX_API_KEY" -fsS --retry 10 --retry-delay 1 \
  "$CONTROL_URL/v1/sandboxes/$SANDBOX_ID/endpoint")
ENDPOINT=$(printf '%s' "$CONNECTION" | python3 -c 'import json,sys; print(json.load(sys.stdin)["url"])')
ACCESS_TOKEN=$(printf '%s' "$CONNECTION" | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
unset CONNECTION

curl -fsS -H "Authorization: Bearer $ACCESS_TOKEN" -H 'Content-Type: application/json' \
  -d '{"argv":["pwd"]}' "$ENDPOINT/v1/commands:run"
```

命令和文件请求发送到 `ENDPOINT`，携带该实例 Token，不能使用控制面管理员 key；不要输出或提交 Token。命令响应包含 stdout、stderr 和 exit_code；更多用法见 [Sandbox Agent API 与 curl 示例](docs/sandbox_agent/API.md)。

尚未到期的受管实例可显式续期：

```bash
curl -H "Authorization: Bearer $AGENT_SANDBOX_API_KEY" -fsS -H 'Content-Type: application/json' \
  -d '{"ttl_seconds":1800}' "$CONTROL_URL/v1/sandboxes/$SANDBOX_ID/renew"
```

`ttl_seconds` 必须为 1～86400 的整数，表示从本次校验时刻起至少保留的秒数；不会在原期限上累加，也不会缩短期限。已到期或进入删除流程时拒绝续期；取消请求后可能已提交，应查询确认结果。

### 4. 删除 sandbox

默认删除运行资源并保留 workspace，成功返回 204：

```bash
curl -H "Authorization: Bearer $AGENT_SANDBOX_API_KEY" -i -X DELETE "$CONTROL_URL/v1/sandboxes/$SANDBOX_ID"
```

确认不再需要文件后，对同一个 ID 显式删卷；该操作不可恢复：

```bash
curl -H "Authorization: Bearer $AGENT_SANDBOX_API_KEY" -i -X DELETE "$CONTROL_URL/v1/sandboxes/$SANDBOX_ID?delete_workspace=true"
```

保留的 workspace 不会自动回收，请保存 sandbox ID 以便后续删除。

### 可选：WSL 本机 Kubernetes 后端

这是独立于上述 Docker 工作流的启动方式。需要已有可访问的集群、namespace、静态证书 kubeconfig，以及节点中已缓存的 Agent/代理镜像；控制面不创建 namespace、不构建或拉取镜像、不选择 current-context。当前只支持 basic，不支持 Docker 自定义环境、独立工作区或暂停/恢复。

在仓库根确认已有集群，再从 `control_plane/` 启动；沿用已配置的 `AGENT_SANDBOX_API_KEY`，不要为另一客户端生成不同 key：

```bash
kubectl --context=agent-sandbox-k8s get nodes
kubectl --context=agent-sandbox-k8s get namespace agent-sandbox-test
cd control_plane
uv sync --locked
export AGENT_SANDBOX_RUNTIME=kubernetes
export AGENT_SANDBOX_K8S_KUBECONFIG="$HOME/.kube/config"
export AGENT_SANDBOX_K8S_CONTEXT=agent-sandbox-k8s
export AGENT_SANDBOX_K8S_NAMESPACE=agent-sandbox-test
export AGENT_SANDBOX_K8S_AGENT_IMAGE=agent-sandbox-basic:dev
export AGENT_SANDBOX_K8S_PROXY_IMAGE=agent-sandbox-proxy-upstream-test:dev
unset AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS
export AGENT_SANDBOX_DATABASE_PATH="$PWD/.local/kubernetes-lifecycle.sqlite3"
uv run --locked uvicorn control_plane.app:app --app-dir .. --host 127.0.0.1 --port 18083
```

在另一个已配置相同管理员 key 的终端验证：

```bash
curl -fsS http://127.0.0.1:18083/healthz
curl -fsS -H "Authorization: Bearer $AGENT_SANDBOX_API_KEY" http://127.0.0.1:18083/readyz
```

两种镜像配置必须明确提供；示例代理标签是现有 Minikube 已验证的独立缓存镜像，不代表会自动加载。可选设置 `AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS=agent-sandbox-workspace` 启用私有 PVC，否则保留 emptyDir。`imagePullPolicy=Never`、静态证书认证和本机访问限制保持不变。

Docker 与 Kubernetes 必须分别管理自己的生命周期数据库；切换配置不会迁移或重新归属旧记录。未指定路径时，Docker 默认 `.local/lifecycle.sqlite3`，Kubernetes 默认 `.local/kubernetes-lifecycle.sqlite3`；显式 `create_app(database_path=...)` 或注入 Store 优先。不要把已有 Docker 库指给 Kubernetes，反之亦然。

配置成功和根 `/readyz` 成功仅表示后端可探测。正式 Kubernetes 配置会自动持有本地入口管理器；新建 basic 实例在资源启动、原实例整体就绪和认证入口准备完成后才返回 201，随后可沿上述创建、查询 endpoint、命令/文件和删除流程使用。默认最多持有 8 个入口会话，独立于实例数量额度；满额时创建会失败并补偿本次资源。

Kubernetes 创建增加有限的就绪等待和入口启动阶段，客户端应给足 HTTP 读取预算，例如 curl 使用 `--max-time 120`，Python SDK 可显式设置 `SandboxClient(..., timeout=120)`。超时不证明远端未创建或已取消，不能自动重试 POST。`started` 字段保持兼容，后续实例 readyz 仍实时检查。

GET endpoint 仍只查询已有会话；应用重启后不会自动发现旧实例或失效重连，已有实例的显式初始入口及单轮 pending 恢复继续使用原装配参数。当前仅验收 WSL loopback，本轮不提供远程网关。更多预算、补偿及配置说明见 [Kubernetes 实现](docs/control_plane/kubernetes/IMPLEMENTATION.md#新建实例自动入口实施2026-11-13)。

## 项目结构

```text
control_plane/   Python/FastAPI 控制面与 SQLite 生命周期存储
sandbox_agent/   Go 命令、文件和 SSE 服务
sdk/python/      独立同步 Python SDK 与 HTTP 边界测试
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

- 控制面使用单管理员 key，新动态执行入口使用独立 Token；没有多租户授权。旧实例与固定 Compose 入口未被改造，已知旧地址仍可能可用。所有服务仍只限可信本机，入口认证不代表容器内 Agent 隔离或完整安全隔离。
- Docker socket 仅供宿主侧控制面访问，不得将其或宿主凭证挂载进 sandbox。
- 容器限制尚未构成经过完整验证的安全隔离，workspace 也没有磁盘配额。
- Docker 与 SQLite 的部分失败仍可能需要重试或人工核验；SDK 暂不封装 SSE 和后台命令。
- 同库受管实例默认最多 10 条记录（`max_instances`），创建并发默认 2；没有整机 CPU/内存预算或磁盘配额。
- 周期回收以单进程、单 worker 为前提，没有多 worker 协调；未确认创建的记录和无记录旧资源不自动接管。

## 文档

- [文档目录与阅读导航](docs/README.md)
- [Sandbox Agent 设计](docs/sandbox_agent/DESIGN.md)、[API 与 curl 示例](docs/sandbox_agent/API.md)
- [控制面启动与设计](docs/control_plane/DESIGN.md)
- [控制面 API](docs/control_plane/API.md)
- [Python SDK](docs/sdk/python.md)、[工作区复用脚本](examples/python-workspace-reuse.py)
- [Docker 设计](docs/control_plane/docker/DESIGN.md)、[实现与验收](docs/control_plane/docker/IMPLEMENTATION.md)
- [目录与职责](docs/REPOSITORY_LAYOUT.md)
- [开发进度与后续计划](docs/PROJECT_PROGRESS.md)
- [贡献与协作约定](AGENTS.md)

## 许可证

项目尚未选择许可证，当前没有 `LICENSE` 文件。
