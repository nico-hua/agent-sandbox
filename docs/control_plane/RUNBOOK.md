# Docker 控制面运行手册

控制面运行在 Linux/WSL 宿主侧。动态实例使用 Docker daemon、本地镜像和 SQLite，入口绑定到 IPv4 loopback。请求示例见 [项目 README](../../README.md) 和 [控制面 API](API.md)。

<a id="preset-images"></a>

## 准备本地镜像

在仓库根目录预先构建所需镜像。DockerRuntime 按实例环境检查本地镜像及固定入口镜像，非空出口白名单还需要出口镜像。创建过程中使用预检获得的镜像 ID。

```bash
docker build -t agent-sandbox-basic:dev ./sandbox_agent
docker build --build-arg RUNTIME_BASE_IMAGE=python:3.12.15-alpine3.23 \
  -t agent-sandbox-python:dev ./sandbox_agent
docker build --build-arg RUNTIME_BASE_IMAGE=node:24.22.0-alpine3.23 \
  -t agent-sandbox-node:dev ./sandbox_agent
docker build -t agent-sandbox-proxy:dev -f deploy/local/Dockerfile.proxy deploy/local
docker build -t agent-sandbox-egress:dev ./egress_proxy
```

basic 使用 Dockerfile 内的 Alpine 3.22。Python 和 Node 镜像复用同一个 Go Agent，包含相应解释器和包管理器。Alpine 使用 musl，依赖原生扩展的包需要单独验证兼容性。固定 Compose 是独立开发链路，可通过 [本地脚本](../../deploy/local/run-local.sh) 启动。

<a id="custom-environments"></a>

### 自定义环境

由维护者预先构建受信镜像并在应用装配时通过 `additional_environments` 注册。镜像需提供 `/usr/local/bin/agent`、`sandbox` 用户、`/workspace`、wget 和 Agent 健康接口。业务请求只提交注册名。修改注册表影响新建实例及未来工作区兼容性检查，已有资源仍通过原标签查询和删除。示例中的 `tools` 基于 basic 镜像，测试用标记位于镜像内。

```bash
docker build -t agent-sandbox-tools:dev examples/custom-environment
cd control_plane
PYTHONPATH=.. uv run --locked uvicorn custom_environment_app:app --app-dir ../examples/custom-environment \
  --host 127.0.0.1 --port 18083
```

`PYTHONPATH=..` 允许示例入口导入仓库中的控制面。示例应用、镜像定义和环境约束见 [自定义应用](../../examples/custom-environment/custom_environment_app.py) 与 [固定运行配置](DESIGN.md#资源上限与固定安全配置)。

## 启动和配置

在 `control_plane/` 执行 `uv sync --locked`，在服务和客户端环境安全配置同一个 `AGENT_SANDBOX_API_KEY`，再启动单 worker。

```bash
uv run --locked uvicorn control_plane.app:app --app-dir .. --host 127.0.0.1 --port 18083
```

`AGENT_SANDBOX_DATABASE_PATH` 可指定可信数据库路径。显式 Store 与显式数据库路径优先于环境变量。默认路径为 `control_plane/.local/lifecycle.sqlite3`，独立于当前目录。应用装配可配置 `max_concurrent_creates`、`max_instances`、恢复选项与可信环境映射。管理 key 与实例 Token 的使用边界见 [认证设计](DESIGN.md#控制面认证与启动配置)。

旧 `AGENT_SANDBOX_RUNTIME=docker` 仍可保留。其他运行时值、任何遗留 `AGENT_SANDBOX_K8S_*` 设置，以及原默认 `kubernetes-lifecycle.sqlite3` 路径会被拒绝。使用这个版本前，从启动环境清除已停用的设置并选定经核实的 Docker 数据库。

## 显式 schema 升级

启动初始化只接受空新库或经过 DDL 和外键核验的 v6。升级前停止使用该库的控制面，并从原部署记录或备份来源核实该库一直由 Docker 控制面写入。文件名、DDL 和空回执表都无法证明来源。来源不明或来自 Kubernetes 的数据库应保留原状。命令要求显式声明 `--source docker --confirm-docker-source`，它代表操作者已经完成来源核实。

从仓库根指定实际数据库和**尚不存在**的备份文件。备份目录应已存在，且与源数据库及其伴随文件路径不同。

```bash
control_plane/.venv/bin/python -m control_plane.storage.migrations \
  --database /absolute/path/to/lifecycle.sqlite3 \
  --backup /absolute/private/path/lifecycle-before-v6.sqlite3 \
  --source docker --confirm-docker-source
```

升级持有源库的 RunLock，使用 `BEGIN IMMEDIATE` 阻止其他 SQLite 写入。准确核验 v2、v3、v4 或 v5 DDL 及外键。若 v5 存在历史回执记录，升级会拒绝并保留数据。核验后通过 SQLite backup API 写出独立数据库快照，包含已提交的 WAL 数据。备份经完整性检查并持久化后，以独占、不覆盖的方式发布。备份含原实例 Token，仍应存放在私有目录。

升级在同一 SQLite 事务内执行结构变更并核验 v6。实例原 ID、事务、微秒时间、期限、状态、版本、Token、工作区及占用保留。v2 补上可空的工作区引用，v3 补上凭据表，v4 只变更版本，v5 移除已验证为空的历史回执结构。v1 与未知结构会被拒绝。升级失败时 SQLite 回滚；已经发布的备份保留，适合独立核验。对已核验的 v6 重试会报告已完成。

需要恢复时，先停止使用该库的进程，保留当前库及其伴随文件以供核对。将备份复制到新的私有路径，用 SQLite `PRAGMA integrity_check` 和 `PRAGMA user_version` 验证。该备份保留升级前 schema，可由原版本在独立配置中读取，或重新显式升级验证后的副本。切换回旧元数据前应核对快照之后的 Docker 变化及凭据，避免重新启用已删除的实例记录。

## 隔离验收

默认测试依赖临时 SQLite、假 Runtime 和假 Docker 客户端。本地 loopback 测试需要能绑定端口。默认套件和编译检查见 [项目 README](../../README.md)。

真实集成使用预构建的 Agent、入口、语言、自定义和出口镜像。请在 Docker daemon 可用且允许创建本次唯一资源的环境执行。测试以原 ID 和完整标签核验清理归属；临时数据库与开发数据库分离。

```bash
cd control_plane
RUN_DOCKER_INTEGRATION=1 uv run --locked pytest -q -m docker_integration tests/integration
```

该套件覆盖创建、就绪、入口、命令、SSE、文件、Python、Node、自定义环境、TTL、暂停恢复、重启、应用端口、工作区复用和默认直连拒绝。需要验证白名单实际 HTTP/HTTPS 可达时，额外设置 `RUN_EGRESS_PUBLIC_INTEGRATION=1` 并重跑。这需要 DNS 及相应公网目标可用。若正向目标不可达，应报告结果；本地拒绝和 healthy 状态各有独立语义。

SDK 端到端使用独立临时 SQLite、loopback 端口与本次唯一 Docker 资源。为控制面和 SDK 测试进程设置同一个本次管理员 key，仅通过环境传递。在 `sdk/python/` 执行以下命令。临时控制面 URL 和 key 由调用环境提供。

```bash
RUN_SDK_INTEGRATION=1 SDK_TEST_ISOLATED=1 uv run --locked pytest -q -m sdk_integration
```

SDK 端到端验证需要 `SDK_TEST_CONTROL_URL` 与 `SDK_TEST_API_KEY`。测试结束后核对本次实例、网络、容器和卷的精确身份及归属，再处理本次残留。

## 受控 HTTP 服务入口

应用进程监听容器内 `0.0.0.0` 的许可端口（1024–65535，排除 8080）。通过管理 API 获取 `/v1/sandboxes/{id}/services/{port}/endpoint` 和实例 Token，再请求返回的带前缀 URL。入口使用 Docker DNS 解析同实例的 `sandbox` 地址，在转发前移除认证和转发相关请求头。若应用无响应，先用实例入口的 Agent 命令确认进程仍在运行、监听地址及实际端口。路径前缀、静态资源、重定向与 Cookie 由应用适配。更多错误和请求示例见 [服务入口 API](API.md#查询受控-http-服务入口)。
