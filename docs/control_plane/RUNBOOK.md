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

由维护者预先构建受信镜像，在 `.env` 的 `AGENT_SANDBOX_ADDITIONAL_ENVIRONMENTS` 中用 JSON 对象注册环境名和镜像引用。示例可填写 `AGENT_SANDBOX_ADDITIONAL_ENVIRONMENTS='{"tools":"agent-sandbox-tools:dev"}'`。镜像需提供 `/usr/local/bin/agent`、`sandbox` 用户、`/workspace`、wget 和 Agent 健康接口。业务请求只提交注册名。修改注册表影响新建实例及未来工作区兼容性检查，已有资源仍通过原标签查询和删除。示例中的 `tools` 基于 basic 镜像，测试用标记位于镜像内。

```bash
docker build -t agent-sandbox-tools:dev examples/custom-environment
cd control_plane
PYTHONPATH=.. uv run --locked uvicorn custom_environment_app:create_app --factory --app-dir ../examples/custom-environment \
  --host 127.0.0.1 --port 18083
```

`PYTHONPATH=..` 允许示例入口导入仓库中的控制面。示例应用、镜像定义和环境约束见 [自定义应用](../../examples/custom-environment/custom_environment_app.py) 与 [固定运行配置](DESIGN.md#资源上限与固定安全配置)。

## 启动和配置

在 `control_plane/` 执行 `uv sync --locked`，并从 `.env.example` 准备 `.env`。管理员认证默认关闭。启用时在 `.env` 设置 `AGENT_SANDBOX_AUTH_ENABLED=true` 并填写 `AGENT_SANDBOX_API_KEY`，调用终端使用同一个 key。启动单 worker。

```bash
uv run --locked uvicorn control_plane.app:app --app-dir .. --host 127.0.0.1 --port 18083
```

应用从固定的 `control_plane/.env` 读取配置。缺项或空值使用默认值。`AGENT_SANDBOX_RUNTIME` 默认 `docker`，其他值会阻止启动。`AGENT_SANDBOX_DATABASE_PATH` 可指定数据库，相对路径以 `control_plane/` 为基准。认证、超时、回收、恢复、容量和镜像映射的格式见 [环境文件模板](../../control_plane/.env.example)。

## 切换 HTTP 响应协议

控制面和 Agent 的 JSON 响应统一为整数 `code`、`message`、`data`。旧客户端应改为检查 HTTP 状态，从 `data` 解包结果，并用 200 与 `data: null` 确认无返回值的成功。后台提交和取消同样返回 200，实际任务进度由任务 ID 和快照状态表达。旧 `error.code` 调整为状态分类，只有约定的实例就绪暂态错误携带 `data.reason`。详见 [控制面 API](API.md#响应协议迁移) 与 [SDK 错误兼容](../sdk/python.md#错误重试与取消)。

1. 先构建所用的 basic、Python、Node 和自定义 Agent 镜像，以及 `agent-sandbox-proxy:dev`。每个自定义镜像都应包含本版 Agent。按上文命令更新预制镜像，随后部署新控制面与 SDK。
2. 已有动态实例继续运行启动时固定的镜像 ID。旧 Agent 仍发送裸 JSON，旧入口仍发送旧的代理错误格式。升级后的主动就绪检查会拒绝旧 Agent 健康响应。对旧实例先用匹配旧协议的客户端备份所需文件，随后用新镜像创建实例并重新获取 endpoint 和 Token。控制面的查询、续期、暂停、恢复和删除使用升级后的管理协议。
3. 独立工作区要求原不可变镜像 ID。跨 Agent 镜像版本时，从旧实例导出所需文件，给新镜像创建新的工作区并恢复文件。旧工作区的身份和现有文件可继续按原归属管理。
4. 固定 Compose 的本地链路需重新构建 Agent 并重建容器，使 Nginx 载入更新的 `proxy.conf`。固定命名卷可继续保留文件。SSE 已开始的事件、文件下载的字节格式、用户应用响应及 Egress HTTP/CONNECT 代理协议保持各自的原有格式。

## 数据库初始化

启动持有实际数据库路径的 RunLock 后，初始化空新库或核验现有 schema 6 的完整 DDL 和外键。版本或结构不匹配时，控制面拒绝启动并保留数据库内容。需要建立独立控制面时，通过 `AGENT_SANDBOX_DATABASE_PATH` 指定新的数据库路径。

## 隔离验收

默认测试依赖临时 SQLite、假 Runtime 和假 Docker 客户端。本地 loopback 测试需要能绑定端口。默认套件和编译检查见 [项目 README](../../README.md)。

真实集成使用预构建的 Agent、入口、语言、自定义和出口镜像。请在 Docker daemon 可用且允许创建本次唯一资源的环境执行。测试以原 ID 和完整标签核验清理归属；临时数据库与开发数据库分离。

```bash
cd control_plane
RUN_DOCKER_INTEGRATION=1 uv run --locked pytest -q -m docker_integration tests/integration
```

该套件覆盖创建、就绪、入口、命令、SSE、文件、Python、Node、自定义环境、TTL、暂停恢复、重启、应用端口、工作区复用和默认直连拒绝。需要验证白名单实际 HTTP/HTTPS 可达时，额外设置 `RUN_EGRESS_PUBLIC_INTEGRATION=1` 并重跑。这需要 DNS 及相应公网目标可用。若正向目标不可达，应报告结果；本地拒绝和 healthy 状态各有独立语义。

SDK 端到端使用独立临时 SQLite、loopback 端口与本次唯一 Docker 资源。在隔离控制面的 `.env` 中开启认证并配置本次管理员 key，在 SDK 测试进程的环境中传入同一个 key。在 `sdk/python/` 执行以下命令。临时控制面 URL 和客户端 key 由调用环境提供。

```bash
RUN_SDK_INTEGRATION=1 SDK_TEST_ISOLATED=1 uv run --locked pytest -q -m sdk_integration
```

SDK 端到端验证需要 `SDK_TEST_CONTROL_URL` 与 `SDK_TEST_API_KEY`。测试结束后核对本次实例、网络、容器和卷的精确身份及归属，再处理本次残留。

## 受控 HTTP 服务入口

应用进程监听容器内 `0.0.0.0` 的许可端口（1024–65535，排除 8080）。通过管理 API 获取 `/v1/sandboxes/{id}/services/{port}/endpoint` 和实例 Token，再请求返回的带前缀 URL。入口使用 Docker DNS 解析同实例的 `sandbox` 地址，在转发前移除认证和转发相关请求头。若应用无响应，先用实例入口的 Agent 命令确认进程仍在运行、监听地址及实际端口。路径前缀、静态资源、重定向与 Cookie 由应用适配。更多错误和请求示例见 [服务入口 API](API.md#查询受控-http-服务入口)。
