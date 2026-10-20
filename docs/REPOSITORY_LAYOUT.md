# 仓库目录与职责

项目由宿主侧 Python 控制面、容器内 Go Agent 和本地 Docker 部署文件组成。组件的设计和 HTTP 契约分开维护，完整导航见[文档目录](README.md)。

## 源码与部署布局

```text
agent-sandbox/
├── README.md
├── AGENTS.md
├── control_plane/
│   ├── app.py
│   ├── pyproject.toml
│   ├── uv.lock
│   ├── api/
│   │   ├── health.py
│   │   ├── sandboxes.py
│   │   └── errors.py
│   ├── core/
│   │   ├── models.py
│   │   ├── errors.py
│   │   ├── service.py
│   │   ├── tasks.py
│   │   └── reaper.py
│   ├── runtime/
│   │   ├── sandbox_runtime.py
│   │   └── docker/
│   │       ├── docker_runtime.py
│   │       └── transaction.py
│   ├── storage/
│   │   ├── AGENTS.md
│   │   └── lifecycle_store.py
│   └── tests/
├── sandbox_agent/
│   ├── go.mod
│   ├── main.go
│   ├── Dockerfile
│   ├── command/
│   └── server/
├── deploy/local/
│   ├── compose.yml
│   ├── Dockerfile.proxy
│   ├── proxy.conf
│   ├── run-local.sh
│   └── smoke-local.sh
└── docs/
```

`control_plane/` 顶层 Python 源文件仅有 `app.py`，采用 namespace package，从仓库根导入 `control_plane.*`。`sandbox_agent/` 是独立 Go module，测试与被测模块放在同一目录。

## 模块职责

| 位置 | 职责与依赖边界 |
| --- | --- |
| [control_plane/app.py](../control_plane/app.py) | 装配 Runtime、Store 和共享 Service；在 lifespan 初始化存储并管理后台回收 |
| [control_plane/api/](../control_plane/api/) | 输入约束、只读请求整体超时、固定错误映射和 HTTP 响应；通过 Service 调用后端 |
| [control_plane/core/](../control_plane/core/) | 模型、领域错误、生命周期编排、关键任务等待与回收调度；不依赖 FastAPI 或 Docker SDK |
| [control_plane/runtime/](../control_plane/runtime/) | 公共异步契约及 Docker 适配；同步资源创建、核验与回收不访问 SQLite |
| [control_plane/storage/](../control_plane/storage/) | Store 协议、标准库 SQLite、状态及事务/版本保护；不调用 Docker 或 HTTP |
| [sandbox_agent/main.go](../sandbox_agent/main.go) | 命令行参数、监听、信号与 HTTP 服务生命周期 |
| [sandbox_agent/command/](../sandbox_agent/command/) | 直接执行 argv、环境合并、超时、输出限制和 Linux 进程组取消 |
| [sandbox_agent/server/](../sandbox_agent/server/) | 健康、同步命令、SSE 和 workspace 文件接口；共享命令并发额度 |
| [deploy/local/](../deploy/local/) | 固定 Compose 实例、入口代理镜像与本地脚本 |

生命周期调用链为 `API → SandboxService → SandboxRuntime/Store`。命令、文件与 SSE 通过实例 endpoint 进入 Agent，不由生命周期 API 转发。Docker socket 仅由宿主侧控制面访问。

## 文档布局

`docs/` 根目录保留导航、仓库目录和开发进度；组件资料分别位于 `sandbox_agent/` 与 `control_plane/`。Control Plane 下仅维护设计、API 和 `docker/`，Docker 资料进一步分为设计与实现两个文件。

- [Sandbox Agent 设计](sandbox_agent/DESIGN.md)、[API 与示例](sandbox_agent/API.md)。
- [Control Plane 设计](control_plane/DESIGN.md)、[HTTP API](control_plane/API.md)。
- [Docker 设计](control_plane/docker/DESIGN.md)、[实现与验收](control_plane/docker/IMPLEMENTATION.md)。
- [开发进度与已知限制](PROJECT_PROGRESS.md)。

## 路径与开发约定

- Python 依赖和锁文件由 `uv` 管理；pytest 保持 `testpaths = ["tests"]`、`pythonpath = [".."]`，源码不修改 `sys.path`。
- 控制面默认数据库由 `app.py` 定位到 `control_plane/.local/lifecycle.sqlite3`；导入和装配不初始化数据库或连接 Docker。当前 schema 2 不提供旧库迁移。
- Agent 镜像构建上下文仅包含 `sandbox_agent/`。本地脚本从自身位置定位 `deploy/local/compose.yml`，与调用时的工作目录无关。
- 生命周期协议、恢复规则和启动命令见 [Control Plane 设计](control_plane/DESIGN.md)，不在目录文档中重复维护。
- 根 [AGENTS.md](../AGENTS.md) 负责仓库协作约束；[存储协作约束](../control_plane/storage/AGENTS.md) 补充 SQLite 模块规则。本地数据库、凭据与虚拟环境不进入版本库。
