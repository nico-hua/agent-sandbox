# Agent Sandbox 项目协作指南

## 项目状态与边界

Agent Sandbox 面向 AI Agent 工作负载，当前支持 Linux/WSL 本机 Docker。控制面具备管理员认证、实例和独立工作区管理、SQLite 生命周期、回收及受控 pending 恢复。容器内 Go Agent 负责命令、SSE、后台任务和文件，Go Egress Proxy 负责精确域名的 HTTP/CONNECT 出口，Python SDK 封装现有 HTTP 契约。项目面向可信本机开发，隔离边界仍需独立威胁模型和验证。

## 开始工作前

1. 先检查实际代码、工具链、测试和文档。
2. 新功能先说明假设、信任边界、数据流、失败场景和验收标准。
3. 涉及新隔离边界、公共 API、持久化模型、运行时或部署方式时，先提交简短设计说明并取得确认。
4. 优先验证一条完整链路。目录按现有职责组织，避免无关重构或预留抽象。

## 组件与依赖

- `control_plane/app.py` 负责装配、lifespan 和路由注册，是控制面顶层唯一的业务 Python 文件。Python 配置和锁文件放在 `control_plane/`。使用 `uv`，依赖变更同步更新 `pyproject.toml` 与 `uv.lock`。
- `config/` 读取可信配置。HTTP 层通过 `SandboxService` 处理规格、错误、认证和响应。`domain/` 只依赖标准库。`services/` 管理生命周期和工作区编排，依赖薄 Runtime 与 Store 契约。
- `runtime/` 的唯一生产实现为 `DockerRuntime`，只操作 Docker。Store 只操作元数据。保留同一 SQLite 事务内的实例、凭据和工作区占用，不把跨实体事务拆成多个 Store。
- 控制面导入统一使用 `control_plane.<层>`，源码不修改 `sys.path`。pytest 保持 `testpaths = ["tests"]`、`pythonpath = [".."]`，共享夹具使用 conftest 或明确的 fakes。
- 各具名函数及测试函数都有函数级注释或 docstring，概括职责和重要语义。Go 导出声明按名称开头。复杂分支的内部注释解释原因。拆分以独立职责为界，保持最小变更。
- `sandbox_agent/`、`egress_proxy/` 为独立 Go module，`sdk/python/` 为独立 Python 项目，`deploy/local/` 保存本地部署。镜像构建以组件目录为上下文，本地脚本从自身位置定位 Compose 文件。

## 安全和生命周期不变量

- 控制面绑定 loopback，正常启动要求管理员 Bearer key。新动态实例入口使用单独的实例 Token。管理员 key、宿主目录和 Docker socket 不得交给 sandbox 或代理。密钥、凭据、本地库和生成的配置不得提交。
- Docker client 延迟创建，ping 使用 0.5 秒 transport timeout，生命周期操作与查询使用 5 秒。每次操作负责关闭其客户端。根就绪探测只调用只读 `ping()`。
- 公共创建只允许注册环境及受约束 CPU、内存、TTL 和精确域名白名单。环境映射由可信代码装配，解析后固定镜像 ID。Docker 网络、入口和挂载采用固定安全策略，命令与文件数据路径留在数据面。
- SandboxService 持有每实例操作锁、准入和跨实体补偿。创建、资源记录与逆序回滚保持原事务。删除前必须核对资源精确 ID、完整归属标签和预期事务。结果不明时保留恢复依据与占用，不以超时或取消推定工作线程已停止。
- 启动在实际数据库路径持 RunLock 后才初始化 SQLite。只支持当前 schema 6。默认验证使用临时库，禁止修改、重建或清理开发数据库及工作负载。详见 [存储约束](control_plane/storage/AGENTS.md) 和 [运行手册](docs/control_plane/RUNBOOK.md)。

## 测试、文档与 Git

- 新增行为须有聚焦用例，覆盖期限、取消、并发失败、占用释放、路径与权限等相关边界。默认测试不修改 Docker 资源或连接公网。真实集成显式启用，清理仅限本次精确 ID 与完整归属核验通过的资源。
- 控制面执行 `uv sync --locked`、`uv run --locked pytest -q`、`uv run --locked python -m compileall -q app.py config api domain services runtime storage common tests` 和 `uv tree`。SDK 执行 `uv sync --locked` 与 `uv run --locked pytest -q`。
- 两个 Go module 各执行 `go test ./...`、`go test -race ./...` 与 `go vet ./...`。仓库根执行 `docker compose -f deploy/local/compose.yml config --quiet` 和 `git diff --check`。隔离集成命令与公网开关见 [运行手册](docs/control_plane/RUNBOOK.md)。
- 文档优先使用中文。README 负责定位、现状和快速开始，`docs/PROJECT_PROGRESS.md` 按日期倒序记录可验收变更、验证结果及待办。公共行为变化同步更新协议、示例和 SDK 文档。
- 保留用户已有修改，避免无关格式化。禁止擅自 reset、force push、删除用户文件或修改远程仓库。报告实际验证结果、运行环境及未执行的检查。
