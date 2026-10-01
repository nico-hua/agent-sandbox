# 仓库目录与职责迁移

本次按已确认的用户方案重组现有实现，保持 HTTP 协议、资源命名、镜像标签、固定安全配置、超时和清理语义不变。不新增运行时、公共 API 或依赖，不自动提交。

## 目标与边界

- `control_plane/` 是宿主侧 Python 项目根目录，顶层 Python 源文件只有 `app.py`；采用 namespace package，从仓库根目录导入 `control_plane.*`。
- `api/` 负责健康、就绪、创建和查询 HTTP 路由与错误映射；`core/` 保存模型、领域错误和 SandboxService 编排，不依赖 FastAPI 或 Docker SDK。
- `runtime/sandbox_runtime.py` 定义公共 ABC 契约；Docker 后端集中在 `runtime/docker/docker_runtime.py` 与 `transaction.py`，分别负责异步入口/查询映射和创建事务/归属核验/逆序回滚。探测 transport timeout 保持 0.5 秒，生命周期 transport timeout 保持 5 秒。
- `sandbox_agent/` 保留独立 Go module，`command/` 和 `server/` 与各自测试直接位于该目录；镜像构建上下文仅包含此组件。
- `deploy/local/` 保存 Compose、代理和脚本；Compose 构建上下文为 `../../sandbox_agent`，脚本从自身位置定位配置。
- 合并原控制面协作约束到根 `AGENTS.md`；旧 `.venv` 留在原处，新环境通过 `uv sync --locked` 创建。

## 数据流与失败路径

应用装配一个 Runtime 和一个 SandboxService，再将同一个 Service 注入两组路由；`create_app(runtime=None, readiness_timeout=1.0, sandbox_query_timeout=1.0)` 支持 FakeRuntime。导入和装配均不连接 Docker，API 统一通过 Service 进入公共 Runtime 契约，同步 SDK 调用仍在线程中运行。进程健康直接返回，HTTP 超时与错误脱敏保留在 API。创建先核对镜像，再创建网络、volume、容器并启动；返回 create ID 后立即记录。失败或取消只按本次记录的精确 ID 与完整标签逆序回滚，重复取消必须等待清理。查询不依赖进程内创建记录，保留 Docker 状态与健康映射、超时和错误脱敏。

## 实施与验收清单

- [x] 记录迁移前 Python、Go 默认测试基线及环境限制。
- [x] 迁移 Go 与部署文件，更新 module/import、Docker COPY 和脚本路径，验证 Go 测试、race、vet 与 Compose 路径。
- [x] 按职责组织 Python 并迁移全部测试，运行时入口统一后以 Runtime 注入替代独立 probe/creator/reader，保留 HTTP 断言与锁文件；pytest 使用 `testpaths = ["tests"]`、`pythonpath = [".."]`。
- [x] 验证新模块首次导入与应用装配不创建 Docker client，运行全部 Python 回归和 compileall，启动新 Uvicorn 入口。
- [x] 更新根协作说明、README 和当前使用文档，记录迁移关系和实际验证结果。
- [x] 在不重建现有开发容器、网络或卷的前提下构建镜像，显式运行真实 Docker 集成测试并核对资源清理；检查最终 diff。

重点复核：晚到和重复取消、部分创建后 inspect 失败、标签或 ID 变化、共享 Service 与 FakeRuntime 的 ping/create/get 委托、从任意工作目录运行部署脚本。验证结果见 `PROJECT_PROGRESS.md`。

## 运行时入口统一设计与合并关系

本次按用户给定方案统一调用链，不增加运行时、公共 HTTP API、持久化模型或依赖。假设现有固定资源规格和错误/状态契约继续适用；Service 负责 ID 与规格，运行时负责后端操作，API 负责 HTTP 请求约束、超时和响应。

| 原模块 | 当前归属 |
| --- | --- |
| `core/protocols.py` 的 SandboxRuntime | `runtime/sandbox_runtime.py`，从 Protocol 改为 ABC，并纳入 ping |
| RuntimeProbe、SandboxCreator、SandboxReader | 移除，由 API 统一依赖 SandboxService |
| `runtime/docker_probe.py`、`docker_runtime.py`、`docker_query.py` | `runtime/docker/docker_runtime.py` |
| `runtime/docker_create.py`、`ownership.py` | `runtime/docker/transaction.py` |
| `runtime/docker_client.py` | ping 工厂与协议合入 docker_runtime.py；生命周期工厂与协议合入 transaction.py |

取消、重复取消、提交后回滚、资源 ID/完整归属标签核验和逆序清理沿用现有实现。验收以默认测试、纯契约/API 不加载 Docker 后端、应用装配不连接 Docker、启动入口和编译/diff 检查为准；真实 Docker 集成仍需显式启用。
