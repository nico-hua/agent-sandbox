# 仓库目录与职责迁移

本次按已确认的用户方案重组现有实现，保持 HTTP 协议、资源命名、镜像标签、固定安全配置、超时和清理语义不变。不新增运行时、公共 API 或依赖，不自动提交。

## 目标与边界

- `control_plane/` 是宿主侧 Python 项目根目录，顶层 Python 源文件只有 `app.py`；采用 namespace package，从仓库根目录导入 `control_plane.*`。
- `api/` 负责健康、就绪、创建、查询和删除 HTTP 路由与错误映射；`core/` 保存模型、领域错误和 SandboxService 编排，不依赖 FastAPI 或 Docker SDK。
- `storage/lifecycle_store.py` 集中最小 Store 协议与标准库 SQLite 实现，负责可更新的生命周期元数据，不访问 Docker；Service 协调 Store 与 Runtime，不增加 Repository 框架或数据库包装层。
- `runtime/sandbox_runtime.py` 定义公共 ABC 契约；Docker 后端集中在 `runtime/docker/`：`docker_runtime.py` 负责异步入口、线程调度、取消等待和查询映射，`transaction.py` 集中同步 Docker 资源生命周期操作与归属工具，包含创建事务、创建失败逆序回滚，以及独立的正式删除资源发现、核验和回收。探测 transport timeout 保持 0.5 秒，生命周期 transport timeout 保持 5 秒。
- `sandbox_agent/` 保留独立 Go module，`command/` 和 `server/` 与各自测试直接位于该目录；镜像构建上下文仅包含此组件。
- `deploy/local/` 保存 Compose、代理和脚本；Compose 构建上下文为 `../../sandbox_agent`，脚本从自身位置定位配置。
- 合并原控制面协作约束到根 `AGENTS.md`；旧 `.venv` 留在原处，新环境通过 `uv sync --locked` 创建。

## 数据流与失败路径

应用装配 Runtime、生命周期 Store 和共享的 `SandboxService(runtime, store)`，再将同一个 Service 注入两组路由；`create_app()` 支持注入 `runtime`、`store` 或 `database_path`，保留既有超时参数。默认数据库按 `app.py` 文件位置定位到 `control_plane/.local/lifecycle.sqlite3`；FastAPI lifespan 显式初始化，失败阻止启动。导入和装配不创建数据库或连接 Docker，API 统一通过 Service，SQLite 与 Docker 同步调用均在线程中运行。

创建先持久化 version=0 的元数据，再由 Runtime 核对镜像、创建五项资源并启动；低层 create 返回后立即记录精确标识，失败或取消按本次精确 ID 与完整标签逆序回滚。Service 仅在 Runtime 明确完成失败回滚后按原事务 ID 和版本清理预写记录；结果不明确或回滚不完整则保留记录与 sandbox ID。SQLite 写入期间取消须等待最终结果，首次及重复取消都等待关键收尾并读取后台异常。SQLite 与 Docker 没有跨系统原子事务，元数据不证明实际实例存在。

状态查询先通过 Runtime 确认实例，再核对 Store 记录的实例 ID、内部事务 ID 与创建时间，使用数据库当前过期时间，Docker 状态映射保持原义。无记录时保留 Docker 初始标签或旧实例 `null`，不自动接管或补写；数据库不可用或身份不匹配返回稳定错误，不静默回退。内部 `transaction_id` 不进入公开 HTTP 响应。

手动删除沿用该调用链，Runtime 契约新增可选 `expected_transaction_id`。`docker_runtime.py` 保留异步入口、线程调度和取消等待；`transaction.py` 从 Docker 标签恢复五类受管资源，预检完整归属、事务和唯一性，有预期事务时对全部存留角色核对，再按精确标识回收，不要求主容器存在。Runtime 成功后才按原事务和版本删除元数据，失败保留记录；默认保留 workspace 同样清理实例记录，残卷仍按 Docker 标签管理。删除不套用只读查询超时，首次及重复取消等待 Runtime 与 Store 关键收尾；不增加持久化删除任务或后台回收。Docker 删除细节见 [删除设计](docker/DELETION.md)，跨系统流程见 [控制面设计](CONTROL_PLANE.md)。

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
