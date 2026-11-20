# 仓库目录与职责

```text
control_plane/
  app.py           配置装配、lifespan 和路由注册
  pyproject.toml
  uv.lock
  config/          可信进程设置
  api/             健康、实例、工作区和认证 HTTP 边界
  domain/          规格、身份、状态、结果和领域错误
  services/        实例、独立工作区和维护编排
  runtime/         SandboxRuntime 契约、Docker 资源操作和 Agent 健康探针
  storage/         SQLite 元数据、schema 校验和 RunLock
  common/          任务等待与取消收尾
  tests/           API、services、runtime、storage、integration 测试
sandbox_agent/     独立 Go module，提供命令、SSE、后台任务和文件
egress_proxy/      独立 Go module，提供 HTTP/CONNECT 出口
sdk/python/        独立同步 SDK 及其测试
deploy/local/      固定 Compose、动态入口镜像和本地脚本
examples/          可信自定义镜像及工作区复用示例
docs/              文档导航、设计、协议、运行手册和进度
```

## 控制面模块映射

| 位置 | 职责 |
| --- | --- |
| `config/settings.py` | 默认数据库路径、可信路径校验、旧配置兼容 |
| `api/{auth,errors,health,sandboxes,workspaces}.py` | 管理员认证、稳定错误映射及 HTTP 输入输出 |
| `domain/{models,errors}.py` | 标准库领域类型与约束 |
| `services/sandbox_service.py` | 实例创建和生命周期统一入口、准入、实例锁、跨实体事务与补偿 |
| `services/workspace_service.py` | 独立卷的创建、查询、删除，以及实例绑定的校验 |
| `services/maintenance.py` | 串行回收调度、候选处理、pending 核验和受控恢复 |
| `services/endpoint_access.py` | 核对原实例、原事务与代理凭据摘要 |
| `runtime/base.py`、`runtime/docker_runtime.py` | 异步契约与唯一生产 Docker 适配实现 |
| `runtime/creation.py`、`runtime/images.py` | 创建事务、取消和回滚，以及可信镜像注册、预检与固定 ID |
| `runtime/resources.py`、`runtime/inspection.py` | 清单发现、精确归属与资源操作原语，以及状态、拓扑、入口和 pending 证据检查 |
| `runtime/lifecycle.py`、`runtime/workspaces.py` | 原容器暂停、恢复、删除和独立卷操作 |
| `runtime/agent_probe.py` | 经核验 loopback endpoint 的有界 Agent HTTP 健康检查 |
| `storage/base.py`、`storage/sqlite_store.py` | 存储协议与错误，以及唯一 SQLite Store，实例和工作区使用同一事务 |
| `storage/schema.py`、`storage/run_lock.py` | v7 结构与校验、同库系统独占锁 |
| `common/async_tasks.py` | 关键任务等待、重复取消和实际结果收尾 |

测试按入口与组件分目录，共享夹具集中在 `conftest.py`、`fakes.py`、Runtime/Storage fakes 和集成 helpers。这些测试导入真实契约与测试夹具，隔离集成使用临时数据库和原资源 ID/完整标签清理。

依赖从应用装配进入 HTTP、服务及适配层。HTTP 依赖统一服务入口。领域类型不依赖适配层。服务通过 Runtime 与 Store 契约协调，Runtime 和 Store 之间没有调用关系。更多信息见 [系统架构](ARCHITECTURE.md) 与 [控制面设计](control_plane/DESIGN.md)。
