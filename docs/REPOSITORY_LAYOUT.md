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
  runtime/         SandboxRuntime 契约和 Agent 健康探针
    docker/        Docker 运行时实现和资源操作
  storage/         生命周期存储契约
    sqlite/        SQLite 元数据、schema 初始化和 RunLock
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
| `config/{settings,defaults}.py` | 可信配置快照、解析校验与共享默认值 |
| `api/{health,sandbox,workspace}_router.py` | HTTP 路由、等待预算和业务调用 |
| `api/{auth,errors,request,response}.py` | 管理员认证、稳定错误映射、严格请求解析和响应协议 |
| `domain/{sandbox,workspace,endpoint,maintenance}.py` | 实例规格与生命周期、工作区身份与占用、入口凭据、维护结果 |
| `domain/{errors,validation}.py` | 领域异常和共用字段校验 |
| `services/sandbox_service.py` | 生命周期统一入口、创建准入、实例锁及状态协调 |
| `services/sandbox_creation.py` | pending 预写、资源创建、active 确认和按原事务补偿 |
| `services/sandbox_lifecycle.py` | 手动删除、回收和创建补偿共用的元数据删除守卫 |
| `services/sandbox_queries.py` | 公开摘要分页、运行时与元数据核对、Agent 就绪探测 |
| `services/workspace_service.py` | 独立卷的创建、查询、删除，以及实例绑定的校验 |
| `services/sandbox_maintenance.py` | 串行回收调度、候选处理、pending 核验和受控恢复 |
| `services/endpoint_access.py` | Agent 与应用入口授权，核对原实例、原事务与代理凭据摘要 |
| `runtime/base_runtime.py`、`runtime/agent_probe.py` | 运行时异步契约与经核验 loopback endpoint 的 Agent HTTP 健康检查 |
| `runtime/docker/docker_runtime.py`、`runtime/docker/client.py` | `DockerRuntime` 装配及异步调用，探针和生命周期使用各自的惰性客户端工厂 |
| `runtime/docker/{creation,images}.py` | 创建事务、取消和回滚，以及可信镜像注册、预检与固定 ID |
| `runtime/docker/{resources,inspection}.py` | 资源清单、标签和精确归属操作，以及实例状态、入口和 pending 资源查询 |
| `runtime/docker/{endpoint,egress}.py` | 入口代理和出站代理校验，供查询和暂停流程共用 |
| `runtime/docker/{lifecycle,workspaces}.py` | 原容器暂停、恢复、删除和独立卷操作 |
| `storage/base.py`、`storage/sqlite/sqlite_store.py` | 存储协议与错误，以及保留跨实体事务的 SQLite Store |
| `storage/sqlite/{schema,run_lock}.py` | v7 初始化与结构校验、同库系统独占锁 |
| `common/async_tasks.py` | 关键任务等待、重复取消和实际结果收尾 |

测试按入口与组件分目录，共享夹具集中在 `conftest.py`、`fakes.py`、Runtime/Storage fakes 和集成 helpers。这些测试导入真实契约与测试夹具，隔离集成使用临时数据库和原资源 ID/完整标签清理。

应用装配连接 HTTP、服务及适配器。HTTP 依赖统一服务入口，服务通过 Runtime 与 Store 契约协调，领域类型只使用标准库和同层模块。配置独立解析，Docker 和 SQLite 各自实现契约。更多信息见 [系统架构](ARCHITECTURE.md) 与 [控制面设计](control_plane/DESIGN.md)。
