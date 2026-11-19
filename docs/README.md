# 项目文档

从 [项目 README](../README.md) 开始，按需查阅以下文档。

| 文档 | 内容 |
| --- | --- |
| [系统架构](ARCHITECTURE.md) | 控制面、Docker 资源、Agent、入口与出口的边界和请求路径 |
| [目录与职责](REPOSITORY_LAYOUT.md) | 源码、测试与部署布局 |
| [控制面 HTTP API](control_plane/API.md) | 管理认证、实例与工作区操作、入口和错误契约 |
| [控制面设计](control_plane/DESIGN.md) | 生命周期、SQLite、资源拓扑、身份与跨系统失败处理 |
| [Docker 控制面运行手册](control_plane/RUNBOOK.md) | 构建、启动、数据库升级和隔离验收 |
| [Sandbox Agent API](sandbox_agent/API.md) | 命令、SSE、后台任务和文件协议及示例 |
| [Sandbox Agent 设计](sandbox_agent/DESIGN.md) | 运行、资源限制、命令与文件边界 |
| [Egress Proxy 设计](egress_proxy/DESIGN.md) | HTTP/CONNECT、精确域名匹配与 IPv4 拨号 |
| [Python SDK](sdk/python.md) | 安装、接口、错误与复用示例 |
| [开发进度](PROJECT_PROGRESS.md) | 按日期记录变更、实际验证和后续问题 |

维护 API 文档时同步对应客户端与示例。部署、数据库升级和验收步骤由运行手册维护，源码映射由目录文档维护。
