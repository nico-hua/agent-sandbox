# 项目文档

文档包括系统架构，以及 Sandbox Agent、Control Plane、Docker 后端和 Egress Proxy 的组件说明，描述当前代码已经实现的行为。快速启动和最小使用示例见[项目 README](../README.md)。

## 阅读顺序

1. 先读[系统架构设计](ARCHITECTURE.md)，了解组件关系和请求链路，再通过[仓库目录与职责](REPOSITORY_LAYOUT.md)定位源码、测试和部署文件。
2. 需要调用接口时，分别阅读 [Sandbox Agent API](sandbox_agent/API.md) 和 [Control Plane API](control_plane/API.md)。命令、文件和 SSE 请求发往实例 endpoint；生命周期请求发往控制面。
3. 需要理解实现时，阅读 [Sandbox Agent 设计](sandbox_agent/DESIGN.md)、[Control Plane 设计](control_plane/DESIGN.md)，再进入 Docker 的[设计](control_plane/docker/DESIGN.md)与[实现](control_plane/docker/IMPLEMENTATION.md)。独立出口代理见 [Egress Proxy 设计](egress_proxy/DESIGN.md)。
4. 已完成工作、历史验证环境和后续计划统一记录在[开发进度](PROJECT_PROGRESS.md)。

## 文档目录

```text
docs/
├── README.md
├── REPOSITORY_LAYOUT.md
├── PROJECT_PROGRESS.md
├── ARCHITECTURE.md
├── egress_proxy/
│   └── DESIGN.md
├── sandbox_agent/
│   ├── DESIGN.md
│   └── API.md
└── control_plane/
    ├── DESIGN.md
    ├── API.md
    └── docker/
        ├── DESIGN.md
        └── IMPLEMENTATION.md
```

## 内容与维护边界

| 文档 | 负责的内容 |
| --- | --- |
| [系统架构设计](ARCHITECTURE.md) | 系统 Mermaid 架构图、组件职责、请求链路、信任边界与当前限制 |
| [仓库目录与职责](REPOSITORY_LAYOUT.md) | 源码、测试、部署和文档布局，以及组件依赖边界 |
| [Egress Proxy 设计](egress_proxy/DESIGN.md) | 独立 HTTP/CONNECT 代理的启动、协议、错误、资源限制与验证边界 |
| [开发进度](PROJECT_PROGRESS.md) | 按日期记录开发、验证结果、待开发功能和已知限制 |
| [Sandbox Agent 设计](sandbox_agent/DESIGN.md) | Go 服务装配、命令执行、SSE、文件访问与取消清理 |
| [Sandbox Agent API](sandbox_agent/API.md) | 数据面请求和响应、限制、错误、curl 示例及固定 Compose 使用 |
| [Control Plane 设计](control_plane/DESIGN.md) | 应用装配、生命周期、SQLite、异常恢复、续期和过期回收 |
| [Control Plane API](control_plane/API.md) | 控制面 HTTP 协议、错误响应、生命周期参数与调用示例 |
| [Docker 设计](control_plane/docker/DESIGN.md) | 专属资源、网络拓扑、代理入口、固定权限和资源限制 |
| [Docker 实现](control_plane/docker/IMPLEMENTATION.md) | 创建与回滚、归属核验、查询、删除、取消、测试与手动验收 |

原动态入口、删除和异常处理资料已并入相应设计与实现章节。已完成的实施清单转为当前行为、验证范围和历史结果，避免将旧计划误读为待实现能力。协议字段与错误表以各组件 API 文档为准，其它文档通过链接引用。

文档正文使用中文，保留必要的英文标识符；代码示例中的注释使用英文。容器限制和命令清理能力均有适用边界，当前项目不承诺生产级安全隔离。
