# 项目开发进度

本文档记录 Agent Sandbox 已完成的开发工作、后续功能和已知优化事项。开发进度按日期倒序排列，最新记录位于最前。

## 开发进度

### 2026-09-23

- 实现最小 Agent HTTP daemon：支持可配置监听地址、`GET /healthz`、HTTP 超时及 SIGINT/SIGTERM 优雅关闭。
- CommandRunner 增加 stdin、Cwd 和 Env 支持，保持 argv 与标准流的字面量传递语义。
- 增加 context、请求级 Timeout 和 Linux 进程组清理，取消时终止仍在同一进程组中的后代进程。
- 增加 stdout/stderr 独立字节上限，超限时取消整个命令进程组并返回可识别错误。
- 完善上述行为的单元测试、race 验证、代码注释规范和项目进度文档。

### 2026-09-22

- 在 `agent/` 下创建独立的 Go 1.25 module。
- 实现最小 CommandRunner，直接通过 argv 启动程序，不隐式使用 Shell。
- 支持独立写出 stdout 和 stderr，并区分非零退出码与命令启动失败。
- 添加空 argv、输出流、退出码、启动错误和参数原样传递测试。

### 2026-09-17

- 初始化项目说明、协作约定和通用 `.gitignore`。
- 记录项目目标、安全边界和小步迭代原则。

## 待开发功能

- SIGTERM 优雅退出和信号转发。
- 对主动创建新会话或逃离进程组的后代进程进行可靠清理。
- 总输出共享额度、输出行数限制、磁盘配额、日志轮转或运行时动态调整额度。
- HTTP 命令执行接口、SSE 输出和后台任务管理。
- PTY 与交互式任务支持。
- sandbox 生命周期控制面，以及创建、就绪、续期、暂停、恢复和删除流程。
- Docker 或其他隔离运行时、资源限制、网络策略、认证和审计。

## 待优化问题

- 当前 CommandRunner 直接在 Agent 所在环境启动本地进程，不提供 sandbox 隔离。
- context 取消和 Timeout 使用 Linux 进程组及 `SIGKILL` 清理进程；当前没有 SIGTERM 宽限期，也无法清理通过 `setsid` 等方式逃离进程组的后代进程。
- 输出限制当前按 stdout 和 stderr 分别统计原始字节，不解释文本编码，也不限制调用方 writer 自身的存储方式。
- CommandRunner 测试面向 Linux/WSL，并依赖 `sh`、`printf`、`pwd` 和 `cat` 等系统程序；尚未覆盖其他平台。
- `Request.Env` 尚不支持删除变量；其中的 `PATH` 也不改变 `Argv[0]` 的初始可执行文件查找规则。
- 项目尚未建立正式的威胁模型，也未验证资源、文件系统和网络隔离边界。
