
# Agent Sandbox

一个从零开始探索 AI Agent sandbox 设计与实现的项目。

## 项目状态

项目当前处于早期实现阶段。`agent/` 是一个使用 Go 1.25 的独立 module，包含可长期运行的最小 HTTP daemon 和 CommandRunner。HTTP 服务目前只提供健康检查；CommandRunner 支持 argv、标准流、工作目录、环境变量、超时、进程组清理和单流输出限制，但尚未接入 HTTP。

当前 CommandRunner 直接在 Agent 所在环境中创建进程，不提供容器、namespace 或其他 sandbox 隔离。HTTP 服务仅使用 Go 标准库；隔离运行时和部署平台仍未确定。

## 为什么做这个项目

AI Agent 可能需要执行命令、修改文件、运行代码、访问网络或启动用户服务。一个可用的 sandbox 不只是启动一个容器，还需要回答：

- 工作负载与宿主机之间的隔离边界在哪里？
- 如何限制 CPU、内存、进程数、磁盘和运行时间？
- 如何安全地执行命令并实时返回输出？
- 如何暴露服务 endpoint，同时避免未经授权的访问？
- 如何控制 sandbox 的出站网络？
- 创建失败、进程崩溃、服务重启和超时后如何清理或恢复？

本项目优先学习和验证这些基础问题，不以一开始复现完整商业平台为目标。

## 设计方向（尚未冻结）

后续实现可能围绕以下概念展开：

- 生命周期控制面：创建、查询、续期、暂停、恢复和删除 sandbox。
- sandbox 数据面：在隔离环境内执行命令、文件操作和 Agent 任务。
- 运行时适配：将统一的 sandbox 请求转换为具体的容器或其他隔离运行时。
- 网络和访问控制：限制出站网络，并对公开 endpoint 做认证。
- 可靠性：状态、TTL、超时、取消、回滚、清理和重启恢复。
- 可观察性：日志、指标、事件和可诊断的失败原因。

这些是学习方向，不是已经承诺的目录结构或技术选型。新增技术方案前，应先说明为什么需要它，以及它解决了哪个已验证的问题。

## 推荐的第一个垂直切片

建议先只实现一条最小、可测试的链路：

```text
启动一个受限的本地 sandbox
    -> sandbox 内运行一个执行 agent
    -> 通过 HTTP 执行一个命令
    -> 返回 stdout、stderr 和退出码
    -> 支持 timeout 和取消
    -> 删除 sandbox 并确认资源已清理
```

在这条链路稳定之前，暂不同时引入多种运行时、多语言 SDK、复杂网络代理、快照、资源池或微 VM。

目前已完成基础命令执行、取消、同进程组清理以及最小 HTTP daemon 和健康检查；尚未实现受限 sandbox，也没有 HTTP 命令执行接口。

## 当前如何开始

当前实现和测试面向 Linux/WSL，需要安装 Go 1.25。开始参与前请先阅读：

- [AGENTS.md](AGENTS.md)：Codex 和贡献者的工作约定。
- [项目开发进度](docs/PROJECT_PROGRESS.md)：已完成工作、待开发功能和待优化问题。

在 `agent/` 目录运行验证：

```bash
go test ./...
go test -race ./...
go vet ./...
```

启动 Agent HTTP 服务：

```bash
go run .
```

服务默认监听 `127.0.0.1:8080`。可以使用 `-listen` 修改地址：

```bash
go run . -listen 0.0.0.0:8080
```

健康检查：

```bash
curl -i http://127.0.0.1:8080/healthz
```

`GET /healthz` 返回 `200 OK` 和 `{"status":"ok"}`；该路径的其他方法返回 `405 Method Not Allowed`。SIGINT 和 SIGTERM 会触发最长 5 秒的优雅关闭。

CommandRunner 当前提供以下最小接口：

```go
var stdout bytes.Buffer
result, err := command.Run(
	context.Background(),
	command.Request{
		Argv:                    []string{"printf", "%s", "hello"},
		Cwd:                     "/tmp",
		Env:                     map[string]string{"EXAMPLE": "literal value"},
		Timeout:                 5 * time.Second,
		MaxOutputBytesPerStream: 1024,
	},
	nil,
	&stdout,
	nil,
)
```

`Argv` 会直接传递给目标程序，不经过隐式 Shell。独立的 stdin 参数为 nil 时子进程读取到 EOF；`Cwd` 为空时继承当前工作目录；`Env` 为空时继承 Agent 环境，非空时在继承环境上覆盖或新增变量。`Timeout` 为零时只使用调用方 context，正数时创建请求级 deadline。`MaxOutputBytesPerStream` 分别限制 stdout 和 stderr，零值表示不限制。

程序正常结束或返回非零状态时，退出码通过 `Result` 返回且 `error` 为 nil；程序无法启动时返回退出码 `-1` 和非 nil error。调用方取消、超时或输出超限返回 `ExitCode=-1`，错误可以通过 `errors.Is` 区分。Linux/WSL 下取消会向命令的独立进程组发送 `SIGKILL`。

当前限制包括：

- HTTP 服务目前只有健康检查，尚未提供命令执行 API、认证、TLS、SSE 或后台任务。
- 命令取消仅清理仍在同一进程组中的进程；尚未实现 SIGTERM 宽限期、信号转发或逃逸进程清理。
- 输出限制按 stdout 和 stderr 分别统计原始字节，不提供共享额度、磁盘配额或日志轮转。
- 不支持环境变量删除语义或环境变量文件。
- `Request.Env` 中的 `PATH` 只影响子进程环境，不改变 `Argv[0]` 的初始查找规则。
- 当前执行不构成 sandbox 隔离，不能用于安全运行不可信代码。

## 安全声明

当前项目尚未达到可运行不可信代码的生产安全标准。不要把当前版本部署到生产环境，也不要把宿主机 Docker socket、云凭证、生产密钥或任意宿主机目录交给 sandbox 工作负载。

任何“安全隔离”结论都必须建立在明确的威胁模型、隔离机制说明和攻击性验证之上。普通容器、进程隔离、网络策略和微 VM 的安全边界不同，不能混为一谈。

## 使用 Codex 协作

建议将工作拆成可独立验收的小任务：

1. 先分析现有文件和目标，不预先假定技术栈。
2. 明确本次只实现的垂直切片和不实现的内容。
3. 先补测试或验收标准，再编写实现。
4. 运行与改动范围匹配的验证。
5. 在 README 或设计文档中记录已经确定的约束。

不要直接要求一次性“完整复现一个 sandbox 平台”。应优先让每个阶段都能运行、观察和解释。

## 许可证

项目尚未选择许可证，因此当前不包含 `LICENSE` 文件。正式公开发布前需要由项目维护者选择并添加合适的开源许可证。
