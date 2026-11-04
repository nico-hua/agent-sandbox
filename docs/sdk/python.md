# 同步 Python SDK

Python SDK 将现有控制面 HTTP API 与实例 Agent HTTP API 封装为同步调用。分发包名为 `agent-sandbox-sdk`，导入名为 `agent_sandbox`，需要 Python 3.12+。运行时仅依赖 HTTPX 与标准库，不导入控制面、Docker SDK 或 FastAPI；客户端不连接 Docker，也不提供 CLI、异步接口、SSE、后台命令、持久化、自动续期或自动资源回收。

HTTP 契约以 [Control Plane API](../control_plane/API.md) 和 [Sandbox Agent API](../sandbox_agent/API.md) 为准。SDK 不改变服务端状态、字段、错误码或隔离边界。控制面默认要求单管理员 Bearer API key；数据面 endpoint 由控制面返回，Agent 命令与文件入口仍无认证。仍限可信本机开发，控制面认证不构成完整执行隔离。

## 安装与最小调用

在仓库根目录安装独立 SDK，无需安装控制面依赖：

```bash
python -m pip install ./sdk/python
```

仓库没有发布到 PyPI 的前提；上述命令安装本地源码包。构建 wheel 后也可在外部虚拟环境安装 `sdk/python/dist/` 中的 wheel。

```python
import os
import sys
from agent_sandbox import SDKError, SandboxClient

with SandboxClient(
    "http://127.0.0.1:18083", api_key=os.environ["AGENT_SANDBOX_API_KEY"], timeout=10.0,
) as client:
    sandbox = client.create_sandbox(environment="basic", ttl_seconds=300)
    print(sandbox.sandbox_id)
    try:
        sandbox.wait_ready(timeout=30)
        result = sandbox.run(["pwd"])
        print(result.exit_code, result.stdout, result.stderr)
    finally:
        primary = sys.exception()
        try:
            sandbox.delete(delete_workspace=True)
        except SDKError:
            print("Cleanup failed; inspect", sandbox.sandbox_id)
            if primary is None:
                raise
```

先按[控制面启动配置](../control_plane/DESIGN.md#控制面认证与启动配置)在调用进程安全配置与服务相同的 `AGENT_SANDBOX_API_KEY`；示例由调用方显式读取后传入 SDK，不打印 key。

这段最小示例会显式删除实例及其私有卷；清理失败报告 ID 并保留原始异常。`create_sandbox()` 返回初始状态，不等待就绪；命令退出码非零不会抛异常。`argv` 直接执行，不解析 shell；需要重定向时须显式使用 `['sh', '-c', '...']`，不要拼入不可信输入。

`SandboxClient.close()` 可重复调用，`with` 退出只关闭 HTTP 连接；不会删除实例或工作区。关闭后继续请求抛出 `ClientClosedError`。可在新客户端通过 `get_sandbox(id)` 或 `get_workspace(id)` 重新取得句柄。

## 公开接口

`SandboxClient(base_url='http://127.0.0.1:18083', *, api_key=None, timeout=5.0, transport=None)` 拥有一个同步 HTTPX Client。`timeout` 接受有限正数或各阶段均有限的 `httpx.Timeout`；`transport` 可用于 `httpx.MockTransport` 测试，由 SDK 负责关闭。SDK 不读取环境代理，不跟随重定向，保留正常 TLS 校验；连接池最多 16 个连接、8 个 keepalive 连接。

| 对象 | 方法或属性 | 返回与行为 |
| --- | --- | --- |
| `SandboxClient` | `create_sandbox(*, environment=…, ttl_seconds=…, resources=…, network=…, workspace_id=…)` | 创建并返回 `Sandbox`；省略字段不发送，使用服务端默认值 |
| `SandboxClient` | `get_sandbox(sandbox_id)` | GET 当前状态后返回 `Sandbox` |
| `SandboxClient` | `create_workspace(*, environment=…)` | 返回新建 `Workspace`，默认环境由服务端决定 |
| `SandboxClient` | `get_workspace(workspace_id)` | GET 当前状态后返回 `Workspace` |
| `SandboxClient` | `close()`、上下文管理器 | 关闭连接，不改变远端资源 |
| `Sandbox` | `sandbox_id`、`info` | 不透明 ID、缓存的 `SandboxInfo` |
| `Sandbox` | `refresh()` | 查询并更新缓存，返回 `SandboxInfo` |
| `Sandbox` | `get_endpoint()` | 查询并返回严格校验的本机 HTTP URL |
| `Sandbox` | `wait_ready(*, timeout=30.0, poll_interval=0.25)` | 轮询控制面 `/readyz`，成功返回 `ReadyResult` |
| `Sandbox` | `run(argv, *, cwd=…, env=…, stdin=…, timeout_ms=…, max_output_bytes_per_stream=…)` | 返回 `CommandResult`，非零退出码仍为正常结果 |
| `Sandbox` | `write_file(path, content)` | 上传 `bytes`，成功返回 `None`，不隐式覆盖文件 |
| `Sandbox` | `read_file(path)` | 返回原始 `bytes` |
| `Sandbox` | `renew(ttl_seconds)` | 返回 `RenewalResult`，同时更新缓存的 `expires_at` |
| `Sandbox` | `pause()`、`resume()` | 成功返回 `None`；恢复后可显式等待就绪 |
| `Sandbox` | `delete(delete_workspace=False)` | 成功返回 `None`；默认保留卷 |
| `Workspace` | `workspace_id`、`info` | 不透明 ID、缓存的 `WorkspaceInfo` |
| `Workspace` | `refresh()` | 查询并更新缓存，返回 `WorkspaceInfo` |
| `Workspace` | `delete()` | 永久删除空闲工作区及文件，成功返回 `None` |

`api_key: str | None = None` 只由调用方显式传入，SDK 不自动读取 `AGENT_SANDBOX_API_KEY` 或其他环境变量。`None` 不发送 Authorization；字符串必须精确匹配 1～128 个 ASCII `[A-Za-z0-9_-]`，空值、空白、非法字符、超长及其他类型在请求前抛出不包含输入值的 `ValueError`。key 只按单次控制面请求附加，发送前核对 URL 的 scheme、host 和有效 port 与配置控制面同源；不放在共享 HTTP client 的默认 header 或 auth 中。所有 Agent 命令与文件请求均不带管理员 key，即使 endpoint 与控制面同源。401 表达为 `APIError(code="unauthorized")`，就绪轮询立即抛出，不继续重试。key 不应出现在日志、CLI 参数、用户命令 env 或 `repr` 中。

除上述 `api_key=None` 外，可选参数必须省略才表示缺省；显式 `None`、布尔值冒充整数、错误容器类型等非 HTTP 输入抛出 `ValueError`。SDK 复制 `argv` 与配置映射，整数 `0` 按原值发送，不替换为默认值；资源、TTL、环境注册、DNS 与状态策略由服务端校验。例如：

```python
sandbox = client.create_sandbox(
    environment="python",
    ttl_seconds=600,
    resources={"cpu_millis": 750, "memory_mib": 384},
    network={"allow_domains": ["pypi.org"]},
)
```

SDK 接受自定义环境名称，由服务端注册表判断是否可用，不接受任意镜像配置。实例 ID 必须为 `sbx_` 加 32 位小写十六进制，工作区 ID 为 `wsp_` 加 32 位小写十六进制；构造 URL 前校验，不能传入路径。

各结果是不可变 dataclass，均可从 `agent_sandbox` 导入：

| 模型 | 字段 |
| --- | --- |
| `SandboxInfo` | `sandbox_id`、`status`、`created_at`、`expires_at`、可选 `reason`、`message`、`workspace_id` |
| `WorkspaceInfo` | `workspace_id`、`status`、`created_at`、可空 `sandbox_id` |
| `CommandResult` | 整数 `exit_code`、文本 `stdout` 与 `stderr` |
| `RenewalResult` | `sandbox_id`、`expires_at` |
| `ReadyResult` | `sandbox_id`、`status='ready'` |

时间解析为带 UTC 时区的 `datetime` 并保留微秒；实例查询的时间可能为 `None`。必要字段、类型、时间与身份不符时抛出 `ProtocolError`；允许未来增加的响应字段与非空状态字符串。`info` 是最近取得的快照，`pause()`、`resume()`、`delete()` 与就绪轮询不隐式刷新它。

## 命令、文件与就绪限制

每次命令或文件操作都重新查询控制面 endpoint，然后向 Agent 发送一次请求，不缓存入口，也不替调用方重试。入口必须是 `http://127.0.0.1:<port>`，仅允许可选的根路径斜线，端口为 1～65535，不接受凭据、其它主机、额外路径、查询或片段。

`run()` 的 `stdout`、`stderr` 是当前 API 的普通文本字符串，不是 Base64。`stdin` 为字符串。省略 `timeout_ms` 使用 Agent 的 5000 ms 默认命令超时；SDK 将 HTTP read timeout 至少扩展到有效命令超时加 5 秒，连接、写入与池等待仍受配置限制。HTTP 超时与 Agent 命令超时不同：后者返回 HTTP 504、`command_timeout`，由 SDK 表达为 `APIError`。输出上限通过 `max_output_bytes_per_stream` 传给 Agent，服务端决定具体合法范围。

文件路径相对于 `/workspace`，通过 query 参数编码，例如 `write_file('input.bin', b'data')`；路径权限、workspace 边界与父目录由 Agent 校验。SDK 不读写本地文件、不编码文本、不创建父目录。`write_file(path, content)` 只接受 `bytes`，最大 10 MiB，超限在查询 endpoint 前拒绝；已有文件由服务端拒绝覆盖，成功要求 POST 返回 201。下载也按实际字节数限制为 10 MiB；需要修改文件时使用明确命令。JSON 成功响应最多 16 MiB，超限或不支持的响应编码视为协议错误；错误体最多 64 KiB，超限或编码不支持时保留 HTTP 状态并使用 `http_error`。SDK 不自动解压响应。

`wait_ready()` 使用单调时钟与有限预算，只请求控制面就绪接口，不先后额外刷新状态或直接调用 Agent。`timeout` 必须有限且非负，`poll_interval` 必须有限且大于零；预算为 0 立即抛出 `ReadinessTimeoutError`。仅对以下状态码和错误码组合继续轮询：

| HTTP | 错误码 |
| --- | --- |
| 503 | `sandbox_not_ready`、`sandbox_endpoint_unavailable`、`sandbox_agent_unavailable`、`docker_unavailable` |
| 504 | `sandbox_readiness_timeout` |

其它 API 错误、网络错误与协议错误立即上抛。轮询把各 HTTP 阶段的 timeout 限制到剩余预算；HTTPX 的阶段 timeout 不等同于不可超越的整个请求总 deadline，最后一次请求仍可能让实际耗时超过预算。轮询不会恢复、续期或删除实例。

## 错误、重试与取消

| 异常 | 含义与诊断 |
| --- | --- |
| `SDKError` | SDK 异常基类 |
| `APIError` | HTTP 非成功结果；含 `status_code`、`code`、`message`、可选 `sandbox_id` 与 `workspace_id` |
| `NetworkError` | HTTP 网络故障，远端操作可能已执行；可能附带已知 ID |
| `SDKTimeoutError` | HTTPX timeout，继承 `NetworkError` |
| `ProtocolError` | 成功响应结构、身份、大小、状态码或 endpoint 不符合约定 |
| `ReadinessTimeoutError` | 就绪轮询预算耗尽，含 `sandbox_id` |
| `ClientClosedError` | 客户端已关闭 |

错误码保留服务端字符串，包括未知的新码，不为每个码创建异常子类。非 JSON 或格式错误的 HTTP 错误使用 `http_error` 与固定消息；不暴露原始 HTML。`str(APIError)` 只包含 HTTP 状态与错误码，不包含响应 message 或原始正文；如需记录 `message`，由调用方决定诊断信息的保存范围。

除显式就绪轮询外，所有 GET 和写操作都只尝试一次；SDK 不自动重放创建、命令、文件或生命周期操作。网络断开、超时、调用方中断或响应丢失不能证明写操作未提交。创建结果不明时保存错误中的诊断 ID；有 ID 可通过新客户端查询，仍无法确认时由运维核验，不能盲目重新创建。命令重试可能重复副作用，文件重试可能遇到禁止覆盖；生命周期重试也须遵循当前 API 的状态与归属要求。

暂停冻结原实例进程，TTL 继续计算；恢复不自动等待就绪。续期按 `max(当前期限, 校验时刻 + ttl_seconds)`，不在原期限累加、不缩短。默认实例删除保留卷；私有卷可用 `delete(delete_workspace=True)` 显式删除。绑定独立工作区的实例禁止此参数，必须先删除实例，再调用 `workspace.delete()`。

## 工作区复用示例

[完整脚本](../../examples/python-workspace-reuse.py) 只使用安装后的 SDK 与标准库。它创建工作区 W，创建并删除实例 A，再由 B 读取同一文件，并通过显式命令修改；最后逆序删除 B 与 W。工作区只保留文件，同一时刻仅允许一个实例持有，不保存进程状态。绑定要求所选环境的实际镜像 ID 相同。

先按[快速开始](../../README.md#快速开始)启动本机服务并准备镜像，然后从仓库根执行：

```bash
uv run --project sdk/python python examples/python-workspace-reuse.py \
  --control-url http://127.0.0.1:18083
```

脚本显式读取调用终端的 `AGENT_SANDBOX_API_KEY` 并传入 SDK，不接受 key 命令行参数、不打印 key。已用 pip 安装时也可直接 `python examples/python-workspace-reuse.py --control-url ...`。脚本只清理本次成功创建的资源；保存并打印 ID，清理失败保留原始异常并报告可能剩余的 ID。创建失败提供的诊断 ID 会另行打印，需核验结果；脚本不会搜索或接管已有开发资源。

## 开发与验证

在 `sdk/python/` 执行，默认 MockTransport 测试不连接服务、不操作 Docker：

```bash
uv sync --locked
uv run --locked pytest
uv run --locked python -m compileall -q agent_sandbox tests
uv build
```

可从仓库根检查示例语法与补丁：

```bash
python -m py_compile examples/python-workspace-reuse.py
git diff --check
```

真实 SDK 验收仅针对维护者单独启动的隔离控制面：必须使用新的临时 SQLite 数据库、独立运行锁和本轮资源记录，启用正常 lifespan，并准备本机 Docker 与所需镜像。不能把正在使用的开发服务当作测试目标，也不能通过删除或重建旧容器、网络、卷或开发数据库准备环境。服务的可信应用装配可用 `create_app(database_path=临时路径, reaper_enabled=False, api_key=测试key)`；这属于测试服务准备，SDK 测试本身不导入服务端或 Docker SDK。

在 `sdk/python/` 显式执行以下命令；前三个隔离开关缺少任意一个时，在发起网络请求前跳过；显式启用后还必须配置与隔离服务相同的 `SDK_TEST_API_KEY`，缺少时在网络和分配资源前失败。该变量仅由集成测试读取并显式传入 SDK：

```bash
RUN_SDK_INTEGRATION=1 \
SDK_TEST_CONTROL_URL=http://127.0.0.1:18084 \
SDK_TEST_ISOLATED=1 \
SDK_TEST_API_KEY="$AGENT_SANDBOX_API_KEY" \
uv run --locked pytest -m sdk_integration -v
```

测试先通过公开 SDK 验证无 key 和错误 key 均返回 401，再用有效 key 创建 W→A→B，覆盖二进制文件、非零命令退出码、占用冲突、暂停/恢复、就绪与续期，以及持久文件复用。真实 HTTP transport 仅记录 Agent 命令/文件请求计数与 Authorization 是否存在，确认不发送管理凭据；另通过命令退出码验证用户进程没有 `AGENT_SANDBOX_API_KEY`，不输出环境或 key。只删除本次已知创建的 ID，清理失败报告剩余 ID 并保留原始失败。SDK 测试不会启动服务或查询 Docker；维护者需在测试前后独立核对 Docker 基线，确认既有资源未改变。
