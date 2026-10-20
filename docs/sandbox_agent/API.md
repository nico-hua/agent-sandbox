# Sandbox Agent API 与 curl 示例

本页对应 [Agent 路由](../../sandbox_agent/server/handler.go)、[命令处理](../../sandbox_agent/server/command_handler.go)、[SSE](../../sandbox_agent/server/command_stream.go) 和 [文件处理](../../sandbox_agent/server/file_handler.go) 的当前实现。内部数据流、安全边界和部署约束见 [设计](DESIGN.md)。以下 HTTP 接口没有认证，仅限本机 Linux/WSL 开发。

## 地址与职责

固定 Compose 入口为 `http://127.0.0.1:18081`。控制面创建的动态实例，应按 [README 快速开始](../../README.md#快速开始) 创建实例、有限重试查询 endpoint，再将返回的 `url` 作为下文 `BASE_URL`。`ready` 与入口可用不是同一个探测结果，endpoint 未就绪时可能返回 503。

Agent 路由只有以下四个。创建、状态查询、endpoint、续期和删除使用控制面的 URL，协议见 [控制面 API](../control_plane/API.md)，不发送到 Agent。

| 方法 | 路径 | 成功响应 |
| --- | --- | --- |
| GET | `/healthz` | 200 JSON 健康状态 |
| POST | `/v1/commands:run` | 200 JSON 命令结果 |
| POST | `/v1/commands:stream` | 200 SSE 前台输出流 |
| POST | `/v1/files?path=<relative-path>` | 201，无响应体 |
| GET | `/v1/files?path=<relative-path>` | 200 原始文件字节 |

命令和文件业务错误使用 `Content-Type: application/json`，结构如下。`code` 用于程序判断，`message` 是对外错误说明，不包含底层启动或文件系统诊断细节。

```json
{"error":{"code":"invalid_request","message":"argv must contain an executable"}}
```

未知路由由 Go `net/http` 返回 404；`/healthz` 的方法错误使用纯文本 405。代理、传输层错误也不保证使用上述 JSON 结构。请求断开或写响应失败时，客户端可能收不到错误响应。

## GET /healthz

成功返回 HTTP 200、`Content-Type: application/json`：

```json
{"status":"ok"}
```

这里只检查 Agent HTTP handler 能响应，不检查 workspace、命令剩余额度、Docker 或外部代理。除 GET 外返回 405，并设置 `Allow: GET`。

## 命令请求：run 与 stream 共用

两个命令接口只接受 POST；其他方法为 405 `method_not_allowed`，`Allow: POST`。请求体最多 1048576 字节（1 MiB），包含所有 JSON 字段和转义开销。只接受一个可解码的请求对象，拒绝未知字段、无效类型、非法 JSON 和后续第二个 JSON 值。建议发送 `Content-Type: application/json`；当前 handler 按 body 解码，不以该请求头作为校验条件。

```json
{
  "argv": ["sh", "-c", "printf '%s:' \"$NAME\"; cat"],
  "cwd": "/workspace",
  "env": {"NAME": "sandbox"},
  "stdin": "hello\n",
  "timeout_ms": 3000,
  "max_output_bytes_per_stream": 65536
}
```

| 字段 | 类型 | 缺省值与约束 |
| --- | --- | --- |
| `argv` | string array | 必填；至少一个元素，`argv[0]` 不能是空字符串。直接执行程序及参数，不隐式使用 shell |
| `cwd` | string | 省略或空字符串时继承 Agent 工作目录；镜像和固定 Compose 中为 `/workspace`。可显式指定绝对或相对目录，不限于 workspace |
| `env` | object of string values | 在 Agent 继承环境上覆盖或增加，变量名大小写敏感；空值可覆盖为 `""`，不修改 Agent 环境 |
| `stdin` | string | 缺省为空；解码后的内容作为 stdin，读完 EOF。不是交互式输入流 |
| `timeout_ms` | integer | 缺省 5000；允许 1～30000 毫秒；执行 context 的取消期限，不是响应的硬期限 |
| `max_output_bytes_per_stream` | integer | 缺省 65536；允许 1～1048576 字节，stdout/stderr 各自独立计数 |

env 的变量名必须非空，不能包含 `=` 或 NUL；变量值不能包含 NUL。timeout 和输出上限的 `null` 与省略一样应用默认值，显式 0 或负数会返回 400，不能用来关闭限制。其他不能由 HTTP 校验提前发现的执行问题，例如程序不存在、不可执行或 cwd 不存在，由 runner 映射为 `command_start_failed`。

同步和 SSE 共享一个命令额度，默认同时 4 条；启动参数 `-max-concurrent-commands` 可设为 1～1024。先校验请求，再获取额度；满额立即返回 429，不排队。HTTP 请求取消会触发执行取消，额度仍需等待 runner 收尾；直接子进程退出但后代持有管道时，存在[超时与清理限制](DESIGN.md#时间取消与进程组)。没有命令 ID、后台任务、后续查询或重连后恢复接口。

### POST /v1/commands:run

运行结束后返回 HTTP 200、`Content-Type: application/json`：

```json
{"exit_code":7,"stdout":"out","stderr":"err"}
```

`stdout` 和 `stderr` 分开返回；没有输出时是空字符串。`exit_code` 是用户程序的退出结果，普通非零退出仍是 HTTP 200，应由调用方检查。Linux 程序被信号终止而不属于 runner 取消错误时，底层退出结果也可能为 -1。

这是 JSON 文本接口，原始输出的非法 UTF-8 字节可能在 JSON 编码时被替换。需要保留二进制输出时使用 SSE 的 Base64 字段，或把结果写入 workspace 后下载。

| HTTP | `error.code` | 含义 |
| --- | --- | --- |
| 400 | `invalid_request` | 无效 JSON/字段、空 argv、无效 env、超出可配置范围 |
| 405 | `method_not_allowed` | 方法不是 POST |
| 413 | `request_too_large` | JSON 请求体超过 1 MiB |
| 413 | `output_limit_exceeded` | stdout 或 stderr 超过单流上限，执行已取消 |
| 422 | `command_start_failed` | 无法启动程序，或其他未单独分类的 runner 错误 |
| 429 | `command_capacity_exceeded` | 同步/SSE 共享的命令额度已满 |
| 504 | `command_timeout` | runner 返回 context deadline exceeded |

runner 错误只返回错误对象，不附带已收集的部分 stdout/stderr。客户端断开或 context 取消时，handler 不再写响应。

### POST /v1/commands:stream

校验和并发错误发生在流开始前，仍按上表返回 JSON HTTP 错误。无法设置流写入期限时还可能返回 500 `stream_unavailable`。通过准备阶段后，Agent 先 flush 以下上游响应头；Nginx 默认会消费或隐藏 `X-Accel-*`，客户端通过代理不一定看到 `X-Accel-Buffering`，详见 [Nginx 代理说明](https://nginx.org/en/docs/http/ngx_http_proxy_module.html#proxy_hide_header)：

```text
HTTP/1.1 200 OK
Content-Type: text/event-stream
Cache-Control: no-cache
X-Accel-Buffering: no
```

每帧包含 `event:` 和一行 JSON `data:`，帧之间以空行分隔：

| `event` | `data` | 语义 |
| --- | --- | --- |
| `stdout` | `{"data_base64":"..."}` | 一块 stdout 原始字节的标准 Base64 编码 |
| `stderr` | `{"data_base64":"..."}` | 一块 stderr 原始字节的标准 Base64 编码 |
| `complete` | `{"exit_code":0}` | 正常运行结束；非零退出同样使用 complete |
| `failure` | `{"error":{"code":"...","message":"..."}}` | runner 失败，使用 `command_timeout`、`output_limit_exceeded` 或 `command_start_failed` |

HTTP 流一旦开始，失败事件仍为 HTTP 200，调用方必须读取终止事件，不能只检查状态码。普通情况下只有一个 `complete` 或 `failure` 作为终止；传输断开、写入或 flush 失败时无法保证终止帧送达。EOF 前没有终止帧表示结果未确认。

输出块边界不对应行、字符或一次程序写入；逐流按到达的顺序拼接解码后的字节，两流之间没有全局顺序保证。输出计数以 Base64 编码前的原始字节为准。超限时可能先收到不超过限额的部分输出，再收到 `failure`。

服务端通过无缓冲输出 channel 提供背压，慢客户端会阻塞输出处理；命令期限也约束等待 channel 交付输出块的 writer。HTTP 流有 35 秒绝对写入期限。断开会触发取消，并在 runner 收尾后释放共享额度，仍受上述[清理限制](DESIGN.md#时间取消与进程组)约束。没有 heartbeat、事件 ID、重放和自动重连语义。客户端使用 `curl -N` 或支持 POST 的流读取器；浏览器原生 `EventSource` 不能直接调用这个 POST 接口。

## 文件请求：POST/GET /v1/files

`path` 必须恰好出现一次，是相对 `/workspace` 的非空路径。允许已有子目录中的文件；拒绝绝对路径、NUL、空路径段、`.`、`..` 和末尾 `/`。查询值按 URL 解码后的路径校验，路径包含特殊字符时应 URL 编码。

路径操作通过 `os.Root` 约束在 workspace 内，不能利用符号链接跳到根外；合法的根内符号链接不是一律拒绝。文件 API 的限制不适用于命令 API，命令可操作 Agent 用户有权访问的其他容器路径。上传/下载共享独立的 2 个并发请求额度，满额立即 429。

| 操作 | 请求/响应与限制 |
| --- | --- |
| POST 上传 | 请求体为原始字节，最多 10485760 字节（10 MiB），不是 multipart。成功为 201 空响应体；不创建父目录，不覆盖已有路径 |
| GET 下载 | 只下载普通文件，最多 10 MiB；成功为 200 原始字节，`Content-Type: application/octet-stream`、精确 `Content-Length`、`Cache-Control: no-store` |

上传以同目录临时文件写完后再发布目标，并清理临时名；同名并发上传仅一个成功。下载先检查文件并将最多 `10 MiB + 1` 字节读入内存复查，再返回内容，不提供 Range。工作区没有磁盘配额，用户命令可以创建超过传输上限的文件。

| HTTP | `error.code` | 触发场景 |
| --- | --- | --- |
| 400 | `invalid_path` | 缺少/重复 path，或路径格式不合法 |
| 400 | `invalid_request` | 不能完整读取上传 body，且请求未取消 |
| 400 | `not_regular_file` | 下载路径不是普通文件 |
| 403 | `file_access_denied` | 无法在根内创建/读取，包括越界符号链接和权限拒绝 |
| 404 | `file_not_found` | 下载文件不存在，或上传父目录不存在 |
| 405 | `method_not_allowed` | 不是 GET/POST；`Allow: GET, POST` |
| 409 | `file_exists` | 上传目标已存在，包含发布时的竞争 |
| 413 | `file_too_large` | 上传或下载超过 10485760 字节 |
| 429 | `file_capacity_exceeded` | 两个文件请求均在处理 |
| 500 | `file_operation_failed` | workspace 不可用、写入/关闭/读取/检查失败，或发布后的临时名清理失败 |

上传超限、读取/写入失败或发布前取消时会尝试清理临时文件，不发布部分目标。发布后临时名清理失败也可能返回 500，此时目标已经存在；请求取消或断开也不证明文件未创建。上传不是可恢复的持久化事务，Agent 崩溃时可能遗留临时文件。并发命令可以修改 workspace，下载不保证快照一致性。

## 本机手动验证

以下 Bash/curl 示例面向 Linux/WSL。固定 Compose 启动会创建或复用固定网络、持久卷，并构建/启动开发容器；已有实例的迁移检查应只使用只读命令，不为了检查而重建。

从仓库根目录检查配置并启动一个固定开发实例：

```bash
docker compose -f deploy/local/compose.yml config --quiet
./deploy/local/run-local.sh
docker compose -f deploy/local/compose.yml ps
docker network inspect agent-sandbox-internal --format '{{.Driver}} {{.Internal}}'
```

启动脚本从自身位置定位 Compose，以 `--wait --wait-timeout 30` 等待服务健康，并拒绝与预期 `bridge true` 不符的固定网络。固定 Compose 不属于控制面 DELETE API 的管理范围；不要用这些启动命令替代动态实例的创建接口。

### 示例变量与入口

固定实例启动后，在同一个 WSL Bash 终端设置变量。HTTP 命令接口没有认证，切勿对不可信网络开放：

```bash
BASE_URL='http://127.0.0.1:18081'
TEST_ID="$(date +%s)-$$"
INPUT="curl-demo-$TEST_ID.txt"
OUTPUT="curl-demo-$TEST_ID-upper.txt"
BIN="curl-demo-$TEST_ID.bin"
LINK="curl-demo-$TEST_ID-escape"
```

对于控制面创建的动态实例，无需启动 Compose；按 [README 快速开始](../../README.md#快速开始) 取得 endpoint 后，将 `BASE_URL` 改为返回的 `url`，其余命令相同。入口 503 时需先在有限期限内重试查询；地址仅供本机 WSL 开发。

保持这些变量在同一终端中，逐段运行后续命令。TEST_ID 用于生成不同文件名；文件 API 不会覆盖同名文件。下文的 sh 是客户端在 argv 中明确指定的程序，Agent 不会隐式执行 Shell。命令接口的 HTTP 200 也不代表命令退出码一定为 0，应同时检查 JSON 的 exit_code。

### 健康检查和命令执行

```bash
curl -i "$BASE_URL/healthz"
```

预期 HTTP 200，JSON 的 status 为 ok。

不传 cwd 时默认工作目录为 /workspace；显式指定 /tmp 则应改变工作目录：

```bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["pwd"]}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["pwd"],"cwd":"/tmp"}' "$BASE_URL/v1/commands:run"
```

两次均应返回 HTTP 200、exit_code 0；stdout 分别为 /workspace 和 /tmp，均以换行结尾。无效 cwd 由 CommandRunner 拒绝：

```bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["pwd"],"cwd":"/workspace/does-not-exist"}' \
  "$BASE_URL/v1/commands:run"
```

预期 HTTP 422、command_start_failed。argv 参数中的空格和 Shell 特殊字符应作为普通文本传入：

```bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["printf","%s","$HOME; *.txt"]}' \
  "$BASE_URL/v1/commands:run"
```

预期 HTTP 200，stdout 原样为 $HOME; *.txt。下面显式执行 sh，验证 env 覆盖、stdin、多流输出：

```bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf \"%s:\" \"$DEMO_NAME\"; cat; printf warning >&2"],"env":{"DEMO_NAME":"sandbox"},"stdin":"hello\n"}' \
  "$BASE_URL/v1/commands:run"
```

预期 HTTP 200、exit_code 0、stdout 为 sandbox:hello 加换行，stderr 为 warning。env 只影响该次命令。所有可选字段还可放在同一请求中：

```bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf \"%s:\" \"$DEMO_NAME\"; cat"],"cwd":"/tmp","env":{"DEMO_NAME":"combined"},"stdin":"input","timeout_ms":3000,"max_output_bytes_per_stream":1024}' \
  "$BASE_URL/v1/commands:run"
```

预期 HTTP 200、exit_code 0、stdout 为 combined:input。分别检查自定义超时、输出上限和普通非零退出：

```bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["sleep","2"],"timeout_ms":100}' \
  "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["printf","%s","123456"],"max_output_bytes_per_stream":5}' \
  "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf out; printf err >&2; exit 7"]}' \
  "$BASE_URL/v1/commands:run"
```

预期依次为 HTTP 504 command_timeout、HTTP 413 output_limit_exceeded、HTTP 200 且 exit_code 7（stdout 为 out，stderr 为 err）。未提供限制时，默认超时 5 秒、每流输出上限 64 KiB；显式给 0 会被拒绝，不能借此取消限制：

```bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["true"],"timeout_ms":0}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["true"],"max_output_bytes_per_stream":0}' \
  "$BASE_URL/v1/commands:run"
```

两次均预期 HTTP 400、invalid_request。还可以检查空 argv、无效环境变量名和超过服务端上限的配置；这些请求都不应启动命令：

```bash
curl -i -H 'Content-Type: application/json' -d '{"argv":[]}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' -d '{"argv":[""]}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' -d '{"argv":["true"],"env":{"BAD=NAME":"x"}}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' -d '{"argv":["true"],"timeout_ms":30001}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' -d '{"argv":["true"],"max_output_bytes_per_stream":1048577}' "$BASE_URL/v1/commands:run"
```

预期五次均为 HTTP 400、invalid_request。最大允许的 timeout_ms 为 30000，每流输出上限为 1048576 字节；HTTP 命令请求体本身最多 1 MiB。

### 前台流式命令（SSE）

`POST /v1/commands:stream` 复用同步接口的 JSON 请求字段、参数校验、超时、每流输出上限和命令并发额度。使用 `curl -N`（`--no-buffer`）避免 curl 缓冲响应。这是 POST 接口，不能直接使用浏览器原生 `EventSource`。沿用文首的 `BASE_URL`：

```bash
curl -N -i --max-time 10 -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf first; sleep 2; printf second"]}' \
  "$BASE_URL/v1/commands:stream"
```

预期 HTTP 200、`Content-Type: text/event-stream`。先看到第一段 stdout，约两秒后看到第二段 stdout，最后收到 `complete` 事件。事件形式如下（输出块边界不保证对应一次 `printf`）：

```text
event: stdout
data: {"data_base64":"Zmlyc3Q="}

event: stdout
data: {"data_base64":"c2Vjb25k"}

event: complete
data: {"exit_code":0}
```

`data_base64` 是原始输出字节的 Base64 编码；在 WSL 中可用 `printf '%s' 'Zmlyc3Q=' | base64 -d` 解出 `first`。不要假定单个事件正好是一行文本或完整 UTF-8 字符。上例显式执行 `sh`，Agent 不会隐式添加 Shell。

检查独立的 stderr 和普通非零退出码：

```bash
curl -N -sS --max-time 10 -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf out; printf err >&2; exit 7"]}' \
  "$BASE_URL/v1/commands:stream"
```

预期 stdout、stderr 事件分别出现，随后是 `event: complete` 和 `data: {"exit_code":7}`。超时或输出超限发生在流开始后，会发送 `failure` 终止事件；HTTP 状态仍为 200：

```bash
curl -N -i --max-time 10 -H 'Content-Type: application/json' \
  -d '{"argv":["sleep","5"],"timeout_ms":100}' \
  "$BASE_URL/v1/commands:stream"
curl -N -i --max-time 10 -H 'Content-Type: application/json' \
  -d '{"argv":["printf","%s","123456"],"max_output_bytes_per_stream":5}' \
  "$BASE_URL/v1/commands:stream"
```

预期错误码依次为 `command_timeout` 和 `output_limit_exceeded`；后者最多先发送编码了 `12345` 的 stdout 事件。无法启动程序时也会收到 `failure`，错误码为 `command_start_failed`。流开始前的参数错误仍返回 JSON HTTP 错误：

```bash
curl -N -i -H 'Content-Type: application/json' \
  -d '{"argv":[]}' "$BASE_URL/v1/commands:stream"
```

预期 HTTP 400、`invalid_request`。测试客户端断开，可运行下例，在看到第一段输出后按 Ctrl+C：

```bash
curl -N -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf ready; sleep 20"],"timeout_ms":30000}' \
  "$BASE_URL/v1/commands:stream"
```

断开后客户端不会收到终止事件；服务端触发取消，并在 runner 收尾后释放命令并发额度。后代持有管道时可能无法及时收尾，边界见[超时与清理限制](DESIGN.md#时间取消与进程组)。此接口没有命令 ID 或断开后的状态查询。

### 上传、相对路径处理与下载

文件 API 的 path 是相对 /workspace 的路径。上传请求体是原始字节，成功返回 HTTP 201；同名文件返回 409。

```bash
printf 'hello from curl\n' | curl -i --data-binary @- \
  "$BASE_URL/v1/files?path=$INPUT"
curl -i -H 'Content-Type: application/json' \
  -d "{\"argv\":[\"cat\",\"$INPUT\"],\"cwd\":\"/workspace\"}" \
  "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' \
  -d "{\"argv\":[\"sh\",\"-c\",\"tr a-z A-Z < '$INPUT' > '$OUTPUT'\"],\"cwd\":\"/workspace\",\"timeout_ms\":3000,\"max_output_bytes_per_stream\":65536}" \
  "$BASE_URL/v1/commands:run"
curl -i "$BASE_URL/v1/files?path=$OUTPUT"
```

两个命令均应返回 HTTP 200、exit_code 0；cat 输出 hello from curl，下载结果是 HELLO FROM CURL（末尾有换行），Content-Type 为 application/octet-stream。这也验证相对文件路径按 cwd 解析。文件 API 只能访问 /workspace，但这一约束不适用于命令 API：命令仍可访问 Agent 用户有权限访问的其他容器路径。

二进制往返用 cmp 比较原始字节，不直接在终端显示：

```bash
LOCAL_BIN="$(mktemp)"
DOWNLOADED_BIN="$(mktemp)"
printf '\000A\377' > "$LOCAL_BIN"
curl -i --data-binary @"$LOCAL_BIN" "$BASE_URL/v1/files?path=$BIN"
curl -fsS "$BASE_URL/v1/files?path=$BIN" -o "$DOWNLOADED_BIN"
cmp "$LOCAL_BIN" "$DOWNLOADED_BIN" && printf 'binary round trip OK\n'
```

上传应返回 HTTP 201；cmp 返回 0 并打印 binary round trip OK。结束后仅清理这两个本地临时文件：rm -- "$LOCAL_BIN" "$DOWNLOADED_BIN"。它不会清理 workspace 卷。

### 错误与大小限制

```bash
printf 'again' | curl -i --data-binary @- "$BASE_URL/v1/files?path=$INPUT"
curl -i "$BASE_URL/v1/files?path=curl-demo-$TEST_ID-missing.txt"
curl -i "$BASE_URL/v1/files?path=..%2Fetc%2Fpasswd"
```

预期依次为 HTTP 409 file_exists、HTTP 404 file_not_found、HTTP 400 invalid_path。通过命令创建指向 workspace 外的符号链接，再尝试通过文件 API 下载，应返回 HTTP 403 file_access_denied：

```bash
curl -i -H 'Content-Type: application/json' \
  -d "{\"argv\":[\"ln\",\"-s\",\"/etc/passwd\",\"/workspace/$LINK\"]}" \
  "$BASE_URL/v1/commands:run"
curl -i "$BASE_URL/v1/files?path=$LINK"
```

上传和下载单文件上限均为 10 MiB（10,485,760 字节）。下面只用零字节测试超限，预期 HTTP 413 file_too_large，且不创建文件：

```bash
head -c 10485761 /dev/zero | curl -i --max-time 30 --data-binary @- \
  "$BASE_URL/v1/files?path=curl-demo-$TEST_ID-too-large.bin"
```

上传的文件留在持久化的 /workspace 命名卷中。若要验证容器重建后的持久化，记录 TEST_ID，重建后重新设置 BASE_URL、INPUT 和 OUTPUT，再下载原文件；上述文件示例不自动停止容器或删除 volume。清理时只指定自己创建的测试文件。控制面动态实例的生命周期操作见 [控制面 API](../control_plane/API.md)，固定 Compose 实例不属于该 DELETE API 的管理范围。
