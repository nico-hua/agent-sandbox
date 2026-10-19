# 本地接口 curl 手动测试

以下命令在 WSL 的 Bash 中执行，面向 deploy/local/run-local.sh 启动的容器入口。HTTP 命令接口没有认证，切勿对不可信网络开放。先在一个终端执行：

```bash
cd ~/agent-sandbox
./deploy/local/run-local.sh
BASE_URL='http://127.0.0.1:18081'
TEST_ID="$(date +%s)-$$"
INPUT="curl-demo-$TEST_ID.txt"
OUTPUT="curl-demo-$TEST_ID-upper.txt"
BIN="curl-demo-$TEST_ID.bin"
LINK="curl-demo-$TEST_ID-escape"
```

对于控制面创建的动态实例，无需启动 Compose；按 [README 快速开始](../README.md#快速开始) 取得 endpoint 后，将 `BASE_URL` 改为返回的 `url`，其余命令相同。入口 503 时需先在有限期限内重试查询；地址仅供本机 WSL 开发。

保持这些变量在同一终端中，逐段运行后续命令。TEST_ID 用于生成不同文件名；文件 API 不会覆盖同名文件。下文的 sh 是客户端在 argv 中明确指定的程序，Agent 不会隐式执行 Shell。命令接口的 HTTP 200 也不代表命令退出码一定为 0，应同时检查 JSON 的 exit_code。

## 健康检查和命令执行

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

## 前台流式命令（SSE）

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

断开后客户端不会收到终止事件；服务端应取消命令并释放命令并发额度。此接口没有命令 ID 或断开后的状态查询。

## 上传、相对路径处理与下载

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

## 错误与大小限制

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

上传的文件留在持久化的 /workspace 命名卷中。若要验证容器重建后的持久化，记录 TEST_ID，重建后重新设置 BASE_URL、INPUT 和 OUTPUT，再下载原文件；上述文件示例不自动停止容器或删除 volume。清理时只指定自己创建的测试文件。控制面动态实例的生命周期删除见下一节，固定 Compose 实例不属于该 DELETE API 的管理范围。

## 动态实例的手动删除

这组命令面向本机控制面，创建并仅删除本次记录的两个动态实例。先按 [控制面启动说明](CONTROL_PLANE.md) 准备两个固定镜像并启动 `127.0.0.1:18083`。默认删除会回收 proxy、sandbox、ingress 和 internal 网络，保留 workspace；显式 `delete_workspace=true` 会不可恢复地删除该卷中的全部数据。不要把其他开发实例的 ID 代入这些命令。

```bash
CONTROL_URL='http://127.0.0.1:18083'
CREATED_A=$(curl -fsS -X POST "$CONTROL_URL/v1/sandboxes")
CREATED_B=$(curl -fsS -X POST "$CONTROL_URL/v1/sandboxes")
SANDBOX_A=$(printf '%s' "$CREATED_A" | python3 -c 'import json,sys; print(json.load(sys.stdin)["sandbox_id"])')
SANDBOX_B=$(printf '%s' "$CREATED_B" | python3 -c 'import json,sys; print(json.load(sys.stdin)["sandbox_id"])')
printf 'A=%s\nB=%s\n' "$SANDBOX_A" "$SANDBOX_B"
```

在有限时间内分别等待入口，确认两个变量均获得本机 URL 后再继续。Agent 内部 ready 与代理入口可用分别查询，入口尚未就绪时为 503：

```bash
ENDPOINT_A=''
ENDPOINT_B=''
for attempt in $(seq 1 30); do
  if ENDPOINT_A=$(curl -fsS "$CONTROL_URL/v1/sandboxes/$SANDBOX_A/endpoint" | python3 -c 'import json,sys; print(json.load(sys.stdin)["url"])'); then
    break
  fi
  sleep 1
done
for attempt in $(seq 1 30); do
  if ENDPOINT_B=$(curl -fsS "$CONTROL_URL/v1/sandboxes/$SANDBOX_B/endpoint" | python3 -c 'import json,sys; print(json.load(sys.stdin)["url"])'); then
    break
  fi
  sleep 1
done
printf 'A endpoint=%s\nB endpoint=%s\n' "$ENDPOINT_A" "$ENDPOINT_B"
printf 'retained-data-A' | curl -fsS --data-binary @- "$ENDPOINT_A/v1/files?path=delete-demo.txt"
printf 'independent-data-B' | curl -fsS --data-binary @- "$ENDPOINT_B/v1/files?path=delete-demo.txt"
curl -fsS "$ENDPOINT_A/v1/files?path=delete-demo.txt"
curl -fsS "$ENDPOINT_B/v1/files?path=delete-demo.txt"
```

可在控制面终端按 Ctrl+C，使用原启动命令重启，然后继续在保留上述变量的终端操作。删除不依赖原创建进程的内存，控制面重启后会从 Docker 标签恢复资源。默认删除 A：

```bash
curl -i -X DELETE "$CONTROL_URL/v1/sandboxes/$SANDBOX_A"
curl -i "$CONTROL_URL/v1/sandboxes/$SANDBOX_A"
curl -i "$CONTROL_URL/v1/sandboxes/$SANDBOX_A/endpoint"
curl -i --max-time 3 "$ENDPOINT_A/healthz"
docker volume inspect --format '{{json .Labels}}' "agent-sandbox-$SANDBOX_A-workspace"
curl -i -X DELETE "$CONTROL_URL/v1/sandboxes/$SANDBOX_A"
```

预期 DELETE 两次均为 204 空响应体；A 的查询和 endpoint 查询均为 404，旧入口连接失败，workspace 仍存在。保留卷持续占用磁盘，内部显式单轮过期回收同样默认留卷，尚无周期执行或后台卷回收。文件内容保留由下述显式集成测试使用临时受限容器只读挂载验证，并核对精确资源 ID 与完整标签。

确认 B 仍可访问并执行命令：

```bash
curl -i "$CONTROL_URL/v1/sandboxes/$SANDBOX_B"
curl -fsS "$ENDPOINT_B/healthz"
curl -fsS "$ENDPOINT_B/v1/files?path=delete-demo.txt"
curl -fsS -H 'Content-Type: application/json' \
  -d '{"argv":["printf","B-still-running"]}' "$ENDPOINT_B/v1/commands:run"
```

预期 B 状态为 ready，文件仍为 `independent-data-B`，命令 `exit_code=0` 且 stdout 为 `B-still-running`。确定 A 的数据不再需要后，显式删除其保留卷；这一步不可恢复。最后显式删除本次 B 的全部资源：

```bash
curl -i -X DELETE "$CONTROL_URL/v1/sandboxes/$SANDBOX_A?delete_workspace=true"
curl -i -X DELETE "$CONTROL_URL/v1/sandboxes/$SANDBOX_A?delete_workspace=true"
docker volume inspect "agent-sandbox-$SANDBOX_A-workspace"
curl -i -X DELETE "$CONTROL_URL/v1/sandboxes/$SANDBOX_B?delete_workspace=true"
```

三个 DELETE 均预期 204 空响应体，A 卷 inspect 预期报告不存在。格式合法但没有目标受管资源的 ID 也返回 204；格式无效的 ID 在访问 Docker 前返回 404。归属歧义或冲突为 409 `sandbox_delete_conflict`，Docker 不可用为 503 `docker_unavailable`，清理不完整为含 `sandbox_id` 的 500 `sandbox_delete_failed`。部分清理后对同一 ID 重试，已删除的资源不会重建；网络仍有其他容器连接、卷仍被使用时会报告失败，不强制断开或删除。

在 `control_plane/` 显式执行完整自动验收：

```bash
RUN_DOCKER_INTEGRATION=1 uv run --locked pytest -m docker_integration -v
```

其中删除测试同时核对 A 的四项运行资源消失、默认保留卷中的二进制文件、显式删卷、B 不受影响，以及最终本次精确记录资源全部不存在。禁止用名称前缀批量删除或 `docker prune` 替代清理，固定 Compose、共享资源和归属不匹配的资源不属于目标。实现细节见 [删除设计](docker/DELETION.md)。
