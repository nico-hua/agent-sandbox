# 本地接口 curl 手动测试

以下命令在 WSL 的 Bash 中执行，面向 agent/run-local.sh 启动的容器入口。HTTP 命令接口没有认证，切勿对不可信网络开放。先在一个终端执行：

~~~bash
cd ~/agent-sandbox/agent
./run-local.sh
BASE_URL='http://127.0.0.1:18081'
TEST_ID="$(date +%s)-$$"
INPUT="curl-demo-$TEST_ID.txt"
OUTPUT="curl-demo-$TEST_ID-upper.txt"
BIN="curl-demo-$TEST_ID.bin"
LINK="curl-demo-$TEST_ID-escape"
~~~

保持这些变量在同一终端中，逐段运行后续命令。TEST_ID 用于生成不同文件名；文件 API 不会覆盖同名文件。下文的 sh 是客户端在 argv 中明确指定的程序，Agent 不会隐式执行 Shell。命令接口的 HTTP 200 也不代表命令退出码一定为 0，应同时检查 JSON 的 exit_code。

## 健康检查和命令执行

~~~bash
curl -i "$BASE_URL/healthz"
~~~

预期 HTTP 200，JSON 的 status 为 ok。

不传 cwd 时默认工作目录为 /workspace；显式指定 /tmp 则应改变工作目录：

~~~bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["pwd"]}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["pwd"],"cwd":"/tmp"}' "$BASE_URL/v1/commands:run"
~~~

两次均应返回 HTTP 200、exit_code 0；stdout 分别为 /workspace 和 /tmp，均以换行结尾。无效 cwd 由 CommandRunner 拒绝：

~~~bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["pwd"],"cwd":"/workspace/does-not-exist"}' \
  "$BASE_URL/v1/commands:run"
~~~

预期 HTTP 422、command_start_failed。argv 参数中的空格和 Shell 特殊字符应作为普通文本传入：

~~~bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["printf","%s","$HOME; *.txt"]}' \
  "$BASE_URL/v1/commands:run"
~~~

预期 HTTP 200，stdout 原样为 $HOME; *.txt。下面显式执行 sh，验证 env 覆盖、stdin、多流输出：

~~~bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf \"%s:\" \"$DEMO_NAME\"; cat; printf warning >&2"],"env":{"DEMO_NAME":"sandbox"},"stdin":"hello\n"}' \
  "$BASE_URL/v1/commands:run"
~~~

预期 HTTP 200、exit_code 0、stdout 为 sandbox:hello 加换行，stderr 为 warning。env 只影响该次命令。所有可选字段还可放在同一请求中：

~~~bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf \"%s:\" \"$DEMO_NAME\"; cat"],"cwd":"/tmp","env":{"DEMO_NAME":"combined"},"stdin":"input","timeout_ms":3000,"max_output_bytes_per_stream":1024}' \
  "$BASE_URL/v1/commands:run"
~~~

预期 HTTP 200、exit_code 0、stdout 为 combined:input。分别检查自定义超时、输出上限和普通非零退出：

~~~bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["sleep","2"],"timeout_ms":100}' \
  "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["printf","%s","123456"],"max_output_bytes_per_stream":5}' \
  "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["sh","-c","printf out; printf err >&2; exit 7"]}' \
  "$BASE_URL/v1/commands:run"
~~~

预期依次为 HTTP 504 command_timeout、HTTP 413 output_limit_exceeded、HTTP 200 且 exit_code 7（stdout 为 out，stderr 为 err）。未提供限制时，默认超时 5 秒、每流输出上限 64 KiB；显式给 0 会被拒绝，不能借此取消限制：

~~~bash
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["true"],"timeout_ms":0}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' \
  -d '{"argv":["true"],"max_output_bytes_per_stream":0}' \
  "$BASE_URL/v1/commands:run"
~~~

两次均预期 HTTP 400、invalid_request。还可以检查空 argv、无效环境变量名和超过服务端上限的配置；这些请求都不应启动命令：

~~~bash
curl -i -H 'Content-Type: application/json' -d '{"argv":[]}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' -d '{"argv":[""]}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' -d '{"argv":["true"],"env":{"BAD=NAME":"x"}}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' -d '{"argv":["true"],"timeout_ms":30001}' "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' -d '{"argv":["true"],"max_output_bytes_per_stream":1048577}' "$BASE_URL/v1/commands:run"
~~~

预期五次均为 HTTP 400、invalid_request。最大允许的 timeout_ms 为 30000，每流输出上限为 1048576 字节；HTTP 命令请求体本身最多 1 MiB。

## 上传、相对路径处理与下载

文件 API 的 path 是相对 /workspace 的路径。上传请求体是原始字节，成功返回 HTTP 201；同名文件返回 409。

~~~bash
printf 'hello from curl\n' | curl -i --data-binary @- \
  "$BASE_URL/v1/files?path=$INPUT"
curl -i -H 'Content-Type: application/json' \
  -d "{\"argv\":[\"cat\",\"$INPUT\"],\"cwd\":\"/workspace\"}" \
  "$BASE_URL/v1/commands:run"
curl -i -H 'Content-Type: application/json' \
  -d "{\"argv\":[\"sh\",\"-c\",\"tr a-z A-Z < '$INPUT' > '$OUTPUT'\"],\"cwd\":\"/workspace\",\"timeout_ms\":3000,\"max_output_bytes_per_stream\":65536}" \
  "$BASE_URL/v1/commands:run"
curl -i "$BASE_URL/v1/files?path=$OUTPUT"
~~~

两个命令均应返回 HTTP 200、exit_code 0；cat 输出 hello from curl，下载结果是 HELLO FROM CURL（末尾有换行），Content-Type 为 application/octet-stream。这也验证相对文件路径按 cwd 解析。文件 API 只能访问 /workspace，但这一约束不适用于命令 API：命令仍可访问 Agent 用户有权限访问的其他容器路径。

二进制往返用 cmp 比较原始字节，不直接在终端显示：

~~~bash
LOCAL_BIN="$(mktemp)"
DOWNLOADED_BIN="$(mktemp)"
printf '\000A\377' > "$LOCAL_BIN"
curl -i --data-binary @"$LOCAL_BIN" "$BASE_URL/v1/files?path=$BIN"
curl -fsS "$BASE_URL/v1/files?path=$BIN" -o "$DOWNLOADED_BIN"
cmp "$LOCAL_BIN" "$DOWNLOADED_BIN" && printf 'binary round trip OK\n'
~~~

上传应返回 HTTP 201；cmp 返回 0 并打印 binary round trip OK。结束后仅清理这两个本地临时文件：rm -- "$LOCAL_BIN" "$DOWNLOADED_BIN"。它不会清理 workspace 卷。

## 错误与大小限制

~~~bash
printf 'again' | curl -i --data-binary @- "$BASE_URL/v1/files?path=$INPUT"
curl -i "$BASE_URL/v1/files?path=curl-demo-$TEST_ID-missing.txt"
curl -i "$BASE_URL/v1/files?path=..%2Fetc%2Fpasswd"
~~~

预期依次为 HTTP 409 file_exists、HTTP 404 file_not_found、HTTP 400 invalid_path。通过命令创建指向 workspace 外的符号链接，再尝试通过文件 API 下载，应返回 HTTP 403 file_access_denied：

~~~bash
curl -i -H 'Content-Type: application/json' \
  -d "{\"argv\":[\"ln\",\"-s\",\"/etc/passwd\",\"/workspace/$LINK\"]}" \
  "$BASE_URL/v1/commands:run"
curl -i "$BASE_URL/v1/files?path=$LINK"
~~~

上传和下载单文件上限均为 10 MiB（10,485,760 字节）。下面只用零字节测试超限，预期 HTTP 413 file_too_large，且不创建文件：

~~~bash
head -c 10485761 /dev/zero | curl -i --max-time 30 --data-binary @- \
  "$BASE_URL/v1/files?path=curl-demo-$TEST_ID-too-large.bin"
~~~

上传的文件留在持久化的 /workspace 命名卷中。若要验证容器重建后的持久化，记录 TEST_ID，重建后重新设置 BASE_URL、INPUT 和 OUTPUT，再下载原文件；本手册不自动停止容器或删除 volume。清理时只指定自己创建的测试文件。
