# 受控出口代理

`egress_proxy/` 是独立的 Go 1.25 HTTP/1.1 显式代理，提供普通 HTTP 转发和 CONNECT TCP 隧道。两条路径都经过精确域名白名单、IPv4 地址校验和固定数字 IP 拨号。

当前仅用于可信本机开发，没有认证，默认监听 `127.0.0.1:18084`。尚未接入控制面、Docker 或 sandbox 网络，现有实例的联网行为保持不变。

## 启动

在 `egress_proxy/` 执行：

```bash
go run . -listen 127.0.0.1:18084 -allow-domain example.com
```

`-allow-domain` 可以重复指定；不指定时拒绝所有代理目标，本地健康检查仍可使用。白名单最多 32 条输入，仅接受合法 ASCII DNS 域名，大小写统一并允许单个末尾根点，规范化重名合并。非法配置在监听前失败，不接受通配符、URL 或 IP，不自动放行子域名。

收到 SIGINT 或 SIGTERM 后停止接入，取消普通转发、关闭隧道并等待收尾。HTTP 服务与代理任务共用 5 秒关闭预算；超时返回错误，不宣称尚未结束的操作已经清理完毕。

## 协议

### 本地健康检查

origin-form 的 `GET /healthz` 返回 HTTP 200：

```json
{"status":"ok"}
```

它仅说明服务能够响应，不检查 DNS 或公网。绝对 URL 请求 `http://example.com/healthz` 按普通代理请求处理。

### HTTP 转发

只接受 absolute-form 的 `http://域名/path?query`，端口仅允许 80，省略时补齐。支持 GET、HEAD、POST、PUT、PATCH、DELETE、OPTIONS。拒绝普通相对路径、absolute-form HTTPS、其他 scheme、IP、userinfo、fragment、非法 authority 和协议升级。

上游 URL 与 Host 根据校验后的 URL authority 重建，原始 Host 不能更换目标。请求和响应都移除固定逐跳头、Connection 动态指定头及代理凭据；标准库负责 HTTP framing。独立 Transport 不使用环境代理、不自动解压、不复用上游连接，也不自动跟随重定向。入站普通响应同样关闭连接，避免提前返回的上游响应留下未读取上传数据影响后续请求。

上游状态、端到端响应头和响应体原样转发，3xx 不由代理继续请求 Location。HEAD、204、304 遵守无响应体语义。流式传输按实际字节计数，不完整缓存 body。已知请求体超限提前返回 413，已知上游响应体超限在提交响应前返回 502；响应提交后发生超限、截断或复制错误时直接中止连接，不补写 JSON 或伪装为完整分块响应。

### CONNECT

只接受 authority-form 的 `域名:443`，端口必须显式填写。不接受 URL、IP、userinfo、路径、query 或请求体 framing，包括 `Content-Length: 0`。

先校验目标、取得活动额度并通过受控 Dialer 建连，再返回 `200 Connection Established`；成功响应没有 Content-Length 或 Transfer-Encoding。Hijack 已预读的字节继续参与转发，不丢弃与请求头一起发送的数据。

隧道双向转发原始 TCP 字节，正常 EOF 尽可能通过 CloseWrite 传递，反向流仍可继续。即使客户端在收到 200 前发送写半关闭，也不会将正常 EOF 当作整个隧道的取消。异常、超额、期限到达或服务关闭会关闭两端，handler 等待两条转发路径退出后释放额度。接管后重新设置有限 deadline。

CONNECT 只限制 TCP 连接目标，不保证其中一定运行 TLS，也无法检查 HTTPS 路径、内容或站点二次转发。HTTPS 证书仍由客户端验证；代理不解密 TLS、不注入证书。

## 固定资源限制

| 项目 | 默认上限 |
| --- | --- |
| HTTP 与 CONNECT 共用活动额度 | 16，不排队，耗尽返回 429 |
| 入站 TCP 连接 | 32，包含空闲与已接管连接；超出直接关闭，不保证 HTTP 错误 |
| 请求头、上游响应头 | 各 32 KiB；入站预算含 Go 解析器余量 |
| 读取请求头 | 5 秒 |
| 普通 HTTP 转发 | 30 秒 |
| 请求体、响应体 | 各 10 MiB，按实际字节计数 |
| CONNECT 生命周期 | 60 秒 |
| CONNECT 双向数据 | 每方向 32 MiB |
| DNS 与全部拨号尝试 | 共用 5 秒，遵守调用方更早的 deadline |
| 服务关闭等待 | HTTP 与代理任务共用 5 秒 |

未提交响应的超时错误允许额外最多 1 秒写出固定 504 响应；已经提交的响应直接中止。上游头部通过标准库限制，另有至多 36 KiB 的只读前缀观察，用于恢复 Go 移除 `Connection: close` 时一并移除的动态逐跳字段声明；不自行实现报文 framing。

额度在传输、上传读取及已开始的拨号收尾后归还。服务显式跟踪普通任务与隧道，连接登记和关闭互斥，不以 HTTP Server.Shutdown 单独代表隧道已结束。

## 错误

代理自身错误使用固定 JSON，不包含底层异常、请求体、凭据或完整 URL：

```json
{"error":{"code":"egress_policy_denied","message":"egress policy denied"}}
```

| HTTP | code | 含义 |
| --- | --- | --- |
| 400 | invalid_request | 请求、目标或协议不合法 |
| 403 | egress_policy_denied | 域名或解析地址被拒绝 |
| 405 | method_not_allowed | 不支持的方法 |
| 413 | request_too_large | 请求体超限 |
| 429 | proxy_capacity_exceeded | 活动额度耗尽 |
| 502 | upstream_unavailable | DNS、连接、上游协议或已知响应体大小失败 |
| 503 | proxy_stopping | 服务正在关闭 |
| 504 | upstream_timeout | 建连或转发超时 |

上游返回的错误状态不强制转换为 JSON。HTTP 标准库在 handler 前拒绝的畸形请求、头部超限等可能使用标准库错误响应，例如 431。响应已发送后只能中止，不能再返回新的状态；客户端断开时优先清理。

## 目录和安全边界

- `main.go`：参数、策略装配、监听和信号处理；启动方式为 `go run .`。
- `network/`：不可变 Policy、域名与 IPv4 校验、受控 Dialer；不依赖 HTTP 服务。
- `server/`：请求分派、HTTP/CONNECT、头部过滤、有界传输及连接生命周期；所有上游连接都交给 network.Dialer。
- 测试与实现放在同一目录；保持单个 Go module，不引入第三方依赖。

每次连接重新查询规范化的绝对 DNS 名，仅接受 1～16 个 IPv4 结果。全部结果通过保守拒绝前缀校验后才尝试连接，混入私网、回环、链路本地等地址时整体拒绝。IPv4-mapped IPv6 先规范化，其他 IPv6 拒绝。回退仅使用同轮已校验的数字地址，不二次解析域名。

这里只控制代理自身发起的 IPv4 TCP 80/443 连接，不提供 IPv6、UDP、QUIC、DNS 缓存、动态策略、上游代理、TLS MITM 或分布式协调。允许站点仍可能二次转发；通过单元测试不等于完成宿主机保护、sandbox 全协议出口策略或生产安全隔离。后续再接入运行时资源。

## 验证

默认测试使用 fake resolver、受控拨号映射、本机 HTTP/TLS 服务和内存连接，不访问公网、外部 DNS 或 Docker。TLS 用例信任专属测试证书，不使用 InsecureSkipVerify。测试覆盖转发、CONNECT 预读和半关闭、限制、超时、竞争及关闭。

在 `egress_proxy/` 执行：

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go build ./...
```

可选公网 smoke 需在可信本机显式执行，不属于默认测试。启动上面的示例后，在另一个终端执行：

```bash
curl --max-time 5 http://127.0.0.1:18084/healthz
curl --max-time 10 --noproxy '' --proxy http://127.0.0.1:18084 http://example.com/
curl --max-time 10 --noproxy '' --proxy http://127.0.0.1:18084 https://example.com/
curl -i --max-time 5 --noproxy '' --proxy http://127.0.0.1:18084 http://not-allowed.invalid/
```

最后一项应返回策略拒绝，不执行目标 DNS。实际验证记录见[开发进度](PROJECT_PROGRESS.md)；本机模拟验证不代表公网或 sandbox 端到端验收。
