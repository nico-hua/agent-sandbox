# Control 控制面 HTTP API

本文档描述当前 WSL 宿主侧 Python/FastAPI Control 对外提供的 HTTP 接口。HTTP 路由与错误映射位于 `control_plane/api/health.py` 和 `control_plane/api/sandboxes.py`，应用装配入口为 `control_plane/app.py`。所有运行时调用统一经过 `SandboxService`；`/healthz` 直接返回进程健康，HTTP 状态码、超时和响应契约保持不变。

## 1. 服务边界

Control 与容器内的 Go Agent 是两个独立进程。Control 负责检查 Docker 可用性，以及创建、查询和手动删除由本项目管理的 sandbox；它不直接提供命令执行或文件传输接口。

本地开发默认使用：

```text
http://127.0.0.1:18083
```

Control 当前没有认证，只能监听本机地址，不得暴露到不可信网络。Docker socket 只供宿主侧 Control 使用，不能挂载进 sandbox 或入口代理。

除无响应体的请求外，接口响应使用 JSON。错误响应采用统一结构：

```json
{
  "error": {
    "code": "stable_error_code",
    "message": "stable error message"
  }
}
```

底层 Docker 异常、socket 路径、环境变量和调用栈不会写入错误响应。

## 2. 进程健康检查

### `GET /healthz`

检查 Control HTTP 进程自身是否能够处理请求。该接口不访问 Docker daemon。

请求示例：

```bash
curl -i http://127.0.0.1:18083/healthz
```

成功响应为 HTTP 200：

```json
{
  "status": "ok"
}
```

该接口返回成功不代表 Docker 可用，也不代表任何 sandbox 已就绪。

## 3. Docker 就绪检查

### `GET /readyz`

通过一次只读 Docker `ping()` 检查 Control 当前是否能够访问 Docker daemon。探测具有有限超时，不会创建、启动、停止或删除 Docker 资源。

请求示例：

```bash
curl -i http://127.0.0.1:18083/readyz
```

Docker 可用时返回 HTTP 200：

```json
{
  "status": "ready"
}
```

Docker 不可用、权限不足或探测超时时返回 HTTP 503：

```json
{
  "status": "unavailable",
  "error": {
    "code": "docker_unavailable",
    "message": "Docker runtime is unavailable"
  }
}
```

Docker 暂时不可用不会阻止 Control 进程启动，`GET /healthz` 仍可返回 HTTP 200。

## 4. 创建 sandbox

### `POST /v1/sandboxes`

使用固定的 Docker 安全基线创建一个由本项目管理的 sandbox。请求方不能指定镜像、命令、挂载、网络或其他 Docker 参数。

请求体必须完全为空。`{}`、空白字符或其他任意请求体都会被拒绝。

请求示例：

```bash
curl -i -X POST http://127.0.0.1:18083/v1/sandboxes
```

创建成功时返回 HTTP 201：

```json
{
  "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
  "status": "started"
}
```

`sandbox_id` 使用 `sbx_` 前缀和 32 位小写十六进制随机值。`started` 只表示 Docker 已完成容器启动调用，不表示容器内 Agent 已通过健康检查，也不表示入口 proxy 已通过健康检查。

创建前检查固定的 `agent-sandbox:dev` 与 `agent-sandbox-proxy:dev` 镜像均存在；不自动 pull/build。每次创建专属 internal bridge、ingress bridge、workspace volume、无宿主端口映射的 sandbox 和专属 proxy。sandbox 仅连 internal，以 `sandbox` 为 DNS 别名；proxy 连接两网，仅在 `127.0.0.1` 动态发布 8080，workspace 只挂载给 sandbox。资源带有完整的项目、sandbox ID、事务 ID、受管标记和资源类型标签。

### 创建错误

| HTTP 状态码 | `error.code` | 含义 |
| --- | --- | --- |
| 400 | `invalid_request` | 请求体不为空 |
| 409 | `sandbox_resource_conflict` | 生成的资源与已有资源冲突 |
| 503 | `sandbox_image_unavailable` | Agent 或代理固定镜像不存在 |
| 503 | `docker_unavailable` | Docker daemon 不可用 |
| 500 | `sandbox_create_failed` | 创建失败，但本次已记录资源已完成回滚 |
| 500 | `sandbox_cleanup_failed` | 创建失败且本次资源未能全部回滚 |

普通创建错误示例：

```json
{
  "error": {
    "code": "sandbox_create_failed",
    "message": "sandbox creation failed"
  }
}
```

回滚不完整时，错误对象额外包含 sandbox ID，便于人工定位本项目管理的残留资源：

```json
{
  "error": {
    "code": "sandbox_cleanup_failed",
    "message": "sandbox creation failed and cleanup was incomplete",
    "sandbox_id": "sbx_0123456789abcdef0123456789abcdef"
  }
}
```

## 5. 查询 sandbox 状态

### `GET /v1/sandboxes/{sandbox_id}`

读取指定 sandbox 当前的 Docker 容器状态和 Docker health 状态。查询以 Docker 中的项目归属标签为数据来源，不依赖 Control 进程内字典，因此 Control 重启后仍可查询此前创建且仍存在的受管 sandbox。

路径参数必须符合以下格式：

```text
sbx_<32 位小写十六进制字符>
```

请求示例：

```bash
SANDBOX_ID=sbx_0123456789abcdef0123456789abcdef
curl -i "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}"
```

查询成功时返回 HTTP 200：

```json
{
  "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
  "status": "ready",
  "reason": "health_check_passed",
  "message": "Agent health check passed"
}
```

`status` 是面向调用方的概要状态，`reason` 和 `message` 说明产生该状态的具体事实。

### 状态与原因

| Docker 事实 | `status` | `reason` | `message` |
| --- | --- | --- | --- |
| 容器已创建但尚未运行 | `starting` | `container_created` | `container has been created but is not running` |
| 容器正在重启 | `starting` | `container_restarting` | `container is restarting` |
| 容器运行中，未配置 health check | `running` | `health_check_not_configured` | `container is running but no health check is configured` |
| 容器运行中，health check 正在启动 | `starting` | `health_check_starting` | `container is running and its health check is starting` |
| health check 为 `healthy` | `ready` | `health_check_passed` | `Agent health check passed` |
| health check 为 `unhealthy` | `failed` | `health_check_failed` | `Agent health check is failing` |
| 容器已暂停 | `stopped` | `container_paused` | `container is paused` |
| 容器以退出码 0 结束 | `stopped` | `container_exited` | `container exited successfully` |
| 容器以非零退出码结束 | `failed` | `container_exited_with_error` | `container exited with a non-zero status` |
| 容器因超过内存限制被终止 | `failed` | `container_oom_killed` | `container was terminated after exceeding its memory limit` |
| Docker 将容器标记为 dead | `failed` | `container_dead` | `Docker reports that the container is dead` |

只有 Docker health 明确为 `healthy` 时才返回 `ready`。新构建的 `agent-sandbox:dev` 镜像通过容器内部的 `http://127.0.0.1:8080/healthz` 执行健康检查，health starting 时返回 `starting`，unhealthy 时返回 `failed`。控制面只读取 Docker 状态，创建接口仍返回 `started` 而不等待检查通过。

`ready` 表示容器内部 Agent 健康，不代表宿主机动态入口或外部代理可用。镜像重建不会更新旧容器；没有健康检查的旧容器运行时仍返回 `running / health_check_not_configured`。健康检查失败不会触发容器删除、工作区清理或自动重启。

### 查询错误

| HTTP 状态码 | `error.code` | 含义 |
| --- | --- | --- |
| 404 | `sandbox_not_found` | ID 格式无效、受管容器不存在或归属标签不匹配 |
| 503 | `docker_unavailable` | Docker daemon 不可用 |
| 504 | `sandbox_query_timeout` | 查询未在有限时间内完成 |
| 500 | `sandbox_query_failed` | Docker 返回重复、未知或无法可靠解析的受管状态 |

未知 sandbox 的响应不会说明同名非本项目容器是否存在：

```json
{
  "error": {
    "code": "sandbox_not_found",
    "message": "sandbox not found"
  }
}
```

## 6. 查询本机入口

### `GET /v1/sandboxes/{sandbox_id}/endpoint`

通过 `API → SandboxService → SandboxRuntime` 只读查询专属 proxy 的实际地址，不依赖进程内创建记录，控制面重启后仍可恢复。ID 格式与状态查询相同。

```bash
curl -i "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}/endpoint"
```

HTTP 200：

```json
{"sandbox_id":"sbx_0123456789abcdef0123456789abcdef","url":"http://127.0.0.1:49153"}
```

地址从代理实际 `NetworkSettings.Ports["8080/tcp"]` 读取，不根据 `HostConfig.PortBindings` 推断已发布；仅接受唯一 `127.0.0.1`、1–65535 端口。查询核验 sandbox/proxy 完整归属、相同事务 ID、专属网络标签与实际连接、sandbox DNS 别名和 proxy running/healthy。`ready` 继续表示内部 Agent 健康，入口可用性由本接口单独判断。

| HTTP 状态码 | `error.code` | `error.message` |
| --- | --- | --- |
| 404 | `sandbox_not_found` | `sandbox not found` |
| 503 | `sandbox_endpoint_unavailable` | `sandbox endpoint is unavailable` |
| 504 | `sandbox_endpoint_timeout` | `sandbox endpoint query timed out` |

入口缺失、代理未就绪、归属或事务不匹配、网络错误、端口未实际发布、多个候选 proxy 或 Docker 不可用均返回稳定 503；HTTP 整体或 Docker transport 超时返回稳定 504。没有 proxy 的旧实例返回 503，不会修改既有实例。错误不暴露 Docker 细节。

此 URL 仅供本机 WSL 开发，不能作为远程访问承诺。调用者通过该 URL 访问 Agent `/healthz`、命令、文件和 SSE；控制面不转发这些调用。

## 7. 删除 sandbox

### `DELETE /v1/sandboxes/{sandbox_id}`

通过 `API → SandboxService → SandboxRuntime` 回收受管资源。ID 格式与查询相同，无效 ID 在访问 Docker 前返回 404。删除不要求 sandbox 容器仍存在、健康或 endpoint 可用；控制面重启后从 Docker 标签恢复清单，也可删除仅剩代理、网络或 workspace 的实例。

| 查询参数 | 类型 | 默认值 | 语义 |
| --- | --- | --- | --- |
| `delete_workspace` | boolean | `false` | `true` 时最后一并删除该实例专属 workspace 卷，数据不可恢复 |

默认删除 proxy、sandbox、ingress 网络和 internal 网络，保留 workspace 和文件：

```bash
# 只使用本次创建并记录的 SANDBOX_ID。
curl -i -X DELETE "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}"
```

明确需要删除数据时，显式传参；此操作也适用于运行资源已经删除、仅剩保留卷的情况：

```bash
# 不可恢复地删除该实例 workspace 中的全部数据。
curl -i -X DELETE "http://127.0.0.1:18083/v1/sandboxes/${SANDBOX_ID}?delete_workspace=true"
```

达到请求的目标状态后返回 **HTTP 204，响应体为空**。格式合法但无目标受管资源、重复删除，以及默认删除时仅剩保留卷，也返回 204。成功删除运行资源后，实例状态与 endpoint 查询返回 404，旧入口不再可用；保留卷本身不构成可查询实例。

删除前核验所有候选的完整归属标签、有效且跨角色一致的事务 ID，以及角色唯一性；归属有歧义时先拒绝，不任选一个候选。操作只使用清单中的精确容器/网络 ID 或经核验的卷 Name，每次 stop/remove 前重新读取归属。固定 Compose 实例、共享资源及标签不匹配的资源不属于目标。

容器使用有限停止等待，必要时重新核验后强制终止本实例容器。网络被其他容器连接或卷仍被使用时会报告失败，不强制断开其他容器或强制删除卷。已经消失的资源视为完成；部分失败不会重建已删除项，可对同一 ID 重试继续回收。

### 删除错误

| HTTP 状态码 | `error.code` | `error.message` | 含义 |
| --- | --- | --- | --- |
| 404 | `sandbox_not_found` | `sandbox not found` | ID 格式无效 |
| 409 | `sandbox_delete_conflict` | `sandbox resource ownership conflict` | 候选重复、事务冲突、归属或精确标识变化 |
| 503 | `docker_unavailable` | `Docker runtime is unavailable` | Docker 后端无法完成资源发现 |
| 500 | `sandbox_delete_failed` | `sandbox deletion was incomplete` | 回收未达到目标状态，含 `sandbox_id` |
| 422 | FastAPI 参数校验响应 | FastAPI 参数校验响应 | `delete_workspace` 不是可接受的 boolean |

部分清理失败的稳定响应示例：

```json
{
  "error": {
    "code": "sandbox_delete_failed",
    "message": "sandbox deletion was incomplete",
    "sandbox_id": "sbx_0123456789abcdef0123456789abcdef"
  }
}
```

DELETE 不套用状态/入口查询的 1 秒整体超时；SDK 调用仍有有限 transport timeout。请求取消不等于同步 Docker 操作停止：工作线程继续回收并关闭 client，首次和重复取消均等待收尾后才传播取消。调用者未收到结果时应对同一 ID 重试，以当前资源事实确认结果。

默认保留卷会持续占用宿主磁盘。没有 TTL 或后台卷回收任务；需要调用者记录 sandbox ID 并显式决定数据回收时间。该保留策略不改变创建失败时的事务回滚。

## 8. HTTP 方法

当前各路径只接受本文档声明的方法。使用其他方法时，FastAPI 返回 HTTP 405 `Method Not Allowed`。

## 9. 当前限制

- 没有认证或授权，只能作为本机开发接口使用。
- 没有续期、暂停、恢复、TTL API 或后台资源/卷回收任务。
- 入口只支持本机固定 Agent HTTP，不暴露任意用户服务端口。
- 查询接口只读取 Docker 状态，不会主动启动、停止、修复或清理 sandbox。
- Control 不直接访问容器 IP；容器内部 HEALTHCHECK 请求 Agent `/healthz`，Control 根据 Docker health 状态报告 `ready`。
- 创建响应中的 `started` 与查询响应中的 `ready` 含义不同。
- Control API 不提供 Agent 的命令执行和文件传输能力。
