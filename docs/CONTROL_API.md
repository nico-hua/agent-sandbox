# Control 控制面 HTTP API

本文档描述当前 WSL 宿主侧 Python/FastAPI Control 对外提供的 HTTP 接口。HTTP 路由与错误映射位于 `control_plane/api/health.py` 和 `control_plane/api/sandboxes.py`，应用装配入口为 `control_plane/app.py`。所有运行时调用统一经过 `SandboxService`；`/healthz` 直接返回进程健康，HTTP 状态码、超时和响应契约保持不变。

## 1. 服务边界

Control 与容器内的 Go Agent 是两个独立进程。Control 负责检查 Docker 可用性，以及创建和查询由本项目管理的 sandbox；它不直接提供命令执行或文件传输接口。

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

`sandbox_id` 使用 `sbx_` 前缀和 32 位小写十六进制随机值。`started` 只表示 Docker 已完成容器启动调用，不表示容器内 Agent 已通过健康检查，也不表示该 sandbox 已经可以从宿主机动态访问。

每次创建会使用固定的 `agent-sandbox:dev` 镜像，并创建专属 internal bridge、workspace volume 和无宿主端口映射的容器。资源带有完整的项目、sandbox ID、事务 ID、受管标记和资源类型标签。

### 创建错误

| HTTP 状态码 | `error.code` | 含义 |
| --- | --- | --- |
| 400 | `invalid_request` | 请求体不为空 |
| 409 | `sandbox_resource_conflict` | 生成的资源与已有资源冲突 |
| 503 | `sandbox_image_unavailable` | 固定 sandbox 镜像不存在 |
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

## 6. HTTP 方法

当前各路径只接受本文档声明的方法。使用其他方法时，FastAPI 返回 HTTP 405 `Method Not Allowed`。

## 7. 当前限制

- 没有认证或授权，只能作为本机开发接口使用。
- 没有 sandbox 删除、续期、暂停、恢复或 TTL API。
- 没有动态入口，新创建的 sandbox 不发布宿主机端口。
- 查询接口只读取 Docker 状态，不会主动启动、停止、修复或清理 sandbox。
- Control 不直接访问容器 IP；容器内部 HEALTHCHECK 请求 Agent `/healthz`，Control 根据 Docker health 状态报告 `ready`。
- 创建响应中的 `started` 与查询响应中的 `ready` 含义不同。
- Control API 不提供 Agent 的命令执行和文件传输能力。
