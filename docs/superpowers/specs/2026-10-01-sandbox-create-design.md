# 最小 Sandbox 创建接口设计

## 1. 目标与范围

在 WSL 宿主侧的 Python/FastAPI Control 中增加：

```http
POST /v1/sandboxes
```

该接口使用固定镜像和固定安全基线创建一个由本项目管理的 Docker sandbox，并返回不可预测的 sandbox ID 和容器启动状态。

本阶段只创建 sandbox 本体，不实现：

- sandbox 查询、列表或公开删除接口；
- Agent 就绪检查或动态入口代理；
- 请求方自定义镜像、命令、挂载、环境变量或 Docker 参数；
- TTL、资源池、重试、egress sidecar 或持久化状态数据库。

创建成功只说明 Docker 容器的 start 操作成功，不说明容器内 Agent 已经就绪或可从宿主机访问。

## 2. HTTP 契约

### 2.1 请求

请求不接受配置和请求体：

```http
POST /v1/sandboxes
Content-Length: 0
```

任意非空请求体，包括空 JSON 对象、空白字符或未知字段，都返回 HTTP 400。Handler 检查请求体时最多读取到第一个非空字节即停止，不能为了拒绝请求而无界缓存正文。

### 2.2 成功响应

成功返回 HTTP 201：

```json
{
  "sandbox_id": "sbx_0123456789abcdef0123456789abcdef",
  "status": "started"
}
```

`started` 的唯一含义是容器已由 Docker 成功启动。它不等同于 Agent ready，也不承诺当前存在宿主机访问路径。

### 2.3 稳定错误响应

错误采用现有 Control 风格的稳定 JSON，不包含 Docker socket、宿主路径、环境变量、资源内部 ID、原始 SDK 异常或调用栈。

| HTTP | code | message | 含义 |
|---|---|---|---|
| 400 | `invalid_request` | `request body must be empty` | 请求体非空或请求不符合当前空请求契约 |
| 503 | `sandbox_image_unavailable` | `sandbox image is unavailable` | 固定镜像 `agent-sandbox:dev` 不存在 |
| 503 | `docker_unavailable` | `Docker runtime is unavailable` | Docker client 无法创建、daemon 不可用或 SDK 请求超时 |
| 409 | `sandbox_resource_conflict` | `sandbox resource conflict` | 本次生成的资源名发生冲突；不接管冲突资源 |
| 500 | `sandbox_create_failed` | `sandbox creation failed` | 创建或启动失败，且本次已创建资源均已成功回滚 |
| 500 | `sandbox_cleanup_failed` | `sandbox creation failed and cleanup was incomplete` | 创建失败，并且至少一个本次资源未能安全回滚 |

`sandbox_cleanup_failed` 的 error 对象额外包含本次 `sandbox_id`，用于管理员按标签定位资源。其他错误不返回底层诊断细节。

## 3. ID、名称与归属

Sandbox ID 使用：

```python
"sbx_" + secrets.token_hex(16)
```

即 `sbx_` 加 32 位小写十六进制字符，提供 128 位随机性。

资源命名规则：

- 容器：`agent-sandbox-<sandbox_id>`
- 网络：`agent-sandbox-<sandbox_id>-internal`
- Workspace volume：`agent-sandbox-<sandbox_id>-workspace`

每个资源都设置以下标签：

```text
io.agent-sandbox.managed=true
io.agent-sandbox.project=agent-sandbox
io.agent-sandbox.sandbox-id=<sandbox_id>
io.agent-sandbox.transaction-id=<transaction_id>
io.agent-sandbox.resource=container|network|workspace
```

每次创建尝试生成独立、不可预测的事务 ID。名称只用于可读性和冲突检测；事务 ID 用于拒绝 Docker 幂等 volume API 返回的既有资源。资源归属必须同时依据本次事务记录的 Docker 资源 ID 和完整标签，不能仅依据名称或名称前缀。

## 4. 组件边界

### 4.1 HTTP 应用

`control.app` 负责：

- 注册 `/v1/sandboxes`；
- 验证空请求体；
- 调用注入的 `SandboxCreator`；
- 将领域错误映射为稳定 HTTP 响应。

HTTP 层不导入 Docker SDK，不构造 Docker 参数，也不执行回滚。

最终应用工厂接口为：

```python
def create_app(
    probe: RuntimeProbe | None = None,
    sandbox_creator: SandboxCreator | None = None,
    readiness_timeout: float = 1.0,
) -> FastAPI
```

现有 `/healthz` 和 `/readyz` 契约保持不变。

### 4.2 Sandbox service

新增 `control.sandbox_service`，负责：

- 定义 `SandboxCreator` 与 `SandboxRuntime` 协议；
- 生成 sandbox ID；
- 根据 ID 构造不可变的资源名称和标签；
- 调用 runtime 创建资源；
- 返回 `SandboxCreateResult(sandbox_id, status="started")`；
- 定义 HTTP 层可映射的稳定领域错误。

建议接口：

```python
@dataclass(frozen=True)
class SandboxSpec:
    sandbox_id: str
    container_name: str
    network_name: str
    volume_name: str
    transaction_id: str

    def labels_for(self, resource: str) -> dict[str, str]: ...

@dataclass(frozen=True)
class SandboxCreateResult:
    sandbox_id: str
    status: Literal["started"]

class SandboxRuntime(Protocol):
    async def create(self, spec: SandboxSpec) -> None: ...

class SandboxCreator(Protocol):
    async def create(self) -> SandboxCreateResult: ...

class SandboxService:
    def __init__(
        self,
        runtime: SandboxRuntime,
        id_factory: Callable[[], str] | None = None,
        transaction_id_factory: Callable[[], str] | None = None,
    ) -> None: ...

    async def create(self) -> SandboxCreateResult: ...
```

两个 factory 只用于确定性单元测试和显式集成测试；默认 sandbox ID 和事务 ID 都必须基于独立的 `secrets.token_hex(16)`。

### 4.3 Docker sandbox runtime

新增 `control.sandbox_runtime`，负责所有 Docker SDK 资源操作和精确回滚。`DockerSandboxRuntime` 实现 `SandboxRuntime`，通过一次工作线程调用完成整个同步事务。

默认 Docker client 通过 `docker.from_env(timeout=5.0)` 延迟创建。client 在事务结束时关闭，无论创建成功、创建失败或回滚失败。SDK 不得在模块导入或 FastAPI 启动阶段连接 Docker。

## 5. 固定容器配置

镜像固定为：

```text
agent-sandbox:dev
```

Control 只检查本地镜像，不自动 pull。

容器固定采用现有本地安全基线：

- `init=true`
- `user=sandbox`
- `working_dir=/workspace`
- CPU 配额为 1 核，即 `nano_cpus=1_000_000_000`
- 内存上限 `256m`
- 内存加 swap 总量 `256m`
- `pids_limit=64`
- `cap_drop=["ALL"]`
- `security_opt=["no-new-privileges:true"]`
- `read_only=true`
- `/tmp` 使用 `rw,nosuid,nodev,size=32m,mode=1777` tmpfs
- 专属 volume 以读写方式挂载到 `/workspace`
- 只连接专属 internal bridge 网络
- 不传入 command、entrypoint、环境变量、额外挂载或 host config
- 不发布任何宿主端口
- 不挂载 Docker socket

镜像中的 `EXPOSE 8080` 只描述容器端口，不构成端口发布；验收以容器 `HostConfig.PortBindings` 为空为准。

## 6. 创建事务

同步事务严格按以下顺序执行：

1. 创建 Docker client。
2. 检查固定镜像 `agent-sandbox:dev` 存在。
3. 创建 `driver=bridge`、`internal=true` 的专属网络。
4. 创建专属 workspace volume。
5. 使用固定配置创建容器，但暂不启动。
6. 启动容器。
7. 返回成功。
8. 在 `finally` 中关闭 Docker client。

每次低层资源创建调用成功返回后，在任何后续模型 inspect 前，立即把资源类型、Docker 资源 ID 和预期标签记录到仅属于本次事务的栈中。volume 创建响应必须先核对本次事务标签，避免采用 Docker 静默返回的同名既有 volume。事务不通过名称重新发现资源，也不自动重试或改用新的 ID。

多个请求可以并发创建 sandbox；随机 ID 和 Docker 名称冲突由稳定 409 响应处理。本阶段不增加创建并发限制或队列。

## 7. 取消与线程行为

整个同步事务通过一次 `asyncio.to_thread()` 执行，避免阻塞 FastAPI 事件循环。每个 Docker SDK 请求使用 5 秒 client timeout，不增加独立的 HTTP 创建超时。

异步调用被取消时：

1. 设置线程安全的取消事件；
2. 工作线程在当前 SDK 调用返回后检查该事件；
3. 如果已取消，不再执行下一创建步骤，而是回滚已创建资源；
4. 异步方法等待工作线程完成；如果线程已经在取消到达前完成全部创建，则使用线程返回的本次资源记录再执行一次精确回滚；
5. 回滚成功时重新抛出 `CancelledError`，回滚不完整时抛出 `SandboxCleanupFailed`，避免把残留资源静默隐藏为普通客户端取消。

同步事务在成功时向异步包装层返回仅供内部使用的资源 ID 与标签记录，公共 `SandboxRuntime.create()` 仍返回 `None`。正常成功时该记录被丢弃；取消发生在线程提交之后时，包装层使用新的短超时 Docker client，只按记录的资源 ID 重新获取并核对资源后回滚，最后关闭该 client。这既覆盖“取消发生在中间步骤”的情况，也覆盖“线程刚完成创建但 HTTP 尚未返回”的竞态，避免已取消请求留下无人认领的 sandbox。Docker SDK 调用本身不能被 Python 强制中断，其最长阻塞时间由 client timeout 限制。

## 8. 精确回滚

任一步骤失败时，只逆序处理本次事务栈中已经记录的资源：

1. 容器：核对归属后执行强制删除，确保启动失败或已启动容器可以移除。
2. Volume：核对归属后删除。
3. 网络：核对归属后删除。

每次删除前必须重新加载或按记录的资源 ID 获取对象，并核对：

- 实际资源 ID 等于事务记录的 ID；
- `managed`、`project`、`sandbox-id`、`transaction-id` 和 `resource` 五个标签全部精确匹配。

如果重新加载失败、ID 不一致、标签缺失或标签不匹配，该资源不得删除，并记为回滚失败。同名但不是本次调用创建的资源永远不会进入事务栈，也不会被清理。

创建异常应先按第 9 节分类。如果所有已记录资源均成功删除，继续抛出已经分类的原始领域错误：冲突仍为 `SandboxResourceConflict`，Docker 连接或传输失败仍为 `DockerUnavailable`，其他创建或启动错误为 `SandboxCreateFailed`。如果至少一个资源无法安全删除，则由包含 sandbox ID 的 `SandboxCleanupFailed` 取代原错误，同时把原始创建异常保留为内部异常链，但不返回给 HTTP 客户端。

## 9. Docker 错误分类

- `docker.errors.ImageNotFound`：`SandboxImageUnavailable`
- Docker API HTTP 409：`SandboxResourceConflict`
- client factory、连接或传输超时失败：`DockerUnavailable`
- 其他网络、volume、容器 create 或 start 的 Docker API 错误：触发回滚后映射为 `SandboxCreateFailed`
- 回滚删除失败或归属核对失败：`SandboxCleanupFailed`

镜像检查发生在资源创建之前，因此镜像缺失和初始 Docker 不可用不需要回滚。

## 10. 测试策略

### 10.1 HTTP 单元测试

使用 fake `SandboxCreator` 覆盖：

- 空请求体返回 201、sandbox ID 和 `started`；
- 非空请求体返回 400 且不调用 creator；
- 每种领域错误的 HTTP 状态码和稳定 code；
- 清理失败响应包含 sandbox ID；
- 错误响应不包含底层异常或宿主信息；
- `/healthz` 和 `/readyz` 不回归。

### 10.2 Service 与 runtime 单元测试

使用确定性 ID factory 和 fake runtime 验证 ID、名称、标签及结果。使用 fake Docker client 验证：

- 镜像、网络、volume、容器、启动的严格顺序；
- 所有固定安全配置；
- 成功与失败路径关闭 client；
- 网络、volume、容器创建和启动各阶段失败时的逆序回滚；
- 冲突资源、标签不匹配和 ID 不匹配不会被删除；
- 请求取消会停止后续步骤并等待回滚；
- 回滚失败产生 `SandboxCleanupFailed`。

### 10.3 显式 Docker 集成测试

真实 Docker 测试使用 `docker_integration` marker，并在缺少：

```text
RUN_DOCKER_INTEGRATION=1
```

时跳过。默认 `uv run pytest` 不创建任何 Docker 资源。

显式测试使用唯一 ID，创建后 inspect 并断言：

- 三类资源均有完整标签；
- 网络 `Internal=true` 且 driver 为 bridge；
- 容器只连接专属网络；
- `HostConfig.PortBindings` 为空；
- 容器安全配置与固定基线一致；
- workspace volume 挂载正确。

测试在 `finally` 中只按已知资源 ID 获取对象，核对完整标签后逆序删除。测试前后还需核对原 Compose 容器、`agent-sandbox-internal` 网络和 `agent-sandbox-workspace` volume 的 ID 与状态未改变。

## 11. 文档更新

- `control/AGENTS.md`：将“仅允许 Docker ping”的旧边界更新为“readiness 只允许 ping，sandbox 创建只能经固定配置事务执行”，并记录集成测试必须显式启用和精确清理。
- README：增加创建请求、201 响应和“当前无法访问新 sandbox”的说明。
- `docs/CONTROL_PLANE.md`：增加创建组件、固定配置、资源事务、错误和 curl 示例。
- `docs/PROJECT_PROGRESS.md`：记录最小创建能力；查询、Agent 就绪、动态入口和公开删除继续保留在待开发列表。

文档不得把 `started` 描述为 ready，不得声称当前已经形成完整生命周期闭环。

## 12. 验收边界

完成后应满足：

- Control 可以用固定配置创建带完整归属标签的独立 sandbox；
- 成功响应只返回不可预测 ID 和 `started`；
- 创建失败在 Docker 允许删除的情况下不遗留本次资源；
- 无法回滚时返回明确错误和 sandbox ID，而不是触碰其他资源；
- 新容器无宿主端口映射，只连接专属 internal 网络；
- 原 Compose 项目及其网络和 volume 不被修改；
- 现有健康接口不回归；
- 没有新增查询、动态入口或公开删除能力。
