# 最小 Sandbox 创建接口实施计划

> **给智能体执行者：** 必须使用 `superpowers:subagent-driven-development`（推荐）或 `superpowers:executing-plans` 子技能，逐项执行本计划。步骤使用复选框（`- [ ]`）跟踪进度。

**目标：** 在现有 FastAPI Control 中实现固定安全配置的 `POST /v1/sandboxes`，并保证失败时只精确回滚本次创建的 Docker 资源。

**架构：** HTTP 层只验证空请求并映射领域错误；`SandboxService` 生成不可预测 ID 和资源元数据；`DockerSandboxRuntime` 在单个工作线程中执行镜像检查、资源创建、启动和逆序回滚。默认测试使用 fake，不接触 Docker；真实 Docker 验收必须显式启用。

**技术栈：** Python 3.12、FastAPI、asyncio、Python Docker SDK、pytest、httpx、uv。

**设计文档：** `docs/superpowers/specs/2026-10-01-sandbox-create-design.md`

## 全局约束

- 成功请求必须为空（典型请求为 `Content-Length: 0`），响应固定为 HTTP 201、`sandbox_id` 和 `status="started"`；`started` 不代表 Agent ready。
- 固定镜像为 `agent-sandbox:dev`，不得 pull，也不得接收请求方镜像或 Docker 配置。
- ID 为 `sbx_` 加 `secrets.token_hex(16)`；名称严格遵循设计文档，所有资源设置 `io.agent-sandbox.managed=true`、`io.agent-sandbox.project=agent-sandbox`、`io.agent-sandbox.sandbox-id=<sandbox_id>` 和对应的 `io.agent-sandbox.resource` 标签。
- 每个 sandbox 使用 `driver=bridge`、`internal=true` 的独立网络、独立 workspace volume 和独立容器；`HostConfig.PortBindings` 必须为空，不发布宿主端口。
- 容器固定使用现有 1 CPU、256 MiB、无额外 swap、64 PID、非 root、只读根文件系统、32 MiB `/tmp`、drop ALL capabilities 和 no-new-privileges 基线。
- Docker 同步事务只运行在工作线程；默认 client 使用 `docker.from_env(timeout=5.0)`，成功和失败路径均关闭。
- 回滚只能使用本次事务记录的资源 ID，并在删除前核对完整标签；不得按名称或前缀扫描、接管或删除资源。
- HTTP 错误 code 固定为 `invalid_request`、`sandbox_image_unavailable`、`docker_unavailable`、`sandbox_resource_conflict`、`sandbox_create_failed` 和 `sandbox_cleanup_failed`，状态与 message 使用设计文档表格中的精确值。
- 默认测试不得创建、停止或删除 Docker 资源；真实集成测试必须同时显式设置环境变量和 marker。
- 不修改 `agent/`、现有 Compose 拓扑、`agent-sandbox-internal` 或 `agent-sandbox-workspace`。
- 每个具名 Python 函数和测试函数都必须有简洁 docstring。

## 审查重点

- 客户端发送 chunked 或大体积非空正文时，handler 应在发现首个非空字节后返回 400，且 creator 不被调用；任务 4 覆盖。
- HTTP 请求取消发生在 Docker 调用执行期间时，线程不得继续创建后续资源，且异步方法应等待精确回滚；任务 3 覆盖。
- Docker 创建返回对象后发生失败时，只有实际进入本次事务栈且 ID、标签都匹配的资源能被删除；任务 3 覆盖。
- 容器镜像声明 `EXPOSE 8080` 时仍不得产生 host port binding，且容器只能连接专属 internal 网络；任务 2 和任务 5 覆盖。
- 回滚本身部分失败时，原始创建错误不能掩盖清理不完整状态，HTTP 必须返回 `sandbox_cleanup_failed` 和 sandbox ID；任务 3 和任务 4 覆盖。

---

### Task 1（任务 1）：Sandbox 领域模型、ID 和资源元数据

**文件：**
- 修改：`control/AGENTS.md`
- 新建：`control/control/sandbox_service.py`
- 新建：`control/tests/test_sandbox_service.py`

**接口：**
- 产出：`SandboxSpec`、`SandboxCreateResult`、`SandboxRuntime`、`SandboxCreator`、`SandboxService`。
- 产出：`SandboxService(runtime: SandboxRuntime, id_factory: Callable[[], str] | None = None)`。
- 产出：`async SandboxService.create() -> SandboxCreateResult`。
- 产出：`labels_for(resource: Literal["container", "network", "workspace"]) -> dict[str, str]`。
- 产出：后续 runtime 和 HTTP 层使用的领域错误类型。

- [ ] **步骤 1：更新 Control 模块边界说明**

修改 `control/AGENTS.md`：readiness 探测仍只能执行 `ping()`；sandbox 创建只允许通过固定配置、完整标签和事务回滚路径执行；默认测试不得修改 Docker，显式集成测试只能清理已经核对 ID 和标签的本次资源。

- [ ] **步骤 2：编写领域模型和 service 的失败测试**

在 `test_sandbox_service.py` 增加：

- `test_create_builds_names_and_labels_from_deterministic_id`
- `test_create_returns_started_only_after_runtime_success`
- `test_default_id_has_prefix_and_128_bits_of_lowercase_hex`
- `test_runtime_error_is_preserved_for_http_mapping`

fake runtime 记录收到的 `SandboxSpec`。确定性 ID factory 返回 `sbx_0123456789abcdef0123456789abcdef`，断言三个资源名和四个标签精确匹配设计文档。

- [ ] **步骤 3：运行聚焦测试并确认 RED**

运行：

```bash
cd control
uv run pytest tests/test_sandbox_service.py -v
```

预期：导入失败，因为 `control.sandbox_service` 尚不存在。

- [ ] **步骤 4：实现最小领域模型和 service**

实现精确接口：

```python
@dataclass(frozen=True)
class SandboxSpec: ...

@dataclass(frozen=True)
class SandboxCreateResult:
    sandbox_id: str
    status: Literal["started"]

class SandboxRuntime(Protocol):
    async def create(self, spec: SandboxSpec) -> None: ...

class SandboxCreator(Protocol):
    async def create(self) -> SandboxCreateResult: ...

class SandboxService:
    def __init__(self, runtime: SandboxRuntime, id_factory: Callable[[], str] | None = None) -> None: ...
    async def create(self) -> SandboxCreateResult: ...
```

同时定义 `SandboxImageUnavailable`、`DockerUnavailable`、`SandboxResourceConflict`、`SandboxCreateFailed` 和带 `sandbox_id` 的 `SandboxCleanupFailed`。默认 ID factory 使用 `secrets.token_hex(16)`。

- [ ] **步骤 5：运行聚焦测试和当前完整测试并确认 GREEN**

运行：

```bash
uv run pytest tests/test_sandbox_service.py -v
uv run pytest
```

预期：新测试及现有测试全部通过。

- [ ] **步骤 6：提交任务 1**

```bash
git add control/AGENTS.md control/control/sandbox_service.py control/tests/test_sandbox_service.py
git commit -m "feat(control): define sandbox creation service"
```

### Task 2（任务 2）：固定安全配置的 Docker 创建成功路径

**文件：**
- 新建：`control/control/sandbox_runtime.py`
- 新建：`control/tests/test_sandbox_runtime.py`

**接口：**
- 使用：任务 1 的 `SandboxSpec`、`SandboxRuntime` 和领域错误。
- 产出：`DockerSandboxRuntime(client_factory: Callable[[], DockerClient] | None = None)`。
- 产出：`async DockerSandboxRuntime.create(spec: SandboxSpec) -> None`。
- 产出：默认 factory 调用 `docker.from_env(timeout=5.0)`，但构造 runtime 时不创建 client。

- [ ] **步骤 1：编写成功事务和固定配置的失败测试**

使用只实现本任务需要方法的 fake Docker client，增加：

- `test_runtime_construction_does_not_create_client`
- `test_create_checks_image_then_creates_network_volume_container_and_starts`
- `test_network_is_dedicated_bridge_and_internal`
- `test_container_uses_fixed_image_names_labels_and_security_baseline`
- `test_container_has_no_host_port_binding_or_extra_network`
- `test_success_closes_client_once`

严格断言调用顺序为 `images.get`、`networks.create`、`volumes.create`、`containers.create`、`container.start`。容器 kwargs 必须包含设计文档的固定值，不得包含 command、entrypoint、environment 或 ports。

- [ ] **步骤 2：运行聚焦测试并确认 RED**

运行：

```bash
uv run pytest tests/test_sandbox_runtime.py -v
```

预期：导入失败，因为 `control.sandbox_runtime` 尚不存在。

- [ ] **步骤 3：实现同步成功事务和异步线程适配**

实现：

```python
class DockerSandboxRuntime:
    def __init__(self, client_factory: Callable[[], DockerClient] | None = None) -> None: ...
    async def create(self, spec: SandboxSpec) -> None: ...
    def _create_sync(self, spec: SandboxSpec, cancelled: threading.Event) -> None: ...
```

默认 factory 为 `docker.from_env(timeout=5.0)`。本任务先实现成功路径、client 关闭和固定参数；事务记录结构应能保存 resource 类型、ID、对象及预期标签，为任务 3 回滚使用。

- [ ] **步骤 4：运行聚焦测试和当前完整测试并确认 GREEN**

运行：

```bash
uv run pytest tests/test_sandbox_runtime.py -v
uv run pytest
```

预期：新测试和现有测试全部通过。

- [ ] **步骤 5：提交任务 2**

```bash
git add control/control/sandbox_runtime.py control/tests/test_sandbox_runtime.py
git commit -m "feat(control): create sandbox Docker resources"
```

### Task 3（任务 3）：错误分类、取消和精确回滚

**文件：**
- 修改：`control/control/sandbox_runtime.py`
- 修改：`control/tests/test_sandbox_runtime.py`

**接口：**
- 使用：任务 1 的五类领域错误和任务 2 的事务记录。
- 完成：`DockerSandboxRuntime.create()` 在所有失败、取消和清理路径上的最终语义。

- [ ] **步骤 1：为每个失败阶段编写 RED 测试**

增加：

- `test_missing_image_creates_nothing_and_raises_image_unavailable`
- `test_client_factory_failure_raises_docker_unavailable`
- `test_conflict_does_not_adopt_or_delete_existing_resource`
- `test_volume_conflict_rolls_back_created_network_without_touching_conflicting_volume`
- `test_volume_failure_removes_only_created_network`
- `test_container_failure_removes_volume_then_network`
- `test_start_failure_removes_container_volume_network_in_reverse_order`
- `test_failure_closes_client_once`

每个测试断言删除顺序、对象 ID 和未调用的方法；fake 冲突对象不得进入事务栈。

- [ ] **步骤 2：运行失败阶段测试并确认 RED**

运行：

```bash
uv run pytest tests/test_sandbox_runtime.py -v -k "missing or factory or conflict or failure"
```

预期：测试失败，因为错误分类和回滚尚未实现。

- [ ] **步骤 3：实现 Docker 错误分类和逆序回滚**

增加私有事务记录和回滚方法；删除前重新加载对象并核对资源 ID 与四个标签。容器使用 `remove(force=True)`，volume 和网络使用各自 `remove()`。回滚完成后保留原始分类：冲突仍抛出 `SandboxResourceConflict`，连接或传输失败仍抛出 `DockerUnavailable`，其他失败抛出 `SandboxCreateFailed`；归属核对或删除失败时统一抛出 `SandboxCleanupFailed(spec.sandbox_id)`。

- [ ] **步骤 4：编写归属保护和清理失败测试并确认 RED**

增加：

- `test_rollback_refuses_resource_with_changed_id`
- `test_rollback_refuses_resource_with_missing_or_changed_label`
- `test_cleanup_failure_reports_sandbox_id_and_keeps_original_error_chain`
- `test_rollback_never_searches_for_resources_by_name`

运行：

```bash
uv run pytest tests/test_sandbox_runtime.py -v -k "rollback or cleanup"
```

预期：新增测试失败，直到完整归属核对实现。

- [ ] **步骤 5：实现归属核对和清理失败聚合**

资源 reload、ID 或标签核对失败都必须跳过删除并记录清理错误。回滚尝试所有已记录资源，不能因为第一个删除失败而停止后续安全清理。

- [ ] **步骤 6：编写取消期间回滚测试并确认 RED**

使用 `threading.Event` 阻塞 fake SDK 调用，增加：

- `test_cancel_stops_before_next_create_step_and_waits_for_rollback`
- `test_cancel_during_start_removes_started_or_created_resources`
- `test_cancel_after_thread_commit_rolls_back_committed_resources`
- `test_cancel_does_not_leave_unhandled_thread_exception`
- `test_cancel_with_incomplete_cleanup_raises_cleanup_failed`

不使用固定 sleep；通过事件确认线程进入 SDK 调用，再取消 asyncio task。

- [ ] **步骤 7：实现取消通知、等待和异常消费**

`create()` 创建工作线程 task 并使用 shield 等待；同步成功路径向异步包装层返回只含资源 ID、类型和预期标签的私有事务记录，但公共方法仍返回 `None`。捕获 `CancelledError` 后设置 `threading.Event` 并再次等待线程：线程已自行回滚时直接传播取消，线程已在取消前提交成功时使用新的短超时 client 按记录 ID 获取、核对并精确回滚，随后关闭该 client。清理不完整时抛出 `SandboxCleanupFailed`。线程在每个 SDK 步骤之后和下一步骤之前检查事件。

- [ ] **步骤 8：运行 runtime 测试和完整测试并确认 GREEN**

运行：

```bash
uv run pytest tests/test_sandbox_runtime.py -v
uv run pytest
```

预期：所有错误、取消、回滚及现有测试通过，无未处理 task/thread 异常。

- [ ] **步骤 9：提交任务 3**

```bash
git add control/control/sandbox_runtime.py control/tests/test_sandbox_runtime.py
git commit -m "feat(control): roll back failed sandbox creation"
```

### Task 4（任务 4）：创建 API 与稳定错误映射

**文件：**
- 修改：`control/control/app.py`
- 修改：`control/tests/test_app.py`

**接口：**
- 使用：任务 1 的 `SandboxCreator`、`SandboxCreateResult` 和领域错误。
- 使用：任务 2–3 的 `DockerSandboxRuntime` 与 `SandboxService` 作为默认实现。
- 产出：`POST /v1/sandboxes`。
- 产出：最终 `create_app(probe=None, sandbox_creator=None, readiness_timeout=1.0) -> FastAPI`。

- [ ] **步骤 1：编写成功请求和空正文验证的失败测试**

在 `test_app.py` 增加 fake creator 和：

- `test_create_sandbox_returns_201_id_and_started_status`
- `test_create_sandbox_passes_no_client_configuration_to_creator`
- `test_nonempty_json_or_whitespace_body_returns_400_without_calling_creator`
- `test_chunked_nonempty_body_stops_after_first_nonempty_chunk`
- `test_unsupported_sandbox_method_returns_405_without_calling_creator`

- [ ] **步骤 2：运行 API 聚焦测试并确认 RED**

运行：

```bash
uv run pytest tests/test_app.py -v -k "sandbox"
```

预期：404 或导入失败，因为路由和默认 creator 尚不存在。

- [ ] **步骤 3：实现路由、空正文检查和默认依赖组装**

扩展 `create_app` 签名。实现一个私有异步正文检查函数，只消费到第一个非空 chunk。默认 creator 为 `SandboxService(DockerSandboxRuntime())`，构造应用时不得连接 Docker。

- [ ] **步骤 4：编写领域错误映射测试并确认 RED**

参数化覆盖六类错误，断言设计文档表格中的精确 HTTP 状态、code、message 和敏感信息缺失。单独断言 `SandboxCleanupFailed` 响应包含 sandbox ID；清理成功的普通取消必须重新传播，不能映射为 500，取消期间清理失败则按 `sandbox_cleanup_failed` 处理。

- [ ] **步骤 5：实现稳定错误映射**

用显式异常分支映射设计文档中的 HTTP 契约，不捕获 `BaseException`，不返回 `str(error)`，不记录完整 Docker 异常。

- [ ] **步骤 6：运行应用测试和完整测试并确认 GREEN**

运行：

```bash
uv run pytest tests/test_app.py -v
uv run pytest
```

预期：创建 API、`/healthz`、`/readyz` 和现有测试全部通过。

- [ ] **步骤 7：提交任务 4**

```bash
git add control/control/app.py control/tests/test_app.py
git commit -m "feat(control): expose sandbox creation API"
```

### Task 5（任务 5）：显式 Docker 集成测试

**文件：**
- 修改：`control/pyproject.toml`
- 新建：`control/tests/test_sandbox_docker_integration.py`

**接口：**
- 使用：任务 1–4 的生产 `SandboxService` 和 `DockerSandboxRuntime`。
- 产出：默认跳过、仅通过 `RUN_DOCKER_INTEGRATION=1` 和 `docker_integration` marker 显式执行的真实 Docker 验收。

- [ ] **步骤 1：注册 marker 并编写默认跳过测试**

在 pytest 配置注册 `docker_integration`。集成测试模块使用 `skipif(os.getenv("RUN_DOCKER_INTEGRATION") != "1")`，测试函数具有 docstring。

运行：

```bash
uv run pytest tests/test_sandbox_docker_integration.py -v
```

预期：测试被 skip，且没有创建 Docker 资源。

- [ ] **步骤 2：实现显式成功路径集成测试**

使用唯一确定性 ID 创建 sandbox，inspect 网络、volume 和容器，断言完整标签、internal bridge、单一网络、空 `PortBindings`、固定资源限制、只读根文件系统、tmpfs、volume 挂载和非 root 用户。

`finally` 中只通过已知资源 ID 获取对象；每次删除前核对完整标签，然后按容器、volume、网络顺序清理。不得扫描或删除名称相近资源。

- [ ] **步骤 3：运行默认完整测试并确认不触发 Docker 集成**

运行：

```bash
uv run pytest
```

预期：单元测试通过，集成测试显示 skip，不创建或删除 Docker 资源。

- [ ] **步骤 4：在资源快照保护下显式运行集成测试**

先确认 `agent-sandbox:dev` 存在，并记录现有容器、网络和 volume ID 及原 Compose 资源状态。然后运行：

```bash
RUN_DOCKER_INTEGRATION=1 uv run pytest -m docker_integration -v
```

预期：测试通过；测试 ID 对应的三类资源均已清理；前后快照和原 Compose 资源状态一致。若镜像或 Docker 不可用，应报告受阻，不得 pull、构建、停止或删除现有资源。

- [ ] **步骤 5：提交任务 5**

```bash
git add control/pyproject.toml control/tests/test_sandbox_docker_integration.py
git commit -m "test(control): verify Docker sandbox creation"
```

### Task 6（任务 6）：文档和最终验收

**文件：**
- 修改：`README.md`
- 修改：`docs/CONTROL_PLANE.md`
- 修改：`docs/PROJECT_PROGRESS.md`

**接口：**
- 使用：任务 1–5 的最终 HTTP 契约、固定资源规则和验证命令。
- 产出：准确描述当前创建能力及缺失生命周期环节的用户文档。

- [ ] **步骤 1：更新 README 和 Control 文档**

增加空请求 curl 示例、201 响应、稳定错误、固定安全配置和资源标签。明确新 sandbox 无宿主端口，当前无法通过宿主机访问；`started` 不是 ready；没有查询、动态入口或公开删除 API。

- [ ] **步骤 2：更新项目进度**

在 `2026-10-01` 记录最小创建接口、事务回滚和显式集成测试。待开发项保留查询、Agent 就绪、动态入口、删除、TTL 和恢复能力。

- [ ] **步骤 3：运行自动验证**

运行：

```bash
cd control
uv sync --locked
uv run pytest
uv run python -m compileall -q control tests
cd ..
git diff --check
```

预期：依赖锁定同步成功；单元测试通过，Docker 集成测试默认 skip；编译和 diff 检查通过。

- [ ] **步骤 4：运行受保护的真实 Docker 验收**

在不影响现有资源的前提下执行任务 5 的快照和显式集成测试。额外以本机 Control 进程调用一次 `POST /v1/sandboxes`，记录 201 响应，然后只使用响应 ID 和完整标签核对并清理该手动验收资源。

同时验证：

- `/healthz` 和 `/readyz` 保持原响应；
- 新容器无 host port binding，只连接专属 internal 网络；
- 原 Compose 容器、网络和 workspace volume 未变化；
- 测试和手动验收均没有残留本次资源。

若镜像、Docker 或端口条件不满足，明确报告未执行项，不得修改现有资源以强行通过。

- [ ] **步骤 5：提交任务 6**

```bash
git add README.md docs/CONTROL_PLANE.md docs/PROJECT_PROGRESS.md
git commit -m "docs: document sandbox creation control plane"
```

## 最终验证清单

- [ ] `uv sync --locked`
- [ ] `uv run pytest`
- [ ] `uv run python -m compileall -q control tests`
- [ ] `git diff --check`
- [ ] 显式 Docker 集成测试通过，或明确记录环境阻塞
- [ ] 新资源配置、标签、专属 internal 网络和空 host port binding 经 inspect 验证
- [ ] 原 Compose 项目资源前后不变
- [ ] 本次测试资源无残留
- [ ] 设计、README 和进度文档与实际行为一致
