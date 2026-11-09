# Kubernetes Runtime 实现

本文对应当前 Runtime 探测、主 Pod 规格构建/查询/删除与 HTTP 就绪链路，说明代码位置、客户端装配、收尾和验证步骤。Runtime.create 已复用该规格实现单主 Pod + 默认拒绝 NetworkPolicy 创建闭环，但不表示正式 HTTP 创建接入可用。能力与信任边界见[设计说明](DESIGN.md)，接口响应见[控制面 API](../API.md#3-后端就绪检查默认-docker)，历史验收结果统一记录在[开发进度](../../PROJECT_PROGRESS.md)。

## 代码边界

| 位置 | 当前职责 |
| --- | --- |
| [runtime/kubernetes/kubernetes_runtime.py](../../../control_plane/runtime/kubernetes/kubernetes_runtime.py) | 配置校验、客户端工厂、探测/查询/删除异步入口、线程等待和未实现方法 |
| [runtime/kubernetes/pod_query.py](../../../control_plane/runtime/kubernetes/pod_query.py) | 纯 Pod 身份、时间、容器结构和状态解析，不调用 SDK 或数据库 |
| [runtime/kubernetes/pod_create.py](../../../control_plane/runtime/kubernetes/pod_create.py) | 策略先行提交、启动等待、双资源结果确认、原 UID 回滚与失败证据 |
| [runtime/kubernetes/pod_delete.py](../../../control_plane/runtime/kubernetes/pod_delete.py) | 单 Pod 删除身份/布局校验、前置条件 DELETE、单调预算下的结果确认 |
| [runtime/kubernetes/network_policy.py](../../../control_plane/runtime/kubernetes/network_policy.py) | NetworkPolicy 纯规格、精确发现/归属和拒绝规则核验、原 UID 删除及消失确认 |
| [runtime/kubernetes/pod_spec.py](../../../control_plane/runtime/kubernetes/pod_spec.py) | 无 I/O 的受约束 basic 主 Pod 规格构建，返回独立 V1Pod |
| [runtime/sandbox_runtime.py](../../../control_plane/runtime/sandbox_runtime.py) | 共用异步 Runtime 契约，不包含 Kubernetes SDK 类型 |
| [core/tasks.py](../../../control_plane/core/tasks.py) | 首次/重复取消收尾及最终结果读取 |
| [core/errors.py](../../../control_plane/core/errors.py) | Kubernetes 专属固定领域错误与创建失败证据 |
| [core/service.py](../../../control_plane/core/service.py) | `ping()` 直接委托给注入的 Runtime，不查询 Store |
| [app.py](../../../control_plane/app.py) | 装配共享 Service，按 Runtime 选择固定就绪失败响应 |
| [api/health.py](../../../control_plane/api/health.py) | 进程健康、认证后的就绪调用与 503 响应，不识别具体后端类型 |
| [pyproject.toml](../../../control_plane/pyproject.toml) 与 [uv.lock](../../../control_plane/uv.lock) | 官方 Kubernetes 客户端依赖及锁定版本 |

源码保持浅层结构：`pod_spec.py` 构建纯规格，`pod_create.py` 维护单 Pod 创建事务，`pod_query.py` 提供纯身份/状态解析，`pod_delete.py` 集中维护删除确认流程；客户端装配和异步收尾仍在 `kubernetes_runtime.py`。没有 PodBuilder 类、独立 HTTP 服务、Runtime 注册器或生产切换框架。

## 客户端与同步探测

`KubernetesSandboxRuntime.__init__()` 校验明确的路径、context、namespace，只保存值和工厂。不存在的配置文件不会在构造时读取；实际加载发生在 ping 的工作线程内。

| 阶段 | 对应实现 | 行为 |
| --- | --- | --- |
| 异步入口 | `ping()` | 创建 `asyncio.to_thread()` worker，用 `settle_task(..., propagate_cancellation=True)` 等待 |
| 同步操作 | `_ping_and_close()` | 创建一个客户端，执行一次 Pod 列表调用，在 finally 关闭 |
| 共同收尾 | `_call_and_close()` | 复用 worker 内的客户端获取、错误分类和主错误优先关闭，供 ping/get/delete 使用 |
| Pod 查询 | `_read_primary_pod()` | limit=2 发现、分页和唯一性检查、按名读取及 UID/事务/时间复核 |
| 配置选择 | `_selected_entry()` | 按名称选取唯一 context、user 和 cluster，拒绝缺失/重复条目 |
| 配置加载 | `_load_configuration()` | 安全解析指定文件，限制静态证书认证，用 `KubeConfigLoader` 加载独立配置 |
| 客户端工厂 | `create_probe_client()` | 创建 `ApiClient(configuration=...)` 与 `CoreV1Api` 与 `NetworkingV1Api`，共享同一 ApiClient；任一 facade 构造失败时关闭已取得的客户端 |
| 关闭 | `_close_client()` | 主错误优先；单独关闭失败转换为固定连接错误 |
| 错误转换 | `_probe_failure()` | 按 SDK 类型、状态码和 transport 类型分类，不解析错误正文 |

配置使用 `proxy=None`、`no_proxy="*"`、`retries=False`、`client_go_retries=False` 和 `debug=False`。禁用代理在加载 API host 前完成，避免 SDK 随 host 重新选择环境代理。证书相对文件路径以 kubeconfig 所在目录为基准，不修改 SDK 全局默认配置或调用方环境。

同步查询固定 `limit=1`、`watch=False`、`_request_timeout=(0.5, 0.5)`；返回值只用于确认调用成功，不跟随分页，不将 Pod 内容返回给 Service。客户端由同一工作线程关闭，连接和读取超时的限制见[设计](DESIGN.md#探测与期限)。

测试工厂通过 `client_factory` 注入，签名接收 `kubeconfig_path`、`context` 两个关键字。按所调用能力，返回对象提供 `networking` facade（NetworkPolicy create/list/read/delete），并需提供 `create_namespaced_pod()`、`list_namespaced_pod()`、`read_namespaced_pod()`、`delete_namespaced_pod()`、`read_namespace()` 与 `api_client.close()`；namespace、limit、watch 和 timeout 由 Runtime 设置。工厂只在操作的 worker 中调用，每次操作独立取得和关闭句柄。

## 纯规格构建

`build_pod_spec(spec: SandboxSpec, *, namespace: str, agent_image: str) -> V1Pod` 独立于 Runtime.create。它先验证显式后端参数，复制构造 `SandboxSpec` 以复用现有领域校验，再复用 `sandbox_pod_name()` 和 `labels_for("container")`。复制时保留原字段，不触发默认身份或时钟。没有客户端、文件、环境、DNS 或数据库操作。

namespace 使用与现有 Runtime 相同的 DNS label 规则。镜像只做有界 ASCII 字符检查，不执行完整 registry 语义/内容验证；不得将任意公共请求的值传给该后端参数。仅支持 basic、无独立工作区、无出口策略。输入中的实例 token 不进入任何 Pod 字段。

```python
from datetime import UTC, datetime, timedelta
from control_plane.core.models import SandboxSpec
from control_plane.runtime.kubernetes.pod_spec import build_pod_spec

created = datetime(2026, 11, 6, 8, 0, 0, 123456, tzinfo=UTC)
spec = SandboxSpec(
    sandbox_id="sbx_0123456789abcdef0123456789abcdef",
    transaction_id="txn_0123456789abcdef0123456789abcdef",
    container_name="unused", network_name="unused", volume_name="unused",
    created_at=created, expires_at=created + timedelta(minutes=30),
)
pod = build_pod_spec(
    spec, namespace="agent-sandbox-test", agent_image="agent-sandbox-basic:dev",
)
```

以上只得到 SDK 对象，不提交或启动 Pod。规格固定命令、8080 readiness、Never 重启/拉取、非 root UID/GID/fsGroup 1000、安全上下文和资源配置；`/workspace` 为 1 GiB 磁盘 emptyDir，`/tmp` 为 64 MiB 内存 emptyDir，均没有持久化承诺。字段与限制详见[设计](DESIGN.md#主-pod-规格构建)。与查询/删除兼容的测试仅补入模拟 API 的 UID、resourceVersion、nodeName 和状态，不冒充真实运行。

## Runtime 单 Pod 创建流程

1. `create()` 使用可信 `agent_image`（默认 `agent-sandbox-basic:dev`）调用纯规格构建器，提交前拒绝不支持的功能。
2. `PodCreation` 保存本次事务、业务时间、取消事件与原 UID；`_create_and_close()` 在工作线程延迟获取客户端。
3. `run()` 确认 namespace、Pod 与策略的空候选/确定名称，先单次提交并核验策略，再单次调用 `create_namespaced_pod()`。明确拒绝（尤其 409）不接管冲突对象；但策略已创建时仍须按双资源证据补偿。连接/超时错误不能据此断言没有提交。
4. 验证响应归属与配置后保存 UID，在 30 秒预算内按名读取并复核该 UID，Agent running 后返回，不等待 Ready。规格对比接受资源数量等价和已知默认 false 的省略；凭据挂载、提权等显式限制缺失仍失败。
5. 失败进入 `rollback()`。Pod 与策略的丢失响应分别有界确认；无正面证据或有身份冲突则 UNKNOWN，不重发 POST。先用 `PodDeletion(..., expected_uid=..., delete_policy=False)` 确认 Pod 已消失，再按原策略 UID 清理。Pod 残留或结果不明时保留策略；必须清完两类本次资源才报告 ROLLED_BACK。
6. `_create_and_close()` 在原线程关闭客户端，保留主错误；成功后关闭失败报告 REMAINING。`_probe_failure()` 保留创建/清理领域错误及其 CreationFailureInfo。
7. 外层用 `wait_for_task` 响应取消，再用 `settle_task` 等待在途 SDK 和清理。若线程已经成功，则 `_rollback_created_and_close()` 新开客户端，只补偿已保存的原 UID；重复取消不能中断等待。未经确认的清理不能以普通取消结束。

没有新配置框架、通用事务引擎或 SQLite 操作。`resolve_environment_image`、入口、工作区、暂停恢复和资源核验仍不支持；不能将 Runtime.create 通过等同于 Service 的默认私有工作区补偿已经兼容。

### 显式真实创建验收

确认前述 Minikube 节点和 namespace 后，在 `control_plane/` 执行：

```bash
RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_create_integration.py
```

fixture 为本次唯一 sandbox/事务准备规格，捕获 POST 的原 UID，并保留独立官方客户端兜底清理。成功用例通过 Runtime.create 启动 basic Pod，再等待 get 返回 ready；测试侧有限时 kubectl exec 验证 UID/GID 1000、/workspace 和 /tmp 写入，以及根挂载为只读，随后 Runtime.delete 并确认未找到。exec 不是 Runtime 新接口，不读取用户环境或宿主凭据。

失败用例使用唯一、确定不存在的测试镜像和 imagePullPolicy=Never，验证本地启动失败、已取得原 UID、ROLLED_BACK 及目标 404，不访问镜像仓库。fixture 不把被测试的回滚作为唯一清理依据：始终按完整标签、原始时间和原 UID 独立复核，必要时使用 UID 前置条件优雅删除并有界确认消失；替代对象不清理，失败报告残留。

两个用例默认在读取真实配置和创建客户端前跳过。只使用已有缓存镜像、现有 namespace 与本次 Pod，不操作开发库、其他 Pod、PVC、Secret、RBAC 或插件；没有持久工作区、入口或网络隔离的端到端验收。

## 主 Pod 查询流程

1. `get(sandbox_id)` 先用 `sandbox_pod_name()` 校验公共 ID 并映射合法 Pod 名，不分配新身份或读取当前时间。
2. worker 通过现有工厂打开客户端。`primary_pod_selector()` 只筛选四项归属标签，不过滤事务或 Pod 名。
3. `list_namespaced_pod(namespace, label_selector=..., limit=2, watch=False, _request_timeout=(0.5, 0.5))` 只读取一页。分页未结束或多个候选为查询失败，无候选为未找到，集合级 404 为 namespace 配置错误。
4. `pod_identity()` 校验候选名称、namespace、UID、完整归属、事务和两项时间 annotations，拒绝工作区绑定标记。
5. `read_namespaced_pod(name, namespace, _request_timeout=(0.5, 0.5))` 取得新观察。目标级 404 为未找到；UID、事务或初始时间变化为查询失败，不接管同名替换对象。
6. 新布局还调用 `NetworkPolicyResource.verify_for()`，核对 Pod 绑定的策略 UID、归属、原始时间、精确 selector 和双向零规则；缺失/放宽均返回固定查询错误。`query_result()` 校验单 Agent 容器、Pod/容器事实及 readinessProbe，返回现有 `SandboxQueryResult`；finally 由同一 worker 关闭客户端。

两个 SDK 请求各有有限阶段超时，但不构成严格总 deadline。身份、时间和状态异常转换为固定 `SandboxQueryFailed`，传输失败保留 Kubernetes 专属分类，关闭异常不能覆盖主查询错误。只读取 Pod 元数据与状态，不读取日志、Events、Secret，不执行 exec 或额外 HTTP Agent 探测。

纯解析模块集中维护 Pod 名称、Agent 名称、标签 selector、UTC annotations 和状态规则。Pod 只接受一个名为 `agent` 的普通容器；创建约定和完整状态语义见[设计](DESIGN.md#主-pod-查询约定)。已知等待错误分别提供 `agent_image_pull_failed`、`agent_image_pull_backoff`、`agent_image_unavailable`、`agent_invalid_image`、`agent_config_error`、`agent_create_error`、`agent_run_error`、`agent_crash_loop` 等固定 reason，不透传 Kubernetes message。

`SandboxService.get()` 直接使用返回的内部 transaction_id、created_at 核对持久化记录，再合并数据库当前 expires_at；不因查询而插入、激活或续期。Runtime 无需读取凭据或 SQLite，annotations 仍保存最初期限，元数据记录不代表 Pod 实际存在。

## 单主 Pod 删除流程

1. `delete()` 在主线程校验 ID、可选事务及不支持的工作区参数，随后建立工作线程；不调用 `get()` 或 readiness 解析。
2. `_call_and_close()` 创建独立客户端，`PodDeletion` 建立 45 秒单调时钟预算。每次 SDK 阶段超时取 0.5 秒与剩余预算的一半的较小值。
3. `_inventory()` 使用 managed/project/sandbox-id 三标签，`limit=2`，拒绝多候选和分页；`_read()` 始终精确检查确定名称，包括发现无候选的情况。
4. `_deletion_identity()` 复用 `pod_ownership()`，另外校验 resourceVersion 和当前支持的单 Agent/emptyDir 布局，拒绝未知平台标记、存储绑定或其他 Pod 角色。业务时间、phase、readiness 不作为删除门槛。
5. 对未进入删除的原对象，`_request_delete()` 只发送一次 `delete_namespaced_pod`，传入 UID 和精确读取所得 resourceVersion 的 `V1Preconditions`。不设置 gracePeriodSeconds、不改 finalizer；409 返回冲突，不刷新重试。
6. `_wait_for_absence()` 按 0.1 秒间隔读取原名称，校验原 UID/事务和完整归属。404 后 `_confirm_absence()` 再确认空候选和 namespace 存在，才返回成功。响应丢失也走同一确认路径；预算用尽仍为失败。
7. finally 在同一线程关闭客户端；异步 `settle_task` 等待完成后才传播取消。删除领域错误在 `_probe_failure()` 中保留，不被归并为连接失败；关闭错误不能覆盖主错误。

原 Pod 已有 deletionTimestamp 时只等待，不再次 DELETE。查询/删除期间的不同 API 观察不是原子快照；前置条件保护本次 mutation，不等于跨进程 fencing。成功仅确认 API 对象消失，不保证故障节点进程已被强制隔离。

`SandboxService.delete()` 仍先认领原记录为 deleting，Runtime 成功后才清理对应版本元数据。Runtime 的超时、冲突或无法确认的结果会留下记录；不写 SQLite、不撤销用户数据，也不将工作区参数默默忽略。当前没有开放完整 Kubernetes 生命周期装配。

## HTTP 装配与失败处理

`create_app(runtime=runtime)` 保留既有 Service 装配。应用识别 `KubernetesSandboxRuntime` 后，将固定的 `(code, message)` 传给 `health_router()`；路由本身只执行 `SandboxService.ping()`，不判断 Runtime 类型或调用 SDK。没有注入 Runtime 时仍构造 Docker 后端，其失败状态码、文案和认证行为保持不变。

Kubernetes 配置、权限、连接、超时和关闭失败均在根就绪检查返回固定 503；内部领域分类不变，未知异常内容也不用于响应。`CancelledError` 不作为普通失败吞掉。超时或取消需要等待原工作线程完成关闭，迟到失败被读取，不提前释放仍在使用的客户端。

其他 Runtime 方法，包括镜像解析、入口、资源核验、工作区、暂停/恢复，仍抛出 `KubernetesOperationUnsupported`。未支持的创建规格仍在提交前提供 `UNCHANGED`；这些未实现能力的调用不创建客户端、不访问集群，也不退回 Docker。

## 直接调用只读 ping

在仓库根执行，显式指定本机配置文件与测试目标；无需启动 HTTP 应用或初始化数据库：

```bash
uv run --project control_plane --locked python - <<'PY'
import asyncio
from pathlib import Path
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime

runtime = KubernetesSandboxRuntime(
    kubeconfig_path=Path.home() / ".kube" / "config",
    context="agent-sandbox-k8s",
    namespace="agent-sandbox-test",
)
asyncio.run(runtime.ping())
PY
```

成功返回 `None`，脚本不打印 kubeconfig、API 地址、证书或客户端内容。此调用不会切换当前 context。

## 隔离运行 HTTP 就绪验证

先按[项目快速开始](../../../README.md#快速开始)设置管理员 key。以下在仓库根运行，使用临时数据库并关闭 reaper 与 pending recovery；仅用于验证健康链路，不调用未实现的生命周期接口。退出后临时元数据目录随应用关闭清理，不访问开发数据库：

```bash
uv run --project control_plane --locked python - <<'PY'
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import uvicorn
from control_plane.app import create_app
from control_plane.runtime.kubernetes.kubernetes_runtime import KubernetesSandboxRuntime

runtime = KubernetesSandboxRuntime(
    kubeconfig_path=Path.home() / ".kube" / "config",
    context="agent-sandbox-k8s",
    namespace="agent-sandbox-test",
)
with TemporaryDirectory(prefix="agent-sandbox-kubernetes-") as directory:
    application = create_app(
        runtime=runtime,
        api_key=os.environ["AGENT_SANDBOX_API_KEY"],
        database_path=Path(directory) / "lifecycle.sqlite3",
        reaper_enabled=False,
        pending_recovery_enabled=False,
    )
    uvicorn.run(application, host="127.0.0.1", port=18083)
PY
```

选择未被已有控制面占用的本机端口；不要为了运行此示例停止或替换现有开发服务。在另一个已设置同一 key 的终端执行：

```bash
curl --max-time 5 http://127.0.0.1:18083/healthz
curl --max-time 5 -H "Authorization: Bearer $AGENT_SANDBOX_API_KEY" \
  http://127.0.0.1:18083/readyz
```

进程健康成功为 `{"status":"ok"}`，探测成功为 `{"status":"ready"}`。没有管理员 key 的 `/readyz` 返回 401，不读取 kubeconfig 或执行探测。客户端请求超时或断开不代表服务端工作线程立即停止。

## 默认测试与显式集成

| 测试 | 验证范围 |
| --- | --- |
| [test_kubernetes_runtime.py](../../../control_plane/tests/test_kubernetes_runtime.py) | 延迟装配、显式配置、单页读取、超时、错误脱敏、关闭和取消、未实现能力 |
| [test_kubernetes_health.py](../../../control_plane/tests/test_kubernetes_health.py) | API → Service → Runtime、认证前置、后端错误、导入无副作用、HTTP 超时与重复取消 |
| [test_kubernetes_query.py](../../../control_plane/tests/test_kubernetes_query.py) | ID、selector、唯一性/分页、Pod 身份与时间、状态/探针、故障和取消收尾 |
| [test_kubernetes_service_query.py](../../../control_plane/tests/test_kubernetes_service_query.py) | 真实临时 SQLite 身份合并、更新后期限、冲突、无记录与实例不存在时的只读行为 |
| [test_kubernetes_delete.py](../../../control_plane/tests/test_kubernetes_delete.py) | 精确身份/布局、UID/RV 前置条件、结果不明、总预算、重复取消、关闭与 Service 元数据保留 |
| [test_kubernetes_network.py](../../../control_plane/tests/test_kubernetes_network.py) | 双向零规则、版本/UID 绑定、双资源顺序、部分资源、策略响应丢失及取消/清理证据 |
| [test_kubernetes_create.py](../../../control_plane/tests/test_kubernetes_create.py) | 单次创建、running 等待、冲突/未知结果、原 UID 回滚、晚到成功及重复取消、关闭失败 |
| [test_kubernetes_pod_spec.py](../../../control_plane/tests/test_kubernetes_pod_spec.py) | 纯构建、严格校验、安全/资源/卷、凭据排除、输出独立及 get/delete 兼容 |
| [test_app.py](../../../control_plane/tests/test_app.py) | 默认 Docker、健康路由、Service 委托和应用装配回归 |
| [test_kubernetes_runtime_integration.py](../../../control_plane/tests/test_kubernetes_runtime_integration.py) | 显式目标的真实 Runtime ping 与认证 HTTP 就绪链路 |
| [test_kubernetes_query_integration.py](../../../control_plane/tests/test_kubernetes_query_integration.py) | 双重开关下仅创建本次唯一 Pod，验证真实 get/Service 和 UID 前置条件清理 |
| [test_kubernetes_delete_integration.py](../../../control_plane/tests/test_kubernetes_delete_integration.py) | 双重开关下验证错误事务不删除、Runtime 精确删除、已删除/删除中的幂等行为；fixture 独立清理 |
| [test_kubernetes_network_integration.py](../../../control_plane/tests/test_kubernetes_network_integration.py) | 数字 Pod IP 跨节点正面对照、入/出站及 sandbox 间隔离、readiness 与双资源清理 |
| [test_kubernetes_create_integration.py](../../../control_plane/tests/test_kubernetes_create_integration.py) | 真实 Runtime 创建/删除、ready、非 root/挂载/只读根及缺失镜像回滚；独立 fixture 清理 |
| [test_kubernetes_pod_spec_integration.py](../../../control_plane/tests/test_kubernetes_pod_spec_integration.py) | 双重开关的 server-side dry-run，确认 API 接受规格且没有持久化 Pod |

在 `control_plane/` 执行默认检查；使用 Fake Client，不要求集群运行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_network.py tests/test_kubernetes_create.py tests/test_kubernetes_delete.py tests/test_kubernetes_query.py tests/test_kubernetes_pod_spec.py
uv run --locked pytest -q tests/test_kubernetes_runtime.py tests/test_kubernetes_health.py tests/test_app.py tests/test_kubernetes_query.py tests/test_kubernetes_service_query.py tests/test_kubernetes_delete.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests
```

现有验证环境为 WSL Ubuntu-24.04，Minikube v1.39.0 / Docker driver，Kubernetes v1.37.0，1 个控制节点和 2 个工作节点，containerd 与 Calico，namespace 为 `agent-sandbox-test`。客户端约束 `kubernetes>=37.0.1,<38`，锁定 37.0.1；版本关系参考[官方兼容矩阵](https://github.com/kubernetes-client/python#compatibility)。已有存储类不参与当前探测。

只读确认已有集群，再显式启用集成：

```bash
kubectl --context=agent-sandbox-k8s get nodes
kubectl --context=agent-sandbox-k8s get namespace agent-sandbox-test
RUN_KUBERNETES_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_runtime_integration.py
```

必须同时提供开关、context 和 namespace，否则测试在加载 kubeconfig 和创建客户端前跳过。可用 `KUBERNETES_TEST_KUBECONFIG` 指定配置文件；未提供时使用明确的 `~/.kube/config` 路径，不采用环境中的当前 context。HTTP 集成使用 Fake Store 并关闭回收与恢复，不访问开发库，不创建/修改 Pod、Secret、RBAC、namespace 或插件。跳过不等于真实集群验收通过。

本轮不需要重建镜像或操作 Docker 资源；在仓库根完成 `git diff --check`。实际已运行的测试、平台与未验证项见[开发进度](../../PROJECT_PROGRESS.md)。

### 双重开关的独立资源集成

原 `test_kubernetes_runtime_integration.py` 保持只读。资源测试必须同时设置基础开关、资源开关、context 和 namespace；缺任一项即在创建客户端和读取真实配置前跳过：

```bash
RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_query_integration.py tests/test_kubernetes_delete_integration.py
```

fixture 只选择已缓存 `agent-sandbox-basic:dev` 的现有节点，使用 `imagePullPolicy=Never`，不拉取、构建或加载镜像。它创建随机 sandbox/事务身份的单 Agent Pod，配置固定 `/healthz:8080` readinessProbe、非 root、只读根、有限资源和 `/tmp` emptyDir，关闭 service account token 自动挂载；不创建 Service、Secret、PVC、RBAC 或 namespace。

查询集成中的 Runtime 仍只有读取操作，Pod 创建/删除均属于 fixture。测试以真实 Pod 验证原始 annotations 与 Service 当前期限合并、重建 Store 后恢复以及删除后的 `SandboxNotFound`。临时 SQLite 不连接开发库，不启动回收或恢复；已过期的业务时间也不会触发 Runtime 删除。

删除集成另行调用 `Runtime.delete()`，验证错误事务不会改变原 Pod、DELETE 完成后查询 404、重复删除及等待已删除中的原目标。两类资源测试共享 fixture 的独立兜底清理，不能只依赖被测试的 Runtime.delete；测试断言及 fixture teardown 均通过才算清理完成。

fixture 删除前重新读取准确名称，核对 namespace、全部归属标签、初始时间和记录的 UID，并传入 `V1DeleteOptions(preconditions=V1Preconditions(uid=...))`；同名替换或归属变化时拒绝删除。创建响应丢失时仅发现本次唯一名称和完整身份，取得确认后的 UID 再清理。删除后有界等待确认消失；失败时报告本次名称/UID，不做无条件强删或批量清理。

### 仅 server-side dry-run 的规格集成

此用例默认跳过，也要求基础开关、资源开关、明确 context 与 namespace。复用前文的节点和 namespace 确认命令后，在 `control_plane/` 执行：

```bash
RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_pod_spec_integration.py
```

用例从显式配置创建私有客户端，对唯一随机身份的规格只调用 `create_namespaced_pod(..., dry_run="All")`，设置有限请求超时，最终关闭客户端。前后按名称读取均须为 404；不创建/删除实际 Pod 或其他资源，不访问开发库，不拉取/构建镜像。API 可能将 CPU `1000m` 规范化为 `1`，因此比较资源数值等价，不要求文本不变。

dry-run 验证范围仅为 API admission、校验和默认化，见[官方 dry-run 说明](https://kubernetes.io/docs/reference/using-api/api-concepts/#dry-run)。返回的 UID、resourceVersion 等不是已持久化身份；不得据此写 active 或记录创建成功。镜像运行、卷实际写入、进程启动与就绪由独立真实创建集成验证，不能用本 dry-run 结果代替。

### 默认拒绝策略与实际网络验收

`create_probe_client()` 返回 Core facade，其 `networking` 属性为共享同一 ApiClient 的 NetworkingV1Api。工厂构造任一 facade 失败时关闭原客户端；NetworkPolicyResource 不创建连接、不独立 close。`network_policy.py` 集中维护 `build_network_policy()`、精确名称/selector、归属/原始时间/版本核验、零规则核验和 UID 删除，不使用通用资源编排器。

`pod_spec.py` 写入新布局版本，创建流程保存策略 UID 并绑定到待提交 Pod。get 核验绑定与策略内容；PodDeletion 在有 Pod 时先核对绑定，再按 Pod → 策略清理，回滚用 `delete_policy=False` 将 Pod 确认与原策略 receipt 的删除明确分开。无 Pod POST 的事务只确认 Pod 不存在，不能删除后来出现的冲突 Pod 来解锁策略清理。

本轮复用现有 Minikube profile 恢复了控制节点组件并更新过期的 kubeconfig 目标端口，未重建集群、改变插件或切换原 current-context。维护此环境时先检查：

```bash
minikube status -p agent-sandbox-k8s
kubectl --context=agent-sandbox-k8s --request-timeout=5s get nodes
kubectl --context=agent-sandbox-k8s --request-timeout=5s -n kube-system get daemonsets
kubectl --context=agent-sandbox-k8s --request-timeout=5s get namespace agent-sandbox-test
```

目标未恢复时只报告离线结果，不将集成跳过计作通过。已有缓存可用后，在 `control_plane/` 显式执行：

```bash
RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_network_integration.py
```

网络用例使用两个由 Runtime 创建的 sandbox，以及两个不带隔离策略的专属对照 Agent Pod。测试侧只固定节点放置，跨节点先验证对照 Pod 双向 HTTP 可达，再用数字 Pod IP 验证 sandbox 出站、普通 Pod 入站和两个 sandbox 之间均被拒绝；拒绝后再次检查正面对照与 sandbox readiness，避免把 DNS、服务死亡或控制通道失败误认为隔离。

kubectl exec 仅用于在源 Pod 内发起有界 wget；受测数据实际走源 Pod 的 CNI 网络，未以 exec/port-forward/宿主直连代替隔离证据。控制通道执行失败会使测试失败，不当作连接被策略阻断。允许短暂 CNI 收敛，不承诺绝对无启动窗口；不访问公网，也不加任何临时放行策略。

缺失缓存镜像用例验证启动失败后 Pod 和策略均无残留。fixture 保留独立 SDK 清理，记录 Pod/策略原 UID，始终先确认 Pod 消失才删策略；只核验并清理本次完整归属的资源，尝试清理其他测试资源后统一报告残留，保留原失败信息。受影响显式回归包括 `test_kubernetes_create_integration.py`、`test_kubernetes_delete_integration.py`，历史单 Pod 用例保持原布局，不自动附加策略。

## 排查原则

| 观察 | 优先检查 |
| --- | --- |
| `/healthz` 成功、`/readyz` 为 401 | 调用方是否向控制面发送了正确管理员 Bearer key |
| `KubernetesConfigurationError` | 显式文件路径、context、证书/CA 文件与支持的静态认证方式 |
| `KubernetesPermissionDenied` | 所选身份是否具备当前操作所需的 Pod list/get/delete 和目标 namespace get 权限 |
| `KubernetesTimeout` | 指定 API 的连接和读取是否可在当前阶段预算内完成 |
| `KubernetesConnectionFailed` | 指定 API 的可达性、TLS 校验与客户端收尾 |
| `KubernetesOperationUnsupported` | 调用是否超出 basic 主 Pod + 拒绝策略的实现范围 |
| `SandboxNotFound` | ID 格式、该 namespace 是否有匹配主 Pod、发现后的目标是否已消失 |
| `SandboxQueryFailed` | 唯一性、UID/归属、必需 annotations、单 Agent 结构、probe 和 Pod 状态是否完整一致 |
| `SandboxDeleteConflict` | 归属/绑定、单 Pod 布局、替代 UID 或 DELETE resourceVersion 前置条件是否冲突；不要自动接管新对象 |
| `SandboxDeleteFailed` | 原对象是否仍在优雅终止或被 finalizer 阻塞；保留记录，不强删或提前清理元数据 |

根 HTTP 接口统一隐藏内部失败分类，不应通过输出原始 SDK 异常、kubeconfig、证书或认证头来排查。不要自动切换 context、关闭 TLS 验证、改用其他凭据或创建额外资源来掩盖探测失败。
