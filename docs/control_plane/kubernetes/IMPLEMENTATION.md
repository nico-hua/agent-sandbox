# Kubernetes Runtime 实现

本文对应当前 Runtime 探测、主 Pod 规格构建/查询/删除与 HTTP 就绪链路，说明代码位置、客户端装配、收尾和验证步骤。Runtime.create 已复用该规格实现三资源及可选八资源代理创建闭环，但不表示正式 HTTP 创建接入可用。能力与信任边界见[设计说明](DESIGN.md)，接口响应见[控制面 API](../API.md#3-后端就绪检查默认-docker)，历史验收结果统一记录在[开发进度](../../PROJECT_PROGRESS.md)。

## 代码边界

| 位置 | 当前职责 |
| --- | --- |
| [runtime/kubernetes/kubernetes_runtime.py](../../../control_plane/runtime/kubernetes/kubernetes_runtime.py) | 配置校验、客户端工厂、探测/查询/删除异步入口、线程等待和未实现方法 |
| [runtime/kubernetes/pod_query.py](../../../control_plane/runtime/kubernetes/pod_query.py) | 纯 Pod 身份、时间、容器结构和状态解析，不调用 SDK 或数据库 |
| [runtime/kubernetes/pod_create.py](../../../control_plane/runtime/kubernetes/pod_create.py) | 策略 → Service → Pod 提交、启动等待、三资源结果确认、原 UID 回滚与失败证据 |
| [runtime/kubernetes/pod_delete.py](../../../control_plane/runtime/kubernetes/pod_delete.py) | 单 Pod 删除身份/布局校验、前置条件 DELETE、单调预算下的结果确认 |
| [runtime/kubernetes/network_policy.py](../../../control_plane/runtime/kubernetes/network_policy.py) | NetworkPolicy 纯规格、精确发现/归属和拒绝规则核验、原 UID 删除及消失确认 |
| [runtime/kubernetes/proxy_resources.py](../../../control_plane/runtime/kubernetes/proxy_resources.py) | 可选八资源及历史布局闭集发现、原 UID 记录发布、完整性查询与顺序删除，复用同一 worker 客户端 |
| [runtime/kubernetes/proxy_service.py](../../../control_plane/runtime/kubernetes/proxy_service.py) | Proxy ClusterIP 纯规格、精确身份/路由核验，与 Agent Service 独立选择后端 |
| [runtime/kubernetes/proxy_spec.py](../../../control_plane/runtime/kubernetes/proxy_spec.py) | 无 I/O 的代理、Secret 与最小策略规格，供独立验证与可选 Runtime 布局复用 |
| [runtime/kubernetes/agent_service.py](../../../control_plane/runtime/kubernetes/agent_service.py) | Agent ClusterIP 纯规格、归属/配置核验、精确 UID 删除与消失确认；复用 CoreV1Api |
| [runtime/kubernetes/private_workspace.py](../../../control_plane/runtime/kubernetes/private_workspace.py) | 独立私有 PVC 纯规格、单次提交/结果不明核验、Pending/Bound 观察、消费者保护和原 UID 删除；未接入 Runtime |
| [runtime/kubernetes/local_entry_target.py](../../../control_plane/runtime/kubernetes/local_entry_target.py) | 八资源只读目标解析及不可变、无凭据身份快照，复用完整查询与线程内关闭 |
| [runtime/kubernetes/local_entry_manager.py](../../../control_plane/runtime/kubernetes/local_entry_manager.py) | 有界应用会话、并发打开去重、当次目标核验、单应用归属和删除协调 |
| [runtime/kubernetes/local_entry.py](../../../control_plane/runtime/kubernetes/local_entry.py) | 自有 kubectl 会话、loopback 端口解析、有界输出、目标监测及取消/关闭收尾 |
| [runtime/kubernetes/pod_spec.py](../../../control_plane/runtime/kubernetes/pod_spec.py) | 无 I/O 的受约束 basic 主 Pod 规格构建，返回独立 V1Pod |
| [runtime/sandbox_runtime.py](../../../control_plane/runtime/sandbox_runtime.py) | 共用异步 Runtime 契约，不包含 Kubernetes SDK 类型 |
| [core/tasks.py](../../../control_plane/core/tasks.py) | 首次/重复取消收尾及最终结果读取 |
| [core/errors.py](../../../control_plane/core/errors.py) | Kubernetes 专属固定领域错误与创建失败证据 |
| [core/service.py](../../../control_plane/core/service.py) | `ping()` 直接委托给注入的 Runtime，不查询 Store |
| [app.py](../../../control_plane/app.py) | 装配共享 Service，按 Runtime 选择固定就绪失败响应 |
| [api/health.py](../../../control_plane/api/health.py) | 进程健康、认证后的就绪调用与 503 响应，不识别具体后端类型 |
| [pyproject.toml](../../../control_plane/pyproject.toml) 与 [uv.lock](../../../control_plane/uv.lock) | 官方 Kubernetes 客户端依赖及锁定版本 |

源码保持浅层结构：`pod_spec.py` 构建纯规格，`pod_create.py` 维护三资源创建事务，`pod_query.py` 提供纯身份/状态解析，`pod_delete.py` 集中维护删除确认流程；客户端装配和异步收尾仍在 `kubernetes_runtime.py`。没有 PodBuilder 类、独立 HTTP 服务、Runtime 注册器或生产切换框架。

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

测试工厂通过 `client_factory` 注入，签名接收 `kubeconfig_path`、`context` 两个关键字。按所调用能力，返回对象提供 `networking` facade（NetworkPolicy create/list/read/delete），并需提供 `create_namespaced_pod()`、`list_namespaced_pod()`、`read_namespaced_pod()`、`delete_namespaced_pod()`、`read_namespace()` 、CoreV1 Service 的 create/list/read/delete 与 `api_client.close()`；namespace、limit、watch 和 timeout 由 Runtime 设置。工厂只在操作的 worker 中调用，每次操作独立取得和关闭句柄。

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
3. `run()` 确认 namespace、Pod、Service 与策略的空候选/确定名称，先单次提交并核验策略和 Service，再单次调用 `create_namespaced_pod()`。明确拒绝（尤其 409）不接管冲突对象；但策略已创建时仍须按三资源证据补偿。连接/超时错误不能据此断言没有提交。
4. 验证响应归属与配置后保存 UID，在 30 秒预算内按名读取并复核该 UID，Agent running 后返回，不等待 Ready。规格对比接受资源数量等价和已知默认 false 的省略；凭据挂载、提权等显式限制缺失仍失败。
5. 失败进入 `rollback()`。Pod、Service 与策略的丢失响应分别有界确认；无正面证据或有身份冲突则 UNKNOWN，不重发 POST。先用 `PodDeletion(..., expected_uid=..., delete_policy=False, delete_service=False)` 确认 Pod 已消失，再按原 Service UID、原策略 UID 依次清理。Pod 残留或结果不明时保留二者，Service 未清完时保留策略；必须清完三类本次资源才报告 ROLLED_BACK。
6. `_create_and_close()` 在原线程关闭客户端，保留主错误；成功后关闭失败报告 REMAINING。`_probe_failure()` 保留创建/清理领域错误及其 CreationFailureInfo。
7. 外层用 `wait_for_task` 响应取消，再用 `settle_task` 等待在途 SDK 和清理。若线程已经成功，则 `_rollback_created_and_close()` 新开客户端，只补偿已保存的原 UID；重复取消不能中断等待。未经确认的清理不能以普通取消结束。

没有新配置框架、通用事务引擎或 Runtime 内的 SQLite 操作。`resolve_environment_image`、独立工作区、暂停恢复和资源核验仍不支持；临时工作区的 Service 创建补偿现已通过本文后节的真实 Service/SQLite 验证，入口由显式本地会话交付。

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
5. `read_namespaced_pod(name, namespace, _request_timeout=(0.5, 0.5))` 取得新观察。目标级 404 为未找到；UID、事务、初始时间、布局或资源绑定变化为查询失败，不接管同名替换对象。
6. 新布局还调用 `AgentServiceResource.verify_for()` 核对原 Service UID、归属/时间、内部地址、精确端口与 selector；同时调用 `NetworkPolicyResource.verify_for()`，核对 Pod 绑定的策略 UID、归属、原始时间、精确 selector 和双向零规则；缺失/放宽均返回固定查询错误。`query_result()` 校验单 Agent 容器、Pod/容器事实及 readinessProbe，返回现有 `SandboxQueryResult`；finally 由同一 worker 关闭客户端。

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
| [test_kubernetes_agent_service.py](../../../control_plane/tests/test_kubernetes_agent_service.py) | Service 纯规格/独立性、版本与 UID、配置篡改、默认字段、三资源预检/清理、失败证据和重复取消 |
| [test_kubernetes_network.py](../../../control_plane/tests/test_kubernetes_network.py) | 双向零规则、版本/UID 绑定、历史双资源及新建顺序、部分资源、策略响应丢失及取消/清理证据 |
| [test_kubernetes_create.py](../../../control_plane/tests/test_kubernetes_create.py) | 单次创建、running 等待、冲突/未知结果、原 UID 回滚、晚到成功及重复取消、关闭失败 |
| [test_kubernetes_pod_spec.py](../../../control_plane/tests/test_kubernetes_pod_spec.py) | 纯构建、严格校验、安全/资源/卷、凭据排除、输出独立及 get/delete 兼容 |
| [test_app.py](../../../control_plane/tests/test_app.py) | 默认 Docker、健康路由、Service 委托和应用装配回归 |
| [test_kubernetes_runtime_integration.py](../../../control_plane/tests/test_kubernetes_runtime_integration.py) | 显式目标的真实 Runtime ping 与认证 HTTP 就绪链路 |
| [test_kubernetes_query_integration.py](../../../control_plane/tests/test_kubernetes_query_integration.py) | 双重开关下仅创建本次唯一 Pod，验证真实 get/Service 和 UID 前置条件清理 |
| [test_kubernetes_delete_integration.py](../../../control_plane/tests/test_kubernetes_delete_integration.py) | 双重开关下验证错误事务不删除、Runtime 精确删除、已删除/删除中的幂等行为；fixture 独立清理 |
| [test_kubernetes_agent_service_integration.py](../../../control_plane/tests/test_kubernetes_agent_service_integration.py) | 真实 ClusterIP/EndpointSlice 原 UID 与就绪后端、跨节点正面对照和拒绝、三资源删除及缺失镜像回滚 |
| [test_kubernetes_network_integration.py](../../../control_plane/tests/test_kubernetes_network_integration.py) | 数字 Pod IP 跨节点正面对照、入/出站及 sandbox 间隔离、readiness 与三资源清理 |
| [test_kubernetes_create_integration.py](../../../control_plane/tests/test_kubernetes_create_integration.py) | 真实 Runtime 创建/删除、ready、非 root/挂载/只读根及缺失镜像回滚；独立 fixture 清理 |
| [test_kubernetes_pod_spec_integration.py](../../../control_plane/tests/test_kubernetes_pod_spec_integration.py) | 双重开关的 server-side dry-run，确认 API 接受规格且没有持久化 Pod |

在 `control_plane/` 执行默认检查；使用 Fake Client，不要求集群运行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_agent_service.py tests/test_kubernetes_network.py tests/test_kubernetes_create.py tests/test_kubernetes_delete.py tests/test_kubernetes_query.py tests/test_kubernetes_pod_spec.py
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

`pod_spec.py` 写入新布局版本，创建流程保存策略 UID 并绑定到待提交 Pod。get 核验绑定与策略内容；PodDeletion 在有 Pod 时先核对绑定，再按 Pod → Service → 策略清理，回滚用 `delete_policy=False` 将 Pod 确认与原策略 receipt 的删除明确分开。无 Pod POST 的事务只确认 Pod 不存在，不能删除后来出现的冲突 Pod 来解锁策略清理。

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

缺失缓存镜像用例验证启动失败后 Pod、Service 和策略均无残留。fixture 保留独立 SDK 清理，记录 Pod/Service/策略原 UID，始终先确认 Pod 消失，再删 Service，最后删策略；只核验并清理本次完整归属的资源，尝试清理其他测试资源后统一报告残留，保留原失败信息。受影响显式回归包括 `test_kubernetes_create_integration.py`、`test_kubernetes_delete_integration.py`，历史单 Pod 用例保持原布局，不自动附加策略。

### Agent ClusterIP Service 实现与验收

`agent_service.py` 集中定义 `agent_service_name()`、纯构建函数 `build_agent_service()`、清理归属 `service_identity()`、路由完整性 `verify_agent_service()` 和借用 Core 客户端的 `AgentServiceResource`。Service 版本与 UID annotation 由 `pod_query.py` 严格识别；`pod_spec.py` 只写版本，创建事务在保存 Service 原 UID 后再绑定 Pod，不增加 NetworkPolicy 的 selector 标签。

`AgentServiceResource` 对所有同 sandbox 的 Service 角色执行一页 `limit=2` 发现，并按确定名称精确读取，拒绝重复、分页、外来同名对象及不支持布局。查询用完整性校验，删除只取得合法归属及原 UID 的清理权，不因 selector 被修改就放弃合法残留。DELETE 带 UID/resourceVersion 前置条件，单次请求；5 秒独立单调预算内每隔至多 0.1 秒确认消失，并确认 namespace 仍存在。所有 facade 只借用原 ApiClient，最终由所属工作线程关闭一次。

新布局预检三类对象后按策略、Service、Pod 创建；回滚使用 `PodDeletion(..., delete_policy=False, delete_service=False)` 先确认 Pod 消失，再按保存的原 Service/策略 UID 清理。Service 失联、被替换或无法确认删除时保留策略，Pod 不明时两者都保留。旧单 Pod 和 Pod+策略仍按各自版本核验，无自动补建。

离线聚焦命令现在包括 Service：

```bash
uv run --locked pytest -q tests/test_kubernetes_agent_service.py tests/test_kubernetes_network.py tests/test_kubernetes_create.py tests/test_kubernetes_delete.py tests/test_kubernetes_query.py
```

显式真实验收只复用已有 context/namespace、已缓存 basic 镜像，不拉取或构建，不访问公网：

```bash
RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_agent_service_integration.py
```

用例创建随机三资源身份，通过借用维护 ApiClient 的 DiscoveryV1Api 只读检查控制器生成的 EndpointSlice：原 Service owner UID、原 Pod target UID/IP、ready 后端以及 TCP 8080。跨节点普通 Agent Pod 先访问另一对照 Pod 成功，再通过数字 ClusterIP 验证受测 Agent 被拒绝，之后复核正面对照与就绪 EndpointSlice，避免把无后端当作隔离成功。没有添加策略例外或通过 port-forward 访问。

正常删除和缺失缓存镜像失败回滚都要求三类精确资源 404。fixture 独立记录每种资源的原 UID，按 Pod → Service → NetworkPolicy 兜底清理；遇到归属/UID 冲突报告残留，不删除替代对象，也不依赖被测试的 Runtime 方法作为唯一清理路径。受影响显式回归为网络、创建与删除集成；历史资源测试保持原布局。实际结果与未验证项见开发进度，不将本地 FakeClient 成功代替真实网络验收。

### 独立认证代理与固定 IPv4 上游

以下独立实验仍用于单独验证代理；可选自动 Runtime 生命周期见后文。缺省仍为三资源。`proxy_spec.py` 从有完整身份与业务时间的 Agent Pod 派生：

- `build_proxy_secret(pod, access_token=...)`：不可变 Opaque Secret，只保存 `access-token`；Base64 是 Kubernetes 数据编码，不是加密。
- `build_proxy_pod(pod, service, proxy_image=...)`：先复用 `verify_agent_service()` 核验 Service 归属、原 UID 绑定、端口与 selector，再要求其 ClusterIP 为规范 IPv4；Token 只经 Secret 引用注入。固定非 root UID/GID/fsGroup 101、只读根、禁止提权、drop ALL、RuntimeDefault seccomp，CPU 100m/内存 64Mi，唯一挂载为 64Mi 内存 `/tmp`。没有 ServiceAccount token、工作区、宿主挂载或外部端口。
- `build_proxy_network_policies(pod)`：Agent ingress 只允许同 namespace、sandbox、事务和 `resource=proxy` 的 TCP 8080；代理 ingress 默认拒绝、egress 只允许对应 Agent TCP 8080。原默认拒绝策略不变，测试客户端规则由 fixture 独立提供。

代理仍使用 `deploy/local/Dockerfile.proxy`。新增镜像能力标记 `io.agent-sandbox.proxy-upstream-version=1`，测试使用独立标签 `agent-sandbox-proxy-upstream-test:dev`。启动时缺省沿用 Docker `sandbox:8080` 和 resolver；显式设置 `SANDBOX_AGENT_UPSTREAM_IPV4` 时仅接受四段规范十进制 IPv4，拒绝空值、前导零、URL、端口、空白/控制字符和配置注入。Kubernetes 模式固定上游 8080，不渲染 Docker DNS，暂时对 `/services/` 返回 400，并将上游建连超时设为 2 秒；Docker 模式继续继承原超时。

`envsubst` 只替换 Token 以及脚本内部派生的固定 authority、服务主机、resolver、路由关闭和连接超时变量；同名外部环境值不能覆盖派生结果。Nginx 自身变量保留，Token 在配置校验与 exec 前 unset；私有目录 700、配置 600，任何启动错误只输出固定文案。配置含秘密，不执行 `nginx -T`，也不将配置或 Secret 输出到日志。实例认证、精确匿名 GET `/healthz`、认证头移除、文件流式上传及 SSE 保持原有实现。

离线测试运行真实 shell，使用临时工具替身记录渲染、权限和继承环境，不访问 Docker 或 Kubernetes：

```bash
uv run --locked pytest -q tests/test_kubernetes_proxy.py tests/test_proxy_upstream.py tests/test_kubernetes_agent_service.py tests/test_kubernetes_network.py
```

在仓库根构建并加载唯一测试标签，复用已有集群，不覆盖当前代理或 Agent 镜像：

```bash
docker build -f deploy/local/Dockerfile.proxy -t agent-sandbox-proxy-upstream-test:dev deploy/local
minikube -p agent-sandbox-k8s image load agent-sandbox-proxy-upstream-test:dev
```

确认已有节点和 namespace 后，在 `control_plane/` 显式执行：

```bash
RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_proxy_integration.py
```

测试使用实际 Pod 网络：客户端 Pod 内的有界标准库 HTTP 请求通过代理 Pod IP、Agent ClusterIP、Agent Pod，凭据只在测试 exec 的 stdin 中传递。主用例由现有 Runtime 创建未修改的 basic Agent、Service 和默认拒绝策略，fixture 独立管理 Secret、代理、精确放行和对照客户端。验证缺失/错误 Token 401 且无副作用、正确命令、二进制文件往返和代表性 SSE，普通 Pod 直连 Agent 与其他身份代理访问被拒绝，代理自身也不能连接可达的普通对照目标，移除精确 UID 的放行规则后网关失败；用保持可达的普通目标作为正面对照，不将 DNS 或控制通道失败当作隔离成功。Kubernetes 模式不支持用户应用端口，Service 本身也不构成外部 endpoint。

认证头观测是单独的测试仪器：可信 Python 测试镜像中的 Agent 仅在该用例监听 loopback 8081，由 fixture 启动最多 120 秒的 8080 转发记录器，记录 header 是否存在及普通头是否保留的布尔值，再转发给真实 Agent。生产 Agent、Runtime 构建器及协议不改动，主链路用例不依赖该仪器。测试不回传或记录凭据原文，并检查用户任务环境不含平台凭据。

fixture 在 POST 前登记唯一名称与完整归属，保留原 UID；独立清理时先确认额外 Pod 消失，再清理 Secret/测试策略，Pod 清理不明时保留其余资源并报告名称与 UID。移除额外代理后，原 Runtime 三资源由原 fixture 独立兜底清理。没有按标签批量删、强删或开发数据库操作。该独立实验的资源仍由 fixture 管理；自动布局的八类受管资源由后文 Runtime 生命周期测试负责验证。

同一测试镜像的 Docker 默认模式回归采用仅在测试进程内覆盖镜像常量的命令，不修改源码默认值或任何原镜像标签。在 `control_plane/` 实际执行：

```bash
PYTHONPATH=.. uv run --locked python - <<'PYTEST'
import os
import pytest
import control_plane.runtime.docker.transaction as transaction

os.environ["RUN_DOCKER_INTEGRATION"] = "1"
transaction.PROXY_IMAGE = "agent-sandbox-proxy-upstream-test:dev"
raise SystemExit(pytest.main([
    "-q", "tests/test_endpoint_auth_integration.py",
    "tests/test_service_endpoint_integration.py",
]))
PYTEST
```

这只验证本次独立镜像在 Docker 默认上游模式下的既有认证、命令/文件/SSE 和应用端口行为；不替换开发代理，不是完整 Docker 回归。Runtime 现已可选管理 Secret、代理 Pod 与最小放行规则；现有实例的 WSL endpoint 已有显式会话交付，通用宿主入口、持久工作区及正式 Service/API 创建接入仍未实现。

### 八资源 Runtime 生命周期与历史兼容

在构造 `KubernetesSandboxRuntime` 时显式设置 `proxy_image="agent-sandbox-proxy-upstream-test:dev"` 启用；未配置保持原行为。构造只校验可信参数，创建纯规格阶段校验 `spec.access_token` 并标记预期布局，不读取 kubeconfig 或连接集群。

`proxy_resources.py` 集中处理按版本闭集角色发现、原 UID 记录、完整性查询、两 Pod 顺序清理和五类伴随资源的创建补偿。它只借用当前工作线程的 CoreV1Api/NetworkingV1Api，不创建或关闭第二个 ApiClient。`PodCreation` 仍管理原三资源，在任何变更前执行完整预检，Agent running 后调用 `ProxyCreationResources.create()`；失败先 `stop_proxy()`，原 Agent 消失后 `cleanup_companions()`，最后清理原 Service/策略。代理 running 后还会重新确认 Agent 的运行事实，若此时已退出/失败则补偿。迟到成功取消也携带原 UID 重新开客户端补偿，不让线程或资源无人等待。

`ProxyResources` 的闭集为两 Pod、三 NetworkPolicy、两 Service、一个 Secret；SDK 列表分别 limit=3/4/3/2，watch=False，不跟随分页。list-level 404 不是实例消失，而是配置/namespace 故障。所有候选与确定名称读取必须一致；未知角色、重复角色、冲突事务或同名外来对象均拒绝，不按标签批量删除。

`proxy-resource-uids` 按 Runtime annotation 版本解析：历史版本 1 仅七键/512 字符，新版本 2 增加 proxy_service 原 UID，仅八键/768 字符，均要求无重复键和规范 UUID。查询必须具备该版本完整集合，不按当前 proxy_image 配置推测布局，不通过删除字段或未知版本降级。逐步发布后，每个存活资源可作为后续重试的 UID 依据；UID 或 resourceVersion 测试失败不会刷新身份强行接管。未完成自身记录发布的对象只能凭其他有效记录中已持久化的准确 UID 清理，不能因缺少记录接管同名替代物。查询还核验所有原始时间、预期 Token 摘要、可信镜像以及代理完整规格/网络规则。Secret 对象诊断表示经过隐藏，公开查询与错误不含 Token 或摘要。删除不解码凭据，不要求各资源凭据摘要健康或相同，也不依赖 readiness 或当前镜像注册，按 proxy → agent → secret → proxy_service → service → ingress → isolation → policy 确认消失，配置完整性与清理归属分开。

`proxy_service.py` 集中实现 Proxy Service 纯规格和身份/路由核验，`agent_service.py` 仅提取共用的 ClusterIP 路由检查。共享 Service 发现明确接受 agent-service/proxy-service 两角色，在选择 Agent 前先核验全部候选，拒绝未知/重复角色及事务冲突；不会简单放宽数量后任取首项。创建在代理 Pod 之前单次提交 Proxy Service，记录和发布原 UID；回滚与删除仍在两 Pod 确认消失后才清理 Service，Pod 不明时保留隔离策略。仅剩 Proxy Service 的完整原 UID 记录也能授权精确重试清理。

读取/伴随资源确认与响应丢失核对各使用 5 秒单调预算，Pod 优雅删除各有独立 45 秒预算，SDK 连接/读取阶段至多 0.5 秒；不是强行终止线程的硬总 deadline。启动共享既有 30 秒预算，取消只通知停止推进并等待在途请求和必要补偿；关闭异常不覆盖原创建/清理证据。

在 `control_plane/` 执行默认离线检查：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_proxy_service.py tests/test_proxy_service_spec.py tests/test_kubernetes_proxy_lifecycle.py tests/test_kubernetes_proxy.py tests/test_kubernetes_create.py tests/test_kubernetes_delete.py tests/test_kubernetes_query.py tests/test_kubernetes_network.py tests/test_kubernetes_agent_service.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests
```

`test_proxy_resources.py` 补充 Agent 在代理启动时退出、部分记录发布后残留再清理及损坏摘要的查询/删除区别。测试使用临时 FakeClient、可控事件与单调时钟，覆盖失败阶段、POST 响应丢失、元数据 CAS 失败、重复取消、代理启动失败、Secret/规则/记录篡改、Pod 残留时保留依赖及新 Runtime 清理部分资源。历史替身现在按准确名称返回对象，以验证双 Pod/多策略发现，而非放宽生产数量/归属规则。

确认原 Minikube 三节点和 namespace 以及已缓存的独立代理、basic/Python 镜像后，显式执行：

```bash
RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_proxy_service_integration.py tests/test_kubernetes_proxy_lifecycle_integration.py tests/test_kubernetes_create_integration.py tests/test_kubernetes_delete_integration.py
```

该 fixture 不代建八类受管对象，只记录 Runtime 的 POST 及原 UID；自己创建的客户端和访问规则使用独立身份，不混入 Runtime 角色发现。新增 Proxy Service 用例有界读取控制器生成的 EndpointSlice，核验 Service owner UID、原代理 Pod UID 与 ready 后端；经 ClusterIP 验证正确/错误 Token、命令、二进制文件及 SSE。未获放行客户端有普通上游可达性正面对照，仍不能访问该 Service，跨实例边界保持；删除后检查关联 EndpointSlice 自动消失。主生命周期用例通过实际 Pod 网络验证认证、命令、文件、代表性 SSE、直连/跨实例/额外出站拒绝，并用未配置 proxy_image 的新 Runtime 查询和删除。缺失缓存代理镜像用例保持 imagePullPolicy=Never，验证启动失败及八类资源回滚；部分资源用例移除原 Agent 后由新 Runtime 清理其余资源。

fixture 保留独立官方 SDK 兜底清理，准确核验完整归属、业务时间、原 UID 与 resourceVersion；先 proxy、Agent 后其余依赖，Pod 清理失败时保留限制策略。正常删除、失败回滚和兜底之后均核验本次名称无残留，不使用 Runtime.delete 作为唯一清理手段。受影响显式回归还包括创建、删除、网络及 Agent Service 集成；实际执行项与未执行边界记录在 PROJECT_PROGRESS.md。

## 排查原则

| 观察 | 优先检查 |
| --- | --- |
| `/healthz` 成功、`/readyz` 为 401 | 调用方是否向控制面发送了正确管理员 Bearer key |
| `KubernetesConfigurationError` | 显式文件路径、context、证书/CA 文件与支持的静态认证方式 |
| `KubernetesPermissionDenied` | 所选身份是否具备当前操作所需的 Pod list/get/delete 和目标 namespace get 权限 |
| `KubernetesTimeout` | 指定 API 的连接和读取是否可在当前阶段预算内完成 |
| `KubernetesConnectionFailed` | 指定 API 的可达性、TLS 校验与客户端收尾 |
| `KubernetesOperationUnsupported` | 调用是否超出 basic 主 Pod + Agent Service + 拒绝策略的实现范围 |
| `SandboxNotFound` | ID 格式、该 namespace 是否有匹配主 Pod、发现后的目标是否已消失 |
| `SandboxQueryFailed` | 唯一性、UID/归属、必需 annotations、单 Agent 结构、probe 和 Pod 状态是否完整一致 |
| `SandboxDeleteConflict` | 归属/绑定、单 Pod 布局、替代 UID 或 DELETE resourceVersion 前置条件是否冲突；不要自动接管新对象 |
| `SandboxDeleteFailed` | 原对象是否仍在优雅终止或被 finalizer 阻塞；保留记录，不强删或提前清理元数据 |

根 HTTP 接口统一隐藏内部失败分类，不应通过输出原始 SDK 异常、kubeconfig、证书或认证头来排查。不要自动切换 context、关闭 TLS 验证、改用其他凭据或创建额外资源来掩盖探测失败。

## 独立本地入口会话

`local_entry_target.py` 提供 `resolve_local_entry_target(runtime, sandbox_id)`，使用已有 `_read_primary_pod()`、`ProxyResources.query()` 与 UID 记录，完整验证 Agent、Proxy Service/Pod、Secret 和策略，再返回不可变的 `LocalEntryTarget`。该快照只包含 ID、原事务、namespace、确定名称、原 UID 集合及业务时间，不包含 Token 或摘要。旧七资源、未就绪或身份/路由损坏的实例拒绝转发。客户端仍在所属线程创建和关闭，不访问 SQLite。

`KubernetesLocalEntry(runtime, sandbox_id)` 提供 `await open()`、`await close()` 与 `async with`，以及 `url`、`is_active`、无凭据的 `target`。同一会话在同一事件循环中使用；构造不打开文件、客户端或进程。可信测试可注入 `process_factory` 和 `target_resolver`，生产默认分别使用 `asyncio.create_subprocess_exec` 与完整资源解析。注入快照仍须校验固定名称、事务、namespace、版本和闭集 UID。

启动参数固定为 kubectl、显式 kubeconfig/context/namespace、自有 `--cache-dir`、`port-forward --address=127.0.0.1 --pod-running-timeout=5s pod/<原代理名> :8080`。不使用 shell、Service 自动选 Pod、本地预占端口或环境代理。进程环境仅包含可信 PATH 和固定 locale。kubectl 正规的 `Forwarding from 127.0.0.1:<port> -> 8080` 表示监听已建立；严格检查数字端口后，在交付 URL 前重新解析并比对全部原身份。任何异常都进入自有句柄回收，不交付临时地址。

启动预算为 10 秒；SDK 和已启动进程工厂在取消/超时后仍可能延后返回，必须 settle 后清理迟到句柄，所以这不是整个调用必然在 10 秒结束的硬期限。每 2 秒只读复核目标，空闲 kubectl 不退进程也能发现目标消失或替换；失效时不自动重连。关闭先 TERM，等待至多 2 秒，必要时 KILL 再等待至多 2 秒；确认 reap 后停止管道消费者、删除私有缓存。未确认时返回固定清理失败，保留句柄供显式重试。重复取消不能中断关键等待，关闭只影响自己的子进程，不删除任何 Kubernetes 资源。

stdout/stderr 持续按 4096 字节读取，单行解析片段最多 512 字节；超长行丢弃直至换行，不长期保存日志。异常只采用固定类别，例如 `kubectl_unavailable`、`local_entry_permission_denied`、`local_entry_timeout`、`local_entry_protocol_error`、`local_entry_target_changed`、`local_entry_unavailable` 与 `local_entry_cleanup_failed`。目标查询仍保留现有固定 Kubernetes 领域错误。启动失败或关闭失败均不打印 kubectl 原文、kubeconfig 或凭据。

### 独立使用

仓库根目录执行下列示例，ID 必须来自已存在且整体 ready 的八资源实例：

```bash
uv run --project control_plane --locked python -m examples.kubernetes_local_entry \
  --kubeconfig "$HOME/.kube/config" \
  --context agent-sandbox-k8s \
  --namespace agent-sandbox-test \
  sbx_<实际32位小写十六进制ID>
```

[示例源码](../../../examples/kubernetes_local_entry.py) 只输出 loopback 地址并持有会话，Ctrl+C 关闭自有转发，不生成或打印 Token。调用方另行持有创建时的实例 Access Token，HTTP 请求仍向代理发送该实例 Bearer；不能替换成管理员 Key。地址只在会话活跃时有效；此独立用法本身不接入应用；应用 lifespan 与已有 HTTP endpoint 的显式装配见下节，SDK endpoint 契约不变。

### 验证命令与边界

在 `control_plane/` 执行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_local_entry.py tests/test_kubernetes_proxy_service.py tests/test_kubernetes_proxy_lifecycle.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests
```

确认已有 kubectl/Minikube/namespace 与缓存镜像后，显式执行：

```bash
RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_local_entry_integration.py
```

默认测试只使用 FakeClient/进程工厂和可控事件，不启动 kubectl 或访问集群。集成 fixture 通过 Runtime 创建八资源，不添加客户端网络例外；使用真实 WSL HTTP 验证缺失/错误 Token、命令、二进制文件及 SSE，关闭后独立检查监听消失且实例仍在；原 Pod 删除后验证会话失效，同名替代 Pod 也不被接管。进程由会话 finally 兜底关闭，Kubernetes 对象由独立 registry 按完整归属和原 UID 清理。port-forward 是 Kubernetes API 通道，不作为 Pod/ClusterIP 网络放行证据；Windows、远程访问、宿主入口映射与公共 endpoint 交付仍未验收。

## 应用会话管理与 HTTP endpoint

`KubernetesLocalEntryManager(runtime, max_sessions=8)` 提供 `open(id)`、`lookup(id)`、`close(id, expected_transaction_id=...)` 和 `close_all()`。open 以无 await 的检查/登记避免事件循环并发超额，所有请求等候同一启动任务，旧等待者不能接收后来重建的登记项。lookup 只读已有 ready 会话，重新执行目标解析，再在返回前核对登记项未关闭或被替换。close_all 永久停止新准入、汇总等待全部收尾，失败项继续保留；已关闭管理器不复活，应为下一应用使用新 Runtime/管理器。会话标识包含原事务、时间、UID、凭据摘要与认证版本，不包含 Token。

可信验证装配示例（`backend` 已显式配置 kubeconfig/context/namespace，`isolated_store` 已有匹配的原生命周期记录及凭据；不得使用实际开发库）：

```python
from control_plane.app import create_app
from control_plane.runtime.kubernetes.local_entry_manager import KubernetesLocalEntryManager

manager = KubernetesLocalEntryManager(backend, max_sessions=8)
app = create_app(
    runtime=backend, store=isolated_store, api_key=admin_key,
    reaper_enabled=False, pending_recovery_enabled=False,
    kubernetes_entry_manager=manager,
    kubernetes_initial_sandbox_ids=(existing_sandbox_id,),
    sandbox_query_timeout=10,
)
```

也可只传非空初始 ID，由应用装配一个惰性管理器；默认空 ID/无管理器不启用此能力。管理器与 Runtime 及应用的所有权必须匹配，显式注入不代表可以跨应用共用。入口在 lifespan 开启，未运行 lifespan 时 endpoint 查询不会暗中补开。这里显式增大验证应用的只读查询预算，不修改默认 Docker 的 1 秒设置；SDK 阶段超时和线程 settle 仍不等同于硬总 deadline。

HTTP 请求保持原路径 `/v1/sandboxes/<id>/endpoint`。Runtime 只给出非明文事实，Service 再核验隔离库的原事务、created_at 与 SHA-256 并附加原 Token；管理员 Key 不发送至实例。endpoint 无效或 Kubernetes 二次核验失败返回固定 503，旧实例或凭据损坏继续沿既有错误规则，所有 endpoint 响应 no-store。Kubernetes 服务端口 endpoint 仍拒绝，不能因为 Nginx 镜像有能力就使用该路由。

Runtime 删除通过 `delete_guard` 阻止同 ID 的 open/lookup，在原事务会话已关闭后才开启 SDK 删除；外层关键任务和 settle 将保护延续到重复取消、线程返回及清理。lifespan 在 reaper 停止后调用 close_all，等待回收才释放 SQLite 锁。失败时管理器和 `app.state.kubernetes_entry_run_lock` 保留，不能宣称应用关闭完成；可信调用方须先重试 close_all 确认关闭，再处理保留的运行锁，不可删除锁文件或启动另一应用绕过它。

在 control_plane/ 执行默认聚焦与完整回归：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_local_entry_manager.py tests/test_kubernetes_endpoint.py tests/test_kubernetes_entry_lifespan.py tests/test_kubernetes_local_entry.py tests/test_kubernetes_proxy_service.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests
```

真实交付集成沿用双重开关和明确目标：

```bash
RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_endpoint_integration.py
```

用例通过 Runtime 创建本次八资源，临时 SQLite 保存匹配记录和凭据，lifespan 建立转发；验证管理员拒绝、原 Token/no-store、重复查询地址复用、命令/二进制文件/SSE、应用退出后实例保留、原目标失效后查询拒绝，以及 Runtime 删除时原会话关闭。fixture 独立按原 UID 兜底清理。未使用开发库或对原 namespace/插件/工作负载作配置修改；本地 port-forward 不代表 NetworkPolicy 放行。

## Service basic 临时工作区创建闭环

`SandboxService.create()` 无需判断后端类型。原私有资源补偿保持 `delete_workspace=True`；Kubernetes Runtime 现在先校验其为 bool，并继续对 `expected_workspace` 拒绝，再进入原会话删除协调和精确资源清理。emptyDir 随 Pod 删除，两个 bool 值不产生额外存储 API 调用。`pod_delete.verify_temporary_volumes()` 供单 Pod 和代理布局共同使用，拒绝 PVC、hostPath、混合卷源及其他非 emptyDir 来源；闭集角色、布局版本、原 UID/RV 与事务核验保留。

最小复现使用真实临时 SQLite：Service 已持久化 pending 和原凭据、Runtime 已完成八资源，激活时另一受管 CAS 操作已删除该记录；Service 正常进入原资源补偿。修复前被 KubernetesOperationUnsupported 阻塞并报告 SandboxCleanupFailed，修复后原资源清理完成，调用方收到原本的 SandboxMetadataConflict；记录虽已由并发 CAS 删除，最终原资源也被确认清理。其他测试注入的激活冲突、提交结果不明和晚到成功均使用真实 SQL 事务或明确异步边界，不伪造 active 成功。

在 control_plane/ 执行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_service_create.py tests/test_kubernetes_delete.py tests/test_kubernetes_proxy_lifecycle.py tests/test_kubernetes_endpoint.py tests/test_kubernetes_local_entry_manager.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests
```

显式真实集成：

```bash
RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_service_create_integration.py tests/test_kubernetes_endpoint_integration.py tests/test_kubernetes_proxy_lifecycle_integration.py
```

新用例不采用 fixture 预生成的规格，而由应用的真实 Service 生成 ID/事务/时间/Token，仅观察 pending/凭据已持久化后才交给 Runtime。成功用例验证 active、查询期限、ready 后显式开会话、现有 HTTP endpoint 的凭据及命令/二进制文件/SSE，分别用默认删除与 delete_workspace=True 经 Service 删除。取消用例在实际 activate 提交后暂停其返回，重复取消 Service 请求，再等待原事务资源、凭据、记录和创建额度全部清理；fixture 仍保留独立归属/原 UID 兜底，不能把补偿失败算作成功。

本轮不引入 PVC/PV、独立工作区、克隆/快照或数据保留，不改变默认 Docker 或 schema。当前是可信本地装配下的 basic 临时工作区 Service 闭环，正式生产创建入口配置、持久工作区与其补偿仍待规划；未将 emptyDir 的删除兼容解释为持久存储能力。

## 独立私有 PVC 基础

`private_workspace.py` 尚不由 Runtime 调用。`build_private_workspace(spec, namespace=..., storage_class=...)` 返回新的 V1PersistentVolumeClaim；`PrivateWorkspaceResource(api, spec, namespace=..., storage_class=...)` 只保存借用的 CoreV1Api 与复制后的规格，构造不执行 I/O。辅助对象按单一 worker 顺序使用，不承诺并发操作同一辅助对象；调用方负责自己的 ApiClient、线程和取消收尾，不能在 SDK 线程仍执行时从另一线程关闭客户端。

受管调用顺序如下，`api` 必须来自调用方的显式集群配置，消费者的创建与停止由调用方负责：

```python
claim = PrivateWorkspaceResource(
    api, spec, namespace="agent-sandbox-test",
    storage_class="agent-sandbox-workspace",
)
created = claim.create()  # Pending 也是成功，不等待首个消费者出现前的绑定
observed = claim.inspect(expected_uid=created.uid)
# 调用方创建消费者、使用数据，最终删除消费者并确认其消失。
claim.delete(expected_uid=created.uid)
```

`inspect` 返回冻结的 `PrivateWorkspaceObservation(uid, phase, volume_name)` 或已确认不存在时的 None。phase 只接受 Pending/Bound；异常状态、规格修改及身份冲突明确失败。Bound 接受 Kubernetes 控制器分配的卷名，检查报告的访问模式与至少 1Gi 容量，不直接操作 PV。PVC 保护 finalizer、绑定及 selected-node 等 Kubernetes 自有元数据可正常出现；平台命名空间内的未知版本/身份标记不放行。`Unused=True/False` 是 v1.37 的正常条件，不能作为删除授权；仍须做消费者扫描。

所有请求设置至多各 0.5 秒的连接/读取超时，普通操作单调预算 5 秒，POST 不明结果另有 5 秒确认预算，删除总预算 30 秒，轮询间隔最多 0.1 秒。SDK 阶段超时不等于严格整个操作硬期限。候选查询最多两条，只允许一个当前私有 PVC，无分页；消费者扫描最多 100 个 Pod，使用 limit=101 检测溢出，遇到分页/超限保守拒绝，不能漏掉无平台标签的消费者。对象不存在时还需确认 namespace 可读，因而调用方权限须包含 namespace 读取。

每个辅助对象最多尝试一次 POST。`submitted` 在未知响应时保持 True；`uid` 仅保存正向核验的原身份，失败携带 `CreationFailureInfo`。UNCHANGED 表示尚未提交或被明确拒绝；REMAINING 需要本次正向残留观察，曾经拿到 UID 但之后无法确认时仍是 UNKNOWN；后续显式删除确认完成后可保留 ROLLED_BACK 证据。首次或重复调用不会抹去原 UID，也不会重发 POST。该对象不自动回滚已存在数据，不将一次 404 当作未创建的证据。

删除前消费者必须全部移除，包括仍保留的终态 Pod 对象。删除带原 UID/resourceVersion，不设零宽限期、不移除 finalizer；若已在删除中只确认结果。健康异常与删除身份核验分开，但未知卷规格、绑定和归属异常仍拒绝。确认失败保留错误及原始身份，不将 PVC 删除响应等同于底层 PV 回收或安全擦除。

离线与真实验证分别运行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_private_workspace.py tests/test_kubernetes_service_create.py tests/test_kubernetes_delete.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests

RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_private_workspace_integration.py
```

集成 fixture 首先只读核对既有 StorageClass 的 hostpath.csi.k8s.io、WaitForFirstConsumer 和 Delete；选取已有缓存 basic 镜像的 Ready 节点，以 hostname nodeSelector 创建非 root UID/GID 1000、fsGroup 1000 的受控消费者，不用 nodeName 绕过调度器。第一个消费者写入文件并退出，删除其 Pod 后保留原 PVC，再由同节点第二个消费者读取、修改并验证原数据。最后确认两个消费者消失，经模块删除原 PVC，只观察原 claimRef/PV UID 对应的 PV 被控制器回收，不直接删除 PV。

默认或缺少任一开关时在加载 kubeconfig/创建客户端前跳过。独立兜底按完整归属与原 UID 删除本次消费者和 PVC，消费者清理失败时保留存储并报告残留；不操作开发库、既有存储或集群插件。测试只证明当前 hostpath CSI 的同节点重新挂载，不证明 Runtime 已挂载 PVC、独立工作区互斥、跨节点恢复或磁盘硬配额。下一步是 PVC 挂载及 Runtime 生命周期接入。
