# Kubernetes Runtime 实现

## 独立工作区 PVC 规格基础（2026-11-13）

本阶段按已给定设计增加 `workspaces.py`，仅纯构建/纯校验，不接通 Runtime 工作区方法。采用 `WorkspaceRecord`，将合法 `wsp_<32hex>` 确定映射为 `agent-sandbox-wsp-<32hex>`；先复核既有 volume_name 与 ID 的关系，不把它当 PVC 名称，不接管私有卷。独立角色为 workspace，版本为 workspace-version=1；完整平台归属和独立工作区 ID/creation token 写入 labels，原 image_id、业务创建时间和版本写入 annotations，不写 sandbox/事务/期限或 ownerReference。

实施步骤：

1. 先用 `test_kubernetes_workspace_spec.py` 复现缺少构建/校验能力，并覆盖确定名称、独立身份、固定存储、原 UID 与无 I/O 边界。
2. 在一个模块中实现名称、规格和校验函数，复用已有私有 PVC 的纯 StorageClass/固定规格校验，保持两类资源身份独立。预期原 UID 已给定时严格比较；未给定只返回本次 UID 观察并明确标记未核验原 UID。错误使用固定的配置/参数/WorkspaceConflict 分类，不输出对象或输入值。
3. 运行聚焦组合、完整默认测试、编译与差异检查，记录结果。默认离线，本轮无需集群或 dry-run；不触碰开发数据库、现有资源和 Runtime 生命周期。

边界：Filesystem/ReadWriteOnce/1Gi，StorageClass 必须可信显式提供。创建规格没有预绑定 PV、selector、dataSource 或 ownerReference；已有 PVC 的合法 controller volumeName 只验证结构，不证明 PV 身份或健康。规格/身份校验不读取 status，不把 Pending、Bound 或健康条件视为归属证据；只凭 UID 初次观察不能证明未替换。后续仍需不可变镜像解析、PVC 创建/原 UID 持久记录、结果不明处理、挂载与持久占用释放，不宣称本阶段已实现可恢复创建、工作区复用或独占。


### 纯函数接口与验证

```python
from control_plane.runtime.kubernetes.workspaces import (
    workspace_pvc_name, build_workspace_pvc, verify_workspace_pvc,
)

# record 是调用方已有的合法 WorkspaceRecord；这一步不创建 PVC。
name = workspace_pvc_name(record.workspace_id)
body = build_workspace_pvc(record, namespace="agent-sandbox-test",
                           storage_class="agent-sandbox-workspace")
# 如调用方已有读取到的 PVC 对象及原 UID，可只做纯校验；本模块不负责读取。
facts = verify_workspace_pvc(observed_pvc, record, namespace="agent-sandbox-test",
                             storage_class="agent-sandbox-workspace", expected_uid=original_uid)
```

`body.metadata.name` 使用 ID 派生值，不使用 `record.volume_name` 定位。`WorkspaceRecord` 通过 replace 重新校验，复用原记录时间和摘要，不修补缺失字段。构建只有固定 PVC 规格，没有 Pod、ownerReference、PV 或数据源。校验复用 `private_workspace.verify_private_workspace_configuration` 的纯存储约束，独立执行 workspace 角色/版本/身份检查，不调用私有资源的 `_identity`、健康观察或受管操作。

已读取的 API 对象应有规范 UUID 与有界 resourceVersion；可选原 UID 不匹配时拒绝，不读取新对象或尝试接管。无原 UID 的观察明确返回 `original_uid_verified=False`。普通数据库持有者/状态/version 变化不重写独立归属。PVC status 完全不用于这项校验，任何健康结论、PV 绑定确认或独占结论都留给后续职责；未知平台标记、私有 PVC、替代身份、ownerReference 和扩大的存储配置仍拒绝。

验证在 `control_plane/` 执行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_workspace_spec.py tests/test_kubernetes_private_workspace.py tests/test_kubernetes_persistent_workspace.py tests/test_workspace_reuse.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests
# 仓库根
git diff --check
```

本轮不需要 Minikube 或真实 PVC，不执行 server-side dry-run；离线 SDK 对象/纯函数验证不是 API Server 接受、实际创建、持久性或工作区复用验收。下一阶段为不可变镜像解析、独立 PVC 受管生命周期和原 UID 持久记录，再处理实例挂载与占用释放。实际测试结果见[开发进度](../../PROJECT_PROGRESS.md)。

## Python / Node 环境实施（2026-11-13）

本次复用原资源事务和预装 Agent 镜像，不新增语言服务。实施步骤：先离线复现可选配置未注册及 Pod 对语言环境的拒绝；再让 Runtime 复制冻结 basic/python/node 镜像映射，正式配置按实际映射注册 Service；最后验证完整 HTTP 语言执行及 Python+PVC。`agent_image` 保持 basic 来源，省略时可由映射提供 basic；显式重复值必须一致，否则拒绝，不能静默覆盖。未配置语言不注册，Runtime 本身也在提交前拒绝未知环境。

镜像语法在 Kubernetes 代码中本地验证，不调用 Docker 注册逻辑或联网解析；保留原启动、安全和 Never 拉取策略。Pod 构建仅扩展三种环境，独立工作区、域名出口及不可变镜像 ID 解析仍不支持。查询、检查、入口恢复和删除使用原资源身份，不依赖现注册表。真实验证只使用已有宿主镜像和现集群，先核对每个可能调度节点缓存，缺少时显式离线加载；测试默认不构建、不分发，不修改镜像标签或集群插件。

### 语言镜像准备

继续使用仓库已有 `sandbox_agent/Dockerfile`，basic、Python、Node 都预装同一 Go Agent，不新增语言执行服务。可信维护者可在确实需要构建时参考已有[镜像构建命令](../docker/IMPLEMENTATION.md#preset-images)，使用独立的项目语言镜像；官方 `python` / `node` 镜像本身没有 Agent，不是可直接注册的运行镜像。本次验收使用已有宿主与节点缓存，没有构建或拉取镜像。

在 WSL、已有 Minikube 下，先检查各节点缓存，仅对缺少的既有宿主镜像显式离线加载（不会在 HTTP 创建中执行）：

```bash
for node in agent-sandbox-k8s agent-sandbox-k8s-m02 agent-sandbox-k8s-m03; do
  minikube -p agent-sandbox-k8s ssh --node="$node" -- sudo crictl images
done
# 例如本次只有 Node 未缓存；--daemon 使用宿主镜像，不拉取、不覆盖现缓存。
minikube -p agent-sandbox-k8s image load --daemon --overwrite=false --pull=false agent-sandbox-node:dev
```

应再次逐节点确认 basic、已启用语言与代理镜像均可用；某个节点有镜像不代表其他节点已有。节点缺少镜像会进入已有启动失败与精确回滚，不能自动拉取或回退 basic。正式启动配置及 HTTP 创建命令见 [README](../../../README.md#可选wsl-本机-kubernetes-后端)。`workspace_storage_class` 可同样用于 Python/Node，`workspace_id` 独立复用仍不支持。环境移除只影响后续创建；原资源的检查和删除不读取当前镜像映射。

新增 `test_kubernetes_environments.py` 覆盖配置组合、冻结/矛盾值/非法引用、HTTP 准入、相同 Pod 安全规格、真实 Service+临时 SQLite+FakeClient/会话管理、历史映射移除、镜像失败、入口失败和重复取消的私有 PVC 补偿。真实 `test_kubernetes_environments_integration.py` 双重显式开关下，通过正式配置与 HTTP 验证 basic、Python、Node 和 Python+PVC；不手工 open 或写 active，fixture 仅记录原 UID 并独立兜底。

```bash
# control_plane/，默认保持离线
uv run --locked pytest -q tests/test_kubernetes_environments.py tests/test_backend_configuration.py tests/test_kubernetes_pod_spec.py tests/test_kubernetes_auto_entry.py tests/test_kubernetes_persistent_workspace.py tests/test_environment_registry.py
RUN_KUBERNETES_INTEGRATION=1 \
RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
uv run --locked pytest -q tests/test_kubernetes_environments_integration.py
```

## 新建实例自动入口实施（2026-11-13）

正式配置启用明确的 `prepare_entry_on_create` 模式，应用即使没有初始 ID 也创建并持有管理器；显式注入 Runtime 缺省仍关闭。Service 不感知 Kubernetes 会话。实施已先离线复现缺少管理器/创建后入口的问题，再接入原资源就绪轮询和一次入口准备，最后用真实 HTTP 与临时 SQLite 验证八/九资源闭环。

Runtime 在原资源事务完成后核对原 UID、事务、创建时间和认证事实，在有限单调预算内只读等待整体 ready，再原子申请已有会话额度并打开一次入口；复核会话仍指向原实例后才返回，Service 随后激活。GET endpoint 仍只查询，不重连。入口失败/取消先等待启动任务及会话关闭，再用原 PodCreation UID 证据回滚，私有 PVC 按创建失败删除；未确认收尾则保留 UNKNOWN/REMAINING、pending 和凭据。管理器在应用退出时停止新准入，等待在途创建准备及已有会话收尾，避免 RunLock 提前释放。

重点验证顺序、容量竞争、身份替换、未就绪/超时、迟到成功及重复取消、关闭失败、资源清理失败和激活不明；不重发 POST、不重试转发、不接受替代目标，不改变 Docker 或历史注入模式。真实集成必须由正式配置装配并走 HTTP 创建，不手工 open、写 active 或用 Service 创建替代。

`_create_resources()` 保留原资源创建线程、取消及迟到成功补偿，返回本次 PodCreation 原收据。自动模式在管理器 `creation_scope()` 中持有准备责任；`_created_entry_state()` 复用闭集发现、配置核验与状态解析，要求当前 UID 集合等于本次收据、事务/创建时间与认证摘要不变。PVC 合法 Pending 可继续有限等待，但不放宽最终 Bound/ready 要求。只有整体 ready 才进行一次 open；传入本次构建的 LocalEntryTarget，在 session factory 启动进程前核验，成功后 manager.lookup 再复核。

就绪阶段使用 30 秒单调预算和最多 0.1 秒间隔，原资源启动为 30 秒，转发会话自身启动为 10 秒；SDK 请求、解析复核、停止及回滚仍有各自预算和线程收尾，不能相加后宣称严格 HTTP 硬截止。客户端读取预算应明确加长，例如 120 秒；客户端超时并不证明没有创建，POST 不自动重试。管理器默认容量 8、可信构造范围 1～32，原子准入统计正在启动/尚未确认关闭的会话，独立于实例上限和 Service 创建并发。资源完成后额度不足会触发本事务补偿，既有其他会话不被驱逐。

自动阶段异常或取消后，先等待准备任务及其迟到结果，再在 delete_guard 内按原事务关闭入口；只有关闭确认后才使用原 PodCreation 回滚，包含本次私有 PVC。关闭失败返回固定 entry_cleanup/UNKNOWN 并保留句柄和资源。资源失败保留原 REMAINING/UNKNOWN；只有全部已确认清理才删除对应 pending/凭据。Service 激活冲突沿原补偿，提交不明保留当前事实；Runtime 不另行猜测或删除数据库记录。

close_all 先停止准入并等待 creation_scope 内的 Runtime 准备/补偿，再收尾所有会话，应用在关闭确认后释放 RunLock。默认 ASGI 服务正常关停仍先等待请求完成；没有周期发现、失效重连或孤立后台准备任务。新建成功不重置命令/实例期限，GET endpoint 依旧 lookup-only，之后 wait_ready 会实时重查。

在 `control_plane/` 执行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_auto_entry.py tests/test_kubernetes_service_create.py tests/test_kubernetes_local_entry_manager.py tests/test_kubernetes_entry_lifespan.py tests/test_kubernetes_pending_recovery.py tests/test_backend_configuration.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests

RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_auto_entry_integration.py
```

新增集成从正式环境配置装配应用，fixture 只提供记录 SDK POST 原 UID 的客户端和缓存节点调度约束，不代替创建资源。请求经 HTTP 原子预写 pending/Token，由自动入口返回后才 active；验证实例 readyz、管理员 no-store endpoint、正确/错误 Token、命令/文件/SSE、重复 GET 地址与进程复用，再经 HTTP 删除。私有 PVC 用显式 delete_workspace=true 清理，fixture 仍独立按完整归属/原 UID 兜底并观察 PV 回收。实际结果、初次失败试跑和未运行项见[开发进度](../../PROJECT_PROGRESS.md)。

## 正式后端配置实施（2026-11-13）

通过可信环境配置选择默认 Docker 或 Kubernetes，保留显式 Runtime 注入；不引入后端注册框架或新建实例后的自动入口。已按以下顺序实施：

1. 离线复现环境配置未生效、basic 子集无法注册及 Kubernetes 普通 API 错误未映射；使用 FakeClient/临时库，先确认测试失败。
2. 在 `runtime/configuration.py` 集中解析不可变配置、惰性构造选中后端；Kubernetes 要求显式路径、context、namespace 和两种镜像，默认 basic 并可选注册 python/node，拒绝自定义环境配置。Docker 继续沿原镜像映射。Service 接受合法非空有界子集，省略仍保留三种默认名。
3. app 只组合后端、环境、存储及既有生命周期；默认数据库按后端分离，可信 `AGENT_SANDBOX_DATABASE_PATH` 可覆盖，显式参数优先。不修改 schema 或迁移旧库，不根据配置切换自动接管旧记录。
4. 普通 Kubernetes 不可用错误固定为 503，未实现操作固定为 501；保留 endpoint、readyz 与 Docker 契约。回归认证、RunLock、会话和 pending 恢复；显式只读 Minikube HTTP 验证后，以临时库实际启动 Uvicorn 并检查关闭收尾。

重点边界：未选后端的无效配置不影响启动，注入对象不受后端环境变量重配；导入/构造无文件或网络 I/O；未注册环境在 pending 前拒绝；配置成功不意味着新建实例已有可用 endpoint。

`load_backend_configuration()` 在每次装配时只读取选中后端的值，形成冻结的 `BackendConfiguration`；`build_runtime()` 使用该快照，不再读取环境。缺省或显式 docker 不校验无关 K8s 字段。显式注入 Runtime 时完全跳过后端环境配置，沿注入对象实际类型派生环境；不修改它的镜像、集群或可选配置。构造器仍可用于只读/测试，不强制其启用代理；正式配置构造入口则必须提供两种镜像，手动创建不完整快照也不能降级成匿名布局。

| 可信配置 | 正式 Kubernetes 应用要求 |
| --- | --- |
| `AGENT_SANDBOX_RUNTIME` | 省略为 docker；只接受精确 docker/kubernetes，空值/未知值拒绝 |
| `AGENT_SANDBOX_K8S_KUBECONFIG` | 必填，单个明确文件路径；不读取 current-context 或合并其他文件 |
| `AGENT_SANDBOX_K8S_CONTEXT` | 必填，显式 context |
| `AGENT_SANDBOX_K8S_NAMESPACE` | 必填，已有 namespace，不自动创建 |
| `AGENT_SANDBOX_K8S_AGENT_IMAGE` | 必填，可信 Agent 镜像；使用节点缓存，Never 拉取策略 |
| `AGENT_SANDBOX_K8S_PYTHON_IMAGE` | 可省略；可信预装 Agent 的 Python 镜像，省略不注册、显式空值拒绝 |
| `AGENT_SANDBOX_K8S_NODE_IMAGE` | 可省略；可信预装 Agent 的 Node 镜像，省略不注册、显式空值拒绝 |
| `AGENT_SANDBOX_K8S_PROXY_IMAGE` | 必填，支持 IPv4 Service 上游及认证协议的可信代理镜像 |
| `AGENT_SANDBOX_K8S_WORKSPACE_STORAGE_CLASS` | 可省略，保持 emptyDir；显式提供时启用现有九资源私有 PVC |
| `AGENT_SANDBOX_DATABASE_PATH` | 可省略或指定本后端独立数据库；显式 `database_path` 或注入 Store 优先 |

提供的值不静默 trim，空值、控制字符及不符合现有 Runtime 值校验的配置在装配阶段拒绝；异常不包含原值。后续修改进程环境不重配已有应用，需显式重新装配。管理员 key 独立沿用既有机制，不进入该快照、Pod 或资源标签。运行时只支持静态证书 kubeconfig，不支持 exec/auth-provider、集群内认证或跳过证书验证；构造阶段不读证书，实际请求仍在原工作线程中按有限超时加载和关闭客户端。

Docker 环境保持 basic/python/node 与原自定义映射；Kubernetes 按实际冻结映射注册 basic 及可选 python/node，不调用 Docker 注册/镜像解析函数，非 None 的 `additional_environments`（包括空映射）明确拒绝。Service 接受 1～35 个合法名称输入并冻结，不要求全部 Docker 内置名；省略参数时仍为原三种。未注册环境在创建额度、身份与 pending 之前拒绝，客户端仍不能指定镜像、namespace、路径或 StorageClass。

应用的 Docker 默认库为 `.local/lifecycle.sqlite3`，Kubernetes 默认库为 `.local/kubernetes-lifecycle.sqlite3`，均相对 app.py 位置。后端切换不做数据库迁移、记录重新归属或资源自动接管；显式覆盖路径时由可信运维确保同库只对应原后端。不要将原 Docker 库直接交给 Kubernetes；需要临时验收时先 `export AGENT_SANDBOX_DATABASE_PATH="$(mktemp -d /tmp/agent-sandbox-k8s.XXXXXX)/lifecycle.sqlite3"`，停服后确认已无需要的记录再清理该自有目录。数据库包含实例凭据，不能提交或公开。

可执行 WSL 启动命令见 [README](../../../README.md#可选wsl-本机-kubernetes-后端)。默认模块应用可直接由 Uvicorn 启动，无需临时应用工厂；持有空管理器并仅为本次新创建实例自动准备入口，pending 恢复仍默认关闭。原显式初始入口列表/管理器、恢复参数、认证、RunLock 与 reaper 顺序不变；已有实例不自动发现或重连。创建返回仍为 started，GET 不启动转发，后续可用性需实时检查。

普通生命周期路由将 Kubernetes 领域错误固定为 503 `kubernetes_unavailable` / `Kubernetes runtime is unavailable`，未实现操作为 501 `runtime_operation_unsupported` / `runtime operation is not supported`。Service 续期原有 Docker 异常保留分支也纳入 Kubernetes 领域错误，避免在到达 HTTP 映射前被包装成普通查询 500。endpoint 保持既有 503 收敛规则，readyz 的状态及固定错误体不变，Docker 响应不变。

在 `control_plane/` 验证：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_backend_configuration.py tests/test_environment_registry.py tests/test_api_errors.py tests/test_app.py tests/test_kubernetes_health.py tests/test_kubernetes_entry_lifespan.py tests/test_kubernetes_pending_recovery.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests

RUN_KUBERNETES_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_app_configuration_integration.py
```

此新增集成只使用基础只读开关，不需要资源开关，不创建工作负载；通过新配置构造实际应用，Fake Store 避开开发库，匿名 health 和错误管理员凭据不创建客户端，正确凭据只执行实际 Kubernetes ping。另以新配置、动态 loopback 端口和临时 SQLite 启动真实 Uvicorn 子进程，验证认证、readyz、空列表及 SIGTERM 后锁和监听释放，不创建 sandbox。具体结果、初次验证问题和未运行项见[开发进度](../../PROJECT_PROGRESS.md)。

## 显式启动恢复实施（2026-11-12）

本次沿用已有恢复条件和资源核验，不新增恢复器或后端注册框架。启动顺序为认证 → SQLite RunLock → 存储初始化 → 显式初始入口 → 单轮 pending 恢复 → reaper → 接收请求。默认关闭恢复，不从数据库自动发现入口；八/九资源必须同时满足原身份、完整性、ready、有效入口、持久凭据和提交时未过期才能 CAS 激活。缺失条件只保留记录，不重建、修复或补偿删除资源。

实施已按以下步骤完成：

1. 先以真实 Service、临时 SQLite、Kubernetes Runtime/FakeClient 复现不可用分类和入口准备顺序，补八/九资源及拒绝条件测试。
2. Service 只接收固定领域分类，不导入具体 Runtime/SDK；将 Kubernetes 固定检查结果和连接/权限/配置/超时领域错误映射为 `kubernetes_unavailable`，保留 Docker 原结果及 CAS/期限复核。
3. 应用先准备显式初始会话再恢复；任何启动异常、取消或提交不明都沿既有收尾关闭应用自有会话，关闭未确认前保留 RunLock。测试批次、容量、重复取消及迟到激活。
4. 在既有 Minikube 用唯一资源和临时数据库构造资源完整而记录 pending 的中断状态，通过 lifespan 恢复并验证认证 endpoint；按原 UID 兜底清理，最后运行默认完整回归并记录结果。

重点验证：入口失败不能激活；资源完整不能替代 ready/凭据；提交响应丢失不能回滚或重复激活；关闭失败不能释放应用锁；后端故障和任意原始 reason 不能泄露到恢复结果或日志。

Service 仅依赖 core 中的 Kubernetes 固定领域错误，以及检查结果的封闭类别 `kubernetes_inspection_unavailable`，不导入具体 Runtime 或 SDK。检查不可用与之后 get 发生的配置、权限、连接、超时错误均返回 `kubernetes_unavailable`；Docker 原有不可用类别不变。两种后端不可用均停止单轮恢复并记录固定计数/错误，不因后端故障阻止 HTTP 启动；元数据不可用仍中止启动。已有 endpoint 对失效会话或目标复核失败统一返回 endpoint 不可用，恢复保持原 `skipped/sandbox_endpoint_unavailable` 语义。

可信应用工厂复用原注入方式，不新增默认后端切换配置：

```python
runtime = KubernetesSandboxRuntime(
    kubeconfig_path="/明确的/kubeconfig", context="agent-sandbox-k8s",
    namespace="agent-sandbox-test",
)
app = create_app(
    runtime=runtime, database_path="/可信临时目录/lifecycle.sqlite3",
    pending_recovery_enabled=True, pending_recovery_batch_limit=20,
    kubernetes_initial_sandbox_ids=(existing_sandbox_id,),
    reaper_enabled=False,
)
```

上述 ID 必须替换成同一数据库及现有八/九资源实例的真实 ID；不是用于新建实例的示例。管理员 key 仍由可信参数或进程环境提供，存储必须已具有原 pending 及凭据，不自动迁移开发库。不配置镜像/StorageClass 也可依据既有资源核验恢复；GET endpoint 仍只读取已有会话，不会补开入口。配置的入口打不开会导致启动失败，未配置入口的 pending 则被跳过。

新增 `tests/test_kubernetes_pending_recovery.py` 用真实 SQLite、Runtime 和会话管理器，仅替换 SDK I/O 与进程句柄。覆盖八/九资源、原身份/凭据不变、六类检查结果、就绪和期限门槛、数据库版本竞争、默认关闭、批次、容量、入口失败、激活结果不明、重复取消和关闭失败保留锁。已有 Docker 恢复与应用测试保持兼容。

在 `control_plane/` 执行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_pending_recovery.py tests/test_app_pending_recovery.py tests/test_kubernetes_pending_recovery.py tests/test_kubernetes_service_inspection.py tests/test_kubernetes_resource_inspection.py tests/test_kubernetes_entry_lifespan.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests

RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_pending_recovery_integration.py
```

真实集成创建本次未过期的唯一事务，先原子写 pending/凭据，再由 Runtime 完成资源，模拟激活前中断；新 Runtime、Store 和应用 lifespan 不依赖创建时镜像或存储配置，显式开入口并恢复原记录。对比恢复前后 UID/声明及凭据，验证管理员 endpoint 的 no-store、错误凭据拒绝、原实例 Token 下命令/文件/SSE，最后经 Service 删除、原 UID fixture 兜底并观察 PV 回收。初次试跑误用了旧查询 fixture 的历史时间和占位规格，已修正测试数据，未放宽生产期限或身份条件；实际结果见[开发进度](../../PROJECT_PROGRESS.md)。

## 只读资源检查（2026-11-12）

`KubernetesSandboxRuntime.inspect_resources()` 校验 sandbox/事务格式及未支持的独立工作区绑定，然后在线程中创建客户端并委托 `resource_inspection.py`；客户端由该工作线程关闭。首次或重复取消均等待必要线程收尾，关闭错误不覆盖已有 conflict/changed 证据；没有主要错误时关闭失败返回 unavailable。默认应用后端、数据库 schema、SDK、公共 API 和自动恢复装配没有变化。

`ResourceInspection` 借用 `ProxyResources` 的确定名称、明确角色集合及共享 5 秒单调请求预算。每次请求连接/读取阶段最多各 0.5 秒；预算在后续请求前检查，不是对 SDK 在途调用的严格硬截止。两轮分别先确认 namespace 可读，再有界列举 Pod、Service、NetworkPolicy、Secret、PVC 并精确读取每个确定名称。列表分页/截断不能解释为缺失；标签候选与精确读取不一致、两轮确认出现或替换时报告 changed。读取权限须覆盖这些资源和 namespace，不能靠当前配置省略某类发现。

对已发布的身份、时间、布局版本、UID 收据和配置做独立核验；缺少无矛盾的发布事实返回 partial。存在的错误 claimName、放宽策略、错误 selector、凭据引用或明确违反安全配置的声明仍返回 conflict，不能用另一资源缺失掩盖。完整代理布局复用从 `ProxyResources.query()` 分离的配置核验；普通 get 继续单独检查 PVC Bound 与两容器状态。PVC 请求规格核验和策略纯规格也各复用一处实现，不将 Lost、Resizing、未 Ready 等健康状态混入存在性判断。

公开检查角色通过 `InspectionResourceKind` 兼容扩展，区分两个 Service、三类策略、Secret 和私有 PVC；原 Docker `ResourceKind`、资源标签和检查行为不变。结果只包含 sandbox ID、分类、固定 reason 及角色集合，具体原因表见[设计](DESIGN.md#只读资源完整性检查)。尚未发布足够身份的布局不能确认 all_present；全部不存在时不猜测历史缺失角色。只剩原 PVC 是明确 partial，不触发自动清理。

Service 测试使用真实临时 SQLite、真实 Runtime 和 FakeClient，覆盖原 pending/凭据、只读检查及检查期间数据库状态、版本、事务、时间或记录身份变化。保留既有检查结束后的完整记录复核，不写入存储、不自动激活，不运行 pending 恢复。

在 `control_plane/` 执行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_resource_inspection.py tests/test_kubernetes_service_inspection.py tests/test_service_pending_inspection.py tests/test_kubernetes_persistent_workspace.py tests/test_kubernetes_proxy_lifecycle.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests

RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_resource_inspection_integration.py
```

真实检查测试独立创建八/九资源实例；检查过程借用禁止所有写操作的 SDK facade，核对检查前后原 UID 资源声明不变。依次验证完整布局、缺少 Agent Service、默认删除后原 PVC 保留、显式清理后的全部不存在；资源变化由 fixture 或显式 Runtime 删除制造，检查本身不修复。独立兜底按完整归属和原 UID 清理，关联 PV 仅观察控制器回收。受影响的回归另运行 `tests/test_kubernetes_endpoint_integration.py tests/test_kubernetes_proxy_lifecycle_integration.py`。实际结果见[开发进度](../../PROJECT_PROGRESS.md)，不能以离线通过代替集群验收。

## 私有 PVC 接入（2026-11-12）

本轮沿现有资源事务增量实现版本 3，不创建第二套编排器。可信 `workspace_storage_class` 仅与已注册环境的认证代理配置一起启用，缺省保留版本 2 的 emptyDir；九资源记录新增 workspace UID，StorageClass 与原 UID 同步发布到受管元数据。新布局按精确 claimName 挂载 /workspace，代理不挂卷；原有网络规则和 /tmp 保持。

实施已按以下顺序完成：先用离线测试固定配置、挂载和九资源身份；复用 PrivateWorkspaceResource 接入单次创建、响应不明确认和 Pod 后卷删除；再扩展版本化查询及八/九资源入口快照；最后验证真实 Service/临时 SQLite 的默认保留、显式删除和失败补偿。错误分类抽到 Kubernetes 内部共享模块，解除 PVC 对 Runtime 的反向导入。正常删除允许仅剩原 PVC，创建补偿必须把它纳入完整清理证据；消费者不明时不删卷。验收包含旧八资源回归与现有 Minikube 同节点数据保留，不扩展独立 workspace_id、API/schema、Docker 或 SDK。

本文对应当前 Runtime 探测、主 Pod 规格构建/查询/删除与 HTTP 就绪链路，说明代码位置、客户端装配、收尾和验证步骤。Runtime.create 已复用该规格实现三资源、八资源代理及九资源私有 PVC 创建闭环，但不表示正式 HTTP 创建接入可用。能力与信任边界见[设计说明](DESIGN.md)，接口响应见[控制面 API](../API.md#3-后端就绪检查默认-docker)，历史验收结果统一记录在[开发进度](../../PROJECT_PROGRESS.md)。

## 代码边界

| 位置 | 当前职责 |
| --- | --- |
| [runtime/kubernetes/kubernetes_runtime.py](../../../control_plane/runtime/kubernetes/kubernetes_runtime.py) | 配置校验、客户端工厂、探测/查询/删除异步入口、线程等待和未实现方法 |
| [runtime/kubernetes/pod_query.py](../../../control_plane/runtime/kubernetes/pod_query.py) | 纯 Pod 身份、时间、容器结构和状态解析，不调用 SDK 或数据库 |
| [runtime/kubernetes/pod_create.py](../../../control_plane/runtime/kubernetes/pod_create.py) | 策略 → Service → Pod 提交、启动等待、按版本聚合资源结果、原 UID 回滚与失败证据 |
| [runtime/kubernetes/pod_delete.py](../../../control_plane/runtime/kubernetes/pod_delete.py) | 单 Pod 删除身份/布局校验、前置条件 DELETE、单调预算下的结果确认 |
| [runtime/kubernetes/network_policy.py](../../../control_plane/runtime/kubernetes/network_policy.py) | NetworkPolicy 纯规格、精确发现/归属和拒绝规则核验、原 UID 删除及消失确认 |
| [runtime/kubernetes/proxy_resources.py](../../../control_plane/runtime/kubernetes/proxy_resources.py) | 可选八/九资源及历史布局闭集发现、原 UID 记录发布、完整性查询与顺序删除，复用同一 worker 客户端 |
| [runtime/kubernetes/resource_inspection.py](../../../control_plane/runtime/kubernetes/resource_inspection.py) | 两轮有界只读完整性观察，独立于健康状态，固定分类及不完整发布处理 |
| [runtime/kubernetes/proxy_service.py](../../../control_plane/runtime/kubernetes/proxy_service.py) | Proxy ClusterIP 纯规格、精确身份/路由核验，与 Agent Service 独立选择后端 |
| [runtime/kubernetes/proxy_spec.py](../../../control_plane/runtime/kubernetes/proxy_spec.py) | 无 I/O 的代理、Secret 与最小策略规格，供独立验证与可选 Runtime 布局复用 |
| [runtime/kubernetes/agent_service.py](../../../control_plane/runtime/kubernetes/agent_service.py) | Agent ClusterIP 纯规格、归属/配置核验、精确 UID 删除与消失确认；复用 CoreV1Api |
| [runtime/kubernetes/private_workspace.py](../../../control_plane/runtime/kubernetes/private_workspace.py) | 私有 PVC 纯规格、单次提交/结果不明核验、Pending/Bound 观察、消费者保护和原 UID 删除；由 v3 复用 |
| [runtime/kubernetes/local_entry_target.py](../../../control_plane/runtime/kubernetes/local_entry_target.py) | 八/九资源只读目标解析及不可变、无凭据身份快照，复用完整查询与线程内关闭 |
| [runtime/kubernetes/local_entry_manager.py](../../../control_plane/runtime/kubernetes/local_entry_manager.py) | 有界应用会话、并发打开去重、当次目标核验、单应用归属和删除协调 |
| [runtime/kubernetes/local_entry.py](../../../control_plane/runtime/kubernetes/local_entry.py) | 自有 kubectl 会话、loopback 端口解析、有界输出、目标监测及取消/关闭收尾 |
| [runtime/kubernetes/pod_spec.py](../../../control_plane/runtime/kubernetes/pod_spec.py) | 无 I/O 的受约束 basic 主 Pod 规格构建，返回独立 V1Pod |
| [runtime/sandbox_runtime.py](../../../control_plane/runtime/sandbox_runtime.py) | 共用异步 Runtime 契约，不包含 Kubernetes SDK 类型 |
| [core/tasks.py](../../../control_plane/core/tasks.py) | 首次/重复取消收尾及最终结果读取 |
| [runtime/kubernetes/failures.py](../../../control_plane/runtime/kubernetes/failures.py) | Runtime 与 PVC 共用的脱敏 SDK 错误分类，避免反向导入 |
| [core/errors.py](../../../control_plane/core/errors.py) | Kubernetes 专属固定领域错误与创建失败证据 |
| [core/service.py](../../../control_plane/core/service.py) | `ping()` 直接委托给注入的 Runtime，不查询 Store |
| [runtime/configuration.py](../../../control_plane/runtime/configuration.py) | 冻结选中后端的可信配置，惰性构造，校验可选数据库路径 |
| [app.py](../../../control_plane/app.py) | 选择/注入 Runtime，按后端注册实际环境和独立数据库，保留生命周期与固定就绪失败响应 |
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

namespace 使用与现有 Runtime 相同的 DNS label 规则。Agent 镜像做有界 ASCII、仓库/标签/sha256 引用语法校验，不执行 registry 解析、镜像内容或运行契约验证；不得将任意公共请求的值传给该后端参数。支持 basic/python/node、无独立工作区、无出口策略。输入中的实例 token 不进入任何 Pod 字段。

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

没有新配置框架、通用事务引擎或 Runtime 内的 SQLite 操作。镜像解析、独立工作区和暂停/恢复仍不支持；只读资源核验及显式启动 pending 恢复已实现。临时工作区的 Service 创建补偿现已通过本文后节的真实 Service/SQLite 验证，入口由显式本地会话交付。

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

镜像解析、独立工作区、应用端口入口及暂停/恢复仍抛出 `KubernetesOperationUnsupported`。基础 endpoint 仅交付应用已有会话，资源核验及显式启动 pending 恢复按本文相应章节执行。未支持的创建规格仍在提交前提供 `UNCHANGED`；未实现能力不创建客户端、不访问集群，也不退回 Docker。

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

仓库根目录执行下列示例，ID 必须来自已存在且整体 ready 的八/九资源实例：

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

上节记录的是此前 emptyDir 补偿阶段的验证，本身不涉及 PVC/PV 或数据保留。当前私有持久化及其 Service 补偿见下方九资源章节；正式生产创建入口配置、独立工作区复用及克隆/快照仍待规划，默认 Docker 和 schema 不变。

## 独立私有 PVC 基础

`private_workspace.py` 同时用于独立受管操作与 Runtime 版本 3 私有持久化。`build_private_workspace(spec, namespace=..., storage_class=...)` 返回新的 V1PersistentVolumeClaim；`PrivateWorkspaceResource(api, spec, namespace=..., storage_class=...)` 只保存借用的 CoreV1Api 与复制后的规格，构造不执行 I/O。辅助对象按单一 worker 顺序使用，不承诺并发操作同一辅助对象；调用方负责自己的 ApiClient、线程和取消收尾，不能在 SDK 线程仍执行时从另一线程关闭客户端。

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

默认或缺少任一开关时在加载 kubeconfig/创建客户端前跳过。独立兜底按完整归属与原 UID 删除本次消费者和 PVC，消费者清理失败时保留存储并报告残留；不操作开发库、既有存储或集群插件。测试只证明当前 hostpath CSI 的同节点重新挂载，该独立模块用例不替代下述 Runtime 挂载闭环，也不证明独立工作区互斥、跨节点恢复或磁盘硬配额。下一步为正式创建入口配置及独立工作区复用。


## 九资源私有工作区调用与验证

可信装配显式传入存储配置，默认应用不切换后端，也不开放公共创建请求的 StorageClass 字段：

```python
runtime = KubernetesSandboxRuntime(
    kubeconfig_path="/明确的/kubeconfig", context="agent-sandbox-k8s",
    namespace="agent-sandbox-test", agent_image="agent-sandbox-basic:dev",
    proxy_image="agent-sandbox-proxy-upstream-test:dev",
    workspace_storage_class="agent-sandbox-workspace",
)
```

构造只检查配置；存储配置要求同时启用代理，create 支持已注册的 basic/python/node、合法 Token、无独立 workspace 绑定和无出口白名单。build_pod_spec 纯构建准确 PVC claimName，PrivateWorkspaceResource 实际提交；不等待 PVC 先 Bound，再提交消费者 Pod。Kubernetes 省略 PVC source 的 readOnly=false 时按已知默认值兼容，不放宽 readOnly=true、subPath、mountPropagation、其他卷源或保留布局的 emptyDir-only 校验。

`proxy_resources.py` 按版本 1/2/3 管理七/八/九个明确键，九资源编码最多 1024 字节，新增 workspace 键。StorageClass、私有版本与原 PVC UID 随 UID/resourceVersion 保护的原资源记录一起发布；PVC 的严格元数据检查只接纳这一明确 Runtime 版本及其已知字段。PVC 的原身份核验独立于代理健康摘要；摘要异常仍阻止完整查询/入口交付，不误阻止合法归属的数据清理。共享 failures.py 只分类固定错误，不暴露 SDK 内容。

get 从资源自身记录恢复原配置，验证 Bound、原 UID、PVC 固定规格及实际挂载；不依赖新 Runtime 对象仍配置同一个 StorageClass。LocalEntryTarget 严格区分八/九资源，并包含 PVC UID；替代对象不能沿用旧会话。Runtime.get_endpoint 仍只查应用已有会话，services_version 仍不支持，公共响应不增加 workspace_id 或存储字段。

正常 `await service.delete(sandbox_id)` 确认关闭会话与删除计算/伴随资源，有意保留原 PVC，Service 原事务元数据及凭据按 CAS 清理，实例容量释放。之后可由受信调用方显式 `await service.delete(sandbox_id, delete_workspace=True)`，或以原事务调用新 Runtime.delete，清理仅剩的原 PVC；新 Runtime 无需传代理镜像和 StorageClass。默认重复删除不会误删数据，PVC 的原 expires_at 不触发自动删除，也不自动接入新实例。

创建失败和取消必须删除本次 PVC，Service 原补偿路径传入的 True 因而具有正确含义；先确认消费者消失，再调用 PVC 模块的完整消费者检查及 UID/RV 删除。发布失败、迟到响应、关闭失败或消费者不明时保留 REMAINING/UNKNOWN，不以只清理计算资源冒充 ROLLED_BACK。未能发布足够原 UID 记录的孤立对象继续保守拒绝自动接管，保留 pending/凭据及固定清理诊断用于人工核验；不通过删库或强删存储消除不确定结果。

在 control_plane/ 执行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_persistent_workspace.py tests/test_kubernetes_private_workspace.py tests/test_kubernetes_service_create.py tests/test_kubernetes_delete.py tests/test_kubernetes_endpoint.py tests/test_kubernetes_persistent_entry.py
uv run --locked pytest
uv run --locked python -m compileall -q app.py api core runtime storage tests

RUN_KUBERNETES_INTEGRATION=1 \
  RUN_KUBERNETES_RESOURCE_INTEGRATION=1 \
  KUBERNETES_TEST_CONTEXT=agent-sandbox-k8s \
  KUBERNETES_TEST_NAMESPACE=agent-sandbox-test \
  uv run --locked pytest -q tests/test_kubernetes_persistent_workspace_integration.py
```

受影响的显式旧布局回归命令是同一双开关及 context/namespace 下的 `uv run --locked pytest -q tests/test_kubernetes_service_create_integration.py tests/test_kubernetes_endpoint_integration.py tests/test_kubernetes_proxy_lifecycle_integration.py`。真实测试只使用临时 SQLite、本次唯一身份、缓存镜像和已有 StorageClass；不构建/拉取镜像。Agent/消费者以 hostname nodeSelector 参与调度，避免 nodeName 绕过 WaitForFirstConsumer；fixture 对代理构建施加同一可信调度约束以使用缓存镜像，不削弱生产规格比较。

新集成经 Service 自行生成身份/Token、预写 pending/凭据并激活，ready 后显式打开应用会话，通过现有受认证 HTTP endpoint 执行命令和读写文件。默认删除关闭本次 kubectl 与计算资源后，fixture 用同节点非 root 消费者读取原 PVC 文件，再移除消费者并精确清理卷；其他用例验证 True、缺失代理缓存及激活提交后重复取消的完整补偿。独立 fixture 核验原 UID/完整归属兜底清理，事前记录 PV claimRef 与 UID，事后只观察 Delete 回收，不直接删除 PV。

元数据的 resourceVersion 前置条件竞争可能导致当前创建保守失败并回滚，不自动重试原 POST 或绕过比较。真实验证结果与试跑失败记录见 PROJECT_PROGRESS；这一行为不表示数据安全擦除、硬磁盘配额、独立工作区复用或生产后端切换已经完成。
