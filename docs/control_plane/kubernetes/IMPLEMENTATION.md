# Kubernetes Runtime 实现

本文对应当前只读 Runtime 与 HTTP 就绪链路，说明代码位置、客户端装配、收尾和验证步骤。能力与信任边界见[设计说明](DESIGN.md)，接口响应见[控制面 API](../API.md#3-后端就绪检查默认-docker)，历史验收结果统一记录在[开发进度](../../PROJECT_PROGRESS.md)。

## 代码边界

| 位置 | 当前职责 |
| --- | --- |
| [runtime/kubernetes/kubernetes_runtime.py](../../../control_plane/runtime/kubernetes/kubernetes_runtime.py) | 配置校验、客户端工厂、同步只读探测、线程等待和未实现方法 |
| [runtime/sandbox_runtime.py](../../../control_plane/runtime/sandbox_runtime.py) | 共用异步 Runtime 契约，不包含 Kubernetes SDK 类型 |
| [core/tasks.py](../../../control_plane/core/tasks.py) | 首次/重复取消收尾及最终结果读取 |
| [core/errors.py](../../../control_plane/core/errors.py) | Kubernetes 专属固定领域错误与创建失败证据 |
| [core/service.py](../../../control_plane/core/service.py) | `ping()` 直接委托给注入的 Runtime，不查询 Store |
| [app.py](../../../control_plane/app.py) | 装配共享 Service，按 Runtime 选择固定就绪失败响应 |
| [api/health.py](../../../control_plane/api/health.py) | 进程健康、认证后的就绪调用与 503 响应，不识别具体后端类型 |
| [pyproject.toml](../../../control_plane/pyproject.toml) 与 [uv.lock](../../../control_plane/uv.lock) | 官方 Kubernetes 客户端依赖及锁定版本 |

源码保持 `runtime/kubernetes/__init__.py` 与 `kubernetes_runtime.py` 的简单布局，没有独立 HTTP 服务、Runtime 注册器或生产切换框架。

## 客户端与同步探测

`KubernetesSandboxRuntime.__init__()` 校验明确的路径、context、namespace，只保存值和工厂。不存在的配置文件不会在构造时读取；实际加载发生在 ping 的工作线程内。

| 阶段 | 对应实现 | 行为 |
| --- | --- | --- |
| 异步入口 | `ping()` | 创建 `asyncio.to_thread()` worker，用 `settle_task(..., propagate_cancellation=True)` 等待 |
| 同步操作 | `_ping_and_close()` | 创建一个客户端，执行一次 Pod 列表调用，在 finally 关闭 |
| 配置选择 | `_selected_entry()` | 按名称选取唯一 context、user 和 cluster，拒绝缺失/重复条目 |
| 配置加载 | `_load_configuration()` | 安全解析指定文件，限制静态证书认证，用 `KubeConfigLoader` 加载独立配置 |
| 客户端工厂 | `create_probe_client()` | 创建 `ApiClient(configuration=...)` 与 `CoreV1Api`；后者构造失败时关闭已取得的客户端 |
| 关闭 | `_close_client()` | 主错误优先；单独关闭失败转换为固定连接错误 |
| 错误转换 | `_probe_failure()` | 按 SDK 类型、状态码和 transport 类型分类，不解析错误正文 |

配置使用 `proxy=None`、`no_proxy="*"`、`retries=False`、`client_go_retries=False` 和 `debug=False`。禁用代理在加载 API host 前完成，避免 SDK 随 host 重新选择环境代理。证书相对文件路径以 kubeconfig 所在目录为基准，不修改 SDK 全局默认配置或调用方环境。

同步查询固定 `limit=1`、`watch=False`、`_request_timeout=(0.5, 0.5)`；返回值只用于确认调用成功，不跟随分页，不将 Pod 内容返回给 Service。客户端由同一工作线程关闭，连接和读取超时的限制见[设计](DESIGN.md#探测与期限)。

测试工厂通过 `client_factory` 注入，签名接收 `kubeconfig_path`、`context` 两个关键字。返回对象需提供 `list_namespaced_pod()` 与 `api_client.close()`；namespace、limit、watch 和 timeout 由 Runtime 设置。工厂只在 ping 的 worker 中调用，每次探测独立取得和关闭句柄。

## HTTP 装配与失败处理

`create_app(runtime=runtime)` 保留既有 Service 装配。应用识别 `KubernetesSandboxRuntime` 后，将固定的 `(code, message)` 传给 `health_router()`；路由本身只执行 `SandboxService.ping()`，不判断 Runtime 类型或调用 SDK。没有注入 Runtime 时仍构造 Docker 后端，其失败状态码、文案和认证行为保持不变。

Kubernetes 配置、权限、连接、超时和关闭失败均在根就绪检查返回固定 503；内部领域分类不变，未知异常内容也不用于响应。`CancelledError` 不作为普通失败吞掉。超时或取消需要等待原工作线程完成关闭，迟到失败被读取，不提前释放仍在使用的客户端。

其他 Runtime 方法，包括镜像解析、实例创建/查询/删除、入口、资源核验、工作区、暂停/恢复，统一抛出 `KubernetesOperationUnsupported`。create 的 `creation_failure` 为 `UNCHANGED`；这些调用不创建客户端、不访问集群，也不退回 Docker。

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
| [test_app.py](../../../control_plane/tests/test_app.py) | 默认 Docker、健康路由、Service 委托和应用装配回归 |
| [test_kubernetes_runtime_integration.py](../../../control_plane/tests/test_kubernetes_runtime_integration.py) | 显式目标的真实 Runtime ping 与认证 HTTP 就绪链路 |

在 `control_plane/` 执行默认检查；使用 Fake Client，不要求集群运行：

```bash
uv sync --locked
uv run --locked pytest -q tests/test_kubernetes_runtime.py tests/test_kubernetes_health.py tests/test_app.py
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

## 排查原则

| 观察 | 优先检查 |
| --- | --- |
| `/healthz` 成功、`/readyz` 为 401 | 调用方是否向控制面发送了正确管理员 Bearer key |
| `KubernetesConfigurationError` | 显式文件路径、context、证书/CA 文件与支持的静态认证方式 |
| `KubernetesPermissionDenied` | 所选身份是否能读取该 namespace 的 Pod 列表 |
| `KubernetesTimeout` | 指定 API 的连接和读取是否可在当前阶段预算内完成 |
| `KubernetesConnectionFailed` | 指定 API 的可达性、TLS 校验与客户端收尾 |
| `KubernetesOperationUnsupported` | 调用是否超出当前只读 ping 的实现范围 |

根 HTTP 接口统一隐藏内部失败分类，不应通过输出原始 SDK 异常、kubeconfig、证书或认证头来排查。不要自动切换 context、关闭 TLS 验证、改用其他凭据或创建额外资源来掩盖探测失败。
