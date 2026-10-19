# 手动删除设计与实施计划

本文记录 Docker 垂直链路的手动回收设计，最初阶段未引入数据库、墓碑、TTL、后台任务或第二种运行时。当前 Service 已接入 SQLite 生命周期元数据，下述 Docker 资源回收策略保留；跨系统流程补充如下，原实施与验收记录仍保留其历史含义。

## 当前生命周期存储接入

Service 先读取生命周期记录。有记录时将 `expected_transaction_id` 传给 Runtime，所有存留角色必须在破坏性操作前匹配该事务，包括默认保留的 workspace；不以主容器存在或 `Runtime.get()` 成功为前提。无记录的旧实例仍按原标签规则删除，不隐式建立元数据。

Runtime 确认目标完成后，Service 按读取时的事务 ID 和版本删除元数据；默认留卷同样移除已结束实例记录，残卷继续由 Docker 完整归属标签和显式删卷请求管理。Docker 失败保留记录，Docker 已完成但元数据清理失败或版本冲突返回 `500 sandbox_metadata_cleanup_failed` 与 sandbox ID，允许同 ID 重试，不盲目刷新版本删除记录。存储读取失败返回 `503 sandbox_metadata_unavailable`，尚未开始 Docker 删除。

SQLite 与 Docker 没有跨系统原子事务，SQL 回滚不能撤销 Docker 删除。首次及重复取消都等待 Runtime 与元数据关键收尾的最终结果并读取后台异常；数据库有记录不代表实际实例存在。当前没有自动恢复、过期扫描、自动回收或续期 API，未来续期与删除的协调仍待设计。完整当前错误契约见 [CONTROL_API.md](../CONTROL_API.md)，应用初始化与创建/查询链路见 [CONTROL_PLANE.md](../CONTROL_PLANE.md)。

## 契约与边界

调用链保持 `API → SandboxService → SandboxRuntime → DockerSandboxRuntime`，Runtime 当前签名为 `async delete(sandbox_id, *, delete_workspace=False, expected_transaction_id=None) -> None`，Service 对外参数仍只有实例 ID 与 workspace 策略。Service 在访问后端前拒绝不符合 `sbx_[0-9a-f]{32}` 的 ID。DELETE 不使用只读查询的 1 秒整体超时。

默认删除 proxy、sandbox、ingress 和 internal bridge，保留 workspace 及其数据。`delete_workspace=true` 最后删除该实例专属卷，数据不可恢复。创建失败的事务回滚仍删除本次创建的 workspace，不受正式删除的保留策略影响。

达到目标状态（含合法未知 ID、仅剩保留卷）返回 204 空体；无效 ID 返回 404。归属歧义/冲突返回 `409 sandbox_delete_conflict`；发现时后端不可用返回 `503 docker_unavailable`；无法完成回收返回 `500 sandbox_delete_failed` 并包含 sandbox_id。已经删除的资源不补偿重建，重试从实际标签重新发现残留。

## 资源清单与回收

`runtime/docker/transaction.py` 集中同步 Docker 资源生命周期操作与归属工具，包含创建事务、创建失败逆序回滚，以及正式删除的发现、核验和回收；正式删除策略与创建失败回滚保持独立。`docker_runtime.py` 保留异步 `delete()`、线程调度和取消等待。同步实现合并不改变删除契约、HTTP 和安全语义。

正式删除复用资源管理器与归属工具，不要求容器存在、健康或拓扑完整。按 managed/project/sandbox-id/resource 四个标签分别查询五种角色，包含保留卷；候选重新 inspect，要求完整标签、有效事务 ID、跨资源事务一致、角色唯一。全部清单通过后才执行破坏性操作。过滤条件不匹配的资源不属于目标，不按名称推断归属。

清单记录精确容器/网络 ID、卷 Name 和五项归属标签。每次 stop/remove 前按原标识重新读取并核验；卷也按经过核验的 Name 和事务标签读取。归属或精确标识变化立即拒绝后续操作。拥有 Docker 管理权限的外部主体仍能改标或在核验和删除之间竞争；标签不是认证或防篡改边界。

按 proxy → sandbox → ingress → internal → 可选 workspace 回收。容器 stop 使用 2 秒宽限期；失败时重新核验后使用 `remove(force=True, v=False)` 强制终止该精确容器；正常停止后普通移除且不附带删除卷。网络不强制断开其他容器，卷不强制删除。各项失败后继续尝试可安全处理的后续资源，最终报告未完成；归属变化立即拒绝。每项移除后确认原精确资源不存在，NotFound 视为该项完成。

并发删除依靠 Docker 当前事实及 NotFound 收敛，不依赖创建调用的内存记录或进程锁。仍存在的占用/删除冲突报告失败，后续请求可以继续重试。删除工作线程一旦启动，取消等待并不停止 SDK；首次及重复取消都等待线程回收、关闭 client 后才传播取消，不回滚重建已删除项。

失败项在收尾时按原精确标识再次核验，若另一请求已完成删除则视为完成；归属变化仍拒绝，不能仅因标签筛选不再命中就判断消失。最后重新发现清单，确认请求要删除的角色已无残留。

## 实施计划

- [x] 契约/API：先写 Service 和 HTTP 红测，加入删除契约、ID 校验、默认/显式参数、204 空体及稳定错误映射，证明不受查询超时影响。
- [x] Docker：先写有状态 fake 红测，实现五角色清单、归属和事务预检、每步精确重读、有限停止、缺失幂等、失败重试与取消等待；保留创建/查询/入口回归。
- [x] 集成：本次创建 A/B，新控制面删除 A 后确认入口关闭、查询 404、B 正常；核实默认保留卷和文件，再显式删卷；finally 仅清理本次记录且完整归属核验通过的资源。
- [x] 文档/验收：更新 API、示例、实现说明和进度；运行 WSL 默认 pytest、compileall、显式 Docker 集成、手动本机 HTTP 生命周期与 diff 检查，记录资源/卷/B 的最终状态。结果和环境限制见 [开发进度](../PROJECT_PROGRESS.md#2026-10-19)。

## 重点检查

测试涵盖默认保留和显式删卷、合法未知/无效 ID、重复/并发删除、控制面重启、缺失角色/仅网络/仅卷、标签及精确 ID 变化、无效或不一致事务、重复角色、发现后消失、停止失败、网络/卷占用、部分失败后重试、取消/重复取消/关闭 client。保留卷会持续占用磁盘，后续仍需单独设计卷回收和配额。
