# 生命周期存储协作约束

`base.py` 定义最小 `LifecycleStore` 协议及稳定存储错误。`sqlite/sqlite_store.py` 管理唯一的 SQLite Store 和连接生命周期，`sqlite/schema.py` 负责在 Store 提供的连接上初始化和核验 v7，`sqlite/run_lock.py` 管理本机文件锁。应用装配 SQLite 实现，服务依赖存储协议。Store 使用 Python 标准库，独立于 HTTP 和 Docker。禁止引入 ORM 或单独的工作区数据库。

## 身份、CAS 与事务

- 时间统一为带时区 UTC，整数微秒存储。实例 ID、原事务和创建时间不可变。状态、期限及删除核对原事务和预期版本。记录从 pending、版本 0 开始，成功状态/期限更新递增版本，已经进入 deleting 的精确重试保留原版本。
- 在同一 `BEGIN IMMEDIATE` 内检查所有生命周期记录总数，原子写入 pending、凭据与可选工作区占用。实例上限是严格整数 1～1024，默认 10。满额与存储故障分别报告。只有原记录安全删除才释放名额。
- `endpoint_credentials` 单独保存实例 Token，显式按实例 ID 和原事务读取。普通查询、列表和 repr 不含 Token。生命周期删除级联移除凭据。旧无凭据记录仍可查询和删除。
- `lifecycle_records.workspace_id` 通过外键与唯一约束表示独立工作区的占用。工作区版本更新、实例 pending 写入与凭据共享一个事务，删除实例与递增工作区版本也共享一个事务。SQLite 触发器固定工作区身份和占用期间的版本，并保护当前持有者身份。工作区创建从 pending 开始，按原令牌及版本确认 available。独立删除要求空占用与原身份，失败保留 deleting。
- 每个同步操作在自己的线程与连接内执行，使用参数化 SQL、外键及有限锁等待，所有路径提交或回滚并关闭连接。SQLite 事务内不等待 Docker。关键异步写入取消后仍须等待线程收尾，实际结果决定后续补偿。

## 初始化

- 当前 schema 为 7。`initialize()` 只创建空新库或核验同版本完整 DDL 和外键。默认数据库为 `control_plane/.local/lifecycle.sqlite3`，不回退为空库或内存。Store 导入与构造不执行文件 I/O。
- 新建数据库以独占创建使用权限 `0600`，新建目录为 `0700`，遵循平台能力。版本或结构不兼容时拒绝启动并保留数据库内容。默认测试只使用临时文件。操作说明见 [数据库初始化](../../docs/control_plane/RUNBOOK.md#数据库初始化)。

## 启动、维护与 RunLock

- lifespan 在初始化、后台工作和请求之前获取实际 Store 规范路径的系统锁。同库对象和进程互斥，不同库独立。使用 Linux/WSL 或 Windows 文件原语，非阻塞区分锁竞争、权限问题和平台不支持。句柄不可继承，释放前等待初始化和维护取消收尾，不删除伴随锁文件。
- RunLock 仅约束本地遵循协议的进程。文件存在与 PID 都无法证明锁所有权，取得锁也无法证明旧 Docker 请求结束。
- 回收有界查询到期 active 及可重试 deleting，认领原事务、版本、状态和截止条件并在同一 SQLite 事务内提交。Service 按原认领记录回收。pending 不进入到期删除候选。只读 pending 检查与完整候选恢复由 Service 管理。恢复按完整资源、原身份、ready、入口与有效期限核验原版本，跳过冲突和不确定结果。
- 续期使用一次时钟快照计算 `max(current_expires_at, now + ttl_seconds)`，仅更新未到期的 active 原事务和版本。结果不明时查询实际值。Docker 标签保留初始时间。
- 管理列表在同一短读事务内读取总数与有界键集页，按整数微秒创建时间和实例 ID 排序。分页读取公开摘要，不读取凭据或调用 Runtime。不同分页请求各自取得快照。
