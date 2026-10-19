# 生命周期存储协作约束

本目录是控制面内部的独立生命周期存储模块，当前仅使用 Python 标准库 `sqlite3` 实现 `SQLiteLifecycleStore`，没有独立构建步骤。保持实现集中于 `lifecycle_store.py`，不要引入 ORM、通用 Repository 框架、连接池或其他数据库后端。

## 数据与操作边界

- 生命周期记录采用 `control_plane.core.models` 的时间校验规则，时间必须为带时区 UTC；存储和比较保留整数微秒精度，不使用浮点时间戳或变长时间字符串排序。
- sandbox ID、事务 ID 和创建时间不可被更新操作改变；更新过期时间及删除记录必须核对事务 ID 和预期版本，版本冲突不能覆盖新数据。初始版本为 0，成功更新后递增。
- 数据库路径由构造参数提供。导入和构造不能创建目录、数据库或连接；表结构只能通过显式 `initialize()` 初始化，重复执行不得清空数据。损坏或不兼容数据库应返回稳定存储错误。
- 同步 SQLite 操作通过线程适配执行；每次操作在线程内创建并关闭自己的连接，不跨线程共享连接，不禁用 `check_same_thread`。采用参数化 SQL、有限锁等待，并在所有成功和失败路径提交或回滚、关闭连接。
- Service 通过最小 `LifecycleStore` 协议协调创建预写、查询合并和删除后清理；Store 只操作元数据，不调用 Docker 或依赖 HTTP。默认数据库由 `app.py` 定位到 `control_plane/.local/lifecycle.sqlite3`，在 FastAPI lifespan 中显式初始化，失败必须阻止启动，不降级为临时或内存存储。
- 取消异步等待不表示 SQLite 工作线程已经停止。Service 必须等待关键写入最终结果，再决定是否按原事务 ID 和预期版本补偿；首次和重复取消不能跳过收尾，也不能遗漏后台异常。不要把 SQLite 事务描述为能够撤销 Docker 操作。
- Docker 标签保存初始时间；查询只在实际实例与记录身份、创建时间匹配时采用数据库当前过期时间。不得隐式补写、同步标签或接管无记录旧实例，不增加过期扫描、自动删除或续期 API。本地数据库及其 journal、WAL、共享内存文件不得提交版本库。
- 所有具名函数（包括测试）均需简洁的函数级 docstring。

## 验证

在 `control_plane/` 执行：

```bash
uv run --locked pytest tests/test_lifecycle_store.py -v
uv run --locked pytest
uv run python -m compileall -q app.py api core runtime storage tests
```

在仓库根执行 `git diff --check`。测试使用临时文件数据库和固定时间；并发更新通过同一文件的独立连接验证，不以隔离的内存数据库替代，不依赖真实 sleep。默认测试不得修改 Docker 资源，本模块验证无需创建真实容器。
