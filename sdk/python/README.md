# Agent Sandbox Python SDK

同步 Python SDK，通过控制面管理实例与工作区，通过动态 endpoint 执行命令和传输文件。需要 Python 3.12+，运行时仅依赖 HTTPX 和标准库；服务端应在本机运行。

从仓库根目录安装：

```bash
python -m pip install ./sdk/python
```

```python
import os
import sys
from agent_sandbox import SDKError, SandboxClient

with SandboxClient("http://127.0.0.1:18083", api_key=os.environ["AGENT_SANDBOX_API_KEY"]) as client:
    sandbox = client.create_sandbox(ttl_seconds=300)
    print(sandbox.sandbox_id)  # Save the ID before further operations.
    try:
        sandbox.wait_ready(timeout=30)
        result = sandbox.run(["pwd"])
        print(result.exit_code, result.stdout, result.stderr)
    finally:
        primary = sys.exception()
        try:
            sandbox.delete(delete_workspace=True)
        except SDKError:
            print("Cleanup failed; inspect", sandbox.sandbox_id)
            if primary is None:
                raise
```

先在调用进程安全配置与服务相同的 `AGENT_SANDBOX_API_KEY`。示例由调用方显式读取并传入；SDK 自身不读取环境变量，只对控制面请求发送 key，Agent 命令与文件入口仍无认证。不要打印 key 或将它传入用户命令。

`with` 只关闭 HTTP 连接；资源必须显式删除。示例删除实例私有工作区及其文件，删除失败应保存已打印的 ID；网络错误后服务端操作结果可能不确定。

完整安装、API、超时与错误说明、独立工作区复用和测试命令见 [Python SDK 文档](../../docs/sdk/python.md)。可运行示例见 [python-workspace-reuse.py](../../examples/python-workspace-reuse.py)。
