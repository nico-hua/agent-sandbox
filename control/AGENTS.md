# Control 模块协作指南

本目录是运行在 WSL 宿主侧的独立 Python/FastAPI 控制面，与容器内的 Go Agent 分离。当前只提供进程健康检查和只读 Docker 就绪探测，不具备 sandbox 生命周期管理能力。

## 开发约束

- 使用 `uv` 管理依赖与锁文件；修改依赖后同步更新 `pyproject.toml` 和 `uv.lock`。
- HTTP 层通过注入的探测器访问 Docker，不在路由中散落 Docker SDK 调用。
- Docker client 必须延迟创建、设置有限超时，并在成功和失败路径关闭。
- 就绪探测只能执行只读 `ping()`；不得创建、启动、停止或删除 Docker 资源。
- Docker socket 仅供宿主侧控制面访问，不得挂载到 sandbox 或入口代理。
- 服务没有认证，只能绑定本机地址用于开发，不得暴露到不可信网络。
- 每个具名 Python 函数和测试函数都应包含简洁的函数级 docstring。

## 验证命令

在本目录运行：

```bash
uv sync --locked
uv run pytest
uv run python -m compileall -q control tests
```

手动启动时使用：

```bash
uv run uvicorn control.app:app --host 127.0.0.1 --port 18083
```
