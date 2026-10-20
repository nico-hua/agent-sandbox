# Control Plane Error Handling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在保持公开错误契约和安全收尾的前提下，简化控制面异常处理。

**Architecture:** 沿用现有 API、Service、Runtime 和 Store。集中 API 映射；抽取小型异步等待工具；在原有领域异常上提供明确的创建恢复信息，并以结构化结果描述逆序回滚。

**Tech Stack:** Python >=3.12、FastAPI、Docker SDK、sqlite3、uv、pytest。

**Spec:** [异常处理设计](CONTROL_PLANE_ERRORS.md)。

## Global Constraints

- 不改变 HTTP 状态码、错误码、固定文案、诊断 ID、schema 2 或依赖。
- 保留用户未提交修改；不创建、删除或重建真实 Docker 资源；不自动提交或推送。
- 精确身份、完整标签、事务和版本保护不得降低；创建回滚与正式删除的 workspace 策略不同。
- 所有具名函数/测试有 docstring；错误响应及默认诊断不包含原始后端异常内容。

## Review Focus

- 外层取消后 worker 失败仍须读取最终结果，不向事件循环重复报告。
- 迟到提交、替换事务和失效版本不能误删元数据或其它资源。
- 查询主错误遇到 close 失败时不得丢失。
- 未登记的创建结果不能被已登记资源的成功回滚掩盖。
- endpoint 操作特例、异常子类与脱敏固定文案不因集中映射漂移。

## Task 1：任务等待和取消收尾

**Files:** 新增 `control_plane/core/tasks.py` 和聚焦测试；修改 `core/service.py`、`core/reaper.py`、`runtime/docker/docker_runtime.py` 的等待点。

**Interfaces:** `wait_for_task(task)` 在不取消输入任务的前提下等待；`settle_task(task, propagate_cancellation=False)` 穿过重复取消并读取结果。`wait_for_completion(task)` 返回等待期间记录的取消，由有特殊优先级的调用方读取结果。业务流程保留自己的补偿和最终异常选择。

- [x] 复现现有取消失败；新增 worker 失败且重复取消时事件循环无额外异常的聚焦测试。
- [x] 实现等待基础动作，移除重复等待循环，保持创建/删除/续期策略。
- [x] 运行生命周期、Runtime、续期和回收测试。

## Task 2：HTTP 映射集中化

**Files:** `api/sandboxes.py`、必要的 API 内映射模块及聚焦测试。

**Interfaces:** 共享响应构造接受领域异常和操作上下文；默认映射保留既有字段，endpoint 显式覆盖不可用错误。

- [x] 添加派生异常、固定文案和 endpoint 覆盖的失败测试。
- [x] 集中已知异常映射，路由仅保留操作超时、输入校验和成功响应。
- [x] 运行应用、查询、入口、删除和续期 API 回归。

## Task 3：查询收尾和创建恢复信息

**Files:** `core/errors.py`、`core/service.py`、`runtime/sandbox_runtime.py`、`runtime/docker/transaction.py`、`runtime/docker/docker_runtime.py` 及相应测试。

**Interfaces:** 原有异常附带 `creation_failure`，包含阶段、资源状态、脱敏清理问题；回滚返回结构化报告。Service 依据资源状态选择元数据补偿，不再维护异常类型白名单。

- [x] 添加查询主错误叠加 close 失败、回滚已消失、删除结果复查和清理问题保留的红测。
- [x] 实现主错误优先、结构化逆序回滚和保守未知结果处理。
- [x] 替换 Service 的类型白名单，保持旧异常默认语义和公开响应兼容。
- [x] 运行 Runtime、Service 及所有默认测试。

## Task 4：文档、验证和审查

- [x] 更新 `docs/PROJECT_PROGRESS.md`、控制面及 Docker 文档，记录实际验证平台和限制。
- [x] 运行 `uv --no-cache run --locked --offline --no-sync python -B -m pytest -p no:cacheprovider --timeout=15 -q --tb=short`。
- [x] 运行编译检查、`git diff --check` 并审查增量 diff。
- [x] 独立审查补偿、取消、错误映射和已有修改保留情况，处理重要问题后复验。

## 执行记录

- 采用当前会话执行并并行委派互不重叠的模块任务；用户已授权实施推荐方案，不再增加确认步骤。
- 保留当前分支和工作区：用户改动属于分析基线，另建 worktree 会遗漏这些尚未提交的内容。
- 红测覆盖查询主错误覆盖、派生异常映射、显式资源状态优先于旧异常类型、取消包装诊断丢失及提交后回滚关闭异常。现有 Windows 取消失败作为等待机制的回归基线。
- 整合时修正提交后回滚关闭失败的状态为 UNKNOWN；所有相关包装保留恢复证据；未缩窄写事务的兜底清理捕获。
- 最终验证：Windows/Python 3.14.3 默认测试 796 passed、6 skipped；compileall、全控制面具名函数 docstring 检查、git diff --check 通过。独立审查 170 项聚焦通过，无待处理的重要问题。
- 未执行 Linux/WSL 或真实 Docker 集成；未新增依赖、改 schema、提交或推送。Store 中原有的输入类型异常调整不属于本次实现。
