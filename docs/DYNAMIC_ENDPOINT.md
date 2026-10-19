# 动态 sandbox 本机入口设计与实施

本说明记录从 WSL 完成创建、内部就绪、入口查询和 Agent 调用的动态入口设计。后续增加的手动删除契约见 [删除设计](docker/DELETION.md)：删除先关闭本实例入口，默认保留 workspace，显式参数才删除卷。当前 TTL 仅记录初始创建和计划过期时间，不自动扫描或删除，超过计划时间不改变入口语义；仍不提供续期、认证或用户服务端口暴露。

## 边界与数据流

`API → SandboxService → SandboxRuntime` 保持不变。每个实例独占 internal bridge、普通 ingress bridge、workspace、sandbox 和 proxy。sandbox 仅连接 internal、使用 DNS 别名 `sandbox`，不发布端口；proxy 连接两网，只向 `127.0.0.1` 动态发布 8080。workspace 仅挂载给 sandbox。代理镜像打包现有 `proxy.conf`，直接以 nginx 用户启动 Nginx，保留文件和 SSE 配置。

复用现有 `container/network/workspace` 角色以兼容旧实例，新增 `proxy/ingress`。两镜像先检查存在，不自动 pull/build。创建顺序为 internal、ingress、workspace、sandbox 创建并启动、proxy 创建、连接 internal、启动。每次 create 响应立即记录精确 ID 和完整标签；失败或取消逆序移除 proxy、sandbox、workspace、ingress、internal。提交后的取消与重复取消继续等待清理。

## 查询契约与失败场景

`get_endpoint(sandbox_id) -> SandboxEndpoint` 返回 sandbox_id、url。ID 无效、未知或 sandbox 归属不完整为 404；已有 sandbox 的入口缺失、不健康、歧义、归属或事务不匹配、网络不正确、映射未生效为 503 `sandbox_endpoint_unavailable`；整体或 transport 超时为 504 `sandbox_endpoint_timeout`。错误不输出 Docker 细节。

只读查询重新加载 sandbox、proxy、两网并核验精确 ID 和完整归属，确认 sandbox 仅连 internal、proxy 仅连这两网、sandbox 的 DNS 别名存在、网络为专属 bridge 且 internal 属性正确。代理必须 running 且 healthy。仅从实际 `NetworkSettings.Ports["8080/tcp"]` 接受唯一 `127.0.0.1`、1–65535 端口；HostConfig 不是地址来源。旧实例没有 proxy 返回 503。状态查询的 ready 仍表示内部 Agent 健康。

## 实施与验收计划

1. 增加 endpoint 模型、领域错误、Service ID 校验、Runtime 抽象方法与 API；先用 FakeRuntime 验证调用链、404/503/504 和取消。
2. 扩展现有 transaction.py、角色加载和代理镜像；测试双镜像检查、创建顺序、安全配置、失败及取消逆序回滚和归属拒绝。
3. 在现有 docker_runtime.py 实现只读入口恢复与实际端口解析；测试网络、标签、健康、映射、多个候选和 transport 超时。
4. 显式集成创建两个实例，经真实 endpoint 验证健康、命令、文件、SSE 首帧、重启恢复和工作区独立；以普通 bridge 为公网对照。测试按精确 ID 和完整标签清理本次资源。
5. 更新 README、API、控制面和进度文档；执行用户列出的镜像构建、pytest、compileall、Docker 集成与 diff 检查，记录实际结果。

endpoint 仅是本机 WSL 开发地址，不承诺远程访问。只读查询不是整个调用期间的可用性保证；查询后资源仍可能退出。新资源不接管或重建已有开发实例。

## 手动删除后的入口

`DELETE /v1/sandboxes/{sandbox_id}` 按 proxy、sandbox、ingress、internal 的顺序回收本实例运行资源，成功返回 204 空体。删除完成后旧 endpoint 不再可访问，状态和入口查询均返回 404；保留的 workspace 不构成可查询实例。删除不依赖 endpoint 可用、容器健康或完整网络拓扑，也能清理受管残留。

默认保留的数据仍占用磁盘，`?delete_workspace=true` 会在运行资源之后不可恢复地删除专属卷。部分失败返回 500 并包含 sandbox_id，可以重新请求继续清理；已删除项不重建。真实删除集成测试创建 A/B 两实例，使用新 app/runtime 删除 A，验证旧入口关闭、默认数据保留和显式删卷，同时确认 B 的入口、文件与命令继续可用。最终清理只处理本次记录且精确 ID、完整标签均核对通过的资源。
