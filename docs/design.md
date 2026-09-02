# 设计说明

## 目标与约束

系统的核心职责是兜住外部 API 的短暂或长期不可靠性，同时让内部调用保持快速、统一。入口目标为本地基准环境 p99 100ms 以内（非跨环境硬 SLA）；所有外部 I/O 均异步。MVP 只运行 API、worker、Redis 三类容器，SQLite 文件由 API 与 worker 共享。

## 数据与状态

`notifications` 是事实源，记录请求、状态、尝试次数、下次重试和最后错误；`idempotency_keys` 只维护 24 小时唯一性；`dead_letters` 保存终止原因和快照。attempts 的含义是“已经实际发出的请求次数”：本轮值为旧值加一，第 10 次失败直接入 DLQ，第一次失败后的基础延迟为 1 秒。

状态转换为 `pending → running → success`、`running → pending`（可重试）、`running → dead`（不可重试或达到上限）。stale running 可恢复为 pending；终态 Stream 重投会被幂等跳过。

## 一致性窗口

入口的两次写操作在一个数据库事务中完成，避免通知或 key 孤儿。事务提交后才 XADD，因此存在 commit 成功但 XADD 未发生的窗口；周期扫描补偿。HTTP 2xx 与成功状态提交之间也有窗口，可能造成外部重复调用；选择 at-least-once 后不能在没有供应商幂等支持的情况下消除。

## 并发与恢复

SQLite 设置 WAL、5 秒 busy timeout、foreign keys 和 NullPool，每个仓储方法都是短事务。consumer 使用 XREADGROUP；处理完成后 XACK；未确认消息由 XPENDING/XCLAIM 回收。数据库恢复同时覆盖遗漏入队的 due pending 和超时 running。

## 错误决策

HTTP 响应按状态码纯函数分类。渲染错误表示当前 payload 与配置不匹配，不会随重试自愈，因此直接死信。配置在入队后移除同理。超时、连接错误、408、429、5xx 与未知异常采取保守重试。`last_http_status` 在收到任意响应时更新，无响应异常保持 NULL。

## 安全与边界

YAML 中只保存 `${ENV}` 占位符，启动时展开；模板使用 StrictUndefined。服务按题目假设运行在可信内网，不提供鉴权。它不验证通用 payload schema、不执行供应商回调、不自动回灌死信，也不宣称下游无重复。

## 可测试性

资源通过 `AppContext` 显式注入。仓储用临时真实 SQLite；HTTP 用 MockTransport/respx 同进程拦截；Streams 单元集成用 fakeredis；Docker 冒烟使用标准库 fake HTTP server，避免误认为 respx 能跨进程拦截 worker。

