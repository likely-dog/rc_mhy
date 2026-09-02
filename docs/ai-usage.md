# AI 使用说明

## AI 帮上的

- 从作业 PDF、demo PRD 和 final PRD 中交叉识别了 12 个实现前问题，例如重试 off-by-one、respx 不能跨进程、`pytest --cov` 缺少 pytest-cov，并先写入开发进程文档等待人工确认。
- 生成了 Redis Streams consumer、短事务仓储、严格 Jinja 渲染和 HTTP 分类的初始实现，并用测试验证关键分支。
- 第一轮测试发现外键写入顺序问题后，AI 根据 SQLite 错误栈定位为“无 ORM relationship 时 flush 顺序不受保证”，采用父记录显式 flush 修复，而没有关闭 foreign key。
- 生成了可注入 AppContext 和 MockTransport/fakeredis 测试结构，使 worker 状态机无需真实外部网络即可覆盖。

## 没采纳的建议

- 没有采用 PRD 早期版本的 arq/tenacity：final 已选择手写 Streams consumer 和显式退避，引入框架会出现两套调度语义。
- 没有为每个供应商创建 Adapter 类和工厂；供应商差异保持为 YAML 数据与模板。
- 没有使用进程内 respx 假装验证 Docker worker；跨进程 smoke 改用真实标准库 HTTP server。
- 没有引入 Celery、Kafka、PostgreSQL、Prometheus 或完整 JSON Schema；它们对低吞吐作业 MVP 属于过度设计。
- 没有把 dead 幂等命中改为 409，避免擅自破坏 final PRD 已声明的 200 契约。

## 自己拍板的

- 人工确认采用开发进程文档中的 12 项基线后才开始编码，避免把未决建议默认为需求。
- attempts 明确定义为实际 HTTP 调用次数，第 10 次失败结束；这同时修复首轮 2 秒和上限第 11 次才结束的问题。
- SQLite 继续用于评审便利，但显式采用 WAL、busy timeout、NullPool 和共享 volume，并诚实记录吞吐边界。
- payload 在 API 层只验证为 object；可选 event allow-list 提前拒绝明显错误，模板字段问题由 worker 严格渲染并死信。
- 增加 pytest-cov 并在 README 说明理由，因为覆盖率命令是 DoD 的明确要求，原白名单遗漏了执行插件。

