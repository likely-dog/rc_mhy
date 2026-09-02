# 测试报告

执行时间：2026-09-01 至 2026-09-02（Asia/Shanghai）

## 结论

通过。单元/进程内集成测试、覆盖率门槛、静态检查、Compose 配置检查和真实三容器成功投递均完成。低并发入口 p99 达到 100ms 目标；10 并发基准全部正确受理，但延迟明显超过目标，验证了 README 中 SQLite 只适合低吞吐 MVP 的边界。

## 测试环境

- Windows，Asia/Shanghai
- Python 3.12.4
- pytest 8.4.2，pytest-asyncio 0.26.0，pytest-cov 6.3.0
- Docker 27.2.0，Docker Compose v2.29.2，Docker Desktop Linux engine
- 容器：FastAPI API、手写 Redis Streams worker、Redis 7.4-alpine

## 自动化测试与覆盖率

执行命令：

```bash
python -m pytest --cov=app --cov-report=term-missing --cov-fail-under=60
```

结果：

- 45 collected，45 passed，0 failed
- 总覆盖率：77.36%（门槛 60%）
- `app/api/routes.py`：88%
- `app/dispatcher/retry_policy.py`：97%
- `app/dispatcher/renderer.py`：100%
- `app/worker/tasks.py`：92%
- `app/queue/redis_stream.py`：73%
- `app/db/engine.py`：99%

覆盖行为包括：API 契约、未知 vendor/event、payload 类型、健康降级、顺序与 10 并发幂等、24 小时 key 清理、2xx、400、500 到上限、429 Retry-After、timeout、vendor 配置缺失、终态跳过、pending/running 恢复、Redis Stream read/ack、严格模板渲染，以及 API 到最终状态的三类进程内 E2E。

## 静态与配置检查

```bash
python -m ruff check .
python -m compileall -q app tests
docker compose config -q
```

结果：三个命令均退出码 0；ruff 输出 `All checks passed!`。

## Docker 冒烟与真实集成

使用 `tests/fake_vendor_server.py` 在宿主机启动真实 HTTP 200 server，然后执行：

```bash
docker compose up --build -d
```

最终容器状态：

- `api`：Up / healthy，端口 8000
- `worker`：Up / healthy
- `redis`：Up / healthy

验证结果：

1. `GET /health` → HTTP 200，`{"status":"ok","redis":"up","db":"up"}`。
2. `POST /notify` → HTTP 202，ID `01M1EW5VB0AACVPX7G6QE8RWND`，初始 pending。
3. worker 通过真实容器网络 POST fake vendor，供应商返回 HTTP 200。
4. `GET /notifications/{id}` → success、attempts=1、last_http_status=200。
5. 同一 Idempotency-Key 再次 POST → HTTP 200、同一 ID、status=success、idempotent_hit=true。
6. unknown vendor → HTTP 400。

## 入口延迟基准

基准包含本机客户端、Docker Desktop 端口转发、API、SQLite 事务和 Redis XADD，不是仅测路由函数。供应商投递在 worker 异步执行，不包含在入口耗时中。

### 持久连接、顺序 100 请求

- HTTP 202：100/100
- p50：31.94ms
- p95：54.29ms
- p99：61.63ms
- max：109.13ms

结论：低并发 p99 达到目标 p99 ≤100ms。

### 10 客户端并发、总计 100 请求

- HTTP 202：100/100
- p50：2098.78ms
- p95：2176.14ms
- p99：2233.70ms
- max：2237.71ms

结论：正确性通过，但高并发延迟不达 100ms。基准脚本为每个调用建立客户端连接，因此包含明显连接开销；同时 SQLite 写串行化是主要架构限制之一。若 100ms 是跨环境硬 SLA，应改用 PostgreSQL、复用客户端连接，并建立固定硬件与负载模型的正式压测。

## 测试中发现并修复的问题

1. 第一轮 pytest：`idempotency_keys` 可能先于 notification flush，触发 SQLite FK 失败。修复为同一事务中显式 flush 父记录后插入 key；45 项复测全绿。
2. 第一轮 Docker build：`uvicorn<1` 比 PRD 的 `^0.32` 过宽，pip 大量回溯。修正为 `>=0.32,<0.33`。
3. 第二轮 Docker build：官方 PyPI 下载 uvloop 发生 15 秒 read timeout。Dockerfile 增加 120 秒 timeout 与 10 次 retry，之后构建通过。
4. 首次容器投递：Linux worker 无法解析 `host.docker.internal`。Compose 增加显式 `host-gateway`，新通知一次投递成功。

以上失败均未作为通过结果；报告中的最终结果来自修正后的重新执行。
