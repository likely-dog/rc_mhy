# AI Coding Agent 实施指令：API 通知中转服务 MVP

> **文档定位**：这份文档写给**执行编码的 code agent**（Cursor / Claude Code / Aider 等），不是写给人的教程。
> **上位方案**：飞书设计文档《AI Coding 作业规划：API 通知中转服务（Python MVP）》。本文档是它的**落地施工蓝图**。
> **执行原则**：严格按阶段推进，每阶段完成 **Acceptance Criteria (AC)** 才能进入下一阶段。**禁止跨阶段乱写**、**禁止引入未列出的依赖**、**禁止扩大边界**。

---

## 0. 项目速览（Agent 先读这一节）

| Item     | Value                                                        |
| -------- | ------------------------------------------------------------ |
| Repo 名  | `rc_{your_nickname}`                                         |
| 语言     | Python **3.11+**                                             |
| 主要依赖 | fastapi, uvicorn[standard], httpx, redis, sqlalchemy[asyncio], aiosqlite, pydantic, pydantic-settings, pyyaml, structlog, jinja2, python-ulid, pytest, pytest-asyncio, respx, fakeredis |
| 运行方式 | `docker compose up` 一键起                                   |
| 交付     | GitHub repo（代码 + README + 设计文档 + AI 使用说明）        |
| 时间盒   | 5 天，每天一个阶段 + 收尾                                    |

**你要构建的是**：一个内部 HTTP 服务，接收内部业务系统的"发通知"请求，异步、可靠地转发到外部供应商 API。

**语义**：**At-least-once** 投递，业务方通过 `Idempotency-Key` 做去重。

---

## 1. 硬性边界（Boundary Conditions — Agent 禁区）

> ⚠️ 下面每一条都是**不可协商的约束**。触发任何一条视为方案偏离，必须回退。

### 1.1 范围内（MUST 实现）
- `POST /notify` 接口，接收标准入参，同步返回 `202 Accepted`（≤ 100ms）。
- `GET /health`、`GET /notifications/{id}`（只读，方便调试）。
- 请求持久化到本地 DB，字段完整（见 §4）。
- 通过 Redis Streams 解耦 API 层和 Worker 层。
- Worker 从队列取任务、按 YAML 配置拼装外部请求、用 httpx 发出。
- 失败按指数退避 + 抖动重试；达到上限进 DLQ 表。
- 幂等：同一 `Idempotency-Key` 在 24h 内视为同一请求，返回同一 `notification_id`。
- HTTP 错误分类：**4xx 不重试**（`408`、`429` 除外），**5xx / timeout / connection error / 429** 重试。
- Docker Compose 起 api + worker + redis 三容器。
- pytest 覆盖率 ≥ 60%，且核心路径（幂等、重试、错误分类、DLQ）必须有用例。

### 1.2 范围外（MUST NOT 做）
- ❌ 不做鉴权（不引入 JWT / OAuth / API Key），内部信任模型足够。
- ❌ 不做管理后台 UI。
- ❌ 不做 Adapter 类继承体系，供应商差异**只能**通过 YAML 配置吃掉。
- ❌ 不引入 Kafka / RabbitMQ / Celery / Temporal / Prometheus / Grafana / Jaeger / gRPC。
- ❌ 不做多租户 / 限流 / 熔断（可在 README 里作为"未来演进"说明）。
- ❌ 不做业务方回调 / Webhook 反向通知。
- ❌ 不做 Exactly-once。
- ❌ 不做数据库 migration 框架（MVP 直接 `create_all` 建表）。

### 1.3 依赖白名单
仅允许下面 `pyproject.toml` 的依赖，**任何额外依赖必须先在 README 记录理由**：

```toml
[project.dependencies]
fastapi = "^0.115"
uvicorn = { version = "^0.32", extras = ["standard"] }
httpx = "^0.27"
redis = "^5.2"
sqlalchemy = { version = "^2.0", extras = ["asyncio"] }
aiosqlite = "^0.20"
pydantic = "^2.9"
pydantic-settings = "^2.6"
pyyaml = "^6.0"
structlog = "^24.4"
jinja2 = "^3.1"
python-ulid = "^3.0"

[project.optional-dependencies]
dev = ["pytest ^8.3", "pytest-asyncio ^0.24", "respx ^0.21", "ruff ^0.7", "fakeredis ^2.24"]
```

> 白名单修订说明：已移除 `arq`（改为手写 Stream 消费）与 `tenacity`（重试逻辑为手写退避，未使用）；已加入 `jinja2`（模板渲染）、`python-ulid`（ULID 生成，Python 3.11 无原生 ULID）、`fakeredis`（测试用 Redis，Streams 支持完整，替代 miniredis）。不引入 `freezegun`：与 asyncio 事件循环时间不兼容，时间相关测试改用「注入 now」。

---

## 2. 目录结构（严格遵循，不许改名）

```
rc_{your_nickname}/
├── README.md                    # 设计说明 + AI 使用说明（用户交付物）
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
├── .env.example
├── config/
│   └── vendors.yaml             # 供应商模板配置
├── app/
│   ├── __init__.py
│   ├── main.py                  # FastAPI 入口 + lifespan（周期 stale 回收）
│   ├── settings.py              # Pydantic Settings（读 .env）
│   ├── logging_setup.py         # structlog 配置
│   ├── api/
│   │   ├── __init__.py
│   │   ├── routes.py            # 路由定义
│   │   └── schemas.py           # 请求/响应 Pydantic 模型
│   ├── db/
│   │   ├── __init__.py
│   │   ├── engine.py            # AsyncEngine + Session factory
│   │   └── models.py            # SQLAlchemy ORM: Notification, DeadLetter
│   ├── queue/
│   │   ├── __init__.py
│   │   └── redis_stream.py      # 封装 XADD / XREADGROUP / XACK / XPENDING / XCLAIM
│   ├── worker/
│   │   ├── __init__.py
│   │   ├── consumer.py          # 手写 Stream 消费循环 + 优雅退出 + XCLAIM 回收
│   │   └── tasks.py             # dispatch_notification（注入 AppContext 的纯函数）
│   ├── dispatcher/
│   │   ├── __init__.py
│   │   ├── vendors.py           # 加载 & 校验 vendors.yaml
│   │   ├── renderer.py          # URL / Header / Body 模板渲染
│   │   ├── http_client.py       # httpx AsyncClient 单例
│   │   └── retry_policy.py      # 错误分类 + 退避计算
│   └── domain/
│       ├── __init__.py
│       └── enums.py             # NotificationStatus, ErrorClass
├── tests/
│   ├── __init__.py
│   ├── conftest.py              # fixtures: app, client, redis, db
│   ├── test_api_notify.py
│   ├── test_idempotency.py
│   ├── test_dispatcher.py
│   ├── test_retry_policy.py
│   └── test_e2e.py              # 起 fake vendor server 端到端跑
└── docs/
    ├── design.md
    ├── ai-usage.md
    └── architecture.png         # 可选：架构图导出
```

---

## 3. API 契约（前后端接口定义，禁止擅自改字段名）

### 3.1 `POST /notify`

**Headers**
| Header            | 必填 | 说明                                        |
| ----------------- | ---- | ------------------------------------------- |
| `Content-Type`    | ✅    | `application/json`                          |
| `Idempotency-Key` | ✅    | UUID/雪花 ID/任意 ≤128 字符字符串；用于去重 |

**Body**
```json
{
  "vendor": "ads_system_a",
  "event_type": "user_registered",
  "payload": {
    "user_id": "u_123",
    "campaign_id": "cmp_456",
    "timestamp": "2026-09-01T10:00:00Z"
  }
}
```

**Response（成功入队）** — HTTP `202 Accepted`
```json
{
  "notification_id": "01J...ULID",
  "status": "pending",
  "idempotent_hit": false
}
```

**Response（幂等命中）** — HTTP `200 OK`
```json
{
  "notification_id": "01J...ULID",
  "status": "pending|success|failed|dead",
  "idempotent_hit": true
}
```

**Response（校验失败）** — HTTP `422 Unprocessable Entity`（FastAPI 默认）

**Response（vendor 不存在）** — HTTP `400 Bad Request`
```json
{ "detail": "unknown vendor: xxx" }
```

### 3.2 `GET /notifications/{notification_id}`
返回当前状态、attempts、last_error、created_at 等（调试用）。

### 3.3 `GET /health`
返回 `{"status": "ok", "redis": "up", "db": "up"}`；任一底层挂了返回 `503`。

---

## 4. 数据模型契约（DDL）

### 4.1 `notifications` 表
| 字段               | 类型      | 约束            | 说明                                                  |
| ------------------ | --------- | --------------- | ----------------------------------------------------- |
| `id`               | TEXT(26)  | PK              | ULID                                                  |
| `idempotency_key`  | TEXT(128) | NOT NULL, INDEX | 幂等键（唯一性由 `idempotency_keys` 表保证，见 §4.2） |
| `vendor`           | TEXT(64)  | NOT NULL, INDEX | 供应商 code                                           |
| `event_type`       | TEXT(64)  | NOT NULL        | 事件类型                                              |
| `payload_json`     | TEXT      | NOT NULL        | 原始 payload JSON 字符串                              |
| `status`           | TEXT(16)  | NOT NULL, INDEX | pending/running/success/failed/dead                   |
| `attempts`         | INTEGER   | DEFAULT 0       | 已尝试次数                                            |
| `next_retry_at`    | DATETIME  | NULL            | 下次重试时间（UTC）                                   |
| `last_error`       | TEXT      | NULL            | 上次错误摘要（截断到 1KB）                            |
| `last_http_status` | INTEGER   | NULL            | 上次响应状态码                                        |
| `created_at`       | DATETIME  | NOT NULL        | UTC                                                   |
| `updated_at`       | DATETIME  | NOT NULL        | UTC                                                   |

### 4.2 `idempotency_keys` 表（幂等去重 + 24h TTL 清理）
| 字段              | 类型      | 约束                            | 说明                   |
| ----------------- | --------- | ------------------------------- | ---------------------- |
| `idempotency_key` | TEXT(128) | PK                              | 幂等键，唯一性在此保证 |
| `notification_id` | TEXT(26)  | NOT NULL, FK → notifications.id | 归属的通知             |
| `created_at`      | DATETIME  | NOT NULL                        | UTC，用于 TTL 清理     |

> 幂等去重的唯一性由本表 PK 保证；`notifications.idempotency_key` 仅为普通索引列（便于调试、免 join）。后台协程定期删除 `created_at < now - IDEMPOTENCY_TTL_SECONDS` 的行（见 §7.1 / Phase 2）。

### 4.3 `dead_letters` 表
| 字段              | 类型     | 说明                                                         |
| ----------------- | -------- | ------------------------------------------------------------ |
| `id`              | TEXT(26) | PK, ULID                                                     |
| `notification_id` | TEXT(26) | FK → notifications.id                                        |
| `reason`          | TEXT(32) | max_retries_exceeded / non_retryable_4xx / vendor_config_missing |
| `dumped_at`       | DATETIME | UTC                                                          |
| `snapshot_json`   | TEXT     | notification 完整快照                                        |

### 4.4 状态机
```
pending ──▶ running ──▶ success        （2xx）
    ▲          │
    │          ├──▶ pending (重投，attempts++, next_retry_at 设置)
    │          │
    └──────────┴──▶ dead                （4xx 不可重试 or 超过 max_attempts）

running ──(超时: updated_at < now - RUNNING_TIMEOUT_SECONDS)──▶ pending   （stale 回收，见 §7.4）
```

---

## 5. 配置契约（`config/vendors.yaml`）

```yaml
vendors:
  - code: ads_system_a
    endpoint: "https://vendor-a.example.com/notify"
    method: POST
    timeout_seconds: 5
    headers:
      Authorization: "Bearer ${ADS_A_TOKEN}"      # 支持 ${ENV} 展开
      Content-Type: "application/json"
    body_template: |
      {
        "uid": "{{ payload.user_id }}",
        "campaign": "{{ payload.campaign_id }}",
        "ts": "{{ payload.timestamp }}"
      }

  - code: crm_system_b
    endpoint: "https://crm.example.com/v1/contacts/{{ payload.contact_id }}"
    method: PATCH
    timeout_seconds: 8
    headers:
      X-API-Key: "${CRM_B_KEY}"
    body_template: |
      {"status": "paid"}
```

**渲染规则**：
- 模板引擎用 Jinja2（`jinja2` 已加入依赖白名单，见 §1.3）。
- 第一段——加载时：对原始模板做一次 `${ENV_VAR}` 环境变量展开（`os.path.expandvars`，只认 `${...}`）；`${...}` 不是 Jinja2 语法，故与第二段互不干扰。
- 第二段——分发时：用 Jinja2 渲染 `{{ payload.* }}`，并用 `StrictUndefined`：payload 缺字段时抛错（归类 NON_RETRYABLE / 400），而不是静默渲染成空。
- 启动时校验：`code` 唯一、`endpoint` 是合法 URL、`method` ∈ {GET, POST, PUT, PATCH, DELETE}。校验失败进程直接 exit(1)。

---

## 6. 错误分类与重试策略（重点表格）

### 6.1 分类表
| 情况                       | ErrorClass    | 是否重试 | 备注                                                       |
| -------------------------- | ------------- | -------- | ---------------------------------------------------------- |
| HTTP 2xx                   | SUCCESS       | —        | 标记 success，收工                                         |
| HTTP 3xx                   | NON_RETRYABLE | ❌        | 不跟随重定向；配置错，进 DLQ                               |
| HTTP 400/401/403/404/422   | NON_RETRYABLE | ❌        | 业务/配置错，进 DLQ                                        |
| HTTP 408 Request Timeout   | RETRYABLE     | ✅        | 按退避重试                                                 |
| HTTP 429 Too Many Requests | RETRYABLE     | ✅        | **优先读 `Retry-After` header 决定 delay**，读不到才用退避 |
| HTTP 5xx                   | RETRYABLE     | ✅        | 按退避重试                                                 |
| `httpx.TimeoutException`   | RETRYABLE     | ✅        | 按退避重试                                                 |
| `httpx.ConnectError` / DNS | RETRYABLE     | ✅        | 按退避重试                                                 |
| 其他未知异常               | RETRYABLE     | ✅        | 按退避重试（保守）                                         |

### 6.2 退避参数
```python
BASE_DELAY_SECONDS = 1
MAX_DELAY_SECONDS = 300  # 5 分钟封顶
MAX_ATTEMPTS = 10
JITTER_RATIO = 0.2  # ±20% 抖动


def next_delay(attempts: int) -> float:
    exp = min(BASE_DELAY_SECONDS * (2**attempts), MAX_DELAY_SECONDS)
    jitter = exp * JITTER_RATIO * (random.random() * 2 - 1)
    return max(0.1, exp + jitter)
```

**总重试时长**：10 次约 17 分钟内结束，避免任务在队列里滞留过久。

---

## 7. 关键流程伪代码（Agent 请严格照实现）

### 7.1 API 层接收
```python
async def post_notify(req: NotifyRequest, idempotency_key: str, session):
    # 1) 校验 vendor 存在
    if req.vendor not in vendor_registry:
        raise HTTPException(400, f"unknown vendor: {req.vendor}")

    # 2) 落库 + 幂等去重（单事务原子提交）
    nid = ulid()
    try:
        # 一个事务内同时插入 notification + idempotency_key，任一失败整体回滚
        await session.insert_notification_and_idempotency(nid, idempotency_key, req, now_utc())
    except IntegrityError:
        # 并发同 key 竞态：整事务回滚，孤儿 notification 一并消失
        winner = await session.get_notification_by_key(idempotency_key)
        return NotifyResponse(id=winner.id, status=winner.status, idempotent_hit=True), 200

    # 3) 入队（XADD 到 Redis Streams）—— commit 成功后才入队
    await stream.enqueue(nid)

    # 4) 立即返回 202
    return NotifyResponse(id=nid, status="pending", idempotent_hit=False), 202
```

> ⚠️ 2 和 3 之间理论上有极小的失败窗口（commit 成功但 XADD 前崩溃）。MVP 用「周期 stale 回收」兜底：定期把 `status='pending'` 且 `next_retry_at IS NULL OR next_retry_at <= now()` 的记录重新入队（见 §7.4）。

### 7.2 Worker 分发
```python
async def dispatch_notification(ctx, notification_id):
    # ctx 是注入的 AppContext（含 db / stream / http_client / vendor_registry），
    # 生产由 consumer 传入，测试可传 fake——不再是 arq 的 ctx。
    n = await ctx.db.get(notification_id)
    if n is None or n.status in ("success", "dead"):
        return  # 幂等：已处理过就跳

    await ctx.db.mark_running(n.id, attempts=n.attempts + 1)  # mark_* 必须同时写 updated_at=now
    try:
        vendor = ctx.vendor_registry[n.vendor]
        req = render_request(vendor, n.payload)
        resp = await ctx.http_client.request(**req, timeout=vendor.timeout_seconds)
        cls = classify(resp)
    except (httpx.TimeoutException, httpx.ConnectError) as e:
        cls, resp = ErrorClass.RETRYABLE, None
        last_error = repr(e)[:1024]

    if cls == SUCCESS:
        await ctx.db.mark_success(n.id)
    elif cls == NON_RETRYABLE:
        await ctx.db.mark_dead(n.id, reason="non_retryable_4xx", last=resp)
        await ctx.db.write_dead_letter(n)
    else:  # RETRYABLE
        if n.attempts >= MAX_ATTEMPTS:
            await ctx.db.mark_dead(n.id, reason="max_retries_exceeded", last=resp)
            await ctx.db.write_dead_letter(n)
        else:
            delay = respect_retry_after(resp) or next_delay(n.attempts)
            await ctx.db.mark_pending(n.id, next_retry_at=now_utc() + delay)
            await ctx.stream.enqueue_delayed(n.id, delay)
```

### 7.3 延迟入队
- 简化做法：`await asyncio.sleep(delay)` 后 `XADD`。
- 更优（可选）：用 Redis Sorted Set 存 `(next_retry_at, id)`，独立 scheduler 协程定时把到期任务 XADD 到主 stream。**MVP 推荐第一种**，实现简单。

### 7.4 stale 回收（running 超时 + pending 到期兜底）
- 由 API lifespan 启动后台协程，启动时执行一次，之后每 `RECOVERY_INTERVAL_SECONDS` 跑一次：
```python
async def recover_stale(ctx, now):
    # 1) 卡死的 running 重置回 pending（要求 mark_running 写过 updated_at）
    stuck = await ctx.db.reset_running_stale(now - RUNNING_TIMEOUT_SECONDS)
    # UPDATE notifications SET status='pending', next_retry_at=:now, updated_at=:now
    #  WHERE status='running' AND updated_at < :cutoff RETURNING id
    # 2) 到期的 pending 重新入队
    due = await ctx.db.find_due_pending(now)
    # WHERE status='pending' AND (next_retry_at IS NULL OR next_retry_at <= :now)
    for nid in stuck + due:
        await ctx.stream.enqueue(nid)
```
- 配置：`RUNNING_TIMEOUT_SECONDS = 60`（须 > 最大 vendor timeout 8s）、`RECOVERY_INTERVAL_SECONDS = 30`。

---

## 8. 分阶段实施计划（每阶段有 AC，未通过不进入下一阶段）

### Phase 1 — 骨架 & API 通路（Day 1）
**任务**
1. 初始化 repo：`pyproject.toml` + `.gitignore` + `pre-commit`（可选）
2. 实现 `app/settings.py`、`app/logging_setup.py`、`app/main.py` lifespan
3. 实现 `app/db/models.py`、`app/db/engine.py`（启动 `create_all`）
4. 实现 `app/api/schemas.py`、`app/api/routes.py`：`POST /notify`、`GET /health`、`GET /notifications/{id}`
5. `docker-compose.yml`：api + redis 两服务

**Acceptance Criteria — Phase 1**
- [ ] `docker compose up` 起服务无报错
- [ ] `curl -X POST localhost:8000/notify -H 'Idempotency-Key: k1' -H 'Content-Type: application/json' -d '{"vendor":"x","event_type":"t","payload":{}}'` 返回 400（vendor 不存在，因为 yaml 还没加载）
- [ ] `curl localhost:8000/health` 返回 `{"status":"ok",...}`
- [ ] DB 里能查到写入的记录（如果 vendor 校验通过）
- [ ] `pytest tests/test_api_notify.py` 通过

---

### Phase 2 — 供应商配置 & 幂等（Day 2 上午）
**任务**
1. 实现 `config/vendors.yaml` 加载 + 校验 + `${ENV}` 展开
2. 完成 `POST /notify` 的 vendor 校验
3. 完成幂等：同 key 命中返回 200 + `idempotent_hit=true`（走 `idempotency_keys` 表原子插入，见 §7.1）
4. 幂等键唯一性：`idempotency_keys.idempotency_key` PK + 竞态处理（并发插同 key → `IntegrityError` → 查 & 返）
5. 实现幂等键 24h TTL 清理后台协程（`IDEMPOTENCY_TTL_SECONDS` / `IDEMPOTENCY_CLEANUP_INTERVAL_SECONDS`）

**Acceptance Criteria — Phase 2**
- [ ] 启动时 vendors.yaml 语法错会 exit(1) 并打清晰错误日志
- [ ] 已知 vendor 请求返回 202；未知 vendor 请求返回 400
- [ ] 同一 `Idempotency-Key` 二次请求返回 200 + `idempotent_hit=true` + **同一** `notification_id`
- [ ] 并发 10 次同 key 请求，DB 里只有 1 条记录
- [ ] 造一条 `created_at = now - 25h` 的幂等记录 → 跑清理 → 该行被删；再次用同 key 请求走「新请求」路径返回 202
- [ ] `pytest tests/test_idempotency.py` 通过

---

### Phase 3 — 队列 & Worker & 分发（Day 2 下午 + Day 3 上午）
**任务**
1. 实现 `app/queue/redis_stream.py`：XADD / XREADGROUP / XACK / XPENDING / XCLAIM；启动时 `XGROUP CREATE MKSTREAM`（幂等）
2. 实现 `app/dispatcher/renderer.py`（Jinja2 + `${ENV}` 两段式渲染，见 §5）
3. 实现 `app/dispatcher/http_client.py`（AsyncClient 单例 + 合理连接池）
4. 实现 `app/worker/tasks.py` 的 `dispatch_notification`（**只处理成功路径**，先不管重试）
5. 实现 `app/worker/consumer.py`：XREADGROUP 消费循环 + SIGTERM 优雅退出 + 启动/周期 XCLAIM 回收
6. 更新 `docker-compose.yml` 加 worker 服务（启动命令 `python -m app.worker.consumer`）

**Acceptance Criteria — Phase 3**
- [ ] `docker compose up` 起 api + worker + redis 三容器
- [ ] 用 [`httpbin.org/status/200`](https://httpbin.org/status/200) 或本地 mock 做 fake vendor：POST 一个 notification，30 秒内 DB status 变成 `success`
- [ ] Worker 处理时 log 有 request_id、vendor、attempts、latency_ms 字段
- [ ] Worker 优雅退出：`Ctrl+C` 不会丢已 claim 但未完成的消息（下次启动能被 XCLAIM 拉回）

---

### Phase 4 — 重试 & DLQ（Day 3 下午 + Day 4 上午）
**任务**
1. 实现 `app/dispatcher/retry_policy.py`：错误分类 + `next_delay` + `respect_retry_after`
2. 完成 Worker 里的 RETRYABLE / NON_RETRYABLE 分支
3. 实现 `dead_letters` 表写入
4. 实现周期 stale 回收（§7.4）：`running` 超时重置回 `pending` + 到期 `pending` 重新入队（`RUNNING_TIMEOUT_SECONDS` / `RECOVERY_INTERVAL_SECONDS`）

**Acceptance Criteria — Phase 4**
- [ ] fake vendor 返回 500，观测到多次重试，attempts 单调递增，delay 指数增长
- [ ] 达到 `MAX_ATTEMPTS=10` 后进入 `dead` 状态 + `dead_letters` 表有记录
- [ ] fake vendor 返回 400，**只调用一次**，直接进 `dead`
- [ ] fake vendor 返回 429 with `Retry-After: 3`，下次重试 delay ≈ 3s（±1s 抖动可接受）
- [ ] kill api 进程再启动，未处理完的 pending 任务被重新入队
- [ ] 模拟 worker `mark_running` 后中断（进程被杀）→ `RUNNING_TIMEOUT_SECONDS` 后记录回到 `pending` 并重新入队
- [ ] `pytest tests/test_retry_policy.py` + `tests/test_dispatcher.py` 通过

---

### Phase 5 — 端到端 & 收尾（Day 4 下午 + Day 5）
**任务**
1. `tests/test_e2e.py`：起 `respx` mock 三个 fake vendor（分别恒 2xx / 恒 500 / 前 3 次 500 后 200），验收行为
2. 写 `README.md`（严格按 §11 模板）
3. 写 `docs/design.md`（完整设计说明）
4. 写 `docs/ai-usage.md`（AI 使用说明，见 §12）
5. `ruff check` + `ruff format` 无 warning
6. 记录一段 30–60s 的 demo（可选，`asciinema` 或屏幕录像）

**Acceptance Criteria — Phase 5（也是全项目 DoD）**
- [ ] 全套 `pytest --cov=app` 覆盖率 ≥ 60%，核心路径全绿
- [ ] `docker compose up` 一键跑通 e2e 演示
- [ ] README 有：问题理解、架构图、关键决策与取舍、AI 使用说明、启动步骤、测试方法
- [ ] 无 lint / type warning
- [ ] Git commit 历史清晰（至少 5 个 commit，每阶段一个）

---

## 9. 测试用例生成指南（Agent 必须严格按这一节的流程产出测试）

> **本节目的**：给 code agent 一套**可复现**的方法论——从"读一段业务代码"到"产出一批高覆盖率、高信号的测试用例"。不要跳过方法论直接抄 §9.4 的清单，那只是最低要求。

### 9.1 测试用例的六步生成法（Test Case Derivation Method）

Agent 每写一个模块的测试前，**必须**按下面 6 步走一遍，把结果写在对应 test 文件顶部的 docstring 里，作为"我为什么这么设计测试"的存档。

```
Step 1: 识别被测单元（Subject Under Test）
   ├── 是纯函数 → 用单元测试
   ├── 是有 I/O 副作用的模块 → 用集成测试 + mock 边界
   └── 是跨多组件流程 → 用端到端测试
Step 2: 抽取"输入维度" + "输出维度"
Step 3: 用「等价类划分 + 边界值 + 异常路径」枚举场景
Step 4: 每个场景写成一条 Given-When-Then
Step 5: 落到 pytest 代码（函数名 = 场景 ID）
Step 6: 反向审查：这个测试挂了，能定位到什么 bug？定位不到 = 无效测试，删掉重写
```

#### Step 1：识别被测单元

| 单元类型       | 例子（本项目）                                  | 推荐测试类型                                                 |
| -------------- | ----------------------------------------------- | ------------------------------------------------------------ |
| 纯函数         | `next_delay(attempts)`, `classify(resp)`        | 单元测试，`@pytest.mark.parametrize` 一把梭                  |
| 有副作用的服务 | `dispatch_notification` (调 DB + Redis + HTTP)  | 集成测试：真 DB (SQLite in-memory) + fakeredis + mock HTTP (respx) |
| 端到端流程     | `POST /notify` → Worker → fake vendor → DB 终态 | E2E：全真组件，只 mock 最外层供应商                          |

#### Step 2：抽取输入 / 输出维度

以 `POST /notify` 为例：

**输入维度**
- Header：`Idempotency-Key` 是否存在、长度、字符集
- Body：`vendor`（合法 / 非法 / 缺失）、`event_type`、`payload` 类型（dict / null / string / 嵌套）
- 环境状态：DB 里是否已存在同 key 记录、Redis 是否可达、vendor 配置是否加载

**输出维度**
- HTTP status（202 / 200 / 400 / 422 / 503）
- Response body 字段（`notification_id`, `status`, `idempotent_hit`）
- 副作用（DB 是否新增行 / 是否入队）

#### Step 3：用三类技术枚举场景

对每个输入维度用下面三条组合：

| 技术           | 意思                                          | 例子                                                         |
| -------------- | --------------------------------------------- | ------------------------------------------------------------ |
| **等价类划分** | 把输入分成"行为一致的等价组"，每组挑 1 个代表 | `Idempotency-Key` 长度：{ 1、128、129 } → 前两组合法，最后一组超限 |
| **边界值分析** | 边界更容易出 bug，专门测                      | `attempts` 的边界：0、1、9、10、11                           |
| **异常路径**   | I/O 失败、并发冲突、非法数据                  | Redis 挂、DB 唯一冲突、payload 是 `None`                     |

#### Step 4：Given-When-Then 模板

每条用例的 docstring 必须写清楚：

```python
def test_post_notify_when_duplicate_idempotency_key_returns_200():
    """
    Given: DB 里已有 idempotency_key='k1' 的 pending 通知
    When:  再次 POST /notify 携带同一个 Idempotency-Key='k1'
    Then:  返回 200（不是 202），response.idempotent_hit=True，
           response.notification_id 与已有记录相同，DB 不新增行
    """
```

**函数命名规范**：`test_<单元>_when_<条件>_<期望>`。看到名字就知道在测什么，出错报告一眼定位。

#### Step 5：落到 pytest 代码（骨架示例）

**纯函数（表驱动）**
```python
import pytest
from app.dispatcher.retry_policy import classify, ErrorClass


@pytest.mark.parametrize(
    "status_code, expected",
    [
        (200, ErrorClass.SUCCESS),
        (204, ErrorClass.SUCCESS),
        (301, ErrorClass.NON_RETRYABLE),  # 3xx 不跟随
        (400, ErrorClass.NON_RETRYABLE),
        (401, ErrorClass.NON_RETRYABLE),
        (403, ErrorClass.NON_RETRYABLE),
        (404, ErrorClass.NON_RETRYABLE),
        (408, ErrorClass.RETRYABLE),  # 边界：408 特殊，重试
        (422, ErrorClass.NON_RETRYABLE),
        (429, ErrorClass.RETRYABLE),  # 边界：429 特殊，重试
        (500, ErrorClass.RETRYABLE),
        (502, ErrorClass.RETRYABLE),
        (503, ErrorClass.RETRYABLE),
        (504, ErrorClass.RETRYABLE),
    ],
)
def test_classify_by_status_code(status_code, expected):
    """按 HTTP 状态码分类，验证 §6.1 分类表的每一行"""
    resp = FakeResponse(status_code=status_code)
    assert classify(resp) == expected
```

**集成测试（mock 边界）**
```python
import pytest, respx, httpx
from app.worker.tasks import dispatch_notification


@pytest.mark.asyncio
async def test_dispatch_retries_on_500_until_max_attempts(db, redis, ctx):
    """
    Given: notification A，vendor 恒返回 500
    When:  被 Worker 处理 10 次
    Then:  attempts=10，status=dead，dead_letters 有记录，
           reason='max_retries_exceeded'
    """
    async with respx.mock:
        respx.post("https://vendor-a.example.com/notify").mock(
            return_value=httpx.Response(500, text="boom")
        )
        n = await db.create_notification(vendor="ads_system_a", payload={...})
        for _ in range(10):
            await dispatch_notification(ctx, n.id)

        n = await db.get(n.id)
        assert n.status == "dead"
        assert n.attempts == 10
        dl = await db.get_dead_letter(n.id)
        assert dl.reason == "max_retries_exceeded"
```

**E2E 测试（全真链路）**
```python
@pytest.mark.asyncio
async def test_e2e_success_path(app_client, redis, db, fake_vendor_200):
    """
    Given: fake_vendor 返回 2xx
    When:  POST /notify → Worker 消费队列
    Then:  30s 内 DB status 变为 success
    """
    r = await app_client.post("/notify", headers={"Idempotency-Key": "k1"}, json={...})
    assert r.status_code == 202
    nid = r.json()["notification_id"]

    await wait_until(lambda: db.get_status(nid) == "success", timeout=30)
```

#### Step 6：反向审查（Mutation Test 思路）

每写完一条测试，问自己：**如果我把被测函数改坏，这条测试会挂吗？**

- ❌ 无效示例：`assert result is not None` —— 函数改成永远返回空 dict 也过。
- ✅ 有效示例：`assert result["status"] == "success" and result["attempts"] == 1` —— 状态或计数错都会挂。

**做不到这一点的测试，就是覆盖率造假，删掉重写。**

---

### 9.2 每类测试的用例模板（照抄改字段即可）

#### 9.2.1 API 层测试模板（`test_api_*.py`）

对每个 endpoint 至少覆盖 5 类场景：

| 场景类型         | 具体用例                                    | 断言重点                                |
| ---------------- | ------------------------------------------- | --------------------------------------- |
| **Happy Path**   | 合法请求                                    | 状态码 + response 字段 + DB 新增 + 入队 |
| **参数校验失败** | 缺必填、类型错、超长、非法枚举              | 422 + `detail` 里能定位到字段           |
| **业务校验失败** | vendor 不存在、payload 不符合 vendor schema | 400 + 错误消息                          |
| **幂等命中**     | 同 key 二次请求                             | 200 + 同 id + `idempotent_hit=true`     |
| **并发**         | asyncio.gather 10 次同 key 请求             | DB 只 1 条 + 全部返回同一 id            |

#### 9.2.2 分发器测试模板（`test_dispatcher.py`）

对每种 HTTP 响应 + 每种网络异常各一条：

| 输入                     | 期望                                                         |
| ------------------------ | ------------------------------------------------------------ |
| 200                      | status=success, attempts=1, dead_letters 空                  |
| 204                      | 同上                                                         |
| 400                      | status=dead, attempts=1, dead_letters.reason='non_retryable_4xx' |
| 401/403/404/422          | 同 400                                                       |
| 408                      | 走重试分支                                                   |
| 429 + `Retry-After: 3`   | 下次 next_retry_at ≈ now+3s（±抖动）                         |
| 429 无 Retry-After       | 走标准退避                                                   |
| 500/502/503/504          | 走重试分支                                                   |
| `httpx.TimeoutException` | 走重试分支，last_error 含 "Timeout"                          |
| `httpx.ConnectError`     | 走重试分支                                                   |
| 未知异常                 | 走重试分支（保守）                                           |

#### 9.2.3 纯函数测试模板（`test_retry_policy.py`）

- `classify`：§6.1 表每一行一个 case（`@parametrize`）
- `next_delay`：
  - 单调递增（`next_delay(i) < next_delay(i+2)` 至少 95% 概率成立）
  - 上限：`next_delay(20) <= MAX_DELAY_SECONDS * (1 + JITTER_RATIO)`
  - 下限：`next_delay(0) >= 0.1`
  - 抖动存在：连续调用 100 次，标准差 > 0
- `respect_retry_after`：
  - 合法秒数 → 返回该秒数
  - HTTP date 格式 → 返回相对秒数（可选支持）
  - 非法值 → 返回 None，让上游走 next_delay

#### 9.2.4 幂等测试模板（`test_idempotency.py`）

| 场景                   | 断言                                                         |
| ---------------------- | ------------------------------------------------------------ |
| 串行两次同 key         | 第 2 次 200 + 同 id + DB 1 条                                |
| 并发 N 次同 key (N=10) | DB 1 条 + 全部 200/202 + id 一致                             |
| 同 key 但 payload 不同 | **本 MVP 策略**：以第一次为准，返回 idempotent_hit=true（在 README 里明确记录该策略） |
| 不同 key 相同 payload  | 视为两条独立请求，DB 2 条                                    |

#### 9.2.5 端到端测试模板（`test_e2e.py`）

至少 3 条完整链路：

| 场景                            | 期望终态                                 |
| ------------------------------- | ---------------------------------------- |
| Vendor 恒 2xx                   | `success`，attempts=1，耗时 < 10s        |
| Vendor 前 3 次 500，第 4 次 200 | `success`，attempts=4                    |
| Vendor 恒 500                   | `dead`，attempts=10，dead_letters 有记录 |

---

### 9.3 Fixture / 环境的准备清单（`conftest.py` 必须提供）

```python
# tests/conftest.py 需要提供的 fixture：
@pytest.fixture async def db():                # 每个 test 独立 SQLite in-memory
@pytest.fixture async def redis():             # fakeredis（Streams 支持完整，见 §1.3）
@pytest.fixture async def app_client():        # httpx AsyncClient 挂到 FastAPI app
@pytest.fixture async def ctx():               # 注入的 AppContext（db/redis_stream/http_client/vendor_registry），非 arq
@pytest.fixture async def fake_vendor_200():   # respx mock，恒 200
@pytest.fixture async def fake_vendor_500():   # respx mock，恒 500
@pytest.fixture def frozen_time():             # 可注入的 now 时钟（不用 freezegun，避免 asyncio 时间不兼容）
@pytest.fixture async def wait_until():        # 轮询辅助，超时抛错
```

**关键要求**：
- fixture 之间**不能有隐藏依赖**（DB 不能读 Redis 状态）；
- 每个 test 用完 fixture 必须清理（yield 后 truncate 表 / flushdb）；
- 时间相关测试**必须**注入可调时钟（`now` 参数），否则 CI 抖动会 flaky；不用 `freezegun`（与 asyncio 事件循环时间不兼容）。

---

### 9.4 最小测试用例清单（Phase 5 交付前必须全绿）

| 测试文件             | 用例                              | 期望                                |
| -------------------- | --------------------------------- | ----------------------------------- |
| test_api_notify.py   | 合法请求                          | 202 + notification_id + DB 有记录   |
| test_api_notify.py   | 缺 Idempotency-Key                | 422                                 |
| test_api_notify.py   | 未知 vendor                       | 400                                 |
| test_api_notify.py   | payload 不是 dict                 | 422                                 |
| test_api_notify.py   | vendor 字段过长                   | 422                                 |
| test_idempotency.py  | 同 key 二次请求                   | 200 + 同 id + `idempotent_hit=true` |
| test_idempotency.py  | 并发 10 次同 key                  | DB 只 1 条记录                      |
| test_idempotency.py  | 不同 key 相同 payload             | DB 2 条记录                         |
| test_dispatcher.py   | 2xx 响应                          | status=success, attempts=1          |
| test_dispatcher.py   | 500 响应重试到上限                | status=dead, dead_letters 有记录    |
| test_dispatcher.py   | 400 响应                          | status=dead，只调 1 次              |
| test_dispatcher.py   | 429 with Retry-After: 2           | 下次 delay ≈ 2s                     |
| test_dispatcher.py   | 429 无 Retry-After                | 走标准退避                          |
| test_dispatcher.py   | Timeout                           | attempts++, 走重试                  |
| test_dispatcher.py   | ConnectError                      | attempts++, 走重试                  |
| test_dispatcher.py   | Worker 处理 status=success 的任务 | 幂等跳过，不发请求                  |
| test_retry_policy.py | classify 各种响应（parametrize）  | 覆盖 §6.1 表每一行                  |
| test_retry_policy.py | next_delay 增长曲线               | 单调 + 有抖动 + ≤ MAX               |
| test_retry_policy.py | respect_retry_after 各种输入      | 合法/非法/缺失                      |
| test_e2e.py          | Vendor 恒 2xx                     | 30s 内 status=success               |
| test_e2e.py          | Vendor 前 3 次 500 后 2xx         | attempts=4, status=success          |
| test_e2e.py          | Vendor 恒 500                     | status=dead + DLQ                   |

---

### 9.5 测试用例质量 Checklist（跑 pytest 前 Agent 自查）

每写完一个 test 文件，Agent 必须逐条勾选：

- [ ] **命名清晰**：函数名遵循 `test_<单元>_when_<条件>_<期望>`
- [ ] **有 docstring**：每条 test 顶部都有 Given-When-Then
- [ ] **单一职责**：一条 test 只验证一个行为（多断言 OK，多场景不行）
- [ ] **独立可重跑**：随机顺序跑 3 遍都通过（`pytest -p no:randomly` vs `pytest`）
- [ ] **无外部依赖**：不联网、不依赖真实 vendor（除 fixture 起的 fake）
- [ ] **时间可控**：涉及退避 / 超时的用例注入可调时钟（`now` 参数），不用 `freezegun`
- [ ] **失败信息可诊断**：`assert x == y, f"got {x}, expected {y}"` 或使用 `pytest.approx`
- [ ] **能杀死突变**：手动把被测函数返回值改坏，测试必须挂
- [ ] **无 sleep 硬编码**：等待用 `wait_until` 轮询，不用 `asyncio.sleep(30)`

---

### 9.6 覆盖率与门槛

```bash
pytest --cov=app --cov-report=term-missing --cov-fail-under=60
```

**分层门槛**（比总覆盖率 60% 更严）：
- `app/dispatcher/retry_policy.py`：**≥ 95%**（纯函数无理由低于此）
- `app/dispatcher/renderer.py`：**≥ 90%**
- `app/api/routes.py`：**≥ 80%**
- `app/worker/tasks.py`：**≥ 70%**
- `app/queue/*`、`app/db/*`：**≥ 60%**

**低于门槛怎么办**：先看 `--cov-report=term-missing` 输出的 uncovered 行，判断是**真的没测**还是**这条分支不重要（如启动兜底/降级路径）**。真的没测 → 补测；不重要 → 用 `# pragma: no cover` 显式排除并在 PR 描述里说明。**不允许**为了凑覆盖率写空 assert。

---

### 9.7 Agent 生成测试的执行顺序（按此顺序，别乱）

1. **先写 fixture**（conftest.py），让所有 test 有干净的 DB / Redis / app_client。
2. **先写纯函数测试**（`test_retry_policy.py`）—— 最快见成效，也验证核心算法。
3. **再写 API 层测试**（`test_api_notify.py`, `test_idempotency.py`）—— 验证契约。
4. **再写分发器集成测试**（`test_dispatcher.py`）—— 验证核心业务逻辑。
5. **最后写 E2E**（`test_e2e.py`）—— 慢，跑 CI 时最后跑。
6. **跑 `pytest --cov`** 查漏；对着 §9.6 的分层门槛补测。
7. **跑 §9.5 Checklist** 自查每个文件。

---

## 10. 观测性最小集（不要过度设计）

- **日志**：structlog 输出 JSON；每条日志必带 `notification_id`（有的话）、`vendor`、`attempts`、`event`（如 `enqueued` / `dispatch_start` / `dispatch_success` / `dispatch_retry` / `dispatch_dead`）。
- **指标**（放在日志里即可，不搭 Prometheus）：`dispatch_latency_ms`、`http_status`、`error_class`。
- **健康检查**：`/health` 探测 redis + db。

---

## 11. README.md 结构模板（Agent 必须按这个组织）

```markdown
# API 通知中转服务（rc_your_nickname）

## 1. 问题理解
（一段话讲清楚需求本质：兜住外部不可靠性）

## 2. 快速启动
docker compose up
curl -X POST ... （放一个完整能跑的示例）

## 3. 整体架构
（贴架构图 + 4 层职责说明）

## 4. 关键工程决策
### 4.1 投递语义：At-least-once
### 4.2 队列：Redis Streams（为什么不用 Kafka/RabbitMQ）
### 4.3 供应商差异：YAML 配置（为什么不用 Adapter 类）
### 4.4 数据库：SQLite（为什么先不用 Postgres）
### 4.5 重试与错误分类
### 4.6 幂等策略

## 5. 系统边界
### 5.1 我解决的
### 5.2 我不解决的

## 6. 可靠性设计
### 6.1 消息不丢：先落库再入队 + 周期 stale 回收
### 6.2 消息不重（尽力）：Idempotency-Key + Redis XACK
### 6.3 外部长期不可用：退避上限 + DLQ + 可回灌

## 7. 目录结构
（贴 tree）

## 8. 测试
pytest && pytest --cov=app

## 9. 未来演进
（列 5-7 条）

## 10. AI 使用说明
见 docs/ai-usage.md
```

---

## 12. AI 使用说明写作要点（`docs/ai-usage.md`）

三块内容都要有**具体例子**，不能空话。示例：

```markdown
## AI 帮上的
- 生成手写 Stream 消费循环骨架，节省 15 分钟查文档时间。
- 生成 respx mock 用例模板。
- 建议用 ULID 而不是 UUID v4，被采纳。

## 没采纳的建议
- AI 提议引入 Celery + Redis + beat 做延迟队列 → 拒绝：手写 Stream 消费已够，Celery 配置成本高 3 倍。
- AI 提议给每个 vendor 建 Adapter 类 + 工厂 → 拒绝：与 YAML 模板路线冲突。
- AI 提议在 API 层做同步重试 → 拒绝：API 必须 < 100ms 返回，重试是 Worker 职责。
- AI 提议加 Prometheus + Grafana → 拒绝：MVP 结构化日志已够。

## 自己拍板的
- 「先落库再入队」三步顺序：为了防进程崩溃丢消息。
- 4xx 不重试的语义：符合 HTTP 规范，重试无意义。
- MAX_ATTEMPTS=10 而不是无限：避免队列越积越长。
- SQLite 起步：优先评审体验。
- 幂等责任划分给业务方：至少一次语义的兜底策略。
```

---

## 13. Agent 操作规范（写代码时的 do & don't）

### DO
- ✅ 每完成一个 Phase 停下来跑 AC 检查，全绿再进下一阶段。
- ✅ 每个 module 顶部有 module docstring 说明职责。
- ✅ 所有公共函数 type-hint 齐全（用 pyright / mypy 心里默扫一遍）。
- ✅ 所有对外 I/O（DB / Redis / httpx）都是 async 的。
- ✅ 时间戳统一 UTC，用 `datetime.now(timezone.utc)`。
- ✅ 所有 ID 用 ULID（`python-ulid`，见 §1.3）。
- ✅ 环境变量优先，`.env.example` 列出所有必需变量。

### DON'T
- ❌ 不要在 API 层调 httpx（分发是 Worker 的事）。
- ❌ 不要用同步 `requests` 库。
- ❌ 不要用 `time.sleep`（用 `await asyncio.sleep`）。
- ❌ 不要在 Worker 里做 DB 长事务（每个 task 短事务、及时提交）。
- ❌ 不要 catch `Exception` 又 `pass`，至少 log 出来。
- ❌ 不要把 secret 写进 vendors.yaml，只放 `${ENV}` 占位。
- ❌ 不要为了覆盖率写无意义 assert，测试要覆盖**真实分支**。

---

## 14. 最终 Definition of Done（交作业前的 Checklist）

- [ ] GitHub repo 名字符合 `rc_{your_nickname}` 规范
- [ ] `docker compose up` 一键起，无需额外命令
- [ ] `curl` 示例能在 README 里直接复制跑通
- [ ] pytest 全绿，覆盖率 ≥ 60%
- [ ] README 三大块齐全：问题理解、架构与决策、AI 使用说明
- [ ] `docs/ai-usage.md` 三块内容都有**具体例子**
- [ ] 未来演进路线列了 ≥ 5 条
- [ ] 没有引入白名单外的依赖
- [ ] 没有做边界外的功能（回头对照 §1.2 每一项）
- [ ] 至少 5 个语义清晰的 git commit

---

## 15. 应急预案（Agent 卡住时怎么办）

| 卡点                        | 应对                                                         |
| --------------------------- | ------------------------------------------------------------ |
| Redis Streams API 记不清    | 直接看官方 `XADD` / `XREADGROUP` / `XACK` / `XPENDING` / `XCLAIM` doc，别自己发明 |
| httpx timeout 设置          | 用 `httpx.Timeout(connect=2, read=vendor.timeout, write=2, pool=2)` |
| e2e 测试起 fake vendor 麻烦 | 用 respx 直接 mock；或起一个 `aiohttp` 内嵌 server           |
| 覆盖率不够 60%              | 补 retry_policy 单元测试最划算，纯函数好写                   |

---

**最后一句给 Agent**：这份文档的每一条约束都有工程理由，遇到"想省事"的诱惑先回头看 §1.2 和 §13.DON'T。作业的评审重点是**你能不能识别复杂度并主动管理**，MVP 的美德是"少做"而不是"多做"。

---

## 附录：评审待完善项记录（追加）

> 本节为后端评审后追加，供后续迭代决策。**P1 给出修改建议；P2 仅记录问题、不展开方案。**

### A. P1 — 需补设计（附修改建议）

> 已在 P0 修订中一并解决的 P1 项：#5（`python-ulid` 已入白名单）、#9（扫 pending 条件统一到 §7.4）、#11（consumer 启动/周期 XCLAIM 回收）。以下为仍需决策的项。

#### #6 SQLite 跨容器并发写
**问题**：api + worker 两个容器/进程共享同一 SQLite 文件，SQLite 写锁是库级的，并发写易触发 `database is locked`。
**修改建议**：
- 引擎初始化开启 `PRAGMA journal_mode=WAL` 与 `busy_timeout=5000`。
- docker-compose 用 named volume 挂载 DB 文件（如 `./data:/data`，路径统一 `/data/app.db`），api 与 worker 挂同一卷。
- aiosqlite 连接池用 `poolclass=NullPool`（连接不跨协程复用），并发写靠 WAL + busy_timeout 缓解。
- README「为什么不用 Postgres」补充：SQLite 单文件、多进程写受限，仅适用 MVP 低吞吐。

#### #7 `POST /notify` ≤100ms SLO 无验证
**问题**：硬性 ≤100ms 但无测量/验收手段，路径上（幂等查询 + 单事务插入 + XADD 跨容器）在低配环境未必守得住。
**修改建议**（二选一）：
- 降级为「目标 p99 ≤100ms（尽力而为）」，在 README 写明非硬 SLA；或
- 在 Phase 1/5 加一条 AC：本地 `hey`/`wrk` 压测（如 100 并发），记录 p99，超 100ms 时说明原因。

#### #10 延迟重试依赖 `asyncio.sleep`（内存态）
**问题**：`await asyncio.sleep(delay)` 后 XADD，worker 重启会丢未落定的延迟任务。
**修改建议**：
- MVP 接受该降级，靠 §7.4 周期 stale 回收兜底（`next_retry_at <= now` 的 pending 会被重新入队）；需在 README 写明：重启后到期的任务最迟在下一个 `RECOVERY_INTERVAL_SECONDS` 才入队，存在 ≤30s 额外延迟。
- 若想更严谨：改用 Redis Sorted Set 存 `(next_retry_at, id)` + 常驻 scheduler（§7.3 已列为「更优」）。

#### #12 vendor 缺失会无限重试而非进 DLQ
**问题**：§7.2 里 `ctx.vendor_registry[n.vendor]` 直接索引，vendor 缺失抛 `KeyError` → 落入「未知异常 RETRYABLE」无限重试，与 `dead_letters.reason` 中的 `vendor_config_missing` 不一致。
**修改建议**：
- 改为 `vendor = ctx.vendor_registry.get(n.vendor)`，为 `None` 时走 NON_RETRYABLE，`reason='vendor_config_missing'` 进 DLQ。
- 补测试：`test_dispatcher` 增加「vendor 配置缺失 → 只调用一次，进 DLQ」。

#### #14 `event_type` / `payload` schema 校验缺失
**问题**：§9.2.1 要求「payload 不符合 vendor schema → 400」，但 `vendors.yaml` 无 schema/event_type 定义，无法校验。
**修改建议**：
- MVP：在 `vendors.yaml` 每个 vendor 下加可选 `event_types: [...]`，`POST /notify` 校验 `event_type` 是否在列表内；payload 仅做 `dict` 类型校验、不做深度结构校验（README 说明）。
- 完整 schema（JSON Schema + pydantic 运行时校验）列为「未来演进」，不进 MVP。

#### #17 worker 存活探测 & SQLite volume
**问题**：worker 容器挂了不会自动重启；SQLite 文件在 api/worker 间的共享方式未定义。
**修改建议**：
- docker-compose 给 worker 加 `restart: unless-stopped`；worker 无 HTTP 端口，健康可用 `redis-cli -h redis PING` 或进程存活探测。
- 明确 SQLite 用 named volume 共享（见 #6），DB 路径在 `.env` 里统一。

#### #18 幂等命中的语义一致性
**问题**：§7.1 幂等命中一律返回 200 + 当前 status，未区分「pending（可继续等）」与终态「success/dead」。
**修改建议**：
- 保持 200 + status 透传，但在 §3.1 契约里补一句：命中 `dead` 时由调用方决定是否用新 key 重投；或命中 `dead` 返回 409。MVP 建议前者（改动最小），README 说明即可。

### B. P2 — 打磨项（仅记录问题，暂不展开方案）

- **#13 4xx 默认兜底未定义**：§6.1 只列了 400/401/403/404/422 与 408/429，其余 4xx（409/410/413/414/415/423 等）无归类规则，`classify()` 有未定义分支。
- **#15 幂等去重语义被过度承诺**：§6.2「消息不重（尽力）：Idempotency-Key + Redis XACK」混淆了「入口去重」与「下游不重」；worker 在 vendor 2xx 与 XACK 之间崩溃仍会造成下游重复投递（at-least-once 的固有代价）。
- **#16 `last_http_status` 写入时机未定义**：数据模型有该字段，但各 `mark_*` 流程未说明何时写（`updated_at` 已在 P0 修订中约定写入）。
- **#19 `next_delay` off-by-one**：§7.2 先 `attempts = n.attempts + 1`，§6.2 `2 ** attempts` 导致首轮退避即为 2s，`BASE_DELAY_SECONDS=1` 基本用不到。
- **#20 §7.2 用过期 attempts 判断死信**：`mark_running(attempts=n.attempts+1)` 后 `if n.attempts >= MAX_ATTEMPTS` 读的是内存旧值，存在 off-by-one。
- **#22 分层覆盖率门槛无法自动 enforce**：§9.6 只对总覆盖率 `--cov-fail-under=60` 生效，retry_policy ≥95% 等分层门槛靠人肉核对。
- **#23 e2e 时间口径不一致**：§9.2.5 写「Vendor 恒 2xx 耗时 <10s」，Phase 5 AC 写「30s 内 success」。
- **#24 e2e respx 进程边界**：`test_e2e.py` 用 respx mock vendor，但真 worker 是独立进程，respx 是进程内 mock，无法 mock 独立 worker 进程的 httpx 请求；需明确 e2e 是否把 worker 拉到测试进程内，或用内嵌 HTTP server 起 fake vendor。