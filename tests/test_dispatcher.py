"""Integration tests for worker state transitions, retry, and dead letters."""

from datetime import UTC, datetime

import httpx
import pytest
from ulid import ULID

from app.worker.tasks import dispatch_notification


async def _insert(context, vendor="test_vendor", payload=None):
    notification_id = str(ULID())
    await context.db.insert_notification_and_idempotency(
        notification_id,
        f"key-{notification_id}",
        vendor,
        "created",
        payload or {"value": "x"},
        datetime.now(UTC),
    )
    return notification_id


def _client(handler):
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.mark.asyncio
async def test_dispatch_when_2xx_marks_success(context):
    """Given vendor success, one attempt reaches success."""
    await context.http_client.aclose()
    context.http_client = _client(lambda request: httpx.Response(204))
    notification_id = await _insert(context)
    assert await dispatch_notification(context, notification_id) == "success"
    notification = await context.db.get(notification_id)
    assert notification.status == "success"
    assert notification.attempts == 1
    assert notification.last_http_status == 204


@pytest.mark.asyncio
async def test_dispatch_when_400_marks_dead_once(context):
    """Given non-retryable HTTP 400, notification enters DLQ without requeue."""
    await context.http_client.aclose()
    context.http_client = _client(lambda request: httpx.Response(400))
    notification_id = await _insert(context)
    assert await dispatch_notification(context, notification_id) == "dead"
    assert (await context.db.get(notification_id)).attempts == 1
    assert await context.db.count_dead_letters() == 1
    assert context.stream.delays == []


@pytest.mark.asyncio
async def test_dispatch_when_500_retries_then_dead_at_exact_limit(context):
    """Given persistent 500, exactly max attempts are made before DLQ."""
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    await context.http_client.aclose()
    context.http_client = _client(handler)
    notification_id = await _insert(context)
    outcomes = [await dispatch_notification(context, notification_id) for _ in range(3)]
    assert outcomes == ["retry", "retry", "dead"]
    assert calls == context.settings.max_attempts == 3
    assert (await context.db.get(notification_id)).attempts == 3
    assert await context.db.count_dead_letters() == 1


@pytest.mark.asyncio
async def test_dispatch_when_429_respects_retry_after(context):
    """Given Retry-After seconds, scheduled delay uses the vendor value."""
    await context.http_client.aclose()
    context.http_client = _client(lambda request: httpx.Response(429, headers={"Retry-After": "2"}))
    notification_id = await _insert(context)
    assert await dispatch_notification(context, notification_id) == "retry"
    assert context.stream.delays == [2]


@pytest.mark.asyncio
async def test_dispatch_when_timeout_requeues(context):
    """Given transport timeout, attempt is retained and work is retried."""

    async def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    await context.http_client.aclose()
    context.http_client = _client(handler)
    notification_id = await _insert(context)
    assert await dispatch_notification(context, notification_id) == "retry"
    assert (await context.db.get(notification_id)).attempts == 1


@pytest.mark.asyncio
async def test_dispatch_when_vendor_config_missing_enters_dlq(context):
    """Given config removed after enqueue, work is non-retryable and diagnosable."""
    notification_id = await _insert(context, vendor="removed_vendor")
    assert await dispatch_notification(context, notification_id) == "dead"
    assert await context.db.count_dead_letters() == 1


@pytest.mark.asyncio
async def test_dispatch_when_already_success_skips_http(context):
    """Given a terminal success record, duplicate stream delivery does no HTTP I/O."""
    notification_id = await _insert(context)
    await context.db.mark_success(notification_id, 200, datetime.now(UTC))
    assert await dispatch_notification(context, notification_id) == "skipped"
