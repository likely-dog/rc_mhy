"""Integration tests for TTL cleanup and database-backed stale recovery."""

from datetime import UTC, datetime, timedelta

import pytest
from ulid import ULID

from app.main import cleanup_idempotency, recover_stale


async def _insert(context, now):
    notification_id = str(ULID())
    await context.db.insert_notification_and_idempotency(
        notification_id,
        f"key-{notification_id}",
        "test_vendor",
        "created",
        {"value": "x"},
        now,
    )
    return notification_id


@pytest.mark.asyncio
async def test_cleanup_when_key_older_than_ttl_allows_key_reuse(context):
    """Given a 25-hour-old key, cleanup removes its idempotency ownership."""
    old = datetime.now(UTC) - timedelta(hours=25)
    notification_id = await _insert(context, old)
    assert await cleanup_idempotency(context, datetime.now(UTC)) == 1
    assert await context.db.get_by_idempotency_key(f"key-{notification_id}") is None


@pytest.mark.asyncio
async def test_recover_when_pending_due_enqueues_from_database(context):
    """Given due pending state, recovery reconstructs a missing stream message."""
    notification_id = await _insert(context, datetime.now(UTC))
    context.stream.enqueued.clear()
    count = await recover_stale(context, datetime.now(UTC))
    assert count == 1
    assert context.stream.enqueued == [notification_id]


@pytest.mark.asyncio
async def test_recover_when_running_stale_resets_and_enqueues(context):
    """Given a worker died after claim, timed-out running state returns to pending."""
    old = datetime.now(UTC) - timedelta(minutes=2)
    notification_id = await _insert(context, old)
    await context.db.mark_running(notification_id, 1, old)
    context.stream.enqueued.clear()
    await recover_stale(context, datetime.now(UTC))
    notification = await context.db.get(notification_id)
    assert notification.status == "pending"
    assert notification_id in context.stream.enqueued
