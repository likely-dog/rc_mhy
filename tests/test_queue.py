"""Redis Streams integration tests using an in-memory protocol-compatible server."""

import fakeredis.aioredis
import pytest

from app.queue.redis_stream import RedisStream


@pytest.mark.asyncio
async def test_stream_when_enqueued_can_read_and_acknowledge():
    """Given a consumer group, an enqueued ID is delivered and acknowledged."""
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream = RedisStream(redis, "notifications", "workers")
    await stream.ensure_group()
    await stream.enqueue("n-1")
    response = await stream.read("worker-1", block_ms=1)
    message_id, fields = response[0][1][0]
    assert fields["notification_id"] == "n-1"
    assert await stream.acknowledge(message_id) == 1
    await redis.aclose()


@pytest.mark.asyncio
async def test_stream_when_group_ensured_twice_is_idempotent():
    """Given an existing group, startup creation succeeds again."""
    redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
    stream = RedisStream(redis, "notifications", "workers")
    await stream.ensure_group()
    await stream.ensure_group()
    assert await redis.exists("notifications") == 1
    await redis.aclose()
