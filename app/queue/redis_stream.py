"""Small Redis Streams abstraction used by API, recovery, and worker."""

import asyncio
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError


class RedisStream:
    def __init__(self, redis: Redis, stream_name: str, group: str):
        self.redis = redis
        self.stream_name = stream_name
        self.group = group

    async def ensure_group(self) -> None:
        try:
            await self.redis.xgroup_create(self.stream_name, self.group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def enqueue(self, notification_id: str) -> str:
        message_id = await self.redis.xadd(self.stream_name, {"notification_id": notification_id})
        return message_id.decode() if isinstance(message_id, bytes) else str(message_id)

    async def enqueue_delayed(self, notification_id: str, delay: float) -> None:
        await asyncio.sleep(delay)
        await self.enqueue(notification_id)

    async def read(self, consumer: str, block_ms: int = 1000, count: int = 10):
        return await self.redis.xreadgroup(
            self.group,
            consumer,
            {self.stream_name: ">"},
            count=count,
            block=block_ms,
        )

    async def acknowledge(self, message_id: str) -> int:
        return await self.redis.xack(self.stream_name, self.group, message_id)

    async def claim_stale(
        self, consumer: str, min_idle_ms: int, count: int = 10
    ) -> list[tuple[Any, dict[Any, Any]]]:
        pending = await self.redis.xpending_range(
            self.stream_name, self.group, min="-", max="+", count=count
        )
        ids = [
            item["message_id"]
            for item in pending
            if item["time_since_delivered"] >= min_idle_ms
        ]
        if not ids:
            return []
        return await self.redis.xclaim(self.stream_name, self.group, consumer, min_idle_ms, ids)

    async def ping(self) -> None:
        await self.redis.ping()
