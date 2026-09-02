"""Long-running Redis Streams consumer with acknowledgement and stale claiming."""

import asyncio
import signal
from contextlib import suppress

import structlog
from redis.asyncio import Redis

from app.context import AppContext
from app.db.engine import Repository, create_engine_and_session
from app.dispatcher.http_client import create_http_client
from app.dispatcher.vendors import load_vendor_registry
from app.logging_setup import configure_logging
from app.queue.redis_stream import RedisStream
from app.settings import get_settings
from app.worker.tasks import dispatch_notification

log = structlog.get_logger()


async def run_consumer() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine, session_factory = create_engine_and_session(settings.database_url)
    db = Repository(session_factory)
    await db.create_schema(engine)
    redis = Redis.from_url(settings.redis_url, decode_responses=True)
    stream = RedisStream(redis, settings.stream_name, settings.consumer_group)
    await stream.ensure_group()
    http_client = create_http_client()
    context = AppContext(
        settings,
        engine,
        db,
        stream,
        http_client,
        load_vendor_registry(settings.vendors_config_path),
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)

    await log.ainfo("worker_started", consumer=settings.consumer_name)
    try:
        await _consume(context, stop)
    finally:
        await http_client.aclose()
        await redis.aclose()
        await engine.dispose()
        await log.ainfo("worker_stopped", consumer=settings.consumer_name)


async def _consume(ctx: AppContext, stop: asyncio.Event) -> None:
    claim_interval = max(5, ctx.settings.running_timeout_seconds // 2)
    last_claim = 0.0
    loop = asyncio.get_running_loop()
    while not stop.is_set():
        messages = []
        now = loop.time()
        if now - last_claim >= claim_interval:
            messages.extend(
                await ctx.stream.claim_stale(
                    ctx.settings.consumer_name,
                    ctx.settings.running_timeout_seconds * 1000,
                )
            )
            last_claim = now
        response = await ctx.stream.read(ctx.settings.consumer_name, block_ms=1000)
        for _stream_name, entries in response or []:
            messages.extend(entries)
        for message_id, fields in messages:
            notification_id = fields.get("notification_id") or fields.get(b"notification_id")
            if isinstance(notification_id, bytes):
                notification_id = notification_id.decode()
            try:
                await dispatch_notification(ctx, str(notification_id))
            except Exception as exc:
                await log.aerror(
                    "dispatch_crashed",
                    notification_id=notification_id,
                    error=repr(exc),
                )
                continue
            await ctx.stream.acknowledge(message_id)


def main() -> None:
    asyncio.run(run_consumer())


if __name__ == "__main__":
    main()
