"""FastAPI application factory and lifecycle-owned recovery processes."""

import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path

import structlog
from fastapi import FastAPI
from redis.asyncio import Redis

from app.api.routes import router
from app.context import AppContext
from app.db.engine import Repository, create_engine_and_session
from app.dispatcher.http_client import create_http_client
from app.dispatcher.vendors import load_vendor_registry
from app.logging_setup import configure_logging
from app.queue.redis_stream import RedisStream
from app.settings import Settings, get_settings

log = structlog.get_logger()


def create_app(settings: Settings | None = None) -> FastAPI:
    resolved = settings or get_settings()

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        configure_logging(resolved.log_level)
        _ensure_sqlite_directory(resolved.database_url)
        engine, session_factory = create_engine_and_session(resolved.database_url)
        db = Repository(session_factory)
        await db.create_schema(engine)
        redis = Redis.from_url(resolved.redis_url, decode_responses=True)
        stream = RedisStream(redis, resolved.stream_name, resolved.consumer_group)
        await stream.ensure_group()
        http_client = create_http_client()
        registry = load_vendor_registry(resolved.vendors_config_path)
        context = AppContext(resolved, engine, db, stream, http_client, registry)
        application.state.context = context
        tasks = [
            asyncio.create_task(_recovery_loop(context), name="stale-recovery"),
            asyncio.create_task(_idempotency_cleanup_loop(context), name="idempotency-cleanup"),
        ]
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            for task in tasks:
                with suppress(asyncio.CancelledError):
                    await task
            await http_client.aclose()
            await redis.aclose()
            await engine.dispose()

    application = FastAPI(
        title="API Notification Relay",
        version="0.1.0",
        lifespan=lifespan,
    )
    application.include_router(router)
    return application


async def recover_stale(ctx: AppContext, now: datetime | None = None) -> int:
    reference = now or datetime.now(UTC)
    cutoff = reference - timedelta(seconds=ctx.settings.running_timeout_seconds)
    stuck = await ctx.db.reset_running_stale(cutoff, reference)
    due = await ctx.db.find_due_pending(reference)
    notification_ids = list(dict.fromkeys(stuck + due))
    for notification_id in notification_ids:
        await ctx.stream.enqueue(notification_id)
    if notification_ids:
        await log.ainfo("stale_recovered", count=len(notification_ids))
    return len(notification_ids)


async def _recovery_loop(ctx: AppContext) -> None:
    while True:
        try:
            await recover_stale(ctx)
        except Exception as exc:
            await log.aerror("stale_recovery_failed", error=repr(exc))
        await asyncio.sleep(ctx.settings.recovery_interval_seconds)


async def cleanup_idempotency(ctx: AppContext, now: datetime | None = None) -> int:
    reference = now or datetime.now(UTC)
    cutoff = reference - timedelta(seconds=ctx.settings.idempotency_ttl_seconds)
    return await ctx.db.cleanup_idempotency(cutoff)


async def _idempotency_cleanup_loop(ctx: AppContext) -> None:
    while True:
        try:
            removed = await cleanup_idempotency(ctx)
            if removed:
                await log.ainfo("idempotency_keys_cleaned", count=removed)
        except Exception as exc:
            await log.aerror("idempotency_cleanup_failed", error=repr(exc))
        await asyncio.sleep(ctx.settings.idempotency_cleanup_interval_seconds)


def _ensure_sqlite_directory(database_url: str) -> None:
    prefix = "sqlite+aiosqlite:///"
    if database_url.startswith(prefix) and ":memory:" not in database_url:
        path = database_url.removeprefix(prefix)
        Path(path).parent.mkdir(parents=True, exist_ok=True)


app = create_app()
