"""One-attempt notification dispatch state machine."""

import json
from datetime import UTC, datetime, timedelta
from time import perf_counter

import httpx
import structlog

from app.context import AppContext
from app.dispatcher.renderer import RenderError, render_request
from app.dispatcher.retry_policy import classify_response, next_delay, respect_retry_after
from app.domain.enums import ErrorClass

log = structlog.get_logger()


async def dispatch_notification(ctx: AppContext, notification_id: str) -> str:
    notification = await ctx.db.get(notification_id)
    if notification is None or notification.status in {"success", "dead"}:
        return "skipped"

    vendor = ctx.vendor_registry.get(notification.vendor)
    now = datetime.now(UTC)
    current_attempt = notification.attempts + 1
    await ctx.db.mark_running(notification_id, current_attempt, now)
    started = perf_counter()

    if vendor is None:
        await ctx.db.mark_dead_and_write_letter(
            notification_id,
            "vendor_config_missing",
            f"vendor config missing: {notification.vendor}",
            None,
            now,
        )
        await _log_result(notification, current_attempt, started, "dispatch_dead", None)
        return "dead"

    response: httpx.Response | None = None
    error = ""
    try:
        rendered = render_request(vendor, json.loads(notification.payload_json))
        timeout = httpx.Timeout(
            connect=min(2.0, vendor.timeout_seconds),
            read=vendor.timeout_seconds,
            write=2.0,
            pool=2.0,
        )
        response = await ctx.http_client.request(**rendered, timeout=timeout)
        error_class = classify_response(response)
        if error_class is not ErrorClass.SUCCESS:
            error = f"vendor returned HTTP {response.status_code}"
    except RenderError as exc:
        error_class = ErrorClass.NON_RETRYABLE
        error = str(exc)
    except (httpx.TimeoutException, httpx.ConnectError) as exc:
        error_class = ErrorClass.RETRYABLE
        error = repr(exc)
    except Exception as exc:
        error_class = ErrorClass.RETRYABLE
        error = repr(exc)

    http_status = response.status_code if response is not None else None
    finished = datetime.now(UTC)
    if error_class is ErrorClass.SUCCESS:
        await ctx.db.mark_success(notification_id, http_status or 200, finished)
        await _log_result(notification, current_attempt, started, "dispatch_success", http_status)
        return "success"

    if error_class is ErrorClass.NON_RETRYABLE:
        reason = "non_retryable_4xx" if response is not None else "vendor_config_missing"
        await ctx.db.mark_dead_and_write_letter(
            notification_id, reason, error, http_status, finished
        )
        await _log_result(notification, current_attempt, started, "dispatch_dead", http_status)
        return "dead"

    if current_attempt >= ctx.settings.max_attempts:
        await ctx.db.mark_dead_and_write_letter(
            notification_id, "max_retries_exceeded", error, http_status, finished
        )
        await _log_result(notification, current_attempt, started, "dispatch_dead", http_status)
        return "dead"

    delay = respect_retry_after(response, finished)
    if delay is None:
        delay = next_delay(
            current_attempt,
            ctx.settings.base_delay_seconds,
            ctx.settings.max_delay_seconds,
            ctx.settings.jitter_ratio,
        )
    retry_at = finished + timedelta(seconds=delay)
    await ctx.db.mark_pending(notification_id, retry_at, error, http_status, finished)
    await _log_result(notification, current_attempt, started, "dispatch_retry", http_status)
    await ctx.stream.enqueue_delayed(notification_id, delay)
    return "retry"


async def _log_result(notification, attempts, started, event, http_status) -> None:
    await log.ainfo(
        event,
        notification_id=notification.id,
        vendor=notification.vendor,
        attempts=attempts,
        latency_ms=round((perf_counter() - started) * 1000, 2),
        http_status=http_status,
    )
