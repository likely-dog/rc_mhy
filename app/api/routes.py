"""FastAPI routes for accepting and inspecting notifications."""

from datetime import UTC, datetime

import structlog
from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from sqlalchemy.exc import IntegrityError
from ulid import ULID

from app.api.schemas import (
    HealthResponse,
    NotificationResponse,
    NotifyRequest,
    NotifyResponse,
)
from app.context import AppContext

router = APIRouter()
log = structlog.get_logger()


def _context(request: Request) -> AppContext:
    return request.app.state.context


@router.post(
    "/notify",
    response_model=NotifyResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={200: {"model": NotifyResponse}, 400: {"description": "Unknown vendor/event"}},
)
async def post_notify(
    body: NotifyRequest,
    request: Request,
    response: Response,
    idempotency_key: str = Header(min_length=1, max_length=128),
) -> NotifyResponse:
    ctx = _context(request)
    vendor = ctx.vendor_registry.get(body.vendor)
    if vendor is None:
        raise HTTPException(status_code=400, detail=f"unknown vendor: {body.vendor}")
    if vendor.event_types is not None and body.event_type not in vendor.event_types:
        raise HTTPException(
            status_code=400,
            detail=f"unsupported event_type for vendor {body.vendor}: {body.event_type}",
        )

    now = datetime.now(UTC)
    notification_id = str(ULID())
    try:
        notification = await ctx.db.insert_notification_and_idempotency(
            notification_id,
            idempotency_key,
            body.vendor,
            body.event_type,
            body.payload,
            now,
        )
    except IntegrityError:
        winner = await ctx.db.get_by_idempotency_key(idempotency_key)
        if winner is None:
            raise HTTPException(status_code=503, detail="idempotency resolution failed") from None
        response.status_code = status.HTTP_200_OK
        return NotifyResponse(
            notification_id=winner.id,
            status=winner.status,
            idempotent_hit=True,
        )

    try:
        await ctx.stream.enqueue(notification_id)
    except Exception as exc:
        await log.aerror(
            "enqueue_failed",
            notification_id=notification_id,
            vendor=body.vendor,
            error=repr(exc),
        )
        raise HTTPException(
            status_code=503,
            detail="notification persisted but queue is unavailable; recovery will retry",
        ) from exc

    await log.ainfo("enqueued", notification_id=notification_id, vendor=body.vendor, attempts=0)
    return NotifyResponse(
        notification_id=notification.id,
        status=notification.status,
        idempotent_hit=False,
    )


@router.get("/notifications/{notification_id}", response_model=NotificationResponse)
async def get_notification(notification_id: str, request: Request) -> NotificationResponse:
    notification = await _context(request).db.get(notification_id)
    if notification is None:
        raise HTTPException(status_code=404, detail="notification not found")
    return NotificationResponse.model_validate(notification)


@router.get("/health", response_model=HealthResponse)
async def health(request: Request, response: Response) -> HealthResponse:
    ctx = _context(request)
    redis_status = "up"
    db_status = "up"
    try:
        await ctx.stream.ping()
    except Exception:
        redis_status = "down"
    try:
        await ctx.db.ping()
    except Exception:
        db_status = "down"
    overall = "ok" if redis_status == db_status == "up" else "degraded"
    if overall != "ok":
        response.status_code = 503
    return HealthResponse(status=overall, redis=redis_status, db=db_status)
