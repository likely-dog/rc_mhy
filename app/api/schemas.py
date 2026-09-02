"""Validated public API request and response schemas."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class NotifyRequest(BaseModel):
    vendor: str = Field(min_length=1, max_length=64)
    event_type: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any]


class NotifyResponse(BaseModel):
    notification_id: str
    status: str
    idempotent_hit: bool


class NotificationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    idempotency_key: str
    vendor: str
    event_type: str
    status: str
    attempts: int
    next_retry_at: datetime | None
    last_error: str | None
    last_http_status: int | None
    created_at: datetime
    updated_at: datetime


class HealthResponse(BaseModel):
    status: str
    redis: str
    db: str
