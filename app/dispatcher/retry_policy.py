"""Pure HTTP classification and retry delay helpers."""

import random
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime

import httpx

from app.domain.enums import ErrorClass


def classify_response(response: httpx.Response) -> ErrorClass:
    status = response.status_code
    if 200 <= status < 300:
        return ErrorClass.SUCCESS
    if status in {408, 429} or 500 <= status < 600:
        return ErrorClass.RETRYABLE
    return ErrorClass.NON_RETRYABLE


def next_delay(
    current_attempt: int,
    base_seconds: float = 1.0,
    max_seconds: float = 300.0,
    jitter_ratio: float = 0.2,
    random_value: float | None = None,
) -> float:
    """Delay after current attempt; first failure waits base_seconds."""
    exp = min(base_seconds * (2 ** max(0, current_attempt - 1)), max_seconds)
    value = random.random() if random_value is None else random_value
    jitter = exp * jitter_ratio * (value * 2 - 1)
    return max(0.1, exp + jitter)


def respect_retry_after(
    response: httpx.Response | None, now: datetime | None = None
) -> float | None:
    if response is None or response.status_code != 429:
        return None
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(raw)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            reference = now or datetime.now(UTC)
            return max(0.0, (retry_at - reference).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None
