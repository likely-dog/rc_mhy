"""Unit tests for response classification and deterministic delay calculations."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest

from app.dispatcher.retry_policy import classify_response, next_delay, respect_retry_after
from app.domain.enums import ErrorClass


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (200, ErrorClass.SUCCESS),
        (204, ErrorClass.SUCCESS),
        (301, ErrorClass.NON_RETRYABLE),
        (400, ErrorClass.NON_RETRYABLE),
        (409, ErrorClass.NON_RETRYABLE),
        (408, ErrorClass.RETRYABLE),
        (429, ErrorClass.RETRYABLE),
        (500, ErrorClass.RETRYABLE),
        (599, ErrorClass.RETRYABLE),
    ],
)
def test_classify_response_when_status_varies_returns_contract_class(status, expected):
    """Given representative statuses, classification follows the PRD table."""
    assert classify_response(httpx.Response(status)) is expected


def test_next_delay_when_attempts_increase_uses_first_delay_and_cap():
    """Given neutral jitter, delays start at base, double, then cap."""
    values = [next_delay(i, 1, 4, 0, 0.5) for i in range(1, 6)]
    assert values == [1, 2, 4, 4, 4]


def test_next_delay_when_jitter_extreme_stays_within_ratio():
    """Given extreme random values, jitter remains inside plus/minus 20 percent."""
    assert next_delay(1, 10, 100, 0.2, 0) == pytest.approx(8)
    assert next_delay(1, 10, 100, 0.2, 1) == pytest.approx(12)


@pytest.mark.parametrize("header", [None, "invalid", "-3"])
def test_retry_after_when_missing_or_invalid_has_safe_result(header):
    """Given absent, invalid, or negative Retry-After, no unsafe delay is returned."""
    headers = {} if header is None else {"Retry-After": header}
    result = respect_retry_after(httpx.Response(429, headers=headers))
    assert result is None or result == 0


def test_retry_after_when_http_date_returns_seconds():
    """Given an HTTP-date Retry-After, delay is measured against injected UTC now."""
    now = datetime(2026, 9, 1, tzinfo=UTC)
    retry_at = now + timedelta(seconds=30)
    response = httpx.Response(
        429, headers={"Retry-After": retry_at.strftime("%a, %d %b %Y %H:%M:%S GMT")}
    )
    assert respect_retry_after(response, now) == pytest.approx(30)
