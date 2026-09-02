"""In-process end-to-end tests from HTTP acceptance through worker dispatch."""

import httpx
import pytest

from app.worker.tasks import dispatch_notification


@pytest.mark.asyncio
async def test_e2e_when_vendor_succeeds_reaches_success(app_client, context, valid_body):
    """Given an accepted API call and 2xx vendor, final observable state is success."""
    await context.http_client.aclose()
    context.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200))
    )
    accepted = await app_client.post(
        "/notify", json=valid_body, headers={"Idempotency-Key": "e2e-success"}
    )
    notification_id = accepted.json()["notification_id"]
    await dispatch_notification(context, notification_id)
    observed = await app_client.get(f"/notifications/{notification_id}")
    assert observed.json()["status"] == "success"
    assert observed.json()["attempts"] == 1


@pytest.mark.asyncio
async def test_e2e_when_vendor_recovers_after_failures_reaches_success(
    app_client, context, valid_body
):
    """Given two transient 500s then 200, notification succeeds on attempt three."""
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(500 if calls < 3 else 200)

    await context.http_client.aclose()
    context.http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    accepted = await app_client.post(
        "/notify", json=valid_body, headers={"Idempotency-Key": "e2e-recovery"}
    )
    notification_id = accepted.json()["notification_id"]
    for _ in range(3):
        await dispatch_notification(context, notification_id)
    observed = await app_client.get(f"/notifications/{notification_id}")
    assert observed.json()["status"] == "success"
    assert observed.json()["attempts"] == 3


@pytest.mark.asyncio
async def test_e2e_when_vendor_stays_down_reaches_dlq(app_client, context, valid_body):
    """Given persistent 500s, max attempts produce dead state and one DLQ record."""
    await context.http_client.aclose()
    context.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(500))
    )
    accepted = await app_client.post(
        "/notify", json=valid_body, headers={"Idempotency-Key": "e2e-dead"}
    )
    notification_id = accepted.json()["notification_id"]
    for _ in range(context.settings.max_attempts):
        await dispatch_notification(context, notification_id)
    observed = await app_client.get(f"/notifications/{notification_id}")
    assert observed.json()["status"] == "dead"
    assert await context.db.count_dead_letters() == 1
