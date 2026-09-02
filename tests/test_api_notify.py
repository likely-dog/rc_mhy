"""Contract tests for notification submission, lookup, and health."""

import pytest


@pytest.mark.asyncio
async def test_post_notify_when_valid_persists_and_enqueues(app_client, context, valid_body):
    """Given a valid request, API returns 202 and persists the pending notification."""
    response = await app_client.post(
        "/notify", json=valid_body, headers={"Idempotency-Key": "key-1"}
    )
    assert response.status_code == 202
    body = response.json()
    notification = await context.db.get(body["notification_id"])
    assert notification is not None
    assert notification.status == "pending"
    assert context.stream.enqueued == [body["notification_id"]]


@pytest.mark.asyncio
async def test_post_notify_when_header_missing_returns_422(app_client, valid_body):
    """Given no idempotency key, request validation rejects the call."""
    response = await app_client.post("/notify", json=valid_body)
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_post_notify_when_vendor_unknown_returns_400(app_client, valid_body):
    """Given an unknown vendor, API rejects before persistence."""
    valid_body["vendor"] = "missing"
    response = await app_client.post(
        "/notify", json=valid_body, headers={"Idempotency-Key": "key-2"}
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_post_notify_when_event_unsupported_returns_400(app_client, valid_body):
    """Given a configured event allow-list, an unsupported event is rejected."""
    valid_body["event_type"] = "deleted"
    response = await app_client.post(
        "/notify", json=valid_body, headers={"Idempotency-Key": "key-3"}
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_post_notify_when_payload_not_object_returns_422(app_client, valid_body):
    """Given a non-object payload, schema validation rejects it."""
    valid_body["payload"] = "bad"
    response = await app_client.post(
        "/notify", json=valid_body, headers={"Idempotency-Key": "key-4"}
    )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_notification_when_present_returns_debug_fields(app_client, valid_body):
    """Given an accepted notification, lookup returns its state and attempts."""
    created = await app_client.post(
        "/notify", json=valid_body, headers={"Idempotency-Key": "key-5"}
    )
    response = await app_client.get(f"/notifications/{created.json()['notification_id']}")
    assert response.status_code == 200
    assert response.json()["attempts"] == 0


@pytest.mark.asyncio
async def test_health_when_dependencies_up_returns_ok(app_client):
    """Given healthy DB and queue, health reports all components up."""
    response = await app_client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "redis": "up", "db": "up"}


@pytest.mark.asyncio
async def test_health_when_queue_down_returns_503(app_client, context):
    """Given queue failure, health returns degraded and 503."""
    context.stream.available = False
    response = await app_client.get("/health")
    assert response.status_code == 503
    assert response.json()["redis"] == "down"
