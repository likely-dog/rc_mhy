"""Integration tests for transactional idempotency behavior."""

import asyncio

import pytest


@pytest.mark.asyncio
async def test_notify_when_same_key_repeated_returns_same_id(app_client, valid_body):
    """Given the same key twice, second response is a 200 idempotency hit."""
    first = await app_client.post("/notify", json=valid_body, headers={"Idempotency-Key": "same"})
    second = await app_client.post("/notify", json=valid_body, headers={"Idempotency-Key": "same"})
    assert first.status_code == 202
    assert second.status_code == 200
    assert second.json()["notification_id"] == first.json()["notification_id"]
    assert second.json()["idempotent_hit"] is True


@pytest.mark.asyncio
async def test_notify_when_same_key_concurrent_creates_one_row(app_client, context, valid_body):
    """Given ten concurrent uses of one key, the PK permits one notification only."""
    responses = await asyncio.gather(
        *[
            app_client.post("/notify", json=valid_body, headers={"Idempotency-Key": "race"})
            for _ in range(10)
        ]
    )
    ids = {response.json()["notification_id"] for response in responses}
    assert ids and len(ids) == 1
    assert await context.db.count_notifications() == 1


@pytest.mark.asyncio
async def test_notify_when_keys_differ_creates_two_rows(app_client, context, valid_body):
    """Given identical payloads with different keys, both are accepted independently."""
    for key in ("one", "two"):
        response = await app_client.post(
            "/notify", json=valid_body, headers={"Idempotency-Key": key}
        )
        assert response.status_code == 202
    assert await context.db.count_notifications() == 2
