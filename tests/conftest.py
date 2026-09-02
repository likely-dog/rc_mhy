"""Independent test resources for SQLite, API, HTTP, and queue behavior."""

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

from app.context import AppContext
from app.db.engine import Repository, create_engine_and_session
from app.dispatcher.vendors import VendorConfig
from app.main import create_app
from app.settings import Settings


class FakeStream:
    def __init__(self):
        self.enqueued: list[str] = []
        self.delays: list[float] = []
        self.available = True

    async def enqueue(self, notification_id: str) -> str:
        if not self.available:
            raise ConnectionError("queue down")
        self.enqueued.append(notification_id)
        return str(len(self.enqueued))

    async def enqueue_delayed(self, notification_id: str, delay: float) -> None:
        self.delays.append(delay)
        self.enqueued.append(notification_id)

    async def ping(self) -> None:
        if not self.available:
            raise ConnectionError("queue down")


@pytest.fixture
def vendor_registry() -> dict[str, VendorConfig]:
    return {
        "test_vendor": VendorConfig(
            code="test_vendor",
            endpoint="https://vendor.test/notify",
            method="POST",
            timeout_seconds=1,
            headers={"Content-Type": "application/json"},
            body_template='{"value": "{{ payload.value }}"}',
            event_types=["created"],
        )
    }


@pytest_asyncio.fixture
async def context(
    tmp_path: Path, vendor_registry: dict[str, VendorConfig]
) -> AsyncIterator[AppContext]:
    settings = Settings(
        database_url=f"sqlite+aiosqlite:///{tmp_path / 'test.db'}",
        redis_url="redis://unused:6379/0",
        vendors_config_path="config/vendors.yaml",
        max_attempts=3,
        base_delay_seconds=0.1,
        max_delay_seconds=1,
        jitter_ratio=0,
    )
    engine, factory = create_engine_and_session(settings.database_url)
    db = Repository(factory)
    await db.create_schema(engine)
    stream = FakeStream()
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200)))
    ctx = AppContext(settings, engine, db, stream, client, vendor_registry)
    yield ctx
    await client.aclose()
    await engine.dispose()


@pytest_asyncio.fixture
async def app_client(context: AppContext) -> AsyncIterator[httpx.AsyncClient]:
    app = create_app(context.settings)
    app.state.context = context
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
def valid_body() -> dict:
    return {"vendor": "test_vendor", "event_type": "created", "payload": {"value": "x"}}
