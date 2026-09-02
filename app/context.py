"""Explicitly injected application resources shared by API and worker."""

from dataclasses import dataclass

import httpx
from sqlalchemy.ext.asyncio import AsyncEngine

from app.db.engine import Repository
from app.dispatcher.vendors import VendorConfig
from app.queue.redis_stream import RedisStream
from app.settings import Settings


@dataclass(slots=True)
class AppContext:
    settings: Settings
    engine: AsyncEngine
    db: Repository
    stream: RedisStream
    http_client: httpx.AsyncClient
    vendor_registry: dict[str, VendorConfig]
