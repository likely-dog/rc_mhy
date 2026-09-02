"""Async database lifecycle and short transactional repository operations."""

import json
from datetime import datetime
from typing import Any

from sqlalchemy import delete, event, select, text, update
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool
from ulid import ULID

from app.db.models import Base, DeadLetter, IdempotencyKey, Notification


def create_engine_and_session(
    database_url: str,
) -> tuple[AsyncEngine, async_sessionmaker]:
    engine = create_async_engine(database_url, poolclass=NullPool)

    if database_url.startswith("sqlite"):

        @event.listens_for(engine.sync_engine, "connect")
        def set_sqlite_pragmas(dbapi_connection: Any, _connection_record: Any) -> None:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA busy_timeout=5000")
            cursor.execute("PRAGMA foreign_keys=ON")
            cursor.close()

    return engine, async_sessionmaker(engine, expire_on_commit=False)


class Repository:
    """Persistence boundary; every public mutation commits one short transaction."""

    def __init__(self, session_factory: async_sessionmaker):
        self.session_factory = session_factory

    async def create_schema(self, engine: AsyncEngine) -> None:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

    async def ping(self) -> None:
        async with self.session_factory() as session:
            await session.execute(text("SELECT 1"))

    async def get(self, notification_id: str) -> Notification | None:
        async with self.session_factory() as session:
            return await session.get(Notification, notification_id)

    async def get_by_idempotency_key(self, key: str) -> Notification | None:
        async with self.session_factory() as session:
            statement = (
                select(Notification)
                .join(IdempotencyKey, IdempotencyKey.notification_id == Notification.id)
                .where(IdempotencyKey.idempotency_key == key)
            )
            return await session.scalar(statement)

    async def insert_notification_and_idempotency(
        self,
        notification_id: str,
        idempotency_key: str,
        vendor: str,
        event_type: str,
        payload: dict[str, Any],
        now: datetime,
    ) -> Notification:
        notification = Notification(
            id=notification_id,
            idempotency_key=idempotency_key,
            vendor=vendor,
            event_type=event_type,
            payload_json=json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
            status="pending",
            attempts=0,
            created_at=now,
            updated_at=now,
        )
        async with self.session_factory() as session:
            async with session.begin():
                session.add(notification)
                # No ORM relationship is needed for this MVP, so flush the parent
                # explicitly to guarantee FK ordering while retaining one transaction.
                await session.flush()
                session.add(
                    IdempotencyKey(
                        idempotency_key=idempotency_key,
                        notification_id=notification_id,
                        created_at=now,
                    )
                )
        return notification

    async def mark_running(self, notification_id: str, attempts: int, now: datetime) -> None:
        async with self.session_factory() as session, session.begin():
            await session.execute(
                update(Notification)
                .where(Notification.id == notification_id)
                .values(status="running", attempts=attempts, updated_at=now)
            )

    async def mark_success(self, notification_id: str, http_status: int, now: datetime) -> None:
        async with self.session_factory() as session, session.begin():
            await session.execute(
                update(Notification)
                .where(Notification.id == notification_id)
                .values(
                    status="success",
                    next_retry_at=None,
                    last_error=None,
                    last_http_status=http_status,
                    updated_at=now,
                )
            )

    async def mark_pending(
        self,
        notification_id: str,
        next_retry_at: datetime,
        error: str,
        http_status: int | None,
        now: datetime,
    ) -> None:
        async with self.session_factory() as session, session.begin():
            await session.execute(
                update(Notification)
                .where(Notification.id == notification_id)
                .values(
                    status="pending",
                    next_retry_at=next_retry_at,
                    last_error=error[:1024],
                    last_http_status=http_status,
                    updated_at=now,
                )
            )

    async def mark_dead_and_write_letter(
        self,
        notification_id: str,
        reason: str,
        error: str,
        http_status: int | None,
        now: datetime,
    ) -> None:
        async with self.session_factory() as session, session.begin():
            notification = await session.get(Notification, notification_id)
            if notification is None:
                return
            notification.status = "dead"
            notification.next_retry_at = None
            notification.last_error = error[:1024]
            notification.last_http_status = http_status
            notification.updated_at = now
            snapshot = {
                "id": notification.id,
                "idempotency_key": notification.idempotency_key,
                "vendor": notification.vendor,
                "event_type": notification.event_type,
                "payload": json.loads(notification.payload_json),
                "status": "dead",
                "attempts": notification.attempts,
                "last_error": notification.last_error,
                "last_http_status": notification.last_http_status,
            }
            session.add(
                DeadLetter(
                    id=str(ULID()),
                    notification_id=notification_id,
                    reason=reason,
                    dumped_at=now,
                    snapshot_json=json.dumps(snapshot, ensure_ascii=False),
                )
            )

    async def reset_running_stale(self, cutoff: datetime, now: datetime) -> list[str]:
        async with self.session_factory() as session, session.begin():
            result = await session.execute(
                update(Notification)
                .where(Notification.status == "running", Notification.updated_at < cutoff)
                .values(status="pending", next_retry_at=now, updated_at=now)
                .returning(Notification.id)
            )
            return list(result.scalars())

    async def find_due_pending(self, now: datetime) -> list[str]:
        async with self.session_factory() as session:
            result = await session.scalars(
                select(Notification.id).where(
                    Notification.status == "pending",
                    (Notification.next_retry_at.is_(None)) | (Notification.next_retry_at <= now),
                )
            )
            return list(result)

    async def cleanup_idempotency(self, cutoff: datetime) -> int:
        async with self.session_factory() as session, session.begin():
            result = await session.execute(
                delete(IdempotencyKey).where(IdempotencyKey.created_at < cutoff)
            )
            return result.rowcount or 0

    async def count_notifications(self) -> int:
        async with self.session_factory() as session:
            return len(list(await session.scalars(select(Notification.id))))

    async def count_dead_letters(self) -> int:
        async with self.session_factory() as session:
            return len(list(await session.scalars(select(DeadLetter.id))))
