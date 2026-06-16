import logging
import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment import OutboxStatus
from app.schemas.payment import OutboxMessagePayload

logger = logging.getLogger(__name__)

PAYMENT_PROCESS_EVENT = "payment.process"


class OutboxService:
    """Service for managing outbox events."""

    @staticmethod
    async def create_event(
        session: AsyncSession,
        *,
        event_type: str,
        payload: dict,
    ) -> uuid.UUID:
        """Create a new outbox event within the current transaction."""
        from app.models.payment import OutboxEvent

        event = OutboxEvent(
            payload=payload,
            event_type=event_type,
            status=OutboxStatus.PENDING,
            retries=0,
        )
        session.add(event)
        await session.flush()
        logger.info(f"Created outbox event {event.id} type={event_type}")
        return event.id

    @staticmethod
    async def fetch_pending_events(
        session: AsyncSession,
        *,
        limit: int = 50,
    ) -> list:
        """Fetch pending outbox events ordered by creation time."""
        from app.models.payment import OutboxEvent

        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.status == OutboxStatus.PENDING)
            .order_by(OutboxEvent.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())

    @staticmethod
    async def mark_published(session: AsyncSession, outbox_id: uuid.UUID) -> None:
        """Mark outbox event as published to RabbitMQ."""
        from app.models.payment import OutboxEvent

        event = await session.get(OutboxEvent, outbox_id)
        if event is None:
            return
        event.status = OutboxStatus.PUBLISHED
        await session.flush()

    @staticmethod
    async def mark_processed(session: AsyncSession, outbox_id: uuid.UUID) -> None:
        """Mark outbox event as successfully processed."""
        from app.models.payment import OutboxEvent

        event = await session.get(OutboxEvent, outbox_id)
        if event is None:
            return
        event.status = OutboxStatus.PROCESSED
        await session.flush()

    @staticmethod
    async def mark_failed(session: AsyncSession, outbox_id: uuid.UUID) -> None:
        """Mark outbox event as failed after max retries."""
        from app.models.payment import OutboxEvent

        event = await session.get(OutboxEvent, outbox_id)
        if event is None:
            return
        event.status = OutboxStatus.FAILED
        event.retries += 1
        await session.flush()

    @staticmethod
    async def increment_retries(session: AsyncSession, outbox_id: uuid.UUID) -> int:
        """Increment retry counter and return new value."""
        from app.models.payment import OutboxEvent

        event = await session.get(OutboxEvent, outbox_id)
        if event is None:
            return 0
        event.retries += 1
        await session.flush()
        return event.retries

    @staticmethod
    def build_message_payload(outbox_id: uuid.UUID, payment_id: str) -> OutboxMessagePayload:
        """Build RabbitMQ message payload from outbox record."""
        return OutboxMessagePayload(
            outbox_id=outbox_id,
            payment_id=payment_id,
            event_type=PAYMENT_PROCESS_EVENT,
        )
