"""FastStream consumer: outbox relay, payment processing, webhook delivery."""

import asyncio
import logging
import sys
from uuid import UUID

import httpx
from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitQueue
from app.core.config import settings
from app.core.database import async_session_factory
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import OutboxMessagePayload, WebhookPayload
from app.services.outbox_service import OutboxService
from app.services.payment_service import PaymentService
from app.utils.retry import retry_with_exponential_backoff, simulate_payment_processing

logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

outbox_service = OutboxService()
payment_service = PaymentService(outbox_service)

# Main queue with dead-letter routing to DLQ
payment_queue = RabbitQueue(
    settings.PAYMENT_QUEUE,
    durable=True,
    arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": settings.PAYMENT_DLQ,
    },
)

dlq_queue = RabbitQueue(settings.PAYMENT_DLQ, durable=True)

broker = RabbitBroker(settings.RABBITMQ_URL)
app = FastStream(broker)


async def send_webhook(payment: Payment) -> None:
    """Deliver payment result to merchant webhook with exponential backoff retries."""
    payload = WebhookPayload(
        payment_id=payment.payment_id,
        status=payment.status,
        amount=payment.amount,
        currency=payment.currency,
        processed_at=payment.processed_at,
    )

    async def _post_webhook() -> None:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                payment.webhook_url,
                json=payload.model_dump(mode="json"),
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()

    await retry_with_exponential_backoff(
        _post_webhook,
        max_retries=settings.WEBHOOK_MAX_RETRIES,
        base_delay_seconds=settings.WEBHOOK_BASE_DELAY_SECONDS,
        operation_name=f"webhook for {payment.payment_id}",
    )


async def process_payment_message(message: OutboxMessagePayload) -> None:
    """Core payment processing logic executed by the consumer."""
    async with async_session_factory() as session:
        payment = await PaymentService._get_by_payment_id(session, message.payment_id)
        if payment is None:
            logger.error(f"Payment not found: {message.payment_id}")
            await outbox_service.mark_failed(session, message.outbox_id)
            await session.commit()
            raise ValueError(f"Payment {message.payment_id} not found")

        if payment.status != PaymentStatus.PENDING:
            logger.info(
                f"Payment {message.payment_id} already processed "
                f"(status={payment.status.value}), skipping",
            )
            await outbox_service.mark_processed(session, message.outbox_id)
            await session.commit()
            return

        success = await simulate_payment_processing(
            success_rate=settings.PROCESSING_SUCCESS_RATE,
            min_sleep_seconds=settings.PROCESSING_MIN_SLEEP_SECONDS,
            max_sleep_seconds=settings.PROCESSING_MAX_SLEEP_SECONDS,
        )

        new_status = PaymentStatus.SUCCEEDED if success else PaymentStatus.FAILED
        await payment_service.update_payment_status(
            session,
            payment_id=message.payment_id,
            status=new_status,
        )

        await session.refresh(payment)
        await send_webhook(payment)

        await outbox_service.mark_processed(session, message.outbox_id)
        await session.commit()
        logger.info(f"Successfully processed payment {message.payment_id}")


async def handle_processing_failure(message: OutboxMessagePayload, error: Exception) -> None:
    """Track retries, republish with backoff, or route to DLQ after max attempts."""
    async with async_session_factory() as session:
        retries = await outbox_service.increment_retries(session, message.outbox_id)

        if retries >= settings.CONSUMER_MAX_RETRIES:
            await outbox_service.mark_failed(session, message.outbox_id)
            await session.commit()
            logger.error(
                f"Payment {message.payment_id} exceeded max retries ({retries}), "
                f"routing to DLQ. Last error: {error}",
            )
            await broker.publish(
                {
                    "outbox_id": str(message.outbox_id),
                    "payment_id": message.payment_id,
                    "error": str(error),
                    "retries": retries,
                },
                queue=settings.PAYMENT_DLQ,
            )
            return

        await session.commit()

    delay = settings.WEBHOOK_BASE_DELAY_SECONDS * (2 ** (retries - 1))
    logger.warning(
        f"Processing failed for {message.payment_id}, "
        f"retry {retries}/{settings.CONSUMER_MAX_RETRIES} in {delay:.1f}s: {error}",
    )
    await asyncio.sleep(delay)
    await broker.publish(
        message.model_dump(mode="json"),
        queue=settings.PAYMENT_QUEUE,
    )


@broker.subscriber(payment_queue)
async def on_payment_message(raw_message: dict) -> None:
    """Consume payment.process messages from RabbitMQ."""
    message = OutboxMessagePayload.model_validate(raw_message)
    logger.info(f"Received payment message: {message.payment_id}")

    try:
        await process_payment_message(message)
    except Exception as exc:
        await handle_processing_failure(message, exc)


@broker.subscriber(dlq_queue)
async def on_dlq_message(raw_message: dict) -> None:
    """Log and persist DLQ messages for operational visibility."""
    logger.error(f"DLQ message received: {raw_message}")

    outbox_id_str = raw_message.get("outbox_id")
    if outbox_id_str:
        async with async_session_factory() as session:
            outbox_id = UUID(outbox_id_str)
            await outbox_service.mark_failed(session, outbox_id)
            payment_id = raw_message.get("payment_id")
            if payment_id:
                payment = await PaymentService._get_by_payment_id(session, payment_id)
                if payment and payment.status == PaymentStatus.PENDING:
                    await payment_service.update_payment_status(
                        session,
                        payment_id=payment_id,
                        status=PaymentStatus.FAILED,
                    )
            await session.commit()


async def outbox_relay_loop() -> None:
    """
    Poll pending outbox records and publish them to RabbitMQ.

    Implements the publishing side of the Transactional Outbox pattern.
    """
    logger.info("Starting outbox relay loop")
    while True:
        try:
            async with async_session_factory() as session:
                events = await outbox_service.fetch_pending_events(session)

                for event in events:
                    payment_id = event.payload.get("payment_id")
                    if not payment_id:
                        logger.error(f"Outbox event {event.id} missing payment_id in payload")
                        await outbox_service.mark_failed(session, event.id)
                        continue

                    message = outbox_service.build_message_payload(event.id, payment_id)
                    await broker.publish(
                        message.model_dump(mode="json"),
                        queue=settings.PAYMENT_QUEUE,
                    )
                    await outbox_service.mark_published(session, event.id)
                    logger.info(f"Published outbox event {event.id} for payment {payment_id}")

                await session.commit()
        except Exception as exc:
            logger.exception(f"Outbox relay error: {exc}")

        await asyncio.sleep(settings.OUTBOX_POLL_INTERVAL_SECONDS)


@app.on_startup
async def start_outbox_relay() -> None:
    """Launch background outbox relay when consumer starts."""
    asyncio.create_task(outbox_relay_loop())


if __name__ == "__main__":
    asyncio.run(app.run())
