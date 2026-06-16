import logging
import secrets

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.exceptions import DuplicateIdempotencyKeyError, PaymentNotFoundError
from app.models.payment import Payment, PaymentStatus
from app.schemas.payment import PaymentCreateRequest, PaymentCreateResponse, PaymentResponse
from app.services.outbox_service import OutboxService, PAYMENT_PROCESS_EVENT
from app.utils.retry import utc_now

logger = logging.getLogger(__name__)


def generate_payment_id() -> str:
    """Generate a human-readable unique payment identifier."""
    return f"pay_{secrets.token_hex(8)}"


class PaymentService:
    """Business logic for payment creation and retrieval."""

    def __init__(self, outbox_service: OutboxService | None = None) -> None:
        self._outbox_service = outbox_service or OutboxService()

    async def create_payment(
        self,
        session: AsyncSession,
        *,
        request: PaymentCreateRequest,
        idempotency_key: str,
    ) -> PaymentCreateResponse:
        """
        Create a payment and outbox event atomically.

        Idempotent: returns existing payment if idempotency_key matches.
        """
        existing = await self._get_by_idempotency_key(session, idempotency_key)
        if existing is not None:
            logger.info(f"Idempotent hit for key={idempotency_key} payment_id={existing.payment_id}")
            return PaymentCreateResponse(
                payment_id=existing.payment_id,
                status=existing.status,
                created_at=existing.created_at,
            )

        payment = Payment(
            payment_id=generate_payment_id(),
            amount=request.amount,
            currency=request.currency,
            description=request.description,
            metadata_=request.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            webhook_url=str(request.webhook_url),
        )

        session.add(payment)

        try:
            await session.flush()
        except IntegrityError as exc:
            await session.rollback()
            existing = await self._get_by_idempotency_key(session, idempotency_key)
            if existing is not None:
                return PaymentCreateResponse(
                    payment_id=existing.payment_id,
                    status=existing.status,
                    created_at=existing.created_at,
                )
            raise DuplicateIdempotencyKeyError(
                f"Payment with idempotency key '{idempotency_key}' already exists",
            ) from exc

        outbox_id = await self._outbox_service.create_event(
            session,
            event_type=PAYMENT_PROCESS_EVENT,
            payload={"payment_id": payment.payment_id},
        )

        logger.info(f"Created payment {payment.payment_id} with outbox {outbox_id}")

        return PaymentCreateResponse(
            payment_id=payment.payment_id,
            status=payment.status,
            created_at=payment.created_at,
        )

    async def get_payment(
        self,
        session: AsyncSession,
        payment_id: str,
    ) -> PaymentResponse:
        """Retrieve payment by human-readable payment_id."""
        payment = await self._get_by_payment_id(session, payment_id)
        if payment is None:
            raise PaymentNotFoundError(f"Payment '{payment_id}' not found")

        return PaymentResponse(
            id=payment.id,
            payment_id=payment.payment_id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )

    async def update_payment_status(
        self,
        session: AsyncSession,
        *,
        payment_id: str,
        status: PaymentStatus,
    ) -> Payment | None:
        """Update payment status and processed_at timestamp."""
        payment = await self._get_by_payment_id(session, payment_id)
        if payment is None:
            return None

        payment.status = status
        payment.processed_at = utc_now()
        await session.flush()
        logger.info(f"Updated payment {payment_id} status={status.value}")
        return payment

    @staticmethod
    async def _get_by_idempotency_key(
        session: AsyncSession,
        idempotency_key: str,
    ) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()

    @staticmethod
    async def _get_by_payment_id(
        session: AsyncSession,
        payment_id: str,
    ) -> Payment | None:
        stmt = select(Payment).where(Payment.payment_id == payment_id)
        result = await session.execute(stmt)
        return result.scalar_one_or_none()
