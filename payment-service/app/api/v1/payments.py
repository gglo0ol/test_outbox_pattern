import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db_session
from app.core.security import require_idempotency_key, verify_api_key
from app.exceptions import DuplicateIdempotencyKeyError, PaymentNotFoundError
from app.schemas.payment import PaymentCreateRequest, PaymentCreateResponse, PaymentResponse
from app.services.payment_service import PaymentService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/payments", tags=["payments"])

payment_service = PaymentService()


@router.post(
    "",
    response_model=PaymentCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a new payment",
)
async def create_payment(
    body: PaymentCreateRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    _api_key: Annotated[str, Depends(verify_api_key)],
    idempotency_key: Annotated[str, Depends(require_idempotency_key)],
) -> PaymentCreateResponse:
    """Accept a payment request and enqueue it for asynchronous processing."""
    try:
        return await payment_service.create_payment(
            session,
            request=body,
            idempotency_key=idempotency_key,
        )
    except DuplicateIdempotencyKeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=exc.message,
        ) from exc


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    summary="Get payment by ID",
)
async def get_payment(
    payment_id: str,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    _api_key: Annotated[str, Depends(verify_api_key)],
) -> PaymentResponse:
    """Return full payment details by human-readable payment_id."""
    try:
        return await payment_service.get_payment(session, payment_id)
    except PaymentNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=exc.message,
        ) from exc
