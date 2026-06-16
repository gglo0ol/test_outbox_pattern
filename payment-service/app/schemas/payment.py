from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.models.payment import Currency, PaymentStatus


class PaymentCreateRequest(BaseModel):
    """Request body for creating a new payment."""

    model_config = ConfigDict(strict=True, extra="forbid")

    amount: Decimal = Field(..., gt=0, decimal_places=2)
    currency: Currency
    description: str = Field(..., min_length=1, max_length=1024)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentCreateResponse(BaseModel):
    """202 Accepted response after payment creation."""

    model_config = ConfigDict(strict=True)

    payment_id: str
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    """Full payment details."""

    model_config = ConfigDict(strict=True, from_attributes=True)

    id: UUID
    payment_id: str
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None


class WebhookPayload(BaseModel):
    """Payload sent to the merchant webhook URL."""

    model_config = ConfigDict(strict=True)

    payment_id: str
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime | None = None


class OutboxMessagePayload(BaseModel):
    """Message published to RabbitMQ from outbox relay."""

    model_config = ConfigDict(strict=True)

    outbox_id: UUID
    payment_id: str
    event_type: str
