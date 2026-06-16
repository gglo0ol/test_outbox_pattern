from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.core.config import settings
from app.exceptions import PaymentNotFoundError
from app.main import app


@pytest.mark.asyncio
async def test_create_payment_unauthorized() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/payments",
            json={
                "amount": "100.00",
                "currency": "RUB",
                "description": "Test payment",
                "metadata": {"order_id": "123"},
                "webhook_url": "https://example.com/webhook",
            },
            headers={"Idempotency-Key": "key-1"},
        )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_payment_invalid_api_key() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/payments",
            json={
                "amount": "100.00",
                "currency": "RUB",
                "description": "Test payment",
                "metadata": {},
                "webhook_url": "https://example.com/webhook",
            },
            headers={
                "X-API-Key": "wrong-key",
                "Idempotency-Key": "key-2",
            },
        )
    assert response.status_code == 401


@pytest.mark.asyncio
async def test_create_payment_missing_idempotency_key() -> None:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/v1/payments",
            json={
                "amount": "100.00",
                "currency": "RUB",
                "description": "Test payment",
                "metadata": {},
                "webhook_url": "https://example.com/webhook",
            },
            headers={"X-API-Key": settings.API_KEY},
        )
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_get_payment_not_found() -> None:
    transport = ASGITransport(app=app)
    with patch(
        "app.api.v1.payments.payment_service.get_payment",
        new=AsyncMock(side_effect=PaymentNotFoundError("Payment 'pay_nonexistent' not found")),
    ):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            response = await client.get(
                "/api/v1/payments/pay_nonexistent",
                headers={"X-API-Key": settings.API_KEY},
            )
    assert response.status_code == 404
