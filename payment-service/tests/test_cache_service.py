from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from app.models.payment import Currency, PaymentStatus
from app.schemas.payment import PaymentCreateResponse, PaymentResponse
from app.services.cache_service import CacheService


@pytest.fixture
def mock_redis() -> AsyncMock:
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock(return_value=True)
    redis.delete = AsyncMock(return_value=1)
    return redis


@pytest.fixture
def cache_service(mock_redis: AsyncMock) -> CacheService:
    return CacheService(redis=mock_redis)


@pytest.mark.asyncio
async def test_set_and_get_idempotency(
    cache_service: CacheService,
    mock_redis: AsyncMock,
) -> None:
    response = PaymentCreateResponse(
        payment_id="pay_abc123",
        status=PaymentStatus.PENDING,
        created_at=datetime.now(UTC),
    )

    stored: dict[str, str] = {}

    async def fake_set(key: str, value: str, ex: int) -> bool:
        stored[key] = value
        return True

    async def fake_get(key: str) -> str | None:
        return stored.get(key)

    mock_redis.set = AsyncMock(side_effect=fake_set)
    mock_redis.get = AsyncMock(side_effect=fake_get)

    await cache_service.set_idempotency("key-1", response)
    cached = await cache_service.get_idempotency("key-1")

    assert cached is not None
    assert cached.payment_id == "pay_abc123"
    assert cached.status == PaymentStatus.PENDING


@pytest.mark.asyncio
async def test_set_and_get_payment(
    cache_service: CacheService,
    mock_redis: AsyncMock,
) -> None:
    payment = PaymentResponse(
        id=uuid4(),
        payment_id="pay_xyz",
        amount=Decimal("100.00"),
        currency=Currency.RUB,
        description="Test",
        metadata={"order_id": "1"},
        status=PaymentStatus.SUCCEEDED,
        idempotency_key="key-2",
        webhook_url="https://example.com/hook",
        created_at=datetime.now(UTC),
        processed_at=datetime.now(UTC),
    )

    stored: dict[str, str] = {}

    async def fake_set(key: str, value: str, ex: int) -> bool:
        stored[key] = value
        return True

    async def fake_get(key: str) -> str | None:
        return stored.get(key)

    mock_redis.set = AsyncMock(side_effect=fake_set)
    mock_redis.get = AsyncMock(side_effect=fake_get)

    await cache_service.set_payment(payment)
    cached = await cache_service.get_payment("pay_xyz")

    assert cached is not None
    assert cached.payment_id == "pay_xyz"
    assert cached.status == PaymentStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_outbox_lock_acquire_and_release(
    cache_service: CacheService,
    mock_redis: AsyncMock,
) -> None:
    mock_redis.set = AsyncMock(return_value=True)
    mock_redis.delete = AsyncMock(return_value=1)

    acquired = await cache_service.try_acquire_outbox_lock()
    assert acquired is True

    await cache_service.release_outbox_lock()
    mock_redis.delete.assert_awaited_once_with(CacheService.OUTBOX_RELAY_LOCK_KEY)
