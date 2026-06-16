import logging

from redis.asyncio import Redis
from redis.exceptions import RedisError

from app.core.config import settings
from app.core.redis import get_redis_client
from app.schemas.payment import PaymentCreateResponse, PaymentResponse

logger = logging.getLogger(__name__)


class CacheService:
    """Redis-backed cache for idempotency keys and payment reads."""

    IDEMPOTENCY_PREFIX = "idempotency:"
    PAYMENT_PREFIX = "payment:"
    OUTBOX_RELAY_LOCK_KEY = "lock:outbox:relay"

    def __init__(self, redis: Redis | None = None) -> None:
        self._redis = redis

    @property
    def redis(self) -> Redis:
        return self._redis or get_redis_client()

    @staticmethod
    def _idempotency_key(idempotency_key: str) -> str:
        return f"{CacheService.IDEMPOTENCY_PREFIX}{idempotency_key}"

    @staticmethod
    def _payment_key(payment_id: str) -> str:
        return f"{CacheService.PAYMENT_PREFIX}{payment_id}"

    async def get_idempotency(self, idempotency_key: str) -> PaymentCreateResponse | None:
        """Return cached create-payment response for an idempotency key."""
        try:
            raw = await self.redis.get(self._idempotency_key(idempotency_key))
            if raw is None:
                return None
            return PaymentCreateResponse.model_validate_json(raw)
        except RedisError as exc:
            logger.warning(f"Redis idempotency read failed: {exc}")
            return None

    async def set_idempotency(
        self,
        idempotency_key: str,
        response: PaymentCreateResponse,
    ) -> None:
        """Cache create-payment response for idempotency protection."""
        try:
            await self.redis.set(
                self._idempotency_key(idempotency_key),
                response.model_dump_json(),
                ex=settings.REDIS_IDEMPOTENCY_TTL_SECONDS,
            )
        except RedisError as exc:
            logger.warning(f"Redis idempotency write failed: {exc}")

    async def get_payment(self, payment_id: str) -> PaymentResponse | None:
        """Return cached payment details."""
        try:
            raw = await self.redis.get(self._payment_key(payment_id))
            if raw is None:
                return None
            return PaymentResponse.model_validate_json(raw)
        except RedisError as exc:
            logger.warning(f"Redis payment read failed: {exc}")
            return None

    async def set_payment(self, payment: PaymentResponse) -> None:
        """Cache payment details for fast GET lookups."""
        try:
            await self.redis.set(
                self._payment_key(payment.payment_id),
                payment.model_dump_json(),
                ex=settings.REDIS_PAYMENT_CACHE_TTL_SECONDS,
            )
        except RedisError as exc:
            logger.warning(f"Redis payment write failed: {exc}")

    async def invalidate_payment(self, payment_id: str) -> None:
        """Remove payment from cache (e.g. before status update)."""
        try:
            await self.redis.delete(self._payment_key(payment_id))
        except RedisError as exc:
            logger.warning(f"Redis payment invalidate failed: {exc}")

    async def try_acquire_outbox_lock(self) -> bool:
        """
        Acquire a distributed lock for the outbox relay loop.

        Only one consumer instance should poll/publish outbox events at a time.
        """
        try:
            return bool(
                await self.redis.set(
                    self.OUTBOX_RELAY_LOCK_KEY,
                    "1",
                    nx=True,
                    ex=settings.REDIS_OUTBOX_LOCK_TTL_SECONDS,
                ),
            )
        except RedisError as exc:
            logger.warning(f"Redis outbox lock acquire failed: {exc}")
            return True  # fail-open: process outbox if Redis is unavailable

    async def release_outbox_lock(self) -> None:
        """Release the outbox relay distributed lock."""
        try:
            await self.redis.delete(self.OUTBOX_RELAY_LOCK_KEY)
        except RedisError as exc:
            logger.warning(f"Redis outbox lock release failed: {exc}")
