import logging
from collections.abc import AsyncGenerator

from redis.asyncio import Redis

from app.core.config import settings

logger = logging.getLogger(__name__)

_redis: Redis | None = None


async def init_redis() -> Redis:
    """Initialize the global async Redis connection pool."""
    global _redis
    if _redis is not None:
        return _redis

    _redis = Redis.from_url(
        settings.REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        max_connections=settings.REDIS_MAX_CONNECTIONS,
    )
    await _redis.ping()
    logger.info(f"Connected to Redis at {settings.REDIS_URL}")
    return _redis


async def close_redis() -> None:
    """Close the global Redis connection pool."""
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
        logger.info("Redis connection closed")


def get_redis_client() -> Redis:
    """Return the initialized Redis client."""
    if _redis is None:
        raise RuntimeError("Redis is not initialized. Call init_redis() first.")
    return _redis


async def get_redis() -> AsyncGenerator[Redis, None]:
    """FastAPI dependency that yields the Redis client."""
    yield get_redis_client()
