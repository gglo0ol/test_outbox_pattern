import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def retry_with_exponential_backoff(
    func: Callable[[], Awaitable[T]],
    *,
    max_retries: int = 3,
    base_delay_seconds: float = 1.0,
    operation_name: str = "operation",
) -> T:
    """
    Retry an async callable with exponential backoff.

    Delays: base_delay, base_delay*2, base_delay*4, ...
    Raises the last exception if all attempts fail.
    """
    last_exception: Exception | None = None

    for attempt in range(1, max_retries + 1):
        try:
            return await func()
        except Exception as exc:
            last_exception = exc
            if attempt == max_retries:
                logger.error(
                    f"{operation_name} failed after {max_retries} attempts: {exc}",
                )
                raise

            delay = base_delay_seconds * (2 ** (attempt - 1))
            logger.warning(
                f"{operation_name} attempt {attempt}/{max_retries} failed: {exc}. "
                f"Retrying in {delay:.1f}s",
            )
            await asyncio.sleep(delay)

    assert last_exception is not None
    raise last_exception


async def simulate_payment_processing(
    *,
    success_rate: float,
    min_sleep_seconds: float,
    max_sleep_seconds: float,
) -> bool:
    """Simulate external payment gateway processing with random delay."""
    delay = random.uniform(min_sleep_seconds, max_sleep_seconds)
    logger.info(f"Simulating payment processing for {delay:.2f}s")
    await asyncio.sleep(delay)
    return random.random() < success_rate


def utc_now() -> datetime:
    """Return current UTC datetime with timezone info."""
    return datetime.now(UTC)
