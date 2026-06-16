import pytest

from app.utils.retry import retry_with_exponential_backoff


@pytest.mark.asyncio
async def test_retry_succeeds_on_second_attempt() -> None:
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise ConnectionError("temporary failure")
        return "ok"

    result = await retry_with_exponential_backoff(
        flaky,
        max_retries=3,
        base_delay_seconds=0.01,
        operation_name="test",
    )
    assert result == "ok"
    assert attempts == 2


@pytest.mark.asyncio
async def test_retry_raises_after_max_attempts() -> None:
    async def always_fail() -> None:
        raise RuntimeError("permanent failure")

    with pytest.raises(RuntimeError, match="permanent failure"):
        await retry_with_exponential_backoff(
            always_fail,
            max_retries=3,
            base_delay_seconds=0.01,
            operation_name="test",
        )
