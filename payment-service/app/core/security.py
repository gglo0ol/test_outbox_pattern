from typing import Annotated

from fastapi import Header, HTTPException, status

from app.core.config import settings


async def verify_api_key(
    x_api_key: Annotated[str | None, Header(alias="X-API-Key")] = None,
) -> str:
    """Validate static API key from request header."""
    if not x_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key",
        )
    if x_api_key != settings.API_KEY:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )
    return x_api_key


async def require_idempotency_key(
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> str:
    """Ensure Idempotency-Key header is present and non-empty."""
    key = idempotency_key.strip()
    if not key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Idempotency-Key header is required",
        )
    return key
