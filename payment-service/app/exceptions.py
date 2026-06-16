class PaymentServiceError(Exception):
    """Base exception for payment service errors."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class PaymentNotFoundError(PaymentServiceError):
    """Raised when a payment is not found."""


class DuplicateIdempotencyKeyError(PaymentServiceError):
    """Raised when idempotency key already exists."""
