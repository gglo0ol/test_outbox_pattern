"""Re-export outbox model from payment module for clarity."""

from app.models.payment import OutboxEvent, OutboxStatus

__all__ = ["OutboxEvent", "OutboxStatus"]
