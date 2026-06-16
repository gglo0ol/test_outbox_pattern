"""Initial schema: payments and outbox_events tables.

Revision ID: 001_initial
Revises:
Create Date: 2026-06-16

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payment_id", sa.String(length=64), nullable=False),
        sa.Column("amount", sa.Numeric(precision=18, scale=2), nullable=False),
        sa.Column("currency", sa.Enum("RUB", "USD", "EUR", name="currency_enum", native_enum=False), nullable=False),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("metadata", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "status",
            sa.Enum("pending", "succeeded", "failed", name="payment_status_enum", native_enum=False),
            nullable=False,
        ),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key"),
        sa.UniqueConstraint("payment_id"),
    )
    op.create_index(op.f("ix_payments_idempotency_key"), "payments", ["idempotency_key"], unique=False)
    op.create_index(op.f("ix_payments_payment_id"), "payments", ["payment_id"], unique=False)
    op.create_index(op.f("ix_payments_status"), "payments", ["status"], unique=False)

    op.create_table(
        "outbox_events",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column(
            "status",
            sa.Enum("pending", "published", "processed", "failed", name="outbox_status_enum", native_enum=False),
            nullable=False,
        ),
        sa.Column("retries", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_outbox_events_event_type"), "outbox_events", ["event_type"], unique=False)
    op.create_index(op.f("ix_outbox_events_status"), "outbox_events", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_outbox_events_status"), table_name="outbox_events")
    op.drop_index(op.f("ix_outbox_events_event_type"), table_name="outbox_events")
    op.drop_table("outbox_events")
    op.drop_index(op.f("ix_payments_status"), table_name="payments")
    op.drop_index(op.f("ix_payments_payment_id"), table_name="payments")
    op.drop_index(op.f("ix_payments_idempotency_key"), table_name="payments")
    op.drop_table("payments")
