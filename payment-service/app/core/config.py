from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    APP_NAME: str = "payment-service"
    DEBUG: bool = False
    API_KEY: str = Field(default="change-me-in-production")

    DATABASE_URL: str = Field(
        default="postgresql+asyncpg://payment:payment@localhost:5432/payment_db",
    )
    RABBITMQ_URL: str = Field(default="amqp://guest:guest@localhost:5672/")

    REDIS_URL: str = Field(default="redis://localhost:6379/0")
    REDIS_MAX_CONNECTIONS: int = 20
    REDIS_IDEMPOTENCY_TTL_SECONDS: int = 86_400  # 24 hours
    REDIS_PAYMENT_CACHE_TTL_SECONDS: int = 3_600  # 1 hour
    REDIS_OUTBOX_LOCK_TTL_SECONDS: int = 30

    OUTBOX_POLL_INTERVAL_SECONDS: float = 1.0

    PROCESSING_SUCCESS_RATE: float = 0.9
    PROCESSING_MIN_SLEEP_SECONDS: float = 2.0
    PROCESSING_MAX_SLEEP_SECONDS: float = 5.0

    WEBHOOK_MAX_RETRIES: int = 3
    WEBHOOK_BASE_DELAY_SECONDS: float = 1.0

    PAYMENT_EXCHANGE: str = "payment.exchange"
    PAYMENT_QUEUE: str = "payment.process"
    PAYMENT_DLQ: str = "payment.process.dlq"

    CONSUMER_MAX_RETRIES: int = 3


settings = Settings()
