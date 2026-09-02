"""Environment-backed application settings."""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "sqlite+aiosqlite:///./data/app.db"
    redis_url: str = "redis://localhost:6379/0"
    vendors_config_path: str = "config/vendors.yaml"
    log_level: str = "INFO"
    stream_name: str = "notifications"
    consumer_group: str = "notification-workers"
    consumer_name: str = "worker-1"
    max_attempts: int = Field(default=10, ge=1)
    base_delay_seconds: float = Field(default=1.0, gt=0)
    max_delay_seconds: float = Field(default=300.0, gt=0)
    jitter_ratio: float = Field(default=0.2, ge=0, le=1)
    running_timeout_seconds: int = Field(default=60, ge=10)
    recovery_interval_seconds: int = Field(default=30, ge=1)
    idempotency_ttl_seconds: int = Field(default=86400, ge=1)
    idempotency_cleanup_interval_seconds: int = Field(default=3600, ge=1)


@lru_cache
def get_settings() -> Settings:
    return Settings()
