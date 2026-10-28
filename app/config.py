from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration read from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = Field(default="", validation_alias="DATABASE_URL")
    worker_mode: Literal["embedded", "external"] = "embedded"
    processing_delay_seconds: float = Field(default=30.0, gt=0)
    poll_interval_seconds: float = Field(default=30.0, gt=0)
    claim_timeout_seconds: float = Field(default=90.0, gt=0)
    max_concurrent_jobs: int = Field(default=8, ge=1, le=32)

    @model_validator(mode="after")
    def claim_must_outlive_processing(self) -> "Settings":
        if self.claim_timeout_seconds <= self.processing_delay_seconds:
            raise ValueError("claim_timeout_seconds must exceed processing_delay_seconds")
        return self
