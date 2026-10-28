from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator


TransactionId = Annotated[str, StringConstraints(min_length=1, max_length=128)]
AccountId = Annotated[str, StringConstraints(min_length=1, max_length=128)]
Currency = Annotated[str, StringConstraints(pattern=r"^[A-Za-z]{3}$")]
Amount = Annotated[Decimal, Field(gt=Decimal("0"), max_digits=18, decimal_places=2)]


class TransactionStatus(StrEnum):
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"


class TransactionWebhook(BaseModel):
    model_config = ConfigDict(extra="forbid")

    transaction_id: TransactionId
    source_account: AccountId
    destination_account: AccountId
    amount: Amount
    currency: Currency

    @field_validator("transaction_id", "source_account", "destination_account")
    @classmethod
    def reject_ambiguous_identifiers(cls, value: str) -> str:
        if value != value.strip() or any(ord(char) < 32 or ord(char) == 127 for char in value):
            raise ValueError("Identifiers cannot contain surrounding whitespace or control characters")
        return value

    @field_validator("transaction_id")
    @classmethod
    def require_url_safe_transaction_id(cls, value: str) -> str:
        if "/" in value:
            raise ValueError("transaction_id cannot contain a slash")
        return value


class TransactionResponse(TransactionWebhook):
    status: TransactionStatus
    created_at: datetime
    processed_at: datetime | None


class HealthResponse(BaseModel):
    status: str = "HEALTHY"
    current_time: datetime


class WebhookAcknowledgement(BaseModel):
    status: str = "ACCEPTED"
