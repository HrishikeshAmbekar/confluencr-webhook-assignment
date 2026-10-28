from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

import pytest

from app.models import TransactionResponse, TransactionStatus, TransactionWebhook
from app.storage import StorageError, TransactionClaim


class MemoryTransactionStore:
    """Small test double that models the durable-store behavior needed by the API tests."""

    def __init__(self) -> None:
        self._transactions: dict[str, dict[str, object]] = {}
        self._lock = asyncio.Lock()
        self.completed_count = 0
        self.claim_count = 0

    async def ensure_schema(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def create_transaction(self, transaction: TransactionWebhook) -> bool:
        async with self._lock:
            if transaction.transaction_id in self._transactions:
                return False
            self._transactions[transaction.transaction_id] = {
                "transaction": transaction,
                "status": TransactionStatus.PROCESSING,
                "created_at": datetime.now(UTC),
                "processed_at": None,
                "claimed_at": None,
                "claim_token": None,
                "attempts": 0,
            }
            return True

    async def get_transaction(self, transaction_id: str) -> TransactionResponse | None:
        async with self._lock:
            row = self._transactions.get(transaction_id)
            if row is None:
                return None
            transaction = cast(TransactionWebhook, row["transaction"])
            return TransactionResponse(
                **transaction.model_dump(),
                status=cast(TransactionStatus, row["status"]),
                created_at=cast(datetime, row["created_at"]),
                processed_at=cast(datetime | None, row["processed_at"]),
            )

    async def claim_next_transaction(self, claim_timeout_seconds: float) -> TransactionClaim | None:
        async with self._lock:
            now = datetime.now(UTC)
            for transaction_id, row in self._transactions.items():
                claimed_at = cast(datetime | None, row["claimed_at"])
                if row["status"] != TransactionStatus.PROCESSING:
                    continue
                if claimed_at and claimed_at >= now - timedelta(seconds=claim_timeout_seconds):
                    continue
                row["attempts"] = cast(int, row["attempts"]) + 1
                claim = TransactionClaim(
                    transaction_id=transaction_id,
                    claim_token=uuid4(),
                    attempt=cast(int, row["attempts"]),
                )
                row["claimed_at"] = now
                row["claim_token"] = claim.claim_token
                self.claim_count += 1
                return claim
        return None

    async def mark_processed(self, claim: TransactionClaim) -> bool:
        async with self._lock:
            row = self._transactions[claim.transaction_id]
            if row["claim_token"] != claim.claim_token:
                return False
            row["status"] = TransactionStatus.PROCESSED
            row["processed_at"] = datetime.now(UTC)
            row["claimed_at"] = None
            row["claim_token"] = None
            self.completed_count += 1
            return True


class FailingInsertStore(MemoryTransactionStore):
    async def create_transaction(self, transaction: TransactionWebhook) -> bool:
        raise StorageError("database unavailable")


class CrashingClaimStore(MemoryTransactionStore):
    async def claim_next_transaction(self, claim_timeout_seconds: float) -> TransactionClaim | None:
        raise RuntimeError("unexpected worker failure")


class FailingMarkOnceStore(MemoryTransactionStore):
    def __init__(self) -> None:
        super().__init__()
        self._failed_once = False

    async def mark_processed(self, claim: TransactionClaim) -> bool:
        if not self._failed_once:
            self._failed_once = True
            raise StorageError("temporary database outage")
        return await super().mark_processed(claim)


@pytest.fixture
def payload() -> dict[str, object]:
    return {
        "transaction_id": "txn_abc123def456",
        "source_account": "acc_user_789",
        "destination_account": "acc_merchant_456",
        "amount": 1500,
        "currency": "INR",
    }
