from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

import asyncpg

from app.models import TransactionResponse, TransactionStatus, TransactionWebhook


class StorageError(RuntimeError):
    """Raised when a request cannot be made durable in the transaction store."""


@dataclass(frozen=True)
class TransactionClaim:
    transaction_id: str
    claim_token: UUID


class TransactionStore(Protocol):
    async def ensure_schema(self) -> None: ...

    async def close(self) -> None: ...

    async def create_transaction(self, transaction: TransactionWebhook) -> bool: ...

    async def get_transaction(self, transaction_id: str) -> TransactionResponse | None: ...

    async def claim_next_transaction(self, claim_timeout_seconds: float) -> TransactionClaim | None: ...

    async def mark_processed(self, claim: TransactionClaim) -> bool: ...


SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


class PostgresTransactionStore:
    """Postgres implementation used with Supabase's session pooler."""

    def __init__(self, pool: asyncpg.Pool[asyncpg.Record]) -> None:
        self._pool = pool

    @classmethod
    async def connect(cls, database_url: str) -> "PostgresTransactionStore":
        if not database_url:
            raise StorageError("DATABASE_URL is required")

        try:
            pool = await asyncpg.create_pool(
                dsn=database_url,
                min_size=1,
                max_size=5,
                command_timeout=10,
            )
        except Exception as exc:  # asyncpg exposes several connection exception classes.
            raise StorageError("Could not connect to PostgreSQL") from exc
        return cls(pool)

    async def ensure_schema(self) -> None:
        try:
            schema = SCHEMA_PATH.read_text(encoding="utf-8")
            async with self._pool.acquire() as connection:
                await connection.execute(schema)
        except Exception as exc:
            raise StorageError("Could not initialize the transaction schema") from exc

    async def close(self) -> None:
        await self._pool.close()

    async def create_transaction(self, transaction: TransactionWebhook) -> bool:
        query = """
            INSERT INTO webhook_private.transactions (
                transaction_id, source_account, destination_account, amount, currency, status
            ) VALUES ($1, $2, $3, $4, $5, 'PROCESSING')
            ON CONFLICT (transaction_id) DO NOTHING
            RETURNING transaction_id
        """
        try:
            async with self._pool.acquire() as connection:
                inserted = await connection.fetchval(
                    query,
                    transaction.transaction_id,
                    transaction.source_account,
                    transaction.destination_account,
                    transaction.amount,
                    transaction.currency,
                )
                return inserted is not None
        except Exception as exc:
            raise StorageError("Could not persist the incoming webhook") from exc

    async def get_transaction(self, transaction_id: str) -> TransactionResponse | None:
        query = """
            SELECT transaction_id, source_account, destination_account, amount, currency,
                   status, created_at, processed_at
            FROM webhook_private.transactions
            WHERE transaction_id = $1
        """
        try:
            async with self._pool.acquire() as connection:
                row = await connection.fetchrow(query, transaction_id)
        except Exception as exc:
            raise StorageError("Could not read transaction status") from exc
        return self._to_response(row) if row else None

    async def claim_next_transaction(self, claim_timeout_seconds: float) -> TransactionClaim | None:
        claim_token = uuid4()
        query = """
            WITH candidate AS (
                SELECT transaction_id
                FROM webhook_private.transactions
                WHERE status = 'PROCESSING'
                  AND (
                    claimed_at IS NULL
                    OR claimed_at < NOW() - ($1::double precision * INTERVAL '1 second')
                  )
                ORDER BY created_at, transaction_id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE webhook_private.transactions AS target
            SET claimed_at = NOW(),
                claim_token = $2,
                attempts = target.attempts + 1
            FROM candidate
            WHERE target.transaction_id = candidate.transaction_id
            RETURNING target.transaction_id
        """
        try:
            async with self._pool.acquire() as connection:
                row = await connection.fetchrow(query, claim_timeout_seconds, claim_token)
        except Exception as exc:
            raise StorageError("Could not claim a transaction for processing") from exc
        if row is None:
            return None
        return TransactionClaim(transaction_id=row["transaction_id"], claim_token=claim_token)

    async def mark_processed(self, claim: TransactionClaim) -> bool:
        query = """
            UPDATE webhook_private.transactions
            SET status = 'PROCESSED',
                processed_at = NOW(),
                claimed_at = NULL,
                claim_token = NULL
            WHERE transaction_id = $1
              AND claim_token = $2
              AND status = 'PROCESSING'
            RETURNING transaction_id
        """
        try:
            async with self._pool.acquire() as connection:
                completed = await connection.fetchval(
                    query, claim.transaction_id, claim.claim_token
                )
                return completed is not None
        except Exception as exc:
            # Keeping the claim in the database allows the stale-claim recovery path to retry it.
            raise StorageError("Could not mark transaction as processed") from exc

    @staticmethod
    def _to_response(row: asyncpg.Record) -> TransactionResponse:
        return TransactionResponse(
            transaction_id=row["transaction_id"],
            source_account=row["source_account"],
            destination_account=row["destination_account"],
            amount=row["amount"],
            currency=row["currency"],
            status=TransactionStatus(row["status"]),
            created_at=row["created_at"],
            processed_at=row["processed_at"],
        )
