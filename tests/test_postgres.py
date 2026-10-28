from __future__ import annotations

import asyncio
import os
from urllib.parse import urlparse
from uuid import uuid4

import pytest

from app.models import TransactionStatus, TransactionWebhook
from app.storage import PostgresTransactionStore


TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL", "")


@pytest.mark.skipif(not TEST_DATABASE_URL, reason="TEST_DATABASE_URL is not configured")
def test_postgres_persistence_and_claim_recovery() -> None:
    host = urlparse(TEST_DATABASE_URL).hostname
    if host not in {"localhost", "127.0.0.1"}:
        pytest.fail("Integration tests require a local PostgreSQL database")
    asyncio.run(_exercise_postgres(TEST_DATABASE_URL))


async def _exercise_postgres(database_url: str) -> None:
    store = await PostgresTransactionStore.connect(database_url)
    transaction_id = f"test_{uuid4().hex}"
    transaction = TransactionWebhook(
        transaction_id=transaction_id,
        source_account="source_test",
        destination_account="destination_test",
        amount="123.45",
        currency="INR",
    )

    try:
        await store.ensure_schema()
        inserted = await asyncio.gather(*(store.create_transaction(transaction) for _ in range(8)))
        assert inserted.count(True) == 1
        assert inserted.count(False) == 7

        initial = await store.get_transaction(transaction_id)
        assert initial is not None
        assert initial.status == TransactionStatus.PROCESSING
        assert str(initial.amount) == "123.45"

        first = await store.claim_next_transaction(0.1)
        assert first is not None and first.transaction_id == transaction_id
        assert await store.claim_next_transaction(0.1) is None

        await asyncio.sleep(0.12)
        recovered = await store.claim_next_transaction(0.1)
        assert recovered is not None and recovered.transaction_id == transaction_id
        assert recovered.claim_token != first.claim_token
        assert not await store.mark_processed(first)
        assert await store.mark_processed(recovered)

        finished = await store.get_transaction(transaction_id)
        assert finished is not None
        assert finished.status == TransactionStatus.PROCESSED
        assert finished.processed_at is not None
        assert await store.claim_next_transaction(0.1) is None
    finally:
        async with store._pool.acquire() as connection:
            await connection.execute(
                "DELETE FROM webhook_private.transactions WHERE transaction_id = $1", transaction_id
            )
        await store.close()
