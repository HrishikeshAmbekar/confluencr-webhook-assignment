"""Run the durable worker as a separate process, for example on Vercel Daemons."""

from __future__ import annotations

import asyncio
import logging
import signal

from app.config import Settings
from app.storage import PostgresTransactionStore
from app.worker import TransactionWorker


async def main() -> None:
    settings = Settings()
    store = await PostgresTransactionStore.connect(settings.database_url)
    try:
        await store.ensure_schema()
        worker = TransactionWorker(
            store,
            processing_delay_seconds=settings.processing_delay_seconds,
            poll_interval_seconds=settings.poll_interval_seconds,
            claim_timeout_seconds=settings.claim_timeout_seconds,
            max_concurrent_jobs=settings.max_concurrent_jobs,
        )
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGTERM, worker.stop)
            loop.add_signal_handler(signal.SIGINT, worker.stop)
        except NotImplementedError:
            # Windows can run local tests; production daemon containers run Linux.
            pass
        await worker.run()
    finally:
        await store.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(main())
