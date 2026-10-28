from __future__ import annotations

import asyncio
import logging
import time

from app.logging_config import error_type
from app.storage import StorageError, TransactionClaim, TransactionStore

logger = logging.getLogger("webhook.worker")


class TransactionWorker:
    """Polls durable jobs and performs the assignment's simulated 30-second work."""

    def __init__(
        self,
        store: TransactionStore,
        *,
        processing_delay_seconds: float,
        poll_interval_seconds: float,
        claim_timeout_seconds: float,
        max_concurrent_jobs: int,
    ) -> None:
        self._store = store
        self._processing_delay_seconds = processing_delay_seconds
        self._poll_interval_seconds = poll_interval_seconds
        self._claim_timeout_seconds = claim_timeout_seconds
        self._max_concurrent_jobs = max_concurrent_jobs
        self._stop_event = asyncio.Event()
        self._wake_event = asyncio.Event()
        self._active_jobs: set[asyncio.Task[None]] = set()

    def notify_new_transaction(self) -> None:
        self._wake_event.set()

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()

    async def run(self) -> None:
        logger.info("worker.started")
        try:
            while not self._stop_event.is_set():
                # Clear before querying so notifications arriving during a query are retained.
                self._wake_event.clear()
                while len(self._active_jobs) < self._max_concurrent_jobs:
                    try:
                        claim = await self._store.claim_next_transaction(self._claim_timeout_seconds)
                    except StorageError as exc:
                        logger.error("worker.claim_failed", extra={"error_type": error_type(exc)})
                        break
                    if claim is None:
                        break
                    task = asyncio.create_task(self._process(claim), name=f"transaction-{claim.transaction_id}")
                    self._active_jobs.add(task)
                    task.add_done_callback(self._on_job_done)

                if not self._stop_event.is_set():
                    await self._wait_for_wake()
        finally:
            for task in self._active_jobs:
                task.cancel()
            await asyncio.gather(*self._active_jobs, return_exceptions=True)
            logger.info("worker.stopped")

    async def _process(self, claim: TransactionClaim) -> None:
        context = {"transaction_id": claim.transaction_id, "attempt": claim.attempt}
        started = time.perf_counter()
        logger.info("transaction.processing_started", extra=context)
        try:
            await asyncio.sleep(self._processing_delay_seconds)
            completed = await self._store.mark_processed(claim)
            if completed:
                logger.info(
                    "transaction.processing_completed",
                    extra={**context, "duration_ms": round((time.perf_counter() - started) * 1000, 2)},
                )
            else:
                logger.warning(
                    "transaction.claim_superseded",
                    extra={**context, "duration_ms": round((time.perf_counter() - started) * 1000, 2)},
                )
        except asyncio.CancelledError:
            # The claim remains durable and will be recovered after its timeout.
            logger.warning(
                "transaction.processing_interrupted",
                extra={**context, "duration_ms": round((time.perf_counter() - started) * 1000, 2)},
            )
            raise
        except Exception as exc:
            logger.error(
                "transaction.processing_failed",
                extra={**context, "error_type": error_type(exc),
                       "duration_ms": round((time.perf_counter() - started) * 1000, 2)},
            )

    def _on_job_done(self, task: asyncio.Task[None]) -> None:
        self._active_jobs.discard(task)
        if not task.cancelled() and (error := task.exception()) is not None:
            logger.error("worker.job_failed", extra={"error_type": error_type(error)})
        self._wake_event.set()

    async def _wait_for_wake(self) -> None:
        try:
            await asyncio.wait_for(self._wake_event.wait(), timeout=self._poll_interval_seconds)
        except TimeoutError:
            pass
