from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import AsyncIterator

import simplejson
from fastapi import FastAPI, HTTPException, Request, Response, status

from app.config import Settings
from app.models import HealthResponse, TransactionResponse, TransactionWebhook, WebhookAcknowledgement
from app.storage import PostgresTransactionStore, StorageError, TransactionStore
from app.worker import TransactionWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def create_app(
    *, settings: Settings | None = None, store: TransactionStore | None = None
) -> FastAPI:
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        active_store = store or await PostgresTransactionStore.connect(settings.database_url)
        worker: TransactionWorker | None = None
        worker_task: asyncio.Task[None] | None = None
        try:
            await active_store.ensure_schema()
            app.state.store = active_store
            if settings.worker_mode == "embedded":
                worker = TransactionWorker(
                    active_store,
                    processing_delay_seconds=settings.processing_delay_seconds,
                    poll_interval_seconds=settings.poll_interval_seconds,
                    claim_timeout_seconds=settings.claim_timeout_seconds,
                    max_concurrent_jobs=settings.max_concurrent_jobs,
                )
                worker_task = asyncio.create_task(worker.run(), name="transaction-worker")
            app.state.worker = worker
            app.state.worker_task = worker_task
            yield
        finally:
            if worker is not None:
                worker.stop()
            if worker_task is not None:
                worker_task.cancel()
                await asyncio.gather(worker_task, return_exceptions=True)
            await active_store.close()

    app = FastAPI(
        title="Transaction Webhook Service",
        version="1.0.0",
        description="Durable, idempotent processing for incoming transaction webhooks.",
        lifespan=lifespan,
    )

    @app.get("/", response_model=HealthResponse, tags=["health"])
    async def health_check(request: Request) -> HealthResponse:
        if request.app.state.worker_task is not None and request.app.state.worker_task.done():
            raise HTTPException(status_code=503, detail="Background processor is unavailable")
        return HealthResponse(current_time=datetime.now(UTC))

    @app.post(
        "/v1/webhooks/transactions",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=WebhookAcknowledgement,
        tags=["transactions"],
    )
    async def receive_transaction(
        transaction: TransactionWebhook, request: Request
    ) -> WebhookAcknowledgement:
        if request.app.state.worker_task is not None and request.app.state.worker_task.done():
            raise HTTPException(status_code=503, detail="Background processor is unavailable")
        try:
            inserted = await request.app.state.store.create_transaction(transaction)
        except StorageError as exc:
            logger.exception("Webhook was not acknowledged because it could not be persisted")
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Transaction storage is temporarily unavailable. Please retry the webhook.",
            ) from exc
        if inserted and request.app.state.worker is not None:
            request.app.state.worker.notify_new_transaction()
        return WebhookAcknowledgement()

    @app.get(
        "/v1/transactions/{transaction_id}",
        response_model=list[TransactionResponse],
        tags=["transactions"],
    )
    async def get_transaction_status(transaction_id: str, request: Request) -> Response:
        try:
            transaction = await request.app.state.store.get_transaction(transaction_id)
        except StorageError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Transaction storage is temporarily unavailable.",
            ) from exc
        if transaction is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Transaction not found")
        # Decimal must remain an exact JSON number; float conversion loses cents
        # for large valid NUMERIC(18, 2) values.
        body = simplejson.dumps(
            [transaction.model_dump(mode="python")],
            use_decimal=True,
            default=_json_default,
        )
        return Response(content=body, media_type="application/json")

    return app


app = create_app()
