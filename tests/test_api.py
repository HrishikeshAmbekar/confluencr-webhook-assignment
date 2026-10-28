from __future__ import annotations

import io
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Callable

from fastapi.testclient import TestClient

from app.config import Settings
from app.logging_config import JsonEventFormatter
from app.main import create_app
from app.worker import TransactionWorker
from app.storage import StorageError
from tests.conftest import CrashingClaimStore, FailingInsertStore, FailingMarkOnceStore, MemoryTransactionStore


FAST_SETTINGS = Settings(
    processing_delay_seconds=0.25,
    poll_interval_seconds=0.02,
    claim_timeout_seconds=0.60,
    max_concurrent_jobs=4,
)


def make_client(store: MemoryTransactionStore) -> TestClient:
    return TestClient(create_app(settings=FAST_SETTINGS, store=store))


def wait_until(predicate: Callable[[], bool], *, timeout_seconds: float = 2.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    while not predicate():
        assert time.monotonic() < deadline, "Condition was not met before timeout"
        time.sleep(0.01)


def test_health_check_has_utc_timestamp() -> None:
    with make_client(MemoryTransactionStore()) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.json()["status"] == "HEALTHY"
    assert datetime.fromisoformat(response.json()["current_time"]).utcoffset().total_seconds() == 0


def test_webhook_is_fast_then_becomes_processed(payload: dict[str, object]) -> None:
    store = MemoryTransactionStore()
    with make_client(store) as client:
        started_at = time.perf_counter()
        accepted = client.post("/v1/webhooks/transactions", json=payload)
        elapsed_seconds = time.perf_counter() - started_at
        immediate_status = client.get(f"/v1/transactions/{payload['transaction_id']}")
        wait_until(lambda: store.completed_count == 1)
        finished_status = client.get(f"/v1/transactions/{payload['transaction_id']}")

    assert accepted.status_code == 202
    assert elapsed_seconds < 0.5
    assert immediate_status.json()[0]["status"] == "PROCESSING"
    assert finished_status.json()[0]["status"] == "PROCESSED"
    assert finished_status.json()[0]["processed_at"] is not None


def test_concurrent_duplicate_webhooks_create_one_job(payload: dict[str, object]) -> None:
    store = MemoryTransactionStore()
    with make_client(store) as client:
        def submit() -> int:
            return client.post("/v1/webhooks/transactions", json=payload).status_code

        with ThreadPoolExecutor(max_workers=8) as executor:
            codes = list(executor.map(lambda _: submit(), range(8)))
        wait_until(lambda: store.completed_count == 1)
        status_response = client.get(f"/v1/transactions/{payload['transaction_id']}")

    assert codes == [202] * 8
    assert status_response.json()[0]["status"] == "PROCESSED"
    assert store.completed_count == 1


def test_restart_recovers_an_incomplete_claim(payload: dict[str, object]) -> None:
    store = MemoryTransactionStore()
    slow_settings = FAST_SETTINGS.model_copy(
        update={"processing_delay_seconds": 0.80, "claim_timeout_seconds": 1.20}
    )
    app = create_app(settings=slow_settings, store=store)

    with TestClient(app) as client:
        assert client.post("/v1/webhooks/transactions", json=payload).status_code == 202
        wait_until(lambda: store.claim_count == 1)

    with make_client(store) as recovered_client:
        wait_until(lambda: store.completed_count == 1)
        response = recovered_client.get(f"/v1/transactions/{payload['transaction_id']}")

    assert response.json()[0]["status"] == "PROCESSED"
    assert store.completed_count == 1


def test_distinct_webhooks_are_claimed_concurrently(payload: dict[str, object]) -> None:
    store = MemoryTransactionStore()
    settings = FAST_SETTINGS.model_copy(
        update={"processing_delay_seconds": 0.80, "claim_timeout_seconds": 1.50}
    )
    with TestClient(create_app(settings=settings, store=store)) as client:
        for number in range(3):
            item = {**payload, "transaction_id": f"txn_parallel_{number}"}
            assert client.post("/v1/webhooks/transactions", json=item).status_code == 202
        wait_until(lambda: store.claim_count == 3)
        assert store.completed_count == 0


def test_external_worker_processes_a_persisted_webhook(payload: dict[str, object]) -> None:
    store = MemoryTransactionStore()
    settings = FAST_SETTINGS.model_copy(update={"worker_mode": "external"})
    app = create_app(settings=settings, store=store)
    worker = TransactionWorker(
        store,
        processing_delay_seconds=settings.processing_delay_seconds,
        poll_interval_seconds=settings.poll_interval_seconds,
        claim_timeout_seconds=settings.claim_timeout_seconds,
        max_concurrent_jobs=settings.max_concurrent_jobs,
    )
    with TestClient(app) as client:
        assert client.get("/").status_code == 200
        assert client.post("/v1/webhooks/transactions", json=payload).status_code == 202
        assert client.get(f"/v1/transactions/{payload['transaction_id']}").json()[0]["status"] == "PROCESSING"
        assert client.portal is not None
        future = client.portal.start_task_soon(worker.run)
        try:
            wait_until(lambda: store.completed_count == 1)
        finally:
            client.portal.call(worker.stop)
            future.result(timeout=2)
        assert client.get(f"/v1/transactions/{payload['transaction_id']}").json()[0]["status"] == "PROCESSED"


def test_failed_completion_is_retried_after_claim_expires(payload: dict[str, object], caplog) -> None:
    store = FailingMarkOnceStore()
    app_logger = logging.getLogger("webhook")
    app_logger.addHandler(caplog.handler)
    try:
        with make_client(store) as client:
            assert client.post("/v1/webhooks/transactions", json=payload).status_code == 202
            wait_until(lambda: any(record.getMessage() == "transaction.processing_completed"
                                   for record in caplog.records))
            response = client.get(f"/v1/transactions/{payload['transaction_id']}")
    finally:
        app_logger.removeHandler(caplog.handler)

    assert store.claim_count == 2
    assert response.json()[0]["status"] == "PROCESSED"
    assert any(record.getMessage() == "transaction.processing_failed" and record.attempt == 1
               for record in caplog.records)
    assert any(record.getMessage() == "transaction.processing_completed" and record.attempt == 2
               for record in caplog.records)


def test_conflicting_duplicate_keeps_original_payload(payload: dict[str, object]) -> None:
    store = MemoryTransactionStore()
    with make_client(store) as client:
        first = client.post("/v1/webhooks/transactions", json=payload)
        conflict = client.post(
            "/v1/webhooks/transactions", json={**payload, "amount": 999, "currency": "USD"}
        )
        response = client.get(f"/v1/transactions/{payload['transaction_id']}")

    assert (first.status_code, conflict.status_code) == (202, 202)
    assert response.json()[0]["amount"] == 1500
    assert response.json()[0]["currency"] == "INR"


def test_large_decimal_amount_is_preserved_as_json_number(payload: dict[str, object]) -> None:
    with make_client(MemoryTransactionStore()) as client:
        assert client.post(
            "/v1/webhooks/transactions",
            json={**payload, "amount": "9007199254740991.01"},
        ).status_code == 202
        response = client.get(f"/v1/transactions/{payload['transaction_id']}")

    assert '"amount": 9007199254740991.01' in response.text


def test_invalid_webhooks_are_rejected(payload: dict[str, object]) -> None:
    invalid_payloads = [
        {**payload, "amount": 0},
        {**payload, "amount": -1},
        {**payload, "amount": "1.234"},
        {**payload, "amount": "NaN"},
        {**payload, "currency": "US"},
        {**payload, "transaction_id": ""},
        {**payload, "transaction_id": " txn_abc123def456"},
        {**payload, "transaction_id": "txn/abc123def456"},
        {**payload, "transaction_id": "txn\nabc123def456"},
        {**payload, "source_account": "acc_user_789 "},
        {**payload, "currency": " INR"},
        {**payload, "unexpected_field": "value"},
    ]
    with make_client(MemoryTransactionStore()) as client:
        for invalid_payload in invalid_payloads:
            assert client.post("/v1/webhooks/transactions", json=invalid_payload).status_code == 422


def test_storage_failure_does_not_acknowledge_webhook(payload: dict[str, object]) -> None:
    with make_client(FailingInsertStore()) as client:
        response = client.post("/v1/webhooks/transactions", json=payload)

    assert response.status_code == 503


def test_crashed_worker_is_reported_and_does_not_acknowledge(payload: dict[str, object]) -> None:
    store = CrashingClaimStore()
    app = create_app(settings=FAST_SETTINGS, store=store)
    with TestClient(app) as client:
        wait_until(lambda: app.state.worker_task.done())
        assert client.get("/").status_code == 503
        assert client.post("/v1/webhooks/transactions", json=payload).status_code == 503
        assert client.get(f"/v1/transactions/{payload['transaction_id']}").status_code == 404


def test_unknown_transaction_returns_404() -> None:
    with make_client(MemoryTransactionStore()) as client:
        response = client.get("/v1/transactions/not-found")

    assert response.status_code == 404


def test_structured_logs_trace_processing_without_payload_data(payload: dict[str, object]) -> None:
    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(JsonEventFormatter())
    app_logger = logging.getLogger("webhook")
    app_logger.addHandler(handler)
    store = MemoryTransactionStore()
    private_payload = {**payload, "source_account": "private_source_account"}
    try:
        with make_client(store) as client:
            first = client.post("/v1/webhooks/transactions", json=private_payload)
            duplicate = client.post("/v1/webhooks/transactions", json=private_payload)
            wait_until(lambda: '"event":"transaction.processing_completed"' in output.getvalue())
    finally:
        app_logger.removeHandler(handler)

    events = [json.loads(line) for line in output.getvalue().splitlines()]
    names = [event["event"] for event in events]
    assert "webhook.accepted" in names
    assert "webhook.duplicate" in names
    assert "transaction.processing_started" in names
    assert "transaction.processing_completed" in names
    accepted = next(event for event in events if event["event"] == "webhook.accepted")
    completed_request = next(event for event in events if event["event"] == "request.completed"
                             and event["request_id"] == first.headers["x-request-id"])
    assert accepted["request_id"] == first.headers["x-request-id"]
    assert duplicate.headers["x-request-id"] != first.headers["x-request-id"]
    assert completed_request["status_code"] == 202
    assert completed_request["route"] == "/v1/webhooks/transactions"
    assert completed_request["duration_ms"] >= 0
    completed = next(event for event in events if event["event"] == "transaction.processing_completed")
    assert completed["transaction_id"] == payload["transaction_id"]
    assert completed["attempt"] == 1
    assert completed["duration_ms"] >= 0
    assert "private_source_account" not in output.getvalue()


def test_storage_error_log_does_not_expose_exception_details(payload: dict[str, object]) -> None:
    class SecretErrorStore(MemoryTransactionStore):
        async def create_transaction(self, transaction):
            try:
                raise RuntimeError("secret-database-url")
            except RuntimeError as exc:
                raise StorageError("secret-database-url") from exc

    output = io.StringIO()
    handler = logging.StreamHandler(output)
    handler.setFormatter(JsonEventFormatter())
    app_logger = logging.getLogger("webhook")
    app_logger.addHandler(handler)
    try:
        with make_client(SecretErrorStore()) as client:
            response = client.post("/v1/webhooks/transactions", json=payload)
    finally:
        app_logger.removeHandler(handler)

    events = [json.loads(line) for line in output.getvalue().splitlines()]
    failure = next(event for event in events if event["event"] == "webhook.persistence_failed")
    assert response.status_code == 503
    assert failure["error_type"] == "RuntimeError"
    assert "secret-database-url" not in output.getvalue()
