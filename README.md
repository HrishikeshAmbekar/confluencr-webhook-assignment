# Transaction Webhook Service

A Python/FastAPI service that receives transaction webhooks, acknowledges them quickly, and processes each durable transaction in the background.

## Public links

- GitHub repository: https://github.com/HrishikeshAmbekar/confluencr-webhook-assignment
- Live API: `https://<your-render-service>.onrender.com`

Replace the API placeholder after deployment. The API's interactive docs will be available at `/docs`.

## Design choices

- **Fast acknowledgement:** `POST /v1/webhooks/transactions` first inserts one row into Postgres and immediately returns `202 Accepted`. It never waits for the simulated external call.
- **Idempotency:** `transaction_id` is the Postgres primary key. `INSERT ... ON CONFLICT DO NOTHING` means repeated concurrent webhooks create only one durable job and each receives `202`.
- **Reliable background processing:** the application stores claims, claim timestamps, and attempt counters in Postgres. The worker uses `FOR UPDATE SKIP LOCKED`; if it is interrupted after claiming a job, a restarted service recovers the stale claim and completes it. No work state is written to local disk.
- **Concurrent processing:** up to eight jobs wait asynchronously for the simulated external call. New inserts wake the worker immediately; a 30-second scan recovers pending work after interruptions without constant database polling.
- **Private storage:** the table lives in a private Supabase schema with row security enabled, outside the exposed Data API schemas.
- **Single process deployment:** the free Render plan provides one web-service instance, so the API and durable worker run in one process. A paid or multi-instance deployment should extract the same claim loop into a dedicated worker service.

## API

### Health check

`GET /`

```json
{
  "status": "HEALTHY",
  "current_time": "2026-10-27T10:30:00Z"
}
```

### Receive a webhook

`POST /v1/webhooks/transactions`

```json
{
  "transaction_id": "txn_abc123def456",
  "source_account": "acc_user_789",
  "destination_account": "acc_merchant_456",
  "amount": 1500,
  "currency": "INR"
}
```

Returns `202 Accepted` after the transaction is persisted. If storage or the background processor is unavailable, it returns `503` so the sender can retry safely.

### Retrieve status

`GET /v1/transactions/{transaction_id}`

```json
[
  {
    "transaction_id": "txn_abc123def456",
    "source_account": "acc_user_789",
    "destination_account": "acc_merchant_456",
    "amount": 1500,
    "currency": "INR",
    "status": "PROCESSED",
    "created_at": "2026-10-27T10:30:00Z",
    "processed_at": "2026-10-27T10:30:30Z"
  }
]
```

The status is `PROCESSING` until the 30-second simulated call finishes, then `PROCESSED`. Repeated webhooks with the same ID return `202`; the first stored payload remains authoritative even if a later duplicate contains different fields. Amounts are stored as exact decimal values and returned as JSON numbers without float conversion.

## Run locally

1. Create a Supabase project and use **Connect → Session pooler** to copy its Postgres URI. The session pooler works from IPv4 hosting providers. Add `sslmode=require`.
2. Copy `.env.example` to `.env`, then replace `DATABASE_URL` with that URI.
3. Create and activate a virtual environment, then install dependencies:

   ```powershell
   py -3.13 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -r requirements-dev.txt
   ```

4. Start the service:

   ```powershell
   uvicorn app.main:app --reload
   ```

The service creates the private `webhook_private.transactions` table and index at startup. The same SQL is available in `sql/schema.sql` for review or manual execution.

## Test locally

```powershell
pytest
```

The API tests cover fast acknowledgement, concurrent processing, duplicate delivery, conflicting duplicates, interrupted-claim recovery, temporary storage failures, amount precision, and invalid payloads. A separate PostgreSQL integration test checks the actual schema and claim queries in CI against a local PostgreSQL service. It skips locally unless `TEST_DATABASE_URL` points to localhost. Run the manual API check against a live Supabase-backed service before submission.

Manual API check:

```powershell
curl.exe -i -X POST http://127.0.0.1:8000/v1/webhooks/transactions `
  -H "Content-Type: application/json" `
  -d '{"transaction_id":"txn_abc123def456","source_account":"acc_user_789","destination_account":"acc_merchant_456","amount":1500,"currency":"INR"}'

curl.exe http://127.0.0.1:8000/v1/transactions/txn_abc123def456
```

## Deploy to Render and Supabase

1. Create a free Supabase project and take the session-pooler connection string from **Connect**. Keep the password private.
2. Push this directory to a public GitHub repository.
3. In Render, choose **New → Blueprint** and select that repository. Render reads `render.yaml`.
4. Set the `DATABASE_URL` environment variable to the Supabase connection string. Do not paste it into GitHub or the README.
5. After deployment, open `https://<service>.onrender.com/` once to warm the free instance, then test the webhook and status endpoints.
6. Replace the public-link placeholders at the top of this README with the actual GitHub and Render URLs.

## Security scope

The assignment does not provide a webhook signing secret or authentication scheme, and graders need to call the public endpoints. Use synthetic transaction and account IDs only. For real payment traffic, verify provider signatures on incoming webhooks, authenticate status queries, and add rate limits before accepting customer data. Keep the Supabase database password only in local `.env` or Render's environment settings.

## Free-tier limitation

Render's free web service spins down after 15 minutes without inbound traffic. Its first request after sleep can take about a minute, so it cannot meet the under-500-ms acknowledgement target from a cold start. While warm, the endpoint writes a small Postgres row and is designed to meet the target. If the service sleeps or a database outage lasts beyond that idle window, stored work resumes when the next request wakes it. Production traffic needs an always-on service and a separately scaled worker.
