-- Supabase exposes public-schema tables through its Data API by default.
-- Keep webhook/account data in a schema that is not exposed to the Data API.
CREATE SCHEMA IF NOT EXISTS webhook_private;
REVOKE ALL ON SCHEMA webhook_private FROM PUBLIC;

CREATE TABLE IF NOT EXISTS webhook_private.transactions (
    transaction_id VARCHAR(128) PRIMARY KEY,
    source_account VARCHAR(128) NOT NULL,
    destination_account VARCHAR(128) NOT NULL,
    amount NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency VARCHAR(3) NOT NULL CHECK (currency ~ '^[A-Za-z]{3}$'),
    status TEXT NOT NULL CHECK (status IN ('PROCESSING', 'PROCESSED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    claimed_at TIMESTAMPTZ,
    claim_token UUID,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0)
);

ALTER TABLE webhook_private.transactions ENABLE ROW LEVEL SECURITY;

CREATE INDEX IF NOT EXISTS transactions_pending_claim_idx
    ON webhook_private.transactions (created_at)
    WHERE status = 'PROCESSING';
