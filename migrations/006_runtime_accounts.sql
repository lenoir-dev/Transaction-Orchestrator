BEGIN;

ALTER TABLE signer_accounts
    ADD COLUMN reconciliation_status text NOT NULL DEFAULT 'TRUSTED_SEED'
        CHECK (reconciliation_status IN ('TRUSTED_SEED','RECONCILING','READY',
            'BLOCKED_EXTERNAL_NONCE','RPC_UNAVAILABLE')),
    ADD COLUMN reconciliation_reason text,
    ADD COLUMN signer_status text NOT NULL DEFAULT 'UNKNOWN'
        CHECK (signer_status IN ('UNKNOWN','AVAILABLE','UNAVAILABLE')),
    ADD COLUMN signer_reason text,
    ADD COLUMN reconciliation_token uuid,
    ADD COLUMN network_nonce_initialized boolean NOT NULL DEFAULT true,
    ADD COLUMN network_managed boolean NOT NULL DEFAULT false,
    ADD COLUMN account_next_reconcile_at timestamptz,
    ADD COLUMN nonce_reconciled_at timestamptz,
    ADD COLUMN observed_latest_nonce numeric(20,0)
        CHECK (observed_latest_nonce BETWEEN 0 AND 18446744073709551615),
    ADD COLUMN observed_pending_nonce numeric(20,0)
        CHECK (observed_pending_nonce BETWEEN 0 AND 18446744073709551615);

CREATE INDEX signer_accounts_reconciliation_due ON signer_accounts
    (account_next_reconcile_at,chain_id,signer_id);

ALTER TABLE transaction_intents
    ADD COLUMN runtime_retry_at timestamptz,
    ADD COLUMN runtime_retry_count integer NOT NULL DEFAULT 0
        CHECK (runtime_retry_count BETWEEN 0 AND 30),
    ADD COLUMN runtime_retry_class text;
CREATE INDEX intents_runtime_due ON transaction_intents (runtime_retry_at,intent_id)
    WHERE state IN ('READY','SIGNING','UNCERTAIN','PENDING','MINED',
        'CONFIRMING','CONFIRMED','REORGED');

-- Record only correctness-relevant conflicts. Routine probe latency and
-- endpoint availability are kept outside the durable transaction ledger.
CREATE TABLE account_reconciliation_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chain_id numeric(78,0) NOT NULL,
    signer_address bytea NOT NULL,
    result text NOT NULL,
    reason text NOT NULL,
    local_next_nonce numeric(20,0) NOT NULL,
    observed_latest_nonce numeric(20,0),
    observed_pending_nonce numeric(20,0),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (chain_id,signer_address)
        REFERENCES signer_accounts (chain_id,signer_address)
);
CREATE INDEX account_reconciliation_events_account ON account_reconciliation_events
    (chain_id,signer_address,event_id DESC);

COMMIT;
