BEGIN;

ALTER TABLE transaction_intents
    ADD COLUMN broadcast_first_claimed_at timestamptz,
    ADD COLUMN broadcast_send_count integer NOT NULL DEFAULT 0 CHECK (broadcast_send_count >= 0),
    ADD COLUMN next_reconcile_at timestamptz,
    ADD CONSTRAINT broadcast_claim_has_signed_attempt CHECK (
        broadcast_send_count = 0 OR active_attempt_number IS NOT NULL
    );

CREATE TABLE broadcast_observations (
    observation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    intent_id uuid NOT NULL,
    attempt_number integer NOT NULL DEFAULT 1 CHECK (attempt_number = 1),
    endpoint text NOT NULL CHECK (length(endpoint) BETWEEN 1 AND 64),
    kind text NOT NULL CHECK (kind IN ('CLAIM','SEND','LOOKUP','RECEIPT','NONCE')),
    classification text NOT NULL CHECK (classification IN (
        'CLAIMED','SUCCESS','ALREADY_KNOWN','AMBIGUOUS_TRANSPORT',
        'DETERMINISTIC_REJECTION','NONCE_AMBIGUITY','FEE_REJECTION',
        'PROVIDER_ERROR','HASH_MISMATCH','FOUND_TRANSACTION','FOUND_RECEIPT',
        'NOT_OBSERVED','CONFLICT'
    )),
    transaction_hash bytea NOT NULL CHECK (octet_length(transaction_hash) = 32),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (intent_id, attempt_number, transaction_hash)
        REFERENCES transaction_attempts (intent_id, attempt_number, transaction_hash)
);
CREATE INDEX broadcast_observations_by_intent
    ON broadcast_observations (intent_id, observation_id DESC);

-- A committed send claim is represented by UNCERTAIN plus the existing fenced
-- lease. A crash leaves work recoverable without assuming the RPC was rejected.
ALTER TABLE transaction_intents ADD CONSTRAINT broadcast_state_alignment CHECK (
    state NOT IN ('UNCERTAIN','PENDING') OR
    (active_attempt_number = 1 AND broadcast_first_claimed_at IS NOT NULL)
);

COMMIT;
