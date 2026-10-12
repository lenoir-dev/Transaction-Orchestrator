-- Design migration. Apply only in a disposable PostgreSQL database during Phase 0.
BEGIN;

CREATE TABLE signer_accounts (
    chain_id numeric(78,0) NOT NULL CHECK (chain_id > 0),
    signer_address bytea NOT NULL CHECK (octet_length(signer_address) = 20),
    signer_id text NOT NULL,
    next_nonce numeric(20,0) NOT NULL CHECK (next_nonce >= 0 AND next_nonce <= 18446744073709551615),
    paused_reason text,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, signer_address),
    UNIQUE (chain_id, signer_id),
    UNIQUE (chain_id, signer_address, signer_id)
);

CREATE TABLE transaction_intents (
    intent_id uuid PRIMARY KEY,
    tenant_id text NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 255),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    policy_version text NOT NULL,
    chain_id numeric(78,0) NOT NULL,
    signer_address bytea NOT NULL,
    signer_id text NOT NULL,
    to_address bytea NOT NULL CHECK (octet_length(to_address) = 20),
    value_wei numeric(78,0) NOT NULL CHECK (value_wei >= 0),
    calldata bytea NOT NULL DEFAULT ''::bytea,
    gas_limit bigint NOT NULL CHECK (gas_limit > 0),
    confirmation_depth integer NOT NULL CHECK (confirmation_depth BETWEEN 1 AND 100000),
    max_fee_cap_wei numeric(78,0) NOT NULL CHECK (max_fee_cap_wei > 0),
    max_tip_cap_wei numeric(78,0) NOT NULL CHECK (max_tip_cap_wei > 0 AND max_tip_cap_wei <= max_fee_cap_wei),
    max_replacements integer NOT NULL CHECK (max_replacements BETWEEN 0 AND 100),
    state text NOT NULL CHECK (state IN ('CREATED','RESERVED','SIGNING','READY','UNCERTAIN','PENDING','MINED','CONFIRMED','REORGED','BLOCKED','FAILED')),
    reason_code text,
    resume_state text CHECK (resume_state IS NULL OR resume_state IN ('CREATED','RESERVED','SIGNING','READY','UNCERTAIN','PENDING','MINED','REORGED')),
    active_attempt_number integer,
    winning_attempt_number integer,
    confirmation_count integer NOT NULL DEFAULT 0 CHECK (confirmation_count >= 0),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    lease_token uuid,
    lease_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key),
    UNIQUE (intent_id, chain_id, signer_address),
    FOREIGN KEY (chain_id, signer_address, signer_id) REFERENCES signer_accounts (chain_id, signer_address, signer_id),
    CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
    CHECK ((state = 'BLOCKED') = (resume_state IS NOT NULL)),
    CHECK (active_attempt_number IS NULL OR active_attempt_number > 0),
    CHECK (winning_attempt_number IS NULL OR winning_attempt_number > 0)
);

CREATE TABLE nonce_reservations (
    intent_id uuid PRIMARY KEY,
    chain_id numeric(78,0) NOT NULL,
    signer_address bytea NOT NULL,
    nonce numeric(20,0) NOT NULL CHECK (nonce >= 0 AND nonce <= 18446744073709551615),
    reserved_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (chain_id, signer_address, nonce),
    UNIQUE (intent_id, nonce),
    FOREIGN KEY (intent_id, chain_id, signer_address) REFERENCES transaction_intents (intent_id, chain_id, signer_address),
    FOREIGN KEY (chain_id, signer_address) REFERENCES signer_accounts (chain_id, signer_address)
);

CREATE TABLE transaction_attempts (
    intent_id uuid NOT NULL,
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    nonce numeric(20,0) NOT NULL,
    status text NOT NULL CHECK (status IN ('PREPARING','SIGNED','BROADCAST_CLAIMED','OBSERVED','REJECTED')),
    gas_limit bigint NOT NULL CHECK (gas_limit > 0),
    max_fee_per_gas numeric(78,0) NOT NULL CHECK (max_fee_per_gas > 0),
    max_priority_fee_per_gas numeric(78,0) NOT NULL CHECK (max_priority_fee_per_gas > 0 AND max_priority_fee_per_gas <= max_fee_per_gas),
    raw_tx bytea,
    transaction_hash bytea UNIQUE CHECK (transaction_hash IS NULL OR octet_length(transaction_hash) = 32),
    signed_at timestamptz,
    broadcast_claimed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (intent_id, attempt_number),
    UNIQUE (intent_id, attempt_number, transaction_hash),
    FOREIGN KEY (intent_id, nonce) REFERENCES nonce_reservations (intent_id, nonce),
    CHECK ((raw_tx IS NULL AND transaction_hash IS NULL AND signed_at IS NULL) OR
           (raw_tx IS NOT NULL AND octet_length(raw_tx) > 0 AND transaction_hash IS NOT NULL AND signed_at IS NOT NULL)),
    CHECK (status = 'PREPARING' OR raw_tx IS NOT NULL)
);

ALTER TABLE transaction_intents
    ADD CONSTRAINT active_attempt_fk FOREIGN KEY (intent_id, active_attempt_number)
    REFERENCES transaction_attempts (intent_id, attempt_number) DEFERRABLE INITIALLY DEFERRED,
    ADD CONSTRAINT winning_attempt_fk FOREIGN KEY (intent_id, winning_attempt_number)
    REFERENCES transaction_attempts (intent_id, attempt_number) DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE observed_receipts (
    receipt_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    intent_id uuid NOT NULL,
    attempt_number integer NOT NULL,
    transaction_hash bytea NOT NULL CHECK (octet_length(transaction_hash) = 32),
    block_number numeric(20,0) NOT NULL CHECK (block_number >= 0),
    block_hash bytea NOT NULL CHECK (octet_length(block_hash) = 32),
    execution_status smallint NOT NULL CHECK (execution_status IN (0,1)),
    canonical_observed boolean NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (intent_id, attempt_number, transaction_hash)
        REFERENCES transaction_attempts (intent_id, attempt_number, transaction_hash)
);
CREATE INDEX observed_receipts_by_intent ON observed_receipts (intent_id, observed_at DESC);

CREATE TABLE audit_events (
    event_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    intent_id uuid NOT NULL REFERENCES transaction_intents (intent_id),
    event_type text NOT NULL,
    reason_code text,
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX audit_events_by_intent ON audit_events (intent_id, event_id);
CREATE INDEX intents_recovery ON transaction_intents (state, updated_at)
    WHERE state NOT IN ('CONFIRMED','FAILED');

-- Database-enforced immutability of signed attempt identity and bytes.
CREATE FUNCTION protect_signed_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.raw_tx IS NOT NULL AND
       (NEW.raw_tx IS DISTINCT FROM OLD.raw_tx OR
        NEW.transaction_hash IS DISTINCT FROM OLD.transaction_hash OR
        NEW.nonce IS DISTINCT FROM OLD.nonce OR
        NEW.gas_limit IS DISTINCT FROM OLD.gas_limit OR
        NEW.max_fee_per_gas IS DISTINCT FROM OLD.max_fee_per_gas OR
        NEW.max_priority_fee_per_gas IS DISTINCT FROM OLD.max_priority_fee_per_gas OR
        NEW.signed_at IS DISTINCT FROM OLD.signed_at) THEN
        RAISE EXCEPTION 'signed attempt is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER signed_attempt_immutable BEFORE UPDATE ON transaction_attempts
    FOR EACH ROW EXECUTE FUNCTION protect_signed_attempt();

COMMIT;
