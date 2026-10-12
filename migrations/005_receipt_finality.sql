BEGIN;

ALTER TABLE transaction_intents DROP CONSTRAINT transaction_intents_state_check;
ALTER TABLE transaction_intents ADD CONSTRAINT transaction_intents_state_check CHECK
    (state IN ('CREATED','RESERVED','SIGNING','READY','UNCERTAIN','PENDING',
               'MINED','CONFIRMING','CONFIRMED','REORGED','BLOCKED','FAILED'));
ALTER TABLE transaction_intents DROP CONSTRAINT transaction_intents_resume_state_check;
ALTER TABLE transaction_intents ADD CONSTRAINT transaction_intents_resume_state_check CHECK
    (resume_state IS NULL OR resume_state IN ('CREATED','RESERVED','SIGNING','READY','UNCERTAIN',
        'PENDING','MINED','CONFIRMING','REORGED'));
ALTER TABLE transaction_intents ADD COLUMN max_reorg_depth integer
    CHECK (max_reorg_depth BETWEEN 1 AND 1000),
    ADD COLUMN receipt_next_poll_at timestamptz,
    ADD COLUMN receipt_verified_head numeric(20,0)
    CHECK (receipt_verified_head >= 0);
CREATE INDEX receipt_poll_due ON transaction_intents (receipt_next_poll_at,created_at)
    WHERE state IN ('READY','SIGNING','UNCERTAIN','PENDING','MINED','CONFIRMING','CONFIRMED','REORGED');

ALTER TABLE observed_receipts ADD COLUMN attempt_id uuid,
    ADD COLUMN transaction_index numeric(20,0) CHECK (transaction_index >= 0),
    ADD COLUMN gas_used numeric(20,0) CHECK (gas_used >= 0),
    ADD COLUMN effective_gas_price numeric(78,0) CHECK (effective_gas_price >= 0),
    ADD COLUMN verification_status text NOT NULL DEFAULT 'UNVERIFIED'
        CHECK (verification_status IN ('UNVERIFIED','CANONICAL','ORPHANED')),
    ADD COLUMN confirmations integer NOT NULL DEFAULT 0 CHECK (confirmations >= 0),
    ADD COLUMN verified_head numeric(20,0) CHECK (verified_head >= 0),
    ADD COLUMN last_verified_at timestamptz;
UPDATE observed_receipts r SET attempt_id=a.attempt_id
    FROM transaction_attempts a WHERE a.intent_id=r.intent_id AND a.attempt_number=r.attempt_number;
ALTER TABLE transaction_attempts ADD CONSTRAINT attempt_receipt_identity
    UNIQUE (attempt_id,intent_id,attempt_number,transaction_hash);
ALTER TABLE observed_receipts ALTER COLUMN attempt_id SET NOT NULL,
    ADD CONSTRAINT receipt_attempt_identity FOREIGN KEY (attempt_id,intent_id,attempt_number,transaction_hash)
        REFERENCES transaction_attempts(attempt_id,intent_id,attempt_number,transaction_hash);
CREATE INDEX observed_receipts_identity ON observed_receipts
    (intent_id,transaction_hash,block_number,block_hash,receipt_id DESC);

CREATE TABLE observed_blocks (
    chain_id numeric(78,0) NOT NULL,
    block_number numeric(20,0) NOT NULL CHECK (block_number >= 0),
    block_hash bytea NOT NULL CHECK (octet_length(block_hash)=32),
    parent_hash bytea NOT NULL CHECK (octet_length(parent_hash)=32),
    canonical boolean NOT NULL,
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_verified_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (chain_id,block_number,block_hash)
);

CREATE FUNCTION protect_receipt_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR ROW(OLD.attempt_id,OLD.intent_id,OLD.attempt_number,OLD.transaction_hash,
        OLD.block_number,OLD.block_hash,OLD.execution_status,OLD.transaction_index,OLD.gas_used,
        OLD.effective_gas_price,OLD.observed_at) IS DISTINCT FROM
        ROW(NEW.attempt_id,NEW.intent_id,NEW.attempt_number,NEW.transaction_hash,
        NEW.block_number,NEW.block_hash,NEW.execution_status,NEW.transaction_index,NEW.gas_used,
        NEW.effective_gas_price,NEW.observed_at) THEN
        RAISE EXCEPTION 'receipt inclusion evidence is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER receipt_identity_immutable BEFORE UPDATE OR DELETE ON observed_receipts
    FOR EACH ROW EXECUTE FUNCTION protect_receipt_identity();

CREATE FUNCTION protect_block_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' OR ROW(OLD.chain_id,OLD.block_number,OLD.block_hash,OLD.parent_hash,OLD.observed_at)
        IS DISTINCT FROM ROW(NEW.chain_id,NEW.block_number,NEW.block_hash,NEW.parent_hash,NEW.observed_at) THEN
        RAISE EXCEPTION 'block evidence identity is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER block_identity_immutable BEFORE UPDATE OR DELETE ON observed_blocks
    FOR EACH ROW EXECUTE FUNCTION protect_block_identity();

COMMIT;
