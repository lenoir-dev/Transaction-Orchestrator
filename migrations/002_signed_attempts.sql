BEGIN;

ALTER TABLE transaction_attempts
    ADD COLUMN attempt_id uuid NOT NULL DEFAULT gen_random_uuid(),
    ADD COLUMN policy_snapshot_id text;

UPDATE transaction_attempts a SET policy_snapshot_id = i.policy_version
FROM transaction_intents i WHERE i.intent_id = a.intent_id;

ALTER TABLE transaction_attempts
    ALTER COLUMN policy_snapshot_id SET NOT NULL,
    ADD CONSTRAINT attempt_id_unique UNIQUE (attempt_id),
    ADD CONSTRAINT attempt_material_matches_status CHECK (
        (status = 'PREPARING' AND raw_tx IS NULL AND transaction_hash IS NULL AND signed_at IS NULL) OR
        (status <> 'PREPARING' AND raw_tx IS NOT NULL AND transaction_hash IS NOT NULL AND signed_at IS NOT NULL)
    );

-- An attempt references immutable call fields on its intent and one reservation.
CREATE FUNCTION protect_intent_payload() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(OLD.intent_id, OLD.tenant_id, OLD.idempotency_key, OLD.request_digest,
           OLD.policy_version, OLD.chain_id, OLD.signer_address, OLD.signer_id,
           OLD.to_address, OLD.value_wei, OLD.calldata, OLD.gas_limit,
           OLD.confirmation_depth, OLD.max_fee_cap_wei, OLD.max_tip_cap_wei,
           OLD.max_replacements, OLD.created_at)
       IS DISTINCT FROM
       ROW(NEW.intent_id, NEW.tenant_id, NEW.idempotency_key, NEW.request_digest,
           NEW.policy_version, NEW.chain_id, NEW.signer_address, NEW.signer_id,
           NEW.to_address, NEW.value_wei, NEW.calldata, NEW.gas_limit,
           NEW.confirmation_depth, NEW.max_fee_cap_wei, NEW.max_tip_cap_wei,
           NEW.max_replacements, NEW.created_at) THEN
        RAISE EXCEPTION 'intent payload and policy are immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER intent_payload_immutable BEFORE UPDATE ON transaction_intents
    FOR EACH ROW EXECUTE FUNCTION protect_intent_payload();

CREATE FUNCTION protect_attempt_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'attempt deletion is forbidden';
    END IF;
    IF ROW(OLD.attempt_id, OLD.intent_id, OLD.attempt_number, OLD.nonce,
           OLD.gas_limit, OLD.max_fee_per_gas, OLD.max_priority_fee_per_gas,
           OLD.policy_snapshot_id, OLD.created_at)
       IS DISTINCT FROM
       ROW(NEW.attempt_id, NEW.intent_id, NEW.attempt_number, NEW.nonce,
           NEW.gas_limit, NEW.max_fee_per_gas, NEW.max_priority_fee_per_gas,
           NEW.policy_snapshot_id, NEW.created_at) THEN
        RAISE EXCEPTION 'attempt construction fields are immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER attempt_identity_immutable BEFORE UPDATE OR DELETE ON transaction_attempts
    FOR EACH ROW EXECUTE FUNCTION protect_attempt_identity();

CREATE FUNCTION protect_nonce_reservation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'nonce reservation is immutable';
END $$;
CREATE TRIGGER nonce_reservation_immutable BEFORE UPDATE OR DELETE ON nonce_reservations
    FOR EACH ROW EXECUTE FUNCTION protect_nonce_reservation();

CREATE FUNCTION protect_signer_account() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(OLD.chain_id, OLD.signer_address, OLD.signer_id)
       IS DISTINCT FROM ROW(NEW.chain_id, NEW.signer_address, NEW.signer_id) OR
       NEW.next_nonce < OLD.next_nonce THEN
        RAISE EXCEPTION 'signer identity and nonce cursor cannot move backward';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER signer_account_cursor_monotone BEFORE UPDATE ON signer_accounts
    FOR EACH ROW EXECUTE FUNCTION protect_signer_account();

-- Check the committed pair, not the order of statements in the transaction.
CREATE FUNCTION check_active_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    current_state text;
    resume text;
    active_no integer;
    attempt_status text;
BEGIN
    SELECT state, resume_state, active_attempt_number
    INTO current_state, resume, active_no
    FROM transaction_intents WHERE intent_id = NEW.intent_id;
    IF TG_TABLE_NAME = 'transaction_attempts' THEN
        IF NEW.status <> 'PREPARING' AND
           (current_state IN ('CREATED', 'RESERVED') OR
            (current_state = 'BLOCKED' AND resume IN ('CREATED', 'RESERVED')) OR
            active_no IS NULL OR
            (current_state = 'SIGNING' AND active_no = NEW.attempt_number)) THEN
            RAISE EXCEPTION 'signed attempt conflicts with intent state';
        END IF;
    END IF;
    IF current_state IN ('CREATED', 'RESERVED') AND active_no IS NOT NULL THEN
        RAISE EXCEPTION 'unsigned intent cannot have active attempt';
    END IF;
    IF current_state IN ('SIGNING', 'READY') AND active_no IS NULL THEN
        RAISE EXCEPTION 'active attempt required for signing state';
    END IF;
    IF active_no IS NULL THEN
        RETURN NULL;
    END IF;
    SELECT status INTO attempt_status FROM transaction_attempts
    WHERE intent_id = NEW.intent_id AND attempt_number = active_no;
    IF current_state = 'SIGNING' AND attempt_status <> 'PREPARING' THEN
        RAISE EXCEPTION 'SIGNING requires unsigned active attempt';
    END IF;
    IF current_state = 'READY' AND attempt_status <> 'SIGNED' THEN
        RAISE EXCEPTION 'READY requires signed active attempt';
    END IF;
    IF current_state = 'BLOCKED' AND resume = 'SIGNING' AND attempt_status <> 'PREPARING' THEN
        RAISE EXCEPTION 'blocked signing requires unsigned active attempt';
    END IF;
    IF attempt_status = 'SIGNED' AND current_state IN ('CREATED', 'RESERVED', 'SIGNING') THEN
        RAISE EXCEPTION 'signed attempt conflicts with intent state';
    END IF;
    RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER intent_attempt_alignment
    AFTER INSERT OR UPDATE ON transaction_intents DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_active_attempt();
CREATE CONSTRAINT TRIGGER attempt_intent_alignment
    AFTER INSERT OR UPDATE ON transaction_attempts DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_active_attempt();

COMMIT;
