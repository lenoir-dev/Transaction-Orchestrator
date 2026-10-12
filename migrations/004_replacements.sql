BEGIN;

ALTER TABLE transaction_intents DROP CONSTRAINT broadcast_state_alignment;
ALTER TABLE transaction_intents ADD CONSTRAINT broadcast_state_alignment CHECK (
    state NOT IN ('UNCERTAIN','PENDING') OR
    (active_attempt_number > 0 AND broadcast_first_claimed_at IS NOT NULL)
);
ALTER TABLE broadcast_observations DROP CONSTRAINT broadcast_observations_attempt_number_check;

ALTER TABLE transaction_attempts
    ADD COLUMN replaces_attempt_id uuid REFERENCES transaction_attempts(attempt_id),
    ADD COLUMN replacement_reason text,
    ADD COLUMN bump_bps integer,
    ADD COLUMN eligible_at timestamptz,
    ADD CONSTRAINT replacement_lineage_unique UNIQUE (replaces_attempt_id),
    ADD CONSTRAINT replacement_metadata CHECK (
        (attempt_number=1 AND replaces_attempt_id IS NULL AND replacement_reason IS NULL
         AND bump_bps IS NULL AND eligible_at IS NULL) OR
        (attempt_number>1 AND replaces_attempt_id IS NOT NULL AND replacement_reason IS NOT NULL
         AND bump_bps BETWEEN 1 AND 100000 AND eligible_at IS NOT NULL)
    );

ALTER TABLE transaction_attempts DROP CONSTRAINT transaction_attempts_status_check;
ALTER TABLE transaction_attempts ADD CONSTRAINT transaction_attempts_status_check CHECK (
    status IN ('PREPARING','SIGNED','BROADCAST_CLAIMED','OBSERVED','REJECTED','NOT_BROADCAST')
);

CREATE FUNCTION check_replacement_attempt() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    previous transaction_attempts%ROWTYPE;
    intent transaction_intents%ROWTYPE;
BEGIN
    IF NEW.attempt_number=1 THEN RETURN NEW; END IF;
    SELECT * INTO previous FROM transaction_attempts WHERE attempt_id=NEW.replaces_attempt_id;
    SELECT * INTO intent FROM transaction_intents WHERE intent_id=NEW.intent_id;
    IF previous.attempt_id IS NULL OR previous.intent_id <> NEW.intent_id OR
       previous.attempt_number+1 <> NEW.attempt_number OR previous.raw_tx IS NULL OR
       previous.nonce <> NEW.nonce OR previous.gas_limit <> NEW.gas_limit OR
       previous.policy_snapshot_id <> NEW.policy_snapshot_id OR
       NEW.max_fee_per_gas <= previous.max_fee_per_gas OR
       NEW.max_priority_fee_per_gas <= previous.max_priority_fee_per_gas OR
       NEW.max_fee_per_gas < ceil(previous.max_fee_per_gas*(10000+NEW.bump_bps)/10000) OR
       NEW.max_priority_fee_per_gas < ceil(previous.max_priority_fee_per_gas*(10000+NEW.bump_bps)/10000) OR
       NEW.max_fee_per_gas > intent.max_fee_cap_wei OR
       NEW.max_priority_fee_per_gas > intent.max_tip_cap_wei OR
       NEW.attempt_number > intent.max_replacements+1 THEN
        RAISE EXCEPTION 'invalid replacement lineage, fee bump, or ceiling';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER replacement_attempt_guard BEFORE INSERT ON transaction_attempts
    FOR EACH ROW EXECUTE FUNCTION check_replacement_attempt();

CREATE FUNCTION protect_replacement_metadata() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(OLD.replaces_attempt_id,OLD.replacement_reason,OLD.bump_bps,OLD.eligible_at)
       IS DISTINCT FROM
       ROW(NEW.replaces_attempt_id,NEW.replacement_reason,NEW.bump_bps,NEW.eligible_at) THEN
        RAISE EXCEPTION 'replacement metadata is immutable';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER replacement_metadata_immutable BEFORE UPDATE ON transaction_attempts
    FOR EACH ROW EXECUTE FUNCTION protect_replacement_metadata();

COMMIT;
