package orchestrator

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
)

var (
	ErrReplacementTooEarly = errors.New("replacement interval has not elapsed")
	ErrReplacementLimit    = errors.New("replacement limit reached")
	ErrReceiptObserved     = errors.New("receipt observed for a known attempt")
	ErrEvidenceUnavailable = errors.New("replacement evidence unavailable")
)

type ReplacementOptions struct {
	Now         time.Time // Explicit clock input; tests and callers own scheduling.
	MinAge      time.Duration
	BumpBps     int
	Signing     SignOptions
	Observation BroadcastPolicy
}

func (o ReplacementOptions) valid() bool {
	return !o.Now.IsZero() && o.MinAge >= 0 && o.BumpBps >= 1 && o.BumpBps <= 100000 &&
		o.Signing.SignerTimeout > 0 && o.Signing.LeaseDuration > o.Signing.SignerTimeout+time.Second &&
		o.Observation.RPCDeadline > 0
}

type AttemptRecord struct {
	AttemptIdentity
	ReplacesAttemptID string
	Status            string
	MaxFeePerGasWei   string
	MaxTipPerGasWei   string
	BumpBps           int
}

// AttemptHistory omits raw bytes. The lineage is explicit, not inferred from
// ordering, and a winner is assigned only after receipt evidence is stored.
func (s *Store) AttemptHistory(ctx context.Context, id string) ([]AttemptRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT attempt_id::text,attempt_number,
		COALESCE(replaces_attempt_id::text,''),status,max_fee_per_gas::text,
		max_priority_fee_per_gas::text,COALESCE(bump_bps,0),transaction_hash
		FROM transaction_attempts WHERE intent_id=$1::uuid ORDER BY attempt_number`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var history []AttemptRecord
	for rows.Next() {
		var a AttemptRecord
		var hash []byte
		if err = rows.Scan(&a.AttemptID, &a.AttemptNumber, &a.ReplacesAttemptID, &a.Status,
			&a.MaxFeePerGasWei, &a.MaxTipPerGasWei, &a.BumpBps, &hash); err != nil {
			return nil, err
		}
		a.IntentID = id
		if len(hash) == common.HashLength {
			a.Hash = common.BytesToHash(hash)
		}
		history = append(history, a)
	}
	return history, rows.Err()
}

func (s *Store) SignReplacement(ctx context.Context, id string, signer TransactionSigner,
	endpoints []BroadcastEndpoint, opts ReplacementOptions) (AttemptIdentity, error) {
	ctx, span := otel.Tracer("transaction-orchestrator").Start(ctx, "replacement.sign")
	defer span.End()
	return s.signReplacement(ctx, id, signer, endpoints, opts, signingHooks{})
}

func (s *Store) signReplacement(ctx context.Context, id string, signer TransactionSigner,
	endpoints []BroadcastEndpoint, opts ReplacementOptions, hooks signingHooks) (AttemptIdentity, error) {
	if !opts.valid() || len(endpoints) == 0 || len(endpoints) > 8 {
		return AttemptIdentity{}, ErrInvalid
	}
	state, active, err := s.replacementPosition(ctx, id)
	if err != nil {
		return AttemptIdentity{}, err
	}
	if state == Ready && active > 1 {
		stored, loadErr := s.LoadSignedAttemptNumber(ctx, id, active)
		if loadErr != nil {
			return AttemptIdentity{}, loadErr
		}
		return stored.AttemptIdentity, nil
	}
	if signer == nil {
		return AttemptIdentity{}, ErrInvalid
	}
	var expected []byte
	if err := s.db.QueryRow(ctx, `SELECT signer_address FROM transaction_intents WHERE intent_id=$1::uuid`, id).
		Scan(&expected); err != nil {
		return AttemptIdentity{}, err
	}
	if len(expected) != common.AddressLength {
		return AttemptIdentity{}, ErrInvalid
	}
	addressCtx, cancelAddress := context.WithTimeout(ctx, opts.Signing.SignerTimeout)
	address, err := signer.Address(addressCtx)
	cancelAddress()
	if err != nil {
		return AttemptIdentity{}, classifySignerFailure(ctx, err)
	}
	if !bytes.Equal(expected, address.Bytes()) {
		return AttemptIdentity{}, &SigningError{Code: "SIGNER_ADDRESS_MISMATCH"}
	}
	if state != Signing || active <= 1 {
		winner, err := s.ObserveWinner(ctx, id, endpoints, opts.Observation)
		if err != nil {
			return AttemptIdentity{}, err
		}
		if winner > 0 {
			return AttemptIdentity{}, ErrReceiptObserved
		}
	}
	if hooks.beforeClaim != nil {
		hooks.beforeClaim()
	}
	claim, err := s.claimReplacement(ctx, id, address, opts)
	if err != nil {
		if errors.Is(err, ErrTransition) {
			latestState, latestNumber, readErr := s.replacementPosition(ctx, id)
			if readErr == nil && latestState == Ready && latestNumber > 1 {
				stored, loadErr := s.LoadSignedAttemptNumber(ctx, id, latestNumber)
				if loadErr == nil {
					return stored.AttemptIdentity, nil
				}
			}
		}
		return AttemptIdentity{}, err
	}
	signCtx, cancelSign := context.WithTimeout(ctx, opts.Signing.SignerTimeout)
	defer cancelSign()
	return s.signClaim(signCtx, id, signer, claim, hooks)
}

func (s *Store) replacementPosition(ctx context.Context, id string) (State, int, error) {
	var state State
	var active sql.NullInt64
	err := s.db.QueryRow(ctx, `SELECT state,active_attempt_number FROM transaction_intents
		WHERE intent_id=$1::uuid`, id).Scan(&state, &active)
	if err != nil {
		return "", 0, err
	}
	return state, int(active.Int64), nil
}

// ObserveWinner checks every signed hash before escalation or a replacement
// send. A null lookup from one endpoint is never treated as absence.
func (s *Store) ObserveWinner(ctx context.Context, id string, endpoints []BroadcastEndpoint,
	policy BroadcastPolicy) (int, error) {
	if policy.RPCDeadline <= 0 || len(endpoints) == 0 || len(endpoints) > 8 {
		return 0, ErrInvalid
	}
	history, err := s.AttemptHistory(ctx, id)
	if err != nil {
		return 0, err
	}
	var signed []SignedAttempt
	for _, attempt := range history {
		if attempt.Hash == (common.Hash{}) {
			continue
		}
		stored, loadErr := s.LoadSignedAttemptNumber(ctx, id, attempt.AttemptNumber)
		if loadErr != nil {
			return 0, loadErr
		}
		signed = append(signed, stored)
	}
	if len(signed) == 0 {
		return 0, ErrTransition
	}
	var first types.Transaction
	if err = first.UnmarshalBinary(signed[0].Raw); err != nil {
		return 0, err
	}
	var receiptReadOK, transactionReadOK bool
	var eligible []BroadcastEndpoint
	for _, endpoint := range endpoints {
		if endpoint == nil || !validAlias(endpoint.Alias()) {
			return 0, ErrInvalid
		}
		readCtx, cancel := context.WithTimeout(ctx, policy.RPCDeadline)
		chain, chainErr := endpoint.ChainID(readCtx)
		if chainErr == nil && policy.GenesisHash != (common.Hash{}) {
			var genesis common.Hash
			genesis, chainErr = endpoint.Genesis(readCtx)
			if chainErr == nil && genesis != policy.GenesisHash {
				chainErr = ErrNoEligibleEndpoint
			}
		}
		cancel()
		if chainErr != nil || chain == nil || chain.Cmp(first.ChainId()) != 0 {
			continue
		}
		eligible = append(eligible, endpoint)
		for _, attempt := range signed {
			readCtx, cancel = context.WithTimeout(ctx, policy.RPCDeadline)
			receipt, receiptErr := endpoint.ReceiptByHash(readCtx, attempt.Hash)
			cancel()
			if receiptErr == nil {
				receiptReadOK = true
				if receipt != nil {
					if receipt.Hash != attempt.Hash || receipt.BlockHash == (common.Hash{}) {
						return 0, ErrEvidenceUnavailable
					}
					verifyCtx, stopVerify := context.WithTimeout(ctx, policy.RPCDeadline)
					block, blockErr := endpoint.BlockByHash(verifyCtx, receipt.BlockHash)
					canonical, canonicalErr := endpoint.BlockByNumber(verifyCtx, receipt.BlockNumber)
					stopVerify()
					if blockErr != nil || canonicalErr != nil || block == nil || canonical == nil ||
						block.Hash != receipt.BlockHash || block.Number != receipt.BlockNumber ||
						canonical.Hash != block.Hash || receipt.TransactionIndex >= uint64(len(block.Transactions)) ||
						block.Transactions[receipt.TransactionIndex] != attempt.Hash {
						return 0, ErrEvidenceUnavailable
					}
					return attempt.AttemptNumber, s.recordWinner(ctx, id, attempt.AttemptNumber, endpoint.Alias(), *receipt, *block)
				}
			}
			readCtx, cancel = context.WithTimeout(ctx, policy.RPCDeadline)
			seen, lookupErr := endpoint.TransactionByHash(readCtx, attempt.Hash)
			cancel()
			if lookupErr == nil {
				transactionReadOK = true
				if seen != nil && (seen.Hash() != attempt.Hash || !bytes.Equal(mustRaw(seen), attempt.Raw)) {
					return 0, ErrEvidenceUnavailable
				}
			}
		}
	}
	if !receiptReadOK || !transactionReadOK {
		return 0, ErrEvidenceUnavailable
	}
	sender, err := types.Sender(types.NewLondonSigner(first.ChainId()), &first)
	if err != nil {
		return 0, err
	}
	for _, endpoint := range eligible {
		readCtx, cancel := context.WithTimeout(ctx, policy.RPCDeadline)
		latest, nonceErr := endpoint.LatestNonce(readCtx, sender)
		cancel()
		if nonceErr == nil && latest > first.Nonce() {
			if err := s.blockReplacement(ctx, id, "EXTERNAL_NONCE_CONFLICT"); err != nil {
				return 0, err
			}
			return 0, ErrEvidenceUnavailable
		}
	}
	return 0, nil
}

func (s *Store) recordWinner(ctx context.Context, id string, number int, endpoint string, receipt ReceiptEvidence, block BlockEvidence) error {
	var chain string
	var address []byte
	if err := s.db.QueryRow(ctx, `SELECT chain_id::text,signer_address FROM transaction_intents
		WHERE intent_id=$1::uuid`, id).Scan(&chain, &address); err != nil {
		return err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT 1 FROM signer_accounts WHERE chain_id=$1::numeric
		AND signer_address=$2 FOR UPDATE`, chain, address); err != nil {
		return err
	}
	var state State
	var active int
	var winner sql.NullInt64
	var sendCount int
	if err = tx.QueryRow(ctx, `SELECT state,active_attempt_number,winning_attempt_number,broadcast_send_count
		FROM transaction_intents WHERE intent_id=$1::uuid FOR UPDATE`, id).Scan(&state, &active, &winner, &sendCount); err != nil {
		return err
	}
	if winner.Valid {
		if int(winner.Int64) != number {
			return ErrEvidenceUnavailable
		}
		return tx.Commit(ctx)
	}
	var hash []byte
	if err = tx.QueryRow(ctx, `SELECT transaction_hash FROM transaction_attempts
		WHERE intent_id=$1::uuid AND attempt_number=$2`, id, number).Scan(&hash); err != nil {
		return err
	}
	if !bytes.Equal(hash, receipt.Hash.Bytes()) {
		return ErrEvidenceUnavailable
	}
	var price any
	if receipt.EffectiveGasPrice != "" {
		price = receipt.EffectiveGasPrice
	}
	_, err = tx.Exec(ctx, `INSERT INTO observed_receipts
        (intent_id,attempt_id,attempt_number,transaction_hash,block_number,block_hash,execution_status,
        canonical_observed,verification_status,transaction_index,gas_used,effective_gas_price,
        confirmations,verified_head,last_verified_at)
        SELECT $1::uuid,attempt_id,$2,$3,$4::numeric,$5,$6,true,'CANONICAL',$7::numeric,$8::numeric,
        $9::numeric,1,$4::numeric,clock_timestamp() FROM transaction_attempts
        WHERE intent_id=$1::uuid AND attempt_number=$2`, id, number, hash,
		fmt.Sprint(receipt.BlockNumber), receipt.BlockHash.Bytes(), int(receipt.Status),
		receipt.TransactionIndex, receipt.GasUsed, price)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state='MINED',winning_attempt_number=$2,confirmation_count=1,
 receipt_verified_head=$3::numeric,
		resume_state=NULL,reason_code=NULL,lease_token=NULL,lease_expires_at=NULL,
		next_reconcile_at=NULL,version=version+1,updated_at=now() WHERE intent_id=$1::uuid`, id, number, fmt.Sprint(receipt.BlockNumber))
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO observed_blocks(chain_id,block_number,block_hash,parent_hash,canonical)
        VALUES($1::numeric,$2::numeric,$3,$4,true) ON CONFLICT(chain_id,block_number,block_hash)
        DO UPDATE SET canonical=true,last_verified_at=clock_timestamp()`, chain, block.Number, block.Hash.Bytes(), block.ParentHash.Bytes())
	if err != nil {
		return err
	}
	if active != number && sendCount == 0 {
		_, err = tx.Exec(ctx, `UPDATE transaction_attempts SET status='NOT_BROADCAST'
			WHERE intent_id=$1::uuid AND attempt_number=$2 AND status IN ('SIGNED','BROADCAST_CLAIMED')`, id, active)
		if err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type,evidence)
		VALUES($1::uuid,'RECEIPT_OBSERVED',jsonb_build_object('attempt_number',$2::integer,
		'endpoint',$3::text,'block_hash',$4::text))`, id, number, endpoint, receipt.BlockHash.Hex()); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE signer_accounts SET paused_reason=NULL,updated_at=now()
		WHERE chain_id=$1::numeric AND signer_address=$2 AND paused_reason='INTENT_BLOCKED'
		AND NOT EXISTS(SELECT 1 FROM transaction_intents i JOIN nonce_reservations r USING(intent_id)
		WHERE r.chain_id=$1::numeric AND r.signer_address=$2 AND i.state='BLOCKED')`, chain, address); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) blockReplacement(ctx context.Context, id, reason string) error {
	var chain string
	var address []byte
	if err := s.db.QueryRow(ctx, `SELECT chain_id::text,signer_address FROM transaction_intents
		WHERE intent_id=$1::uuid`, id).Scan(&chain, &address); err != nil {
		return err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT 1 FROM signer_accounts WHERE chain_id=$1::numeric
		AND signer_address=$2 FOR UPDATE`, chain, address); err != nil {
		return err
	}
	var state State
	if err = tx.QueryRow(ctx, `SELECT state FROM transaction_intents WHERE intent_id=$1::uuid FOR UPDATE`, id).Scan(&state); err != nil {
		return err
	}
	if state == Mined {
		return ErrReceiptObserved
	}
	if state != Blocked {
		_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state='BLOCKED',resume_state=$2,
			reason_code=$3,lease_token=NULL,lease_expires_at=NULL,version=version+1,
			updated_at=now() WHERE intent_id=$1::uuid`, id, state, reason)
	} else {
		_, err = tx.Exec(ctx, `UPDATE transaction_intents SET reason_code=$2,version=version+1,
			updated_at=now() WHERE intent_id=$1::uuid`, id, reason)
	}
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE signer_accounts SET paused_reason='INTENT_BLOCKED',
		updated_at=now() WHERE chain_id=$1::numeric AND signer_address=$2
		AND (paused_reason IS NULL OR paused_reason='INTENT_BLOCKED')`, chain, address); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type,reason_code)
		VALUES($1::uuid,'REPLACEMENT_BLOCKED',$2)`, id, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) claimReplacement(ctx context.Context, id string, signer common.Address,
	opts ReplacementOptions) (signingClaim, error) {
	var chain string
	var address []byte
	if err := s.db.QueryRow(ctx, `SELECT chain_id::text,signer_address FROM transaction_intents
		WHERE intent_id=$1::uuid`, id).Scan(&chain, &address); err != nil {
		return signingClaim{}, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return signingClaim{}, err
	}
	defer tx.Rollback(ctx)
	var paused sql.NullString
	if err = tx.QueryRow(ctx, `SELECT paused_reason FROM signer_accounts WHERE chain_id=$1::numeric
		AND signer_address=$2 FOR UPDATE`, chain, address).Scan(&paused); err != nil {
		return signingClaim{}, err
	}
	var state State
	var reason sql.NullString
	var active, limit int
	var winner sql.NullInt64
	var leaseActive bool
	var first sql.NullTime
	var nonce, value, capFee, capTip, policy string
	var signerBytes, toBytes, data []byte
	var gas int64
	err = tx.QueryRow(ctx, `SELECT i.state,i.reason_code,i.active_attempt_number,i.max_replacements,
		i.winning_attempt_number,COALESCE(i.lease_expires_at>clock_timestamp(),false),
		CASE WHEN i.state='PENDING' THEN (SELECT min(b.observed_at) FROM broadcast_observations b
			WHERE b.intent_id=i.intent_id AND b.attempt_number=i.active_attempt_number
			AND b.classification IN ('SUCCESS','FOUND_TRANSACTION','FOUND_RECEIPT'))
		ELSE i.broadcast_first_claimed_at END,
		r.nonce::text,i.value_wei::text,i.max_fee_cap_wei::text,
		i.max_tip_cap_wei::text,i.policy_version,i.signer_address,i.to_address,i.calldata,i.gas_limit
		FROM transaction_intents i JOIN nonce_reservations r USING(intent_id)
		WHERE i.intent_id=$1::uuid FOR UPDATE OF i`, id).Scan(&state, &reason, &active, &limit,
		&winner, &leaseActive, &first, &nonce, &value, &capFee, &capTip, &policy,
		&signerBytes, &toBytes, &data, &gas)
	if err != nil {
		return signingClaim{}, err
	}
	if winner.Valid || !bytes.Equal(signerBytes, signer.Bytes()) {
		return signingClaim{}, ErrTransition
	}
	if paused.Valid && !(state == Blocked && reason.Valid && reason.String == "FEE_REJECTION" &&
		paused.String == "INTENT_BLOCKED") {
		return signingClaim{}, ErrAccountPaused
	}
	if state == Ready && active > 1 {
		return signingClaim{}, ErrTransition
	}
	if state == Signing && active > 1 {
		if leaseActive {
			return signingClaim{}, &SigningError{Code: "SIGNING_IN_PROGRESS", Retryable: true}
		}
	} else if state != Pending && !(state == Blocked && reason.Valid && reason.String == "FEE_REJECTION") {
		return signingClaim{}, ErrTransition
	}
	var attemptID, fee, tip, priorID, previousFee, previousTip string
	replacementReason := "PENDING_AGE"
	number := active + 1
	if state == Signing {
		number = active
		err = tx.QueryRow(ctx, `SELECT attempt_id::text,max_fee_per_gas::text,
			max_priority_fee_per_gas::text,replaces_attempt_id::text,replacement_reason FROM transaction_attempts
			WHERE intent_id=$1::uuid AND attempt_number=$2 AND status='PREPARING'`, id, number).
			Scan(&attemptID, &fee, &tip, &priorID, &replacementReason)
		if err != nil {
			return signingClaim{}, err
		}
		err = tx.QueryRow(ctx, `SELECT max_fee_per_gas::text,max_priority_fee_per_gas::text
			FROM transaction_attempts WHERE attempt_id=$1::uuid`, priorID).
			Scan(&previousFee, &previousTip)
		if err != nil {
			return signingClaim{}, err
		}
	} else {
		if !first.Valid || opts.Now.Before(first.Time.Add(opts.MinAge)) {
			return signingClaim{}, ErrReplacementTooEarly
		}
		if active > limit {
			if err = s.blockReplacementLocked(ctx, tx, id, chain, address, "REPLACEMENT_LIMIT_REACHED", state); err != nil {
				return signingClaim{}, err
			}
			return signingClaim{}, ErrReplacementLimit
		}
		var status string
		err = tx.QueryRow(ctx, `SELECT attempt_id::text,max_fee_per_gas::text,
			max_priority_fee_per_gas::text,status FROM transaction_attempts
			WHERE intent_id=$1::uuid AND attempt_number=$2`, id, active).
			Scan(&priorID, &previousFee, &previousTip, &status)
		if err != nil || (status != "OBSERVED" && status != "BROADCAST_CLAIMED" && status != "REJECTED") {
			return signingClaim{}, ErrTransition
		}
		bumped, bumpErr := BumpFees(FeeInput{previousFee, previousTip}, FeeInput{capFee, capTip}, opts.BumpBps)
		if errors.Is(bumpErr, ErrFeeCeiling) {
			if err = s.blockReplacementLocked(ctx, tx, id, chain, address, "FEE_CEILING_REACHED", state); err != nil {
				return signingClaim{}, err
			}
			return signingClaim{}, ErrFeeCeiling
		}
		if bumpErr != nil {
			return signingClaim{}, bumpErr
		}
		fee, tip = bumped.MaxFeePerGasWei, bumped.MaxTipPerGasWei
		if state == Blocked {
			replacementReason = "FEE_REJECTION_RETRY"
		}
		attemptID, err = newID()
		if err != nil {
			return signingClaim{}, err
		}
		_, err = tx.Exec(ctx, `INSERT INTO transaction_attempts
			(attempt_id,intent_id,attempt_number,nonce,status,gas_limit,max_fee_per_gas,
			 max_priority_fee_per_gas,policy_snapshot_id,replaces_attempt_id,replacement_reason,
			 bump_bps,eligible_at)
			VALUES($1::uuid,$2::uuid,$3,$4::numeric,'PREPARING',$5,$6::numeric,$7::numeric,
			$8,$9::uuid,$10,$11,$12)`, attemptID, id, number, nonce, gas, fee, tip,
			policy, priorID, replacementReason, opts.BumpBps, opts.Now)
		if err != nil {
			return signingClaim{}, err
		}
	}
	token, err := newID()
	if err != nil {
		return signingClaim{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state='SIGNING',resume_state=NULL,
		active_attempt_number=$2,broadcast_first_claimed_at=NULL,broadcast_send_count=0,
		next_reconcile_at=NULL,lease_token=$3::uuid,lease_expires_at=clock_timestamp()+$4::interval,
		reason_code=NULL,version=version+1,updated_at=now() WHERE intent_id=$1::uuid`,
		id, number, token, interval(opts.Signing.LeaseDuration))
	if err != nil {
		return signingClaim{}, err
	}
	if state == Blocked {
		if _, err = tx.Exec(ctx, `UPDATE signer_accounts SET paused_reason=NULL,updated_at=now()
			WHERE chain_id=$1::numeric AND signer_address=$2 AND paused_reason='INTENT_BLOCKED'
			AND NOT EXISTS(SELECT 1 FROM transaction_intents i JOIN nonce_reservations r USING(intent_id)
			WHERE r.chain_id=$1::numeric AND r.signer_address=$2 AND i.state='BLOCKED')`, chain, address); err != nil {
			return signingClaim{}, err
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type,evidence)
		VALUES($1::uuid,'REPLACEMENT_CLAIMED',jsonb_build_object('attempt_number',$2::integer,
		'bump_bps',$3::integer,'max_fee_per_gas',$4::text,'max_tip_per_gas',$5::text,
		'replaces_attempt_id',$6::text,'previous_max_fee_per_gas',$7::text,
		'previous_max_tip_per_gas',$8::text,'reason',$9::text))`,
		id, number, opts.BumpBps, fee, tip, priorID, previousFee, previousTip, replacementReason); err != nil {
		return signingClaim{}, err
	}
	candidate, err := candidateFromValues(chain, nonce, signerBytes, toBytes, value, data, gas, fee, tip, policy)
	if err != nil {
		return signingClaim{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return signingClaim{}, err
	}
	return signingClaim{attemptID: attemptID, number: number, token: token, candidate: candidate}, nil
}

func (s *Store) blockReplacementLocked(ctx context.Context, tx pgx.Tx, id, chain string,
	address []byte, reason string, previous State) error {
	var resume any = previous
	if previous == Blocked {
		resume = Uncertain
	}
	_, err := tx.Exec(ctx, `UPDATE transaction_intents SET state='BLOCKED',resume_state=$2,
		reason_code=$3,version=version+1,updated_at=now() WHERE intent_id=$1::uuid`, id, resume, reason)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE signer_accounts SET paused_reason='INTENT_BLOCKED',updated_at=now()
		WHERE chain_id=$1::numeric AND signer_address=$2
		AND (paused_reason IS NULL OR paused_reason='INTENT_BLOCKED')`, chain, address); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type,reason_code)
		VALUES($1::uuid,'REPLACEMENT_BLOCKED',$2)`, id, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

type ReplacementMetric struct {
	Name   string
	Reason string
	Count  int64
}

func (s *Store) ReplacementMetrics(ctx context.Context) ([]ReplacementMetric, error) {
	rows, err := s.db.Query(ctx, `SELECT name,reason,count(*) FROM (
		SELECT 'txorch_replacements_total' AS name,replacement_reason AS reason
		FROM transaction_attempts WHERE replaces_attempt_id IS NOT NULL
		UNION ALL SELECT 'txorch_replacement_blocked_total',reason_code
		FROM audit_events WHERE event_type='REPLACEMENT_BLOCKED'
	) events GROUP BY name,reason ORDER BY name,reason`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var metrics []ReplacementMetric
	for rows.Next() {
		var m ReplacementMetric
		if err = rows.Scan(&m.Name, &m.Reason, &m.Count); err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}
	return metrics, rows.Err()
}
