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

type FeeInput struct {
	MaxFeePerGasWei string
	MaxTipPerGasWei string
}

type SignOptions struct {
	SignerTimeout time.Duration
	LeaseDuration time.Duration
}

type SigningError struct {
	Code      string
	Retryable bool
}

// Error deliberately excludes signer errors, transaction bytes, and key data.
func (e *SigningError) Error() string { return e.Code }

type AttemptIdentity struct {
	AttemptID     string
	IntentID      string
	AttemptNumber int
	Hash          common.Hash
}

// SignedAttempt is for the internal storage/broadcast boundary, not the HTTP API.
type SignedAttempt struct {
	AttemptIdentity
	Raw []byte
}

type signingClaim struct {
	attemptID string
	number    int
	token     string
	candidate Candidate
	ready     bool
}

type signingHooks struct {
	beforeClaim func()
	afterClaim  func()
	afterSign   func()
	afterCommit func()
}

// SignInitial creates only attempt number one. A repeated call on READY loads
// persisted bytes and never invokes the signer again.
func (s *Store) SignInitial(ctx context.Context, intentID string, signer TransactionSigner,
	fee FeeInput, opts SignOptions) (AttemptIdentity, error) {
	return s.signInitial(ctx, intentID, signer, fee, opts, signingHooks{})
}

func (s *Store) signInitial(ctx context.Context, intentID string, signer TransactionSigner,
	fee FeeInput, opts SignOptions, hooks signingHooks) (AttemptIdentity, error) {
	if existing, err := s.GetIntent(ctx, intentID); err == nil && existing.State == Ready {
		stored, err := s.LoadSignedAttemptNumber(ctx, intentID, 1)
		if err != nil {
			return AttemptIdentity{}, err
		}
		return stored.AttemptIdentity, nil
	} else if err != nil {
		return AttemptIdentity{}, err
	}
	if signer == nil || opts.SignerTimeout <= 0 || opts.LeaseDuration <= opts.SignerTimeout+time.Second {
		return AttemptIdentity{}, ErrInvalid
	}
	var expectedBytes []byte
	err := s.db.QueryRow(ctx, `SELECT i.signer_address FROM transaction_intents i
		JOIN nonce_reservations r USING (intent_id) WHERE i.intent_id=$1::uuid`, intentID).Scan(&expectedBytes)
	if errors.Is(err, pgx.ErrNoRows) {
		return AttemptIdentity{}, ErrTransition
	}
	if err != nil {
		return AttemptIdentity{}, err
	}
	if len(expectedBytes) != common.AddressLength {
		return AttemptIdentity{}, &SigningError{Code: "SIGNER_ACCOUNT_INVALID"}
	}
	signCtx, cancel := context.WithTimeout(ctx, opts.SignerTimeout)
	defer cancel()
	address, err := signer.Address(signCtx)
	if err != nil {
		return AttemptIdentity{}, classifySignerFailure(signCtx, err)
	}
	if address != common.BytesToAddress(expectedBytes) {
		// The reserved nonce must not be bypassed by later allocations.
		if blockErr := s.Block(ctx, intentID, "SIGNER_ADDRESS_MISMATCH"); blockErr != nil &&
			!errors.Is(blockErr, ErrTransition) {
			return AttemptIdentity{}, &SigningError{Code: "SIGNING_PERSISTENCE_FAILED", Retryable: true}
		}
		return AttemptIdentity{}, &SigningError{Code: "SIGNER_ADDRESS_MISMATCH"}
	}
	var claim signingClaim
	for {
		claim, err = s.claimInitial(signCtx, intentID, address, fee, opts.LeaseDuration)
		var inProgress *SigningError
		if errors.As(err, &inProgress) && inProgress.Code == "SIGNING_IN_PROGRESS" {
			select {
			case <-signCtx.Done():
				return AttemptIdentity{}, inProgress
			case <-time.After(15 * time.Millisecond):
				continue
			}
		}
		if err != nil {
			return AttemptIdentity{}, err
		}
		break
	}
	if claim.ready {
		stored, err := s.LoadSignedAttemptNumber(ctx, intentID, 1)
		if err != nil {
			return AttemptIdentity{}, err
		}
		return stored.AttemptIdentity, nil
	}
	return s.signClaim(signCtx, intentID, signer, claim, hooks)
}

// signClaim is shared by initial and replacement attempts. It validates the
// same immutable candidate and recovered sender before any signed-byte write.
func (s *Store) signClaim(signCtx context.Context, intentID string, signer TransactionSigner,
	claim signingClaim, hooks signingHooks) (AttemptIdentity, error) {
	if hooks.afterClaim != nil {
		hooks.afterClaim()
	}
	unsigned, err := BuildType2(claim.candidate)
	if err != nil {
		s.blockSigningClaim(intentID, claim.token, "CONSTRUCTION_INVALID")
		return AttemptIdentity{}, err
	}
	signCtx, signSpan := otel.Tracer("transaction-orchestrator").Start(signCtx, "signer.call")
	signed, err := signer.SignTransaction(signCtx, unsigned)
	signSpan.End()
	if err != nil {
		classified := classifySignerFailure(signCtx, err)
		s.releaseSigningClaim(intentID, claim.token, classified.Code)
		return AttemptIdentity{}, classified
	}
	if err := ValidateSigned(claim.candidate, signed); err != nil {
		code := "SIGNER_INVALID_SIGNATURE"
		if errors.Is(err, ErrSignedMismatch) {
			code = "SIGNER_MUTATED_TRANSACTION"
		}
		s.blockSigningClaim(intentID, claim.token, code)
		return AttemptIdentity{}, &SigningError{Code: code}
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		s.blockSigningClaim(intentID, claim.token, "SIGNER_MALFORMED_OUTPUT")
		return AttemptIdentity{}, &SigningError{Code: "SIGNER_MALFORMED_OUTPUT"}
	}
	hash := signed.Hash()
	var decoded types.Transaction
	if err = decoded.UnmarshalBinary(raw); err != nil || decoded.Hash() != hash ||
		ValidateSigned(claim.candidate, &decoded) != nil {
		s.blockSigningClaim(intentID, claim.token, "SIGNER_MALFORMED_OUTPUT")
		return AttemptIdentity{}, &SigningError{Code: "SIGNER_MALFORMED_OUTPUT"}
	}
	if hooks.afterSign != nil {
		hooks.afterSign()
	}
	// Persist a valid result even if the request deadline expired just after
	// signing. Failure leaves the unsigned claim recoverable after lease expiry.
	persistCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err = s.persistSignedAttempt(persistCtx, intentID, claim.number, claim.token, claim.candidate, raw, hash); err != nil {
		return AttemptIdentity{}, err
	}
	if hooks.afterCommit != nil {
		hooks.afterCommit()
	}
	return AttemptIdentity{AttemptID: claim.attemptID, IntentID: intentID, AttemptNumber: claim.number, Hash: hash}, nil
}

func classifySignerFailure(ctx context.Context, err error) *SigningError {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return &SigningError{Code: "SIGNER_TIMEOUT", Retryable: true}
	}
	return &SigningError{Code: "SIGNER_UNAVAILABLE", Retryable: true}
}

func (s *Store) claimInitial(ctx context.Context, id string, signer common.Address,
	fee FeeInput, lease time.Duration) (signingClaim, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return signingClaim{}, err
	}
	defer tx.Rollback(ctx)
	var state State
	var chain, nonce, value, maxFee, maxTip, policy string
	var signerBytes, toBytes, data []byte
	var gas int64
	var active sql.NullInt64
	var leaseActive bool
	err = tx.QueryRow(ctx, `SELECT i.state,i.chain_id::text,r.nonce::text,i.signer_address,
		i.to_address,i.value_wei::text,i.calldata,i.gas_limit,i.max_fee_cap_wei::text,
		i.max_tip_cap_wei::text,i.policy_version,i.active_attempt_number,
		COALESCE(i.lease_expires_at > clock_timestamp(),false)
		FROM transaction_intents i JOIN nonce_reservations r USING (intent_id)
		WHERE i.intent_id=$1::uuid FOR UPDATE OF i`, id).Scan(&state, &chain, &nonce,
		&signerBytes, &toBytes, &value, &data, &gas, &maxFee, &maxTip, &policy, &active, &leaseActive)
	if errors.Is(err, pgx.ErrNoRows) {
		return signingClaim{}, ErrTransition
	}
	if err != nil {
		return signingClaim{}, err
	}
	if !bytes.Equal(signerBytes, signer.Bytes()) {
		return signingClaim{}, &SigningError{Code: "SIGNER_ADDRESS_MISMATCH"}
	}
	if state == Ready {
		if err = tx.Commit(ctx); err != nil {
			return signingClaim{}, err
		}
		return signingClaim{ready: true}, nil
	}
	if state != Reserved && state != Signing {
		return signingClaim{}, ErrTransition
	}
	if state == Signing && leaseActive {
		return signingClaim{}, &SigningError{Code: "SIGNING_IN_PROGRESS", Retryable: true}
	}
	var attemptID, chosenFee, chosenTip string
	if state == Reserved {
		feeValue, err := decimalUint256(fee.MaxFeePerGasWei, true)
		if err != nil {
			return signingClaim{}, err
		}
		tipValue, err := decimalUint256(fee.MaxTipPerGasWei, true)
		if err != nil || tipValue.Cmp(feeValue) > 0 {
			return signingClaim{}, ErrTransactionFields
		}
		feeCeiling, err := decimalUint256(maxFee, true)
		if err != nil {
			return signingClaim{}, err
		}
		tipCeiling, err := decimalUint256(maxTip, true)
		if err != nil || feeValue.Cmp(feeCeiling) > 0 || tipValue.Cmp(tipCeiling) > 0 {
			return signingClaim{}, ErrTransactionFields
		}
		attemptID, err = newID()
		if err != nil {
			return signingClaim{}, err
		}
		chosenFee, chosenTip = feeValue.String(), tipValue.String()
		_, err = tx.Exec(ctx, `INSERT INTO transaction_attempts
			(attempt_id,intent_id,attempt_number,nonce,status,gas_limit,max_fee_per_gas,
			 max_priority_fee_per_gas,policy_snapshot_id)
			VALUES ($1::uuid,$2::uuid,1,$3::numeric,'PREPARING',$4,$5::numeric,$6::numeric,$7)`,
			attemptID, id, nonce, gas, chosenFee, chosenTip, policy)
		if err != nil {
			return signingClaim{}, err
		}
	} else {
		if !active.Valid || active.Int64 != 1 {
			return signingClaim{}, ErrTransition
		}
		var status string
		err = tx.QueryRow(ctx, `SELECT attempt_id::text,max_fee_per_gas::text,
			max_priority_fee_per_gas::text,status FROM transaction_attempts
			WHERE intent_id=$1::uuid AND attempt_number=1`, id).Scan(&attemptID, &chosenFee, &chosenTip, &status)
		if err != nil || status != "PREPARING" {
			return signingClaim{}, ErrTransition
		}
	}
	token, err := newID()
	if err != nil {
		return signingClaim{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state='SIGNING',active_attempt_number=1,
		lease_token=$2::uuid,lease_expires_at=clock_timestamp()+$3::interval,
		reason_code=NULL,version=version+1,updated_at=now() WHERE intent_id=$1::uuid`,
		id, token, fmt.Sprintf("%d milliseconds", lease.Milliseconds()))
	if err != nil {
		return signingClaim{}, err
	}
	event := "SIGNING_CLAIMED"
	if state == Signing {
		event = "SIGNING_RECLAIMED"
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events (intent_id,event_type)
		VALUES ($1::uuid,$2)`, id, event); err != nil {
		return signingClaim{}, err
	}
	candidate, err := candidateFromValues(chain, nonce, signerBytes, toBytes, value, data,
		gas, chosenFee, chosenTip, policy)
	if err != nil {
		return signingClaim{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return signingClaim{}, err
	}
	return signingClaim{attemptID: attemptID, number: 1, token: token, candidate: candidate}, nil
}

func candidateFromValues(chain, nonce string, signerBytes, toBytes []byte, value string,
	data []byte, gas int64, fee, tip, policy string) (Candidate, error) {
	chainID, err := decimalUint256(chain, true)
	if err != nil {
		return Candidate{}, err
	}
	parsedNonce, err := parseNonce(nonce)
	if err != nil {
		return Candidate{}, err
	}
	amount, err := decimalUint256(value, false)
	if err != nil {
		return Candidate{}, err
	}
	feeCap, err := decimalUint256(fee, true)
	if err != nil {
		return Candidate{}, err
	}
	tipCap, err := decimalUint256(tip, true)
	if err != nil {
		return Candidate{}, err
	}
	if len(signerBytes) != common.AddressLength || len(toBytes) != common.AddressLength || gas <= 0 {
		return Candidate{}, ErrTransactionFields
	}
	c := Candidate{ChainID: chainID, Signer: common.BytesToAddress(signerBytes), Nonce: parsedNonce,
		To: common.BytesToAddress(toBytes), Value: amount, Calldata: bytes.Clone(data),
		GasLimit: uint64(gas), MaxFeePerGas: feeCap, MaxTipPerGas: tipCap, PolicyID: policy}
	if _, err := BuildType2(c); err != nil {
		return Candidate{}, err
	}
	return c, nil
}

func (s *Store) persistSigned(ctx context.Context, id, token string, candidate Candidate,
	raw []byte, hash common.Hash) error {
	return s.persistSignedAttempt(ctx, id, 1, token, candidate, raw, hash)
}

func (s *Store) persistSignedAttempt(ctx context.Context, id string, number int, token string,
	candidate Candidate, raw []byte, hash common.Hash) error {
	var decoded types.Transaction
	if err := decoded.UnmarshalBinary(raw); err != nil || decoded.Hash() != hash ||
		ValidateSigned(candidate, &decoded) != nil {
		return &SigningError{Code: "SIGNED_ATTEMPT_INCONSISTENT"}
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return &SigningError{Code: "SIGNING_PERSISTENCE_FAILED", Retryable: true}
	}
	defer tx.Rollback(ctx)
	var state State
	var currentToken sql.NullString
	var leaseActive bool
	var active sql.NullInt64
	var winner sql.NullInt64
	err = tx.QueryRow(ctx, `SELECT state,lease_token::text,active_attempt_number,winning_attempt_number,
		COALESCE(lease_expires_at > clock_timestamp(),false)
		FROM transaction_intents WHERE intent_id=$1::uuid FOR UPDATE`, id).Scan(&state, &currentToken, &active, &winner, &leaseActive)
	if err != nil {
		return &SigningError{Code: "SIGNING_PERSISTENCE_FAILED", Retryable: true}
	}
	if state != Signing || !currentToken.Valid || currentToken.String != token || !leaseActive ||
		!active.Valid || int(active.Int64) != number || winner.Valid {
		return &SigningError{Code: "SIGNING_CLAIM_LOST", Retryable: true}
	}
	tag, err := tx.Exec(ctx, `UPDATE transaction_attempts SET status='SIGNED',raw_tx=$2,
		transaction_hash=$3,signed_at=clock_timestamp()
		WHERE intent_id=$1::uuid AND attempt_number=$4 AND status='PREPARING'`, id, raw, hash.Bytes(), number)
	if err != nil {
		return &SigningError{Code: "SIGNING_PERSISTENCE_FAILED", Retryable: true}
	}
	if tag.RowsAffected() != 1 {
		return ErrTransition
	}
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state='READY',lease_token=NULL,
		lease_expires_at=NULL,reason_code=NULL,version=version+1,updated_at=now()
		WHERE intent_id=$1::uuid`, id)
	if err != nil {
		return &SigningError{Code: "SIGNING_PERSISTENCE_FAILED", Retryable: true}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events (intent_id,event_type)
		VALUES ($1::uuid,'ATTEMPT_SIGNED')`, id); err != nil {
		return &SigningError{Code: "SIGNING_PERSISTENCE_FAILED", Retryable: true}
	}
	if err = tx.Commit(ctx); err != nil {
		return &SigningError{Code: "SIGNING_PERSISTENCE_FAILED", Retryable: true}
	}
	return nil
}

func (s *Store) releaseSigningClaim(id, token, code string) {
	s.finishFailedClaim(id, token, code, false)
}

func (s *Store) blockSigningClaim(id, token, code string) {
	s.finishFailedClaim(id, token, code, true)
}

func (s *Store) finishFailedClaim(id, token, code string, permanent bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var chain string
	var address []byte
	if err := s.db.QueryRow(ctx, `SELECT chain_id::text,signer_address FROM transaction_intents
		WHERE intent_id=$1::uuid`, id).Scan(&chain, &address); err != nil {
		return
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return // The lease expires; recovery retains the unsigned candidate.
	}
	defer tx.Rollback(ctx)
	// Keep the same account-then-intent lock order as nonce reservation.
	if _, err = tx.Exec(ctx, `SELECT 1 FROM signer_accounts
		WHERE chain_id=$1::numeric AND signer_address=$2 FOR UPDATE`, chain, address); err != nil {
		return
	}
	var current sql.NullString
	var state State
	if err = tx.QueryRow(ctx, `SELECT state,lease_token::text FROM transaction_intents
		WHERE intent_id=$1::uuid FOR UPDATE`, id).Scan(&state, &current); err != nil ||
		state != Signing || !current.Valid || current.String != token {
		return
	}
	if permanent {
		_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state='BLOCKED',resume_state='SIGNING',
			lease_token=NULL,lease_expires_at=NULL,reason_code=$2,version=version+1,updated_at=now()
			WHERE intent_id=$1::uuid`, id, code)
	} else {
		_, err = tx.Exec(ctx, `UPDATE transaction_intents SET lease_token=NULL,
			lease_expires_at=NULL,reason_code=$2,version=version+1,updated_at=now()
			WHERE intent_id=$1::uuid`, id, code)
	}
	if err != nil {
		return
	}
	if permanent {
		if _, err = tx.Exec(ctx, `UPDATE signer_accounts SET paused_reason='INTENT_BLOCKED',updated_at=now()
			WHERE chain_id=$1::numeric AND signer_address=$2
			AND (paused_reason IS NULL OR paused_reason='INTENT_BLOCKED')`, chain, address); err != nil {
			return
		}
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events (intent_id,event_type,reason_code)
		VALUES ($1::uuid,'SIGNING_FAILURE',$2)`, id, code); err != nil {
		return
	}
	_ = tx.Commit(ctx)
}

// LoadSignedAttempt validates the stored binary and locally derived hash on
// every load. Callers receive a copy so they cannot mutate the stored buffer.
func (s *Store) LoadSignedAttempt(ctx context.Context, id string) (SignedAttempt, error) {
	return s.loadSignedAttempt(ctx, id, 0)
}

func (s *Store) LoadSignedAttemptNumber(ctx context.Context, id string, number int) (SignedAttempt, error) {
	if number < 1 {
		return SignedAttempt{}, ErrInvalid
	}
	return s.loadSignedAttempt(ctx, id, number)
}

func (s *Store) loadSignedAttempt(ctx context.Context, id string, number int) (SignedAttempt, error) {
	var result SignedAttempt
	var state State
	var chain, nonce, value, fee, tip, policy, status string
	var signerBytes, toBytes, data, hash []byte
	var gas int64
	err := s.db.QueryRow(ctx, `SELECT i.state,i.chain_id::text,r.nonce::text,i.signer_address,
		i.to_address,i.value_wei::text,i.calldata,a.gas_limit,a.max_fee_per_gas::text,
		a.max_priority_fee_per_gas::text,a.policy_snapshot_id,a.status,a.attempt_id::text,
		a.transaction_hash,a.raw_tx,a.attempt_number
		FROM transaction_intents i JOIN nonce_reservations r USING (intent_id)
		JOIN transaction_attempts a ON a.intent_id=i.intent_id AND
			a.attempt_number=CASE WHEN $2=0 THEN i.active_attempt_number ELSE $2 END
		WHERE i.intent_id=$1::uuid`, id, number).Scan(&state, &chain, &nonce, &signerBytes, &toBytes,
		&value, &data, &gas, &fee, &tip, &policy, &status, &result.AttemptID, &hash, &result.Raw,
		&result.AttemptNumber)
	if errors.Is(err, pgx.ErrNoRows) {
		return SignedAttempt{}, ErrNotFound
	}
	if err != nil {
		return SignedAttempt{}, err
	}
	if (state != Ready && state != Uncertain && state != Pending && state != Blocked &&
		state != Signing && state != Mined && state != Confirming && state != Confirmed && state != Reorged) ||
		(status != "SIGNED" && status != "BROADCAST_CLAIMED" && status != "OBSERVED" &&
			status != "REJECTED" && status != "NOT_BROADCAST") ||
		len(hash) != common.HashLength || len(result.Raw) == 0 {
		return SignedAttempt{}, &SigningError{Code: "SIGNED_ATTEMPT_INCONSISTENT"}
	}
	candidate, err := candidateFromValues(chain, nonce, signerBytes, toBytes, value, data,
		gas, fee, tip, policy)
	if err != nil {
		return SignedAttempt{}, &SigningError{Code: "SIGNED_ATTEMPT_INCONSISTENT"}
	}
	var tx types.Transaction
	if err = tx.UnmarshalBinary(result.Raw); err != nil ||
		!bytes.Equal(tx.Hash().Bytes(), hash) || ValidateSigned(candidate, &tx) != nil {
		return SignedAttempt{}, &SigningError{Code: "SIGNED_ATTEMPT_INCONSISTENT"}
	}
	result.IntentID = id
	result.Hash = common.BytesToHash(hash)
	result.Raw = bytes.Clone(result.Raw)
	return result, nil
}
