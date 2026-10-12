package orchestrator

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
)

type AccountKey struct {
	ChainID  string
	SignerID string
}

type AccountReconciliationState string

const (
	AccountTrustedSeed    AccountReconciliationState = "TRUSTED_SEED"
	AccountReconciling    AccountReconciliationState = "RECONCILING"
	AccountReady          AccountReconciliationState = "READY"
	AccountExternalNonce  AccountReconciliationState = "BLOCKED_EXTERNAL_NONCE"
	AccountRPCUnavailable AccountReconciliationState = "RPC_UNAVAILABLE"
)

type AccountStatus struct {
	Key            AccountKey
	Address        common.Address
	NextNonce      uint64
	Initialized    bool
	NetworkManaged bool
	State          AccountReconciliationState
	Reason         string
	SignerStatus   string
	SignerReason   string
	PausedReason   string
	LatestNonce    uint64
	PendingNonce   uint64
	ReconciledAt   time.Time
}

// RegisterNetworkAccount starts with no trusted nonce seed. Allocation remains
// blocked until pending and latest network evidence has been reconciled.
func (s *Store) RegisterNetworkAccount(ctx context.Context, account Account) error {
	chain, err := decimal(account.ChainID, true)
	if err != nil || account.SignerID == "" || account.InitialNonce != 0 {
		return ErrInvalid
	}
	_, err = s.db.Exec(ctx, `INSERT INTO signer_accounts
		(chain_id,signer_address,signer_id,next_nonce,paused_reason,
		reconciliation_status,reconciliation_reason,network_nonce_initialized,network_managed)
		VALUES($1::numeric,$2,$3,0,'NONCE_RECONCILIATION',
		'RECONCILING','INITIAL_NONCE_REQUIRED',false,true)
		ON CONFLICT (chain_id,signer_address) DO NOTHING`, chain, account.Address[:], account.SignerID)
	if err != nil {
		if IsUniqueViolation(err) {
			return ErrAccountConflict
		}
		return err
	}
	status, err := s.AccountStatus(ctx, AccountKey{ChainID: chain, SignerID: account.SignerID})
	if err != nil || status.Address != common.BytesToAddress(account.Address[:]) || !status.NetworkManaged {
		return ErrAccountConflict
	}
	return nil
}

func (s *Store) AccountStatus(ctx context.Context, key AccountKey) (AccountStatus, error) {
	chain, err := decimal(key.ChainID, true)
	if err != nil || key.SignerID == "" {
		return AccountStatus{}, ErrInvalid
	}
	var out AccountStatus
	var address []byte
	var next string
	var latest, pending sql.NullString
	var reason, paused, signerReason sql.NullString
	var reconciled sql.NullTime
	err = s.db.QueryRow(ctx, `SELECT signer_address,next_nonce::text,network_nonce_initialized,network_managed,
		reconciliation_status,reconciliation_reason,signer_status,signer_reason,paused_reason,
		observed_latest_nonce::text,observed_pending_nonce::text,nonce_reconciled_at
		FROM signer_accounts WHERE chain_id=$1::numeric AND signer_id=$2`, chain, key.SignerID).
		Scan(&address, &next, &out.Initialized, &out.NetworkManaged, &out.State, &reason,
			&out.SignerStatus, &signerReason, &paused,
			&latest, &pending, &reconciled)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, ErrNotFound
	}
	if err != nil {
		return out, err
	}
	out.Key = AccountKey{ChainID: chain, SignerID: key.SignerID}
	out.Address = common.BytesToAddress(address)
	out.NextNonce, err = parseNonce(next)
	if err != nil {
		return out, err
	}
	if reason.Valid {
		out.Reason = reason.String
	}
	if signerReason.Valid {
		out.SignerReason = signerReason.String
	}
	if paused.Valid {
		out.PausedReason = paused.String
	}
	if latest.Valid {
		out.LatestNonce, err = parseNonce(latest.String)
		if err != nil {
			return out, err
		}
	}
	if pending.Valid {
		out.PendingNonce, err = parseNonce(pending.String)
		if err != nil {
			return out, err
		}
	}
	if reconciled.Valid {
		out.ReconciledAt = reconciled.Time
	}
	return out, nil
}

type nonceEvidence struct {
	endpoint string
	latest   uint64
	pending  uint64
	head     BlockEvidence
}

// ReconcileAccount fences allocation before network reads. Its token prevents
// an older worker from committing evidence after a newer reconciliation.
func (s *Store) ReconcileAccount(ctx context.Context, key AccountKey,
	pool *EndpointPool, deadline time.Duration) (AccountStatus, error) {
	ctx, span := otel.Tracer("transaction-orchestrator").Start(ctx, "account.reconcile")
	defer span.End()
	if pool == nil || deadline <= 0 || deadline > time.Minute {
		return AccountStatus{}, ErrInvalid
	}
	before, err := s.AccountStatus(ctx, key)
	if err != nil {
		return AccountStatus{}, err
	}
	token, err := newID()
	if err != nil {
		return AccountStatus{}, err
	}
	_, err = s.db.Exec(ctx, `UPDATE signer_accounts SET reconciliation_status='RECONCILING',
		reconciliation_reason='NETWORK_CHECK_IN_PROGRESS',reconciliation_token=$3::uuid,
		paused_reason=CASE WHEN paused_reason IS NULL OR paused_reason IN
		('NONCE_RECONCILIATION','EXTERNAL_NONCE_CONFLICT')
		THEN 'NONCE_RECONCILIATION' ELSE paused_reason END,
		updated_at=clock_timestamp() WHERE chain_id=$1::numeric AND signer_id=$2`,
		key.ChainID, key.SignerID, token)
	if err != nil {
		return AccountStatus{}, err
	}
	endpoints := pool.Eligible(key.ChainID, EndpointRead|EndpointNonce)
	var observations []nonceEvidence
	for _, ep := range endpoints {
		if ctx.Err() != nil {
			return AccountStatus{}, ctx.Err()
		}
		read, cancel := context.WithTimeout(ctx, deadline)
		head, headErr := ep.LatestBlock(read)
		latest, latestErr := ep.LatestNonce(read, before.Address)
		pending, pendingErr := ep.PendingNonce(read, before.Address)
		cancel()
		if headErr != nil || latestErr != nil || pendingErr != nil || head == nil ||
			pending < latest {
			pool.RecordFailover("unavailable")
			continue
		}
		observations = append(observations, nonceEvidence{ep.Alias(), latest, pending, *head})
	}
	state, reason := AccountReady, ""
	var latest, pending uint64
	if len(observations) == 0 {
		state, reason = AccountRPCUnavailable, "NONCE_EVIDENCE_UNAVAILABLE"
	} else {
		latest, pending = observations[0].latest, observations[0].pending
		for _, view := range observations[1:] {
			if view.latest != latest || view.pending != pending ||
				(view.head.Number == observations[0].head.Number && view.head.Hash != observations[0].head.Hash) {
				state, reason = AccountReconciling, "RPC_NONCE_DISAGREEMENT"
				break
			}
		}
	}
	if state == AccountReady && before.Initialized {
		if latest > before.NextNonce || pending > before.NextNonce {
			state, reason = AccountExternalNonce, "EXTERNAL_NONCE_CONFLICT"
		} else {
			// A nonce below the canonical network cursor must have a known
			// canonical receipt. An omitted or stale local receipt fails closed.
			var uncovered int
			err = s.db.QueryRow(ctx, `SELECT count(*) FROM nonce_reservations r
				JOIN transaction_intents i ON i.intent_id=r.intent_id
				WHERE r.chain_id=$1::numeric AND r.signer_address=$2
				AND r.nonce<$3::numeric AND NOT EXISTS (
					SELECT 1 FROM observed_receipts o WHERE o.intent_id=r.intent_id
					AND o.verification_status='CANONICAL' AND i.state='CONFIRMED')`,
				key.ChainID, before.Address.Bytes(), strconv.FormatUint(latest, 10)).Scan(&uncovered)
			if err != nil {
				return AccountStatus{}, err
			}
			if uncovered > 0 {
				state, reason = AccountExternalNonce, "OWNED_NONCE_NOT_CANONICAL"
			}
		}
		if state == AccountReady && pending > latest {
			// A pending network nonce can overlap a locally reserved nonce
			// without exceeding next_nonce. If that intent has never sent a
			// transaction, the pending progression cannot be attributed to it.
			var unsentOverlap int
			err = s.db.QueryRow(ctx, `SELECT count(*) FROM nonce_reservations r
				JOIN transaction_intents i ON i.intent_id=r.intent_id
				WHERE r.chain_id=$1::numeric AND r.signer_address=$2
				AND r.nonce >= $3::numeric AND r.nonce < $4::numeric
				AND i.broadcast_send_count=0`, key.ChainID, before.Address.Bytes(),
				strconv.FormatUint(latest, 10), strconv.FormatUint(pending, 10)).Scan(&unsentOverlap)
			if err != nil {
				return AccountStatus{}, err
			}
			if unsentOverlap > 0 {
				state, reason = AccountExternalNonce, "PENDING_OVERLAPS_UNSENT_RESERVATION"
			}
		}
	}
	if state == AccountReady && !before.Initialized && pending == math.MaxUint64 {
		state, reason = AccountExternalNonce, "NONCE_RANGE_EXHAUSTED"
	}
	return s.finishAccountReconciliation(ctx, key, token, before, state, reason, latest, pending)
}

func (s *Store) finishAccountReconciliation(ctx context.Context, key AccountKey, token string,
	before AccountStatus, state AccountReconciliationState, reason string,
	latest, pending uint64) (AccountStatus, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return AccountStatus{}, err
	}
	defer tx.Rollback(ctx)
	var currentToken sql.NullString
	var next string
	var initialized bool
	err = tx.QueryRow(ctx, `SELECT reconciliation_token::text,next_nonce::text,
		network_nonce_initialized FROM signer_accounts
		WHERE chain_id=$1::numeric AND signer_id=$2 FOR UPDATE`, key.ChainID, key.SignerID).
		Scan(&currentToken, &next, &initialized)
	if err != nil {
		return AccountStatus{}, err
	}
	if !currentToken.Valid || currentToken.String != token {
		return AccountStatus{}, ErrTransition
	}
	current, err := parseNonce(next)
	if err != nil {
		return AccountStatus{}, err
	}
	if initialized != before.Initialized || current != before.NextNonce {
		state, reason = AccountReconciling, "LOCAL_NONCE_CHANGED"
	}
	newNext := current
	if state == AccountReady && !initialized {
		newNext = pending
	}
	paused := "NONCE_RECONCILIATION"
	if state == AccountExternalNonce {
		paused = "EXTERNAL_NONCE_CONFLICT"
	}
	var pauseValue any = paused
	if state == AccountReady {
		pauseValue = nil
	}
	_, err = tx.Exec(ctx, `UPDATE signer_accounts SET
		next_nonce=$3::numeric,
		network_nonce_initialized=network_nonce_initialized OR $4,
		reconciliation_status=$5,reconciliation_reason=NULLIF($6,''),
		reconciliation_token=NULL,
		paused_reason=CASE WHEN paused_reason IS NULL OR paused_reason IN
		('NONCE_RECONCILIATION','EXTERNAL_NONCE_CONFLICT') THEN $7::text
		ELSE paused_reason END,
		observed_latest_nonce=$8::numeric,observed_pending_nonce=$9::numeric,
		nonce_reconciled_at=CASE WHEN $5='READY' THEN clock_timestamp()
		ELSE nonce_reconciled_at END,updated_at=clock_timestamp()
		WHERE chain_id=$1::numeric AND signer_id=$2`, key.ChainID, key.SignerID,
		strconv.FormatUint(newNext, 10), state == AccountReady, string(state), reason,
		pauseValue, strconv.FormatUint(latest, 10), strconv.FormatUint(pending, 10))
	if err != nil {
		return AccountStatus{}, err
	}
	if state == AccountExternalNonce || reason == "RPC_NONCE_DISAGREEMENT" {
		_, err = tx.Exec(ctx, `INSERT INTO account_reconciliation_events
			(chain_id,signer_address,result,reason,local_next_nonce,
			observed_latest_nonce,observed_pending_nonce)
			VALUES($1::numeric,$2,$3,$4,$5::numeric,$6::numeric,$7::numeric)`,
			key.ChainID, before.Address.Bytes(), string(state), reason,
			strconv.FormatUint(current, 10), strconv.FormatUint(latest, 10),
			strconv.FormatUint(pending, 10))
		if err != nil {
			return AccountStatus{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return AccountStatus{}, err
	}
	return s.AccountStatus(ctx, key)
}

func (s *Store) AccountKeys(ctx context.Context, after AccountKey, limit int) ([]AccountKey, error) {
	if limit < 1 || limit > 1000 {
		return nil, ErrInvalid
	}
	rows, err := s.db.Query(ctx, `SELECT chain_id::text,signer_id FROM signer_accounts
		WHERE ($1::numeric IS NULL OR (chain_id,signer_id)>($1::numeric,$2::text))
		ORDER BY chain_id,signer_id LIMIT $3`, nullableChain(after.ChainID), after.SignerID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AccountKey
	for rows.Next() {
		var key AccountKey
		if err = rows.Scan(&key.ChainID, &key.SignerID); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, rows.Err()
}

func (s *Store) AccountKeysDue(ctx context.Context, after AccountKey, now time.Time, limit int) ([]AccountKey, error) {
	if now.IsZero() || limit < 1 || limit > 1000 {
		return nil, ErrInvalid
	}
	rows, err := s.db.Query(ctx, `SELECT chain_id::text,signer_id FROM signer_accounts
		WHERE (account_next_reconcile_at IS NULL OR account_next_reconcile_at<=$3)
		AND ($1::numeric IS NULL OR (chain_id,signer_id)>($1::numeric,$2::text))
		ORDER BY chain_id,signer_id LIMIT $4`, nullableChain(after.ChainID), after.SignerID, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AccountKey
	for rows.Next() {
		var key AccountKey
		if err = rows.Scan(&key.ChainID, &key.SignerID); err != nil {
			return nil, err
		}
		out = append(out, key)
	}
	return out, rows.Err()
}

func (s *Store) ScheduleAccountCheck(ctx context.Context, key AccountKey, when time.Time) error {
	if when.IsZero() {
		return ErrInvalid
	}
	_, err := s.db.Exec(ctx, `UPDATE signer_accounts SET account_next_reconcile_at=$3
		WHERE chain_id=$1::numeric AND signer_id=$2`, key.ChainID, key.SignerID, when)
	return err
}

func nullableChain(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func (s *Store) SetSignerHealth(ctx context.Context, key AccountKey, available bool, reason string) error {
	if _, err := decimal(key.ChainID, true); err != nil || key.SignerID == "" {
		return ErrInvalid
	}
	state := "AVAILABLE"
	if !available {
		state = "UNAVAILABLE"
		if reason != "SIGNER_UNAVAILABLE" && reason != "SIGNER_IDENTITY_MISMATCH" {
			reason = "SIGNER_UNAVAILABLE"
		}
	} else {
		reason = ""
	}
	result, err := s.db.Exec(ctx, `UPDATE signer_accounts SET signer_status=$3,
		signer_reason=NULLIF($4,''),
		paused_reason=CASE
			WHEN $3='AVAILABLE' AND paused_reason='SIGNER_UNAVAILABLE' THEN NULL
			WHEN $3='UNAVAILABLE' AND paused_reason IS NULL THEN 'SIGNER_UNAVAILABLE'
			ELSE paused_reason END,updated_at=clock_timestamp()
		WHERE chain_id=$1::numeric AND signer_id=$2`, key.ChainID, key.SignerID, state, reason)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *Store) ReserveReconciledNonce(ctx context.Context, id string,
	pool *EndpointPool, deadline time.Duration) (uint64, error) {
	intent, err := s.GetIntent(ctx, id)
	if err != nil {
		return 0, err
	}
	if intent.Nonce != nil {
		return s.ReserveNonce(ctx, id)
	}
	status, err := s.ReconcileAccount(ctx, AccountKey{intent.ChainID, intent.SignerID}, pool, deadline)
	if err != nil {
		return 0, err
	}
	if status.State != AccountReady {
		return 0, fmt.Errorf("account reconciliation: %s", status.State)
	}
	return s.ReserveNonce(ctx, id)
}
