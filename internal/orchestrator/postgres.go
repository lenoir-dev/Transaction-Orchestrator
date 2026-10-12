package orchestrator

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
)

type Store struct{ db *pgxpool.Pool }

func NewStore(db *pgxpool.Pool) *Store { return &Store{db: db} }

// ProvisionAccount establishes the local nonce cursor from an externally
// verified seed. Reprovisioning never rewinds or advances an existing cursor.
func (s *Store) ProvisionAccount(ctx context.Context, account Account) error {
	chain, err := decimal(account.ChainID, true)
	if err != nil || strings.TrimSpace(account.SignerID) == "" {
		return ErrInvalid
	}
	_, err = s.db.Exec(ctx, `INSERT INTO signer_accounts
		(chain_id, signer_address, signer_id, next_nonce) VALUES ($1::numeric,$2,$3,$4::numeric)
		ON CONFLICT (chain_id, signer_address) DO NOTHING`, chain, account.Address[:], account.SignerID,
		strconv.FormatUint(account.InitialNonce, 10))
	if err != nil {
		if IsUniqueViolation(err) {
			return ErrAccountConflict
		}
		return err
	}
	var signer string
	var address []byte
	err = s.db.QueryRow(ctx, `SELECT signer_id, signer_address FROM signer_accounts
		WHERE chain_id=$1::numeric AND signer_address=$2`, chain, account.Address[:]).Scan(&signer, &address)
	if err != nil {
		return err
	}
	if signer != account.SignerID || !bytes.Equal(address, account.Address[:]) {
		return ErrAccountConflict
	}
	return nil
}

// CreateIntent resolves the tenant-scoped key in PostgreSQL. A duplicate
// retains its original policy snapshot even if current defaults have changed.
func (s *Store) CreateIntent(ctx context.Context, req Request, policy Policy) (Intent, error) {
	intent, _, err := s.CreateIntentWithResult(ctx, req, policy)
	return intent, err
}

// ExistingIntent resolves a tenant-scoped idempotency key without admitting
// new work. It lets HTTP retries inspect committed state during RPC outages.
func (s *Store) ExistingIntent(ctx context.Context, req Request) (Intent, error) {
	n, err := normalize(req)
	if err != nil {
		return Intent{}, err
	}
	var id string
	var digest []byte
	err = s.db.QueryRow(ctx, `SELECT intent_id::text,request_digest FROM transaction_intents
		WHERE tenant_id=$1 AND idempotency_key=$2`, req.TenantID, req.IdempotencyKey).Scan(&id, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return Intent{}, ErrNotFound
	}
	if err != nil {
		return Intent{}, err
	}
	if !bytes.Equal(digest, n.digest[:]) {
		return Intent{}, ErrConflict
	}
	return s.GetIntent(ctx, id)
}

// CreateIntentWithResult distinguishes a committed insert from an idempotent
// replay using the same unique-index decision as CreateIntent.
func (s *Store) CreateIntentWithResult(ctx context.Context, req Request, policy Policy) (Intent, bool, error) {
	ctx, span := otel.Tracer("transaction-orchestrator").Start(ctx, "db.intent.create")
	defer span.End()
	n, err := normalize(req)
	if err != nil {
		return Intent{}, false, err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return Intent{}, false, err
	}
	defer tx.Rollback(ctx)
	var id string
	var digest []byte
	err = tx.QueryRow(ctx, `SELECT intent_id::text, request_digest FROM transaction_intents
		WHERE tenant_id=$1 AND idempotency_key=$2`, req.TenantID, req.IdempotencyKey).Scan(&id, &digest)
	if err == nil {
		if !bytes.Equal(digest, n.digest[:]) {
			return Intent{}, false, ErrConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return Intent{}, false, err
		}
		intent, getErr := s.GetIntent(ctx, id)
		return intent, false, getErr
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Intent{}, false, err
	}
	fee, tip, err := policy.validate()
	if err != nil {
		return Intent{}, false, err
	}
	var signerAddress []byte
	err = tx.QueryRow(ctx, `SELECT signer_address FROM signer_accounts
		WHERE chain_id=$1::numeric AND signer_id=$2`, n.chainID, req.SignerID).Scan(&signerAddress)
	if errors.Is(err, pgx.ErrNoRows) {
		return Intent{}, false, ErrAccountConflict
	}
	if err != nil {
		return Intent{}, false, err
	}
	id, err = newID()
	if err != nil {
		return Intent{}, false, err
	}
	err = tx.QueryRow(ctx, `INSERT INTO transaction_intents
		(intent_id, tenant_id, idempotency_key, request_digest, policy_version,
		 chain_id, signer_address, signer_id, to_address, value_wei, calldata,
		 gas_limit, confirmation_depth, max_fee_cap_wei, max_tip_cap_wei,
		 max_replacements, state)
		VALUES ($1::uuid,$2,$3,$4,$5,$6::numeric,$7,$8,$9,$10::numeric,$11,$12,$13,
		        $14::numeric,$15::numeric,$16,'CREATED')
		ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING intent_id::text`,
		id, req.TenantID, req.IdempotencyKey, n.digest[:], policy.Version,
		n.chainID, signerAddress, req.SignerID, n.to, n.value, n.data,
		int64(policy.GasLimit), policy.ConfirmationDepth, fee, tip, policy.MaxReplacements).Scan(&id)
	created := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		// A concurrent insert won. The unique index made this decision durable.
		err = tx.QueryRow(ctx, `SELECT intent_id::text, request_digest FROM transaction_intents
			WHERE tenant_id=$1 AND idempotency_key=$2`, req.TenantID, req.IdempotencyKey).Scan(&id, &digest)
		if err != nil {
			return Intent{}, false, err
		}
		if !bytes.Equal(digest, n.digest[:]) {
			return Intent{}, false, ErrConflict
		}
	} else if err != nil {
		return Intent{}, false, err
	} else {
		if _, err = tx.Exec(ctx, `INSERT INTO audit_events (intent_id,event_type)
			VALUES ($1::uuid,'INTENT_CREATED')`, id); err != nil {
			return Intent{}, false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return Intent{}, false, err
	}
	intent, getErr := s.GetIntent(ctx, id)
	return intent, created, getErr
}

func (s *Store) GetIntent(ctx context.Context, id string) (Intent, error) {
	var i Intent
	var nonce sql.NullString
	err := s.db.QueryRow(ctx, `SELECT i.intent_id::text, i.tenant_id, i.chain_id::text,
		i.signer_id, i.state, COALESCE(i.reason_code,''), r.nonce::text, i.version, i.policy_version
		FROM transaction_intents i LEFT JOIN nonce_reservations r USING (intent_id)
		WHERE i.intent_id=$1::uuid`, id).Scan(&i.ID, &i.TenantID, &i.ChainID, &i.SignerID,
		&i.State, &i.Reason, &nonce, &i.Version, &i.PolicyVersion)
	if errors.Is(err, pgx.ErrNoRows) {
		return Intent{}, ErrNotFound
	}
	if err != nil {
		return Intent{}, err
	}
	if nonce.Valid {
		parsed, err := parseNonce(nonce.String)
		if err != nil {
			return Intent{}, err
		}
		i.Nonce = &parsed
	}
	return i, nil
}

// ReserveNonce serializes replicas on the account row. The transaction owns
// cursor advancement, reservation, intent state, and audit entry together.
func (s *Store) ReserveNonce(ctx context.Context, id string) (uint64, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var chain string
	var address []byte
	err = tx.QueryRow(ctx, `SELECT chain_id::text, signer_address FROM transaction_intents
		WHERE intent_id=$1::uuid`, id).Scan(&chain, &address)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	var next string
	var paused sql.NullString
	var reconciled string
	var initialized bool
	err = tx.QueryRow(ctx, `SELECT next_nonce::text, paused_reason,
		reconciliation_status,network_nonce_initialized FROM signer_accounts
		WHERE chain_id=$1::numeric AND signer_address=$2 FOR UPDATE`, chain, address).
		Scan(&next, &paused, &reconciled, &initialized)
	if err != nil {
		return 0, err
	}
	var state State
	err = tx.QueryRow(ctx, `SELECT state FROM transaction_intents WHERE intent_id=$1::uuid FOR UPDATE`, id).Scan(&state)
	if err != nil {
		return 0, err
	}
	if state == Reserved {
		var existing string
		if err = tx.QueryRow(ctx, `SELECT nonce::text FROM nonce_reservations WHERE intent_id=$1::uuid`, id).Scan(&existing); err != nil {
			return 0, err
		}
		if err = tx.Commit(ctx); err != nil {
			return 0, err
		}
		return parseNonce(existing)
	}
	if state != Created {
		return 0, ErrTransition
	}
	if paused.Valid || !initialized || (reconciled != "READY" && reconciled != "TRUSTED_SEED") {
		return 0, ErrAccountPaused
	}
	nonce, err := parseNonce(next)
	if err != nil {
		return 0, err
	}
	if nonce == math.MaxUint64 {
		return 0, ErrNonceExhausted
	}
	_, err = tx.Exec(ctx, `INSERT INTO nonce_reservations
		(intent_id,chain_id,signer_address,nonce) VALUES ($1::uuid,$2::numeric,$3,$4::numeric)`,
		id, chain, address, next)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `UPDATE signer_accounts SET next_nonce=$3::numeric,updated_at=now()
		WHERE chain_id=$1::numeric AND signer_address=$2`, chain, address, strconv.FormatUint(nonce+1, 10))
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state='RESERVED',version=version+1,updated_at=now()
		WHERE intent_id=$1::uuid`, id)
	if err != nil {
		return 0, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events (intent_id,event_type) VALUES ($1::uuid,'NONCE_RESERVED')`, id)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return nonce, nil
}

// Block applies to pre-sign states. Resume also releases a blocked unsigned
// signing claim. A blocked reserved nonce pauses subsequent allocations.
func (s *Store) Block(ctx context.Context, id, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return ErrInvalid
	}
	return s.changePreSign(ctx, id, reason, true)
}

func (s *Store) Resume(ctx context.Context, id, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return ErrInvalid
	}
	return s.changePreSign(ctx, id, reason, false)
}

func (s *Store) changePreSign(ctx context.Context, id, reason string, block bool) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var chain string
	var address []byte
	err = tx.QueryRow(ctx, `SELECT chain_id::text,signer_address FROM transaction_intents
		WHERE intent_id=$1::uuid`, id).Scan(&chain, &address)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	// Lock account before intent in the same order as nonce allocation.
	if _, err = tx.Exec(ctx, `SELECT 1 FROM signer_accounts
		WHERE chain_id=$1::numeric AND signer_address=$2 FOR UPDATE`, chain, address); err != nil {
		return err
	}
	var state State
	var prior sql.NullString
	err = tx.QueryRow(ctx, `SELECT state,resume_state FROM transaction_intents
		WHERE intent_id=$1::uuid FOR UPDATE`, id).Scan(&state, &prior)
	if err != nil {
		return err
	}
	if block {
		if state != Created && state != Reserved {
			return ErrTransition
		}
		_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state='BLOCKED',resume_state=$2,
			reason_code=$3,version=version+1,updated_at=now() WHERE intent_id=$1::uuid`, id, state, reason)
		if err != nil {
			return err
		}
		if state == Reserved {
			_, err = tx.Exec(ctx, `UPDATE signer_accounts SET paused_reason='INTENT_BLOCKED',updated_at=now()
				WHERE chain_id=$1::numeric AND signer_address=$2
				AND (paused_reason IS NULL OR paused_reason='INTENT_BLOCKED')`, chain, address)
			if err != nil {
				return err
			}
		}
	} else {
		if state != Blocked || !prior.Valid ||
			(prior.String != string(Created) && prior.String != string(Reserved) && prior.String != string(Signing)) {
			return ErrTransition
		}
		_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state=resume_state,resume_state=NULL,
			reason_code=NULL,version=version+1,updated_at=now() WHERE intent_id=$1::uuid`, id)
		if err != nil {
			return err
		}
		if prior.String == string(Reserved) || prior.String == string(Signing) {
			_, err = tx.Exec(ctx, `UPDATE signer_accounts SET paused_reason=NULL,updated_at=now()
				WHERE chain_id=$1::numeric AND signer_address=$2 AND paused_reason='INTENT_BLOCKED'
				AND NOT EXISTS (SELECT 1 FROM transaction_intents i JOIN nonce_reservations r USING (intent_id)
					WHERE r.chain_id=$1::numeric AND r.signer_address=$2 AND i.state='BLOCKED')`, chain, address)
			if err != nil {
				return err
			}
		}
	}
	event := "INTENT_BLOCKED"
	if !block {
		event = "INTENT_RESUMED"
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events (intent_id,event_type,reason_code)
		VALUES ($1::uuid,$2,$3)`, id, event, reason)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// FailUnreserved is limited to a created intent. A reserved nonce remains
// owned and cannot be silently abandoned as a terminal failure.
func (s *Store) FailUnreserved(ctx context.Context, id, reason string) error {
	if strings.TrimSpace(reason) == "" {
		return ErrInvalid
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var state State
	err = tx.QueryRow(ctx, `SELECT state FROM transaction_intents WHERE intent_id=$1::uuid FOR UPDATE`, id).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if state != Created {
		return ErrTransition
	}
	if _, err = tx.Exec(ctx, `UPDATE transaction_intents SET state='FAILED',reason_code=$2,
		version=version+1,updated_at=now() WHERE intent_id=$1::uuid`, id, reason); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events (intent_id,event_type,reason_code)
		VALUES ($1::uuid,'INTENT_FAILED',$2)`, id, reason); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RecoverableIntents reconstructs work solely from committed rows after restart.
func (s *Store) RecoverableIntents(ctx context.Context) ([]Intent, error) {
	rows, err := s.db.Query(ctx, `SELECT intent_id::text FROM transaction_intents
		WHERE state NOT IN ('CONFIRMED','FAILED') ORDER BY created_at,intent_id`)
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	intents := make([]Intent, 0, len(ids))
	for _, id := range ids {
		i, err := s.GetIntent(ctx, id)
		if err != nil {
			return nil, fmt.Errorf("recover intent %s: %w", id, err)
		}
		intents = append(intents, i)
	}
	return intents, nil
}

// IsUniqueViolation is useful when distinguishing database constraints from
// retryable operational failures in tests and later admission paths.
func IsUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
