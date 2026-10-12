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
	ErrBroadcastInProgress = errors.New("broadcast claim is active")
	ErrBroadcastBackoff    = errors.New("broadcast reconciliation is not due")
	ErrNoEligibleEndpoint  = errors.New("no eligible broadcast endpoint")
)

type BroadcastPolicy struct {
	MaxSends    int
	MaxDuration time.Duration
	Backoff     time.Duration
	RPCDeadline time.Duration
	Lease       time.Duration
	GenesisHash common.Hash // Zero disables anchor matching in local fixtures.
}

func (p BroadcastPolicy) valid() bool {
	return p.MaxSends > 0 && p.MaxSends <= 100 && p.MaxDuration > 0 &&
		p.Backoff >= 0 && p.RPCDeadline > 0 && p.Lease > p.RPCDeadline*26+time.Second
}

type BroadcastResult struct {
	State               State
	Hash                common.Hash // Active attempt, which may differ from the receipt winner.
	SendCount           int
	LastOutcome         ResultClass
	WinnerAttemptNumber int
	WinnerHash          common.Hash
}

// BroadcastMetric exposes bounded counters for an exporter. No transaction,
// address, nonce, or intent identifiers enter labels.
type BroadcastMetric struct {
	Endpoint string
	Kind     string
	Result   string
	Count    int64
}

func (s *Store) BroadcastMetrics(ctx context.Context) ([]BroadcastMetric, error) {
	rows, err := s.db.Query(ctx, `SELECT endpoint,kind,classification,count(*)
		FROM broadcast_observations GROUP BY endpoint,kind,classification
		ORDER BY endpoint,kind,classification`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var metrics []BroadcastMetric
	for rows.Next() {
		var m BroadcastMetric
		if err = rows.Scan(&m.Endpoint, &m.Kind, &m.Result, &m.Count); err != nil {
			return nil, err
		}
		metrics = append(metrics, m)
	}
	return metrics, rows.Err()
}

type broadcastClaim struct {
	token    string
	count    int
	first    time.Time
	wasReady bool
}

type broadcastHooks struct {
	afterClaim  func()
	afterRPC    func()
	afterCommit func()
}

// AdvanceBroadcast performs at most one send. UNKNOWN work first checks all
// eligible endpoints for the locally known hash and receipt. Call again after
// the configured backoff; a worker can recover entirely from PostgreSQL.
func (s *Store) AdvanceBroadcast(ctx context.Context, id string, endpoints []BroadcastEndpoint,
	policy BroadcastPolicy) (BroadcastResult, error) {
	ctx, span := otel.Tracer("transaction-orchestrator").Start(ctx, "broadcast.advance")
	defer span.End()
	return s.advanceBroadcast(ctx, id, endpoints, policy, broadcastHooks{})
}

func (s *Store) advanceBroadcast(ctx context.Context, id string, endpoints []BroadcastEndpoint,
	policy BroadcastPolicy, hooks broadcastHooks) (BroadcastResult, error) {
	if !policy.valid() || len(endpoints) == 0 || len(endpoints) > 8 {
		return BroadcastResult{}, ErrInvalid
	}
	stored, err := s.LoadSignedAttempt(ctx, id)
	if err != nil {
		return BroadcastResult{}, err
	}
	var signed types.Transaction
	if err := signed.UnmarshalBinary(stored.Raw); err != nil || signed.Hash() != stored.Hash {
		return BroadcastResult{}, &SigningError{Code: "SIGNED_ATTEMPT_INCONSISTENT"}
	}
	if intent, err := s.GetIntent(ctx, id); err == nil && intent.State == Pending {
		return s.BroadcastStatus(ctx, id)
	} else if err != nil {
		return BroadcastResult{}, err
	}
	eligible := make([]BroadcastEndpoint, 0, len(endpoints))
	aliases := make(map[string]bool, len(endpoints))
	for _, endpoint := range endpoints {
		if endpoint == nil || !validAlias(endpoint.Alias()) {
			return BroadcastResult{}, ErrInvalid
		}
		if aliases[endpoint.Alias()] {
			return BroadcastResult{}, ErrInvalid
		}
		aliases[endpoint.Alias()] = true
		checkCtx, cancel := context.WithTimeout(ctx, policy.RPCDeadline)
		chain, chainErr := endpoint.ChainID(checkCtx)
		if chainErr == nil && policy.GenesisHash != (common.Hash{}) {
			var genesis common.Hash
			genesis, chainErr = endpoint.Genesis(checkCtx)
			if chainErr == nil && genesis != policy.GenesisHash {
				chainErr = ErrNoEligibleEndpoint
			}
		}
		if chainErr == nil && chain != nil && chain.Cmp(signed.ChainId()) == 0 {
			// A null result is expected; an error means the read capability is
			// unavailable and this endpoint cannot support safe recovery.
			_, chainErr = endpoint.TransactionByHash(checkCtx, stored.Hash)
		}
		cancel()
		if chainErr == nil && chain != nil && chain.Cmp(signed.ChainId()) == 0 {
			eligible = append(eligible, endpoint)
		}
	}
	if len(eligible) == 0 {
		return BroadcastResult{}, ErrNoEligibleEndpoint
	}
	if stored.AttemptNumber > 1 {
		winner, receiptErr := s.ObserveWinner(ctx, id, eligible, policy)
		if receiptErr != nil {
			return BroadcastResult{}, receiptErr
		}
		if winner > 0 {
			return s.BroadcastStatus(ctx, id)
		}
	}
	claim, current, err := s.claimBroadcast(ctx, id, stored.Hash, policy)
	if err != nil || current == Pending {
		return BroadcastResult{State: current, Hash: stored.Hash, SendCount: claim.count}, err
	}
	if hooks.afterClaim != nil {
		hooks.afterClaim()
	}
	if stored.AttemptNumber > 1 {
		winner, receiptErr := s.ObserveWinner(ctx, id, eligible, policy)
		if receiptErr != nil {
			return BroadcastResult{}, receiptErr
		}
		if winner > 0 {
			return s.BroadcastStatus(ctx, id)
		}
	}
	// A claimed UNKNOWN row can mean that the prior worker died after node
	// acceptance. Reconciliation must precede any identical rebroadcast.
	if !claim.wasReady {
		found, conflict, err := s.observeClaim(ctx, id, claim.token, stored, &signed, eligible, policy)
		if errors.Is(err, ErrReceiptObserved) {
			return s.BroadcastStatus(ctx, id)
		}
		if err != nil {
			return BroadcastResult{}, err
		}
		if found {
			return BroadcastResult{State: Pending, Hash: stored.Hash, SendCount: claim.count,
				LastOutcome: ResultSuccess}, nil
		}
		if conflict {
			return BroadcastResult{State: Blocked, Hash: stored.Hash, SendCount: claim.count,
				LastOutcome: ResultNonce}, nil
		}
	}
	if claim.count >= policy.MaxSends || time.Since(claim.first) >= policy.MaxDuration {
		if err := s.finishBroadcast(ctx, id, claim.token, "policy", "LOOKUP", "NOT_OBSERVED",
			Blocked, "OUTCOME_UNRESOLVED", stored.Hash, policy.Backoff); err != nil {
			return BroadcastResult{}, err
		}
		return BroadcastResult{State: Blocked, Hash: stored.Hash, SendCount: claim.count}, nil
	}
	endpoint := eligible[claim.count%len(eligible)]
	// Reload the persisted material immediately before each send, including
	// rebroadcast. Normal SQL cannot mutate signed bytes, but this also detects
	// storage corruption between the initial read and the network boundary.
	latest, loadErr := s.LoadSignedAttempt(ctx, id)
	var exact types.Transaction
	if loadErr != nil || latest.Hash != stored.Hash || !bytes.Equal(latest.Raw, stored.Raw) ||
		exact.UnmarshalBinary(latest.Raw) != nil || exact.Hash() != stored.Hash ||
		!bytes.Equal(latest.Raw, mustRaw(&signed)) {
		_ = s.finishBroadcast(ctx, id, claim.token, endpoint.Alias(), "SEND", "HASH_MISMATCH",
			Blocked, "SIGNED_ATTEMPT_INCONSISTENT", stored.Hash, policy.Backoff)
		return BroadcastResult{}, &SigningError{Code: "SIGNED_ATTEMPT_INCONSISTENT"}
	}
	if stored.AttemptNumber > 1 {
		winner, receiptErr := s.ObserveWinner(ctx, id, eligible, policy)
		if receiptErr != nil {
			return BroadcastResult{}, receiptErr
		}
		if winner > 0 {
			return s.BroadcastStatus(ctx, id)
		}
	}
	count, err := s.prepareSend(ctx, id, claim.token, endpoint.Alias(), stored.Hash)
	if err != nil {
		return BroadcastResult{}, err
	}
	rpcCtx, cancel := context.WithTimeout(ctx, policy.RPCDeadline)
	returned, sendErr := endpoint.SendRawTransaction(rpcCtx, latest.Raw)
	cancel()
	if hooks.afterRPC != nil {
		hooks.afterRPC()
	}
	result := ClassifySend(sendErr)
	state, reason := Uncertain, ""
	switch result {
	case ResultSuccess:
		if returned != stored.Hash {
			result, state, reason = ResultHashMismatch, Blocked, "RPC_HASH_MISMATCH"
		} else {
			state = Pending
		}
	case ResultAlreadyKnown, ResultNonce:
		// An error string is only a hint; verify the exact hash on a read path.
		found, conflict, err := s.observeClaim(ctx, id, claim.token, stored, &signed, eligible, policy)
		if errors.Is(err, ErrReceiptObserved) {
			return s.BroadcastStatus(ctx, id)
		}
		if err != nil {
			return BroadcastResult{}, err
		}
		if found {
			return BroadcastResult{State: Pending, Hash: stored.Hash, SendCount: count,
				LastOutcome: result}, nil
		} else if conflict {
			return BroadcastResult{State: Blocked, Hash: stored.Hash, SendCount: count,
				LastOutcome: result}, nil
		}
	case ResultFee:
		state, reason = Blocked, "FEE_REJECTION"
	case ResultRejected:
		state, reason = Blocked, "DETERMINISTIC_REJECTION"
	}
	if err := s.finishBroadcast(ctx, id, claim.token, endpoint.Alias(), "SEND", string(result),
		state, reason, stored.Hash, policy.Backoff); err != nil {
		return BroadcastResult{}, err
	}
	if hooks.afterCommit != nil {
		hooks.afterCommit()
	}
	return BroadcastResult{State: state, Hash: stored.Hash, SendCount: count, LastOutcome: result}, nil
}

func mustRaw(tx *types.Transaction) []byte {
	raw, _ := tx.MarshalBinary()
	return raw
}

func (s *Store) claimBroadcast(ctx context.Context, id string, hash common.Hash,
	policy BroadcastPolicy) (broadcastClaim, State, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return broadcastClaim{}, "", err
	}
	defer tx.Rollback(ctx)
	var state State
	var active bool
	var count int
	var first, due sql.NullTime
	var savedHash []byte
	err = tx.QueryRow(ctx, `SELECT i.state,COALESCE(i.lease_expires_at>clock_timestamp(),false),
		i.broadcast_send_count,i.broadcast_first_claimed_at,i.next_reconcile_at,a.transaction_hash
		FROM transaction_intents i JOIN transaction_attempts a
		ON a.intent_id=i.intent_id AND a.attempt_number=i.active_attempt_number
		WHERE i.intent_id=$1::uuid FOR UPDATE OF i`, id).
		Scan(&state, &active, &count, &first, &due, &savedHash)
	if err != nil {
		return broadcastClaim{}, "", err
	}
	if !bytes.Equal(savedHash, hash.Bytes()) {
		return broadcastClaim{}, "", ErrTransition
	}
	if state == Pending {
		return broadcastClaim{count: count}, Pending, nil
	}
	if state != Ready && state != Uncertain {
		return broadcastClaim{}, state, ErrTransition
	}
	if active {
		return broadcastClaim{}, state, ErrBroadcastInProgress
	}
	if state == Uncertain && due.Valid && time.Now().Before(due.Time) {
		return broadcastClaim{}, state, ErrBroadcastBackoff
	}
	token, err := newID()
	if err != nil {
		return broadcastClaim{}, "", err
	}
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state='UNCERTAIN',
		broadcast_first_claimed_at=COALESCE(broadcast_first_claimed_at,clock_timestamp()),
		lease_token=$2::uuid,lease_expires_at=clock_timestamp()+$3::interval,
		next_reconcile_at=NULL,version=version+1,updated_at=now()
		WHERE intent_id=$1::uuid`, id, token, interval(policy.Lease))
	if err != nil {
		return broadcastClaim{}, "", err
	}
	if state == Ready {
		if _, err = tx.Exec(ctx, `UPDATE transaction_attempts SET status='BROADCAST_CLAIMED'
			WHERE intent_id=$1::uuid AND attempt_number=(SELECT active_attempt_number
			FROM transaction_intents WHERE intent_id=$1::uuid) AND status='SIGNED'`, id); err != nil {
			return broadcastClaim{}, "", err
		}
	}
	if err = insertObservation(ctx, tx, id, "claim", "CLAIM", "CLAIMED", hash); err != nil {
		return broadcastClaim{}, "", err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type)
		VALUES($1::uuid,'BROADCAST_CLAIMED')`, id); err != nil {
		return broadcastClaim{}, "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return broadcastClaim{}, "", err
	}
	start := time.Now()
	if first.Valid {
		start = first.Time
	}
	return broadcastClaim{token: token, count: count, first: start, wasReady: state == Ready}, Uncertain, nil
}

func interval(d time.Duration) string { return fmt.Sprintf("%d milliseconds", d.Milliseconds()) }

func insertObservation(ctx context.Context, tx pgx.Tx, id, endpoint, kind, class string, hash common.Hash) error {
	_, err := tx.Exec(ctx, `INSERT INTO broadcast_observations
		(intent_id,attempt_number,endpoint,kind,classification,transaction_hash)
		SELECT i.intent_id,i.active_attempt_number,$2,$3,$4,$5 FROM transaction_intents i
		WHERE i.intent_id=$1::uuid`, id, endpoint, kind, class, hash.Bytes())
	return err
}

func (s *Store) prepareSend(ctx context.Context, id, token, endpoint string, hash common.Hash) (int, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	var count int
	err = tx.QueryRow(ctx, `UPDATE transaction_intents SET broadcast_send_count=broadcast_send_count+1,
		version=version+1,updated_at=now() WHERE intent_id=$1::uuid AND state='UNCERTAIN'
		AND lease_token=$2::uuid AND lease_expires_at>clock_timestamp()
		RETURNING broadcast_send_count`, id, token).Scan(&count)
	if err != nil {
		return 0, ErrBroadcastInProgress
	}
	if err = insertObservation(ctx, tx, id, endpoint, "CLAIM", "CLAIMED", hash); err != nil {
		return 0, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type)
		VALUES($1::uuid,'BROADCAST_SEND_PREPARED')`, id); err != nil {
		return 0, err
	}
	return count, tx.Commit(ctx)
}

func (s *Store) finishBroadcast(ctx context.Context, id, token, endpoint, kind, class string,
	state State, reason string, hash common.Hash, backoff time.Duration) error {
	// Persist even if the request deadline expired after the network call.
	commitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx, err := s.db.Begin(commitCtx)
	if err != nil {
		return err
	}
	defer tx.Rollback(commitCtx)
	var chain string
	var signerAddress []byte
	if err = tx.QueryRow(commitCtx, `SELECT chain_id::text,signer_address
		FROM transaction_intents WHERE intent_id=$1::uuid`, id).Scan(&chain, &signerAddress); err != nil {
		return err
	}
	// Match the account -> intent lock order used by nonce allocation.
	if _, err = tx.Exec(commitCtx, `SELECT 1 FROM signer_accounts
		WHERE chain_id=$1::numeric AND signer_address=$2 FOR UPDATE`, chain, signerAddress); err != nil {
		return err
	}
	var current State
	var currentToken sql.NullString
	err = tx.QueryRow(commitCtx, `SELECT state,lease_token::text FROM transaction_intents
		WHERE intent_id=$1::uuid FOR UPDATE`, id).Scan(&current, &currentToken)
	if err != nil {
		return err
	}
	if current != Uncertain || !currentToken.Valid || currentToken.String != token {
		return ErrBroadcastInProgress
	}
	if err = insertObservation(commitCtx, tx, id, endpoint, kind, class, hash); err != nil {
		return err
	}
	var resume any
	if state == Blocked {
		resume = string(Uncertain)
	}
	var due any
	if state == Uncertain {
		due = interval(backoff)
	}
	_, err = tx.Exec(commitCtx, `UPDATE transaction_intents SET state=$2,resume_state=$3,
		reason_code=NULLIF($4,''),lease_token=NULL,lease_expires_at=NULL,
		next_reconcile_at=CASE WHEN $5::text IS NULL THEN NULL
		ELSE clock_timestamp()+$5::interval END,version=version+1,updated_at=now()
		WHERE intent_id=$1::uuid`, id, state, resume, reason, due)
	if err != nil {
		return err
	}
	if state == Pending {
		if _, err = tx.Exec(commitCtx, `UPDATE transaction_attempts SET status='OBSERVED'
			WHERE intent_id=$1::uuid AND attempt_number=(SELECT active_attempt_number
			FROM transaction_intents WHERE intent_id=$1::uuid)`, id); err != nil {
			return err
		}
	}
	if state == Blocked {
		if _, err = tx.Exec(commitCtx, `UPDATE signer_accounts SET paused_reason='INTENT_BLOCKED',
			updated_at=now() WHERE chain_id=$1::numeric AND signer_address=$2
			AND (paused_reason IS NULL OR paused_reason='INTENT_BLOCKED')`, chain, signerAddress); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(commitCtx, `INSERT INTO audit_events(intent_id,event_type,reason_code,
		evidence) VALUES($1::uuid,'BROADCAST_OUTCOME',NULLIF($2,''),
		jsonb_build_object('endpoint',$3::text,'classification',$4::text))`,
		id, reason, endpoint, class); err != nil {
		return err
	}
	return tx.Commit(commitCtx)
}

// observeClaim uses the exact signed identity. A null lookup is only a local
// observation; a latest nonce above ours without matching evidence blocks.
func (s *Store) observeClaim(ctx context.Context, id, token string, stored SignedAttempt,
	signed *types.Transaction, endpoints []BroadcastEndpoint, policy BroadcastPolicy) (bool, bool, error) {
	for _, endpoint := range endpoints {
		readCtx, cancel := context.WithTimeout(ctx, policy.RPCDeadline)
		found, err := endpoint.TransactionByHash(readCtx, stored.Hash)
		cancel()
		if err == nil && found != nil {
			if found.Hash() != stored.Hash || !bytes.Equal(mustRaw(found), stored.Raw) {
				return false, true, s.finishBroadcast(ctx, id, token, endpoint.Alias(), "LOOKUP",
					"CONFLICT", Blocked, "RPC_INCONSISTENT", stored.Hash, policy.Backoff)
			}
			return true, false, s.finishBroadcast(ctx, id, token, endpoint.Alias(), "LOOKUP",
				"FOUND_TRANSACTION", Pending, "", stored.Hash, policy.Backoff)
		}
		readCtx, cancel = context.WithTimeout(ctx, policy.RPCDeadline)
		receipt, receiptErr := endpoint.ReceiptByHash(readCtx, stored.Hash)
		cancel()
		if receiptErr == nil && receipt != nil {
			if receipt.Hash != stored.Hash || receipt.BlockHash == (common.Hash{}) {
				return false, true, s.finishBroadcast(ctx, id, token, endpoint.Alias(), "RECEIPT",
					"CONFLICT", Blocked, "RPC_INCONSISTENT", stored.Hash, policy.Backoff)
			}
			return true, false, s.finishBroadcast(ctx, id, token, endpoint.Alias(), "RECEIPT",
				"FOUND_RECEIPT", Pending, "", stored.Hash, policy.Backoff)
		}
	}
	sender, err := types.Sender(types.NewLondonSigner(signed.ChainId()), signed)
	if err != nil {
		return false, false, err
	}
	for _, endpoint := range endpoints {
		readCtx, cancel := context.WithTimeout(ctx, policy.RPCDeadline)
		latest, nonceErr := endpoint.LatestNonce(readCtx, sender)
		cancel()
		if nonceErr == nil && latest > signed.Nonce() {
			if stored.AttemptNumber > 1 {
				winner, receiptErr := s.ObserveWinner(ctx, id, endpoints, policy)
				if receiptErr == nil && winner > 0 {
					return false, false, ErrReceiptObserved
				}
			}
			return false, true, s.finishBroadcast(ctx, id, token, endpoint.Alias(), "NONCE",
				"CONFLICT", Blocked, "EXTERNAL_NONCE_CONFLICT", stored.Hash, policy.Backoff)
		}
	}
	return false, false, nil
}

// BroadcastStatus is a read-only projection; it never includes raw bytes.
func (s *Store) BroadcastStatus(ctx context.Context, id string) (BroadcastResult, error) {
	var state State
	var count int
	var hash []byte
	var winning sql.NullInt64
	var winnerHash []byte
	var last sql.NullString
	err := s.db.QueryRow(ctx, `SELECT i.state,i.broadcast_send_count,a.transaction_hash,
		i.winning_attempt_number,w.transaction_hash,
		(SELECT classification FROM broadcast_observations b WHERE b.intent_id=i.intent_id
		 AND b.attempt_number=i.active_attempt_number
		 ORDER BY observation_id DESC LIMIT 1) FROM transaction_intents i
		JOIN transaction_attempts a ON a.intent_id=i.intent_id AND a.attempt_number=i.active_attempt_number
		LEFT JOIN transaction_attempts w ON w.intent_id=i.intent_id
		 AND w.attempt_number=i.winning_attempt_number
		WHERE i.intent_id=$1::uuid`, id).Scan(&state, &count, &hash, &winning, &winnerHash, &last)
	if err != nil {
		return BroadcastResult{}, err
	}
	if len(hash) != common.HashLength {
		return BroadcastResult{}, ErrTransition
	}
	result := BroadcastResult{State: state, SendCount: count, Hash: common.BytesToHash(hash),
		LastOutcome: ResultClass(last.String)}
	if winning.Valid && len(winnerHash) == common.HashLength {
		result.WinnerAttemptNumber = int(winning.Int64)
		result.WinnerHash = common.BytesToHash(winnerHash)
	}
	return result, nil
}
