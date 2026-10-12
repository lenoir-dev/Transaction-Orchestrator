package orchestrator

import (
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

var ErrReceiptBackoff = errors.New("receipt reconciliation is not due")

// ReceiptPolicy is an explicit product policy. A count is not consensus finality.
type ReceiptPolicy struct {
	Now           time.Time
	Interval      time.Duration
	RPCDeadline   time.Duration
	TotalDeadline time.Duration
	MaxReorgDepth int
	MaxAttempts   int
	GenesisHash   common.Hash
}

func (p ReceiptPolicy) valid() bool {
	return !p.Now.IsZero() && p.Interval >= 0 && p.RPCDeadline > 0 &&
		p.TotalDeadline > p.RPCDeadline && p.MaxReorgDepth >= 1 && p.MaxReorgDepth <= 1000 &&
		p.MaxAttempts >= 1 && p.MaxAttempts <= 101
}

// ReceiptConfirmations includes the receipt block and bounds operational counts
// before addition, including a uint64 head at the numeric limit.
func ReceiptConfirmations(head, block uint64) (int, error) {
	if head < block || head-block >= 1000000000 {
		return 0, ErrEvidenceUnavailable
	}
	return int(head - block + 1), nil
}

func receiptReorgDepth(previous int, head, block uint64) uint64 {
	depth := uint64(previous)
	if head >= block {
		distance := head - block
		if distance == ^uint64(0) {
			return distance
		}
		if distance+1 > depth {
			depth = distance + 1
		}
	}
	return depth
}

type ReceiptRecord struct {
	AttemptID         string
	AttemptNumber     int
	Hash              common.Hash
	BlockNumber       uint64
	BlockHash         common.Hash
	Status            uint64
	TransactionIndex  uint64
	GasUsed           uint64
	EffectiveGasPrice string
	Verification      string
	Confirmations     int
	VerifiedHead      uint64
}

type ConfirmationStatus struct {
	State                State
	Reason               string
	ActiveAttemptNumber  int
	WinningAttemptNumber int
	WinnerHash           common.Hash
	Confirmations        int
	Required             int
	VerifiedHead         uint64
	ReceiptBlockNumber   uint64
	ReceiptBlockHash     common.Hash
	ExecutionStatus      uint64
	ExecutionOutcome     string
	MaxReorgDepth        int
	ReorgEvents          int
}

func (s *Store) ConfirmationStatus(ctx context.Context, id string) (ConfirmationStatus, error) {
	var out ConfirmationStatus
	var winner sql.NullInt64
	var head sql.NullString
	var max sql.NullInt64
	var hash, blockHash []byte
	var block sql.NullString
	var status sql.NullInt64
	err := s.db.QueryRow(ctx, `SELECT i.state,COALESCE(i.reason_code,''),COALESCE(i.active_attempt_number,0),i.winning_attempt_number,
        i.confirmation_count,i.confirmation_depth,i.receipt_verified_head::text,i.max_reorg_depth,
        a.transaction_hash,r.block_number::text,r.block_hash,r.execution_status,
        (SELECT count(*) FROM audit_events e WHERE e.intent_id=i.intent_id AND e.event_type IN ('RECEIPT_REORGED','DEEP_REORG_BLOCKED'))
        FROM transaction_intents i LEFT JOIN transaction_attempts a ON a.intent_id=i.intent_id
        AND a.attempt_number=i.winning_attempt_number
        LEFT JOIN LATERAL (SELECT block_number,block_hash,execution_status FROM observed_receipts
            WHERE intent_id=i.intent_id AND verification_status='CANONICAL'
            AND attempt_number=i.winning_attempt_number ORDER BY receipt_id DESC LIMIT 1) r ON true
        WHERE i.intent_id=$1::uuid`, id).Scan(&out.State, &out.Reason, &out.ActiveAttemptNumber, &winner,
		&out.Confirmations, &out.Required, &head, &max, &hash, &block, &blockHash, &status, &out.ReorgEvents)
	if err != nil {
		return out, err
	}
	if winner.Valid {
		out.WinningAttemptNumber = int(winner.Int64)
	}
	if len(hash) == 32 {
		out.WinnerHash = common.BytesToHash(hash)
	}
	if head.Valid {
		fmt.Sscan(head.String, &out.VerifiedHead)
	}
	if block.Valid {
		fmt.Sscan(block.String, &out.ReceiptBlockNumber)
	}
	if len(blockHash) == 32 {
		out.ReceiptBlockHash = common.BytesToHash(blockHash)
	}
	if status.Valid {
		out.ExecutionStatus = uint64(status.Int64)
		out.ExecutionOutcome = "REVERTED"
		if out.ExecutionStatus == 1 {
			out.ExecutionOutcome = "SUCCEEDED"
		}
	} else {
		out.ExecutionOutcome = "UNKNOWN"
	}
	if max.Valid {
		out.MaxReorgDepth = int(max.Int64)
	}
	return out, nil
}

func (s *Store) ReceiptHistory(ctx context.Context, id string) ([]ReceiptRecord, error) {
	rows, err := s.db.Query(ctx, `SELECT r.attempt_id::text,r.attempt_number,r.transaction_hash,
        r.block_number::text,r.block_hash,r.execution_status,
        COALESCE(r.transaction_index,0)::text,COALESCE(r.gas_used,0)::text,
        COALESCE(r.effective_gas_price::text,''),r.verification_status,r.confirmations,
        COALESCE(r.verified_head,0)::text FROM observed_receipts r WHERE intent_id=$1::uuid ORDER BY receipt_id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var history []ReceiptRecord
	for rows.Next() {
		var r ReceiptRecord
		var hash, block []byte
		var number, index, gas, head string
		if err = rows.Scan(&r.AttemptID, &r.AttemptNumber, &hash, &number, &block, &r.Status,
			&index, &gas, &r.EffectiveGasPrice, &r.Verification, &r.Confirmations, &head); err != nil {
			return nil, err
		}
		r.Hash = common.BytesToHash(hash)
		r.BlockHash = common.BytesToHash(block)
		fmt.Sscan(number, &r.BlockNumber)
		fmt.Sscan(index, &r.TransactionIndex)
		fmt.Sscan(gas, &r.GasUsed)
		fmt.Sscan(head, &r.VerifiedHead)
		history = append(history, r)
	}
	return history, rows.Err()
}

type receiptSnapshot struct {
	state        State
	version      int64
	chain        string
	address      []byte
	depth        int
	nonce        uint64
	max          sql.NullInt64
	next         sql.NullTime
	verifiedHead uint64
	winner       int
	prior        *ReceiptRecord
	attempts     []AttemptRecord
}

func (s *Store) loadReceiptSnapshot(ctx context.Context, id string) (receiptSnapshot, error) {
	var v receiptSnapshot
	var winner sql.NullInt64
	var head sql.NullString
	var nonce string
	err := s.db.QueryRow(ctx, `SELECT state,version,chain_id::text,signer_address,confirmation_depth,
        max_reorg_depth,receipt_next_poll_at,receipt_verified_head::text,winning_attempt_number,
 (SELECT nonce::text FROM nonce_reservations r WHERE r.intent_id=i.intent_id)
        FROM transaction_intents i WHERE intent_id=$1::uuid`, id).Scan(&v.state, &v.version, &v.chain, &v.address,
		&v.depth, &v.max, &v.next, &head, &winner, &nonce)
	if err != nil {
		return v, err
	}
	fmt.Sscan(nonce, &v.nonce)
	if head.Valid {
		fmt.Sscan(head.String, &v.verifiedHead)
	}
	if winner.Valid {
		v.winner = int(winner.Int64)
	}
	v.attempts, err = s.AttemptHistory(ctx, id)
	if err != nil {
		return v, err
	}
	if v.winner > 0 {
		var r ReceiptRecord
		var hash, block []byte
		var number string
		err = s.db.QueryRow(ctx, `SELECT attempt_id::text,attempt_number,transaction_hash,
            block_number::text,block_hash,execution_status,verification_status,confirmations
            FROM observed_receipts WHERE intent_id=$1::uuid AND attempt_number=$2
            AND verification_status IN ('CANONICAL','UNVERIFIED') ORDER BY receipt_id DESC LIMIT 1`, id, v.winner).
			Scan(&r.AttemptID, &r.AttemptNumber, &hash, &number, &block, &r.Status, &r.Verification, &r.Confirmations)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return v, err
		}
		if err == nil {
			r.Hash = common.BytesToHash(hash)
			r.BlockHash = common.BytesToHash(block)
			fmt.Sscan(number, &r.BlockNumber)
			v.prior = &r
		}
	}
	return v, nil
}

type candidateReceipt struct {
	attempt  AttemptRecord
	receipt  ReceiptEvidence
	block    BlockEvidence
	head     BlockEvidence
	endpoint string
}

type receiptHooks struct {
	afterReceipt    func()
	beforeCommit    func()
	afterCommit     func()
	afterReorgWrite func()
}

type receiptRead struct {
	head       BlockEvidence
	old        *BlockEvidence
	candidates []candidateReceipt
	endpoint   string
	nonce      uint64
	visible    bool
}

func readWithDeadline[T any](ctx context.Context, timeout time.Duration, fn func(context.Context) (T, error)) (T, error) {
	call, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(call)
}

func readReceiptEndpoint(ctx context.Context, ep BroadcastEndpoint, snap receiptSnapshot, p ReceiptPolicy) (receiptRead, error) {
	var out receiptRead
	out.endpoint = ep.Alias()
	chain, err := readWithDeadline(ctx, p.RPCDeadline, ep.ChainID)
	if err != nil {
		return out, err
	}
	if chain == nil || chain.String() != snap.chain {
		return out, ErrNoEligibleEndpoint
	}
	if p.GenesisHash != (common.Hash{}) {
		genesis, e := readWithDeadline(ctx, p.RPCDeadline, ep.Genesis)
		if e != nil || genesis != p.GenesisHash {
			return out, ErrNoEligibleEndpoint
		}
	}
	head, err := readWithDeadline(ctx, p.RPCDeadline, ep.LatestBlock)
	if err != nil || head == nil || head.Hash == (common.Hash{}) {
		return out, ErrEvidenceUnavailable
	}
	out.head = *head
	if head.Number < snap.verifiedHead {
		return out, ErrEvidenceUnavailable
	}
	if snap.prior != nil && head.Number >= snap.prior.BlockNumber {
		old, e := readWithDeadline(ctx, p.RPCDeadline, func(c context.Context) (*BlockEvidence, error) { return ep.BlockByNumber(c, snap.prior.BlockNumber) })
		if e != nil || old == nil || old.Number != snap.prior.BlockNumber {
			return out, ErrEvidenceUnavailable
		}
		out.old = old
	}
	for _, attempt := range snap.attempts {
		if attempt.Hash == (common.Hash{}) {
			continue
		}
		receipt, e := readWithDeadline(ctx, p.RPCDeadline, func(c context.Context) (*ReceiptEvidence, error) { return ep.ReceiptByHash(c, attempt.Hash) })
		if e != nil {
			return out, ErrEvidenceUnavailable
		}
		if receipt == nil {
			seen, lookupErr := readWithDeadline(ctx, p.RPCDeadline, func(c context.Context) (*types.Transaction, error) { return ep.TransactionByHash(c, attempt.Hash) })
			if lookupErr != nil {
				return out, ErrEvidenceUnavailable
			}
			if seen != nil {
				if seen.Hash() != attempt.Hash {
					return out, ErrEvidenceUnavailable
				}
				out.visible = true
			}
			continue
		}
		if receipt.Hash != attempt.Hash || receipt.BlockHash == (common.Hash{}) || receipt.BlockNumber > head.Number || receipt.Status > 1 {
			return out, ErrEvidenceUnavailable
		}
		byHash, e := readWithDeadline(ctx, p.RPCDeadline, func(c context.Context) (*BlockEvidence, error) { return ep.BlockByHash(c, receipt.BlockHash) })
		if e != nil || byHash == nil || byHash.Hash != receipt.BlockHash || byHash.Number != receipt.BlockNumber ||
			receipt.TransactionIndex >= uint64(len(byHash.Transactions)) || byHash.Transactions[receipt.TransactionIndex] != attempt.Hash {
			return out, ErrEvidenceUnavailable
		}
		canonical, e := readWithDeadline(ctx, p.RPCDeadline, func(c context.Context) (*BlockEvidence, error) { return ep.BlockByNumber(c, receipt.BlockNumber) })
		if e != nil || canonical == nil || canonical.Number != receipt.BlockNumber {
			return out, ErrEvidenceUnavailable
		}
		if canonical.Hash != byHash.Hash {
			continue
		} // An orphan is evidence, never a winner.
		out.candidates = append(out.candidates, candidateReceipt{attempt, *receipt, *byHash, *head, ep.Alias()})
	}
	nonce, e := readWithDeadline(ctx, p.RPCDeadline, func(c context.Context) (uint64, error) { return ep.LatestNonce(c, common.BytesToAddress(snap.address)) })
	if e != nil {
		return out, ErrEvidenceUnavailable
	}
	out.nonce = nonce
	stable, e := readWithDeadline(ctx, p.RPCDeadline, func(c context.Context) (*BlockEvidence, error) { return ep.BlockByNumber(c, head.Number) })
	if e != nil || stable == nil || stable.Number != head.Number || stable.Hash != head.Hash {
		return out, ErrEvidenceUnavailable
	}
	for _, c := range out.candidates {
		check, e := readWithDeadline(ctx, p.RPCDeadline, func(ctx context.Context) (*BlockEvidence, error) { return ep.BlockByNumber(ctx, c.block.Number) })
		if e != nil || check == nil || check.Number != c.block.Number || check.Hash != c.block.Hash {
			return out, ErrEvidenceUnavailable
		}
	}
	return out, nil
}

// ReconcileReceipts performs one bounded, restart-safe poll. No RPC is called
// while holding a database lock. A version check fences a concurrent poll.
func (s *Store) ReconcileReceipts(ctx context.Context, id string, endpoints []BroadcastEndpoint, p ReceiptPolicy) (ConfirmationStatus, error) {
	ctx, span := otel.Tracer("transaction-orchestrator").Start(ctx, "receipt.reconcile")
	defer span.End()
	return s.reconcileReceipts(ctx, id, endpoints, p, receiptHooks{})
}

func (s *Store) reconcileReceipts(ctx context.Context, id string, endpoints []BroadcastEndpoint, p ReceiptPolicy, hooks receiptHooks) (ConfirmationStatus, error) {
	if !p.valid() || len(endpoints) == 0 || len(endpoints) > 8 {
		return ConfirmationStatus{}, ErrInvalid
	}
	work, cancel := context.WithTimeout(ctx, p.TotalDeadline)
	defer cancel()
	snap, err := s.loadReceiptSnapshot(work, id)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	if snap.state != Ready && snap.state != Signing && snap.state != Pending && snap.state != Uncertain && snap.state != Mined && snap.state != Confirming && snap.state != Confirmed && snap.state != Reorged {
		return ConfirmationStatus{}, ErrTransition
	}
	if snap.max.Valid && int(snap.max.Int64) != p.MaxReorgDepth {
		return ConfirmationStatus{}, ErrInvalid
	}
	if snap.next.Valid && p.Now.Before(snap.next.Time) {
		return ConfirmationStatus{}, ErrReceiptBackoff
	}
	if len(snap.attempts) > p.MaxAttempts {
		return ConfirmationStatus{}, ErrInvalid
	}
	var reads []receiptRead
	aliases := map[string]bool{}
	for _, ep := range endpoints {
		if ep == nil || !validAlias(ep.Alias()) || aliases[ep.Alias()] {
			return ConfirmationStatus{}, ErrInvalid
		}
		aliases[ep.Alias()] = true
		read, e := readReceiptEndpoint(work, ep, snap, p)
		if e == nil {
			reads = append(reads, read)
		}
	}
	if len(reads) == 0 {
		return s.receiptPollFailure(ctx, id, snap, p)
	}
	var selected *candidateReceipt
	var oldMismatch bool
	var newest uint64
	for _, read := range reads {
		if read.head.Number > newest {
			newest = read.head.Number
		}
		if snap.prior != nil && read.old != nil && read.old.Hash != snap.prior.BlockHash {
			oldMismatch = true
		}
		for i := range read.candidates {
			c := read.candidates[i]
			if selected != nil && (selected.attempt.AttemptNumber != c.attempt.AttemptNumber || selected.block.Hash != c.block.Hash) {
				return s.receiptPollFailure(ctx, id, snap, p)
			}
			if selected == nil || c.head.Number > selected.head.Number {
				copy := c
				selected = &copy
			}
		}
	}
	if snap.prior != nil {
		for _, read := range reads {
			if read.old != nil && (read.old.Hash != snap.prior.BlockHash) != oldMismatch {
				return s.receiptPollFailure(ctx, id, snap, p)
			}
		}
	}
	if selected != nil {
		for _, read := range reads {
			if read.head.Number < selected.block.Number {
				continue
			}
			if read.endpoint == selected.endpoint {
				continue
			}
			// A peer with the same height must agree on the canonical identity.
			peer, e := readWithDeadline(work, p.RPCDeadline, func(c context.Context) (*BlockEvidence, error) {
				for _, ep := range endpoints {
					if ep.Alias() == read.endpoint {
						return ep.BlockByNumber(c, selected.block.Number)
					}
				}
				return nil, ErrNoEligibleEndpoint
			})
			if e != nil || peer == nil || peer.Number != selected.block.Number || peer.Hash != selected.block.Hash {
				return s.receiptPollFailure(ctx, id, snap, p)
			}
		}
	}
	if oldMismatch {
		return s.commitReceiptReorg(work, id, snap, newest, p, hooks)
	}
	if selected != nil {
		return s.commitReceiptCanonical(work, id, snap, *selected, p, hooks)
	}
	if snap.state == Reorged {
		for _, read := range reads {
			if read.nonce > snap.nonce {
				if e := s.blockReplacement(work, id, "EXTERNAL_NONCE_CONFLICT"); e != nil {
					return ConfirmationStatus{}, e
				}
				return s.ConfirmationStatus(work, id)
			}
		}
		visible := false
		for _, read := range reads {
			visible = visible || read.visible
		}
		return s.commitReceiptPending(work, id, snap, p, visible)
	}
	return s.commitReceiptPoll(work, id, snap, p, "RECEIPT_NOT_OBSERVED")
}

func (s *Store) receiptTransaction(ctx context.Context, id string, snap receiptSnapshot) (pgx.Tx, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `SELECT 1 FROM signer_accounts WHERE chain_id=$1::numeric AND signer_address=$2 FOR UPDATE`, snap.chain, snap.address)
	if err != nil {
		tx.Rollback(ctx)
		return nil, err
	}
	var version int64
	err = tx.QueryRow(ctx, `SELECT version FROM transaction_intents WHERE intent_id=$1::uuid FOR UPDATE`, id).Scan(&version)
	if err != nil || version != snap.version {
		tx.Rollback(ctx)
		return nil, ErrTransition
	}
	return tx, nil
}

func (s *Store) commitReceiptCanonical(ctx context.Context, id string, snap receiptSnapshot, c candidateReceipt, p ReceiptPolicy, hooks receiptHooks) (ConfirmationStatus, error) {
	tx, err := s.receiptTransaction(ctx, id, snap)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	defer tx.Rollback(ctx)
	if snap.prior != nil && snap.prior.BlockHash == c.block.Hash && snap.prior.Status != c.receipt.Status {
		return ConfirmationStatus{}, ErrEvidenceUnavailable
	}
	if snap.winner > 0 && snap.winner != c.attempt.AttemptNumber {
		return ConfirmationStatus{}, ErrEvidenceUnavailable
	}
	count, countErr := ReceiptConfirmations(c.head.Number, c.receipt.BlockNumber)
	if countErr != nil {
		return ConfirmationStatus{}, countErr
	}
	state := Mined
	if int(count) >= snap.depth {
		state = Confirmed
	} else if snap.prior != nil && snap.prior.Verification == "CANONICAL" {
		state = Confirming
	}
	for _, block := range []BlockEvidence{c.block, c.head} {
		_, err = tx.Exec(ctx, `INSERT INTO observed_blocks(chain_id,block_number,block_hash,parent_hash,canonical)
            VALUES($1::numeric,$2::numeric,$3,$4,true) ON CONFLICT(chain_id,block_number,block_hash)
            DO UPDATE SET canonical=true,last_verified_at=clock_timestamp()`, snap.chain, block.Number, block.Hash.Bytes(), block.ParentHash.Bytes())
		if err != nil {
			return ConfirmationStatus{}, err
		}
	}
	// The old row remains an immutable inclusion observation; progress updates
	// only verification metadata for the same block identity.
	var existing int64
	err = tx.QueryRow(ctx, `SELECT receipt_id FROM observed_receipts WHERE intent_id=$1::uuid
        AND attempt_number=$2 AND block_number=$3::numeric AND block_hash=$4
        AND verification_status='CANONICAL' ORDER BY receipt_id DESC LIMIT 1`,
		id, c.attempt.AttemptNumber, c.block.Number, c.block.Hash.Bytes()).Scan(&existing)
	if errors.Is(err, pgx.ErrNoRows) {
		var price any
		if c.receipt.EffectiveGasPrice != "" {
			price = c.receipt.EffectiveGasPrice
		}
		_, err = tx.Exec(ctx, `INSERT INTO observed_receipts(intent_id,attempt_id,attempt_number,transaction_hash,
            block_number,block_hash,execution_status,canonical_observed,transaction_index,gas_used,
            effective_gas_price,verification_status,confirmations,verified_head,last_verified_at)
            VALUES($1::uuid,$2::uuid,$3,$4,$5::numeric,$6,$7,true,$8::numeric,$9::numeric,
            $10::numeric,'CANONICAL',$11,$12::numeric,clock_timestamp())`, id, c.attempt.AttemptID,
			c.attempt.AttemptNumber, c.receipt.Hash.Bytes(), c.receipt.BlockNumber, c.receipt.BlockHash.Bytes(),
			c.receipt.Status, c.receipt.TransactionIndex, c.receipt.GasUsed, price, count, c.head.Number)
	} else if err == nil {
		_, err = tx.Exec(ctx, `UPDATE observed_receipts SET canonical_observed=true,verification_status='CANONICAL',
            confirmations=$2,verified_head=$3::numeric,last_verified_at=clock_timestamp()
            WHERE receipt_id=$1`, existing, count, c.head.Number)
	}
	if err != nil {
		return ConfirmationStatus{}, err
	}
	if hooks.afterReceipt != nil {
		hooks.afterReceipt()
	}
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state=$2,winning_attempt_number=$3,
		lease_token=NULL,lease_expires_at=NULL,next_reconcile_at=NULL,
        confirmation_count=$4,receipt_verified_head=$5::numeric,max_reorg_depth=COALESCE(max_reorg_depth,$6),
        receipt_next_poll_at=$7,reason_code=NULL,version=version+1,updated_at=now()
        WHERE intent_id=$1::uuid`, id, state, c.attempt.AttemptNumber, count, c.head.Number, p.MaxReorgDepth, p.Now.Add(p.Interval))
	if err != nil {
		return ConfirmationStatus{}, err
	}
	event := "CONFIRMATION_PROGRESS"

	if state == Confirmed && snap.state != Confirmed {
		event = "TRANSACTION_CONFIRMED"
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type,evidence)
        VALUES($1::uuid,$2,jsonb_build_object('attempt_number',$3::integer,'block_number',$4::text,
        'block_hash',$5::text,'head',$6::text,'confirmations',$7::integer,'required',$8::integer,
        'execution_status',$9::integer,'endpoint',$10::text,'head_hash',$11::text))`, id, event, c.attempt.AttemptNumber,
		fmt.Sprint(c.block.Number), c.block.Hash.Hex(), fmt.Sprint(c.head.Number), count, snap.depth, c.receipt.Status, c.endpoint, c.head.Hash.Hex())
	if err != nil {
		return ConfirmationStatus{}, err
	}
	if snap.prior == nil || snap.prior.Verification != "CANONICAL" {
		for _, kind := range []string{"RECEIPT_OBSERVED", "RECEIPT_CANONICAL", "WINNER_CHANGED"} {
			_, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type,evidence)
                VALUES($1::uuid,$2,jsonb_build_object('attempt_number',$3::integer,'block_hash',$4::text))`, id, kind, c.attempt.AttemptNumber, c.block.Hash.Hex())
			if err != nil {
				return ConfirmationStatus{}, err
			}
		}
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err = tx.Commit(ctx); err != nil {
		return ConfirmationStatus{}, err
	}
	if hooks.afterCommit != nil {
		hooks.afterCommit()
	}
	return s.ConfirmationStatus(ctx, id)
}

func (s *Store) commitReceiptReorg(ctx context.Context, id string, snap receiptSnapshot, head uint64, p ReceiptPolicy, hooks receiptHooks) (ConfirmationStatus, error) {
	tx, err := s.receiptTransaction(ctx, id, snap)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	defer tx.Rollback(ctx)
	old := snap.prior
	depth := receiptReorgDepth(old.Confirmations, head, old.BlockNumber)
	deep := depth > uint64(p.MaxReorgDepth)
	_, err = tx.Exec(ctx, `UPDATE observed_receipts SET verification_status='ORPHANED',canonical_observed=false,
        last_verified_at=clock_timestamp() WHERE intent_id=$1::uuid AND attempt_number=$2
        AND block_hash=$3 AND verification_status IN ('CANONICAL','UNVERIFIED')`, id, old.AttemptNumber, old.BlockHash.Bytes())
	if err != nil {
		return ConfirmationStatus{}, err
	}
	_, err = tx.Exec(ctx, `UPDATE observed_blocks SET canonical=false,last_verified_at=clock_timestamp()
        WHERE chain_id=$1::numeric AND block_number=$2::numeric AND block_hash=$3`,
		snap.chain, old.BlockNumber, old.BlockHash.Bytes())
	if err != nil {
		return ConfirmationStatus{}, err
	}
	if hooks.afterReorgWrite != nil {
		hooks.afterReorgWrite()
	}
	state := Reorged
	var resume any
	reason := "RECEIPT_REORGED"
	if deep {
		state = Blocked
		resume = Reorged
		reason = "DEEP_REORG_BLOCKED"
	}
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state=$2,resume_state=$3,
        reason_code=$4,winning_attempt_number=NULL,confirmation_count=0,
        receipt_verified_head=$5::numeric,max_reorg_depth=COALESCE(max_reorg_depth,$6),
        receipt_next_poll_at=NULL,version=version+1,updated_at=now()
        WHERE intent_id=$1::uuid`, id, state, resume, reason, head, p.MaxReorgDepth)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	if deep {
		_, err = tx.Exec(ctx, `UPDATE signer_accounts SET paused_reason='DEEP_REORG_BLOCKED',updated_at=now()
            WHERE chain_id=$1::numeric AND signer_address=$2`, snap.chain, snap.address)
		if err != nil {
			return ConfirmationStatus{}, err
		}
	}
	event := "RECEIPT_REORGED"
	if deep {
		event = "DEEP_REORG_BLOCKED"
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type,reason_code,evidence)
        VALUES($1::uuid,$2,$3,jsonb_build_object('old_attempt',$4::integer,'old_block',$5::text,
        'old_hash',$6::text,'head',$7::text,'depth',$8::numeric))`, id, event, reason,
		old.AttemptNumber, fmt.Sprint(old.BlockNumber), old.BlockHash.Hex(), fmt.Sprint(head), fmt.Sprint(depth))
	if err != nil {
		return ConfirmationStatus{}, err
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err = tx.Commit(ctx); err != nil {
		return ConfirmationStatus{}, err
	}
	if hooks.afterCommit != nil {
		hooks.afterCommit()
	}
	return s.ConfirmationStatus(ctx, id)
}

func (s *Store) commitReceiptPending(ctx context.Context, id string, snap receiptSnapshot, p ReceiptPolicy, visible bool) (ConfirmationStatus, error) {
	tx, err := s.receiptTransaction(ctx, id, snap)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	defer tx.Rollback(ctx)
	state := Uncertain
	if visible {
		state = Pending
	}
	var activeStatus string
	err = tx.QueryRow(ctx, `SELECT a.status FROM transaction_intents i JOIN transaction_attempts a
        ON a.intent_id=i.intent_id AND a.attempt_number=i.active_attempt_number WHERE i.intent_id=$1::uuid`, id).Scan(&activeStatus)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	if activeStatus == "SIGNED" || activeStatus == "NOT_BROADCAST" {
		state = Ready
		_, err = tx.Exec(ctx, `UPDATE transaction_attempts SET status='SIGNED' WHERE intent_id=$1::uuid
            AND attempt_number=(SELECT active_attempt_number FROM transaction_intents WHERE intent_id=$1::uuid)`, id)
		if err != nil {
			return ConfirmationStatus{}, err
		}
	}
	if activeStatus == "PREPARING" {
		state = Signing
	}
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET state=$2,reason_code=NULL,
        receipt_next_poll_at=$3,max_reorg_depth=COALESCE(max_reorg_depth,$4),
        version=version+1,updated_at=now() WHERE intent_id=$1::uuid`, id, state, p.Now.Add(p.Interval), p.MaxReorgDepth)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type) VALUES($1::uuid,'REORG_RECONCILED')`, id)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ConfirmationStatus{}, err
	}
	return s.ConfirmationStatus(ctx, id)
}

// ConfirmationMetrics has bounded event names and no chain or transaction identifiers.
type ConfirmationMetric struct {
	Name   string
	Result string
	Count  int64
}

func (s *Store) ConfirmationMetrics(ctx context.Context) ([]ConfirmationMetric, error) {
	rows, err := s.db.Query(ctx, `SELECT name,result,count(*) FROM (
        SELECT 'txorch_receipt_observations_total' AS name,event_type AS result FROM audit_events
            WHERE event_type IN ('RECEIPT_OBSERVED','CONFIRMATION_PROGRESS','RECEIPT_REORGED','RECEIPT_NOT_OBSERVED','RECEIPT_EVIDENCE_UNAVAILABLE','DEEP_REORG_BLOCKED')
        UNION ALL SELECT 'txorch_confirmed_total', 'INCLUDED' FROM audit_events WHERE event_type='TRANSACTION_CONFIRMED'
        UNION ALL SELECT 'txorch_reorged_receipts_total','REORGED' FROM audit_events WHERE event_type IN ('RECEIPT_REORGED','DEEP_REORG_BLOCKED')
        ) events GROUP BY name,result ORDER BY name,result`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ConfirmationMetric
	for rows.Next() {
		var m ConfirmationMetric
		if err = rows.Scan(&m.Name, &m.Result, &m.Count); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	var confirming int64
	if err = s.db.QueryRow(ctx, `SELECT count(*) FROM transaction_intents WHERE state='CONFIRMING'`).Scan(&confirming); err != nil {
		return nil, err
	}
	out = append(out, ConfirmationMetric{Name: "txorch_confirming", Result: "CURRENT", Count: confirming})
	return out, nil
}

func (s *Store) commitReceiptPoll(ctx context.Context, id string, snap receiptSnapshot, p ReceiptPolicy, event string) (ConfirmationStatus, error) {
	tx, err := s.receiptTransaction(ctx, id, snap)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET receipt_next_poll_at=$2,
        max_reorg_depth=COALESCE(max_reorg_depth,$3),version=version+1 WHERE intent_id=$1::uuid`, id, p.Now.Add(p.Interval), p.MaxReorgDepth)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO audit_events(intent_id,event_type) VALUES($1::uuid,$2)`, id, event)
	if err != nil {
		return ConfirmationStatus{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ConfirmationStatus{}, err
	}
	return s.ConfirmationStatus(ctx, id)
}

// ReceiptReady reports the durable incident flag for readiness integrations.
func (s *Store) ReceiptReady(ctx context.Context) error {
	var deep bool
	if err := s.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM signer_accounts WHERE paused_reason='DEEP_REORG_BLOCKED')`).Scan(&deep); err != nil {
		return err
	}
	if deep {
		return errors.New("deep reorg requires operator reconciliation")
	}
	return nil
}

// ConfirmationHistogram supplies bounded buckets for txorch_confirmation_seconds.
// Duration starts at the first observation of the current inclusion block.
type ConfirmationBucket struct {
	UpperBoundSeconds float64
	Count             int64
}
type ConfirmationHistogram struct {
	Count      int64
	SumSeconds float64
	Buckets    []ConfirmationBucket
}

func (s *Store) ConfirmationHistogram(ctx context.Context) (ConfirmationHistogram, error) {
	var h ConfirmationHistogram
	var buckets [6]int64
	err := s.db.QueryRow(ctx, `WITH durations AS (
        SELECT GREATEST(0,extract(epoch FROM e.occurred_at-r.observed_at))::float8 AS seconds
        FROM audit_events e JOIN LATERAL (SELECT min(observed_at) AS observed_at FROM observed_receipts
        WHERE intent_id=e.intent_id AND encode(block_hash,'hex')=substr(e.evidence->>'block_hash',3)) r ON true
        WHERE e.event_type='TRANSACTION_CONFIRMED' AND r.observed_at IS NOT NULL)
        SELECT count(*),COALESCE(sum(seconds),0),count(*) FILTER(WHERE seconds<=1),
        count(*) FILTER(WHERE seconds<=5),count(*) FILTER(WHERE seconds<=15),
        count(*) FILTER(WHERE seconds<=60),count(*) FILTER(WHERE seconds<=300),
        count(*) FILTER(WHERE seconds<=3600) FROM durations`).Scan(&h.Count, &h.SumSeconds,
		&buckets[0], &buckets[1], &buckets[2], &buckets[3], &buckets[4], &buckets[5])
	if err != nil {
		return h, err
	}
	for i, bound := range []float64{1, 5, 15, 60, 300, 3600} {
		h.Buckets = append(h.Buckets, ConfirmationBucket{bound, buckets[i]})
	}
	return h, nil
}

// ReceiptWork includes confirmed inclusions so a later supported reorg remains
// observable after restart. The caller bounds worker concurrency and chain policy.
func (s *Store) ReceiptWork(ctx context.Context, now time.Time, limit int) ([]string, error) {
	if now.IsZero() || limit < 1 || limit > 1000 {
		return nil, ErrInvalid
	}
	rows, err := s.db.Query(ctx, `SELECT i.intent_id::text FROM transaction_intents i
        WHERE i.state IN ('READY','SIGNING','UNCERTAIN','PENDING','MINED','CONFIRMING','CONFIRMED','REORGED')
        AND (receipt_next_poll_at IS NULL OR receipt_next_poll_at<=$1)
        AND EXISTS(SELECT 1 FROM transaction_attempts a WHERE a.intent_id=i.intent_id AND a.raw_tx IS NOT NULL)
        ORDER BY COALESCE(receipt_next_poll_at,created_at),i.intent_id LIMIT $2`, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) receiptPollFailure(ctx context.Context, id string, snap receiptSnapshot, p ReceiptPolicy) (ConfirmationStatus, error) {
	persist, cancel := context.WithTimeout(ctx, p.RPCDeadline)
	defer cancel()
	status, err := s.commitReceiptPoll(persist, id, snap, p, "RECEIPT_EVIDENCE_UNAVAILABLE")
	if err != nil {
		return status, err
	}
	return status, ErrEvidenceUnavailable
}
