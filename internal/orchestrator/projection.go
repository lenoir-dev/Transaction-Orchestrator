package orchestrator

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

type IntentProjection struct {
	IntentID       string              `json:"intent_id"`
	State          State               `json:"state"`
	ReasonCode     string              `json:"reason_code,omitempty"`
	ChainID        string              `json:"chain_id"`
	SignerID       string              `json:"signer_id"`
	To             string              `json:"to"`
	ValueWei       string              `json:"value_wei"`
	Nonce          *string             `json:"nonce,omitempty"`
	ActiveAttempt  *AttemptSummary     `json:"active_attempt,omitempty"`
	WinningAttempt *AttemptSummary     `json:"winning_attempt,omitempty"`
	Attempts       []AttemptSummary    `json:"attempts"`
	Receipts       []ReceiptSummary    `json:"receipt_history"`
	Broadcast      BroadcastSummary    `json:"broadcast"`
	Confirmation   ConfirmationSummary `json:"confirmation"`
	Account        AccountReadiness    `json:"account"`
	ReorgEvents    int                 `json:"reorg_events"`
	CreatedAt      time.Time           `json:"created_at"`
	UpdatedAt      time.Time           `json:"updated_at"`
}

type AttemptSummary struct {
	AttemptID         string `json:"attempt_id"`
	AttemptNumber     int    `json:"attempt_number"`
	ReplacesAttemptID string `json:"replaces_attempt_id,omitempty"`
	Status            string `json:"status"`
	Nonce             string `json:"nonce"`
	GasLimit          uint64 `json:"gas_limit"`
	MaxFeePerGas      string `json:"max_fee_per_gas"`
	MaxPriorityFee    string `json:"max_priority_fee_per_gas"`
	TransactionHash   string `json:"transaction_hash,omitempty"`
}

type ReceiptSummary struct {
	AttemptNumber     int        `json:"attempt_number"`
	TransactionHash   string     `json:"transaction_hash"`
	BlockNumber       string     `json:"block_number"`
	BlockHash         string     `json:"block_hash"`
	ExecutionStatus   int        `json:"execution_status"`
	Verification      string     `json:"verification"`
	Confirmations     int        `json:"confirmations"`
	TransactionIndex  string     `json:"transaction_index,omitempty"`
	GasUsed           string     `json:"gas_used,omitempty"`
	EffectiveGasPrice string     `json:"effective_gas_price,omitempty"`
	ObservedAt        time.Time  `json:"observed_at"`
	LastVerifiedAt    *time.Time `json:"last_verified_at,omitempty"`
}

type BroadcastObservationSummary struct {
	AttemptNumber int    `json:"attempt_number"`
	Endpoint      string `json:"endpoint"`
	Kind          string `json:"kind"`
	Result        string `json:"result"`
	Count         int64  `json:"count"`
}

type BroadcastSummary struct {
	SendCount    int                           `json:"send_count"`
	Observations []BroadcastObservationSummary `json:"observations"`
}

type ConfirmationSummary struct {
	Observed        int    `json:"observed"`
	Required        int    `json:"required"`
	VerifiedHead    string `json:"verified_head,omitempty"`
	BlockNumber     string `json:"block_number,omitempty"`
	BlockHash       string `json:"block_hash,omitempty"`
	ExecutionStatus *int   `json:"execution_status,omitempty"`
	MaxReorgDepth   int    `json:"max_reorg_depth,omitempty"`
}

type AccountReadiness struct {
	ReconciliationStatus AccountReconciliationState `json:"reconciliation_status"`
	ReconciliationReason string                     `json:"reconciliation_reason,omitempty"`
	SignerStatus         string                     `json:"signer_status"`
	BlockedReason        string                     `json:"blocked_reason,omitempty"`
}

func hexValue(value []byte) string { return "0x" + hex.EncodeToString(value) }

// IntentView reads one tenant's intent, attempts, broadcast evidence and
// receipts from one repeatable-read snapshot. Raw signed bytes are never read.
func (s *Store) IntentView(ctx context.Context, id, tenant string) (IntentProjection, error) {
	view := IntentProjection{Attempts: []AttemptSummary{}, Receipts: []ReceiptSummary{}}
	view.Broadcast.Observations = []BroadcastObservationSummary{}
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return view, err
	}
	defer tx.Rollback(ctx)
	var to []byte
	var nonce, head, maxDepth sql.NullString
	var active, winner sql.NullInt64
	var accountReason, paused, reason sql.NullString
	err = tx.QueryRow(ctx, `SELECT i.intent_id::text,i.state,i.reason_code,i.chain_id::text,
		i.signer_id,i.to_address,i.value_wei::text,n.nonce::text,
		i.active_attempt_number,i.winning_attempt_number,i.confirmation_count,
		i.confirmation_depth,i.receipt_verified_head::text,i.max_reorg_depth::text,
		i.broadcast_send_count,a.reconciliation_status,a.reconciliation_reason,
		a.signer_status,a.paused_reason,i.created_at,i.updated_at,
		(SELECT count(*) FROM audit_events e WHERE e.intent_id=i.intent_id
		 AND e.event_type IN ('RECEIPT_REORGED','DEEP_REORG_BLOCKED'))
		FROM transaction_intents i
		JOIN signer_accounts a ON a.chain_id=i.chain_id AND a.signer_address=i.signer_address
		LEFT JOIN nonce_reservations n ON n.intent_id=i.intent_id
		WHERE i.intent_id=$1::uuid AND i.tenant_id=$2`, id, tenant).Scan(
		&view.IntentID, &view.State, &reason, &view.ChainID, &view.SignerID,
		&to, &view.ValueWei, &nonce, &active, &winner, &view.Confirmation.Observed,
		&view.Confirmation.Required, &head, &maxDepth, &view.Broadcast.SendCount,
		&view.Account.ReconciliationStatus, &accountReason, &view.Account.SignerStatus,
		&paused, &view.CreatedAt, &view.UpdatedAt, &view.ReorgEvents)
	if errors.Is(err, pgx.ErrNoRows) {
		return view, ErrNotFound
	}
	if err != nil {
		return view, err
	}
	view.To = hexValue(to)
	view.ReasonCode = reason.String
	view.Account.ReconciliationReason = accountReason.String
	view.Account.BlockedReason = paused.String
	if nonce.Valid {
		view.Nonce = &nonce.String
	}
	if head.Valid {
		view.Confirmation.VerifiedHead = head.String
	}
	if maxDepth.Valid {
		view.Confirmation.MaxReorgDepth, err = strconv.Atoi(maxDepth.String)
		if err != nil {
			return view, err
		}
	}
	rows, err := tx.Query(ctx, `SELECT attempt_id::text,attempt_number,
		COALESCE(replaces_attempt_id::text,''),status,nonce::text,gas_limit,
		max_fee_per_gas::text,max_priority_fee_per_gas::text,transaction_hash
		FROM transaction_attempts WHERE intent_id=$1::uuid ORDER BY attempt_number`, id)
	if err != nil {
		return view, err
	}
	for rows.Next() {
		var a AttemptSummary
		var hash []byte
		if err = rows.Scan(&a.AttemptID, &a.AttemptNumber, &a.ReplacesAttemptID,
			&a.Status, &a.Nonce, &a.GasLimit, &a.MaxFeePerGas, &a.MaxPriorityFee, &hash); err != nil {
			rows.Close()
			return view, err
		}
		if len(hash) == 32 {
			a.TransactionHash = hexValue(hash)
		}
		view.Attempts = append(view.Attempts, a)
		if active.Valid && int64(a.AttemptNumber) == active.Int64 {
			copy := a
			view.ActiveAttempt = &copy
		}
		if winner.Valid && int64(a.AttemptNumber) == winner.Int64 {
			copy := a
			view.WinningAttempt = &copy
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return view, err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT attempt_number,transaction_hash,block_number::text,
		block_hash,execution_status,verification_status,confirmations,
		transaction_index::text,gas_used::text,effective_gas_price::text,
		observed_at,last_verified_at FROM observed_receipts
		WHERE intent_id=$1::uuid ORDER BY receipt_id`, id)
	if err != nil {
		return view, err
	}
	for rows.Next() {
		var receipt ReceiptSummary
		var hash, block []byte
		var index, gas, price sql.NullString
		var verified sql.NullTime
		if err = rows.Scan(&receipt.AttemptNumber, &hash, &receipt.BlockNumber,
			&block, &receipt.ExecutionStatus, &receipt.Verification,
			&receipt.Confirmations, &index, &gas, &price,
			&receipt.ObservedAt, &verified); err != nil {
			rows.Close()
			return view, err
		}
		receipt.TransactionHash = hexValue(hash)
		receipt.BlockHash = hexValue(block)
		receipt.TransactionIndex = index.String
		receipt.GasUsed = gas.String
		receipt.EffectiveGasPrice = price.String
		if verified.Valid {
			receipt.LastVerifiedAt = &verified.Time
		}
		view.Receipts = append(view.Receipts, receipt)
		if receipt.Verification == "CANONICAL" && view.WinningAttempt != nil &&
			receipt.AttemptNumber == view.WinningAttempt.AttemptNumber {
			view.Confirmation.BlockNumber = receipt.BlockNumber
			view.Confirmation.BlockHash = receipt.BlockHash
			status := receipt.ExecutionStatus
			view.Confirmation.ExecutionStatus = &status
		}
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return view, err
	}
	rows.Close()
	rows, err = tx.Query(ctx, `SELECT attempt_number,endpoint,kind,classification,count(*)
		FROM broadcast_observations WHERE intent_id=$1::uuid
		GROUP BY attempt_number,endpoint,kind,classification
		ORDER BY attempt_number,endpoint,kind,classification`, id)
	if err != nil {
		return view, err
	}
	for rows.Next() {
		var observation BroadcastObservationSummary
		if err = rows.Scan(&observation.AttemptNumber, &observation.Endpoint,
			&observation.Kind, &observation.Result, &observation.Count); err != nil {
			rows.Close()
			return view, err
		}
		view.Broadcast.Observations = append(view.Broadcast.Observations, observation)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return view, err
	}
	rows.Close()
	if err = tx.Commit(ctx); err != nil {
		return view, err
	}
	return view, nil
}
