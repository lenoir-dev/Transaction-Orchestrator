package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

func receiptPolicy() ReceiptPolicy {
	return ReceiptPolicy{Now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), Interval: 0,
		RPCDeadline: time.Second, TotalDeadline: 10 * time.Second, MaxReorgDepth: 4, MaxAttempts: 101}
}
func pollReceipt(t *testing.T, f *replacementFixture, p ReceiptPolicy) ConfirmationStatus {
	t.Helper()
	out, err := f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, p)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
func snapshotAnvil(t *testing.T, r *HTTPRPC) string {
	t.Helper()
	var id string
	if err := r.call(context.Background(), "evm_snapshot", []any{}, &id); err != nil {
		t.Fatal(err)
	}
	return id
}
func revertAnvil(t *testing.T, r *HTTPRPC, id string) {
	t.Helper()
	var ok bool
	if err := r.call(context.Background(), "evm_revert", []any{id}, &ok); err != nil || !ok {
		t.Fatalf("snapshot revert %v: %v", ok, err)
	}
}
func branchWithoutReceipt(t *testing.T, r *HTTPRPC, hash common.Hash, blocks int) {
	t.Helper()
	var result json.RawMessage
	if err := r.call(context.Background(), "anvil_dropTransaction", []any{hash.Hex()}, &result); err != nil {
		t.Fatal(err)
	}
	if err := r.call(context.Background(), "anvil_dropAllTransactions", []any{}, &result); err != nil {
		t.Fatal(err)
	}
	// A changed timestamp gives an explicit competing block identity.
	if err := r.call(context.Background(), "evm_increaseTime", []any{37}, &result); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < blocks; i++ {
		mineAnvil(t, r)
	}
}
func assertReceiptLedger(t *testing.T, f *replacementFixture, wantAttempts int) {
	t.Helper()
	var intents, reservations, attempts int
	err := f.pool.QueryRow(context.Background(), `SELECT
        (SELECT count(*) FROM transaction_intents WHERE intent_id=$1::uuid),
        (SELECT count(*) FROM nonce_reservations WHERE intent_id=$1::uuid),
        (SELECT count(*) FROM transaction_attempts WHERE intent_id=$1::uuid)`, f.intent.ID).
		Scan(&intents, &reservations, &attempts)
	if err != nil || intents != 1 || reservations != 1 || attempts != wantAttempts {
		t.Fatalf("ledger %d/%d/%d: %v", intents, reservations, attempts, err)
	}
}

func TestReceiptConfirmationBoundaries(t *testing.T) {
	f := newReplacementFixture(t)
	p := receiptPolicy()
	mineAnvil(t, f.direct)
	first := pollReceipt(t, f, p)
	if first.State != Mined || first.Confirmations != 1 || first.WinningAttemptNumber != 1 {
		t.Fatalf("first: %+v", first)
	}
	same := pollReceipt(t, f, p)
	if same.State != Confirming || same.Confirmations != 1 {
		t.Fatalf("same head: %+v", same)
	}
	mineAnvil(t, f.direct)
	below := pollReceipt(t, f, p)
	if below.State != Confirming || below.Confirmations != 2 {
		t.Fatalf("threshold-1: %+v", below)
	}
	mineAnvil(t, f.direct)
	exact := pollReceipt(t, f, p)
	if exact.State != Confirmed || exact.Confirmations != 3 {
		t.Fatalf("threshold: %+v", exact)
	}
	mineAnvil(t, f.direct)
	above := pollReceipt(t, f, p)
	if above.State != Confirmed || above.Confirmations != 4 {
		t.Fatalf("threshold+1: %+v", above)
	}
	_, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer, []BroadcastEndpoint{f.direct}, replacementOptions())
	if !errors.Is(err, ErrTransition) && !errors.Is(err, ErrReceiptObserved) {
		t.Fatalf("confirmed replacement: %v", err)
	}
	assertReceiptLedger(t, f, 1)
	history, err := f.store.ReceiptHistory(context.Background(), f.intent.ID)
	if err != nil || len(history) != 1 || history[0].GasUsed != 21000 || history[0].Verification != "CANONICAL" {
		t.Fatalf("receipt metadata: %+v %v", history, err)
	}
}

func TestAnvilReceiptReorgDepths(t *testing.T) {
	for _, depth := range []int{1, 2, 4, 5} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			f := newReplacementFixture(t)
			p := receiptPolicy()
			base := snapshotAnvil(t, f.direct)
			mineAnvil(t, f.direct)
			for i := 1; i < depth; i++ {
				mineAnvil(t, f.direct)
			}
			old := pollReceipt(t, f, p)
			revertAnvil(t, f.direct, base)
			branchWithoutReceipt(t, f.direct, f.raw.Hash, depth)
			changed := pollReceipt(t, f, p)
			if depth > p.MaxReorgDepth {
				if changed.State != Blocked || changed.WinningAttemptNumber != 0 {
					t.Fatalf("deep: %+v", changed)
				}
				if f.store.ReceiptReady(context.Background()) == nil {
					t.Fatal("deep reorg did not degrade readiness")
				}
				var pause string
				if err := f.pool.QueryRow(context.Background(), `SELECT paused_reason FROM signer_accounts WHERE signer_id='replace'`).Scan(&pause); err != nil || pause != "DEEP_REORG_BLOCKED" {
					t.Fatalf("deep pause %s: %v", pause, err)
				}
			} else {
				if changed.State != Reorged || changed.WinningAttemptNumber != 0 {
					t.Fatalf("reorg: %+v", changed)
				}
				recovered := pollReceipt(t, f, p)
				if recovered.State != Uncertain {
					t.Fatalf("pending after reorg: %+v", recovered)
				}
				_, e := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer, []BroadcastEndpoint{f.direct}, replacementOptions())
				if !errors.Is(e, ErrTransition) {
					t.Fatalf("reorg triggered escalation: %v", e)
				}
				if _, err := f.direct.SendRawTransaction(context.Background(), f.raw.Raw); err != nil {
					t.Fatal(err)
				}
				mineAnvil(t, f.direct)
				included := pollReceipt(t, f, p)
				if included.WinningAttemptNumber != 1 || included.ReceiptBlockHash == old.ReceiptBlockHash {
					t.Fatalf("reincluded: %+v", included)
				}
			}
			history, err := f.store.ReceiptHistory(context.Background(), f.intent.ID)
			if err != nil || len(history) < 1 || history[0].Verification != "ORPHANED" {
				t.Fatalf("orphan history: %+v %v", history, err)
			}
			assertReceiptLedger(t, f, 1)
		})
	}
}

func TestAnvilReplacementReceiptReorg(t *testing.T) {
	f := newReplacementFixture(t)
	p := receiptPolicy()
	base := snapshotAnvil(t, f.direct)
	second, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer, []BroadcastEndpoint{f.direct}, replacementOptions())
	if err != nil {
		t.Fatal(err)
	}
	raw2, err := f.store.LoadSignedAttemptNumber(context.Background(), f.intent.ID, 2)
	if err != nil {
		t.Fatal(err)
	}
	mineAnvil(t, f.direct)
	old := pollReceipt(t, f, p)
	if old.WinningAttemptNumber != 1 {
		t.Fatalf("branch A winner %+v", old)
	}
	revertAnvil(t, f.direct, base)
	branchWithoutReceipt(t, f.direct, f.raw.Hash, 1)
	if out := pollReceipt(t, f, p); out.State != Reorged {
		t.Fatalf("orphan: %+v", out)
	}
	if _, err = f.direct.SendRawTransaction(context.Background(), raw2.Raw); err != nil {
		t.Fatal(err)
	}
	mineAnvil(t, f.direct)
	next := pollReceipt(t, f, p)
	if next.WinningAttemptNumber != 2 || next.WinnerHash != second.Hash {
		t.Fatalf("branch B winner %+v", next)
	}
	history, err := f.store.ReceiptHistory(context.Background(), f.intent.ID)
	if err != nil || len(history) != 2 || history[0].Verification != "ORPHANED" || history[1].Verification != "CANONICAL" {
		t.Fatalf("fork history %+v %v", history, err)
	}
	assertReceiptLedger(t, f, 2)
}

type receiptOverride struct {
	BroadcastEndpoint
	alias    string
	head     func(context.Context) (*BlockEvidence, error)
	byHash   func(context.Context, common.Hash) (*BlockEvidence, error)
	byNumber func(context.Context, uint64) (*BlockEvidence, error)
}

func (r receiptOverride) Alias() string { return r.alias }
func (r receiptOverride) LatestBlock(c context.Context) (*BlockEvidence, error) {
	if r.head != nil {
		return r.head(c)
	}
	return r.BroadcastEndpoint.LatestBlock(c)
}
func (r receiptOverride) BlockByHash(c context.Context, h common.Hash) (*BlockEvidence, error) {
	if r.byHash != nil {
		return r.byHash(c, h)
	}
	return r.BroadcastEndpoint.BlockByHash(c, h)
}
func (r receiptOverride) BlockByNumber(c context.Context, n uint64) (*BlockEvidence, error) {
	if r.byNumber != nil {
		return r.byNumber(c, n)
	}
	return r.BroadcastEndpoint.BlockByNumber(c, n)
}

func TestReceiptInvalidStaleAndMultiRPC(t *testing.T) {
	f := newReplacementFixture(t)
	p := receiptPolicy()
	base := snapshotAnvil(t, f.direct)
	mineAnvil(t, f.direct)
	original, err := f.direct.ReceiptByHash(context.Background(), f.raw.Hash)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{"block_hash", "number", "index", "status"} {
		t.Run(mutation, func(t *testing.T) {
			wrong := *original
			switch mutation {
			case "block_hash":
				wrong.BlockHash = common.HexToHash("0x42")
			case "number":
				wrong.BlockNumber++
			case "index":
				wrong.TransactionIndex = 99
			case "status":
				wrong.Status = 2
			}
			ep := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "false", receipt: func(context.Context, common.Hash) (*ReceiptEvidence, error) { return &wrong, nil }}
			_, e := f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{ep}, p)
			if !errors.Is(e, ErrEvidenceUnavailable) {
				t.Fatalf("invalid %s: %v", mutation, e)
			}
			state, _ := f.store.GetIntent(context.Background(), f.intent.ID)
			if state.State != Pending {
				t.Fatalf("false receipt changed state %s", state.State)
			}
		})
	}
	old := pollReceipt(t, f, p)
	oldHead, _ := f.direct.LatestBlock(context.Background())
	oldBlock, _ := f.direct.BlockByHash(context.Background(), old.ReceiptBlockHash)
	stale := receiptOverride{BroadcastEndpoint: f.direct, alias: "stale", head: func(context.Context) (*BlockEvidence, error) {
		return &BlockEvidence{Hash: common.HexToHash("0x11"), Number: 0}, nil
	}}
	_, err = f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{stale}, p)
	if !errors.Is(err, ErrEvidenceUnavailable) {
		t.Fatalf("stale %v", err)
	}
	after, err := f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{stale, f.direct}, p)
	if err != nil || after.WinningAttemptNumber != 1 || after.Confirmations != 1 {
		t.Fatalf("fallback %+v %v", after, err)
	}
	missing := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "missing", receipt: func(context.Context, common.Hash) (*ReceiptEvidence, error) { return nil, nil }}
	held, err := f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{missing}, p)
	if err != nil || held.WinningAttemptNumber != 1 {
		t.Fatalf("receipt disappearance %+v %v", held, err)
	}
	revertAnvil(t, f.direct, base)
	branchWithoutReceipt(t, f.direct, f.raw.Hash, 1)
	oldView := receiptOverride{BroadcastEndpoint: overrideEndpoint{BroadcastEndpoint: f.direct, alias: "old", receipt: func(context.Context, common.Hash) (*ReceiptEvidence, error) { return original, nil }}, alias: "old",
		head:     func(context.Context) (*BlockEvidence, error) { return oldHead, nil },
		byHash:   func(context.Context, common.Hash) (*BlockEvidence, error) { return oldBlock, nil },
		byNumber: func(context.Context, uint64) (*BlockEvidence, error) { return oldBlock, nil }}
	_, err = f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{oldView, f.direct}, p)
	if !errors.Is(err, ErrEvidenceUnavailable) {
		t.Fatalf("provider disagreement %v", err)
	}
	heldStatus, _ := f.store.ConfirmationStatus(context.Background(), f.intent.ID)
	if heldStatus.WinningAttemptNumber != 1 {
		t.Fatalf("disagreement revoked winner %+v", heldStatus)
	}
	if out := pollReceipt(t, f, p); out.State != Reorged {
		t.Fatalf("reorg %+v", out)
	}
	foreign := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "foreign", nonce: func(context.Context, common.Address) (uint64, error) { return 2, nil }}
	blocked, err := f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{foreign}, p)
	if err != nil || blocked.State != Blocked {
		t.Fatalf("foreign nonce %+v %v", blocked, err)
	}
	assertReceiptLedger(t, f, 1)
}

func TestAnvilReceiptBranchReturn(t *testing.T) {
	f := newReplacementFixture(t)
	p := receiptPolicy()
	base := snapshotAnvil(t, f.direct)
	mineAnvil(t, f.direct)
	a := pollReceipt(t, f, p)
	var stateA string
	if err := f.direct.call(context.Background(), "anvil_dumpState", []any{true}, &stateA); err != nil {
		t.Fatal(err)
	}
	revertAnvil(t, f.direct, base)
	branchWithoutReceipt(t, f.direct, f.raw.Hash, 1)
	if out := pollReceipt(t, f, p); out.State != Reorged {
		t.Fatalf("B %+v", out)
	}
	var loaded bool
	if err := f.direct.call(context.Background(), "anvil_loadState", []any{stateA}, &loaded); err != nil || !loaded {
		t.Fatalf("load A %v %v", loaded, err)
	}
	returned := pollReceipt(t, f, p)
	if returned.ReceiptBlockHash != a.ReceiptBlockHash || returned.WinningAttemptNumber != 1 {
		t.Fatalf("A return %+v", returned)
	}
	history, err := f.store.ReceiptHistory(context.Background(), f.intent.ID)
	if err != nil || len(history) != 2 || history[0].BlockHash != history[1].BlockHash || history[0].Verification != "ORPHANED" || history[1].Verification != "CANONICAL" {
		t.Fatalf("A-B-A history %+v %v", history, err)
	}
	assertReceiptLedger(t, f, 1)
}

func TestAnvilOriginalWinsAfterReplacementReorg(t *testing.T) {
	f := newReplacementFixture(t)
	p := receiptPolicy()
	base := snapshotAnvil(t, f.direct)
	second, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer, []BroadcastEndpoint{f.direct}, replacementOptions())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, broadcastPolicy()); err != nil {
		t.Fatal(err)
	}
	mineAnvil(t, f.direct)
	if out := pollReceipt(t, f, p); out.WinningAttemptNumber != 2 {
		t.Fatalf("replacement A %+v", out)
	}
	revertAnvil(t, f.direct, base)
	branchWithoutReceipt(t, f.direct, second.Hash, 1)
	if out := pollReceipt(t, f, p); out.State != Reorged {
		t.Fatalf("reorg %+v", out)
	}
	if _, err = f.direct.SendRawTransaction(context.Background(), f.raw.Raw); err != nil {
		t.Fatal(err)
	}
	mineAnvil(t, f.direct)
	if out := pollReceipt(t, f, p); out.WinningAttemptNumber != 1 {
		t.Fatalf("original B %+v", out)
	}
	assertReceiptLedger(t, f, 2)
}

func TestReceiptExecutionReverted(t *testing.T) {
	policy := testPolicy()
	policy.GasLimit = 50000
	policy.ConfirmationDepth = 1
	policy.MaxFeeCapWei = "100000000000"
	policy.MaxTipCapWei = "10000000000"
	f := newReplacementFixtureWithPolicy(t, policy)
	var result json.RawMessage
	if err := f.direct.call(context.Background(), "anvil_setCode", []any{testAddress, "0x60006000fd"}, &result); err != nil {
		t.Fatal(err)
	}
	mineAnvil(t, f.direct)
	out := pollReceipt(t, f, receiptPolicy())
	if out.State != Confirmed || out.ExecutionStatus != 0 || out.ExecutionOutcome != "REVERTED" || out.Confirmations != 1 {
		t.Fatalf("reverted inclusion %+v", out)
	}
	samples, err := f.store.ConfirmationHistogram(context.Background())
	if err != nil || samples.Count != 1 || samples.SumSeconds < 0 {
		t.Fatalf("durations %+v %v", samples, err)
	}
	assertReceiptLedger(t, f, 1)
}

func TestReceiptProcessCrash(t *testing.T) {
	if point := os.Getenv("TXORCH_RECEIPT_CRASH_POINT"); point != "" {
		crashReceiptChild(point)
		return
	}
	for _, point := range []string{"after_receipt", "after_mined", "during_confirming", "after_confirmed", "during_reorg"} {
		t.Run(point, func(t *testing.T) {
			f := newReplacementFixture(t)
			base := snapshotAnvil(t, f.direct)
			mineAnvil(t, f.direct)
			if point == "during_confirming" || point == "after_confirmed" || point == "during_reorg" {
				pollReceipt(t, f, receiptPolicy())
			}
			if point == "during_confirming" {
				mineAnvil(t, f.direct)
			}
			if point == "after_confirmed" {
				mineAnvil(t, f.direct)
				mineAnvil(t, f.direct)
			}
			if point == "during_reorg" {
				revertAnvil(t, f.direct, base)
				branchWithoutReceipt(t, f.direct, f.raw.Hash, 1)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReceiptProcessCrash$")
			cmd.Env = append(os.Environ(), "TXORCH_RECEIPT_CRASH_POINT="+point, "TXORCH_TEST_SCHEMA="+f.schema,
				"TXORCH_TEST_INTENT_ID="+f.intent.ID, "TXORCH_ANVIL_URL="+f.direct.URL)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 9 {
				t.Fatalf("child crash %v %s", err, output)
			}
			f.pool.Close()
			f.pool = nil
			f.open(t)
			before, err := f.store.ConfirmationStatus(context.Background(), f.intent.ID)
			if err != nil {
				t.Fatal(err)
			}
			history, err := f.store.ReceiptHistory(context.Background(), f.intent.ID)
			if err != nil {
				t.Fatal(err)
			}
			if point == "after_receipt" && (before.State != Pending || len(history) != 0) {
				t.Fatalf("partial receipt commit %+v %+v", before, history)
			}
			if point == "after_mined" && before.State != Mined {
				t.Fatalf("mined lost %+v", before)
			}
			if point == "during_confirming" && (before.State != Mined || before.Confirmations != 1) {
				t.Fatalf("partial progress %+v", before)
			}
			if point == "after_confirmed" && before.State != Confirmed {
				t.Fatalf("confirmed lost %+v", before)
			}
			if point == "during_reorg" && (before.WinningAttemptNumber != 1 || history[0].Verification != "CANONICAL") {
				t.Fatalf("partial rollback %+v %+v", before, history)
			}
			recovered := pollReceipt(t, f, receiptPolicy())
			if point == "during_reorg" && recovered.State != Reorged {
				t.Fatalf("reorg recovery %+v", recovered)
			}
			assertReceiptLedger(t, f, 1)
		})
	}
}
func crashReceiptChild(point string) {
	config, err := pgxpool.ParseConfig(os.Getenv("TXORCH_TEST_DATABASE_URL"))
	if err != nil {
		os.Exit(2)
	}
	config.ConnConfig.RuntimeParams["search_path"] = os.Getenv("TXORCH_TEST_SCHEMA")
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		os.Exit(2)
	}
	r := &HTTPRPC{Name: "direct", URL: os.Getenv("TXORCH_ANVIL_URL"), Client: &http.Client{Timeout: time.Second}}
	hooks := receiptHooks{}
	switch point {
	case "after_receipt":
		hooks.afterReceipt = func() { os.Exit(9) }
	case "after_mined", "after_confirmed":
		hooks.afterCommit = func() { os.Exit(9) }
	case "during_confirming":
		hooks.beforeCommit = func() { os.Exit(9) }
	case "during_reorg":
		hooks.afterReorgWrite = func() { os.Exit(9) }
	default:
		os.Exit(2)
	}
	_, _ = NewStore(pool).reconcileReceipts(context.Background(), os.Getenv("TXORCH_TEST_INTENT_ID"), []BroadcastEndpoint{r}, receiptPolicy(), hooks)
	os.Exit(2)
}

func TestReceiptConcurrentAndScheduling(t *testing.T) {
	f := newReplacementFixture(t)
	mineAnvil(t, f.direct)
	p := receiptPolicy()
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, p)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil && !errors.Is(err, ErrTransition) {
			t.Fatal(err)
		}
	}
	history, err := f.store.ReceiptHistory(context.Background(), f.intent.ID)
	if err != nil || len(history) != 1 {
		t.Fatalf("concurrent receipts %+v %v", history, err)
	}
	p.Interval = time.Minute
	if _, err = f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, p); err != nil {
		t.Fatal(err)
	}
	_, err = f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, p)
	if !errors.Is(err, ErrReceiptBackoff) {
		t.Fatalf("schedule %v", err)
	}
	f.pool.Close()
	f.pool = nil
	f.open(t)
	_, err = f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, p)
	if !errors.Is(err, ErrReceiptBackoff) {
		t.Fatalf("restart schedule %v", err)
	}
	for _, q := range []string{`UPDATE observed_receipts SET block_number=42`, `UPDATE observed_receipts SET execution_status=0`, `DELETE FROM observed_receipts`, `UPDATE observed_blocks SET parent_hash=decode(repeat('00',32),'hex')`} {
		if _, err = f.pool.Exec(context.Background(), q); err == nil {
			t.Fatalf("evidence mutation allowed: %s", q)
		}
	}
	assertReceiptLedger(t, f, 1)
}

func TestReceiptConfirmedStalenessAndPolicyBounds(t *testing.T) {
	f := newReplacementFixture(t)
	p := receiptPolicy()
	mineAnvil(t, f.direct)
	mineAnvil(t, f.direct)
	mineAnvil(t, f.direct)
	confirmed := pollReceipt(t, f, p)
	if confirmed.State != Confirmed {
		t.Fatalf("confirmed %+v", confirmed)
	}
	work, err := f.store.ReceiptWork(context.Background(), p.Now, 1)
	if err != nil || len(work) != 1 || work[0] != f.intent.ID {
		t.Fatalf("confirmed polling work %v %v", work, err)
	}
	stale := receiptOverride{BroadcastEndpoint: f.direct, alias: "stale", head: func(context.Context) (*BlockEvidence, error) {
		return &BlockEvidence{Number: 1, Hash: confirmed.ReceiptBlockHash}, nil
	}}
	_, err = f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{stale}, p)
	if !errors.Is(err, ErrEvidenceUnavailable) {
		t.Fatalf("stale confirmed %v", err)
	}
	held, _ := f.store.ConfirmationStatus(context.Background(), f.intent.ID)
	if held.State != Confirmed || held.Confirmations != 3 {
		t.Fatalf("confirmed regressed %+v", held)
	}
	for _, change := range []func(*ReceiptPolicy){func(p *ReceiptPolicy) { p.MaxReorgDepth = 0 }, func(p *ReceiptPolicy) { p.MaxReorgDepth = 1001 }, func(p *ReceiptPolicy) { p.MaxAttempts = 0 }, func(p *ReceiptPolicy) { p.TotalDeadline = p.RPCDeadline }, func(p *ReceiptPolicy) { p.MaxReorgDepth = 2 }} {
		bad := p
		change(&bad)
		if _, err = f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid policy %+v: %v", bad, err)
		}
	}
}

func TestReceiptBoundedDeadline(t *testing.T) {
	f := newReplacementFixture(t)
	p := receiptPolicy()
	p.RPCDeadline = 20 * time.Millisecond
	p.TotalDeadline = 100 * time.Millisecond
	slow := receiptOverride{BroadcastEndpoint: f.direct, alias: "slow", head: func(ctx context.Context) (*BlockEvidence, error) { <-ctx.Done(); return nil, ctx.Err() }}
	started := time.Now()
	_, err := f.store.ReconcileReceipts(context.Background(), f.intent.ID, []BroadcastEndpoint{slow}, p)
	if !errors.Is(err, ErrEvidenceUnavailable) || time.Since(started) > time.Second {
		t.Fatalf("bounded poll %v duration %v", err, time.Since(started))
	}
}
