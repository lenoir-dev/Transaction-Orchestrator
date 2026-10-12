package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5/pgxpool"
)

type replacementFixture struct {
	*broadcastFixture
	signer *DevLocalSigner
}

func newReplacementFixture(t *testing.T) *replacementFixture {
	policy := testPolicy()
	policy.MaxFeeCapWei = "100000000000"
	policy.MaxTipCapWei = "10000000000"
	policy.MaxReplacements = 2
	return newReplacementFixtureWithPolicy(t, policy)
}

func newReplacementFixtureWithPolicy(t *testing.T, policy Policy) *replacementFixture {
	t.Helper()
	if os.Getenv("TXORCH_TEST_DATABASE_URL") == "" || os.Getenv("TXORCH_ANVIL_BINARY") == "" {
		t.Skip("PostgreSQL and Anvil are required")
	}
	f := newFixture(t)
	r := startAnvil(t)
	var enabled json.RawMessage
	if err := r.call(context.Background(), "evm_setAutomine", []any{false}, &enabled); err != nil {
		t.Fatal(err)
	}
	var automine bool
	if err := r.call(context.Background(), "anvil_getAutomine", []any{}, &automine); err != nil || automine {
		t.Fatalf("Anvil mining control unavailable: %v", err)
	}
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewDevLocalSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	address, _ := signer.Address(context.Background())
	var addr [20]byte
	copy(addr[:], address.Bytes())
	if err = f.store.ProvisionAccount(context.Background(), Account{ChainID: "31337", SignerID: "replace", Address: addr}); err != nil {
		t.Fatal(err)
	}
	var funded json.RawMessage
	if err = r.call(context.Background(), "anvil_setBalance", []any{address.Hex(), "0x3635c9adc5dea00000"}, &funded); err != nil {
		t.Fatal(err)
	}
	req := testRequest("replacement")
	req.SignerID = "replace"
	intent, err := f.store.CreateIntent(context.Background(), req, policy)
	if err != nil {
		t.Fatal(err)
	}
	if nonce, err := f.store.ReserveNonce(context.Background(), intent.ID); err != nil || nonce != 0 {
		t.Fatalf("nonce %d: %v", nonce, err)
	}
	_, err = f.store.SignInitial(context.Background(), intent.ID, signer,
		FeeInput{MaxFeePerGasWei: "2000000000", MaxTipPerGasWei: "1000000000"}, signingOptions())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := f.store.LoadSignedAttempt(context.Background(), intent.ID)
	if err != nil {
		t.Fatal(err)
	}
	b := &broadcastFixture{fixture: f, direct: r, intent: intent, raw: raw}
	result, err := f.store.AdvanceBroadcast(context.Background(), intent.ID, []BroadcastEndpoint{r}, broadcastPolicy())
	if err != nil || result.State != Pending {
		t.Fatalf("pending original: %+v %v", result, err)
	}
	if receipt, err := r.ReceiptByHash(context.Background(), raw.Hash); err != nil || receipt != nil {
		t.Fatalf("original mined before controlled block: %v", err)
	}
	return &replacementFixture{broadcastFixture: b, signer: signer}
}

func replacementOptions() ReplacementOptions {
	return ReplacementOptions{Now: time.Now().Add(time.Hour), MinAge: 2 * time.Minute, BumpBps: 1500,
		Signing:     SignOptions{SignerTimeout: 5 * time.Second, LeaseDuration: 10 * time.Second},
		Observation: broadcastPolicy()}
}

func mineAnvil(t *testing.T, r *HTTPRPC) {
	t.Helper()
	var result json.RawMessage
	if err := r.call(context.Background(), "evm_mine", []any{}, &result); err != nil {
		t.Fatal(err)
	}
}

func (f *replacementFixture) assertHistory(t *testing.T, want int) {
	t.Helper()
	history, err := f.store.AttemptHistory(context.Background(), f.intent.ID)
	if err != nil || len(history) != want {
		t.Fatalf("history count=%d want=%d: %v", len(history), want, err)
	}
	var prior *types.Transaction
	for i, a := range history {
		if a.AttemptNumber != i+1 {
			t.Fatalf("attempt numbering %+v", a)
		}
		stored, err := f.store.LoadSignedAttemptNumber(context.Background(), f.intent.ID, i+1)
		if err != nil {
			t.Fatal(err)
		}
		var tx types.Transaction
		if err = tx.UnmarshalBinary(stored.Raw); err != nil || tx.Hash() != a.Hash || tx.Nonce() != 0 {
			t.Fatalf("attempt %d changed: %v", i+1, err)
		}
		if i > 0 {
			if a.ReplacesAttemptID != history[i-1].AttemptID || a.BumpBps != 1500 ||
				tx.GasFeeCap().Cmp(prior.GasFeeCap()) <= 0 || tx.GasTipCap().Cmp(prior.GasTipCap()) <= 0 ||
				tx.To() == nil || *tx.To() != *prior.To() || tx.Value().Cmp(prior.Value()) != 0 ||
				!bytes.Equal(tx.Data(), prior.Data()) || tx.Gas() != prior.Gas() || tx.ChainId().Cmp(prior.ChainId()) != 0 {
				t.Fatalf("replacement semantics differ: %+v", a)
			}
		}
		prior = &tx
	}
	var reservations int
	if err = f.pool.QueryRow(context.Background(), `SELECT count(*) FROM nonce_reservations
		WHERE intent_id=$1::uuid`, f.intent.ID).Scan(&reservations); err != nil || reservations != 1 {
		t.Fatalf("nonce ownership=%d: %v", reservations, err)
	}
}

func TestAnvilReplacementMinesFirst(t *testing.T) {
	f := newReplacementFixture(t)
	second, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer,
		[]BroadcastEndpoint{f.direct}, replacementOptions())
	if err != nil || second.AttemptNumber != 2 || second.Hash == f.raw.Hash {
		t.Fatalf("replacement: %+v %v", second, err)
	}
	f.assertHistory(t, 2)
	result, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
		[]BroadcastEndpoint{f.direct}, broadcastPolicy())
	if err != nil || result.State != Pending || result.Hash != second.Hash {
		t.Fatalf("replacement broadcast: %+v %v", result, err)
	}
	mineAnvil(t, f.direct)
	winner, err := f.store.ObserveWinner(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, broadcastPolicy())
	if err != nil || winner != 2 {
		t.Fatalf("replacement winner=%d: %v", winner, err)
	}
	var state State
	var winning int
	if err = f.pool.QueryRow(context.Background(), `SELECT state,winning_attempt_number FROM transaction_intents
		WHERE intent_id=$1::uuid`, f.intent.ID).Scan(&state, &winning); err != nil || state != Mined || winning != 2 {
		t.Fatalf("winner state=%s number=%d: %v", state, winning, err)
	}
	projection, err := f.store.BroadcastStatus(context.Background(), f.intent.ID)
	if err != nil || projection.WinnerAttemptNumber != 2 || projection.WinnerHash != second.Hash {
		t.Fatalf("replacement winner projection: %+v %v", projection, err)
	}
	var received string
	if err = f.direct.call(context.Background(), "eth_getBalance", []any{testAddress, "latest"}, &received); err != nil || received != "0x1" {
		t.Fatalf("one execution balance=%s: %v", received, err)
	}
	f.assertHistory(t, 2)
}

func TestAnvilOriginalMinesBeforeReplacementSend(t *testing.T) {
	f := newReplacementFixture(t)
	second, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer,
		[]BroadcastEndpoint{f.direct}, replacementOptions())
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.store.advanceBroadcast(context.Background(), f.intent.ID,
		[]BroadcastEndpoint{f.direct}, broadcastPolicy(), broadcastHooks{
			afterClaim: func() { mineAnvil(t, f.direct) },
		})
	if err != nil || result.State != Mined || result.Hash != second.Hash ||
		result.WinnerAttemptNumber != 1 || result.WinnerHash != f.raw.Hash {
		t.Fatalf("receipt did not stop send: %+v %v", result, err)
	}
	var winner int
	var status string
	var sends int
	if err = f.pool.QueryRow(context.Background(), `SELECT i.winning_attempt_number,a.status,i.broadcast_send_count
		FROM transaction_intents i JOIN transaction_attempts a ON a.intent_id=i.intent_id
		AND a.attempt_number=2 WHERE i.intent_id=$1::uuid`, f.intent.ID).Scan(&winner, &status, &sends); err != nil || winner != 1 || status != "NOT_BROADCAST" || sends != 0 {
		t.Fatalf("winner=%d replacement=%s sends=%d: %v", winner, status, sends, err)
	}
	stored, err := f.store.LoadSignedAttemptNumber(context.Background(), f.intent.ID, 2)
	if err != nil || stored.Hash != second.Hash {
		t.Fatalf("historical signed attempt lost: %v", err)
	}
}

func TestReplacementChainLimitCeilingAndUnderpriced(t *testing.T) {
	f := newReplacementFixture(t)
	for number := 2; number <= 3; number++ {
		id, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer,
			[]BroadcastEndpoint{f.direct}, replacementOptions())
		if err != nil || id.AttemptNumber != number {
			t.Fatalf("replacement %d: %+v %v", number, id, err)
		}
		got, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
			[]BroadcastEndpoint{f.direct}, broadcastPolicy())
		if err != nil || got.State != Pending {
			t.Fatalf("broadcast %d: %+v %v", number, got, err)
		}
	}
	f.assertHistory(t, 3)
	_, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer,
		[]BroadcastEndpoint{f.direct}, replacementOptions())
	if !errors.Is(err, ErrReplacementLimit) {
		t.Fatalf("fourth attempt: %v", err)
	}
	var reason string
	if err = f.pool.QueryRow(context.Background(), `SELECT reason_code FROM transaction_intents
		WHERE intent_id=$1::uuid`, f.intent.ID).Scan(&reason); err != nil || reason != "REPLACEMENT_LIMIT_REACHED" {
		t.Fatalf("limit reason=%s: %v", reason, err)
	}
	metrics, err := f.store.ReplacementMetrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var created, blocked int64
	for _, metric := range metrics {
		if metric.Name == "txorch_replacements_total" && metric.Reason == "PENDING_AGE" {
			created = metric.Count
		}
		if metric.Name == "txorch_replacement_blocked_total" && metric.Reason == "REPLACEMENT_LIMIT_REACHED" {
			blocked = metric.Count
		}
	}
	if created != 2 || blocked != 1 {
		t.Fatalf("replacement metrics: %+v", metrics)
	}

	under := newReplacementFixture(t)
	second, err := under.store.SignReplacement(context.Background(), under.intent.ID, under.signer,
		[]BroadcastEndpoint{under.direct}, replacementOptions())
	if err != nil {
		t.Fatal(err)
	}
	endpoint := overrideEndpoint{BroadcastEndpoint: under.direct, alias: "underpriced",
		send: func(context.Context, []byte) (common.Hash, error) {
			return common.Hash{}, &RPCFault{Code: -32000, Message: "replacement transaction underpriced"}
		}}
	got, err := under.store.AdvanceBroadcast(context.Background(), under.intent.ID,
		[]BroadcastEndpoint{endpoint}, broadcastPolicy())
	if err != nil || got.State != Blocked || got.LastOutcome != ResultFee {
		t.Fatalf("underpriced: %+v %v", got, err)
	}
	third, err := under.store.SignReplacement(context.Background(), under.intent.ID, under.signer,
		[]BroadcastEndpoint{under.direct}, replacementOptions())
	if err != nil || third.AttemptNumber != 3 || third.Hash == second.Hash {
		t.Fatalf("bounded next step: %+v %v", third, err)
	}
	under.assertHistory(t, 3)
}

func TestReplacementUnknownAndConcurrent(t *testing.T) {
	f := newReplacementFixture(t)
	opts := replacementOptions()
	opts.Signing.SignerTimeout = 15 * time.Second
	opts.Signing.LeaseDuration = 20 * time.Second
	var wg sync.WaitGroup
	start := make(chan struct{})
	ids := make(chan AttemptIdentity, 32)
	errs := make(chan error, 32)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer,
				[]BroadcastEndpoint{f.direct}, opts)
			if err != nil {
				errs <- err
			} else {
				ids <- id
			}
		}()
	}
	close(start)
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		var signing *SigningError
		if !errors.As(err, &signing) || signing.Code != "SIGNING_IN_PROGRESS" {
			t.Errorf("worker: %v", err)
		}
	}
	if t.Failed() {
		return
	}
	for id := range ids {
		if id.AttemptNumber != 2 {
			t.Fatalf("unexpected attempt %+v", id)
		}
	}
	f.assertHistory(t, 2)
	proxy, sends := faultProxy(t, f.direct.URL, false)
	first, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
		[]BroadcastEndpoint{proxy, f.direct}, broadcastPolicy())
	if err != nil || first.State != Uncertain || sends.Load() != 1 {
		t.Fatalf("replacement unknown: %+v %v", first, err)
	}
	_, err = f.store.SignReplacement(context.Background(), f.intent.ID, f.signer,
		[]BroadcastEndpoint{f.direct}, opts)
	if !errors.Is(err, ErrTransition) {
		t.Fatalf("unknown triggered another attempt: %v", err)
	}
	f.pool.Close()
	f.pool = nil
	f.open(t)
	recovered, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
		[]BroadcastEndpoint{proxy, f.direct}, broadcastPolicy())
	if err != nil || recovered.State != Pending || sends.Load() != 1 {
		t.Fatalf("unknown recovery: %+v %v", recovered, err)
	}
	f.assertHistory(t, 2)
}

func TestReplacementCeilingAndEarly(t *testing.T) {
	f := newReplacementFixture(t)
	opts := replacementOptions()
	opts.Now = time.Now()
	_, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer, []BroadcastEndpoint{f.direct}, opts)
	if !errors.Is(err, ErrReplacementTooEarly) {
		t.Fatalf("interval: %v", err)
	}
	policy := testPolicy()
	policy.MaxFeeCapWei = "2200000000"
	policy.MaxTipCapWei = "1100000000"
	policy.MaxReplacements = 2
	low := newReplacementFixtureWithPolicy(t, policy)
	_, err = low.store.SignReplacement(context.Background(), low.intent.ID, low.signer,
		[]BroadcastEndpoint{low.direct}, replacementOptions())
	if !errors.Is(err, ErrFeeCeiling) {
		t.Fatalf("fee ceiling: %v", err)
	}
	var reason string
	if err = low.pool.QueryRow(context.Background(), `SELECT reason_code FROM transaction_intents
		WHERE intent_id=$1::uuid`, low.intent.ID).Scan(&reason); err != nil || reason != "FEE_CEILING_REACHED" {
		t.Fatalf("ceiling reason=%s: %v", reason, err)
	}
	low.assertHistory(t, 1)
}

func TestReplacementProcessCrash(t *testing.T) {
	if point := os.Getenv("TXORCH_REPLACEMENT_CRASH_POINT"); point != "" {
		crashReplacementChild(point)
		return
	}
	for _, point := range []string{"before_claim", "after_claim", "during_call", "after_sign", "after_commit"} {
		t.Run(point, func(t *testing.T) {
			f := newReplacementFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReplacementProcessCrash$")
			cmd.Env = append(os.Environ(), "TXORCH_REPLACEMENT_CRASH_POINT="+point,
				"TXORCH_TEST_SCHEMA="+f.schema, "TXORCH_TEST_INTENT_ID="+f.intent.ID,
				"TXORCH_ANVIL_URL="+f.direct.URL,
				"TXORCH_TEST_DEV_KEY="+fmt.Sprintf("%x", crypto.FromECDSA(f.signer.key)))
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 9 {
				t.Fatalf("child crash: %v %s", err, output)
			}
			f.pool.Close()
			f.pool = nil
			f.open(t)
			if point != "after_commit" && point != "before_claim" {
				_, err = f.pool.Exec(context.Background(), `UPDATE transaction_intents SET
					lease_expires_at=clock_timestamp()-interval '1 second' WHERE intent_id=$1::uuid`, f.intent.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			id, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer,
				[]BroadcastEndpoint{f.direct}, replacementOptions())
			if err != nil || id.AttemptNumber != 2 {
				t.Fatalf("recovery: %+v %v", id, err)
			}
			f.assertHistory(t, 2)
			if point == "after_claim" {
				var complete bool
				err = f.pool.QueryRow(context.Background(), `SELECT count(*)=2 FROM audit_events
					WHERE intent_id=$1::uuid AND event_type='REPLACEMENT_CLAIMED'
					AND evidence->>'replaces_attempt_id' <> ''
					AND evidence->>'previous_max_fee_per_gas' <> ''`, f.intent.ID).Scan(&complete)
				if err != nil || !complete {
					t.Fatalf("reclaimed audit lineage: %v %v", complete, err)
				}
			}
		})
	}
}

func crashReplacementChild(point string) {
	config, err := pgxpool.ParseConfig(os.Getenv("TXORCH_TEST_DATABASE_URL"))
	if err != nil {
		os.Exit(2)
	}
	config.ConnConfig.RuntimeParams["search_path"] = os.Getenv("TXORCH_TEST_SCHEMA")
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		os.Exit(2)
	}
	key, err := crypto.HexToECDSA(os.Getenv("TXORCH_TEST_DEV_KEY"))
	if err != nil {
		os.Exit(2)
	}
	signer, err := NewDevLocalSigner(key)
	if err != nil {
		os.Exit(2)
	}
	r := &HTTPRPC{Name: "direct", URL: os.Getenv("TXORCH_ANVIL_URL"), Client: &http.Client{Timeout: time.Second}}
	hooks := signingHooks{}
	var boundary TransactionSigner = signer
	switch point {
	case "before_claim":
		hooks.beforeClaim = func() { os.Exit(9) }
	case "after_claim":
		hooks.afterClaim = func() { os.Exit(9) }
	case "during_call":
		boundary = exitDuringSigner{signer}
	case "after_sign":
		hooks.afterSign = func() { os.Exit(9) }
	case "after_commit":
		hooks.afterCommit = func() { os.Exit(9) }
	default:
		os.Exit(2)
	}
	_, _ = NewStore(pool).signReplacement(context.Background(), os.Getenv("TXORCH_TEST_INTENT_ID"),
		boundary, []BroadcastEndpoint{r}, replacementOptions(), hooks)
	os.Exit(2)
}

func TestReplacementBroadcastCrash(t *testing.T) {
	if point := os.Getenv("TXORCH_REPLACEMENT_BROADCAST_CRASH_POINT"); point != "" {
		crashReplacementBroadcastChild(point)
		return
	}
	for _, point := range []string{"after_rpc", "after_unknown"} {
		t.Run(point, func(t *testing.T) {
			f := newReplacementFixture(t)
			id, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer,
				[]BroadcastEndpoint{f.direct}, replacementOptions())
			if err != nil {
				t.Fatal(err)
			}
			endpoint := f.direct
			if point == "after_unknown" {
				endpoint, _ = faultProxy(t, f.direct.URL, false)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestReplacementBroadcastCrash$")
			cmd.Env = append(os.Environ(), "TXORCH_REPLACEMENT_BROADCAST_CRASH_POINT="+point,
				"TXORCH_TEST_SCHEMA="+f.schema, "TXORCH_TEST_INTENT_ID="+f.intent.ID,
				"TXORCH_ANVIL_URL="+endpoint.URL)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 9 {
				t.Fatalf("child crash: %v %s", err, output)
			}
			f.pool.Close()
			f.pool = nil
			f.open(t)
			intent, err := f.store.GetIntent(context.Background(), f.intent.ID)
			if err != nil || intent.State != Uncertain {
				t.Fatalf("durable unknown: %+v %v", intent, err)
			}
			if point == "after_rpc" {
				_, err = f.pool.Exec(context.Background(), `UPDATE transaction_intents SET
					lease_expires_at=clock_timestamp()-interval '1 second' WHERE intent_id=$1::uuid`, f.intent.ID)
				if err != nil {
					t.Fatal(err)
				}
			}
			seen, err := f.direct.TransactionByHash(context.Background(), id.Hash)
			if err != nil || seen == nil {
				t.Fatalf("Anvil acceptance missing: %v", err)
			}
			got, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
				[]BroadcastEndpoint{f.direct}, broadcastPolicy())
			if err != nil || got.State != Pending || got.Hash != id.Hash {
				t.Fatalf("recovery: %+v %v", got, err)
			}
			f.assertHistory(t, 2)
		})
	}
}

func TestReplacementDatabaseAndSignerGuards(t *testing.T) {
	f := newReplacementFixture(t)
	id, err := f.store.SignReplacement(context.Background(), f.intent.ID, f.signer,
		[]BroadcastEndpoint{f.direct}, replacementOptions())
	if err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		`UPDATE transaction_attempts SET raw_tx=decode('02','hex') WHERE attempt_id=$1::uuid`,
		`UPDATE transaction_attempts SET max_fee_per_gas=max_fee_per_gas+1 WHERE attempt_id=$1::uuid`,
		`UPDATE transaction_attempts SET nonce=nonce+1 WHERE attempt_id=$1::uuid`,
		`UPDATE transaction_attempts SET replaces_attempt_id=NULL WHERE attempt_id=$1::uuid`,
		`UPDATE transaction_attempts SET bump_bps=1 WHERE attempt_id=$1::uuid`,
	} {
		if _, err = f.pool.Exec(context.Background(), statement, id.AttemptID); err == nil {
			t.Fatalf("replacement mutation accepted: %s", statement)
		}
	}
	rogueID, _ := newID()
	for _, tc := range []struct {
		gas      int
		fee, tip string
	}{
		{21001, "3000000000", "1500000000"},
		{21000, "2300000001", "1150000001"},
	} {
		// The second case uses a nominally higher fee but an excessive bump
		// requirement, proving the database does not trust caller math.
		bps := 1500
		if tc.gas == 21000 {
			bps = 5000
		}
		_, err = f.pool.Exec(context.Background(), `INSERT INTO transaction_attempts
			(attempt_id,intent_id,attempt_number,nonce,status,gas_limit,max_fee_per_gas,
			max_priority_fee_per_gas,policy_snapshot_id,replaces_attempt_id,replacement_reason,bump_bps,eligible_at)
			VALUES($1::uuid,$2::uuid,3,0,'PREPARING',$3,$4::numeric,$5::numeric,'test-v1',
			$6::uuid,'PENDING_AGE',$7,now())`, rogueID, f.intent.ID, tc.gas, tc.fee, tc.tip, id.AttemptID, bps)
		if err == nil {
			t.Fatalf("rogue lineage accepted: %+v", tc)
		}
	}
	f.assertHistory(t, 2)

	bad := newReplacementFixture(t)
	mutated := alteredSigner{base: bad.signer, change: func(tx *types.Transaction) *types.Transaction {
		d := dynamicCopy(tx)
		d.Value.Add(d.Value, big.NewInt(1))
		return types.NewTx(d)
	}}
	_, err = bad.store.SignReplacement(context.Background(), bad.intent.ID, mutated,
		[]BroadcastEndpoint{bad.direct}, replacementOptions())
	var signing *SigningError
	if !errors.As(err, &signing) || signing.Code != "SIGNER_MUTATED_TRANSACTION" {
		t.Fatalf("mutated replacement accepted: %v", err)
	}
	var state State
	var raw []byte
	if err = bad.pool.QueryRow(context.Background(), `SELECT i.state,a.raw_tx FROM transaction_intents i
		JOIN transaction_attempts a ON a.intent_id=i.intent_id AND a.attempt_number=2
		WHERE i.intent_id=$1::uuid`, bad.intent.ID).Scan(&state, &raw); err != nil || state != Blocked || raw != nil {
		t.Fatalf("invalid replacement persisted: state=%s err=%v", state, err)
	}
}

func crashReplacementBroadcastChild(point string) {
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
	hooks := broadcastHooks{}
	if point == "after_rpc" {
		hooks.afterRPC = func() { os.Exit(9) }
	}
	if point == "after_unknown" {
		hooks.afterCommit = func() { os.Exit(9) }
	}
	_, _ = NewStore(pool).advanceBroadcast(context.Background(), os.Getenv("TXORCH_TEST_INTENT_ID"),
		[]BroadcastEndpoint{r}, broadcastPolicy(), hooks)
	os.Exit(2)
}
