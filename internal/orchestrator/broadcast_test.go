package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5/pgxpool"
)

func broadcastPolicy() BroadcastPolicy {
	return BroadcastPolicy{MaxSends: 3, MaxDuration: time.Minute, Backoff: 0,
		RPCDeadline: 300 * time.Millisecond, Lease: 10 * time.Second}
}

type broadcastFixture struct {
	*fixture
	direct *HTTPRPC
	intent Intent
	raw    SignedAttempt
}

func startAnvil(t *testing.T) *HTTPRPC {
	t.Helper()
	bin := os.Getenv("TXORCH_ANVIL_BINARY")
	if bin == "" {
		t.Skip("set TXORCH_ANVIL_BINARY to run local Anvil integration")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, bin, "--host", "127.0.0.1", "--port",
		new(big.Int).SetInt64(int64(port)).String(), "--chain-id", "31337", "--silent")
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); _ = cmd.Wait() })
	r := &HTTPRPC{Name: "direct", URL: "http://127.0.0.1:" + new(big.Int).SetInt64(int64(port)).String(),
		Client: &http.Client{Timeout: time.Second}}
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		check, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
		chain, err := r.ChainID(check)
		stop()
		if err == nil && chain.Cmp(big.NewInt(31337)) == 0 {
			return r
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("Anvil failed to start")
	return nil
}

func newBroadcastFixture(t *testing.T) *broadcastFixture {
	t.Helper()
	if os.Getenv("TXORCH_TEST_DATABASE_URL") == "" || os.Getenv("TXORCH_ANVIL_BINARY") == "" {
		t.Skip("PostgreSQL and Anvil are required")
	}
	f := newFixture(t)
	r := startAnvil(t)
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
	if err := f.store.ProvisionAccount(context.Background(), Account{ChainID: "31337", SignerID: "dev", Address: addr}); err != nil {
		t.Fatal(err)
	}
	var balance json.RawMessage
	if err := r.call(context.Background(), "anvil_setBalance", []any{address.Hex(), "0xde0b6b3a7640000"}, &balance); err != nil {
		t.Fatal(err)
	}
	req := testRequest("broadcast")
	req.SignerID = "dev"
	intent, err := f.store.CreateIntent(context.Background(), req, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if n, err := f.store.ReserveNonce(context.Background(), intent.ID); err != nil || n != 0 {
		t.Fatalf("nonce=%d: %v", n, err)
	}
	_, err = f.store.SignInitial(context.Background(), intent.ID, signer,
		FeeInput{MaxFeePerGasWei: "1000000000", MaxTipPerGasWei: "100000000"}, signingOptions())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := f.store.LoadSignedAttempt(context.Background(), intent.ID)
	if err != nil {
		t.Fatal(err)
	}
	return &broadcastFixture{fixture: f, direct: r, intent: intent, raw: raw}
}

func (f *broadcastFixture) counts(t *testing.T, wantSends int) {
	t.Helper()
	var intents, nonces, attempts, sends int
	err := f.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM transaction_intents WHERE intent_id=$1::uuid),
		(SELECT count(*) FROM nonce_reservations WHERE intent_id=$1::uuid),
		(SELECT count(*) FROM transaction_attempts WHERE intent_id=$1::uuid),
		(SELECT broadcast_send_count FROM transaction_intents WHERE intent_id=$1::uuid)`,
		f.intent.ID).Scan(&intents, &nonces, &attempts, &sends)
	if err != nil || intents != 1 || nonces != 1 || attempts != 1 || sends != wantSends {
		t.Fatalf("ledger intent=%d nonce=%d attempt=%d sends=%d: %v", intents, nonces, attempts, sends, err)
	}
	loaded, err := f.store.LoadSignedAttempt(context.Background(), f.intent.ID)
	if err != nil || loaded.Hash != f.raw.Hash || !bytes.Equal(loaded.Raw, f.raw.Raw) {
		t.Fatalf("signed material changed: %v", err)
	}
}

// The proxy forwards the actual request to Anvil before dropping its response.
func faultProxy(t *testing.T, upstream string, dropBefore bool) (*HTTPRPC, *atomic.Int64) {
	t.Helper()
	var sends atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(io.LimitReader(req.Body, 2<<20))
		var call struct {
			Method string `json:"method"`
		}
		_ = json.Unmarshal(body, &call)
		if call.Method == "eth_sendRawTransaction" {
			sends.Add(1)
			if dropBefore {
				conn, _, _ := w.(http.Hijacker).Hijack()
				conn.Close()
				return
			}
		}
		up, err := http.Post(upstream, "application/json", bytes.NewReader(body))
		if err != nil {
			w.WriteHeader(502)
			return
		}
		defer up.Body.Close()
		result, _ := io.ReadAll(io.LimitReader(up.Body, 2<<20))
		if call.Method == "eth_sendRawTransaction" {
			var response struct {
				Result string          `json:"result"`
				Error  json.RawMessage `json:"error"`
			}
			_ = json.Unmarshal(result, &response)
			if response.Result == "" || len(response.Error) != 0 {
				w.WriteHeader(502)
				return
			}
			conn, _, _ := w.(http.Hijacker).Hijack()
			conn.Close()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(result)
	}))
	t.Cleanup(srv.Close)
	return &HTTPRPC{Name: "proxy", URL: srv.URL, Client: &http.Client{Timeout: time.Second}}, &sends
}

func TestAnvilResponseLostAndRestart(t *testing.T) {
	f := newBroadcastFixture(t)
	proxy, sends := faultProxy(t, f.direct.URL, false)
	result, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
		[]BroadcastEndpoint{proxy, f.direct}, broadcastPolicy())
	if err != nil || result.State != Uncertain || result.LastOutcome != ResultTransport || sends.Load() != 1 {
		t.Fatalf("accepted response lost: %+v %v sends=%d", result, err, sends.Load())
	}
	visible, err := f.direct.TransactionByHash(context.Background(), f.raw.Hash)
	if err != nil || visible == nil || visible.Hash() != f.raw.Hash {
		t.Fatalf("Anvil did not accept exact hash: %v", err)
	}
	f.pool.Close()
	f.pool = nil
	f.open(t)
	result, err = f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
		[]BroadcastEndpoint{proxy, f.direct}, broadcastPolicy())
	if err != nil || result.State != Pending || sends.Load() != 1 {
		t.Fatalf("restart reconciliation: %+v %v", result, err)
	}
	f.counts(t, 1)
	metrics, err := f.store.BroadcastMetrics(context.Background())
	if err != nil || len(metrics) == 0 {
		t.Fatalf("durable broadcast metrics: %v", err)
	}
	var claims, sendsSeen, found int
	for _, m := range metrics {
		if strings.Contains(m.Endpoint, f.raw.Hash.Hex()) || m.Count < 1 {
			t.Fatalf("unbounded or invalid metric: %+v", m)
		}
		switch m.Kind {
		case "CLAIM":
			claims += int(m.Count)
		case "SEND":
			sendsSeen += int(m.Count)
		case "LOOKUP":
			found += int(m.Count)
		}
	}
	if claims < 2 || sendsSeen != 1 || found < 1 {
		t.Fatalf("incomplete durable history claim=%d send=%d lookup=%d", claims, sendsSeen, found)
	}
}

func TestAnvilRequestLostAndExactRebroadcast(t *testing.T) {
	f := newBroadcastFixture(t)
	proxy, sends := faultProxy(t, f.direct.URL, true)
	first, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
		[]BroadcastEndpoint{proxy, f.direct}, broadcastPolicy())
	if err != nil || first.State != Uncertain {
		t.Fatalf("request drop: %+v %v", first, err)
	}
	visible, err := f.direct.TransactionByHash(context.Background(), f.raw.Hash)
	if err != nil || visible != nil {
		t.Fatalf("request unexpectedly reached Anvil: %v", err)
	}
	second, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
		[]BroadcastEndpoint{proxy, f.direct}, broadcastPolicy())
	if err != nil || second.State != Pending || sends.Load() != 1 {
		t.Fatalf("exact fallback rebroadcast: %+v %v", second, err)
	}
	f.counts(t, 2)
}

type overrideEndpoint struct {
	BroadcastEndpoint
	alias    string
	chain    *big.Int
	chainErr error
	send     func(context.Context, []byte) (common.Hash, error)
	lookup   func(context.Context, common.Hash) (*types.Transaction, error)
	receipt  func(context.Context, common.Hash) (*ReceiptEvidence, error)
	nonce    func(context.Context, common.Address) (uint64, error)
}

func (e overrideEndpoint) Alias() string { return e.alias }
func (e overrideEndpoint) ChainID(ctx context.Context) (*big.Int, error) {
	if e.chainErr != nil {
		return nil, e.chainErr
	}
	if e.chain != nil {
		return e.chain, nil
	}
	return e.BroadcastEndpoint.ChainID(ctx)
}
func (e overrideEndpoint) SendRawTransaction(ctx context.Context, b []byte) (common.Hash, error) {
	if e.send != nil {
		return e.send(ctx, b)
	}
	return e.BroadcastEndpoint.SendRawTransaction(ctx, b)
}
func (e overrideEndpoint) TransactionByHash(ctx context.Context, h common.Hash) (*types.Transaction, error) {
	if e.lookup != nil {
		return e.lookup(ctx, h)
	}
	return e.BroadcastEndpoint.TransactionByHash(ctx, h)
}
func (e overrideEndpoint) ReceiptByHash(ctx context.Context, h common.Hash) (*ReceiptEvidence, error) {
	if e.receipt != nil {
		return e.receipt(ctx, h)
	}
	return e.BroadcastEndpoint.ReceiptByHash(ctx, h)
}
func (e overrideEndpoint) LatestNonce(ctx context.Context, a common.Address) (uint64, error) {
	if e.nonce != nil {
		return e.nonce(ctx, a)
	}
	return e.BroadcastEndpoint.LatestNonce(ctx, a)
}

func TestBroadcastOutcomesAndEligibility(t *testing.T) {
	t.Run("success and idempotent", func(t *testing.T) {
		f := newBroadcastFixture(t)
		p := broadcastPolicy()
		genesis, _ := f.direct.Genesis(context.Background())
		p.GenesisHash = genesis
		got, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, p)
		if err != nil || got.State != Pending || got.Hash != f.raw.Hash {
			t.Fatalf("send: %+v %v", got, err)
		}
		again, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, p)
		if err != nil || again.State != Pending {
			t.Fatalf("idempotent: %+v %v", again, err)
		}
		f.counts(t, 1)
	})
	t.Run("wrong chain never receives bytes", func(t *testing.T) {
		f := newBroadcastFixture(t)
		var sends atomic.Int64
		wrong := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "wrong", chain: big.NewInt(1),
			send: func(context.Context, []byte) (common.Hash, error) { sends.Add(1); return common.Hash{}, nil }}
		_, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{wrong}, broadcastPolicy())
		if !errors.Is(err, ErrNoEligibleEndpoint) || sends.Load() != 0 {
			t.Fatalf("wrong network sent: %v", err)
		}
		got, _ := f.store.GetIntent(context.Background(), f.intent.ID)
		if got.State != Ready {
			t.Fatal(got.State)
		}
	})
	t.Run("wrong response hash blocks", func(t *testing.T) {
		f := newBroadcastFixture(t)
		wrong := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "wrong-hash",
			send: func(context.Context, []byte) (common.Hash, error) { return common.HexToHash("0x01"), nil }}
		got, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{wrong}, broadcastPolicy())
		if err != nil || got.State != Blocked || got.LastOutcome != ResultHashMismatch {
			t.Fatalf("mismatch: %+v %v", got, err)
		}
		f.counts(t, 1)
	})
	for _, tc := range []struct {
		name, message string
		want          State
		latest        uint64
	}{
		{"fee", "transaction underpriced", Blocked, 0},
		{"nonce conflict", "nonce too low", Blocked, 1},
		{"nonce unresolved", "nonce too low", Uncertain, 0},
		{"provider", "unsupported method", Uncertain, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newBroadcastFixture(t)
			ep := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "classified",
				send: func(context.Context, []byte) (common.Hash, error) {
					return common.Hash{}, &RPCFault{Code: -32000, Message: tc.message}
				},
				nonce: func(context.Context, common.Address) (uint64, error) { return tc.latest, nil }}
			got, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{ep}, broadcastPolicy())
			if err != nil || got.State != tc.want {
				t.Fatalf("classified: %+v %v", got, err)
			}
			f.counts(t, 1)
			if tc.want == Blocked {
				req := testRequest("after-block")
				req.SignerID = "dev"
				other, err := f.store.CreateIntent(context.Background(), req, testPolicy())
				if err != nil {
					t.Fatal(err)
				}
				if _, err = f.store.ReserveNonce(context.Background(), other.ID); !errors.Is(err, ErrAccountPaused) {
					t.Fatalf("blocked nonce gap did not pause account: %v", err)
				}
			}
		})
	}
	t.Run("unavailable primary uses fallback", func(t *testing.T) {
		f := newBroadcastFixture(t)
		bad := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "unavailable", chainErr: io.EOF}
		got, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
			[]BroadcastEndpoint{bad, f.direct}, broadcastPolicy())
		if err != nil || got.State != Pending {
			t.Fatalf("fallback: %+v %v", got, err)
		}
		f.counts(t, 1)
	})
}

func TestAlreadyKnownFallbackReceiptAndNonceFound(t *testing.T) {
	for _, mode := range []string{"already known", "nonce found", "receipt"} {
		t.Run(mode, func(t *testing.T) {
			f := newBroadcastFixture(t)
			if mode == "receipt" {
				proxy, _ := faultProxy(t, f.direct.URL, true)
				first, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{proxy}, broadcastPolicy())
				if err != nil || first.State != Uncertain {
					t.Fatalf("setup unknown: %+v %v", first, err)
				}
			}
			if _, err := f.direct.SendRawTransaction(context.Background(), f.raw.Raw); err != nil {
				t.Fatal(err)
			}
			if mode == "receipt" {
				var mined json.RawMessage
				if err := f.direct.call(context.Background(), "evm_mine", []any{}, &mined); err != nil {
					t.Fatal(err)
				}
				receipt, err := f.direct.ReceiptByHash(context.Background(), f.raw.Hash)
				if err != nil || receipt == nil {
					t.Fatalf("Anvil receipt unavailable: %v", err)
				}
			}
			ep := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "not-found",
				lookup: func(context.Context, common.Hash) (*types.Transaction, error) { return nil, nil }}
			if mode == "receipt" {
				got, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{ep}, broadcastPolicy())
				if err != nil || got.State != Pending {
					t.Fatalf("receipt observation: %+v %v", got, err)
				}
				f.counts(t, 1)
				return
			}
			message := "already known"
			if mode == "nonce found" {
				message = "nonce too low"
			}
			ep.send = func(context.Context, []byte) (common.Hash, error) {
				return common.Hash{}, &RPCFault{Code: -32000, Message: message}
			}
			got, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{ep, f.direct}, broadcastPolicy())
			if err != nil || got.State != Pending {
				t.Fatalf("found via fallback: %+v %v", got, err)
			}
			f.counts(t, 1)
		})
	}
}

func TestBroadcastConcurrentWorkersAndBounds(t *testing.T) {
	f := newBroadcastFixture(t)
	var sends atomic.Int64
	ep := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "counted",
		send: func(ctx context.Context, b []byte) (common.Hash, error) {
			sends.Add(1)
			return f.direct.SendRawTransaction(ctx, b)
		}}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{ep}, broadcastPolicy())
			if err != nil && !errors.Is(err, ErrBroadcastInProgress) {
				t.Errorf("worker: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if sends.Load() != 1 {
		t.Fatalf("actual sends=%d", sends.Load())
	}
	f.counts(t, 1)

	other := newBroadcastFixture(t)
	proxy, _ := faultProxy(t, other.direct.URL, true)
	p := broadcastPolicy()
	p.MaxSends = 2
	for i := 0; i < 2; i++ {
		got, err := other.store.AdvanceBroadcast(context.Background(), other.intent.ID, []BroadcastEndpoint{proxy}, p)
		if err != nil || got.State != Uncertain {
			t.Fatalf("bounded send %d: %+v %v", i, got, err)
		}
	}
	got, err := other.store.AdvanceBroadcast(context.Background(), other.intent.ID, []BroadcastEndpoint{proxy}, p)
	if err != nil || got.State != Blocked {
		t.Fatalf("bounded stop: %+v %v", got, err)
	}
	other.counts(t, 2)
}

func TestTransportBackoffAndDuration(t *testing.T) {
	for _, failure := range []error{context.DeadlineExceeded, io.EOF, ErrRPCMalformed} {
		t.Run(string(ClassifySend(failure))+failure.Error(), func(t *testing.T) {
			f := newBroadcastFixture(t)
			ep := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "transport",
				send: func(context.Context, []byte) (common.Hash, error) { return common.Hash{}, failure }}
			p := broadcastPolicy()
			p.Backoff = time.Hour
			got, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{ep}, p)
			if err != nil || got.State != Uncertain {
				t.Fatalf("ambiguous send: %+v %v", got, err)
			}
			_, err = f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{ep}, p)
			if !errors.Is(err, ErrBroadcastBackoff) {
				t.Fatalf("backoff: %v", err)
			}
			_, err = f.pool.Exec(context.Background(), `UPDATE transaction_intents SET
				broadcast_first_claimed_at=clock_timestamp()-interval '2 hours',
				next_reconcile_at=clock_timestamp()-interval '1 second' WHERE intent_id=$1::uuid`, f.intent.ID)
			if err != nil {
				t.Fatal(err)
			}
			p.MaxDuration = time.Minute
			got, err = f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{ep}, p)
			if err != nil || got.State != Blocked {
				t.Fatalf("duration stop: %+v %v", got, err)
			}
			f.counts(t, 1)
		})
	}
}

func TestBroadcastProcessCrash(t *testing.T) {
	if point := os.Getenv("TXORCH_BROADCAST_CRASH_POINT"); point != "" {
		crashBroadcastChild(point)
		return
	}
	for _, point := range []string{"before_rpc", "after_rpc"} {
		t.Run(point, func(t *testing.T) {
			f := newBroadcastFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestBroadcastProcessCrash$")
			cmd.Env = append(os.Environ(), "TXORCH_BROADCAST_CRASH_POINT="+point,
				"TXORCH_TEST_SCHEMA="+f.schema, "TXORCH_TEST_INTENT_ID="+f.intent.ID,
				"TXORCH_ANVIL_URL="+f.direct.URL)
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 9 {
				t.Fatalf("child crash: %v %s", err, output)
			}
			f.pool.Close()
			f.pool = nil
			f.open(t)
			got, _ := f.store.GetIntent(context.Background(), f.intent.ID)
			if got.State != Uncertain {
				t.Fatalf("durable claim: %s", got.State)
			}
			_, err = f.pool.Exec(context.Background(), `UPDATE transaction_intents SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE intent_id=$1::uuid`, f.intent.ID)
			if err != nil {
				t.Fatal(err)
			}
			visible, err := f.direct.TransactionByHash(context.Background(), f.raw.Hash)
			if err != nil || (point == "after_rpc" && visible == nil) || (point == "before_rpc" && visible != nil) {
				t.Fatalf("node evidence: %v", err)
			}
			result, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{f.direct}, broadcastPolicy())
			if err != nil || result.State != Pending {
				t.Fatalf("restart recovery: %+v %v", result, err)
			}
			want := 1
			if point == "before_rpc" {
				want = 1
			}
			f.counts(t, want)
		})
	}
}

func crashBroadcastChild(point string) {
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
	if point == "before_rpc" {
		hooks.afterClaim = func() { os.Exit(9) }
	}
	if point == "after_rpc" {
		hooks.afterRPC = func() { os.Exit(9) }
	}
	_, _ = NewStore(pool).advanceBroadcast(context.Background(), os.Getenv("TXORCH_TEST_INTENT_ID"),
		[]BroadcastEndpoint{r}, broadcastPolicy(), hooks)
	os.Exit(2)
}

func TestSendClassificationAndCorruptBytes(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want ResultClass
	}{
		{context.DeadlineExceeded, ResultTransport}, {io.EOF, ResultTransport},
		{errors.New("connection reset by peer"), ResultTransport}, {ErrRPCMalformed, ResultProvider},
		{&RPCFault{Code: -32000, Message: "intrinsic gas too low"}, ResultRejected},
	} {
		if got := ClassifySend(tc.err); got != tc.want {
			t.Fatalf("class=%s want=%s", got, tc.want)
		}
	}
	f := newBroadcastFixture(t)
	_, err := f.pool.Exec(context.Background(), `ALTER TABLE transaction_attempts DISABLE TRIGGER signed_attempt_immutable`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.pool.Exec(context.Background(), `UPDATE transaction_attempts SET raw_tx=decode('02','hex') WHERE intent_id=$1::uuid`, f.intent.ID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.pool.Exec(context.Background(), `ALTER TABLE transaction_attempts ENABLE TRIGGER signed_attempt_immutable`)
	if err != nil {
		t.Fatal(err)
	}
	var sends atomic.Int64
	ep := overrideEndpoint{BroadcastEndpoint: f.direct, alias: "counted",
		send: func(context.Context, []byte) (common.Hash, error) { sends.Add(1); return common.Hash{}, nil }}
	_, err = f.store.AdvanceBroadcast(context.Background(), f.intent.ID, []BroadcastEndpoint{ep}, broadcastPolicy())
	var signingErr *SigningError
	if !errors.As(err, &signingErr) || sends.Load() != 0 {
		t.Fatalf("corrupt bytes sent: %v", err)
	}
}

func TestRPCMalformedResponse(t *testing.T) {
	r := &HTTPRPC{Name: "bad", URL: "http://invalid.local", Client: &http.Client{
		Transport: responseTransport{}, Timeout: time.Second}}
	_, err := r.SendRawTransaction(context.Background(), []byte{2})
	if !errors.Is(err, ErrRPCMalformed) || ClassifySend(err) != ResultProvider {
		t.Fatalf("malformed: %v", err)
	}
	if strings.Contains(err.Error(), "garbled") {
		t.Fatal("response leaked")
	}
}

type responseTransport struct{}

func (responseTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(
		`{"jsonrpc":"2.0","id":1,"result":"garbled`)), Header: make(http.Header)}, nil
}
