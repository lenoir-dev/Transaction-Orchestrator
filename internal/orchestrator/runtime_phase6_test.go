package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

type phase6Endpoint struct {
	BroadcastEndpoint
	name    string
	chain   func(context.Context) (*big.Int, error)
	head    func(context.Context) (*BlockEvidence, error)
	latest  func(context.Context, common.Address) (uint64, error)
	pending func(context.Context, common.Address) (uint64, error)
	send    func(context.Context, []byte) (common.Hash, error)
	tx      func(context.Context, common.Hash) (*types.Transaction, error)
}

func (e phase6Endpoint) Alias() string { return e.name }
func (e phase6Endpoint) ChainID(ctx context.Context) (*big.Int, error) {
	if e.chain != nil {
		return e.chain(ctx)
	}
	return e.BroadcastEndpoint.ChainID(ctx)
}
func (e phase6Endpoint) LatestBlock(ctx context.Context) (*BlockEvidence, error) {
	if e.head != nil {
		return e.head(ctx)
	}
	return e.BroadcastEndpoint.LatestBlock(ctx)
}
func (e phase6Endpoint) LatestNonce(ctx context.Context, a common.Address) (uint64, error) {
	if e.latest != nil {
		return e.latest(ctx, a)
	}
	return e.BroadcastEndpoint.LatestNonce(ctx, a)
}
func (e phase6Endpoint) PendingNonce(ctx context.Context, a common.Address) (uint64, error) {
	if e.pending != nil {
		return e.pending(ctx, a)
	}
	return e.BroadcastEndpoint.PendingNonce(ctx, a)
}
func (e phase6Endpoint) SendRawTransaction(ctx context.Context, raw []byte) (common.Hash, error) {
	if e.send != nil {
		return e.send(ctx, raw)
	}
	return e.BroadcastEndpoint.SendRawTransaction(ctx, raw)
}
func (e phase6Endpoint) TransactionByHash(ctx context.Context, h common.Hash) (*types.Transaction, error) {
	if e.tx != nil {
		return e.tx(ctx, h)
	}
	return e.BroadcastEndpoint.TransactionByHash(ctx, h)
}

func phase6Pool(t *testing.T, endpoints ...BroadcastEndpoint) *EndpointPool {
	t.Helper()
	var configs []EndpointConfig
	for _, ep := range endpoints {
		configs = append(configs, EndpointConfig{ID: ep.Alias(), RPC: ep, ChainID: "31337",
			Capabilities: EndpointRead | EndpointBroadcast | EndpointNonce, MaxInflight: 2})
	}
	p, err := NewEndpointPool(configs, EndpointPoolPolicy{ProbeTimeout: time.Second,
		Cooldown: time.Second, FailureLimit: 2, MaxHeadLag: 1, MaxRPCInflight: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err = p.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPhase6EndpointEligibilityAndRecovery(t *testing.T) {
	f := newBroadcastFixture(t)
	var wrongSends atomic.Int64
	wrong := phase6Endpoint{BroadcastEndpoint: f.direct, name: "wrong",
		chain: func(context.Context) (*big.Int, error) { return big.NewInt(1), nil },
		send: func(context.Context, []byte) (common.Hash, error) {
			wrongSends.Add(1)
			return common.Hash{}, errors.New("wrong chain was sent bytes")
		}}
	p := phase6Pool(t, wrong, f.direct)
	if got := p.Eligible("31337", EndpointRead|EndpointBroadcast|EndpointNonce); len(got) != 1 || got[0].Alias() != "direct" {
		t.Fatalf("wrong-chain eligibility: %+v", p.Status())
	}
	if p.Status()[0].State != EndpointIneligible || p.Status()[0].Reason != "WRONG_CHAIN" {
		t.Fatalf("wrong-chain status: %+v", p.Status())
	}
	result, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
		p.Eligible("31337", EndpointRead|EndpointBroadcast|EndpointNonce), broadcastPolicy())
	if err != nil || result.State != Pending || wrongSends.Load() != 0 {
		t.Fatalf("wrong-chain send: %+v %v count=%d", result, err, wrongSends.Load())
	}

	var recoverable atomic.Bool
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	temporary := phase6Endpoint{BroadcastEndpoint: f.direct, name: "temporary",
		chain: func(ctx context.Context) (*big.Int, error) {
			if !recoverable.Load() {
				return nil, context.DeadlineExceeded
			}
			return f.direct.ChainID(ctx)
		}}
	pool, err := NewEndpointPool([]EndpointConfig{{ID: "temporary", RPC: temporary,
		ChainID: "31337", Capabilities: EndpointRead | EndpointNonce, MaxInflight: 1}},
		EndpointPoolPolicy{ProbeTimeout: time.Second, Cooldown: time.Second,
			FailureLimit: 1, MaxHeadLag: 1, MaxRPCInflight: 1,
			Now: func() time.Time { return time.Unix(0, now.Load()) }})
	if err != nil {
		t.Fatal(err)
	}
	_ = pool.Refresh(context.Background())
	if pool.Status()[0].State != EndpointCooldown {
		t.Fatalf("cooldown: %+v", pool.Status())
	}
	recoverable.Store(true)
	_ = pool.Refresh(context.Background())
	if pool.Status()[0].State != EndpointCooldown {
		t.Fatal("cooldown ended without time advance")
	}
	now.Add(int64(2 * time.Second))
	_ = pool.Refresh(context.Background())
	if pool.Status()[0].State != EndpointHealthy {
		t.Fatalf("recovery: %+v", pool.Status())
	}
}

func TestPhase6ExternalNonceRace(t *testing.T) {
	f := newFixture(t)
	r := startAnvil(t)
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	address := crypto.PubkeyToAddress(key.PublicKey)
	var accountAddress [20]byte
	copy(accountAddress[:], address.Bytes())
	account := Account{ChainID: "31337", SignerID: "network", Address: accountAddress}
	if err = f.store.RegisterNetworkAccount(context.Background(), account); err != nil {
		t.Fatal(err)
	}
	intent, err := f.store.CreateIntent(context.Background(), Request{TenantID: "tenant", IdempotencyKey: "external-race",
		ChainID: "31337", SignerID: "network", To: testAddress, ValueWei: "1", Calldata: "0x", GasProfile: "standard"}, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ReserveNonce(context.Background(), intent.ID); !errors.Is(err, ErrAccountPaused) {
		t.Fatalf("unreconciled account reserved nonce: %v", err)
	}
	p := phase6Pool(t, r)
	status, err := f.store.ReconcileAccount(context.Background(), AccountKey{"31337", "network"}, p, time.Second)
	if err != nil || status.State != AccountReady || status.NextNonce != 0 {
		t.Fatalf("initial status %+v %v", status, err)
	}
	var funded json.RawMessage
	if err = r.call(context.Background(), "anvil_setBalance", []any{address.Hex(), "0x3635c9adc5dea00000"}, &funded); err != nil {
		t.Fatal(err)
	}
	to := common.HexToAddress(testAddress)
	tx := types.NewTx(&types.DynamicFeeTx{ChainID: big.NewInt(31337), Nonce: 0,
		Gas: 21000, GasFeeCap: big.NewInt(2_000_000_000), GasTipCap: big.NewInt(1_000_000_000),
		To: &to, Value: big.NewInt(1)})
	signed, err := types.SignTx(tx, types.NewLondonSigner(big.NewInt(31337)), key)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.SendRawTransaction(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ReserveReconciledNonce(context.Background(), intent.ID, p, time.Second); err == nil {
		t.Fatal("external nonce was allocated locally")
	}
	status, err = f.store.AccountStatus(context.Background(), AccountKey{"31337", "network"})
	if err != nil || status.State != AccountExternalNonce || status.NextNonce != 0 || status.PausedReason != "EXTERNAL_NONCE_CONFLICT" {
		t.Fatalf("external conflict %+v %v", status, err)
	}
	var reservations int
	if err = f.pool.QueryRow(context.Background(), `SELECT count(*) FROM nonce_reservations WHERE intent_id=$1::uuid`, intent.ID).Scan(&reservations); err != nil || reservations != 0 {
		t.Fatalf("reservation count %d: %v", reservations, err)
	}
}

func TestPhase6PendingExternalNonceOverlapsReservation(t *testing.T) {
	f := newFixture(t)
	r := startAnvil(t)
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	address := crypto.PubkeyToAddress(key.PublicKey)
	var accountAddress [20]byte
	copy(accountAddress[:], address.Bytes())
	accountKey := AccountKey{ChainID: "31337", SignerID: "pending-external"}
	if err = f.store.RegisterNetworkAccount(context.Background(), Account{
		ChainID: accountKey.ChainID, SignerID: accountKey.SignerID, Address: accountAddress}); err != nil {
		t.Fatal(err)
	}
	p := phase6Pool(t, r)
	if status, e := f.store.ReconcileAccount(context.Background(), accountKey, p, time.Second); e != nil || status.State != AccountReady {
		t.Fatalf("initial reconciliation %+v %v", status, e)
	}
	req := testRequest("pending-external")
	req.SignerID = accountKey.SignerID
	intent, err := f.store.CreateIntent(context.Background(), req, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if nonce, e := f.store.ReserveNonce(context.Background(), intent.ID); e != nil || nonce != 0 {
		t.Fatalf("local reservation=%d: %v", nonce, e)
	}
	var automine, balance json.RawMessage
	if err = r.call(context.Background(), "evm_setAutomine", []any{false}, &automine); err != nil {
		t.Fatal(err)
	}
	if err = r.call(context.Background(), "anvil_setBalance", []any{address.Hex(), "0x3635c9adc5dea00000"}, &balance); err != nil {
		t.Fatal(err)
	}
	to := common.HexToAddress(testAddress)
	tx := types.NewTx(&types.DynamicFeeTx{ChainID: big.NewInt(31337), Nonce: 0,
		Gas: 21000, GasFeeCap: big.NewInt(2_000_000_000), GasTipCap: big.NewInt(1_000_000_000),
		To: &to, Value: big.NewInt(1)})
	signed, err := types.SignTx(tx, types.NewLondonSigner(big.NewInt(31337)), key)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := signed.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.SendRawTransaction(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	status, err := f.store.ReconcileAccount(context.Background(), accountKey, p, time.Second)
	if err != nil || status.State != AccountExternalNonce ||
		status.Reason != "PENDING_OVERLAPS_UNSENT_RESERVATION" || status.NextNonce != 1 {
		t.Fatalf("pending external overlap %+v %v", status, err)
	}
	if _, err = f.store.ReserveNonce(context.Background(), intent.ID); err != nil {
		t.Fatalf("existing reservation changed: %v", err)
	}
	nextReq := testRequest("pending-external-next")
	nextReq.SignerID = accountKey.SignerID
	next, err := f.store.CreateIntent(context.Background(), nextReq, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ReserveNonce(context.Background(), next.ID); !errors.Is(err, ErrAccountPaused) {
		t.Fatalf("new reservation bypassed account block: %v", err)
	}
}

func TestPhase6ReconciliationRestartAndSeedGuard(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	var existing [20]byte
	copy(existing[:], firstAddress())
	if err := f.store.RegisterNetworkAccount(ctx, Account{ChainID: "31337", SignerID: "operator", Address: existing}); !errors.Is(err, ErrAccountConflict) {
		t.Fatalf("legacy seed was silently converted: %v", err)
	}
	r := startAnvil(t)
	var address [20]byte
	address[0] = 0x52
	key := AccountKey{ChainID: "31337", SignerID: "restart-account"}
	if err := f.store.RegisterNetworkAccount(ctx, Account{ChainID: key.ChainID, SignerID: key.SignerID, Address: address}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	var block atomic.Bool
	ep := phase6Endpoint{BroadcastEndpoint: r, name: "restart-rpc",
		pending: func(c context.Context, a common.Address) (uint64, error) {
			if block.Load() {
				close(entered)
				<-c.Done()
				return 0, c.Err()
			}
			return r.PendingNonce(c, a)
		}}
	p := phase6Pool(t, ep)
	block.Store(true)
	workCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, err := f.store.ReconcileAccount(workCtx, key, p, time.Second)
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("reconciliation did not reach RPC read")
	}
	status, err := f.store.AccountStatus(ctx, key)
	if err != nil || status.State != AccountReconciling || status.PausedReason != "NONCE_RECONCILIATION" {
		t.Fatalf("durable in-progress state %+v %v", status, err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled reconciliation: %v", err)
	}
	block.Store(false)
	f.pool.Close()
	f.open(t)
	restarted := f.store
	status, err = restarted.ReconcileAccount(ctx, key, phase6Pool(t, r), time.Second)
	if err != nil || status.State != AccountReady || !status.Initialized || status.NextNonce != 0 {
		t.Fatalf("reconciliation after restart %+v %v", status, err)
	}
}

func TestPhase6NonceDisagreementAndTerminalReceipt(t *testing.T) {
	t.Run("higher disagreement", func(t *testing.T) {
		f := newFixture(t)
		r := startAnvil(t)
		var address [20]byte
		address[0] = 0x42
		if err := f.store.RegisterNetworkAccount(context.Background(), Account{ChainID: "31337", SignerID: "disagree", Address: address}); err != nil {
			t.Fatal(err)
		}
		high := phase6Endpoint{BroadcastEndpoint: r, name: "high",
			latest:  func(context.Context, common.Address) (uint64, error) { return 2, nil },
			pending: func(context.Context, common.Address) (uint64, error) { return 2, nil }}
		p := phase6Pool(t, r, high)
		status, err := f.store.ReconcileAccount(context.Background(), AccountKey{"31337", "disagree"}, p, time.Second)
		if err != nil || status.State != AccountReconciling || status.Reason != "RPC_NONCE_DISAGREEMENT" || status.Initialized {
			t.Fatalf("disagreement %+v %v", status, err)
		}
	})
	t.Run("stale lower and terminal receipt", func(t *testing.T) {
		f := newReplacementFixture(t)
		for i := 0; i < 3; i++ {
			mineAnvil(t, f.direct)
		}
		if out := pollReceipt(t, f, receiptPolicy()); out.State != Confirmed {
			t.Fatalf("receipt %+v", out)
		}
		lower := phase6Endpoint{BroadcastEndpoint: f.direct, name: "lower",
			latest:  func(context.Context, common.Address) (uint64, error) { return 0, nil },
			pending: func(context.Context, common.Address) (uint64, error) { return 0, nil }}
		p := phase6Pool(t, lower, f.direct)
		status, err := f.store.ReconcileAccount(context.Background(), AccountKey{"31337", "replace"}, p, time.Second)
		if err != nil || status.State != AccountReconciling || status.NextNonce != 1 {
			t.Fatalf("lower provider regressed nonce %+v %v", status, err)
		}
		p = phase6Pool(t, f.direct)
		status, err = f.store.ReconcileAccount(context.Background(), AccountKey{"31337", "replace"}, p, time.Second)
		if err != nil || status.State != AccountReady || status.NextNonce != 1 || status.LatestNonce != 1 {
			t.Fatalf("canonical receipt did not reconcile %+v %v", status, err)
		}
	})
}

func TestPhase6MultiRPCBroadcastAndLookup(t *testing.T) {
	f := newBroadcastFixture(t)
	var automine json.RawMessage
	if err := f.direct.call(context.Background(), "evm_setAutomine", []any{false}, &automine); err != nil {
		t.Fatal(err)
	}
	proxy, sends := faultProxy(t, f.direct.URL, true)
	p := phase6Pool(t, proxy, f.direct)
	eligible := p.Eligible("31337", EndpointRead|EndpointBroadcast|EndpointNonce)
	first, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, eligible, broadcastPolicy())
	if err != nil || first.State != Uncertain || sends.Load() != 1 {
		t.Fatalf("first %+v %v", first, err)
	}
	second, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID, eligible, broadcastPolicy())
	if err != nil || second.State != Pending || second.Hash != f.raw.Hash {
		t.Fatalf("fallback %+v %v", second, err)
	}
	f.counts(t, 2)
	missing := phase6Endpoint{BroadcastEndpoint: f.direct, name: "missing",
		tx: func(context.Context, common.Hash) (*types.Transaction, error) { return nil, nil }}
	lookup := phase6Pool(t, missing, f.direct)
	winner, err := f.store.ObserveWinner(context.Background(), f.intent.ID,
		lookup.Eligible("31337", EndpointRead|EndpointNonce), broadcastPolicy())
	if err != nil || winner != 0 {
		t.Fatalf("lookup fallback winner=%d: %v", winner, err)
	}
	f.counts(t, 2)
}

func phase6RuntimeConfig() RuntimeConfig {
	return RuntimeConfig{Workers: 4, BatchSize: 16, StartupAccountBatch: 100,
		PollInterval: 50 * time.Millisecond, WorkTimeout: 20 * time.Second,
		AccountRPCDeadline: time.Second, AccountRefreshInterval: time.Minute,
		RetryBase: 100 * time.Millisecond,
		RetryMax:  time.Second, Broadcast: broadcastPolicy(), Receipt: receiptPolicy(),
		Replacement: replacementOptions(), Now: func() time.Time {
			return time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
		}}
}

func TestPhase6RuntimeRestartAndShutdown(t *testing.T) {
	t.Run("uncertain and confirming restart", func(t *testing.T) {
		f := newBroadcastFixture(t)
		proxy, _ := faultProxy(t, f.direct.URL, true)
		first, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
			[]BroadcastEndpoint{proxy}, broadcastPolicy())
		if err != nil || first.State != Uncertain {
			t.Fatalf("uncertain %+v %v", first, err)
		}
		p := phase6Pool(t, f.direct)
		runtime1, err := NewRuntime(f.store, p, phase6RuntimeConfig(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = runtime1.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		intent, err := f.store.GetIntent(context.Background(), f.intent.ID)
		if err != nil || intent.State != Pending {
			t.Fatalf("pending %+v %v", intent, err)
		}
		mineAnvil(t, f.direct)
		runtime2, err := NewRuntime(f.store, p, phase6RuntimeConfig(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = runtime2.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		status, err := f.store.ConfirmationStatus(context.Background(), f.intent.ID)
		if err != nil || status.State != Mined {
			t.Fatalf("mined %+v %v", status, err)
		}
		if err = runtime2.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		status, err = f.store.ConfirmationStatus(context.Background(), f.intent.ID)
		if err != nil || status.State != Confirming {
			t.Fatalf("confirming %+v %v", status, err)
		}
		mineAnvil(t, f.direct)
		mineAnvil(t, f.direct)
		runtime3, err := NewRuntime(f.store, p, phase6RuntimeConfig(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = runtime3.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		status, err = f.store.ConfirmationStatus(context.Background(), f.intent.ID)
		if err != nil || status.State != Confirmed {
			t.Fatalf("confirmed %+v %v", status, err)
		}
		f.counts(t, 2)
	})
	t.Run("cancel in flight send", func(t *testing.T) {
		f := newBroadcastFixture(t)
		started := make(chan struct{})
		var calls atomic.Int64
		slow := phase6Endpoint{BroadcastEndpoint: f.direct, name: "slow",
			send: func(ctx context.Context, _ []byte) (common.Hash, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-ctx.Done()
				return common.Hash{}, ctx.Err()
			}}
		p := phase6Pool(t, slow)
		runtime, err := NewRuntime(f.store, p, phase6RuntimeConfig(), nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- runtime.Run(ctx) }()
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("send did not start")
		}
		cancel()
		select {
		case err = <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("workers did not stop")
		}
		if runtime.active.Load() != 0 || p.Status()[0].Inflight != 0 {
			t.Fatalf("inflight after shutdown: runtime=%d rpc=%+v", runtime.active.Load(), p.Status())
		}
	})
}

func TestPhase6SignerAndChainIsolation(t *testing.T) {
	f := newFixture(t)
	r := startAnvil(t)
	var second [20]byte
	second[0] = 0x32
	if err := f.store.ProvisionAccount(context.Background(), Account{ChainID: "31337", SignerID: "second", Address: second}); err != nil {
		t.Fatal(err)
	}
	first, err := f.store.CreateIntent(context.Background(), testRequest("first-chain"), testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	secondReq := testRequest("second-signer")
	secondReq.SignerID = "second"
	other, err := f.store.CreateIntent(context.Background(), secondReq, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ReserveNonce(context.Background(), first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ReserveNonce(context.Background(), other.ID); err != nil {
		t.Fatal(err)
	}
	if err = f.store.ProvisionAccount(context.Background(), Account{ChainID: "31338", SignerID: "operator", Address: [20]byte{0x11}}); err != nil {
		t.Fatal(err)
	}
	chainB := testRequest("chain-b")
	chainB.ChainID = "31338"
	third, err := f.store.CreateIntent(context.Background(), chainB, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if nonce, e := f.store.ReserveNonce(context.Background(), third.ID); e != nil || nonce != 0 {
		t.Fatalf("chain B nonce=%d: %v", nonce, e)
	}
	conflict := phase6Endpoint{BroadcastEndpoint: r, name: "conflict",
		latest: func(_ context.Context, a common.Address) (uint64, error) {
			if a == common.BytesToAddress(firstAddress()) {
				return 2, nil
			}
			return 0, nil
		},
		pending: func(_ context.Context, a common.Address) (uint64, error) {
			if a == common.BytesToAddress(firstAddress()) {
				return 2, nil
			}
			return 0, nil
		}}
	p := phase6Pool(t, conflict)
	status, err := f.store.ReconcileAccount(context.Background(), AccountKey{"31337", "operator"}, p, time.Second)
	if err != nil || status.State != AccountExternalNonce {
		t.Fatalf("first signer %+v %v", status, err)
	}
	if _, err = f.store.ReserveNonce(context.Background(), first.ID); err != nil {
		t.Fatal(err)
	}
	secondStatus, err := f.store.AccountStatus(context.Background(), AccountKey{"31337", "second"})
	if err != nil || secondStatus.NextNonce != 1 || secondStatus.PausedReason != "" {
		t.Fatalf("second signer %+v %v", secondStatus, err)
	}
	thirdStatus, err := f.store.AccountStatus(context.Background(), AccountKey{"31338", "operator"})
	if err != nil || thirdStatus.NextNonce != 1 || thirdStatus.PausedReason != "" {
		t.Fatalf("second chain %+v %v", thirdStatus, err)
	}
	newReq := testRequest("second-continues")
	newReq.SignerID = "second"
	continued, err := f.store.CreateIntent(context.Background(), newReq, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if nonce, e := f.store.ReserveNonce(context.Background(), continued.ID); e != nil || nonce != 1 {
		t.Fatalf("second progression=%d: %v", nonce, e)
	}
}

func firstAddress() []byte {
	b := make([]byte, 20)
	for i := range b {
		b[i] = 0x11
	}
	return b
}

func TestPhase6BoundedRuntimeLoad(t *testing.T) {
	f := newFixture(t)
	r := startAnvil(t)
	const count = 12
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		key, err := crypto.GenerateKey()
		if err != nil {
			t.Fatal(err)
		}
		signer, err := NewDevLocalSigner(key)
		if err != nil {
			t.Fatal(err)
		}
		address, _ := signer.Address(context.Background())
		var rawAddress [20]byte
		copy(rawAddress[:], address.Bytes())
		name := fmt.Sprintf("load-%d", i)
		if err = f.store.ProvisionAccount(context.Background(), Account{ChainID: "31337", SignerID: name, Address: rawAddress}); err != nil {
			t.Fatal(err)
		}
		var funded json.RawMessage
		if err = r.call(context.Background(), "anvil_setBalance", []any{address.Hex(), "0x3635c9adc5dea00000"}, &funded); err != nil {
			t.Fatal(err)
		}
		req := testRequest(name)
		req.SignerID = name
		intent, err := f.store.CreateIntent(context.Background(), req, testPolicy())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.store.ReserveNonce(context.Background(), intent.ID); err != nil {
			t.Fatal(err)
		}
		if _, err = f.store.SignInitial(context.Background(), intent.ID, signer,
			FeeInput{MaxFeePerGasWei: "1000000000", MaxTipPerGasWei: "100000000"}, signingOptions()); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, intent.ID)
	}
	var inflight, peak atomic.Int64
	counted := phase6Endpoint{BroadcastEndpoint: r, name: "counted",
		send: func(ctx context.Context, raw []byte) (common.Hash, error) {
			active := inflight.Add(1)
			for {
				old := peak.Load()
				if active <= old || peak.CompareAndSwap(old, active) {
					break
				}
			}
			defer inflight.Add(-1)
			return r.SendRawTransaction(ctx, raw)
		}}
	p := phase6Pool(t, counted)
	cfg := phase6RuntimeConfig()
	cfg.BatchSize = 4
	runtime, err := NewRuntime(f.store, p, cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err = runtime.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range ids {
		intent, e := f.store.GetIntent(context.Background(), id)
		if e != nil || intent.State == Ready || intent.Nonce == nil || *intent.Nonce != 0 {
			t.Fatalf("load intent %+v %v", intent, e)
		}
		var attempts, reservations int
		if e = f.pool.QueryRow(context.Background(), `SELECT
			(SELECT count(*) FROM transaction_attempts WHERE intent_id=$1::uuid),
			(SELECT count(*) FROM nonce_reservations WHERE intent_id=$1::uuid)`, id).
			Scan(&attempts, &reservations); e != nil || attempts != 1 || reservations != 1 {
			t.Fatalf("load ledger %d/%d: %v", attempts, reservations, e)
		}
	}
	if peak.Load() > 2 || inflight.Load() != 0 || runtime.active.Load() != 0 {
		t.Fatalf("concurrency bounds peak=%d inflight=%d workers=%d", peak.Load(), inflight.Load(), runtime.active.Load())
	}
}

func TestPhase6NewAccountPendingInitialization(t *testing.T) {
	f := newFixture(t)
	r := startAnvil(t)
	var address [20]byte
	address[0] = 0x64
	if err := f.store.RegisterNetworkAccount(context.Background(), Account{
		ChainID: "31337", SignerID: "pending-seed", Address: address}); err != nil {
		t.Fatal(err)
	}
	view := phase6Endpoint{BroadcastEndpoint: r, name: "pending-view",
		latest:  func(context.Context, common.Address) (uint64, error) { return 2, nil },
		pending: func(context.Context, common.Address) (uint64, error) { return 3, nil }}
	p := phase6Pool(t, view)
	status, err := f.store.ReconcileAccount(context.Background(), AccountKey{"31337", "pending-seed"}, p, time.Second)
	if err != nil || status.State != AccountReady || !status.Initialized || status.NextNonce != 3 ||
		status.LatestNonce != 2 || status.PendingNonce != 3 {
		t.Fatalf("pending initialization %+v %v", status, err)
	}
	req := testRequest("pending-seed")
	req.SignerID = "pending-seed"
	intent, err := f.store.CreateIntent(context.Background(), req, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if nonce, e := f.store.ReserveNonce(context.Background(), intent.ID); e != nil || nonce != 3 {
		t.Fatalf("initialized nonce=%d: %v", nonce, e)
	}
}

func TestPhase6StaleEndpointExclusion(t *testing.T) {
	f := newReplacementFixture(t)
	mineAnvil(t, f.direct)
	mineAnvil(t, f.direct)
	genesis, err := f.direct.BlockByNumber(context.Background(), 0)
	if err != nil || genesis == nil {
		t.Fatal(err)
	}
	stale := phase6Endpoint{BroadcastEndpoint: f.direct, name: "stale",
		head: func(context.Context) (*BlockEvidence, error) { return genesis, nil }}
	p := phase6Pool(t, stale, f.direct)
	if status := p.Status(); status[0].State != EndpointDegraded || status[0].Reason != "STALE_HEAD" {
		t.Fatalf("stale endpoint %+v", status)
	}
	reads := p.Eligible("31337", EndpointRead|EndpointNonce)
	if len(reads) != 1 || reads[0].Alias() != "direct" {
		t.Fatalf("eligible reads %+v", p.Status())
	}
	policy := receiptPolicy()
	confirmed, err := f.store.ReconcileReceipts(context.Background(), f.intent.ID, reads, policy)
	if err != nil || confirmed.WinningAttemptNumber != 1 || confirmed.Confirmations != 2 {
		t.Fatalf("fresh receipt %+v %v", confirmed, err)
	}
}

func TestPhase6MultiRPCLookupFindsExactAttempt(t *testing.T) {
	f := newBroadcastFixture(t)
	proxy, _ := faultProxy(t, f.direct.URL, true)
	first, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
		[]BroadcastEndpoint{proxy}, broadcastPolicy())
	if err != nil || first.State != Uncertain {
		t.Fatalf("uncertain %+v %v", first, err)
	}
	if _, err = f.direct.SendRawTransaction(context.Background(), f.raw.Raw); err != nil {
		t.Fatal(err)
	}
	missing := phase6Endpoint{BroadcastEndpoint: f.direct, name: "missing",
		tx: func(context.Context, common.Hash) (*types.Transaction, error) { return nil, nil }}
	p := phase6Pool(t, missing, f.direct)
	result, err := f.store.AdvanceBroadcast(context.Background(), f.intent.ID,
		p.Eligible("31337", EndpointRead|EndpointBroadcast|EndpointNonce), broadcastPolicy())
	if err != nil || result.State != Pending || result.Hash != f.raw.Hash || result.SendCount != 1 {
		t.Fatalf("lookup fallback %+v %v", result, err)
	}
	f.counts(t, 1)
}

func TestPhase6ReplacementAndReorgStartup(t *testing.T) {
	t.Run("eligible replacement", func(t *testing.T) {
		f := newReplacementFixture(t)
		p := phase6Pool(t, f.direct)
		cfg := phase6RuntimeConfig()
		signer := func(key AccountKey) TransactionSigner {
			if key.SignerID == "replace" {
				return f.signer
			}
			return nil
		}
		first, err := NewRuntime(f.store, p, cfg, signer)
		if err != nil {
			t.Fatal(err)
		}
		if err = first.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		intent, err := f.store.GetIntent(context.Background(), f.intent.ID)
		if err != nil || intent.State != Ready {
			t.Fatalf("replacement ready %+v %v", intent, err)
		}
		f.assertHistory(t, 2)
		restarted, err := NewRuntime(f.store, p, cfg, signer)
		if err != nil {
			t.Fatal(err)
		}
		if err = restarted.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		f.assertHistory(t, 2)
	})
	t.Run("reorg evidence", func(t *testing.T) {
		f := newReplacementFixture(t)
		base := snapshotAnvil(t, f.direct)
		mineAnvil(t, f.direct)
		if out := pollReceipt(t, f, receiptPolicy()); out.State != Mined {
			t.Fatalf("mined %+v", out)
		}
		revertAnvil(t, f.direct, base)
		branchWithoutReceipt(t, f.direct, f.raw.Hash, 1)
		p := phase6Pool(t, f.direct)
		runtime, err := NewRuntime(f.store, p, phase6RuntimeConfig(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err = runtime.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
		out, err := f.store.ConfirmationStatus(context.Background(), f.intent.ID)
		if err != nil || out.State != Reorged || out.WinningAttemptNumber != 0 {
			t.Fatalf("reorg restart %+v %v", out, err)
		}
		assertReceiptLedger(t, f, 1)
	})
}

func TestPhase6RuntimeRetrySchedule(t *testing.T) {
	f := newBroadcastFixture(t)
	when := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := f.store.RecordRuntimeOutcome(context.Background(), f.intent.ID, when,
		ErrNoEligibleEndpoint, 100*time.Millisecond, time.Second); err != nil {
		t.Fatal(err)
	}
	work, err := f.store.LifecycleWorkAfter(context.Background(), when, "", 10)
	if err != nil || len(work) != 0 {
		t.Fatalf("retry scheduled too early %v %v", work, err)
	}
	work, err = f.store.LifecycleWorkAfter(context.Background(), when.Add(100*time.Millisecond), "", 10)
	if err != nil || len(work) != 1 {
		t.Fatalf("retry not due %v %v", work, err)
	}
	if err = f.store.RecordRuntimeOutcome(context.Background(), f.intent.ID, when.Add(100*time.Millisecond),
		ErrNoEligibleEndpoint, 100*time.Millisecond, time.Second); err != nil {
		t.Fatal(err)
	}
	work, err = f.store.LifecycleWorkAfter(context.Background(), when.Add(299*time.Millisecond), "", 10)
	if err != nil || len(work) != 0 {
		t.Fatalf("exponential backoff %v %v", work, err)
	}
	if err = f.store.RecordRuntimeOutcome(context.Background(), f.intent.ID, when, nil,
		100*time.Millisecond, time.Second); err != nil {
		t.Fatal(err)
	}
	work, err = f.store.LifecycleWorkAfter(context.Background(), when, "", 10)
	if err != nil || len(work) != 1 {
		t.Fatalf("success did not clear retry %v %v", work, err)
	}
}

func TestPhase6SignerUnavailableIsolation(t *testing.T) {
	f := newReplacementFixture(t)
	p := phase6Pool(t, f.direct)
	signer := func(key AccountKey) TransactionSigner {
		if key.SignerID == "replace" {
			return f.signer
		}
		return nil
	}
	runtime, err := NewRuntime(f.store, p, phase6RuntimeConfig(), signer)
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	missing, err := f.store.AccountStatus(context.Background(), AccountKey{"31337", "operator"})
	if err != nil || missing.SignerStatus != "UNAVAILABLE" || missing.PausedReason != "SIGNER_UNAVAILABLE" {
		t.Fatalf("missing signer %+v %v", missing, err)
	}
	healthy, err := f.store.AccountStatus(context.Background(), AccountKey{"31337", "replace"})
	if err != nil || healthy.SignerStatus != "AVAILABLE" || healthy.PausedReason != "" {
		t.Fatalf("healthy signer %+v %v", healthy, err)
	}
	request := testRequest("healthy-continues")
	request.SignerID = "replace"
	intent, err := f.store.CreateIntent(context.Background(), request, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if nonce, e := f.store.ReserveNonce(context.Background(), intent.ID); e != nil || nonce != 1 {
		t.Fatalf("healthy account progression=%d: %v", nonce, e)
	}
}

func TestPhase6RuntimeStatusProjection(t *testing.T) {
	f := newBroadcastFixture(t)
	p := phase6Pool(t, f.direct)
	runtime, err := NewRuntime(f.store, p, phase6RuntimeConfig(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.store.SetSignerHealth(context.Background(), AccountKey{"31337", "operator"}, false, "SIGNER_UNAVAILABLE"); err != nil {
		t.Fatal(err)
	}
	status, err := runtime.Status(context.Background())
	if err != nil || status.ActiveWorkers != 0 || status.Queued != 0 ||
		status.Backlog[Ready] != 1 || status.AccountStates[AccountTrustedSeed] != 2 ||
		status.BlockedReasons["SIGNER_UNAVAILABLE"] != 1 || len(status.Chains) != 1 ||
		status.Chains[0].EligibleRPCCount != 1 {
		t.Fatalf("runtime status %+v %v", status, err)
	}
	eps := p.Eligible("31337", EndpointRead|EndpointNonce)
	if len(eps) != 1 {
		t.Fatal("missing eligible read endpoint")
	}
	if _, err = eps[0].LatestNonce(context.Background(), common.Address{}); err != nil {
		t.Fatal(err)
	}
	metrics := p.Metrics()
	if len(metrics) != 1 || metrics[0].Endpoint != "direct" ||
		metrics[0].Method != "eth_getTransactionCount_latest" || metrics[0].Status != "ok" ||
		metrics[0].Count != 1 {
		t.Fatalf("bounded RPC labels %+v", metrics)
	}
}
