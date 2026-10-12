package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

func percentile(samples []float64, percent int) float64 {
	copySamples := append([]float64(nil), samples...)
	sort.Float64s(copySamples)
	index := (len(copySamples)*percent + 99) / 100
	if index < 1 {
		index = 1
	}
	return copySamples[index-1]
}

func processCPU() float64 {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return 0
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) +
		float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func TestPhase7BenchmarkSmoke(t *testing.T) {
	f := newAPIFixture(t)
	ctx := context.Background()
	key, err := crypto.HexToECDSA(strings.Repeat("0", 63) + "2")
	if err != nil {
		t.Fatal(err)
	}
	secondSigner, err := NewDevLocalSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	address, err := secondSigner.Address(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var raw [20]byte
	copy(raw[:], address.Bytes())
	if err := f.store.RegisterNetworkAccount(ctx, Account{ChainID: "31337", SignerID: "api2", Address: raw}); err != nil {
		t.Fatal(err)
	}
	var funded json.RawMessage
	if err := f.rpc.call(ctx, "anvil_setBalance", []any{address.Hex(), "0x3635c9adc5dea00000"}, &funded); err != nil {
		t.Fatal(err)
	}
	lookup := func(account AccountKey) TransactionSigner {
		if account.ChainID != "31337" {
			return nil
		}
		switch account.SignerID {
		case "api":
			return f.signer
		case "api2":
			return secondSigner
		}
		return nil
	}
	f.runtime, err = NewRuntime(f.store, f.pool, phase6RuntimeConfig(), lookup)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	f.server.Close()
	cfg := f.api.cfg
	cfg.Runtime, cfg.Signer = f.runtime, lookup
	cfg.Accounts = []AccountKey{{ChainID: "31337", SignerID: "api"}, {ChainID: "31337", SignerID: "api2"}}
	f.api, err = NewHTTPAPI(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(f.api.Handler())
	t.Cleanup(f.server.Close)
	var automine json.RawMessage
	if err := f.rpc.call(ctx, "evm_setAutomine", []any{false}, &automine); err != nil {
		t.Fatal(err)
	}
	const intents = 12
	const signerCount = 2
	const unknownCount = 3
	const replacementCount = 3
	startCPU := processCPU()
	start := time.Now()
	type admitted struct {
		view IntentProjection
		ms   float64
		err  string
	}
	results := make([]admitted, intents)
	var wg sync.WaitGroup
	for i := 0; i < intents; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			signerID := "api"
			if i%signerCount == 1 {
				signerID = "api2"
			}
			body := apiBody("1")
			if signerID == "api2" {
				body = fmt.Sprintf(`{"chain_id":"31337","signer_id":"api2","to":"%s","value_wei":"1","calldata":"0x","gas_policy":{"profile":"standard"}}`, testAddress)
			}
			begin := time.Now()
			response, data := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token,
				fmt.Sprintf("benchmark-%02d", i), body)
			results[i].ms = float64(time.Since(begin).Microseconds()) / 1000
			if response.StatusCode != 201 {
				results[i].err = fmt.Sprintf("create %d: %s", response.StatusCode, data)
				return
			}
			results[i].view = decodeView(t, data)
		}(i)
	}
	wg.Wait()
	createSamples := make([]float64, 0, intents)
	for i, result := range results {
		if result.err != "" || result.view.IntentID == "" {
			t.Fatalf("admission %d: %s", i, result.err)
		}
		createSamples = append(createSamples, result.ms)
	}
	proxy, lostResponses := faultProxy(t, f.rpc.URL, false)
	reconcileSamples := make([]float64, 0, unknownCount)
	for i := range results {
		id := results[i].view.IntentID
		if i < unknownCount {
			out, err := f.store.AdvanceBroadcast(ctx, id, []BroadcastEndpoint{proxy}, broadcastPolicy())
			if err != nil || out.State != Uncertain {
				t.Fatalf("unknown %d: %+v %v", i, out, err)
			}
			begin := time.Now()
			out, err = f.store.AdvanceBroadcast(ctx, id, []BroadcastEndpoint{f.rpc}, broadcastPolicy())
			reconcileSamples = append(reconcileSamples, float64(time.Since(begin).Microseconds())/1000)
			if err != nil || out.State != Pending {
				t.Fatalf("unknown recovery %d: %+v %v", i, out, err)
			}
		} else {
			out, err := f.store.AdvanceBroadcast(ctx, id, []BroadcastEndpoint{f.rpc}, broadcastPolicy())
			if err != nil || out.State != Pending {
				t.Fatalf("normal broadcast %d: %+v %v", i, out, err)
			}
		}
		if i >= unknownCount && i < unknownCount+replacementCount {
			signer := f.signer
			if results[i].view.SignerID == "api2" {
				signer = secondSigner
			}
			attempt, err := f.store.SignReplacement(ctx, id, signer, []BroadcastEndpoint{f.rpc}, replacementOptions())
			if err != nil || attempt.AttemptNumber != 2 {
				t.Fatalf("replacement %d: %+v %v", i, attempt, err)
			}
			out, err := f.store.AdvanceBroadcast(ctx, id, []BroadcastEndpoint{f.rpc}, broadcastPolicy())
			if err != nil || out.State != Pending {
				t.Fatalf("replacement send %d: %+v %v", i, out, err)
			}
		}
	}
	if lostResponses.Load() != unknownCount {
		t.Fatalf("unknown fault profile sends=%d", lostResponses.Load())
	}
	confirmationStart := time.Now()
	for i := 0; i < 3; i++ {
		mineAnvil(t, f.rpc)
	}
	for _, admitted := range results {
		out, err := f.store.ReconcileReceipts(ctx, admitted.view.IntentID, []BroadcastEndpoint{f.rpc}, receiptPolicy())
		if err != nil || out.State != Confirmed || out.Confirmations != 3 {
			t.Fatalf("confirmation %s: %+v %v", admitted.view.IntentID, out, err)
		}
	}
	confirmationMS := float64(time.Since(confirmationStart).Microseconds()) / 1000
	var actualIntents, actualReservations, actualAttempts, signedAttempts, confirmed int
	var duplicateNonces int
	err = f.fixture.pool.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM transaction_intents WHERE tenant_id='client-a' AND idempotency_key LIKE 'benchmark-%'),
		(SELECT count(*) FROM nonce_reservations n JOIN transaction_intents i USING(intent_id) WHERE i.tenant_id='client-a' AND i.idempotency_key LIKE 'benchmark-%'),
		(SELECT count(*) FROM transaction_attempts a JOIN transaction_intents i USING(intent_id) WHERE i.tenant_id='client-a' AND i.idempotency_key LIKE 'benchmark-%'),
		(SELECT count(*) FROM transaction_attempts a JOIN transaction_intents i USING(intent_id) WHERE i.tenant_id='client-a' AND i.idempotency_key LIKE 'benchmark-%' AND a.raw_tx IS NOT NULL),
		(SELECT count(*) FROM transaction_intents WHERE tenant_id='client-a' AND idempotency_key LIKE 'benchmark-%' AND state='CONFIRMED'),
		(SELECT count(*) FROM (SELECT chain_id,signer_address,nonce FROM nonce_reservations GROUP BY chain_id,signer_address,nonce HAVING count(*)>1) duplicates)`).
		Scan(&actualIntents, &actualReservations, &actualAttempts, &signedAttempts, &confirmed, &duplicateNonces)
	if err != nil || actualIntents != intents || actualReservations != intents || actualAttempts != intents+replacementCount ||
		signedAttempts != actualAttempts || confirmed != intents || duplicateNonces != 0 {
		t.Fatalf("invalid benchmark ledger %d/%d/%d signed=%d confirmed=%d duplicate=%d: %v",
			actualIntents, actualReservations, actualAttempts, signedAttempts, confirmed, duplicateNonces, err)
	}
	for _, admitted := range results {
		var rawCount int
		if err := f.fixture.pool.QueryRow(ctx, `SELECT count(*) FROM transaction_attempts WHERE intent_id=$1::uuid AND raw_tx IS NOT NULL`,
			admitted.view.IntentID).Scan(&rawCount); err != nil || rawCount < 1 {
			t.Fatalf("lost signed attempt: %d %v", rawCount, err)
		}
	}
	input := fmt.Sprintf("intents=%d;reservations=%d;attempts=%d;confirmed=%d;duplicate_nonces=%d",
		actualIntents, actualReservations, actualAttempts, confirmed, duplicateNonces)
	sum := sha256.Sum256([]byte(input))
	expected := sha256.Sum256([]byte("intents=12;reservations=12;attempts=15;confirmed=12;duplicate_nonces=0"))
	if sum != expected {
		t.Fatalf("benchmark checksum %x != %x", sum, expected)
	}
	var heap runtime.MemStats
	runtime.ReadMemStats(&heap)
	totalSeconds := time.Since(start).Seconds()
	var pgVersion string
	if err := f.fixture.pool.QueryRow(ctx, "SHOW server_version").Scan(&pgVersion); err != nil {
		t.Fatal(err)
	}
	evidence := map[string]any{
		"result": "PASS", "workload": map[string]any{
			"intents": intents, "signers": signerCount, "per_signer_intents": intents / signerCount,
			"normal_percent": 50, "unknown_percent": 25, "replacement_percent": 25,
			"rpc_profile":  "loopback Anvil; three accepted responses dropped; no added latency",
			"worker_count": phase6RuntimeConfig().Workers, "db_pool_max_connections": 24,
			"rpc_concurrency": 4,
		},
		"environment": map[string]any{"os": runtime.GOOS, "architecture": runtime.GOARCH,
			"logical_cpus": runtime.NumCPU(), "go_version": runtime.Version(), "postgres_version": pgVersion,
			"anvil_version": os.Getenv("TXORCH_ANVIL_VERSION")},
		"latency_ms": map[string]float64{"create_p50": percentile(createSamples, 50),
			"create_p95": percentile(createSamples, 95), "reconciliation_p50": percentile(reconcileSamples, 50),
			"reconciliation_p95": percentile(reconcileSamples, 95), "confirmation_total": confirmationMS},
		"throughput": map[string]float64{"intents_per_second": intents / totalSeconds,
			"db_rows_per_second": float64(actualIntents+actualReservations+actualAttempts) / totalSeconds},
		"resources": map[string]any{"process_cpu_seconds": processCPU() - startCPU,
			"heap_alloc_bytes": heap.Alloc},
		"correctness": map[string]any{"intents": actualIntents, "nonce_reservations": actualReservations,
			"attempts": actualAttempts, "signed_attempts": signedAttempts,
			"confirmed": confirmed, "duplicate_active_nonces": duplicateNonces,
			"checksum_input": input, "checksum_sha256": hex.EncodeToString(sum[:])},
		"duration_seconds": totalSeconds,
	}
	if path := os.Getenv("TXORCH_BENCHMARK_RESULT"); path != "" {
		data, err := json.MarshalIndent(evidence, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("BENCHMARK PASS checksum=%x intents/s=%.2f duration=%s", sum, intents/totalSeconds, time.Since(start))
}
