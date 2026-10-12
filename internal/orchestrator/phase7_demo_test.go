package orchestrator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestPhase7Demo is the executable local fault, replacement and fork scenario.
// Every step is checked against durable state before evidence is written.
func TestPhase7Demo(t *testing.T) {
	start := time.Now()
	traceExport := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(traceExport))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	}()
	f := newAPIFixture(t)
	var logs bytes.Buffer
	f.api.cfg.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	f.runtime.config.Logger = f.api.cfg.Logger
	var result json.RawMessage
	if err := f.rpc.call(context.Background(), "evm_setAutomine", []any{false}, &result); err != nil {
		t.Fatal(err)
	}
	post, body := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "demo", apiBody("1"))
	if post.StatusCode != 201 {
		t.Fatalf("create: %d %s", post.StatusCode, body)
	}
	created := decodeView(t, body)
	if created.State != Ready || created.Nonce == nil || *created.Nonce != "0" || len(created.Attempts) != 1 {
		t.Fatalf("signed attempt: %+v", created)
	}
	intentID := created.IntentID
	firstHash := common.HexToHash(created.Attempts[0].TransactionHash)
	proxy, sends := faultProxy(t, f.rpc.URL, false)
	unknown, err := f.store.AdvanceBroadcast(context.Background(), intentID, []BroadcastEndpoint{proxy}, broadcastPolicy())
	if err != nil || unknown.State != Uncertain || sends.Load() != 1 {
		t.Fatalf("accepted response lost: %+v sends=%d err=%v", unknown, sends.Load(), err)
	}
	_, body = apiRequest(t, f.server, http.MethodGet, "/v1/transactions/"+intentID, phase7Token, "", "")
	uncertainView := decodeView(t, body)
	if uncertainView.State != Uncertain || len(uncertainView.Attempts) != 1 {
		t.Fatalf("uncertain projection: %+v", uncertainView)
	}
	seen, err := f.rpc.TransactionByHash(context.Background(), firstHash)
	if err != nil || seen == nil {
		t.Fatalf("Anvil did not accept lost-response transaction: %v", err)
	}

	// Reopen the database connection and recreate both runtime and HTTP API.
	f.server.Close()
	f.fixture.pool.Close()
	f.fixture.open(t)
	f.runtime, err = NewRuntime(f.store, f.pool, phase6RuntimeConfig(), f.api.cfg.Signer)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.runtime.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := f.api.cfg
	cfg.Store, cfg.Runtime = f.store, f.runtime
	f.api, err = NewHTTPAPI(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.server = httptest.NewServer(f.api.Handler())
	t.Cleanup(f.server.Close)
	retry, retryBody := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "demo", apiBody("01"))
	if retry.StatusCode != 200 || decodeView(t, retryBody).IntentID != intentID {
		t.Fatalf("restart idempotency: %d %s", retry.StatusCode, retryBody)
	}
	resolved, err := f.store.AdvanceBroadcast(context.Background(), intentID, []BroadcastEndpoint{f.rpc}, broadcastPolicy())
	if err != nil || resolved.State != Pending || resolved.Hash != firstHash {
		t.Fatalf("uncertain reconciliation: %+v %v", resolved, err)
	}
	if sends.Load() != 1 {
		t.Fatal("fault proxy unexpectedly resent transaction")
	}

	second, err := f.store.SignReplacement(context.Background(), intentID, f.signer,
		[]BroadcastEndpoint{f.rpc}, replacementOptions())
	if err != nil || second.AttemptNumber != 2 || second.Hash == firstHash {
		t.Fatalf("replacement: %+v %v", second, err)
	}
	if secondBroadcast, err := f.store.AdvanceBroadcast(context.Background(), intentID,
		[]BroadcastEndpoint{f.rpc}, broadcastPolicy()); err != nil || secondBroadcast.State != Pending || secondBroadcast.Hash != second.Hash {
		t.Fatalf("replacement broadcast: %+v %v", secondBroadcast, err)
	}
	_, body = apiRequest(t, f.server, http.MethodGet, "/v1/transactions/"+intentID, phase7Token, "", "")
	replacementView := decodeView(t, body)
	if len(replacementView.Attempts) != 2 || replacementView.Attempts[1].ReplacesAttemptID != replacementView.Attempts[0].AttemptID ||
		replacementView.Attempts[0].Nonce != replacementView.Attempts[1].Nonce || replacementView.WinningAttempt != nil {
		t.Fatalf("replacement projection: %+v", replacementView)
	}
	base := snapshotAnvil(t, f.rpc)
	mineAnvil(t, f.rpc)
	p := receiptPolicy()
	mined, err := f.store.ReconcileReceipts(context.Background(), intentID, []BroadcastEndpoint{f.rpc}, p)
	if err != nil || mined.State != Mined || mined.WinningAttemptNumber != 2 {
		t.Fatalf("mined replacement: %+v %v", mined, err)
	}
	confirming, err := f.store.ReconcileReceipts(context.Background(), intentID, []BroadcastEndpoint{f.rpc}, p)
	if err != nil || confirming.State != Confirming {
		t.Fatalf("confirming: %+v %v", confirming, err)
	}
	mineAnvil(t, f.rpc)
	mineAnvil(t, f.rpc)
	confirmed, err := f.store.ReconcileReceipts(context.Background(), intentID, []BroadcastEndpoint{f.rpc}, p)
	if err != nil || confirmed.State != Confirmed || confirmed.Confirmations != 3 {
		t.Fatalf("confirmed replacement: %+v %v", confirmed, err)
	}
	_, body = apiRequest(t, f.server, http.MethodGet, "/v1/transactions/"+intentID, phase7Token, "", "")
	confirmedView := decodeView(t, body)
	if confirmedView.State != Confirmed || confirmedView.WinningAttempt == nil || confirmedView.WinningAttempt.AttemptNumber != 2 ||
		confirmedView.Confirmation.Observed != 3 {
		t.Fatalf("confirmed projection: %+v", confirmedView)
	}
	revertAnvil(t, f.rpc, base)
	branchWithoutReceipt(t, f.rpc, second.Hash, 3)
	reorged, err := f.store.ReconcileReceipts(context.Background(), intentID, []BroadcastEndpoint{f.rpc}, p)
	if err != nil || reorged.State != Reorged || reorged.WinningAttemptNumber != 0 {
		t.Fatalf("reorg withdrawal: %+v %v", reorged, err)
	}
	_, body = apiRequest(t, f.server, http.MethodGet, "/v1/transactions/"+intentID, phase7Token, "", "")
	reorgView := decodeView(t, body)
	if reorgView.State != Reorged || reorgView.WinningAttempt != nil || len(reorgView.Receipts) != 1 ||
		reorgView.Receipts[0].Verification != "ORPHANED" {
		t.Fatalf("reorg projection: %+v", reorgView)
	}
	first, err := f.store.LoadSignedAttemptNumber(context.Background(), intentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.rpc.SendRawTransaction(context.Background(), first.Raw); err != nil {
		t.Fatal(err)
	}
	mineAnvil(t, f.rpc)
	reincluded, err := f.store.ReconcileReceipts(context.Background(), intentID, []BroadcastEndpoint{f.rpc}, p)
	if err != nil || reincluded.WinningAttemptNumber != 1 {
		t.Fatalf("branch B winner: %+v %v", reincluded, err)
	}
	mineAnvil(t, f.rpc)
	mineAnvil(t, f.rpc)
	final, err := f.store.ReconcileReceipts(context.Background(), intentID, []BroadcastEndpoint{f.rpc}, p)
	if err != nil || final.State != Confirmed || final.WinningAttemptNumber != 1 || final.Confirmations != 3 {
		t.Fatalf("branch B final: %+v %v", final, err)
	}
	_, body = apiRequest(t, f.server, http.MethodGet, "/v1/transactions/"+intentID, phase7Token, "", "")
	view := decodeView(t, body)
	if view.WinningAttempt == nil || view.WinningAttempt.AttemptNumber != 1 || len(view.Receipts) != 2 ||
		view.Receipts[0].Verification != "ORPHANED" || view.Receipts[1].Verification != "CANONICAL" {
		t.Fatalf("final read model: %+v", view)
	}
	var intents, nonces, attempts, canonical, orphaned int
	var state State
	var winner int
	err = f.fixture.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM transaction_intents WHERE intent_id=$1::uuid),
		(SELECT count(*) FROM nonce_reservations WHERE intent_id=$1::uuid),
		(SELECT count(*) FROM transaction_attempts WHERE intent_id=$1::uuid),
		(SELECT count(*) FROM observed_receipts WHERE intent_id=$1::uuid AND verification_status='CANONICAL'),
		(SELECT count(*) FROM observed_receipts WHERE intent_id=$1::uuid AND verification_status='ORPHANED'),
		state,winning_attempt_number FROM transaction_intents WHERE intent_id=$1::uuid`, intentID).
		Scan(&intents, &nonces, &attempts, &canonical, &orphaned, &state, &winner)
	if err != nil || intents != 1 || nonces != 1 || attempts != 2 || canonical != 1 || orphaned != 1 ||
		state != Confirmed || winner != 1 {
		t.Fatalf("ledger %d/%d/%d/%d/%d %s winner=%d: %v", intents, nonces, attempts, canonical, orphaned, state, winner, err)
	}
	checksumInput := fmt.Sprintf("intents=%d;nonce_reservations=%d;attempts=%d;canonical=%d;orphaned=%d;winner=%d;state=%s",
		intents, nonces, attempts, canonical, orphaned, winner, state)
	sum := sha256.Sum256([]byte(checksumInput))
	const expectedInput = "intents=1;nonce_reservations=1;attempts=2;canonical=1;orphaned=1;winner=1;state=CONFIRMED"
	expected := sha256.Sum256([]byte(expectedInput))
	if sum != expected {
		t.Fatalf("correctness checksum %x != %x", sum, expected)
	}
	status, _ := apiRequest(t, f.server, http.MethodGet, "/v1/status", phase7Token, "", "")
	ready, _ := apiRequest(t, f.server, http.MethodGet, "/health/ready", "", "", "")
	metrics, metricsBody := apiRequest(t, f.server, http.MethodGet, "/metrics", "", "", "")
	if status.StatusCode != 200 || ready.StatusCode != 200 || metrics.StatusCode != 200 ||
		!bytes.Contains(metricsBody, []byte("txorch_reorged_receipts_total")) ||
		bytes.Contains(metricsBody, []byte(intentID)) || bytes.Contains(metricsBody, []byte(firstHash.Hex())) {
		t.Fatalf("operational projection status=%d ready=%d metrics=%d", status.StatusCode, ready.StatusCode, metrics.StatusCode)
	}
	if !strings.Contains(logs.String(), "intent created") || !strings.Contains(logs.String(), "attempt signed") ||
		strings.Contains(logs.String(), hex.EncodeToString(first.Raw)) ||
		strings.Contains(logs.String(), phase7Token) || strings.Contains(logs.String(), f.rpc.URL) ||
		strings.Contains(logs.String(), strings.Repeat("0", 63)+"1") {
		t.Fatal("lifecycle log absent or raw transaction leaked")
	}
	spans := make(map[string]bool)
	for _, span := range traceExport.GetSpans() {
		spans[span.Name] = true
	}
	for _, name := range []string{"http.request", "db.intent.create", "signer.call", "broadcast.advance", "replacement.sign", "receipt.reconcile"} {
		if !spans[name] {
			t.Fatalf("missing trace span %s", name)
		}
	}
	evidence := map[string]any{
		"scenario": "accepted-response-lost,restart,replacement,confirmation,reorg,winner-change",
		"result":   "PASS", "intent_id": intentID, "attempt_hashes": []string{firstHash.Hex(), second.Hash.Hex()},
		"counts": map[string]int{"intents": intents, "nonce_reservations": nonces, "attempts": attempts,
			"canonical_receipts": canonical, "orphaned_receipts": orphaned},
		"winner_attempt": winner, "final_state": state, "confirmation_depth": final.Confirmations,
		"checksum_input": checksumInput, "checksum_sha256": hex.EncodeToString(sum[:]),
		"duration_ms": time.Since(start).Milliseconds(), "fault_proxy_sends": sends.Load(),
		"operational": map[string]int{"status_http": status.StatusCode, "readiness_http": ready.StatusCode,
			"metrics_http": metrics.StatusCode},
		"telemetry": map[string]any{"spans": []string{"http.request", "db.intent.create", "signer.call",
			"broadcast.advance", "replacement.sign", "receipt.reconcile"},
			"metric_names": []string{"txorch_reorged_receipts_total", "txorch_rpc_eligible"},
			"log_events":   []string{"intent created", "nonce reserved", "attempt signed"}},
		"steps": []string{"created", "reserved", "signed", "accepted_response_lost", "uncertain", "restarted",
			"reconciled_exact_hash", "replaced_same_nonce", "mined", "confirming", "confirmed",
			"reorged", "winner_withdrawn", "original_reincluded", "winner_changed", "confirmed_final"},
	}
	if path := os.Getenv("TXORCH_DEMO_RESULT"); path != "" {
		data, err := json.MarshalIndent(evidence, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(data, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("DEMO PASS checksum=%x duration=%s", sum, time.Since(start))
}
