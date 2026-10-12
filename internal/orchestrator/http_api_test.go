package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

const phase7Token = "local-test-token-0123456789abcdef"

type apiFixture struct {
	*fixture
	rpc     *HTTPRPC
	pool    *EndpointPool
	runtime *Runtime
	signer  TransactionSigner
	api     *HTTPAPI
	server  *httptest.Server
}

func newAPIFixture(t *testing.T) *apiFixture {
	t.Helper()
	f := newFixture(t)
	rpc := startAnvil(t)
	key, err := crypto.HexToECDSA(strings.Repeat("0", 63) + "1")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewDevLocalSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	address, err := signer.Address(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var raw [20]byte
	copy(raw[:], address.Bytes())
	if err = f.store.RegisterNetworkAccount(context.Background(), Account{
		ChainID: "31337", SignerID: "api", Address: raw}); err != nil {
		t.Fatal(err)
	}
	var balance json.RawMessage
	if err = rpc.call(context.Background(), "anvil_setBalance", []any{address.Hex(), "0x3635c9adc5dea00000"}, &balance); err != nil {
		t.Fatal(err)
	}
	pool := phase6Pool(t, rpc)
	lookup := func(key AccountKey) TransactionSigner {
		if key.ChainID == "31337" && key.SignerID == "api" {
			return signer
		}
		return nil
	}
	runtime, err := NewRuntime(f.store, pool, phase6RuntimeConfig(), lookup)
	if err != nil {
		t.Fatal(err)
	}
	if err = runtime.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	cfg := HTTPConfig{Store: f.store, Runtime: runtime, Endpoints: pool,
		Signer: lookup, Tokens: map[string]string{phase7Token: "client-a", "other-test-token-0123456789abcdef": "client-b"},
		Accounts: []AccountKey{{ChainID: "31337", SignerID: "api"}},
		Policies: map[string]Policy{"standard": func() Policy {
			p := testPolicy()
			p.MaxFeeCapWei = "100000000000"
			p.MaxTipCapWei = "10000000000"
			return p
		}()},
		Fees:    map[string]FeeInput{"standard": {MaxFeePerGasWei: "2000000000", MaxTipPerGasWei: "1000000000"}},
		Signing: signingOptions(), NonceReadTimeout: time.Second,
		RequestTimeout: 15 * time.Second, MaxBodyBytes: 8192, MaxCalldataBytes: 1024,
		MaxBacklog: 1000, RuntimeReady: func() bool { return true },
		Version: "test", SourceIdentity: "test-source"}
	api, err := NewHTTPAPI(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(api.Handler())
	t.Cleanup(server.Close)
	return &apiFixture{fixture: f, rpc: rpc, pool: pool, runtime: runtime, signer: signer, api: api, server: server}
}

func apiBody(value string) string {
	return fmt.Sprintf(`{"chain_id":"31337","signer_id":"api","to":"%s","value_wei":"%s","calldata":"0x","gas_policy":{"profile":"standard"}}`, testAddress, value)
}

func apiRequest(t *testing.T, server *httptest.Server, method, path, token, key, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, data
}

func decodeView(t *testing.T, data []byte) IntentProjection {
	t.Helper()
	var view IntentProjection
	if err := json.Unmarshal(data, &view); err != nil {
		t.Fatalf("decode view: %v: %s", err, data)
	}
	return view
}

func TestPhase7APIContract(t *testing.T) {
	f := newAPIFixture(t)
	resp, data := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "same-key", apiBody("1"))
	if resp.StatusCode != 201 {
		t.Fatalf("create status %d: %s", resp.StatusCode, data)
	}
	view := decodeView(t, data)
	if view.State != Ready || view.Nonce == nil || *view.Nonce != "0" || len(view.Attempts) != 1 ||
		view.Attempts[0].TransactionHash == "" || view.WinningAttempt != nil || view.To != testAddress {
		t.Fatalf("created projection %+v", view)
	}
	if bytes.Contains(data, []byte("raw_tx")) || bytes.Contains(data, []byte("private")) {
		t.Fatal("sensitive transaction material in API")
	}
	replayed, second := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "same-key", apiBody("01"))
	if replayed.StatusCode != 200 || decodeView(t, second).IntentID != view.IntentID {
		t.Fatalf("canonical replay %d: %s", replayed.StatusCode, second)
	}
	conflict, _ := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "same-key", apiBody("2"))
	if conflict.StatusCode != 409 {
		t.Fatalf("conflict %d", conflict.StatusCode)
	}
	malformed, _ := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "bad-json", "{")
	if malformed.StatusCode != 400 {
		t.Fatalf("malformed %d", malformed.StatusCode)
	}
	oversize, _ := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "oversize", strings.Repeat("x", 8200))
	if oversize.StatusCode != 413 {
		t.Fatalf("oversized %d", oversize.StatusCode)
	}
	get, found := apiRequest(t, f.server, http.MethodGet, "/v1/transactions/"+view.IntentID, phase7Token, "", "")
	if get.StatusCode != 200 || decodeView(t, found).IntentID != view.IntentID {
		t.Fatalf("get %d: %s", get.StatusCode, found)
	}
	foreign, _ := apiRequest(t, f.server, http.MethodGet, "/v1/transactions/"+view.IntentID, "other-test-token-0123456789abcdef", "", "")
	if foreign.StatusCode != 404 {
		t.Fatalf("tenant isolation %d", foreign.StatusCode)
	}
	missing, _ := apiRequest(t, f.server, http.MethodGet, "/v1/transactions/00000000-0000-4000-8000-000000000001", phase7Token, "", "")
	if missing.StatusCode != 404 {
		t.Fatalf("missing %d", missing.StatusCode)
	}
	unauthorized, _ := apiRequest(t, f.server, http.MethodGet, "/v1/status", "", "", "")
	if unauthorized.StatusCode != 401 {
		t.Fatalf("unauthorized %d", unauthorized.StatusCode)
	}
	status, statusBody := apiRequest(t, f.server, http.MethodGet, "/v1/status", phase7Token, "", "")
	if status.StatusCode != 200 || !bytes.Contains(statusBody, []byte(`"source_identity":"test-source"`)) {
		t.Fatalf("status %d: %s", status.StatusCode, statusBody)
	}
	live, _ := apiRequest(t, f.server, http.MethodGet, "/health/live", "", "", "")
	ready, _ := apiRequest(t, f.server, http.MethodGet, "/health/ready", "", "", "")
	if live.StatusCode != 200 || ready.StatusCode != 200 {
		t.Fatalf("health live=%d ready=%d", live.StatusCode, ready.StatusCode)
	}
	metrics, metricsBody := apiRequest(t, f.server, http.MethodGet, "/metrics", "", "", "")
	if metrics.StatusCode != 200 || !bytes.Contains(metricsBody, []byte("txorch_rpc_eligible")) ||
		bytes.Contains(metricsBody, []byte(view.IntentID)) || bytes.Contains(metricsBody, []byte(view.Attempts[0].TransactionHash)) {
		t.Fatalf("metrics %d: %s", metrics.StatusCode, metricsBody)
	}
}

func TestPhase7APIConcurrentIdempotencyAndRestart(t *testing.T) {
	f := newAPIFixture(t)
	const workers = 32
	results := make(chan IntentProjection, workers)
	errors := make(chan string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, data := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "parallel", apiBody("7"))
			if resp.StatusCode != 200 && resp.StatusCode != 201 {
				errors <- fmt.Sprintf("%d %s", resp.StatusCode, data)
				return
			}
			results <- decodeView(t, data)
		}()
	}
	wg.Wait()
	close(results)
	close(errors)
	for failure := range errors {
		t.Fatal(failure)
	}
	var id string
	for result := range results {
		if id == "" {
			id = result.IntentID
		}
		if result.IntentID != id {
			t.Fatal("duplicate intent from concurrent POST")
		}
	}
	var intents, reservations, attempts int
	if err := f.fixture.pool.QueryRow(context.Background(), `SELECT
		(SELECT count(*) FROM transaction_intents WHERE tenant_id='client-a' AND idempotency_key='parallel'),
		(SELECT count(*) FROM nonce_reservations WHERE intent_id=$1::uuid),
		(SELECT count(*) FROM transaction_attempts WHERE intent_id=$1::uuid)`, id).Scan(&intents, &reservations, &attempts); err != nil {
		t.Fatal(err)
	}
	if intents != 1 || reservations != 1 || attempts != 1 {
		t.Fatalf("ledger %d/%d/%d", intents, reservations, attempts)
	}
	f.server.Close()
	newAPI, err := NewHTTPAPI(f.api.cfg)
	if err != nil {
		t.Fatal(err)
	}
	restarted := httptest.NewServer(newAPI.Handler())
	defer restarted.Close()
	resp, data := apiRequest(t, restarted, http.MethodPost, "/v1/transactions", phase7Token, "parallel", apiBody("7"))
	if resp.StatusCode != 200 || decodeView(t, data).IntentID != id {
		t.Fatalf("restart retry %d: %s", resp.StatusCode, data)
	}
}

func TestPhase7ReadinessIsolation(t *testing.T) {
	f := newAPIFixture(t)
	created, createdBody := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "before-outage", apiBody("1"))
	if created.StatusCode != 201 {
		t.Fatalf("create before outage %d: %s", created.StatusCode, createdBody)
	}
	id := decodeView(t, createdBody).IntentID
	before := f.api.readiness(context.Background())
	if !before.Ready {
		t.Fatalf("initial readiness %+v", before)
	}
	if err := f.store.SetSignerHealth(context.Background(), AccountKey{"31337", "api"}, false, "SIGNER_UNAVAILABLE"); err != nil {
		t.Fatal(err)
	}
	resp, data := apiRequest(t, f.server, http.MethodGet, "/health/ready", "", "", "")
	if resp.StatusCode != 503 || !bytes.Contains(data, []byte("no_ready_account")) {
		t.Fatalf("blocked ready %d: %s", resp.StatusCode, data)
	}
	blockedGET, blockedBody := apiRequest(t, f.server, http.MethodGet, "/v1/transactions/"+id, phase7Token, "", "")
	if blockedGET.StatusCode != 200 || decodeView(t, blockedBody).Account.BlockedReason != "SIGNER_UNAVAILABLE" {
		t.Fatalf("blocked account projection %d: %s", blockedGET.StatusCode, blockedBody)
	}
	live, _ := apiRequest(t, f.server, http.MethodGet, "/health/live", "", "", "")
	if live.StatusCode != 200 {
		t.Fatal("account failure killed liveness")
	}
	retry, retryBody := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "before-outage", apiBody("01"))
	if retry.StatusCode != 200 || decodeView(t, retryBody).IntentID != id {
		t.Fatalf("idempotent retry during outage %d: %s", retry.StatusCode, retryBody)
	}
	conflict, _ := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "before-outage", apiBody("2"))
	if conflict.StatusCode != 409 {
		t.Fatalf("conflicting retry during outage %d", conflict.StatusCode)
	}
	newRequest, _ := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "during-outage", apiBody("1"))
	if newRequest.StatusCode != 503 {
		t.Fatalf("new work accepted during outage %d", newRequest.StatusCode)
	}
	if err := f.store.SetSignerHealth(context.Background(), AccountKey{"31337", "api"}, true, ""); err != nil {
		t.Fatal(err)
	}
	wrong := phase6Endpoint{BroadcastEndpoint: f.rpc, name: "wrong",
		chain: func(context.Context) (*big.Int, error) { return big.NewInt(1), nil }}
	f.api.cfg.Endpoints = phase6Pool(t, wrong)
	resp, data = apiRequest(t, f.server, http.MethodGet, "/health/ready", "", "", "")
	if resp.StatusCode != 503 || !bytes.Contains(data, []byte("wrong_chain_rpc")) {
		t.Fatalf("wrong chain ready %d: %s", resp.StatusCode, data)
	}
}

func TestPhase7TraceSafety(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	previous := otel.GetTracerProvider()
	otel.SetTracerProvider(provider)
	defer func() {
		otel.SetTracerProvider(previous)
		_ = provider.Shutdown(context.Background())
	}()
	f := newAPIFixture(t)
	response, body := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token, "traced", apiBody("3"))
	if response.StatusCode != 201 {
		t.Fatalf("traced admission %d: %s", response.StatusCode, body)
	}
	seen := make(map[string]bool)
	for _, span := range exporter.GetSpans() {
		seen[span.Name] = true
		for _, attribute := range span.Attributes {
			key := strings.ToLower(string(attribute.Key))
			if strings.Contains(key, "secret") || strings.Contains(key, "private") ||
				strings.Contains(key, "raw") || strings.Contains(key, "url") ||
				strings.Contains(attribute.Value.Emit(), phase7Token) {
				t.Fatalf("unsafe trace attribute %s", attribute.Key)
			}
		}
	}
	for _, name := range []string{"http.request", "db.admission", "db.intent.create", "account.reconcile", "signer.call", "rpc.call"} {
		if !seen[name] {
			t.Fatalf("missing external-effect span %s: %+v", name, seen)
		}
	}
}

func TestPhase7MetricCardinality(t *testing.T) {
	f := newAPIFixture(t)
	response, body := apiRequest(t, f.server, http.MethodPost, "/v1/transactions", phase7Token,
		"metric-cardinality", apiBody("123"))
	if response.StatusCode != 201 {
		t.Fatalf("create %d: %s", response.StatusCode, body)
	}
	view := decodeView(t, body)
	response, body = apiRequest(t, f.server, http.MethodGet, "/metrics", "", "", "")
	if response.StatusCode != 200 {
		t.Fatalf("metrics %d: %s", response.StatusCode, body)
	}
	if bytes.Contains(body, []byte(view.IntentID)) || bytes.Contains(body, []byte(view.Attempts[0].TransactionHash)) ||
		bytes.Contains(body, []byte(view.To)) || bytes.Contains(body, []byte(`signer_id=`)) ||
		bytes.Contains(body, []byte(`request_id=`)) || bytes.Contains(body, []byte(`nonce=`)) {
		t.Fatal("high-cardinality identifier appeared in metric exposition")
	}
	allowed := map[string]bool{"result": true, "state": true, "reason": true,
		"endpoint": true, "method": true, "status": true, "le": true, "chain": true, "kind": true}
	series := 0
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, "txorch_") {
			continue
		}
		series++
		if series > 200 {
			t.Fatal("unexpected metric series growth")
		}
		begin, end := strings.IndexByte(line, '{'), strings.IndexByte(line, '}')
		if begin < 0 || end < 0 {
			continue
		}
		for _, label := range strings.Split(line[begin+1:end], ",") {
			key, _, ok := strings.Cut(label, "=")
			if !ok || !allowed[key] {
				t.Fatalf("unbounded metric label %q", label)
			}
		}
	}
	if series == 0 {
		t.Fatal("no metric series emitted")
	}
}

func TestPhase7DeepReorgReadinessWithOtherAccount(t *testing.T) {
	f := newAPIFixture(t)
	key, err := crypto.HexToECDSA(strings.Repeat("0", 63) + "2")
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewDevLocalSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	address, err := signer.Address(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var raw [20]byte
	copy(raw[:], address.Bytes())
	second := AccountKey{ChainID: "31337", SignerID: "api2"}
	if err := f.store.RegisterNetworkAccount(context.Background(), Account{
		ChainID: second.ChainID, SignerID: second.SignerID, Address: raw}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ReconcileAccount(context.Background(), second, f.pool, time.Second); err != nil {
		t.Fatal(err)
	}
	if err := f.store.SetSignerHealth(context.Background(), second, true, ""); err != nil {
		t.Fatal(err)
	}
	f.api.cfg.Accounts = append(f.api.cfg.Accounts, second)
	initial := f.api.readiness(context.Background())
	if !initial.Ready {
		t.Fatalf("second account not ready: %+v", initial)
	}
	if err := f.store.SetSignerHealth(context.Background(), AccountKey{"31337", "api"}, false,
		"SIGNER_UNAVAILABLE"); err != nil {
		t.Fatal(err)
	}
	isolated := f.api.readiness(context.Background())
	if !isolated.Ready {
		t.Fatalf("unrelated signer blocked whole chain: %+v", isolated)
	}
	if _, err := f.fixture.pool.Exec(context.Background(), `UPDATE signer_accounts SET paused_reason='DEEP_REORG_BLOCKED'
		WHERE chain_id=31337 AND signer_id='api'`); err != nil {
		t.Fatal(err)
	}
	status := f.api.readiness(context.Background())
	if status.Ready || !strings.Contains(strings.Join(status.Reasons, ","), "manual_reorg_intervention") {
		t.Fatalf("deep reorg hidden by another ready account: %+v", status)
	}
}

func TestPhase7ErrorLogRedaction(t *testing.T) {
	f := newAPIFixture(t)
	var log bytes.Buffer
	f.api.cfg.Logger = slog.New(slog.NewJSONHandler(&log, nil))
	request := httptest.NewRequest(http.MethodPost, "/v1/transactions", nil)
	request = request.WithContext(context.WithValue(request.Context(), requestIDKey{}, "redaction-test"))
	response := httptest.NewRecorder()
	f.api.failure(response, request, errors.New("https://user:secret@127.0.0.1:8545/private"))
	if response.Code != 503 || bytes.Contains(response.Body.Bytes(), []byte("secret")) ||
		bytes.Contains(log.Bytes(), []byte("secret")) || bytes.Contains(log.Bytes(), []byte("127.0.0.1")) {
		t.Fatalf("error detail escaped: response=%s log=%s", response.Body.String(), log.String())
	}
}
