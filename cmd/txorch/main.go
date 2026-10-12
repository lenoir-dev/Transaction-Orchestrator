package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"transaction-orchestrator/internal/orchestrator"
)

var sourceIdentity = "unavailable"

func required(name string) (string, error) {
	value := os.Getenv(name)
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func localURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	return u.Hostname() == "localhost" || (net.ParseIP(u.Hostname()) != nil && net.ParseIP(u.Hostname()).IsLoopback())
}

func signerFromHex(value string) (*ecdsa.PrivateKey, error) {
	key := strings.TrimPrefix(value, "0x")
	if len(key) != 64 {
		return nil, errors.New("development key must be 32 bytes")
	}
	if _, err := hex.DecodeString(key); err != nil {
		return nil, errors.New("development key is not hex")
	}
	return crypto.HexToECDSA(key)
}

func run() error {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Printf("txorch phase7-dev source=%s go=%s\n", sourceIdentity, runtime.Version())
		return nil
	}
	if len(os.Args) > 1 && os.Args[1] != "serve" {
		return errors.New("usage: txorch [serve|version]")
	}
	if os.Getenv("TXORCH_DEV_MODE") != "1" {
		return errors.New("this binary requires explicit TXORCH_DEV_MODE=1")
	}
	dbURL, err := required("TXORCH_DATABASE_URL")
	if err != nil {
		return err
	}
	rpcURL, err := required("TXORCH_RPC_URL")
	if err != nil {
		return err
	}
	if !localURL(rpcURL) {
		return errors.New("development RPC must be loopback")
	}
	chainID, err := required("TXORCH_CHAIN_ID")
	if err != nil {
		return err
	}
	signerID, err := required("TXORCH_SIGNER_ID")
	if err != nil {
		return err
	}
	secret, err := required("TXORCH_DEV_PRIVATE_KEY")
	if err != nil {
		return err
	}
	token, err := required("TXORCH_API_TOKEN")
	if err != nil {
		return err
	}
	tenant, err := required("TXORCH_TENANT_ID")
	if err != nil {
		return err
	}
	key, err := signerFromHex(secret)
	if err != nil {
		return err
	}
	signer, err := orchestrator.NewDevLocalSigner(key)
	if err != nil {
		return err
	}
	address, err := signer.Address(context.Background())
	if err != nil {
		return err
	}
	var rawAddress [20]byte
	copy(rawAddress[:], address.Bytes())
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	var traceProvider *sdktrace.TracerProvider
	if os.Getenv("TXORCH_TRACE_STDOUT") == "1" {
		exporter, err := stdouttrace.New(stdouttrace.WithWriter(os.Stdout))
		if err != nil {
			return err
		}
		traceProvider = sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
		otel.SetTracerProvider(traceProvider)
		defer traceProvider.Shutdown(context.Background())
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	poolCfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		return err
	}
	poolCfg.MaxConns = 16
	db, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Ping(ctx); err != nil {
		return err
	}
	store := orchestrator.NewStore(db)
	account := orchestrator.Account{ChainID: chainID, SignerID: signerID, Address: rawAddress}
	if err = store.RegisterNetworkAccount(ctx, account); err != nil {
		return err
	}
	rpc := &orchestrator.HTTPRPC{Name: "local", URL: rpcURL, Client: &http.Client{Timeout: 2 * time.Second}}
	endpoints, err := orchestrator.NewEndpointPool([]orchestrator.EndpointConfig{{
		ID: "local", RPC: rpc, ChainID: chainID,
		Capabilities: orchestrator.EndpointRead | orchestrator.EndpointBroadcast | orchestrator.EndpointNonce,
		MaxInflight:  4,
	}}, orchestrator.EndpointPoolPolicy{ProbeTimeout: 2 * time.Second,
		Cooldown: time.Second, FailureLimit: 2, MaxHeadLag: 2, MaxRPCInflight: 8})
	if err != nil {
		return err
	}
	lookup := func(candidate orchestrator.AccountKey) orchestrator.TransactionSigner {
		if candidate.ChainID == chainID && candidate.SignerID == signerID {
			return signer
		}
		return nil
	}
	broadcast := orchestrator.BroadcastPolicy{MaxSends: 3, MaxDuration: 5 * time.Minute,
		Backoff: 250 * time.Millisecond, RPCDeadline: time.Second, Lease: 40 * time.Second}
	signing := orchestrator.SignOptions{SignerTimeout: 2 * time.Second, LeaseDuration: 5 * time.Second}
	replacement := orchestrator.ReplacementOptions{Now: time.Now(), MinAge: 120 * time.Second,
		BumpBps: 1500, Signing: signing, Observation: broadcast}
	receipt := orchestrator.ReceiptPolicy{Now: time.Now(), Interval: 500 * time.Millisecond,
		RPCDeadline: time.Second, TotalDeadline: 5 * time.Second, MaxReorgDepth: 4, MaxAttempts: 4}
	worker, err := orchestrator.NewRuntime(store, endpoints, orchestrator.RuntimeConfig{
		Workers: 4, BatchSize: 32, StartupAccountBatch: 16, PollInterval: 250 * time.Millisecond,
		WorkTimeout: 10 * time.Second, AccountRPCDeadline: time.Second,
		AccountRefreshInterval: time.Second, RetryBase: 250 * time.Millisecond,
		RetryMax: 10 * time.Second, Broadcast: broadcast, Receipt: receipt,
		Replacement: replacement, Logger: logger,
	}, lookup)
	if err != nil {
		return err
	}
	var running atomic.Bool
	runtimeDone := make(chan error, 1)
	runCtx, cancelRuntime := context.WithCancel(ctx)
	defer cancelRuntime()
	go func() {
		running.Store(true)
		defer running.Store(false)
		runtimeDone <- worker.Run(runCtx)
	}()
	api, err := orchestrator.NewHTTPAPI(orchestrator.HTTPConfig{
		Store: store, Runtime: worker, Endpoints: endpoints, Signer: lookup,
		Tokens:   map[string]string{token: tenant},
		Accounts: []orchestrator.AccountKey{{ChainID: chainID, SignerID: signerID}},
		Policies: map[string]orchestrator.Policy{"standard": {Version: "local-v1", GasLimit: 21000,
			ConfirmationDepth: 3, MaxFeeCapWei: "10000000000", MaxTipCapWei: "1000000000",
			MaxReplacements: 2}},
		Fees:    map[string]orchestrator.FeeInput{"standard": {MaxFeePerGasWei: "2000000000", MaxTipPerGasWei: "100000000"}},
		Signing: signing, NonceReadTimeout: time.Second, RequestTimeout: 15 * time.Second,
		MaxBodyBytes: 64 << 10, MaxCalldataBytes: 16 << 10, MaxBacklog: 10000,
		RuntimeReady: running.Load, Logger: logger, Version: "phase7-dev", SourceIdentity: sourceIdentity,
	})
	if err != nil {
		return err
	}
	addr := os.Getenv("TXORCH_HTTP_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	server := &http.Server{Addr: addr, Handler: api.Handler(), ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 30 * time.Second}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.ListenAndServe() }()
	logger.Info("http service started", "address", addr, "chain_id", chainID,
		"signer_id", signerID, "source_identity", sourceIdentity)
	var result error
	select {
	case <-ctx.Done():
	case result = <-runtimeDone:
		if !errors.Is(result, context.Canceled) {
			logger.Error("runtime stopped", "error_type", fmt.Sprintf("%T", result))
		}
	case result = <-serverDone:
		if !errors.Is(result, http.ErrServerClosed) {
			logger.Error("http server stopped", "error_type", fmt.Sprintf("%T", result))
		}
	}
	cancelRuntime()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err = server.Shutdown(shutdownCtx); err != nil {
		return err
	}
	if errors.Is(result, context.Canceled) || errors.Is(result, http.ErrServerClosed) {
		return nil
	}
	return result
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "service stopped: %T\n", err)
		os.Exit(1)
	}
}
