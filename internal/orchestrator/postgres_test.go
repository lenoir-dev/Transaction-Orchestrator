package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const testAddress = "0x1111111111111111111111111111111111111111"

type fixture struct {
	admin  *pgxpool.Pool
	pool   *pgxpool.Pool
	url    string
	schema string
	store  *Store
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	url := os.Getenv("TXORCH_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set TXORCH_TEST_DATABASE_URL for PostgreSQL integration tests")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	id, err := newID()
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	schema := "txorch_test_" + strings.ReplaceAll(id, "-", "")[:16]
	// Test names are controlled in this package; quote the schema identifier.
	if _, err = admin.Exec(ctx, `CREATE SCHEMA "`+schema+`"`); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	f := &fixture{admin: admin, url: url, schema: schema}
	t.Cleanup(func() {
		if f.pool != nil {
			f.pool.Close()
		}
		_, err := admin.Exec(context.Background(), `DROP SCHEMA "`+schema+`" CASCADE`)
		if err != nil {
			t.Errorf("drop test schema: %v", err)
		}
		admin.Close()
	})
	f.open(t)
	migrations, err := filepath.Glob(filepath.Join("..", "..", "migrations", "*.sql"))
	if err != nil || len(migrations) == 0 {
		t.Fatalf("list migrations: %v", err)
	}
	for _, path := range migrations {
		migration, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.pool.Exec(ctx, string(migration), pgx.QueryExecModeSimpleProtocol); err != nil {
			t.Fatalf("execute migration %s: %v", filepath.Base(path), err)
		}
	}
	var address [20]byte
	for i := range address {
		address[i] = 0x11
	}
	if err = f.store.ProvisionAccount(ctx, Account{ChainID: "31337", SignerID: "operator", Address: address}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *fixture) open(t *testing.T) {
	t.Helper()
	config, err := pgxpool.ParseConfig(f.url)
	if err != nil {
		t.Fatal(err)
	}
	config.MaxConns = 24
	config.ConnConfig.RuntimeParams["search_path"] = f.schema
	f.pool, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	f.store = NewStore(f.pool)
}

func testRequest(key string) Request {
	return Request{TenantID: "tenant", IdempotencyKey: key, ChainID: "31337", SignerID: "operator",
		To: testAddress, ValueWei: "1", Calldata: "0x", GasProfile: "standard"}
}

func testPolicy() Policy {
	return Policy{Version: "test-v1", GasLimit: 21000, ConfirmationDepth: 3,
		MaxFeeCapWei: "1000000000", MaxTipCapWei: "100000000", MaxReplacements: 2}
}

func TestConcurrentNonceReservations(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	const count = 128
	ids := make([]string, count)
	for i := range ids {
		intent, err := f.store.CreateIntent(ctx, testRequest(fmt.Sprintf("nonce-%d", i)), testPolicy())
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = intent.ID
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	values := make(chan uint64, count)
	errorsCh := make(chan error, count)
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			n, err := f.store.ReserveNonce(ctx, id)
			if err != nil {
				errorsCh <- err
				return
			}
			values <- n
		}(id)
	}
	close(start)
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	close(values)
	var got []int
	for n := range values {
		got = append(got, int(n))
	}
	sort.Ints(got)
	if len(got) != count {
		t.Fatalf("got %d reservations, want %d", len(got), count)
	}
	for i, n := range got {
		if n != i {
			t.Fatalf("reservation %d has nonce %d", i, n)
		}
	}
	var distinct, rows int
	err := f.pool.QueryRow(ctx, `SELECT count(*),count(DISTINCT nonce) FROM nonce_reservations`).Scan(&rows, &distinct)
	if err != nil || rows != count || distinct != count {
		t.Fatalf("database ownership count=%d distinct=%d: %v", rows, distinct, err)
	}
}

func TestConcurrentIdempotency(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	const count = 100
	var wg sync.WaitGroup
	start := make(chan struct{})
	ids := make(chan string, count)
	errorsCh := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			intent, err := f.store.CreateIntent(ctx, testRequest("same-key"), testPolicy())
			if err != nil {
				errorsCh <- err
				return
			}
			ids <- intent.ID
		}()
	}
	close(start)
	wg.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		} else if id != first {
			t.Fatalf("duplicate key produced two intents: %s and %s", first, id)
		}
	}
	req := testRequest("same-key")
	req.ValueWei = "2"
	if _, err := f.store.CreateIntent(ctx, req, testPolicy()); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting immutable request: %v", err)
	}
	var rows int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM transaction_intents`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("intent rows=%d: %v", rows, err)
	}
}

func TestConflictingIdempotencyRace(t *testing.T) {
	f := newFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, value := range []string{"1", "2"} {
		go func(value string) {
			<-start
			req := testRequest("conflicting-key")
			req.ValueWei = value
			_, err := f.store.CreateIntent(ctx, req, testPolicy())
			results <- err
		}(value)
	}
	close(start)
	a, b := <-results, <-results
	if !((a == nil && errors.Is(b, ErrConflict)) || (b == nil && errors.Is(a, ErrConflict))) {
		t.Fatalf("one winner and one deterministic conflict required: %v, %v", a, b)
	}
	var rows int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM transaction_intents`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("conflict race left %d intents: %v", rows, err)
	}
}

func TestRestartAfterCreate(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	first, err := f.store.CreateIntent(ctx, testRequest("ack-lost"), testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	f.pool.Close()
	f.pool = nil
	f.open(t)
	got, err := f.store.CreateIntent(ctx, testRequest("ack-lost"), testPolicy())
	if err != nil || got.ID != first.ID || got.State != Created {
		t.Fatalf("same key after restart: %+v %v", got, err)
	}
	var events int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE intent_id=$1::uuid`, first.ID).Scan(&events); err != nil || events != 1 {
		t.Fatalf("duplicate create event count=%d: %v", events, err)
	}
}

func TestProcessCrashRecovery(t *testing.T) {
	if phase := os.Getenv("TXORCH_TEST_CRASH_PHASE"); phase != "" {
		config, err := pgxpool.ParseConfig(os.Getenv("TXORCH_TEST_DATABASE_URL"))
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		config.ConnConfig.RuntimeParams["search_path"] = os.Getenv("TXORCH_TEST_SCHEMA")
		pool, err := pgxpool.NewWithConfig(context.Background(), config)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		store := NewStore(pool)
		switch phase {
		case "create":
			_, err = store.CreateIntent(context.Background(), testRequest("process-crash"), testPolicy())
		case "reserve":
			_, err = store.ReserveNonce(context.Background(), os.Getenv("TXORCH_TEST_INTENT_ID"))
		default:
			err = fmt.Errorf("unknown crash phase")
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		// Terminate without closing the pool or running test cleanup.
		os.Exit(9)
	}
	f := newFixture(t)
	crash := func(phase, intentID string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestProcessCrashRecovery$")
		cmd.Env = append(os.Environ(),
			"TXORCH_TEST_CRASH_PHASE="+phase,
			"TXORCH_TEST_SCHEMA="+f.schema,
			"TXORCH_TEST_INTENT_ID="+intentID)
		output, err := cmd.CombinedOutput()
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 9 {
			t.Fatalf("child %s exit=%v output=%s", phase, err, output)
		}
	}
	crash("create", "")
	f.pool.Close()
	f.pool = nil
	f.open(t)
	intent, err := f.store.CreateIntent(context.Background(), testRequest("process-crash"), testPolicy())
	if err != nil || intent.State != Created {
		t.Fatalf("create recovery: %+v %v", intent, err)
	}
	crash("reserve", intent.ID)
	f.pool.Close()
	f.pool = nil
	f.open(t)
	got, err := f.store.GetIntent(context.Background(), intent.ID)
	if err != nil || got.State != Reserved || got.Nonce == nil || *got.Nonce != 0 {
		t.Fatalf("reservation recovery: %+v %v", got, err)
	}
	nonce, err := f.store.ReserveNonce(context.Background(), intent.ID)
	if err != nil || nonce != 0 {
		t.Fatalf("idempotent reservation after crash=%d: %v", nonce, err)
	}
	var events int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM audit_events WHERE intent_id=$1::uuid`, intent.ID).Scan(&events); err != nil || events != 2 {
		t.Fatalf("committed event count=%d: %v", events, err)
	}
}

func TestRestartAndPreSignTransitions(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	intent, err := f.store.CreateIntent(ctx, testRequest("restart"), testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ReserveNonce(ctx, intent.ID); err != nil {
		t.Fatal(err)
	}
	f.pool.Close()
	f.pool = nil
	f.open(t)
	changedPolicy := testPolicy()
	changedPolicy.Version = "test-v2"
	same, err := f.store.CreateIntent(ctx, testRequest("restart"), changedPolicy)
	if err != nil || same.ID != intent.ID || same.PolicyVersion != "test-v1" || same.Nonce == nil || *same.Nonce != 0 {
		t.Fatalf("restart did not recover original identity/policy/nonce: %+v %v", same, err)
	}
	if err = f.store.Block(ctx, intent.ID, "OPERATOR_HOLD"); err != nil {
		t.Fatal(err)
	}
	second, err := f.store.CreateIntent(ctx, testRequest("next"), testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.store.ReserveNonce(ctx, second.ID); !errors.Is(err, ErrAccountPaused) {
		t.Fatalf("blocked reserved nonce must pause allocation: %v", err)
	}
	recovered, err := f.store.RecoverableIntents(ctx)
	if err != nil || len(recovered) != 2 {
		t.Fatalf("recovery found %d intents: %v", len(recovered), err)
	}
	if err = f.store.Resume(ctx, intent.ID, "HOLD_RESOLVED"); err != nil {
		t.Fatal(err)
	}
	n, err := f.store.ReserveNonce(ctx, second.ID)
	if err != nil || n != 1 {
		t.Fatalf("next nonce after resume=%d: %v", n, err)
	}
	if err = f.store.FailUnreserved(ctx, second.ID, "INVALID"); !errors.Is(err, ErrTransition) {
		t.Fatalf("reserved nonce must not become terminal failure: %v", err)
	}
	var events int
	if err = f.pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE intent_id=$1::uuid`, intent.ID).Scan(&events); err != nil || events != 4 {
		t.Fatalf("audit events=%d: %v", events, err)
	}
}

func TestDatabaseNonceConstraint(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	a, _ := f.store.CreateIntent(ctx, testRequest("a"), testPolicy())
	b, _ := f.store.CreateIntent(ctx, testRequest("b"), testPolicy())
	if _, err := f.store.ReserveNonce(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	_, err := f.pool.Exec(ctx, `INSERT INTO nonce_reservations (intent_id,chain_id,signer_address,nonce)
		SELECT $1::uuid,chain_id,signer_address,0 FROM transaction_intents WHERE intent_id=$1::uuid`, b.ID)
	if !IsUniqueViolation(err) {
		t.Fatalf("database accepted conflicting nonce: %v", err)
	}
	for _, query := range []string{
		`UPDATE nonce_reservations SET nonce=nonce+1 WHERE intent_id=$1::uuid`,
		`DELETE FROM nonce_reservations WHERE intent_id=$1::uuid`,
	} {
		if _, err := f.pool.Exec(ctx, query, a.ID); err == nil {
			t.Fatalf("database allowed nonce reservation mutation: %s", query)
		}
	}
	if _, err := f.pool.Exec(ctx, `UPDATE signer_accounts SET next_nonce=next_nonce-1
		WHERE chain_id=31337 AND signer_address=decode(repeat('11',20),'hex')`); err == nil {
		t.Fatal("database allowed nonce cursor rollback")
	}
}
