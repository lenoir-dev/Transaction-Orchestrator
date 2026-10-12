package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/jackc/pgx/v5/pgxpool"
)

func signingFixture(t *testing.T) (*fixture, *DevLocalSigner) {
	t.Helper()
	f := newFixture(t)
	key, err := crypto.GenerateKey()
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
	if err := f.store.ProvisionAccount(context.Background(), Account{
		ChainID: "31337", SignerID: "dev", Address: raw, InitialNonce: 7,
	}); err != nil {
		t.Fatal(err)
	}
	return f, signer
}

func signingIntent(t *testing.T, f *fixture, key string) Intent {
	t.Helper()
	req := testRequest(key)
	req.SignerID = "dev"
	req.ValueWei = "42"
	req.Calldata = "0xdeadbeef"
	policy := testPolicy()
	policy.GasLimit = 75000
	intent, err := f.store.CreateIntent(context.Background(), req, policy)
	if err != nil {
		t.Fatal(err)
	}
	if nonce, err := f.store.ReserveNonce(context.Background(), intent.ID); err != nil || nonce < 7 {
		t.Fatalf("reserve nonce=%d: %v", nonce, err)
	}
	return intent
}

func signingFee() FeeInput {
	return FeeInput{MaxFeePerGasWei: "100000000", MaxTipPerGasWei: "10000000"}
}

func signingOptions() SignOptions {
	return SignOptions{SignerTimeout: 2 * time.Second, LeaseDuration: 5 * time.Second}
}

func TestValidSignedAttemptAndImmutability(t *testing.T) {
	f, signer := signingFixture(t)
	intent := signingIntent(t, f, "valid")
	identity, err := f.store.SignInitial(context.Background(), intent.ID, signer, signingFee(), signingOptions())
	if err != nil || identity.AttemptNumber != 1 {
		t.Fatalf("sign: %+v %v", identity, err)
	}
	stored, err := f.store.LoadSignedAttempt(context.Background(), intent.ID)
	if err != nil || stored.Hash != identity.Hash || stored.AttemptID != identity.AttemptID {
		t.Fatalf("stored identity mismatch: %+v %v", stored.AttemptIdentity, err)
	}
	var decoded types.Transaction
	if err = decoded.UnmarshalBinary(stored.Raw); err != nil || decoded.Hash() != stored.Hash ||
		decoded.Type() != types.DynamicFeeTxType || decoded.Nonce() != 7 ||
		!bytes.Equal(decoded.Data(), []byte{0xde, 0xad, 0xbe, 0xef}) {
		t.Fatalf("signed raw transaction round trip: %v", err)
	}
	originalRaw := bytes.Clone(stored.Raw)
	stored.Raw[0] = 0
	again, err := f.store.LoadSignedAttempt(context.Background(), intent.ID)
	if err != nil || !bytes.Equal(again.Raw, originalRaw) {
		t.Fatalf("stored bytes changed through caller buffer: %v", err)
	}
	repeat, err := f.store.SignInitial(context.Background(), intent.ID, nil,
		FeeInput{MaxFeePerGasWei: "999999999999"}, SignOptions{})
	if err != nil || repeat != identity {
		t.Fatalf("READY did not return existing attempt: %+v %v", repeat, err)
	}
	for _, statement := range []string{
		`UPDATE transaction_attempts SET raw_tx=decode('02','hex') WHERE intent_id=$1::uuid`,
		`UPDATE transaction_attempts SET transaction_hash=decode(repeat('00',32),'hex') WHERE intent_id=$1::uuid`,
		`UPDATE transaction_attempts SET nonce=nonce+1 WHERE intent_id=$1::uuid`,
		`UPDATE transaction_attempts SET max_fee_per_gas=max_fee_per_gas+1 WHERE intent_id=$1::uuid`,
		`UPDATE transaction_attempts SET policy_snapshot_id='other' WHERE intent_id=$1::uuid`,
		`UPDATE transaction_attempts SET status='PREPARING' WHERE intent_id=$1::uuid`,
		`DELETE FROM transaction_attempts WHERE intent_id=$1::uuid`,
		`UPDATE transaction_intents SET calldata=decode('00','hex') WHERE intent_id=$1::uuid`,
	} {
		if _, err := f.pool.Exec(context.Background(), statement, intent.ID); err == nil {
			t.Fatalf("immutable signed material changed through SQL: %s", statement)
		}
	}
	var attempts int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM transaction_attempts WHERE intent_id=$1::uuid`,
		intent.ID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("attempt rows=%d: %v", attempts, err)
	}
}

type countingSigner struct {
	TransactionSigner
	calls atomic.Int64
}

func (s *countingSigner) SignTransaction(ctx context.Context, tx *types.Transaction) (*types.Transaction, error) {
	s.calls.Add(1)
	return s.TransactionSigner.SignTransaction(ctx, tx)
}

func TestConcurrentSignInitial(t *testing.T) {
	f, signer := signingFixture(t)
	intent := signingIntent(t, f, "concurrent-sign")
	counted := &countingSigner{TransactionSigner: signer}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const workers = 32
	start := make(chan struct{})
	results := make(chan AttemptIdentity, workers)
	errorsCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			id, err := f.store.SignInitial(ctx, intent.ID, counted, signingFee(), signingOptions())
			if err != nil {
				errorsCh <- err
				return
			}
			results <- id
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
	close(results)
	var first AttemptIdentity
	seen := 0
	for id := range results {
		if seen == 0 {
			first = id
		} else if id != first {
			t.Fatalf("parallel calls returned different attempts: %+v %+v", first, id)
		}
		seen++
	}
	if seen != workers || counted.calls.Load() != 1 {
		t.Fatalf("results=%d signer calls=%d", seen, counted.calls.Load())
	}
	var attempts int
	if err := f.pool.QueryRow(ctx, `SELECT count(*) FROM transaction_attempts WHERE intent_id=$1::uuid`, intent.ID).Scan(&attempts); err != nil || attempts != 1 {
		t.Fatalf("attempt rows=%d: %v", attempts, err)
	}
}

type alteredSigner struct {
	base   TransactionSigner
	change func(*types.Transaction) *types.Transaction
	lie    *common.Address
	err    error
}

func (s alteredSigner) Address(ctx context.Context) (common.Address, error) {
	if s.lie != nil {
		return *s.lie, nil
	}
	return s.base.Address(ctx)
}

func (s alteredSigner) SignTransaction(ctx context.Context, tx *types.Transaction) (*types.Transaction, error) {
	if s.err != nil {
		return nil, s.err
	}
	if s.change != nil {
		tx = s.change(tx)
	}
	return s.base.SignTransaction(ctx, tx)
}

func dynamicCopy(tx *types.Transaction) *types.DynamicFeeTx {
	to := *tx.To()
	return &types.DynamicFeeTx{ChainID: new(big.Int).Set(tx.ChainId()), Nonce: tx.Nonce(),
		GasTipCap: new(big.Int).Set(tx.GasTipCap()), GasFeeCap: new(big.Int).Set(tx.GasFeeCap()),
		Gas: tx.Gas(), To: &to, Value: new(big.Int).Set(tx.Value()), Data: bytes.Clone(tx.Data())}
}

func TestAdversarialSignerOutput(t *testing.T) {
	mutations := map[string]func(*types.DynamicFeeTx){
		"chain": func(d *types.DynamicFeeTx) { d.ChainID.Add(d.ChainID, big.NewInt(1)) },
		"nonce": func(d *types.DynamicFeeTx) { d.Nonce++ },
		"destination": func(d *types.DynamicFeeTx) {
			a := common.HexToAddress("0x3333333333333333333333333333333333333333")
			d.To = &a
		},
		"calldata": func(d *types.DynamicFeeTx) { d.Data[0] ^= 0xff },
		"value":    func(d *types.DynamicFeeTx) { d.Value.Add(d.Value, big.NewInt(1)) },
		"gas":      func(d *types.DynamicFeeTx) { d.Gas++ },
		"fee":      func(d *types.DynamicFeeTx) { d.GasFeeCap.Add(d.GasFeeCap, big.NewInt(1)) },
		"access list": func(d *types.DynamicFeeTx) {
			d.AccessList = types.AccessList{{Address: *d.To}}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			f, signer := signingFixture(t)
			intent := signingIntent(t, f, "mutation")
			malicious := alteredSigner{base: signer, change: func(tx *types.Transaction) *types.Transaction {
				d := dynamicCopy(tx)
				mutate(d)
				return types.NewTx(d)
			}}
			_, err := f.store.SignInitial(context.Background(), intent.ID, malicious, signingFee(), signingOptions())
			assertSignerCode(t, err, "SIGNER_MUTATED_TRANSACTION", false)
			assertUnsignedBlocked(t, f, intent.ID)
		})
	}
	t.Run("wrong recovered sender", func(t *testing.T) {
		f, signer := signingFixture(t)
		intent := signingIntent(t, f, "wrong-sender")
		otherKey, _ := crypto.GenerateKey()
		other, _ := NewDevLocalSigner(otherKey)
		expected, _ := signer.Address(context.Background())
		_, err := f.store.SignInitial(context.Background(), intent.ID,
			alteredSigner{base: other, lie: &expected}, signingFee(), signingOptions())
		assertSignerCode(t, err, "SIGNER_INVALID_SIGNATURE", false)
		assertUnsignedBlocked(t, f, intent.ID)
	})
	t.Run("unsigned output", func(t *testing.T) {
		f, signer := signingFixture(t)
		intent := signingIntent(t, f, "unsigned-output")
		_, err := f.store.SignInitial(context.Background(), intent.ID,
			unsignedSigner{TransactionSigner: signer}, signingFee(), signingOptions())
		assertSignerCode(t, err, "SIGNER_INVALID_SIGNATURE", false)
		assertUnsignedBlocked(t, f, intent.ID)
	})
	t.Run("nil output", func(t *testing.T) {
		f, signer := signingFixture(t)
		intent := signingIntent(t, f, "nil-output")
		_, err := f.store.SignInitial(context.Background(), intent.ID,
			nilOutputSigner{TransactionSigner: signer}, signingFee(), signingOptions())
		assertSignerCode(t, err, "SIGNER_MUTATED_TRANSACTION", false)
		assertUnsignedBlocked(t, f, intent.ID)
	})
	t.Run("wrong configured signer", func(t *testing.T) {
		f, signer := signingFixture(t)
		intent := signingIntent(t, f, "wrong-address")
		otherKey, _ := crypto.GenerateKey()
		other, _ := NewDevLocalSigner(otherKey)
		_, err := f.store.SignInitial(context.Background(), intent.ID, other, signingFee(), signingOptions())
		assertSignerCode(t, err, "SIGNER_ADDRESS_MISMATCH", false)
		got, _ := f.store.GetIntent(context.Background(), intent.ID)
		if got.State != Blocked || got.Reason != "SIGNER_ADDRESS_MISMATCH" {
			t.Fatalf("wrong signer did not block intent: %+v", got)
		}
		var attempts int
		if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM transaction_attempts
			WHERE intent_id=$1::uuid`, intent.ID).Scan(&attempts); err != nil || attempts != 0 {
			t.Fatalf("wrong signer created attempt: %d %v", attempts, err)
		}
		assertAccountPaused(t, f)
		if err := f.store.Resume(context.Background(), intent.ID, "SIGNER_CORRECTED"); err != nil {
			t.Fatal(err)
		}
		if _, err := f.store.SignInitial(context.Background(), intent.ID, signer,
			signingFee(), signingOptions()); err != nil {
			t.Fatalf("corrected signer should use original nonce: %v", err)
		}
	})
}

type unsignedSigner struct{ TransactionSigner }

func (s unsignedSigner) SignTransaction(_ context.Context, tx *types.Transaction) (*types.Transaction, error) {
	return tx, nil
}

type nilOutputSigner struct{ TransactionSigner }

func (s nilOutputSigner) SignTransaction(context.Context, *types.Transaction) (*types.Transaction, error) {
	return nil, nil
}

func TestSigningFeeInputBounds(t *testing.T) {
	for _, input := range []FeeInput{
		{MaxFeePerGasWei: "1", MaxTipPerGasWei: "2"},
		{MaxFeePerGasWei: "1000000001", MaxTipPerGasWei: "10000000"},
		{MaxFeePerGasWei: "100000000", MaxTipPerGasWei: "100000001"},
		{MaxFeePerGasWei: new(big.Int).Lsh(big.NewInt(1), 256).String(), MaxTipPerGasWei: "1"},
	} {
		f, signer := signingFixture(t)
		intent := signingIntent(t, f, "fee-bounds")
		if _, err := f.store.SignInitial(context.Background(), intent.ID, signer, input, signingOptions()); err == nil {
			t.Fatal("invalid fee input accepted")
		}
		got, _ := f.store.GetIntent(context.Background(), intent.ID)
		if got.State != Reserved {
			t.Fatalf("invalid fee input claimed work: %s", got.State)
		}
	}
}

func assertSignerCode(t *testing.T, err error, code string, retryable bool) {
	t.Helper()
	var classified *SigningError
	if !errors.As(err, &classified) || classified.Code != code || classified.Retryable != retryable ||
		classified.Error() != code {
		t.Fatalf("signer error=%v, want %s retryable=%v", err, code, retryable)
	}
}

func assertUnsignedBlocked(t *testing.T, f *fixture, id string) {
	t.Helper()
	got, err := f.store.GetIntent(context.Background(), id)
	if err != nil || got.State != Blocked {
		t.Fatalf("intent should be blocked: %+v %v", got, err)
	}
	var raw []byte
	var status string
	if err := f.pool.QueryRow(context.Background(), `SELECT status,raw_tx FROM transaction_attempts
		WHERE intent_id=$1::uuid`, id).Scan(&status, &raw); err != nil || status != "PREPARING" || raw != nil {
		t.Fatalf("attempt should remain unsigned: %s %v", status, err)
	}
	assertAccountPaused(t, f)
}

func assertAccountPaused(t *testing.T, f *fixture) {
	t.Helper()
	req := testRequest("paused-next")
	req.SignerID = "dev"
	created, err := f.store.CreateIntent(context.Background(), req, testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.store.ReserveNonce(context.Background(), created.ID); !errors.Is(err, ErrAccountPaused) {
		t.Fatalf("blocked nonce allowed new allocation: %v", err)
	}
}

func TestResumeBlockedSigningClaim(t *testing.T) {
	f, signer := signingFixture(t)
	intent := signingIntent(t, f, "resume-invalid")
	malicious := alteredSigner{base: signer, change: func(tx *types.Transaction) *types.Transaction {
		d := dynamicCopy(tx)
		d.Nonce++
		return types.NewTx(d)
	}}
	_, err := f.store.SignInitial(context.Background(), intent.ID, malicious, signingFee(), signingOptions())
	assertSignerCode(t, err, "SIGNER_MUTATED_TRANSACTION", false)
	assertUnsignedBlocked(t, f, intent.ID)
	if err := f.store.Resume(context.Background(), intent.ID, "SIGNER_CORRECTED"); err != nil {
		t.Fatal(err)
	}
	identity, err := f.store.SignInitial(context.Background(), intent.ID, signer, signingFee(), signingOptions())
	if err != nil || identity.AttemptNumber != 1 {
		t.Fatalf("retry original attempt: %+v %v", identity, err)
	}
	var count int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM transaction_attempts
		WHERE intent_id=$1::uuid`, intent.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("retry created another attempt: %d %v", count, err)
	}
}

type waitingSigner struct{ TransactionSigner }

func (s waitingSigner) SignTransaction(ctx context.Context, _ *types.Transaction) (*types.Transaction, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSignerFailuresAndRetry(t *testing.T) {
	f, signer := signingFixture(t)
	intent := signingIntent(t, f, "timeout")
	opts := SignOptions{SignerTimeout: 30 * time.Millisecond, LeaseDuration: 2 * time.Second}
	_, err := f.store.SignInitial(context.Background(), intent.ID, waitingSigner{signer}, signingFee(), opts)
	assertSignerCode(t, err, "SIGNER_TIMEOUT", true)
	got, _ := f.store.GetIntent(context.Background(), intent.ID)
	if got.State != Signing || got.Reason != "SIGNER_TIMEOUT" {
		t.Fatalf("timeout state: %+v", got)
	}
	if _, err = f.store.SignInitial(context.Background(), intent.ID, signer, signingFee(), signingOptions()); err != nil {
		t.Fatalf("retry same attempt: %v", err)
	}
	var attempts int
	_ = f.pool.QueryRow(context.Background(), `SELECT count(*) FROM transaction_attempts WHERE intent_id=$1::uuid`, intent.ID).Scan(&attempts)
	if attempts != 1 {
		t.Fatalf("timeout retry created %d attempts", attempts)
	}
	other := signingIntent(t, f, "unavailable")
	_, err = f.store.SignInitial(context.Background(), other.ID,
		alteredSigner{base: signer, err: errors.New("fixture failure with sensitive detail")}, signingFee(), signingOptions())
	assertSignerCode(t, err, "SIGNER_UNAVAILABLE", true)
}

func TestDatabaseSigningAlignment(t *testing.T) {
	f, signer := signingFixture(t)
	intent := signingIntent(t, f, "alignment")
	address, _ := signer.Address(context.Background())
	claim, err := f.store.claimInitial(context.Background(), intent.ID, address, signingFee(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.pool.Exec(context.Background(), `UPDATE transaction_intents SET state='READY'
		WHERE intent_id=$1::uuid`, intent.ID); err == nil {
		t.Fatal("READY accepted unsigned attempt")
	}
	unsigned, _ := BuildType2(claim.candidate)
	signed, _ := signer.SignTransaction(context.Background(), unsigned)
	raw, _ := signed.MarshalBinary()
	if _, err := f.pool.Exec(context.Background(), `UPDATE transaction_attempts
		SET status='SIGNED',raw_tx=$2,transaction_hash=$3,signed_at=now()
		WHERE intent_id=$1::uuid`, intent.ID, raw, signed.Hash().Bytes()); err == nil {
		t.Fatal("signed attempt accepted while intent remained SIGNING")
	}
	if err := f.store.persistSigned(context.Background(), intent.ID, claim.token, claim.candidate,
		raw, common.Hash{}); err == nil {
		t.Fatal("persistence boundary accepted bytes with mismatched hash")
	}
}

func TestSigningCrashBoundaries(t *testing.T) {
	if point := os.Getenv("TXORCH_SIGNING_CRASH_POINT"); point != "" {
		crashSigningChild(point)
		return
	}
	for _, point := range []string{"before_call", "during_call", "before_commit", "after_commit"} {
		t.Run(point, func(t *testing.T) {
			f, signer := signingFixture(t)
			intent := signingIntent(t, f, "crash-"+point)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSigningCrashBoundaries$")
			cmd.Env = append(os.Environ(),
				"TXORCH_SIGNING_CRASH_POINT="+point,
				"TXORCH_TEST_SCHEMA="+f.schema,
				"TXORCH_TEST_INTENT_ID="+intent.ID,
				"TXORCH_TEST_DEV_KEY="+fmt.Sprintf("%x", crypto.FromECDSA(signer.key)))
			output, err := cmd.CombinedOutput()
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != 9 {
				t.Fatalf("child did not reach crash point %s: %v %s", point, err, output)
			}
			f.pool.Close()
			f.pool = nil
			f.open(t)
			var originalID string
			var originalRaw []byte
			if err := f.pool.QueryRow(context.Background(), `SELECT attempt_id::text,raw_tx
				FROM transaction_attempts WHERE intent_id=$1::uuid AND attempt_number=1`, intent.ID).
				Scan(&originalID, &originalRaw); err != nil {
				t.Fatal(err)
			}
			if point == "after_commit" {
				stored, err := f.store.LoadSignedAttempt(context.Background(), intent.ID)
				if err != nil || !bytes.Equal(stored.Raw, originalRaw) || stored.AttemptID != originalID {
					t.Fatalf("committed signed bytes not recovered: %v", err)
				}
				identity, err := f.store.SignInitial(context.Background(), intent.ID, nil, FeeInput{}, SignOptions{})
				if err != nil || identity.Hash != stored.Hash || identity.AttemptID != originalID {
					t.Fatalf("restart rebuilt an existing signed attempt: %+v %v", identity, err)
				}
			} else {
				if originalRaw != nil {
					t.Fatal("bytes persisted before signing commit")
				}
				_, err := f.pool.Exec(context.Background(), `UPDATE transaction_intents
					SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE intent_id=$1::uuid`, intent.ID)
				if err != nil {
					t.Fatal(err)
				}
				changedFee := FeeInput{MaxFeePerGasWei: "200000000", MaxTipPerGasWei: "20000000"}
				identity, err := f.store.SignInitial(context.Background(), intent.ID, signer, changedFee, signingOptions())
				if err != nil || identity.AttemptID != originalID {
					t.Fatalf("same candidate was not recovered: %+v %v", identity, err)
				}
				stored, err := f.store.LoadSignedAttempt(context.Background(), intent.ID)
				if err != nil {
					t.Fatal(err)
				}
				var decoded types.Transaction
				_ = decoded.UnmarshalBinary(stored.Raw)
				if decoded.GasFeeCap().Cmp(big.NewInt(100000000)) != 0 || decoded.Nonce() != 7 ||
					!bytes.Equal(decoded.Data(), []byte{0xde, 0xad, 0xbe, 0xef}) {
					t.Fatal("recovery changed the committed candidate")
				}
			}
			var count int
			if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM transaction_attempts
				WHERE intent_id=$1::uuid`, intent.ID).Scan(&count); err != nil || count != 1 {
				t.Fatalf("recovery created %d attempts: %v", count, err)
			}
		})
	}
}

type exitDuringSigner struct{ TransactionSigner }

func (s exitDuringSigner) SignTransaction(context.Context, *types.Transaction) (*types.Transaction, error) {
	os.Exit(9)
	return nil, nil
}

func crashSigningChild(point string) {
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
	var boundary signingHooks
	switch point {
	case "before_call":
		boundary.afterClaim = func() { os.Exit(9) }
	case "before_commit":
		boundary.afterSign = func() { os.Exit(9) }
	case "after_commit":
		boundary.afterCommit = func() { os.Exit(9) }
	case "during_call":
		signerBoundary := exitDuringSigner{signer}
		_, _ = NewStore(pool).signInitial(context.Background(), os.Getenv("TXORCH_TEST_INTENT_ID"),
			signerBoundary, signingFee(), signingOptions(), boundary)
		os.Exit(2)
	default:
		os.Exit(2)
	}
	_, _ = NewStore(pool).signInitial(context.Background(), os.Getenv("TXORCH_TEST_INTENT_ID"),
		signer, signingFee(), signingOptions(), boundary)
	os.Exit(2)
}
