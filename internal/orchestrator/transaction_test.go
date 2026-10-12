package orchestrator

import (
	"bytes"
	"math"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

func sampleCandidate() Candidate {
	return Candidate{
		ChainID: big.NewInt(31337), Signer: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Nonce: 7, To: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Value: big.NewInt(42), Calldata: []byte{0xde, 0xad, 0xbe, 0xef}, GasLimit: 75000,
		MaxFeePerGas: big.NewInt(100000000), MaxTipPerGas: big.NewInt(10000000), PolicyID: "fixture-v1",
	}
}

func TestDeterministicType2Construction(t *testing.T) {
	c := sampleCandidate()
	a, err := BuildType2(c)
	if err != nil {
		t.Fatal(err)
	}
	b, err := BuildType2(c)
	if err != nil {
		t.Fatal(err)
	}
	araw, _ := a.MarshalBinary()
	braw, _ := b.MarshalBinary()
	if !bytes.Equal(araw, braw) || a.Type() != types.DynamicFeeTxType ||
		a.ChainId().Cmp(c.ChainID) != 0 || a.Nonce() != 7 || *a.To() != c.To ||
		a.Value().Cmp(c.Value) != 0 || !bytes.Equal(a.Data(), c.Calldata) ||
		a.Gas() != c.GasLimit || a.GasFeeCap().Cmp(c.MaxFeePerGas) != 0 ||
		a.GasTipCap().Cmp(c.MaxTipPerGas) != 0 || len(a.AccessList()) != 0 {
		t.Fatal("type-2 field mapping or deterministic encoding failed")
	}
	c.Calldata[0] = 0x00
	c.Value.SetInt64(99)
	if !bytes.Equal(a.Data(), []byte{0xde, 0xad, 0xbe, 0xef}) || a.Value().Int64() != 42 {
		t.Fatal("transaction retained mutable input")
	}
}

func TestTransactionQuantityBounds(t *testing.T) {
	for name, mutate := range map[string]func(*Candidate){
		"chain overflow": func(c *Candidate) { c.ChainID = new(big.Int).Lsh(big.NewInt(1), 256) },
		"value overflow": func(c *Candidate) { c.Value = new(big.Int).Lsh(big.NewInt(1), 256) },
		"fee overflow":   func(c *Candidate) { c.MaxFeePerGas = new(big.Int).Lsh(big.NewInt(1), 256) },
		"tip above fee":  func(c *Candidate) { c.MaxTipPerGas = big.NewInt(100000001) },
		"negative value": func(c *Candidate) { c.Value = big.NewInt(-1) },
		"missing gas":    func(c *Candidate) { c.GasLimit = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			c := sampleCandidate()
			mutate(&c)
			if _, err := BuildType2(c); err == nil {
				t.Fatal("invalid construction accepted")
			}
		})
	}
	if _, err := decimalUint256(new(big.Int).Lsh(big.NewInt(1), 256).String(), true); err == nil {
		t.Fatal("decimal uint256 overflow accepted")
	}
	if n, err := parseNonce(new(big.Int).SetUint64(math.MaxUint64).String()); err != nil || n != math.MaxUint64 {
		t.Fatalf("uint64 nonce boundary: %d %v", n, err)
	}
	if _, err := parseNonce("18446744073709551616"); err == nil {
		t.Fatal("nonce overflow accepted")
	}
}

func TestDevelopmentSignerOwnsKeyCopy(t *testing.T) {
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	signer, err := NewDevLocalSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	address, _ := signer.Address(t.Context())
	key.D.SetInt64(1)
	key.PublicKey.X.SetInt64(0)
	c := sampleCandidate()
	c.Signer = address
	unsigned, err := BuildType2(c)
	if err != nil {
		t.Fatal(err)
	}
	signed, err := signer.SignTransaction(t.Context(), unsigned)
	if err != nil || ValidateSigned(c, signed) != nil {
		t.Fatalf("external key mutation affected signer: %v", err)
	}
}
