package orchestrator

import (
	"bytes"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

var (
	ErrTransactionFields = errors.New("invalid transaction construction fields")
	ErrSignedMismatch    = errors.New("signed transaction differs from requested fields")
	ErrSignedSender      = errors.New("signed transaction sender mismatch")
)

// Candidate is a snapshot of immutable intent fields, its reserved nonce,
// and the selected fee values stored on one attempt.
type Candidate struct {
	ChainID      *big.Int
	Signer       common.Address
	Nonce        uint64
	To           common.Address
	Value        *big.Int
	Calldata     []byte
	GasLimit     uint64
	MaxFeePerGas *big.Int
	MaxTipPerGas *big.Int
	PolicyID     string
}

var maxUint256 = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))

func validUint256(n *big.Int, positive bool) bool {
	return n != nil && n.Sign() >= 0 && (!positive || n.Sign() > 0) && n.Cmp(maxUint256) <= 0
}

// BuildType2 has no clock, randomness, network reads, or mutable policy input.
// Callers supply only values already committed to the intent and attempt.
func BuildType2(c Candidate) (*types.Transaction, error) {
	if !validUint256(c.ChainID, true) || !validUint256(c.Value, false) ||
		!validUint256(c.MaxFeePerGas, true) || !validUint256(c.MaxTipPerGas, true) ||
		c.MaxTipPerGas.Cmp(c.MaxFeePerGas) > 0 || c.GasLimit == 0 || c.PolicyID == "" {
		return nil, ErrTransactionFields
	}
	to := c.To
	return types.NewTx(&types.DynamicFeeTx{
		ChainID:   new(big.Int).Set(c.ChainID),
		Nonce:     c.Nonce,
		GasTipCap: new(big.Int).Set(c.MaxTipPerGas),
		GasFeeCap: new(big.Int).Set(c.MaxFeePerGas),
		Gas:       c.GasLimit,
		To:        &to,
		Value:     new(big.Int).Set(c.Value),
		Data:      bytes.Clone(c.Calldata),
	}), nil
}

// ValidateSigned checks every business and fee field before accepting signer
// output. Sender recovery also rejects missing or malformed signatures.
func ValidateSigned(c Candidate, signed *types.Transaction) error {
	if signed == nil || signed.Type() != types.DynamicFeeTxType ||
		signed.ChainId().Cmp(c.ChainID) != 0 || signed.Nonce() != c.Nonce ||
		signed.To() == nil || *signed.To() != c.To || signed.Value().Cmp(c.Value) != 0 ||
		!bytes.Equal(signed.Data(), c.Calldata) || signed.Gas() != c.GasLimit ||
		signed.GasFeeCap().Cmp(c.MaxFeePerGas) != 0 ||
		signed.GasTipCap().Cmp(c.MaxTipPerGas) != 0 || len(signed.AccessList()) != 0 {
		return ErrSignedMismatch
	}
	sender, err := types.Sender(types.NewLondonSigner(c.ChainID), signed)
	if err != nil || sender != c.Signer {
		return ErrSignedSender
	}
	return nil
}

func decimalUint256(value string, positive bool) (*big.Int, error) {
	n, ok := new(big.Int).SetString(value, 10)
	if !ok || !validUint256(n, positive) {
		return nil, ErrTransactionFields
	}
	return n, nil
}
