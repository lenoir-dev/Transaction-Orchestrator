package orchestrator

import (
	"context"
	"crypto/ecdsa"
	"errors"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

// TransactionSigner receives a fully constructed transaction. It cannot
// choose the call, nonce, chain, or fee policy through this interface.
type TransactionSigner interface {
	Address(ctx context.Context) (common.Address, error)
	SignTransaction(ctx context.Context, tx *types.Transaction) (*types.Transaction, error)
}

// DevLocalSigner holds an explicit development key in process memory. A
// production custody adapter must implement TransactionSigner separately.
type DevLocalSigner struct {
	key     *ecdsa.PrivateKey
	address common.Address
}

func NewDevLocalSigner(key *ecdsa.PrivateKey) (*DevLocalSigner, error) {
	if key == nil || key.D == nil || key.PublicKey.X == nil || key.PublicKey.Y == nil {
		return nil, errors.New("invalid development signer key")
	}
	owned, err := crypto.ToECDSA(crypto.FromECDSA(key))
	if err != nil || owned.PublicKey.X.Cmp(key.PublicKey.X) != 0 ||
		owned.PublicKey.Y.Cmp(key.PublicKey.Y) != 0 {
		return nil, errors.New("invalid development signer key")
	}
	return &DevLocalSigner{key: owned, address: crypto.PubkeyToAddress(owned.PublicKey)}, nil
}

func (s *DevLocalSigner) Address(ctx context.Context) (common.Address, error) {
	if err := ctx.Err(); err != nil {
		return common.Address{}, err
	}
	return s.address, nil
}

func (s *DevLocalSigner) SignTransaction(ctx context.Context, tx *types.Transaction) (*types.Transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if tx == nil || tx.Type() != types.DynamicFeeTxType {
		return nil, ErrTransactionFields
	}
	signed, err := types.SignTx(tx, types.NewLondonSigner(tx.ChainId()), s.key)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return signed, nil
}
