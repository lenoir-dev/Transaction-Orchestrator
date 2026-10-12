package orchestrator

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

// BroadcastEndpoint is a configured, bounded RPC boundary. Alias is a stable,
// low-cardinality identifier; it must not contain credentials or a URL.
type BroadcastEndpoint interface {
	Alias() string
	ChainID(context.Context) (*big.Int, error)
	Genesis(context.Context) (common.Hash, error)
	SendRawTransaction(context.Context, []byte) (common.Hash, error)
	TransactionByHash(context.Context, common.Hash) (*types.Transaction, error)
	ReceiptByHash(context.Context, common.Hash) (*ReceiptEvidence, error)
	LatestNonce(context.Context, common.Address) (uint64, error)
	PendingNonce(context.Context, common.Address) (uint64, error)
	BlockByHash(context.Context, common.Hash) (*BlockEvidence, error)
	BlockByNumber(context.Context, uint64) (*BlockEvidence, error)
	LatestBlock(context.Context) (*BlockEvidence, error)
}

type ReceiptEvidence struct {
	Hash              common.Hash
	BlockHash         common.Hash
	BlockNumber       uint64
	Status            uint64
	TransactionIndex  uint64
	GasUsed           uint64
	EffectiveGasPrice string
}

type BlockEvidence struct {
	Hash         common.Hash
	Number       uint64
	ParentHash   common.Hash
	Transactions []common.Hash
}

type RPCFault struct {
	Code    int
	Message string
}

func (e *RPCFault) Error() string { return "RPC rejected request" }

type ResultClass string

const (
	ResultSuccess      ResultClass = "SUCCESS"
	ResultAlreadyKnown ResultClass = "ALREADY_KNOWN"
	ResultTransport    ResultClass = "AMBIGUOUS_TRANSPORT"
	ResultRejected     ResultClass = "DETERMINISTIC_REJECTION"
	ResultNonce        ResultClass = "NONCE_AMBIGUITY"
	ResultFee          ResultClass = "FEE_REJECTION"
	ResultProvider     ResultClass = "PROVIDER_ERROR"
	ResultHashMismatch ResultClass = "HASH_MISMATCH"
)

// Error text is interpreted only here, for known node validation responses.
// An unfamiliar error is never interpreted as proof of nonacceptance.
func ClassifySend(err error) ResultClass {
	if err == nil {
		return ResultSuccess
	}
	var fault *RPCFault
	if errors.As(err, &fault) {
		m := strings.ToLower(fault.Message)
		switch {
		case strings.Contains(m, "already known") || strings.Contains(m, "already imported"):
			return ResultAlreadyKnown
		case strings.Contains(m, "nonce too low"):
			return ResultNonce
		case strings.Contains(m, "underpriced") || strings.Contains(m, "fee cap less than") ||
			strings.Contains(m, "max fee per gas less than") || strings.Contains(m, "base fee exceeds"):
			return ResultFee
		case strings.Contains(m, "invalid sender") || strings.Contains(m, "invalid signature") ||
			strings.Contains(m, "intrinsic gas too low") || strings.Contains(m, "invalid transaction"):
			return ResultRejected
		default:
			return ResultProvider
		}
	}
	if errors.Is(err, ErrRPCMalformed) {
		return ResultProvider
	}
	// Network and deadline errors may occur after the endpoint accepted the bytes.
	return ResultTransport
}

var ErrRPCMalformed = errors.New("malformed RPC response")

type HTTPRPC struct {
	Name   string
	URL    string
	Client *http.Client
}

func (r *HTTPRPC) Alias() string { return r.Name }

func (r *HTTPRPC) call(ctx context.Context, method string, params any, target any) error {
	if r == nil || r.Client == nil || r.URL == "" {
		return ErrRPCMalformed
	}
	request, _ := json.Marshal(struct {
		Version string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{"2.0", 1, method, params})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.URL, bytes.NewReader(request))
	if err != nil {
		return ErrRPCMalformed
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return ErrRPCMalformed
	}
	var envelope struct {
		Version string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   *RPCFault       `json:"error"`
	}
	limited := io.LimitReader(resp.Body, 2<<20)
	decoder := json.NewDecoder(limited)
	if decoder.Decode(&envelope) != nil || envelope.Version != "2.0" ||
		string(envelope.ID) != "1" || len(envelope.Result) == 0 && envelope.Error == nil {
		return ErrRPCMalformed
	}
	var extra any
	if decoder.Decode(&extra) != io.EOF {
		return ErrRPCMalformed
	}
	if envelope.Error != nil {
		return envelope.Error
	}
	if err = json.Unmarshal(envelope.Result, target); err != nil {
		return ErrRPCMalformed
	}
	return nil
}

func rpcQuantity(value string) (*big.Int, error) {
	if !strings.HasPrefix(value, "0x") || len(value) < 3 || len(value) > 66 {
		return nil, ErrRPCMalformed
	}
	if len(value) > 3 && value[2] == '0' {
		return nil, ErrRPCMalformed
	}
	for _, c := range value[2:] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return nil, ErrRPCMalformed
		}
	}
	n := new(big.Int)
	if _, ok := n.SetString(value[2:], 16); !ok {
		return nil, ErrRPCMalformed
	}
	return n, nil
}

func (r *HTTPRPC) ChainID(ctx context.Context) (*big.Int, error) {
	var s string
	if err := r.call(ctx, "eth_chainId", []any{}, &s); err != nil {
		return nil, err
	}
	return rpcQuantity(s)
}

func (r *HTTPRPC) Genesis(ctx context.Context) (common.Hash, error) {
	var block *struct {
		Hash string `json:"hash"`
	}
	if err := r.call(ctx, "eth_getBlockByNumber", []any{"0x0", false}, &block); err != nil {
		return common.Hash{}, err
	}
	if block == nil || !validHash(block.Hash) {
		return common.Hash{}, ErrRPCMalformed
	}
	return common.HexToHash(block.Hash), nil
}

func (r *HTTPRPC) SendRawTransaction(ctx context.Context, raw []byte) (common.Hash, error) {
	var s string
	if err := r.call(ctx, "eth_sendRawTransaction", []any{"0x" + hex.EncodeToString(raw)}, &s); err != nil {
		return common.Hash{}, err
	}
	if !validHash(s) {
		return common.Hash{}, ErrRPCMalformed
	}
	return common.HexToHash(s), nil
}

func (r *HTTPRPC) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, error) {
	var raw json.RawMessage
	if err := r.call(ctx, "eth_getTransactionByHash", []any{hash.Hex()}, &raw); err != nil {
		return nil, err
	}
	if string(raw) == "null" {
		return nil, nil
	}
	var tx types.Transaction
	if err := json.Unmarshal(raw, &tx); err != nil {
		return nil, ErrRPCMalformed
	}
	return &tx, nil
}

func (r *HTTPRPC) ReceiptByHash(ctx context.Context, hash common.Hash) (*ReceiptEvidence, error) {
	var raw *struct {
		Hash              string `json:"transactionHash"`
		Block             string `json:"blockHash"`
		Number            string `json:"blockNumber"`
		Status            string `json:"status"`
		Index             string `json:"transactionIndex"`
		GasUsed           string `json:"gasUsed"`
		EffectiveGasPrice string `json:"effectiveGasPrice"`
	}
	if err := r.call(ctx, "eth_getTransactionReceipt", []any{hash.Hex()}, &raw); err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	n, err := rpcQuantity(raw.Number)
	if err != nil || !n.IsUint64() || !validHash(raw.Hash) || !validHash(raw.Block) {
		return nil, ErrRPCMalformed
	}
	status, statusErr := rpcQuantity(raw.Status)
	if statusErr != nil || !status.IsUint64() || status.Uint64() > 1 {
		return nil, ErrRPCMalformed
	}
	index, indexErr := rpcQuantity(raw.Index)
	gas, gasErr := rpcQuantity(raw.GasUsed)
	if indexErr != nil || gasErr != nil || !index.IsUint64() || !gas.IsUint64() {
		return nil, ErrRPCMalformed
	}
	price := ""
	if raw.EffectiveGasPrice != "" {
		parsed, priceErr := rpcQuantity(raw.EffectiveGasPrice)
		if priceErr != nil {
			return nil, ErrRPCMalformed
		}
		price = parsed.String()
	}
	return &ReceiptEvidence{Hash: common.HexToHash(raw.Hash), BlockHash: common.HexToHash(raw.Block),
		BlockNumber: n.Uint64(), Status: status.Uint64(), TransactionIndex: index.Uint64(),
		GasUsed: gas.Uint64(), EffectiveGasPrice: price}, nil
}

func (r *HTTPRPC) block(ctx context.Context, selector any, byHash bool) (*BlockEvidence, error) {
	method := "eth_getBlockByNumber"
	if byHash {
		method = "eth_getBlockByHash"
	}
	var raw *struct {
		Hash         string   `json:"hash"`
		Number       string   `json:"number"`
		Parent       string   `json:"parentHash"`
		Transactions []string `json:"transactions"`
	}
	if err := r.call(ctx, method, []any{selector, false}, &raw); err != nil {
		return nil, err
	}
	if raw == nil {
		return nil, nil
	}
	n, err := rpcQuantity(raw.Number)
	if err != nil || !n.IsUint64() || !validHash(raw.Hash) || !validHash(raw.Parent) {
		return nil, ErrRPCMalformed
	}
	b := &BlockEvidence{Hash: common.HexToHash(raw.Hash), Number: n.Uint64(), ParentHash: common.HexToHash(raw.Parent)}
	for _, hash := range raw.Transactions {
		if !validHash(hash) {
			return nil, ErrRPCMalformed
		}
		b.Transactions = append(b.Transactions, common.HexToHash(hash))
	}
	return b, nil
}

func (r *HTTPRPC) BlockByHash(ctx context.Context, hash common.Hash) (*BlockEvidence, error) {
	return r.block(ctx, hash.Hex(), true)
}
func (r *HTTPRPC) BlockByNumber(ctx context.Context, number uint64) (*BlockEvidence, error) {
	return r.block(ctx, fmt.Sprintf("0x%x", number), false)
}
func (r *HTTPRPC) LatestBlock(ctx context.Context) (*BlockEvidence, error) {
	return r.block(ctx, "latest", false)
}

func (r *HTTPRPC) LatestNonce(ctx context.Context, address common.Address) (uint64, error) {
	return r.accountNonce(ctx, address, "latest")
}

func (r *HTTPRPC) PendingNonce(ctx context.Context, address common.Address) (uint64, error) {
	return r.accountNonce(ctx, address, "pending")
}

func (r *HTTPRPC) accountNonce(ctx context.Context, address common.Address, tag string) (uint64, error) {
	var s string
	if err := r.call(ctx, "eth_getTransactionCount", []any{address.Hex(), tag}, &s); err != nil {
		return 0, err
	}
	n, err := rpcQuantity(s)
	if err != nil || !n.IsUint64() {
		return 0, ErrRPCMalformed
	}
	return n.Uint64(), nil
}

// Keep RPC failures out of public error strings, including URL credentials.
func rpcCode(c ResultClass) error { return fmt.Errorf("broadcast RPC outcome: %s", c) }

func validAlias(s string) bool {
	if len(s) < 1 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

func validHash(s string) bool {
	if len(s) != 66 || !strings.HasPrefix(s, "0x") {
		return false
	}
	_, err := hex.DecodeString(s[2:])
	return err == nil
}
