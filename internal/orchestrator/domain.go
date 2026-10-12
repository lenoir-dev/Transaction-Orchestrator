package orchestrator

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

var (
	ErrConflict        = errors.New("idempotency key conflicts with immutable request")
	ErrNotFound        = errors.New("intent not found")
	ErrInvalid         = errors.New("invalid intent or policy")
	ErrTransition      = errors.New("invalid state transition")
	ErrAccountPaused   = errors.New("signer account is paused")
	ErrAccountConflict = errors.New("signer account identity conflicts")
	ErrNonceExhausted  = errors.New("nonce range exhausted")
)

type State string

const (
	Created    State = "CREATED"
	Reserved   State = "RESERVED"
	Signing    State = "SIGNING"
	Ready      State = "READY"
	Uncertain  State = "UNCERTAIN"
	Pending    State = "PENDING"
	Mined      State = "MINED"
	Confirming State = "CONFIRMING"
	Confirmed  State = "CONFIRMED"
	Reorged    State = "REORGED"
	Blocked    State = "BLOCKED"
	Failed     State = "FAILED"
)

// CanTransition describes lifecycle edges. Persistence methods additionally
// enforce evidence and phase-specific preconditions before taking an edge.
func CanTransition(from, to State) bool {
	allowed := map[State][]State{
		Created:    {Reserved, Blocked, Failed},
		Reserved:   {Signing, Blocked},
		Signing:    {Ready, Blocked},
		Ready:      {Uncertain, Blocked},
		Uncertain:  {Pending, Signing, Mined, Blocked},
		Pending:    {Uncertain, Signing, Mined, Blocked},
		Mined:      {Confirming, Confirmed, Failed, Reorged, Blocked},
		Confirming: {Confirmed, Reorged, Blocked},
		Confirmed:  {Reorged},
		Failed:     {Reorged},
		Reorged:    {Mined, Pending, Uncertain, Blocked},
		Blocked:    {Created, Reserved, Signing, Ready, Uncertain, Pending, Mined, Confirming, Reorged},
	}
	for _, state := range allowed[from] {
		if state == to {
			return true
		}
	}
	return false
}

type Request struct {
	TenantID          string
	IdempotencyKey    string
	ChainID           string
	SignerID          string
	To                string
	ValueWei          string
	Calldata          string
	GasProfile        string
	GasLimit          uint64 // Zero means the profile's configured limit.
	ConfirmationDepth int    // Zero means the configured depth.
}

type Policy struct {
	Version           string
	GasLimit          uint64
	ConfirmationDepth int
	MaxFeeCapWei      string
	MaxTipCapWei      string
	MaxReplacements   int
}

type Account struct {
	ChainID      string
	SignerID     string
	Address      [20]byte
	InitialNonce uint64 // Explicit trusted seed; this package does not query a chain.
}

type Intent struct {
	ID            string
	TenantID      string
	ChainID       string
	SignerID      string
	State         State
	Reason        string
	Nonce         *uint64
	Version       int64
	PolicyVersion string
}

type normalizedRequest struct {
	chainID string
	to      []byte
	value   string
	data    []byte
	digest  [32]byte
}

func decimal(value string, positive bool) (string, error) {
	if value == "" || len(value) > 78 {
		return "", ErrInvalid
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return "", ErrInvalid
		}
	}
	n := new(big.Int)
	if _, ok := n.SetString(value, 10); !ok || (positive && n.Sign() == 0) {
		return "", ErrInvalid
	}
	return n.String(), nil
}

func hexBytes(value string, size int) ([]byte, error) {
	if !strings.HasPrefix(value, "0x") || len(value)%2 != 0 {
		return nil, ErrInvalid
	}
	b, err := hex.DecodeString(value[2:])
	if err != nil || (size >= 0 && len(b) != size) {
		return nil, ErrInvalid
	}
	return b, nil
}

func normalize(req Request) (normalizedRequest, error) {
	if strings.TrimSpace(req.TenantID) == "" || len(req.IdempotencyKey) < 1 || len(req.IdempotencyKey) > 255 ||
		strings.TrimSpace(req.SignerID) == "" ||
		(req.GasProfile != "standard" && req.GasProfile != "cautious") ||
		req.GasLimit > math.MaxInt64 || req.ConfirmationDepth < 0 || req.ConfirmationDepth > 100000 {
		return normalizedRequest{}, ErrInvalid
	}
	chain, err := decimal(req.ChainID, true)
	if err != nil {
		return normalizedRequest{}, err
	}
	value, err := decimal(req.ValueWei, false)
	if err != nil {
		return normalizedRequest{}, err
	}
	to, err := hexBytes(req.To, 20)
	if err != nil {
		return normalizedRequest{}, err
	}
	data, err := hexBytes(req.Calldata, -1)
	if err != nil {
		return normalizedRequest{}, err
	}
	canonical := struct {
		ChainID, SignerID, To, ValueWei, Calldata, GasProfile string
		GasLimit                                              uint64
		ConfirmationDepth                                     int
	}{chain, req.SignerID, hex.EncodeToString(to), value, hex.EncodeToString(data), req.GasProfile,
		req.GasLimit, req.ConfirmationDepth}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return normalizedRequest{}, err
	}
	return normalizedRequest{chainID: chain, to: to, value: value, data: data, digest: sha256.Sum256(encoded)}, nil
}

func (p Policy) validate() (fee, tip string, err error) {
	if p.Version == "" || p.GasLimit == 0 || p.GasLimit > math.MaxInt64 ||
		p.ConfirmationDepth < 1 || p.ConfirmationDepth > 100000 ||
		p.MaxReplacements < 0 || p.MaxReplacements > 100 {
		return "", "", ErrInvalid
	}
	fee, err = decimal(p.MaxFeeCapWei, true)
	if err != nil {
		return "", "", err
	}
	tip, err = decimal(p.MaxTipCapWei, true)
	if err != nil {
		return "", "", err
	}
	a, _ := new(big.Int).SetString(fee, 10)
	b, _ := new(big.Int).SetString(tip, 10)
	if b.Cmp(a) > 0 {
		return "", "", ErrInvalid
	}
	return fee, tip, nil
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	s := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", s[:8], s[8:12], s[12:16], s[16:20], s[20:]), nil
}

func parseNonce(value string) (uint64, error) {
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid stored nonce: %w", err)
	}
	return n, nil
}
