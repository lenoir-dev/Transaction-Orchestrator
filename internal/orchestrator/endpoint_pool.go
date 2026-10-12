package orchestrator

import (
	"context"
	"errors"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type EndpointCapability uint8

const (
	EndpointRead EndpointCapability = 1 << iota
	EndpointBroadcast
	EndpointNonce
)

type EndpointState string

const (
	EndpointHealthy    EndpointState = "HEALTHY"
	EndpointDegraded   EndpointState = "DEGRADED"
	EndpointCooldown   EndpointState = "COOLDOWN"
	EndpointIneligible EndpointState = "INELIGIBLE"
)

type EndpointConfig struct {
	ID           string
	RPC          BroadcastEndpoint
	ChainID      string
	GenesisHash  common.Hash
	Capabilities EndpointCapability
	MaxInflight  int
}

type EndpointPoolPolicy struct {
	ProbeTimeout   time.Duration
	Cooldown       time.Duration
	FailureLimit   int
	MaxHeadLag     uint64
	MaxRPCInflight int
	Now            func() time.Time
}

type EndpointStatus struct {
	ID            string        `json:"id"`
	ChainID       string        `json:"chain_id"`
	State         EndpointState `json:"state"`
	Reason        string        `json:"reason,omitempty"`
	Head          uint64        `json:"head"`
	HeadHash      common.Hash   `json:"head_hash"`
	Failures      int           `json:"failures"`
	CooldownUntil time.Time     `json:"cooldown_until,omitempty"`
	Inflight      int           `json:"inflight"`
}

type RPCMetric struct {
	Endpoint string
	Method   string
	Status   string
	Count    uint64
	Latency  time.Duration
}

type endpointEntry struct {
	config EndpointConfig
	status EndpointStatus
	sem    chan struct{}
}

// EndpointPool records availability, not chain consensus. A configured ID is
// the only endpoint label exposed through status and metrics.
type EndpointPool struct {
	mu       sync.Mutex
	entries  []*endpointEntry
	policy   EndpointPoolPolicy
	rpcSlots chan struct{}
	metrics  map[string]*RPCMetric
	failover map[string]uint64
}

func NewEndpointPool(configs []EndpointConfig, policy EndpointPoolPolicy) (*EndpointPool, error) {
	if len(configs) == 0 || len(configs) > 8 || policy.ProbeTimeout <= 0 ||
		policy.Cooldown <= 0 || policy.Cooldown > time.Hour || policy.FailureLimit < 1 ||
		policy.FailureLimit > 100 || policy.MaxHeadLag > 1000 ||
		policy.MaxRPCInflight < 1 || policy.MaxRPCInflight > 256 {
		return nil, ErrInvalid
	}
	if policy.Now == nil {
		policy.Now = time.Now
	}
	p := &EndpointPool{policy: policy, rpcSlots: make(chan struct{}, policy.MaxRPCInflight),
		metrics: make(map[string]*RPCMetric), failover: make(map[string]uint64)}
	seen := make(map[string]bool)
	for _, cfg := range configs {
		if cfg.RPC == nil || !validAlias(cfg.ID) || cfg.RPC.Alias() != cfg.ID || seen[cfg.ID] ||
			cfg.MaxInflight < 1 || cfg.MaxInflight > policy.MaxRPCInflight ||
			cfg.Capabilities == 0 || cfg.Capabilities & ^(EndpointRead|EndpointBroadcast|EndpointNonce) != 0 {
			return nil, ErrInvalid
		}
		if _, err := decimal(cfg.ChainID, true); err != nil {
			return nil, ErrInvalid
		}
		seen[cfg.ID] = true
		p.entries = append(p.entries, &endpointEntry{config: cfg,
			status: EndpointStatus{ID: cfg.ID, ChainID: cfg.ChainID, State: EndpointCooldown,
				Reason: "NOT_PROBED"}, sem: make(chan struct{}, cfg.MaxInflight)})
	}
	return p, nil
}

func (p *EndpointPool) now() time.Time { return p.policy.Now() }

// Refresh probes each configured endpoint once with a deadline. A wrong chain
// or anchor is permanently ineligible for this pool configuration.
func (p *EndpointPool) Refresh(ctx context.Context) error {
	for _, e := range p.entries {
		p.mu.Lock()
		status := e.status
		p.mu.Unlock()
		if status.State == EndpointIneligible ||
			(status.State == EndpointCooldown && p.now().Before(status.CooldownUntil)) {
			continue
		}
		p.probe(ctx, e)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	maxHead := make(map[string]uint64)
	for _, e := range p.entries {
		if e.status.State == EndpointHealthy && e.status.Head > maxHead[e.config.ChainID] {
			maxHead[e.config.ChainID] = e.status.Head
		}
	}
	for _, e := range p.entries {
		if e.status.State == EndpointHealthy &&
			maxHead[e.config.ChainID]-e.status.Head > p.policy.MaxHeadLag {
			e.status.State, e.status.Reason = EndpointDegraded, "STALE_HEAD"
		}
	}
	return ctx.Err()
}

func (p *EndpointPool) probe(ctx context.Context, e *endpointEntry) {
	check, cancel := context.WithTimeout(ctx, p.policy.ProbeTimeout)
	defer cancel()
	if err := p.acquire(check, e); err != nil {
		p.probeFailure(e, "PROBE_TIMEOUT")
		return
	}
	defer p.release(e)
	rpc := e.config.RPC
	chain, err := rpc.ChainID(check)
	if err != nil || chain == nil {
		p.probeFailure(e, "CHAIN_UNAVAILABLE")
		return
	}
	expected, _ := new(big.Int).SetString(e.config.ChainID, 10)
	if chain.Cmp(expected) != 0 {
		p.probeHardFailure(e, "WRONG_CHAIN")
		return
	}
	if e.config.GenesisHash != (common.Hash{}) {
		genesis, genesisErr := rpc.Genesis(check)
		if genesisErr != nil {
			p.probeFailure(e, "ANCHOR_UNAVAILABLE")
			return
		}
		if genesis != e.config.GenesisHash {
			p.probeHardFailure(e, "WRONG_ANCHOR")
			return
		}
	}
	head, err := rpc.LatestBlock(check)
	if err != nil || head == nil || head.Hash == (common.Hash{}) {
		p.probeFailure(e, "HEAD_UNAVAILABLE")
		return
	}
	canonical, err := rpc.BlockByNumber(check, head.Number)
	if err != nil || canonical == nil || canonical.Hash != head.Hash {
		p.probeFailure(e, "HEAD_INCONSISTENT")
		return
	}
	if e.config.Capabilities&EndpointRead != 0 {
		if _, err = rpc.TransactionByHash(check, common.Hash{}); err != nil {
			p.probeFailure(e, "LOOKUP_UNAVAILABLE")
			return
		}
		if _, err = rpc.ReceiptByHash(check, common.Hash{}); err != nil {
			p.probeFailure(e, "RECEIPT_UNAVAILABLE")
			return
		}
	}
	if e.config.Capabilities&EndpointNonce != 0 {
		if _, err = rpc.LatestNonce(check, common.Address{}); err != nil {
			p.probeFailure(e, "NONCE_UNAVAILABLE")
			return
		}
		if _, err = rpc.PendingNonce(check, common.Address{}); err != nil {
			p.probeFailure(e, "PENDING_NONCE_UNAVAILABLE")
			return
		}
	}
	p.mu.Lock()
	e.status.State, e.status.Reason, e.status.Failures = EndpointHealthy, "", 0
	e.status.Head, e.status.HeadHash = head.Number, head.Hash
	e.status.CooldownUntil = time.Time{}
	p.mu.Unlock()
}

func (p *EndpointPool) probeHardFailure(e *endpointEntry, reason string) {
	p.mu.Lock()
	e.status.State, e.status.Reason = EndpointIneligible, reason
	p.mu.Unlock()
}

func (p *EndpointPool) probeFailure(e *endpointEntry, reason string) {
	p.mu.Lock()
	e.status.State, e.status.Reason = EndpointCooldown, reason
	e.status.CooldownUntil = p.now().Add(p.policy.Cooldown)
	p.mu.Unlock()
}

func (p *EndpointPool) acquire(ctx context.Context, e *endpointEntry) error {
	select {
	case p.rpcSlots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case e.sem <- struct{}{}:
		p.mu.Lock()
		e.status.Inflight++
		p.mu.Unlock()
		return nil
	case <-ctx.Done():
		<-p.rpcSlots
		return ctx.Err()
	}
}

func (p *EndpointPool) release(e *endpointEntry) {
	<-e.sem
	<-p.rpcSlots
	p.mu.Lock()
	e.status.Inflight--
	p.mu.Unlock()
}

func (p *EndpointPool) observed(e *endpointEntry, method string, started time.Time, err error) {
	result := "ok"
	if err != nil {
		result = "error"
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key := e.config.ID + "/" + method + "/" + result
	m := p.metrics[key]
	if m == nil {
		m = &RPCMetric{Endpoint: e.config.ID, Method: method, Status: result}
		p.metrics[key] = m
	}
	m.Count++
	m.Latency += p.now().Sub(started)
	if err == nil {
		e.status.Failures = 0
		return
	}
	if errors.Is(err, context.Canceled) {
		return
	}
	e.status.Failures++
	if e.status.Failures >= p.policy.FailureLimit && e.status.State != EndpointIneligible {
		e.status.State, e.status.Reason = EndpointCooldown, "RPC_FAILURES"
		e.status.CooldownUntil = p.now().Add(p.policy.Cooldown)
	}
}

// Eligible returns at most eight independently checked endpoints. Degraded
// heads are omitted from current-state decisions; absence is never inferred.
func (p *EndpointPool) Eligible(chainID string, capability EndpointCapability) []BroadcastEndpoint {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []BroadcastEndpoint
	for _, e := range p.entries {
		if e.config.ChainID == chainID && e.status.State == EndpointHealthy &&
			e.config.Capabilities&capability == capability {
			out = append(out, &managedEndpoint{pool: p, entry: e})
		}
	}
	return out
}

func (p *EndpointPool) Status() []EndpointStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]EndpointStatus, 0, len(p.entries))
	for _, e := range p.entries {
		out = append(out, e.status)
	}
	return out
}

func (p *EndpointPool) Metrics() []RPCMetric {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]RPCMetric, 0, len(p.metrics))
	for _, value := range p.metrics {
		out = append(out, *value)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Endpoint != out[j].Endpoint {
			return out[i].Endpoint < out[j].Endpoint
		}
		if out[i].Method != out[j].Method {
			return out[i].Method < out[j].Method
		}
		return out[i].Status < out[j].Status
	})
	return out
}

func (p *EndpointPool) RecordFailover(reason string) {
	if reason != "timeout" && reason != "unavailable" && reason != "not_found" {
		reason = "other"
	}
	p.mu.Lock()
	p.failover[reason]++
	p.mu.Unlock()
}

func (p *EndpointPool) Failovers() map[string]uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make(map[string]uint64, len(p.failover))
	for key, count := range p.failover {
		out[key] = count
	}
	return out
}

type managedEndpoint struct {
	pool  *EndpointPool
	entry *endpointEntry
}

func (m *managedEndpoint) Alias() string { return m.entry.config.ID }

func managedCall[T any](m *managedEndpoint, ctx context.Context, method string,
	call func(context.Context) (T, error)) (T, error) {
	ctx, span := otel.Tracer("transaction-orchestrator").Start(ctx, "rpc.call",
		trace.WithAttributes(attribute.String("rpc.method", method), attribute.String("rpc.endpoint", m.entry.config.ID)))
	defer span.End()
	var zero T
	m.pool.mu.Lock()
	eligible := m.entry.status.State == EndpointHealthy
	m.pool.mu.Unlock()
	if !eligible {
		return zero, ErrNoEligibleEndpoint
	}
	if err := m.pool.acquire(ctx, m.entry); err != nil {
		return zero, err
	}
	defer m.pool.release(m.entry)
	started := m.pool.now()
	value, err := call(ctx)
	m.pool.observed(m.entry, method, started, err)
	return value, err
}

func (m *managedEndpoint) ChainID(ctx context.Context) (*big.Int, error) {
	return managedCall(m, ctx, "eth_chainId", m.entry.config.RPC.ChainID)
}
func (m *managedEndpoint) Genesis(ctx context.Context) (common.Hash, error) {
	return managedCall(m, ctx, "eth_getBlockByNumber", m.entry.config.RPC.Genesis)
}
func (m *managedEndpoint) SendRawTransaction(ctx context.Context, raw []byte) (common.Hash, error) {
	return managedCall(m, ctx, "eth_sendRawTransaction", func(c context.Context) (common.Hash, error) {
		return m.entry.config.RPC.SendRawTransaction(c, raw)
	})
}
func (m *managedEndpoint) TransactionByHash(ctx context.Context, hash common.Hash) (*types.Transaction, error) {
	return managedCall(m, ctx, "eth_getTransactionByHash", func(c context.Context) (*types.Transaction, error) {
		return m.entry.config.RPC.TransactionByHash(c, hash)
	})
}
func (m *managedEndpoint) ReceiptByHash(ctx context.Context, hash common.Hash) (*ReceiptEvidence, error) {
	return managedCall(m, ctx, "eth_getTransactionReceipt", func(c context.Context) (*ReceiptEvidence, error) {
		return m.entry.config.RPC.ReceiptByHash(c, hash)
	})
}
func (m *managedEndpoint) LatestNonce(ctx context.Context, address common.Address) (uint64, error) {
	return managedCall(m, ctx, "eth_getTransactionCount_latest", func(c context.Context) (uint64, error) {
		return m.entry.config.RPC.LatestNonce(c, address)
	})
}
func (m *managedEndpoint) PendingNonce(ctx context.Context, address common.Address) (uint64, error) {
	return managedCall(m, ctx, "eth_getTransactionCount_pending", func(c context.Context) (uint64, error) {
		return m.entry.config.RPC.PendingNonce(c, address)
	})
}
func (m *managedEndpoint) BlockByHash(ctx context.Context, hash common.Hash) (*BlockEvidence, error) {
	return managedCall(m, ctx, "eth_getBlockByHash", func(c context.Context) (*BlockEvidence, error) {
		return m.entry.config.RPC.BlockByHash(c, hash)
	})
}
func (m *managedEndpoint) BlockByNumber(ctx context.Context, number uint64) (*BlockEvidence, error) {
	return managedCall(m, ctx, "eth_getBlockByNumber", func(c context.Context) (*BlockEvidence, error) {
		return m.entry.config.RPC.BlockByNumber(c, number)
	})
}
func (m *managedEndpoint) LatestBlock(ctx context.Context) (*BlockEvidence, error) {
	return managedCall(m, ctx, "eth_getBlockByNumber", m.entry.config.RPC.LatestBlock)
}
