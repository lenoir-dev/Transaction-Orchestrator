package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type RuntimeConfig struct {
	Workers                int
	BatchSize              int
	StartupAccountBatch    int
	PollInterval           time.Duration
	WorkTimeout            time.Duration
	AccountRPCDeadline     time.Duration
	AccountRefreshInterval time.Duration
	RetryBase              time.Duration
	RetryMax               time.Duration
	Broadcast              BroadcastPolicy
	Receipt                ReceiptPolicy
	Replacement            ReplacementOptions
	Now                    func() time.Time
	Ticks                  <-chan time.Time
	Logger                 *slog.Logger
}

type RuntimeStatus struct {
	ActiveWorkers        int64                                 `json:"active_workers"`
	Queued               int64                                 `json:"queued"`
	Backlog              map[State]int64                       `json:"backlog"`
	Endpoints            []EndpointStatus                      `json:"endpoints"`
	Chains               []ChainStatus                         `json:"chains"`
	AccountStates        map[AccountReconciliationState]int64  `json:"account_states"`
	BlockedReasons       map[string]int64                      `json:"blocked_reasons"`
	ReconciliationCounts map[AccountReconciliationState]uint64 `json:"reconciliation_counts"`
}

type ChainStatus struct {
	ChainID           string `json:"chain_id"`
	EligibleRPCCount  int    `json:"eligible_rpc_count"`
	Head              uint64 `json:"head"`
	StaleEndpoints    int    `json:"stale_endpoints"`
	DegradedEndpoints int    `json:"degraded_endpoints"`
}

type Runtime struct {
	store                *Store
	endpoints            *EndpointPool
	config               RuntimeConfig
	signer               func(AccountKey) TransactionSigner
	active               atomic.Int64
	queued               atomic.Int64
	mu                   sync.Mutex
	reconciliationCounts map[AccountReconciliationState]uint64
	intentCursor         string
	accountCursor        AccountKey
	accountStartupDone   bool
	startedAt            time.Time
}

func NewRuntime(store *Store, endpoints *EndpointPool, cfg RuntimeConfig,
	signer func(AccountKey) TransactionSigner) (*Runtime, error) {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	cfg.Receipt.Now = cfg.Now()
	cfg.Replacement.Now = cfg.Now()
	if store == nil || store.db == nil || endpoints == nil || cfg.Workers < 1 || cfg.Workers > 64 ||
		cfg.BatchSize < 1 || cfg.BatchSize > 1000 || cfg.StartupAccountBatch < 1 ||
		cfg.StartupAccountBatch > 100 || cfg.PollInterval < 10*time.Millisecond ||
		cfg.PollInterval > time.Minute || cfg.WorkTimeout <= 0 ||
		cfg.WorkTimeout > 5*time.Minute || cfg.AccountRPCDeadline <= 0 ||
		cfg.AccountRPCDeadline > cfg.WorkTimeout || !cfg.Broadcast.valid() ||
		cfg.AccountRefreshInterval < 100*time.Millisecond || cfg.AccountRefreshInterval > 24*time.Hour ||
		cfg.RetryBase < 10*time.Millisecond || cfg.RetryMax < cfg.RetryBase ||
		cfg.RetryMax > time.Hour ||
		!cfg.Receipt.valid() || !cfg.Replacement.valid() ||
		cfg.Workers > int(store.db.Config().MaxConns)-2 {
		return nil, ErrInvalid
	}
	return &Runtime{store: store, endpoints: endpoints, config: cfg,
		signer: signer, startedAt: time.Now(),
		reconciliationCounts: make(map[AccountReconciliationState]uint64)}, nil
}

// RunOnce has a bounded queue and joins every worker before returning. Its
// cursors make a repeatedly failing intent unable to monopolize the first page.
func (r *Runtime) RunOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.endpoints.Refresh(ctx); err != nil {
		return err
	}
	if err := r.reconcileAccounts(ctx); err != nil {
		return err
	}
	r.mu.Lock()
	cursor := r.intentCursor
	r.mu.Unlock()
	work, err := r.store.LifecycleWorkAfter(ctx, r.config.Now(), cursor, r.config.BatchSize)
	if err != nil {
		return err
	}
	if len(work) == 0 && cursor != "" {
		work, err = r.store.LifecycleWorkAfter(ctx, r.config.Now(), "", r.config.BatchSize)
		if err != nil {
			return err
		}
	}
	if len(work) != 0 {
		r.mu.Lock()
		r.intentCursor = work[len(work)-1]
		r.mu.Unlock()
		jobs := make(chan string, r.config.BatchSize)
		failures := make(chan error, r.config.BatchSize)
		var wg sync.WaitGroup
		for i := 0; i < r.config.Workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for id := range jobs {
					r.queued.Add(-1)
					if ctx.Err() != nil {
						continue
					}
					r.active.Add(1)
					workCtx, cancel := context.WithTimeout(ctx, r.config.WorkTimeout)
					before, _ := r.store.GetIntent(workCtx, id)
					outcome := r.processIntent(workCtx, id)
					if r.config.Logger != nil {
						after, readErr := r.store.GetIntent(workCtx, id)
						if readErr == nil && after.State != before.State {
							level := slog.LevelInfo
							if after.State == Blocked {
								level = slog.LevelWarn
							}
							r.config.Logger.Log(workCtx, level, "intent state changed",
								"intent_id", id, "from", before.State, "to", after.State,
								"reason", after.Reason)
						}
					}
					cancel()
					if err := r.store.RecordRuntimeOutcome(ctx, id, r.config.Now(), outcome,
						r.config.RetryBase, r.config.RetryMax); err != nil {
						failures <- err
					}
					r.active.Add(-1)
				}
			}()
		}
		for _, id := range work {
			r.queued.Add(1)
			select {
			case jobs <- id:
			case <-ctx.Done():
				r.queued.Add(-1)
				close(jobs)
				wg.Wait()
				return ctx.Err()
			}
		}
		close(jobs)
		wg.Wait()
		close(failures)
		for err := range failures {
			return err
		}
	}
	return ctx.Err()
}

func (r *Runtime) Run(ctx context.Context) error {
	var ticker *time.Ticker
	ticks := r.config.Ticks
	if ticks == nil {
		ticker = time.NewTicker(r.config.PollInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		if err := r.RunOnce(ctx); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case _, ok := <-ticks:
			if !ok {
				return nil
			}
		}
	}
}

func (r *Runtime) processIntent(ctx context.Context, id string) error {
	intent, err := r.store.GetIntent(ctx, id)
	if err != nil {
		return err
	}
	account, err := r.store.AccountStatus(ctx, AccountKey{intent.ChainID, intent.SignerID})
	if err != nil {
		return err
	}
	reads := r.endpoints.Eligible(intent.ChainID, EndpointRead|EndpointNonce)
	sends := r.endpoints.Eligible(intent.ChainID, EndpointRead|EndpointBroadcast|EndpointNonce)
	switch intent.State {
	case Ready, Uncertain:
		if account.State != AccountReady || account.PausedReason != "" ||
			account.ReconciledAt.Before(r.startedAt) {
			if intent.State == Uncertain && len(reads) != 0 {
				policy := r.config.Receipt
				policy.Now = r.config.Now()
				_, _ = r.store.ReconcileReceipts(ctx, id, reads, policy)
			}
			return ErrAccountPaused
		}
		if len(sends) == 0 {
			return ErrNoEligibleEndpoint
		}
		result, advanceErr := r.store.AdvanceBroadcast(ctx, id, sends, r.config.Broadcast)
		err = advanceErr
		if result.LastOutcome == ResultTransport {
			r.endpoints.RecordFailover("timeout")
		}
		if result.LastOutcome == ResultProvider {
			r.endpoints.RecordFailover("unavailable")
		}
		return err
	case Signing:
		// An in-progress replacement claim can be resumed from the stored fee
		// and attempt snapshot. Initial signing remains owned by its caller.
		if len(reads) == 0 {
			return ErrNoEligibleEndpoint
		}
		status, position, e := r.store.replacementPosition(ctx, id)
		if e != nil {
			return e
		}
		if status == Signing && position > 1 {
			return r.tryReplacement(ctx, intent, reads)
		}
		return nil
	case Pending, Mined, Confirming, Confirmed, Reorged:
		if len(reads) == 0 {
			return ErrNoEligibleEndpoint
		}
		policy := r.config.Receipt
		policy.Now = r.config.Now()
		status, e := r.store.ReconcileReceipts(ctx, id, reads, policy)
		if e != nil && !errors.Is(e, ErrReceiptBackoff) && !errors.Is(e, ErrEvidenceUnavailable) {
			return e
		}
		if status.State == Pending && account.State == AccountReady && account.PausedReason == "" &&
			!account.ReconciledAt.Before(r.startedAt) {
			return r.tryReplacement(ctx, intent, reads)
		}
		return nil
	default:
		return nil
	}
}

func (r *Runtime) tryReplacement(ctx context.Context, intent Intent, reads []BroadcastEndpoint) error {
	account, err := r.store.AccountStatus(ctx, AccountKey{intent.ChainID, intent.SignerID})
	if err != nil {
		return err
	}
	if account.State != AccountReady || account.PausedReason != "" || account.ReconciledAt.Before(r.startedAt) {
		return ErrAccountPaused
	}
	if r.signer == nil {
		return nil
	}
	signer := r.signer(AccountKey{ChainID: intent.ChainID, SignerID: intent.SignerID})
	if signer == nil {
		return nil
	}
	opts := r.config.Replacement
	opts.Now = r.config.Now()
	_, err = r.store.SignReplacement(ctx, intent.ID, signer, reads, opts)
	var signingErr *SigningError
	if errors.As(err, &signingErr) {
		_ = r.store.SetSignerHealth(ctx, AccountKey{intent.ChainID, intent.SignerID}, false, "SIGNER_UNAVAILABLE")
	}
	if errors.Is(err, ErrReplacementTooEarly) || errors.Is(err, ErrReplacementLimit) ||
		errors.Is(err, ErrReceiptObserved) || errors.Is(err, ErrTransition) ||
		errors.Is(err, ErrEvidenceUnavailable) || errors.Is(err, ErrAccountPaused) {
		return nil
	}
	return err
}

func (r *Runtime) reconcileAccounts(ctx context.Context) error {
	r.mu.Lock()
	cursor := r.accountCursor
	startup := !r.accountStartupDone
	r.mu.Unlock()
	var keys []AccountKey
	var err error
	if startup {
		keys, err = r.store.AccountKeys(ctx, cursor, r.config.StartupAccountBatch)
	} else {
		keys, err = r.store.AccountKeysDue(ctx, cursor, r.config.Now(), r.config.StartupAccountBatch)
	}
	if err != nil {
		return err
	}
	if len(keys) == 0 && startup {
		r.mu.Lock()
		r.accountStartupDone = true
		r.accountCursor = AccountKey{}
		r.mu.Unlock()
		keys, err = r.store.AccountKeysDue(ctx, AccountKey{}, r.config.Now(), r.config.StartupAccountBatch)
		if err != nil {
			return err
		}
	} else if len(keys) == 0 && cursor.ChainID != "" {
		keys, err = r.store.AccountKeysDue(ctx, AccountKey{}, r.config.Now(), r.config.StartupAccountBatch)
		if err != nil {
			return err
		}
	}
	for _, key := range keys {
		if err := ctx.Err(); err != nil {
			return err
		}
		// A bad account is contained to its own durable status. Database and
		// cancellation errors still stop this scheduling cycle.
		status, reconcileErr := r.store.ReconcileAccount(ctx, key, r.endpoints, r.config.AccountRPCDeadline)
		err = reconcileErr
		if err != nil {
			return err
		}
		r.mu.Lock()
		r.reconciliationCounts[status.State]++
		r.mu.Unlock()
		if r.signer != nil {
			available, reason := false, "SIGNER_UNAVAILABLE"
			if signer := r.signer(key); signer != nil {
				check, cancel := context.WithTimeout(ctx, r.config.Replacement.Signing.SignerTimeout)
				address, addressErr := signer.Address(check)
				cancel()
				if addressErr == nil && address == status.Address {
					available, reason = true, ""
				} else if addressErr == nil && address != (common.Address{}) {
					reason = "SIGNER_IDENTITY_MISMATCH"
				}
			}
			if err := r.store.SetSignerHealth(ctx, key, available, reason); err != nil {
				return err
			}
		}
		if err := r.store.ScheduleAccountCheck(ctx, key,
			r.config.Now().Add(r.config.AccountRefreshInterval)); err != nil {
			return err
		}
		r.mu.Lock()
		r.accountCursor = key
		r.mu.Unlock()
	}
	return nil
}

func (r *Runtime) Status(ctx context.Context) (RuntimeStatus, error) {
	backlog, err := r.store.LifecycleCounts(ctx, r.config.Now())
	if err != nil {
		return RuntimeStatus{}, err
	}
	states, err := r.store.AccountStateCounts(ctx)
	if err != nil {
		return RuntimeStatus{}, err
	}
	blocked, err := r.store.AccountBlockedCounts(ctx)
	if err != nil {
		return RuntimeStatus{}, err
	}
	endpoints := r.endpoints.Status()
	chainMap := make(map[string]*ChainStatus)
	for _, ep := range endpoints {
		chain := chainMap[ep.ChainID]
		if chain == nil {
			chain = &ChainStatus{ChainID: ep.ChainID}
			chainMap[ep.ChainID] = chain
		}
		if ep.State == EndpointHealthy {
			chain.EligibleRPCCount++
			if ep.Head > chain.Head {
				chain.Head = ep.Head
			}
		} else if ep.State == EndpointDegraded {
			chain.DegradedEndpoints++
			if ep.Reason == "STALE_HEAD" {
				chain.StaleEndpoints++
			}
		}
	}
	chains := make([]ChainStatus, 0, len(chainMap))
	for _, chain := range chainMap {
		chains = append(chains, *chain)
	}
	sort.Slice(chains, func(i, j int) bool { return chains[i].ChainID < chains[j].ChainID })
	r.mu.Lock()
	counts := make(map[AccountReconciliationState]uint64, len(r.reconciliationCounts))
	for state, count := range r.reconciliationCounts {
		counts[state] = count
	}
	r.mu.Unlock()
	return RuntimeStatus{ActiveWorkers: r.active.Load(), Queued: r.queued.Load(),
		Backlog: backlog, Endpoints: endpoints, Chains: chains,
		AccountStates: states, BlockedReasons: blocked, ReconciliationCounts: counts}, nil
}

func (s *Store) LifecycleWorkAfter(ctx context.Context, now time.Time, after string, limit int) ([]string, error) {
	if now.IsZero() || limit < 1 || limit > 1000 {
		return nil, ErrInvalid
	}
	var cursor any
	if after != "" {
		cursor = after
	}
	rows, err := s.db.Query(ctx, `SELECT intent_id::text FROM transaction_intents
		WHERE ($1::uuid IS NULL OR intent_id>$1::uuid)
		AND (runtime_retry_at IS NULL OR runtime_retry_at<=$2)
		AND (state='READY' OR
			(state='UNCERTAIN' AND (next_reconcile_at IS NULL OR next_reconcile_at<=$2)) OR
			(state IN ('SIGNING','PENDING','MINED','CONFIRMING','CONFIRMED','REORGED')
			AND (receipt_next_poll_at IS NULL OR receipt_next_poll_at<=$2)))
		ORDER BY intent_id LIMIT $3`, cursor, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) LifecycleCounts(ctx context.Context, now time.Time) (map[State]int64, error) {
	rows, err := s.db.Query(ctx, `SELECT state,count(*) FROM transaction_intents
		WHERE (runtime_retry_at IS NULL OR runtime_retry_at<=$1)
		AND (state='READY' OR
		(state='UNCERTAIN' AND (next_reconcile_at IS NULL OR next_reconcile_at<=$1)) OR
		(state IN ('SIGNING','PENDING','MINED','CONFIRMING','CONFIRMED','REORGED')
		AND (receipt_next_poll_at IS NULL OR receipt_next_poll_at<=$1)))
		GROUP BY state`, now)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[State]int64)
	for rows.Next() {
		var state State
		var count int64
		if err = rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		out[state] = count
	}
	return out, rows.Err()
}

func runtimeRetryClass(err error) string {
	var signingErr *SigningError
	switch {
	case errors.As(err, &signingErr):
		return "SIGNER_UNAVAILABLE"
	case errors.Is(err, ErrNoEligibleEndpoint), errors.Is(err, ErrEvidenceUnavailable):
		return "RPC_UNAVAILABLE"
	case errors.Is(err, context.DeadlineExceeded):
		return "TIMEOUT"
	case errors.Is(err, ErrAccountPaused):
		return "ACCOUNT_BLOCKED"
	case errors.Is(err, ErrTransition), errors.Is(err, ErrInvalid):
		return "DETERMINISTIC"
	default:
		return "TRANSIENT"
	}
}

// RecordRuntimeOutcome stores only scheduling state. It never changes attempt
// identity or the broadcast/receipt correctness ledger.
func (s *Store) RecordRuntimeOutcome(ctx context.Context, id string, now time.Time,
	outcome error, base, maximum time.Duration) error {
	if now.IsZero() || base <= 0 || maximum < base {
		return ErrInvalid
	}
	if outcome == nil {
		_, err := s.db.Exec(ctx, `UPDATE transaction_intents SET runtime_retry_at=NULL,
			runtime_retry_count=0,runtime_retry_class=NULL WHERE intent_id=$1::uuid`, id)
		return err
	}
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var count int
	err = tx.QueryRow(ctx, `SELECT runtime_retry_count FROM transaction_intents
		WHERE intent_id=$1::uuid FOR UPDATE`, id).Scan(&count)
	if err != nil {
		return err
	}
	if count < 30 {
		count++
	}
	delay := base
	for i := 1; i < count && delay < maximum; i++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}
	_, err = tx.Exec(ctx, `UPDATE transaction_intents SET runtime_retry_count=$2,
		runtime_retry_at=$3,runtime_retry_class=$4 WHERE intent_id=$1::uuid`,
		id, count, now.Add(delay), runtimeRetryClass(outcome))
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) AccountStateCounts(ctx context.Context) (map[AccountReconciliationState]int64, error) {
	rows, err := s.db.Query(ctx, `SELECT reconciliation_status,count(*) FROM signer_accounts
		GROUP BY reconciliation_status`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[AccountReconciliationState]int64)
	for rows.Next() {
		var state AccountReconciliationState
		var count int64
		if err = rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		out[state] = count
	}
	return out, rows.Err()
}

func (s *Store) AccountBlockedCounts(ctx context.Context) (map[string]int64, error) {
	rows, err := s.db.Query(ctx, `SELECT paused_reason,count(*) FROM signer_accounts
		WHERE paused_reason IS NOT NULL GROUP BY paused_reason`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]int64)
	for rows.Next() {
		var reason string
		var count int64
		if err = rows.Scan(&reason, &count); err != nil {
			return nil, err
		}
		switch reason {
		case "EXTERNAL_NONCE_CONFLICT", "NONCE_RECONCILIATION", "SIGNER_UNAVAILABLE",
			"INTENT_BLOCKED", "DEEP_REORG_BLOCKED":
		default:
			reason = "OTHER"
		}
		out[reason] += count
	}
	return out, rows.Err()
}
