package orchestrator

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sort"
	"strings"
)

type readinessView struct {
	Ready   bool     `json:"ready"`
	Reasons []string `json:"reasons"`
}

func (a *HTTPAPI) readiness(ctx context.Context) readinessView {
	view := readinessView{Ready: true, Reasons: []string{}}
	add := func(reason string) {
		for _, existing := range view.Reasons {
			if existing == reason {
				return
			}
		}
		view.Ready = false
		view.Reasons = append(view.Reasons, reason)
	}
	if err := a.cfg.Store.db.Ping(ctx); err != nil {
		add("database_unavailable")
		return view
	}
	if !a.cfg.RuntimeReady() {
		add("runtime_not_started")
	}
	chains := make(map[string]bool)
	for _, key := range a.cfg.Accounts {
		chains[key.ChainID] = true
	}
	for chain := range chains {
		if len(a.cfg.Endpoints.Eligible(chain, EndpointRead|EndpointBroadcast|EndpointNonce)) == 0 {
			wrong := false
			for _, ep := range a.cfg.Endpoints.Status() {
				if ep.ChainID == chain && (ep.Reason == "WRONG_CHAIN" || ep.Reason == "WRONG_ANCHOR") {
					wrong = true
				}
			}
			if wrong {
				add("wrong_chain_rpc")
			} else {
				add("no_eligible_rpc")
			}
		}
		readyAccounts := 0
		deep := false
		for _, key := range a.cfg.Accounts {
			if key.ChainID != chain {
				continue
			}
			account, err := a.cfg.Store.AccountStatus(ctx, key)
			if err != nil {
				add("database_unavailable")
				continue
			}
			if account.PausedReason == "DEEP_REORG_BLOCKED" {
				deep = true
			}
			if account.State == AccountReady && account.PausedReason == "" && account.SignerStatus == "AVAILABLE" {
				readyAccounts++
			}
		}
		if deep {
			add("manual_reorg_intervention")
		}
		if readyAccounts == 0 {
			add("no_ready_account")
		}
	}
	sort.Strings(view.Reasons)
	return view
}

func (a *HTTPAPI) live(w http.ResponseWriter, r *http.Request) {
	a.writeJSON(w, http.StatusOK, map[string]string{"status": "live"})
}

func (a *HTTPAPI) ready(w http.ResponseWriter, r *http.Request) {
	view := a.readiness(r.Context())
	status := http.StatusOK
	if !view.Ready {
		status = http.StatusServiceUnavailable
	}
	a.writeJSON(w, status, view)
}

func (a *HTTPAPI) status(w http.ResponseWriter, r *http.Request) {
	if _, ok := a.tenant(r); !ok {
		a.writeError(w, requestID(r.Context()), http.StatusUnauthorized, "UNAUTHORIZED", "Authentication required")
		return
	}
	state, err := a.cfg.Runtime.Status(r.Context())
	if err != nil {
		a.failure(w, r, err)
		return
	}
	ready := a.readiness(r.Context())
	name := "healthy"
	if !ready.Ready {
		name = "degraded"
	}
	type chainView struct {
		ChainStatus
		Ready bool `json:"ready"`
	}
	chains := make([]chainView, 0, len(state.Chains))
	for _, chain := range state.Chains {
		item := chainView{ChainStatus: chain, Ready: chain.EligibleRPCCount > 0}
		var configured, available int
		for _, key := range a.cfg.Accounts {
			if key.ChainID != chain.ChainID {
				continue
			}
			configured++
			account, err := a.cfg.Store.AccountStatus(r.Context(), key)
			if err == nil && account.State == AccountReady && account.PausedReason == "" && account.SignerStatus == "AVAILABLE" {
				available++
			}
		}
		item.Ready = item.Ready && configured > 0 && available > 0
		chains = append(chains, item)
	}
	a.writeJSON(w, http.StatusOK, struct {
		Status  string            `json:"status"`
		Ready   readinessView     `json:"readiness"`
		Chains  []chainView       `json:"chains"`
		Runtime RuntimeStatus     `json:"runtime"`
		Build   map[string]string `json:"build"`
	}{name, ready, chains, state, map[string]string{
		"version": a.cfg.Version, "source_identity": a.cfg.SourceIdentity,
		"go_version": runtime.Version(),
	}})
}

func metricLine(b *strings.Builder, name, labels string, value any) {
	if labels != "" {
		name += "{" + labels + "}"
	}
	fmt.Fprintf(b, "%s %v\n", name, value)
}

func fixedReason(reason string) string {
	switch reason {
	case "STUCK_PENDING", "FEE_REJECTION_RETRY", "FEE_CEILING_REACHED", "REPLACEMENT_LIMIT", "MIN_INTERVAL", "REORG_RECOVERY":
		return reason
	default:
		return "OTHER"
	}
}

func fixedReceiptEvent(event string) string {
	switch event {
	case "RECEIPT_OBSERVED", "CONFIRMATION_PROGRESS", "RECEIPT_REORGED",
		"RECEIPT_NOT_OBSERVED", "RECEIPT_EVIDENCE_UNAVAILABLE", "DEEP_REORG_BLOCKED":
		return event
	default:
		return "OTHER"
	}
}

// Metrics are projected from durable ledgers or bounded in-memory endpoint
// counters. Only configured endpoint IDs and fixed result classes are labels.
func (a *HTTPAPI) metrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var intents, attempts, pending, unknown, reservations, signerErrors int64
	err := a.cfg.Store.db.QueryRow(ctx, `SELECT
		(SELECT count(*) FROM transaction_intents),
		(SELECT count(*) FROM transaction_attempts WHERE raw_tx IS NOT NULL),
		(SELECT count(*) FROM transaction_intents WHERE state='PENDING'),
		(SELECT count(*) FROM transaction_intents WHERE state='UNCERTAIN'),
		(SELECT count(*) FROM nonce_reservations),
		(SELECT count(*) FROM audit_events WHERE reason_code LIKE 'SIGNER_%')`).Scan(
		&intents, &attempts, &pending, &unknown, &reservations, &signerErrors)
	if err != nil {
		a.failure(w, r, err)
		return
	}
	backlog, err := a.cfg.Runtime.Status(ctx)
	if err != nil {
		a.failure(w, r, err)
		return
	}
	replacements, err := a.cfg.Store.ReplacementMetrics(ctx)
	if err != nil {
		a.failure(w, r, err)
		return
	}
	confirmations, err := a.cfg.Store.ConfirmationMetrics(ctx)
	if err != nil {
		a.failure(w, r, err)
		return
	}
	histogram, err := a.cfg.Store.ConfirmationHistogram(ctx)
	if err != nil {
		a.failure(w, r, err)
		return
	}
	a.mu.Lock()
	replayed := a.replayed
	a.mu.Unlock()
	var b strings.Builder
	metricLine(&b, "txorch_intents_total", `result="created",state="CREATED"`, intents)
	metricLine(&b, "txorch_intents_total", `result="replayed",state="EXISTING"`, replayed)
	metricLine(&b, "txorch_attempts_total", `result="signed"`, attempts)
	metricLine(&b, "txorch_pending", "", pending)
	metricLine(&b, "txorch_unknown_outcomes", "", unknown)
	metricLine(&b, "txorch_nonce_reservations", "", reservations)
	metricLine(&b, "txorch_signer_errors_total", "", signerErrors)
	replacementCounts := make(map[string]int64)
	blockedCounts := make(map[string]int64)
	for _, item := range replacements {
		switch item.Name {
		case "txorch_replacements_total":
			replacementCounts[fixedReason(item.Reason)] += item.Count
		case "txorch_replacement_blocked_total":
			blockedCounts[fixedReason(item.Reason)] += item.Count
		}
	}
	if len(replacementCounts) == 0 {
		replacementCounts["STUCK_PENDING"] = 0
	}
	for _, group := range []struct {
		name   string
		counts map[string]int64
	}{{"txorch_replacements_total", replacementCounts}, {"txorch_replacement_blocked_total", blockedCounts}} {
		reasons := make([]string, 0, len(group.counts))
		for reason := range group.counts {
			reasons = append(reasons, reason)
		}
		sort.Strings(reasons)
		for _, reason := range reasons {
			metricLine(&b, group.name, `reason="`+reason+`"`, group.counts[reason])
		}
	}
	reorgCount := int64(0)
	confirmedCount := int64(0)
	confirmingCount := int64(0)
	receiptCounts := make(map[string]int64)
	for _, item := range confirmations {
		switch item.Name {
		case "txorch_reorged_receipts_total":
			reorgCount += item.Count
		case "txorch_confirmed_total":
			confirmedCount += item.Count
		case "txorch_confirming":
			confirmingCount += item.Count
		case "txorch_receipt_observations_total":
			receiptCounts[fixedReceiptEvent(item.Result)] += item.Count
		}
	}
	metricLine(&b, "txorch_reorged_receipts_total", "", reorgCount)
	metricLine(&b, "txorch_confirmed_total", "", confirmedCount)
	metricLine(&b, "txorch_confirming", "", confirmingCount)
	receiptEvents := make([]string, 0, len(receiptCounts))
	for event := range receiptCounts {
		receiptEvents = append(receiptEvents, event)
	}
	sort.Strings(receiptEvents)
	for _, event := range receiptEvents {
		metricLine(&b, "txorch_receipt_observations_total", `result="`+event+`"`, receiptCounts[event])
	}
	for _, bucket := range histogram.Buckets {
		metricLine(&b, "txorch_confirmation_seconds_bucket", fmt.Sprintf(`le="%g"`, bucket.UpperBoundSeconds), bucket.Count)
	}
	metricLine(&b, "txorch_confirmation_seconds_bucket", `le="+Inf"`, histogram.Count)
	metricLine(&b, "txorch_confirmation_seconds_sum", "", histogram.SumSeconds)
	metricLine(&b, "txorch_confirmation_seconds_count", "", histogram.Count)
	for _, ep := range a.cfg.Endpoints.Status() {
		eligible := 0
		if ep.State == EndpointHealthy {
			eligible = 1
		}
		metricLine(&b, "txorch_rpc_eligible", `endpoint="`+ep.ID+`"`, eligible)
	}
	latencies := make(map[string]float64)
	for _, m := range a.cfg.Endpoints.Metrics() {
		labels := fmt.Sprintf(`endpoint="%s",method="%s",status="%s"`, m.Endpoint, m.Method, m.Status)
		metricLine(&b, "txorch_rpc_requests_total", labels, m.Count)
		latencies[fmt.Sprintf(`endpoint="%s",method="%s"`, m.Endpoint, m.Method)] += m.Latency.Seconds()
	}
	latencyLabels := make([]string, 0, len(latencies))
	for labels := range latencies {
		latencyLabels = append(latencyLabels, labels)
	}
	sort.Strings(latencyLabels)
	for _, labels := range latencyLabels {
		metricLine(&b, "txorch_rpc_latency_seconds", labels, latencies[labels])
	}
	for reason, count := range a.cfg.Endpoints.Failovers() {
		metricLine(&b, "txorch_rpc_failovers_total", `reason="`+reason+`"`, count)
	}
	for reason, count := range backlog.BlockedReasons {
		metricLine(&b, "txorch_accounts_blocked", `reason="`+reason+`"`, count)
	}
	for state, count := range backlog.ReconciliationCounts {
		metricLine(&b, "txorch_account_reconciliation_total", `result="`+string(state)+`"`, count)
	}
	for state, count := range backlog.Backlog {
		metricLine(&b, "txorch_runtime_backlog", `kind="`+string(state)+`"`, count)
	}
	metricLine(&b, "txorch_runtime_workers", `kind="active"`, backlog.ActiveWorkers)
	metricLine(&b, "txorch_runtime_workers", `kind="queued"`, backlog.Queued)
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, b.String())
}
