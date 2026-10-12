# API and operational signals

The HTTP API implements the [OpenAPI contract](../api/openapi.yaml). A configured bearer token identifies a tenant. A POST with the same tenant/key/canonical payload returns the same intent with 200; a new intent returns 201; a changed payload returns 409. A committed replay can be read during signer or RPC degradation without admitting new work. GET reads intent, attempts, broadcasts and receipts from one read-only repeatable-read snapshot and omits raw signed bytes. New admissions require a ready account, eligible RPC and bounded runtime backlog. The body is limited to 64 KiB in the included binary, calldata to 16 KiB and the idempotency key to 255 bytes. Request handling uses a 15-second context deadline; server header/read/write/idle timeouts are 3/15/20/30 seconds, with a 10-second shutdown deadline.

/health/live reports HTTP process liveness independent of RPC. /health/ready checks PostgreSQL, runtime startup, eligible chain RPC and at least one ready configured account per chain. It returns stable machine-readable reasons such as database_unavailable, wrong_chain_rpc, no_eligible_rpc, runtime_not_started, no_ready_account and manual_reorg_intervention. An unrelated blocked account does not fail a chain with another ready account. /v1/status is authenticated and aggregates worker, backlog, chain, endpoint and account state plus version, source identity and Go version; it does not list arbitrary account identities.

`make build` and the Phase 7 Docker gate embed the current source-tree checksum into the binary. A direct build without this linker value reports `unavailable`. The checksum is computed by `scripts/source_identity.py`; it is not a Git commit or a release provenance claim.

| Metric | Meaning |
| --- | --- |
| txorch_intents_total{result,state} | Durable intent rows created; in-memory idempotent HTTP replay count. state=CREATED denotes creation event, not current state. |
| txorch_attempts_total{result} | Persisted signed attempts. |
| txorch_pending | Current intents exactly in PENDING; excludes READY, UNCERTAIN and CONFIRMING. |
| txorch_unknown_outcomes | Current intents exactly in UNCERTAIN. |
| txorch_replacements_total{reason} | Durable replacement events by fixed reason class. |
| txorch_replacement_blocked_total{reason} | Durable replacement blocks by fixed reason class. |
| txorch_confirmation_seconds | Fixed-bucket histogram of durable confirmation durations. |
| txorch_nonce_reservations | Durable reservation row count. |
| txorch_rpc_requests_total{endpoint,method,status} | In-memory RPC pool operation count. |
| txorch_rpc_latency_seconds{endpoint,method} | Cumulative observed RPC duration in seconds for the pool lifetime. |
| txorch_signer_errors_total | Durable signer-related audit events. |
| txorch_reorged_receipts_total | Durable receipt reorg events. |
| txorch_confirmed_total, txorch_confirming | Durable confirmation transitions and current confirming intents. |
| txorch_receipt_observations_total{result} | Durable receipt poll events by fixed result class. |
| txorch_rpc_eligible, txorch_rpc_failovers_total, txorch_accounts_blocked, txorch_account_reconciliation_total, txorch_runtime_backlog, txorch_runtime_workers | Current endpoint, account and worker operations. |

Metric labels use fixed classes and configured endpoint IDs. Intent IDs, request IDs, hashes, nonce, signer and destination never appear as labels. /metrics is unauthenticated for a local scraper; restrict it at the deployment network boundary. Counts derived from PostgreSQL survive process restart; in-memory HTTP replay and RPC counts reset. No remote metrics backend is required for request handling.

Structured JSON logs cover HTTP admission, reservation and signing and runtime durable transitions. INFO is reserved for state changes, WARN for recoverable degradation and ERROR for failures. The bounded request ID appears in the response header and log context. OpenTelemetry spans cover HTTP, DB admission, signer, RPC, broadcast, replacement, account and receipt reconciliation. The optional local stdout exporter is enabled with TXORCH_TRACE_STDOUT=1. Span attributes omit private keys, signed raw bytes and RPC URLs. Sensitive identifiers are reserved for controlled logs/traces and the tenant-scoped API rather than metrics labels.

Unexpected error values are logged by Go type rather than rendering their raw message, since transport errors can contain credential-bearing URLs. HTTP responses use a fixed code, message and request ID. `TestPhase7ErrorLogRedaction` injects a credential-bearing error and checks both log and response output.
