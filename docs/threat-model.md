# Threat model

## Boundary and assets

Assets are signer keys, transaction authorization, funds controlled by configured signers, nonce ownership, signed raw bytes, PostgreSQL state and backups, and RPC configuration. Actors include legitimate and compromised API clients, malicious request senders, faulty or malicious RPC endpoints, compromised signer infrastructure, and operators. The authenticated tenant and policy service authorize a bounded intent; the orchestrator constructs exact type-2 bytes; the signer signs only that constructed transaction; PostgreSQL preserves intent and nonce history; RPC endpoints provide untrusted observations and submission transport.

Narrow Go boundary:

```go
type TransactionSigner interface {
    Address(ctx context.Context) (common.Address, error)
    SignTransaction(ctx context.Context, tx *types.Transaction) (*types.Transaction, error)
}
```

Business logic never reads seed phrases or arbitrary key paths. The development adapter owns an explicit test key and copies it on construction; caller input cannot choose that key. The expected account address is checked before signing. The returned sender, chain ID, nonce, destination, value, calldata, gas, access list and fee limits are checked afterward. Signer unavailability fails closed. Production KMS/HSM integration and key provisioning are out of scope. The broadcast boundary submits only validated persisted raw bytes; anyone with a copy could also submit them.

| Threat | Control | Residual risk |
| --- | --- | --- |
| Arbitrary signing or transaction substitution | Signer accepts a constructed transaction; immutable intent, account check, post-sign decode and comparison | Static local token boundary is not a production authorization system; compromised policy or signer infrastructure |
| Duplicate business operation or replayed API key | Tenant/key uniqueness, canonical digest, stable intent ID | Business caller using distinct keys for same action; caller must supply stable keys |
| Nonce collision/theft/race | Exclusive account ownership, row lock, unique reservation, pause on external consumption | Key used outside service or malicious signer |
| Destination/calldata/value mutation | Immutable intent and attempt triggers, policy snapshot, signed attempt verification | Privileged DB administrator or compromised process |
| Fee exhaustion/infinite replacement | Per-intent fee caps and worst-case gas budget, count and interval limits; quotas remain a deployment control | Chain fee volatility or operator misconfiguration |
| Replay of raw signed transaction | Chain ID binding; exact-byte replay is idempotent per canonical nonce; restricted storage | Reorg or cross-fork behavior, raw-byte exfiltration and mempool spam |
| Secret leakage | No keys/raw bytes in ordinary API or errors, development key confined to signer, restricted DB roles and backups | Raw signed bytes are stored without application-level encryption; operational access compromise and sensitive calldata |
| RPC equivocation/stale provider | Chain ID, optional genesis anchor, read capability, multiple hash/receipt lookups, fail-closed mismatch handling | Canonical receipts and head disagreement are checked; correlated provider dishonesty, wall-clock head freshness, censorship and unsupported deep reorg remain residual risks |
| Request flood | Static token scope, bounded request body/calldata/key, runtime backlog admission and server timeouts | No per-tenant rate limiter; resource exhaustion remains possible before admission |
| Crash/DB inconsistency | Short signing/broadcast transactions, audit, fenced leases, exact-byte reload and hash validation before send | Storage loss, split brain against a second database authority |

Operator actions that unpause accounts or change fee policy require separate authenticated roles and audit. No such mutation endpoint exists in v1's public API. Endpoint URLs and credentials are secret configuration; no public RPC credentials appear in this repository. A source compromise or privileged DB access can defeat application controls and needs infrastructure controls and recovery drills.

A replacement keeps the original call and owned nonce. PostgreSQL lineage constraints reject changed gas, nonce or policy snapshot, while signer validation checks chain, recipient, value and calldata against the durable intent. The receipt check before a replacement send narrows the original-mined race; an unavailable or equivocal RPC can still hide inclusion. Pool admission differs by client, and a rejected fee bump is recorded without assuming that the original disappeared.

Receipt checks reject zero/wrong hashes, wrong block numbers, impossible indices and inconsistent execution status. A canonical block body must contain the attempt hash at the receipt index. The service records only the configured RPC canonical view; it does not verify consensus or receipt proofs. A stale head below durable verified evidence cannot reduce confirmation state. Conflicting eligible canonical views hold progress without voting. Shallow invalidation revokes the winner atomically; an unsupported depth pauses the account and fails the readiness projection. A missing receipt alone cannot prove reorg. Premature confirmation is prevented by explicit `head - receipt_block + 1` arithmetic and persisted per-intent depth.

## Operational runtime threats

| Threat | Control | Residual risk |
| --- | --- | --- |
| Wrong-chain or wrong-anchor endpoint | Identity and anchor probe makes it ineligible before broadcast | Misconfigured anchor omission or compromised endpoint lying consistently |
| Stale head or omitted transaction visibility | Head-lag exclusion, exact-hash fallback, durable receipt history; null lookup never proves absence | Correlated stale providers, private mempools, censorship |
| Conflicting nonce views or external signer transaction | Compare eligible latest/pending views and owned confirmed receipts; pause affected account and preserve attempts | External sender can race immediately after a reconciliation read |
| Endpoint failure storm | Per-endpoint cooldown, bounded global/endpoint inflight calls, deadlines | Shared upstream outage stalls progress for that chain |
| Worker starvation or unbounded retry | Fixed worker pool, keyset work pages, durable capped backoff | A large account set may delay a later key until subsequent cycles |
| Shutdown during send or reconciliation | Context cancellation, fenced claims and account token, restart from durable state | A sent transaction may remain `UNCERTAIN` until exact-hash lookup |

Multi-RPC improves availability and visibility. It cannot establish global mempool absence or Byzantine canonical truth. Endpoint health is an operational signal, never a consensus vote. See [ADR 0007](adr/0007-rpc-runtime.md) for the nonce evidence and startup rules.

## HTTP and evidence boundary

| Threat | Control | Residual risk |
| --- | --- | --- |
| Oversized payload or idempotency abuse | Bounded HTTP body, calldata, key and request deadline; tenant-scoped unique digest | Many distinct valid keys can still consume work; external quotas are required |
| API-induced work amplification | Refuse new work without a ready account, eligible RPC or backlog headroom; committed retries remain readable | A configured threshold is coarse and does not replace admission quotas |
| Status or metrics exposure | Tenant token for status; status aggregates accounts; fixed metric labels; deployment must restrict metrics network access | Local metrics endpoint is unauthenticated by design |
| Log or trace secret leakage | No raw signed bytes/private keys/RPC URLs in ordinary fields or span attributes; bounded request ID | Application logs can contain operational identifiers; protect retention and access |
| Benchmark misuse | Correctness checksum precedes throughput, workload and machine fields are recorded | Local timings cannot certify production capacity |
| Demo fault fixture escape | Ephemeral loopback Anvil and proxy; explicit development mode and loopback RPC restriction | Operators must not expose development keys or local API beyond trusted hosts |
