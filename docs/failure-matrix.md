# Failure matrix

`Closed` means no new intent or nonce from uncertain evidence; a bounded exact-byte rebroadcast is permitted under ADR 0003. Signing, broadcast, replacement, receipt, confirmation, reorg, crash and local HTTP rows have tests. Deployment-only scenarios remain outside the local gate.

| Failure | Durable state and action | Retry / posture | Evidence test |
| --- | --- | --- | --- |
| DB unavailable before create | No intent acknowledged | Retry same key / closed | DB outage before insert |
| DB failure after intent commit | `CREATED`; caller may miss response | Retry key returns row / closed | Commit then drop API response |
| Signer unavailable | `SIGNING` lease expires, then `BLOCKED` after budget | Bounded retry / closed | Signer connection refusal |
| Signer timeout | `SIGNING`; no signed bytes committed, no send | Retry after lease / closed | Hold signer response |
| Wrong configured signer | `BLOCKED`; account paused, no attempt claimed | Resume after signer correction / closed | Address mismatch test |
| Signer changes call, nonce, chain, gas, or fees | `BLOCKED`; attempt remains `PREPARING` | Operator review / closed | Adversarial signer table |
| Crash after claim before signer call | `SIGNING`, attempt #1 `PREPARING` | Reclaim same candidate / closed | Child process exit at claim |
| Crash during signer call | `SIGNING`, attempt #1 `PREPARING` | Reclaim same candidate / closed | Child exits inside signer |
| Crash after signer result before commit | `SIGNING`, no raw bytes | Re-sign same candidate / closed | Child exits before signed commit |
| Crash after signed commit | `READY`, exact bytes/hash durable | Load bytes without re-sign / closed | Child exits after commit |
| Crash after reservation | `RESERVED`, nonce retained | Resume same intent / closed | Kill after commit |
| Crash after signing before broadcast | `READY`, raw bytes/hash retained | Exact-byte claim/send / closed | Kill after signed commit |
| Crash during broadcast | `UNCERTAIN` fenced lease | Reconcile before exact rebroadcast / closed | Child exits before/after RPC |
| RPC accepts, response lost | `UNCERTAIN` | Query local hash; exact rebroadcast only if unresolved / closed | Proxy forwards to Anvil then drops |
| RPC says already known | `UNCERTAIN` until matching lookup, then `PENDING` | Query hash / closed | Inject error, lookup match |
| All RPC unavailable | `UNCERTAIN` or prior durable state | Bounded read retry; no replacement / closed | Primary unavailable fallback test |
| Stale RPC | Hold state when head is below durable verified head | Eligible fallback / closed | `TestReceiptInvalidStaleAndMultiRPC`, confirmed-state regression |
| Two concurrent intents | Distinct reserved nonces or one blocked | Retry DB conflict / closed | Parallel reservation barrier |
| Duplicate key | One intent; conflicting digest 409 | Return existing / closed | Parallel duplicate POST |
| External tx consumes nonce | `BLOCKED/EXTERNAL_NONCE_CONFLICT` | Operator reconciliation / closed | Latest nonce advances without exact hash fixture |
| Mempool drops attempt | `UNCERTAIN` after visibility loss | Exact rebroadcast / closed | Evict local pool |
| Pending too long | `PENDING` until the configured interval permits a replacement | Bounded fee bump / closed | Delayed mining clock |
| Replacement underpriced | `UNCERTAIN`/`PENDING`, record rejection | Reconcile, bounded policy / closed | Node price-bump rejection |
| Original mines during replacement | `MINED` with original winner | Stop further sends; observe / closed | Race barrier after signing |
| Replacement mines first | `MINED` with replacement winner | Observe all hashes / closed | Mine replacement first |
| Receipt appears | Validate hash/height/body/canonical identity; `MINED`, then `CONFIRMING` or `CONFIRMED` | Scheduled policy checks / closed | Confirmation boundary tests |
| Receipt disappears | Hold winner if old block is still canonical; orphan it only on changed canonical hash | Reconcile all attempts / closed | Null receipt and branch fixtures |
| Restart during uncertainty | `UNCERTAIN` recovered | Hash/receipt queries, same bytes / closed | Reopen pool and child-process crash tests |
| Fee ceiling reached | `BLOCKED/FEE_CEILING_REACHED` | Operator action / closed | Next integer bump exceeds configured cap |
| RPC disagreement | Hold state; return evidence-unavailable with durable next-poll schedule | Recheck eligible providers / closed | Conflicting canonical views |
| Deep reorg after threshold | `BLOCKED/DEEP_REORG_BLOCKED`, account paused, readiness degraded | Operator resync / closed | Depth 5 beyond window 4 |

An underpriced replacement retains its immutable bytes and can move from `BLOCKED/FEE_REJECTION` through a fresh bounded reconciliation and interval to the next attempt. A transport-unknown result first reconciles the same hash; it is never an immediate fee-bump trigger. Receipt evidence for an older attempt can supersede the active attempt and records the older attempt as winner.

## Runtime and account reconciliation

| Failure | Durable state and action | Retry / posture | Evidence test |
| --- | --- | --- | --- |
| One RPC down | Healthy endpoint remains eligible; failed endpoint cools down | Bounded fallback | `TestPhase6EndpointEligibilityAndRecovery` |
| Wrong-chain RPC | Endpoint `INELIGIBLE`; no signed bytes sent there | Reconfigure endpoint | `TestPhase6EndpointEligibilityAndRecovery` |
| Stale head | Endpoint `DEGRADED`; excluded from current-state reads | Probe again on next refresh | `TestPhase6StaleEndpointExclusion` |
| Conflicting account nonce | Account `RECONCILING`; no new reservation | Repeat evidence read | `TestPhase6NonceDisagreementAndTerminalReceipt` |
| External transaction advances nonce | Account `BLOCKED_EXTERNAL_NONCE`; no renumber or reuse | Operator investigation | `TestPhase6ExternalNonceRace` |
| External pending transaction overlaps unsent reservation | Account `BLOCKED_EXTERNAL_NONCE`; retain owned nonce and signed history | Operator investigation | `TestPhase6PendingExternalNonceOverlapsReservation` |
| Restart with `UNCERTAIN` or `PENDING` | Reuse durable hash and receipt work | Bounded scheduler cycle | `TestPhase6RuntimeRestartAndShutdown` |
| Restart near replacement or reorg | Existing attempt lineage and receipt history retained | Resume policy checks | `TestPhase6ReplacementAndReorgStartup` |
| Restart during confirmation | Continue canonical depth check | Resume scheduled receipt poll | `TestPhase6RuntimeRestartAndShutdown` |
| Shutdown during external send | Fenced state and exact raw bytes remain; workers exit | Reconcile same hash after restart | `TestPhase6RuntimeRestartAndShutdown` |
| Worker backlog | Fixed workers, keyset pages and durable retries | Next cycle advances cursor | `TestPhase6BoundedRuntimeLoad`, `TestPhase6RuntimeRetrySchedule` |

## HTTP and operational boundary

| Failure | HTTP and durable behavior | Evidence |
| --- | --- | --- |
| Duplicate POST, same payload | 201 once, 200 with the same intent on replay; one nonce and attempt | `TestPhase7APIConcurrentIdempotencyAndRestart` |
| Duplicate POST, changed payload | 409 without new intent | `TestPhase7APIContract` |
| Accepted response lost | `UNCERTAIN`, exact-hash reconciliation, no second operation | `TestPhase7Demo` |
| Database unavailable | 503 on new work, readiness reports `database_unavailable`; liveness remains independent | `TestPhase7ReadinessIsolation`, `TestPhase7APIContract` |
| Signer/account blocked | 503 on new work; committed same-key retry remains readable | `TestPhase7ReadinessIsolation` |
| No eligible RPC | New work is not admitted; readiness reports `no_eligible_rpc` or `wrong_chain_rpc` | `TestPhase7ReadinessIsolation` |
| Service restart | Durable intent and signed bytes are reloaded; same-key POST returns 200 | `TestPhase7Demo`, `TestPhase7APIConcurrentIdempotencyAndRestart` |
| Oversized request | 413 before database/RPC work | `TestPhase7APIContract` |
| HTTP deadline | Bounded request context and server timeouts; caller can retry the same key | HTTP configuration and idempotency tests |
| Graceful shutdown | Bounded server drain and runtime cancellation; durable state survives | `TestPhase6RuntimeRestartAndShutdown`, server configuration |
| Metrics exporter unavailable | Local exposition remains available; no remote exporter is required | `TestPhase7APIContract` |
