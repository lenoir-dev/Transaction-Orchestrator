# Verification evidence

The database integration suite creates an isolated schema, applies migrations `001` through `006` in order, then removes the schema after each test. Broadcast tests start ephemeral local Anvil processes and use generated local keys; no public RPC endpoint is contacted.

| Claim | Permanent check |
| --- | --- |
| Deterministic type-2 construction and exact fields | `TestDeterministicType2Construction` |
| uint64 and uint256 bounds, fee order and configured ceilings | `TestTransactionQuantityBounds`, `TestSigningFeeInputBounds` |
| Signer identity and field validation | `TestAdversarialSignerOutput` |
| Exact raw encoding and locally computed hash | `TestValidSignedAttemptAndImmutability`, `TestDatabaseSigningAlignment` |
| Signed attempt and intent immutability | `TestValidSignedAttemptAndImmutability` |
| One durable attempt under concurrent requests | `TestConcurrentSignInitial` |
| Timeout and infrastructure failure classification | `TestSignerFailuresAndRetry` |
| Process exit before signer call, during call, before signed commit, after commit | `TestSigningCrashBoundaries` |
| Existing intent, idempotency, nonce, and restart behavior | PostgreSQL tests in `postgres_test.go` |
| Signing boundary never submits | `scripts/verify_phase2.py` scans signing and persistence files |
| Node accepts, response lost; restart recovers hash | `TestAnvilResponseLostAndRestart` forwards to Anvil then closes the proxy response |
| Request lost before node; exact rebroadcast | `TestAnvilRequestLostAndExactRebroadcast` closes before forwarding |
| Chain mismatch, hash mismatch, fee and nonce classification | `TestBroadcastOutcomesAndEligibility` |
| Already known, fallback visibility, receipt observation | `TestAlreadyKnownFallbackReceiptAndNonceFound` |
| Bounded sends and concurrent workers | `TestBroadcastConcurrentWorkersAndBounds` |
| Crash before RPC and after acceptance before commit | `TestBroadcastProcessCrash` exits a child process, then reloads PostgreSQL |
| Corrupt raw bytes never sent; classified transport and malformed RPC | `TestSendClassificationAndCorruptBytes`, `TestRPCMalformedResponse` |
| No new signing in broadcast boundary | `scripts/verify_phase3.py` and one-attempt SQL assertions |

The previous persistence and signed-attempt suites passed on PostgreSQL 16.15 and 18.6. The Phase 5 target includes Phase 4 and requires PostgreSQL 18 and Anvil; an unset URL or binary fails the gate rather than silently skipping integration. Exact run results belong in the release evidence, not in fabricated performance claims.

| Phase 4 claim | Permanent check |
| --- | --- |
| Same-call, same-nonce replacement and one Anvil execution | `TestAnvilReplacementMinesFirst` |
| Original receipt before replacement send stops send | `TestAnvilOriginalMinesBeforeReplacementSend` |
| #1 → #2 → #3 lineage, count limit and underpriced progression | `TestReplacementChainLimitCeilingAndUnderpriced` |
| UNKNOWN recovery and 32 competing workers | `TestReplacementUnknownAndConcurrent` |
| Interval and fee ceiling | `TestReplacementCeilingAndEarly` |
| Claim/sign and broadcast crash boundaries | `TestReplacementProcessCrash`, `TestReplacementBroadcastCrash` |
| SQL lineage/immutability and adversarial signer | `TestReplacementDatabaseAndSignerGuards` |
| Fee rounding, uint256 bounds and property seeds | `TestBumpFeesBoundsAndRounding`, `FuzzBumpFees` |
| Boundary inventory and absence of finality/cancellation transition | `scripts/verify_phase4.py` |

`make verify-phase4` runs the Phase 3 gate, formatting, module integrity, vet, ordinary and race-enabled suites, then the replacement boundary check. The replacement suite is complemented by canonical receipt, confirmation policy and reorg checks in the Phase 5 target.

| Phase 5 claim | Permanent check |
| --- | --- |
| Head receipt, N−1 / N / N+1 counts; no replacement after confirmation | `TestReceiptConfirmationBoundaries` |
| Reorg depth 1/2/4 and deep depth 5 readiness failure | `TestAnvilReceiptReorgDepths` |
| Original #1 on A, replacement #2 on B | `TestAnvilReplacementReceiptReorg` |
| Replacement #2 on A, original #1 on B | `TestAnvilOriginalWinsAfterReplacementReorg` |
| Exact A → B → A inclusion identity with retained orphan row | `TestAnvilReceiptBranchReturn` |
| False receipt fields, null receipt, stale endpoint and conflicting providers | `TestReceiptInvalidStaleAndMultiRPC` |
| Confirmed state holds under stale head; configuration bounds | `TestReceiptConfirmedStalenessAndPolicyBounds` |
| Canonical failed execution remains confirmed inclusion with REVERTED outcome | `TestReceiptExecutionReverted` |
| Atomic receipt/mined/progress/confirmed/reorg crash boundaries | `TestReceiptProcessCrash` |
| 32 receipt workers, scheduling restart and SQL evidence immutability | `TestReceiptConcurrentAndScheduling` |
| Bounded RPC deadline | `TestReceiptBoundedDeadline` |

The local fixture uses Anvil snapshot/revert, explicit pool clearing, deterministic branch divergence, and dump/load for exact branch return. Every reorg assertion verifies one intent, one reservation and the expected immutable attempt count. PostgreSQL 18.6 and Anvil 1.8.5 are the verification targets. `make verify-phase5` includes vet, module integrity, ordinary and race-enabled suites plus artifact hygiene and boundary inventories. Confirmation count remains a configurable risk policy, not an assertion of absolute finality.

## Phase 6 boundary inventory

| Claim | Permanent check |
| --- | --- |
| Wrong-chain exclusion, cooldown and recovery | `TestPhase6EndpointEligibilityAndRecovery` |
| New account pending nonce initialization and external nonce race | `TestPhase6NewAccountPendingInitialization`, `TestPhase6ExternalNonceRace` |
| External pending transaction overlaps an unsent owned nonce | `TestPhase6PendingExternalNonceOverlapsReservation` |
| Disagreeing provider nonces; lower stale view does not reset ownership; terminal receipt explains progression | `TestPhase6NonceDisagreementAndTerminalReceipt` |
| Reconciliation interruption and restart; explicit-seed account cannot silently change mode | `TestPhase6ReconciliationRestartAndSeedGuard` |
| Same-attempt broadcast fallback and exact-hash lookup | `TestPhase6MultiRPCBroadcastAndLookup`, `TestPhase6MultiRPCLookupFindsExactAttempt` |
| Stale endpoint excluded from current-state reads | `TestPhase6StaleEndpointExclusion` |
| `UNCERTAIN`, `PENDING`, confirmation restart and clean shutdown | `TestPhase6RuntimeRestartAndShutdown` |
| Replacement eligibility and reorg work after restart | `TestPhase6ReplacementAndReorgStartup` |
| Signer and chain isolation | `TestPhase6SignerAndChainIsolation`, `TestPhase6SignerUnavailableIsolation` |
| Worker and RPC bounds, retry scheduling | `TestPhase6BoundedRuntimeLoad`, `TestPhase6RuntimeRetrySchedule` |
| Internal status projection and bounded endpoint method labels | `TestPhase6RuntimeStatusProjection` |

The Phase 6 gate checks PostgreSQL 18, Anvil 1.8.5, formatting, module integrity, vet, ordinary and race-enabled tests, design references, hygiene, and the boundary inventory. The full local gate passed on Go 1.27.2 darwin/arm64, PostgreSQL 18.6 and official Anvil/Forge 1.8.5 with disposable schemas and local chains. Runtime availability classification is not a provider consensus protocol.

## Phase 7 evidence map

| Claim | Executable evidence |
| --- | --- |
| Tenant-scoped create, canonical replay, conflict and bounded request | `TestPhase7APIContract`, `TestPhase7APIConcurrentIdempotencyAndRestart` |
| Same-key recovery during account outage, healthy liveness and degraded readiness | `TestPhase7ReadinessIsolation`; deep reorg incident with another ready account: `TestPhase7DeepReorgReadinessWithOtherAccount` |
| Coherent safe attempt, receipt and winner projection | `Store.IntentView` repeatable-read transaction; `TestPhase7Demo` checks `UNCERTAIN`, replacement, `CONFIRMING`, `CONFIRMED` and reorg views |
| Bounded metric labels and safe spans | `TestPhase7MetricCardinality`, `TestPhase7TraceSafety` |
| Node accepts then response disappears; restart finds exact hash | `TestPhase7Demo`, validated `artifacts/demo-result.json` |
| Same-nonce replacement, fork withdrawal and changed canonical winner | `TestPhase7Demo`, PostgreSQL ledger checksum and receipt history |
| Correctness-gated local performance | `TestPhase7BenchmarkSmoke`, `scripts/phase7_local.py` validator, three raw runs in `artifacts/benchmark-runs.json` |
| Compiled server behavior | Phase 7 runner launches the built binary against isolated PostgreSQL/Anvil and checks readiness, POST, idempotent replay and GET |
| Clean-source reproduction | `make clean-clone-verify` copies only public source, starts fresh PostgreSQL/Anvil and repeats tests, compiled service smoke, demo and benchmark smoke |
| Image packaging | `docker build` in `make verify-phase7`; multi-stage `Dockerfile` with non-root runtime |

The Phase 7 source identity is a SHA-256 over sorted public relative paths, each followed by NUL, decimal byte length, NUL and raw file bytes. `scripts/source_identity.py` defines the exact file selection and algorithm. This identity is a source snapshot checksum, not a Git commit. Local demo and benchmark output records actual measured tool versions and timings; no production operation or audit history is asserted.
