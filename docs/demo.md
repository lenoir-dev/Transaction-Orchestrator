# Deterministic local demonstration

make demo uses disposable PostgreSQL 18 and Anvil 1.8.5 instances, a local development signer, the HTTP API and a fault proxy. The runner applies migrations to a fresh database. It downloads and verifies the official Anvil release when no matching binary is supplied. The fixture uses controlled mining and snapshots; it does not wait for a real fee interval or public chain.

The executable scenario is TestPhase7Demo:

1. POST an intent, persist one nonce reservation and immutable signed attempt #1, and read its locally known hash through GET.
2. Forward attempt #1 to Anvil, then close the proxy connection before returning the accepted response. Assert UNCERTAIN and independent Anvil visibility of the same hash.
3. Reopen the PostgreSQL connection, recreate the runtime and HTTP server, retry the same idempotency key, then reconcile the exact hash. Assert one intent, reservation and attempt.
4. Keep mining disabled, create and broadcast attempt #2 with the same nonce and call, and verify lineage in GET. Mine it, observe MINED, CONFIRMING, then depth-three CONFIRMED.
5. Revert the chain snapshot and mine a competing branch without attempt #2. Assert REORGED, the withdrawn winner and retained orphan receipt. Re-include attempt #1, advance confirmations and assert it becomes the canonical winner.
6. Read status, readiness, metrics and trace/log evidence. Query PostgreSQL for exactly one intent, one reservation, two immutable attempts, one orphan and one canonical receipt, and final CONFIRMED winner #1.

The canonical checksum input is intents=1;nonce_reservations=1;attempts=2;canonical=1;orphaned=1;winner=1;state=CONFIRMED. SHA-256 is 501706ff13ee280f7ed9a07d25f4a5a1d779419a80063f1cd5bb1c7d2cf1bffb. The validator recomputes it from the recorded counts and rejects omitted steps. The JSON result records tool versions, attempt hashes, duration, fault count and each asserted step. Hashes and durations naturally vary between runs; the correctness checksum does not.

The fault fixture is bound to the ephemeral local Anvil URL. The server binary accepts a development RPC URL only on loopback when TXORCH_DEV_MODE=1; the demo does not use public chain credentials.
