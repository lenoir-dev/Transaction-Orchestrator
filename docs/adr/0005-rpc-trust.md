# ADR 0005: Multi-RPC trust boundary

Status: Accepted design.

Configured endpoints improve availability, broadcast reach, and observation coverage. They are not votes in blockchain consensus. Chain ID, optional genesis anchor and method/read support determine eligibility. Receipt polling ignores heads below its durable verified head. Wall-clock freshness and a general cross-chain lag detector remain deployment work. Null lookups and provider errors are observations, not global facts.

Broadcast exact bytes under bounded calls. Receipt polling validates block-by-hash membership and canonical block-by-number identity, then verifies a stable head view. Eligible peers that disagree about canonical receipt blocks hold confirmation progress and return `ErrEvidenceUnavailable`, with the polling schedule and failure audit retained. An unavailable or stale endpoint can be bypassed for a healthy configured endpoint; no majority voting is used. A shorter post-reorg head is treated as unavailable until it reaches the durable verified height.

The service trusts the configured chain's canonical view and signer/account policy, with residual risk from correlated or malicious providers, deep reorgs, censorship, and unavailable history. For a high-value deployment, independently operated providers and stronger finalized-block policies need explicit configuration and tests; a fixed depth alone is a risk threshold, not economic finality.
