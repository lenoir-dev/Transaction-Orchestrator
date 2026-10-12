# ADR 0006: Canonical receipts, confirmation count and bounded reorg recovery

Status: Accepted and implemented.

## Upstream semantics

`eth_getTransactionReceipt` is unavailable for a pending transaction and identifies inclusion by transaction hash, block hash, block number and transaction index; status 0 is failed EVM execution within a mined transaction. `eth_getBlockByHash` provides the named block context, while block-by-number refers to the endpoint's current canonical chain. The `latest`, `safe` and `finalized` tags are supported in the Ethereum RPC specification and go-ethereum APIs, but this implementation uses `latest` with a configurable count. Neither one receipt nor a fixed number of confirmations establishes universal or absolute finality.

Primary references:

- [Ethereum JSON-RPC methods and tags](https://ethereum.org/developers/docs/apis/json-rpc/).
- [EIP-1474 receipt fields](https://eips.ethereum.org/EIPS/eip-1474).
- [go-ethereum ethclient block/receipt APIs](https://github.com/ethereum/go-ethereum/blob/master/ethclient/ethclient.go).
- [Official Anvil RPC methods](https://www.getfoundry.sh/anvil/rpc-methods).

The local HTTP adapter uses the same block/receipt RPC semantics directly. It validates quantity and fixed-width hash encodings, then the receipt service validates block identity and transaction membership. It does not verify a receipt trie proof or consensus proof.

## Chosen product policy

Creation resolves a chain-specific confirmation depth into the immutable intent (1–100000). Polling supplies an explicit clock, interval, per-RPC deadline, total read deadline, at most eight endpoints and a 1–101 attempt budget. Reads run serially, without spawned goroutines or open DB transactions. A bounded due-work query includes confirmed inclusions after restart. Success, absence and evidence failure persist the next poll.

A receipt is canonical only after:

1. Its transaction hash and nonzero block hash match known signed material.
2. Block-by-hash has the stated hash and number and the transaction at the stated index.
3. Canonical block at that number has the same hash.
4. The head is not below the receipt or the durable verified head.
5. Rechecking the head's canonical identity and receipt height produces a coherent view.
6. Other eligible readable endpoints do not report a competing canonical receipt-block hash.

Confirmations are `canonical_head - receipt_block + 1`: a receipt at the head counts as one. The first below-threshold observation is `MINED`, the next is `CONFIRMING`, and threshold satisfaction is `CONFIRMED`. Depth 1 explicitly permits immediate confirmation. `CONFIRMED` describes inclusion under this policy; `SUCCEEDED`/`REVERTED` describes execution. Status 0 never means missing inclusion and never triggers a fee retry.

## Evidence and transaction boundaries

Inclusion identity is transaction hash + block hash + block number. Receipt rows keep attempt UUID, transaction index, execution status, gas used, effective gas price when available, observation time and verification metadata. Inclusion fields are immutable. Progress updates verification metadata for the same live observation. An orphan remains in history; re-inclusion appends a new observation, even when an exact prior branch returns. Block history permits competing hashes at one height and retains only relevant receipt/head evidence.

A version-fenced account/intent transaction commits receipt/progress, winner, state, schedule, block evidence and audit atomically. Process exits before commit roll all these changes back. Post-commit recovery uses only PostgreSQL evidence. Reorg invalidation atomically marks the old receipt orphaned, clears winner/count and records `REORGED`. A later poll scans every known attempt before selecting another canonical winner. Without a receipt, visibility/active-candidate state determines `PENDING`, `UNCERTAIN`, `READY` or `SIGNING`; nonce consumption without a known inclusion blocks reconciliation. No new intent, nonce or fee attempt is created by rollback.

## Supported reorg window

`MaxReorgDepth` is configured between 1 and 1000, stored on the first poll, and must match subsequent calls. The conservative required window is the larger of the prior verified confirmation count and the current head distance from the old receipt block. This avoids pretending to know a common ancestor beyond retained evidence. The test policy uses depth 3 and reorg window 4; no universal deployment value is imposed.

Within the window, even a confirmed inclusion can enter `REORGED`. Beyond it, the service enters `BLOCKED/DEEP_REORG_BLOCKED`, preserves orphan history, pauses the account and fails `ReceiptReady`. An operator must resynchronize the incident under a separate authenticated procedure before clearing readiness; a generic resume does not clear that flag.

## Provider trust and limits

A null receipt with an unchanged canonical block holds existing state. Conflicting eligible block views hold progress and schedule a retry; providers are never counted as votes. Heads below the durable verified height cannot regress confirmation state. This also means a genuinely shorter new branch must catch up before the policy can establish its canonical identity. One healthy configured provider can support availability when others fail. Correlated false data, wall-clock freshness, consensus verification and unsupported deep forks remain outside this policy.

Anvil snapshots/reverts consume snapshots, pool clearing removes pending contenders, and dump/load retains exact block state for A → B → A. Tests assert actual block identity changes and preserved intent/nonce/attempt history. No public RPC or chain is used.
