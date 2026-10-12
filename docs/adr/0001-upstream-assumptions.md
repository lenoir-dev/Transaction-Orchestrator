# ADR 0001: Upstream assumptions

Status: Accepted design; revisit before adding chain I/O. Checked 2026-10-06 against official sources.

Go 1.27.2 is selected in `go.mod`. The PostgreSQL driver is pgx/v5 v5.11.0; PostgreSQL 16.15 passed the persistence suite and PostgreSQL 18.6 is the signing integration baseline. Recheck supported versions when updating dependencies. [Go releases](https://go.dev/doc/devel/release), [pgx documentation](https://pkg.go.dev/github.com/jackc/pgx/v5), [PostgreSQL row locks](https://www.postgresql.org/docs/18/explicit-locking.html).

go-ethereum v1.17.7 is pinned. Type-2 construction uses `types.NewTx(&types.DynamicFeeTx{...})`, with `ChainID`, `Nonce`, `GasTipCap`, `GasFeeCap`, `Gas`, `To`, `Value`, `Data`, and an empty access list. `types.NewLondonSigner` recovers the sender; signed `Transaction.Hash()` and `MarshalBinary()` provide the local hash and exact typed encoding. [Release](https://github.com/ethereum/go-ethereum/releases/tag/v1.17.7), [types API](https://pkg.go.dev/github.com/ethereum/go-ethereum@v1.17.7/core/types).

EIP-1559 type 2 uses chain ID, nonce, priority cap, fee cap, gas limit, destination, amount, data, access list, and signature; effective fee depends on base fee. The base fee may rise after signing, so a fixed cap may temporarily prevent inclusion. [EIP-1559](https://eips.ethereum.org/EIPS/eip-1559).

`eth_sendRawTransaction` submits signed bytes and may return a hash; a lost response does not establish rejection. `eth_getTransactionByHash` and `eth_getTransactionReceipt` can return null; a receipt is absent while pending. `eth_getTransactionCount(address, "latest")` reflects latest chain state; `"pending"` reflects a provider's pending state. Neither pending count nor null proves global mempool absence, nor does JSON-RPC provide an atomic compare-and-swap nonce reservation, finality guarantee, or standard error taxonomy for "already known". [Ethereum JSON-RPC](https://ethereum.org/developers/docs/apis/json-rpc/).

Replacement admission is client and node policy, not an EIP-1559 consensus rule. Geth documents a default `--txpool.pricebump` of 10%; its pool checks both fee cap and tip cap for a bump and can return `ErrReplaceUnderpriced`. Endpoints may use other thresholds, mempool views, or policies. The v1 proposed default bump is 15% with rounding up plus one wei on both caps, configurable per chain and tested against actual supported endpoints. A rejection is evidence about that endpoint only. [Geth options](https://geth.ethereum.org/docs/fundamentals/command-line-options), [Geth tx list](https://github.com/ethereum/go-ethereum/blob/v1.17.7/core/txpool/legacypool/list.go).

Prometheus officially supports a Go client and `/metrics` handler; OpenTelemetry Go traces and metrics are stable. No telemetry library version is pinned until implementation. [Prometheus Go guide](https://prometheus.io/docs/guides/go-application/), [OpenTelemetry Go](https://opentelemetry.io/docs/languages/go/).

The precise supported chain/fork, endpoint client diversity, confirmation depth, fee budget, signer adapter, and dependency patch versions are deployment decisions. A pre-implementation compatibility pass must check them rather than treating this snapshot as a perpetual guarantee.
