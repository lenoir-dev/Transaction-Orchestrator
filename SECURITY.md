# Security

The implementation includes a development signer and can submit stored signed bytes to configured RPC endpoints. Signed bytes can also be submitted by anyone who obtains them; restrict database access and backups. Do not submit keys, credentials, signed raw transactions, or sensitive calldata in issue reports. See [the threat model](docs/threat-model.md) for trust boundaries and residual risks.

Before any deployment, assign a private vulnerability reporting channel, review signer/account authorization and key handling, configure quotas and fee ceilings, and exercise backup and recovery. No security audit or production suitability is claimed.

Inclusion and finality are chain and provider dependent. Multiple RPC endpoints improve availability and visibility, not consensus or proof of global mempool absence. External same-signer transactions can invalidate local nonce assumptions. Automatic reorg recovery is bounded and deeper divergence requires operator action. The development signer is not production KMS/HSM custody. No production operating history or independent audit exists; the local benchmark is not an SLA.
