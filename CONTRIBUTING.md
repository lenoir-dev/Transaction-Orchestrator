# Contributing

Changes should preserve the invariants in [SPEC.md](SPEC.md) and update the relevant ADR, schema, API contract, failure case, and verification evidence together. Run `make verify` with a disposable PostgreSQL database. Add deterministic fault and restart tests for any later external side effect. Do not commit keys, RPC credentials, signed transaction bytes, or chain-specific claims without a reproducible source.
