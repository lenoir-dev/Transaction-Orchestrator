#!/usr/bin/env python3
"""Check the bounded replacement boundary and its local fault scenarios."""

from pathlib import Path
import re
import sys

root = Path(__file__).resolve().parents[1]
replacement = (root / "internal/orchestrator/replacement.go").read_text()
broadcast = (root / "internal/orchestrator/broadcast.go").read_text()
fees = (root / "internal/orchestrator/fees.go").read_text()
tests = (root / "internal/orchestrator/replacement_test.go").read_text()
fee_tests = (root / "internal/orchestrator/fees_test.go").read_text()
schema = (root / "migrations/004_replacements.sql").read_text()

required = {
    "lineage and immutability": all(x in schema for x in
        ("replaces_attempt_id", "protect_replacement_metadata", "check_replacement_attempt")),
    "both fee fields and ceiling": all(x in fees for x in
        ("MaxFeePerGasWei", "MaxTipPerGasWei", "ErrFeeCeiling")),
    "fenced claim and receipt check": all(x in replacement + broadcast for x in
        ("lease_token", "ObserveWinner", "ReceiptByHash", "TransactionByHash", "LatestNonce")),
    "bounded interval and count": all(x in replacement for x in
        ("MinAge", "ErrReplacementTooEarly", "ErrReplacementLimit")),
    "bounded metrics": all(x in replacement for x in
        ("txorch_replacements_total", "txorch_replacement_blocked_total")),
    "fee property checks": all(x in fee_tests for x in
        ("TestBumpFeesBoundsAndRounding", "FuzzBumpFees")),
}
for name in (
    "TestAnvilReplacementMinesFirst", "TestAnvilOriginalMinesBeforeReplacementSend",
    "TestReplacementChainLimitCeilingAndUnderpriced", "TestReplacementUnknownAndConcurrent",
    "TestReplacementCeilingAndEarly", "TestReplacementProcessCrash",
    "TestReplacementBroadcastCrash", "TestReplacementDatabaseAndSignerGuards",
):
    required[name] = f"func {name}(" in tests

for state in ("CONFIRMED", "REORGED", "CANCELLED"):
    required[f"no {state} transition"] = not re.search(
        rf"(?:state\s*=\s*|SET\s+state\s*=\s*)['\"]{state}['\"]",
        replacement + broadcast, re.IGNORECASE)

missing = [name for name, present in required.items() if not present]
if missing:
    sys.exit("Phase 4 verification failed: " + ", ".join(missing))
print("Phase 4 replacement boundary and scenario inventory passed")
