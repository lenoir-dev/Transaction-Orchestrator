#!/usr/bin/env python3
"""Inventory Phase 6 runtime boundaries and require the pinned local node."""

import os
from pathlib import Path
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
binary = os.environ.get("TXORCH_ANVIL_BINARY")
if not binary or not Path(binary).is_file():
    sys.exit("TXORCH_ANVIL_BINARY must name an Anvil 1.8.5 binary")
try:
    version = subprocess.run([binary, "--version"], check=True, capture_output=True,
                             text=True, timeout=10).stdout.splitlines()[0]
except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired, IndexError):
    sys.exit("could not verify local Anvil version")
if version != "anvil Version: 1.8.5":
    sys.exit(f"Anvil 1.8.5 required; actual: {version}")

endpoint = (root / "internal/orchestrator/endpoint_pool.go").read_text()
account = (root / "internal/orchestrator/account_reconciliation.go").read_text()
runtime = (root / "internal/orchestrator/runtime.go").read_text()
tests = (root / "internal/orchestrator/runtime_phase6_test.go").read_text()
schema = (root / "migrations/006_runtime_accounts.sql").read_text()
checks = {
    "configured endpoint eligibility": all(x in endpoint for x in (
        "GenesisHash", "Capabilities", "WRONG_CHAIN", "WRONG_ANCHOR",
        "STALE_HEAD", "MaxRPCInflight", "CooldownUntil")),
    "bounded RPC telemetry": all(x in endpoint for x in (
        "RPCMetric", "Failovers", "MaxInflight", "rpcSlots")),
    "durable nonce reconciliation": all(x in account + schema for x in (
        "reconciliation_token", "network_nonce_initialized", "PendingNonce",
        "LatestNonce", "EXTERNAL_NONCE_CONFLICT", "account_reconciliation_events")),
    "bounded runtime and retry": all(x in runtime + schema for x in (
        "Workers", "BatchSize", "account_next_reconcile_at", "runtime_retry_at",
        "RecordRuntimeOutcome", "wg.Wait()", "AccountBlockedCounts")),
}
for name in (
    "TestPhase6EndpointEligibilityAndRecovery",
    "TestPhase6ExternalNonceRace",
    "TestPhase6PendingExternalNonceOverlapsReservation",
    "TestPhase6ReconciliationRestartAndSeedGuard",
    "TestPhase6NonceDisagreementAndTerminalReceipt",
    "TestPhase6MultiRPCBroadcastAndLookup",
    "TestPhase6RuntimeRestartAndShutdown",
    "TestPhase6SignerAndChainIsolation",
    "TestPhase6BoundedRuntimeLoad",
    "TestPhase6NewAccountPendingInitialization",
    "TestPhase6StaleEndpointExclusion",
    "TestPhase6MultiRPCLookupFindsExactAttempt",
    "TestPhase6ReplacementAndReorgStartup",
    "TestPhase6RuntimeRetrySchedule",
    "TestPhase6SignerUnavailableIsolation",
    "TestPhase6RuntimeStatusProjection",
):
    checks[name] = f"func {name}(" in tests
failed = [name for name, ok in checks.items() if not ok]
if failed:
    sys.exit("Phase 6 boundary inventory failed: " + ", ".join(failed))
print("Phase 6 endpoint, nonce, runtime, integration and hygiene inventory passed; " + version)
