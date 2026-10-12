#!/usr/bin/env python3
"""Check the bounded canonical receipt boundary and permanent local scenarios."""
from pathlib import Path
import sys

root = Path(__file__).resolve().parents[1]
service = (root / 'internal/orchestrator/receipt.go').read_text()
rpc = (root / 'internal/orchestrator/rpc.go').read_text()
tests = (root / 'internal/orchestrator/receipt_test.go').read_text()
schema = (root / 'migrations/005_receipt_finality.sql').read_text()
checks = {
    'receipt identity/history': all(x in schema for x in ('attempt_id', 'block_hash', 'verification_status', 'protect_receipt_identity')),
    'competing block identities': 'PRIMARY KEY (chain_id,block_number,block_hash)' in schema,
    'canonical block/body and head checks': all(x in service + rpc for x in ('BlockByHash', 'BlockByNumber', 'TransactionIndex', 'LatestBlock')),
    'bounded polling and restart work': all(x in service for x in ('TotalDeadline', 'MaxAttempts', 'receipt_next_poll_at', 'ReceiptWork', 'version != snap.version')),
    'count policy and deep incident': all(x in service for x in ('head - block + 1', 'ReceiptConfirmations', 'MaxReorgDepth', 'DEEP_REORG_BLOCKED', 'ReceiptReady')),
    'bounded telemetry': all(x in service for x in ('txorch_confirmed_total', 'txorch_reorged_receipts_total', 'txorch_receipt_observations_total', 'txorch_confirmation_seconds')),
    'no indexer or cancellation': all(x not in service for x in ('eth_getLogs', 'CANCELLED', 'CancelTransaction')),
}
for name in (
    'TestReceiptConfirmationBoundaries', 'TestAnvilReceiptReorgDepths',
    'TestAnvilReplacementReceiptReorg', 'TestAnvilOriginalWinsAfterReplacementReorg',
    'TestAnvilReceiptBranchReturn', 'TestReceiptInvalidStaleAndMultiRPC',
    'TestReceiptExecutionReverted', 'TestReceiptProcessCrash',
    'TestReceiptConcurrentAndScheduling', 'TestReceiptConfirmedStalenessAndPolicyBounds',
    'TestReceiptBoundedDeadline',
):
    checks[name] = f'func {name}(' in tests
failed = [name for name, ok in checks.items() if not ok]
if failed:
    sys.exit('Phase 5 verification failed: ' + ', '.join(failed))
print('Phase 5 canonical receipt, confirmation and reorg boundary inventory passed')
