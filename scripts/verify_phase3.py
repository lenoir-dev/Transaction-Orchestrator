#!/usr/bin/env python3
"""Require the local fault target and guard the exact-byte broadcast boundary."""

import os
from pathlib import Path
import re
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
binary = os.environ.get("TXORCH_ANVIL_BINARY")
if not binary or not Path(binary).is_file():
    sys.exit("TXORCH_ANVIL_BINARY must name a local Anvil binary")
try:
    version = subprocess.run([binary, "--version"], capture_output=True, text=True,
                             check=True, timeout=10).stdout.splitlines()[0]
except (OSError, subprocess.CalledProcessError, subprocess.TimeoutExpired, IndexError):
    sys.exit("could not verify local Anvil binary")

broadcast = (root / "internal/orchestrator/broadcast.go").read_text()
rpc = (root / "internal/orchestrator/rpc.go").read_text()
for token in ("LoadSignedAttempt", "lease_token", "TransactionByHash", "ReceiptByHash",
              "SendRawTransaction", "broadcast_send_count"):
    if token not in broadcast + rpc:
        sys.exit("broadcast boundary missing " + token)
for token in ("SignInitial(", "BuildType2(", "SignTransaction(", "ReserveNonce(",
              "INSERT INTO transaction_attempts", "attempt_number=2"):
    if token in broadcast + rpc:
        sys.exit("replacement or signing crossed the broadcast boundary: " + token)
if re.search(r"\b(?:eth_sendRawTransaction)\b", broadcast):
    sys.exit("broadcast service bypassed its RPC interface")
print("Phase 3 exact-byte boundary and no-replacement guard passed; " + version)
