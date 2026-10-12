#!/usr/bin/env python3
"""Keep the signing and persistence boundary free of network submission."""

from pathlib import Path
import re
import sys

root = Path(__file__).resolve().parents[1]
runtime = [root / "internal" / "orchestrator" / (name + ".go") for name in
           ("domain", "postgres", "transaction", "signer", "signing")]
forbidden = (
    r"eth_sendRawTransaction",
    r"\bSendTransaction\s*\(",
    r"\bSendRawTransaction\s*\(",
    r'"github\.com/ethereum/go-ethereum/(?:ethclient|rpc)"',
)
errors = []
for path in runtime:
    content = path.read_text(encoding="utf-8")
    if any(re.search(pattern, content) for pattern in forbidden):
        errors.append(str(path.relative_to(root)))
if errors:
    sys.exit("transaction submission boundary violated: " + ", ".join(errors))
print(f"Signing boundary has no transaction submission in {len(runtime)} Go files")
