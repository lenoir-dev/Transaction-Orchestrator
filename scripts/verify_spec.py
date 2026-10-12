#!/usr/bin/env python3
"""Validate the repository's design artifacts."""

from pathlib import Path
import hashlib
import re
import sys

try:
    import yaml
except ImportError:
    sys.exit("PyYAML is required for the OpenAPI parse check")

ROOT = Path(__file__).resolve().parents[1]
REQUIRED = [
    "README.md", "SPEC.md", "SECURITY.md", "CONTRIBUTING.md", "LICENSE",
    "api/openapi.yaml", "migrations/001_initial.sql", "docs/database.md",
    "docs/failure-matrix.md", "docs/threat-model.md", "docs/testing.md", "docs/persistence.md",
    "docs/architecture.md", "docs/evidence.md", "docs/demo.md", "migrations/002_signed_attempts.sql",
    "migrations/003_broadcast.sql", "scripts/verify_phase3.py",
    "migrations/004_replacements.sql", "scripts/verify_phase4.py",
    "migrations/005_receipt_finality.sql", "scripts/verify_phase5.py",
    "docs/adr/0006-receipt-confirmation.md", "docs/adr/0007-rpc-runtime.md",
    "migrations/006_runtime_accounts.sql", "scripts/verify_phase6.py",
    *[f"docs/adr/{n:04d}-{name}.md" for n, name in [
        (1, "upstream-assumptions"), (2, "nonce-ownership"),
        (3, "unknown-outcome"), (4, "replacement"), (5, "rpc-trust")]],
]
errors = []
for name in REQUIRED:
    if not (ROOT / name).is_file():
        errors.append(f"missing {name}")

if errors:
    sys.exit("\n".join(errors))

sources = {name: (ROOT / name).read_text(encoding="utf-8") for name in REQUIRED}
if hashlib.sha256((ROOT / "LICENSE").read_bytes()).hexdigest() != "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30":
    errors.append("LICENSE differs from the standard Apache-2.0 text")
for path in [*ROOT.glob("internal/**/*.go"), *ROOT.glob("cmd/**/*.go"), *ROOT.glob("scripts/*.py"),
             *ROOT.glob("docs/**/*.md"), *ROOT.glob("migrations/*.sql"),
             ROOT / "Makefile", ROOT / "Dockerfile", ROOT / ".dockerignore",
             ROOT / "go.mod", ROOT / "go.sum"]:
    if path.is_file():
        sources[str(path.relative_to(ROOT))] = path.read_text(encoding="utf-8")
provenance_token_hashes = {
    "32e83e92d45d71f6", "d4f0bc5a29de06b5", "8c70b25cbbbe8e79",
    "487b91042c7cf27a", "cf07194ee232eb53", "eb7526362ee7678c",
    "60965168ce762e94", "57de4cf40144bdf7",
}
spec = sources["SPEC.md"]
for token in ("Transaction Intent", "CREATED", "RESERVED", "SIGNING", "READY",
              "UNCERTAIN", "PENDING", "MINED", "CONFIRMED", "REORGED",
              "BLOCKED", "FAILED", "idempotency", "nonce", "receipt"):
    if token.lower() not in spec.lower():
        errors.append(f"SPEC missing {token}")

schema = sources["migrations/001_initial.sql"]
for token in ("CREATE TABLE signer_accounts", "CREATE TABLE transaction_intents",
              "CREATE TABLE nonce_reservations", "CREATE TABLE transaction_attempts",
              "CREATE TABLE observed_receipts", "CREATE TABLE audit_events",
              "UNIQUE (tenant_id, idempotency_key)",
              "UNIQUE (chain_id, signer_address, nonce)", "protect_signed_attempt"):
    if token not in schema:
        errors.append(f"schema missing {token}")

signing_schema = sources["migrations/002_signed_attempts.sql"]
for token in ("ADD COLUMN attempt_id", "policy_snapshot_id", "intent_payload_immutable",
              "attempt_identity_immutable", "intent_attempt_alignment", "attempt_intent_alignment"):
    if token not in signing_schema:
        errors.append(f"signing migration missing {token}")

broadcast_schema = sources["migrations/003_broadcast.sql"]
for token in ("CREATE TABLE broadcast_observations", "broadcast_send_count",
              "broadcast_state_alignment", "transaction_hash"):
    if token not in broadcast_schema:
        errors.append(f"broadcast migration missing {token}")

replacement_schema = sources["migrations/004_replacements.sql"]
for token in ("replaces_attempt_id", "replacement_reason", "bump_bps",
              "check_replacement_attempt", "protect_replacement_metadata"):
    if token not in replacement_schema:
        errors.append(f"replacement migration missing {token}")

try:
    api = yaml.safe_load(sources["api/openapi.yaml"])
    assert api["openapi"].startswith("3.1.")
    for path in ("/v1/transactions", "/v1/transactions/{id}", "/v1/status",
                 "/health/live", "/health/ready", "/metrics"):
        assert path in api["paths"], path
    assert any(p.get("$ref") == "#/components/parameters/IdempotencyKey"
               for p in api["paths"]["/v1/transactions"]["post"]["parameters"])
    def check_refs(value):
        if isinstance(value, dict):
            for key, child in value.items():
                if key == "$ref" and isinstance(child, str) and child.startswith("#/"):
                    target = api
                    for part in child[2:].split("/"):
                        target = target[part.replace("~1", "/").replace("~0", "~")]
                else:
                    check_refs(child)
        elif isinstance(value, list):
            for child in value:
                check_refs(child)
    check_refs(api)
except (AssertionError, KeyError, TypeError, yaml.YAMLError) as exc:
    errors.append(f"OpenAPI parse/reference check: {exc}")

for name, content in sources.items():
    if name.endswith(".md"):
        for link in re.findall(r"\]\(([^)]+)\)", content):
            if "://" not in link and not link.startswith("#"):
                path = (ROOT / name).parent / link.split("#")[0]
                if not path.exists():
                    errors.append(f"broken local link {name}: {link}")
    tokens = re.findall(r"[A-Za-z]+", content.lower())
    if name != "go.sum" and any(hashlib.sha256(token.encode()).hexdigest()[:16] in provenance_token_hashes
           for token in tokens):
        errors.append(f"{name}: provenance terminology")
    for pattern, label in [
        (r"(?:/Users/|/home/[^ /]+/|/workspace/|[A-Z]:\\\\Users\\\\)", "local path"),
        (r"(?i)(?:api[_-]?key|secret|private[_-]?key)\s*[:=]\s*['\"]?[A-Za-z0-9_/-]{20,}", "possible secret"),
    ]:
        if name == "scripts/verify_spec.py" and label == "local path":
            continue
        if re.search(pattern, content):
            errors.append(f"{name}: {label}")

if errors:
    sys.exit("Design verification failed:\n" + "\n".join(errors))
print(f"Design verification passed: {len(REQUIRED)} required artifacts, OpenAPI, references, invariants, and hygiene")
print("PostgreSQL migration and runtime checks run in the integration suite when configured")
