#!/usr/bin/env python3
"""Check the Phase 7 public API, evidence and hygiene contract."""

from pathlib import Path
import re
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]
api = yaml.safe_load((ROOT / "api/openapi.yaml").read_text())
implementation = (ROOT / "internal/orchestrator/http_api.go").read_text()
projection = (ROOT / "internal/orchestrator/projection.go").read_text()
metrics = (ROOT / "internal/orchestrator/http_observability.go").read_text()
tests = "\n".join(path.read_text() for path in (ROOT / "internal/orchestrator").glob("*test.go"))
errors = []

routes = {(verb.lower(), route) for verb, route in re.findall(
    r'a\.mux\.HandleFunc\("(GET|POST) ([^\"]+)"', implementation)}
documented = {(verb, route) for route, item in api["paths"].items()
              for verb in item if verb in ("get", "post")}
if routes != documented:
    errors.append(f"route drift: runtime={sorted(routes)} spec={sorted(documented)}")

post = api["paths"]["/v1/transactions"]["post"]
if not {"200", "201", "400", "401", "409", "413", "415", "422", "503", "504"}.issubset(post["responses"]):
    errors.append("POST response codes drifted")
for code in ("http.StatusCreated", "http.StatusOK", "http.StatusConflict", "http.StatusRequestEntityTooLarge",
             "http.StatusUnsupportedMediaType", "http.StatusUnprocessableEntity", "http.StatusServiceUnavailable"):
    if code not in implementation:
        errors.append(f"runtime response missing {code}")
if "ApiError" not in str(api["components"]["responses"]["Error"]):
    errors.append("OpenAPI error response schema missing")
for name in ("code", "message", "request_id"):
    if name not in api["components"]["schemas"]["ApiError"]["required"]:
        errors.append(f"stable error field missing: {name}")

schema = api["components"]["schemas"]["TransactionIntent"]
for field in ("intent_id", "state", "chain_id", "signer_id", "to", "value_wei", "nonce",
              "active_attempt", "winning_attempt", "attempts", "receipt_history", "broadcast",
              "confirmation", "account", "reorg_events", "created_at", "updated_at"):
    if field not in schema["properties"] or f'json:"{field}' not in projection:
        errors.append(f"read-model field drift: {field}")
for field in ("attempt_number", "transaction_hash", "nonce", "max_fee_per_gas",
              "max_priority_fee_per_gas", "replaces_attempt_id", "status"):
    if field not in api["components"]["schemas"]["TransactionAttempt"]["properties"]:
        errors.append(f"attempt history field missing: {field}")

for name in ("txorch_intents_total", "txorch_attempts_total", "txorch_pending", "txorch_unknown_outcomes",
             "txorch_replacements_total", "txorch_confirmation_seconds", "txorch_nonce_reservations",
             "txorch_rpc_requests_total", "txorch_rpc_latency_seconds", "txorch_signer_errors_total",
             "txorch_reorged_receipts_total"):
    if name not in metrics:
        errors.append(f"metric missing: {name}")
for forbidden in ('intent_id=', 'request_id=', 'transaction_hash=', 'signer_address=', 'destination=', 'nonce='):
    if forbidden in metrics:
        errors.append(f"high-cardinality metric label: {forbidden}")

for name in ("TestPhase7APIContract", "TestPhase7APIConcurrentIdempotencyAndRestart",
             "TestPhase7ReadinessIsolation", "TestPhase7DeepReorgReadinessWithOtherAccount",
             "TestPhase7TraceSafety", "TestPhase7MetricCardinality",
	     "TestPhase7ErrorLogRedaction",
             "TestPhase7Demo", "TestPhase7BenchmarkSmoke"):
    if f"func {name}(" not in tests:
        errors.append(f"integration test missing: {name}")
for file in ("Dockerfile", ".dockerignore", "scripts/phase7_local.py", "scripts/source_identity.py",
             "docs/demo.md", "docs/benchmark.md", "docs/observability.md"):
    if not (ROOT / file).is_file():
        errors.append(f"Phase 7 artifact missing: {file}")
docker = (ROOT / "Dockerfile").read_text()
for token in ("FROM golang:", "FROM gcr.io/distroless/", "USER 65532:65532", "ENTRYPOINT"):
    if token not in docker:
        errors.append(f"Docker runtime requirement missing: {token}")
if errors:
    sys.exit("Phase 7 contract failed:\n" + "\n".join(errors))
print("Phase 7 route, response, projection, metrics, test and artifact contract passed")
