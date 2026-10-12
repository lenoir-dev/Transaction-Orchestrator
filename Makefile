.PHONY: verify-design verify verify-phase2 verify-phase3 verify-phase4 verify-phase5 verify-phase6 verify-phase7 verify-release demo benchmark-smoke clean-clone-verify build
export PATH := $(shell go env GOPATH)/bin:$(PATH)

verify-design:
	python3 scripts/verify_spec.py

verify: verify-design
	@test -z "$$(gofmt -l cmd/txorch/*.go internal/orchestrator/*.go)"
	go mod verify
	go vet ./...
	go test ./...
	go test -race ./...

verify-phase2:
	python3 scripts/check_postgres18.py
	python3 scripts/verify_phase2.py
	$(MAKE) verify

verify-phase3:
	python3 scripts/check_postgres18.py
	python3 scripts/verify_phase2.py
	python3 scripts/verify_phase3.py
	$(MAKE) verify

verify-phase4: verify-phase3
	python3 scripts/verify_phase4.py

verify-phase5: verify-phase4
	python3 scripts/verify_phase5.py

verify-phase6: verify-phase5
	python3 scripts/verify_phase6.py

build:
	go build -ldflags "-X main.sourceIdentity=$$(python3 scripts/source_identity.py)" -o txorch ./cmd/txorch

demo:
	python3 scripts/phase7_local.py demo

benchmark-smoke:
	python3 scripts/phase7_local.py benchmark

clean-clone-verify:
	python3 scripts/phase7_local.py clean-clone

verify-phase7:
	python3 scripts/phase7_local.py verify

verify-release:
	python3 scripts/release.py preflight
	$(MAKE) verify-phase7
	go test ./internal/orchestrator -run '^$$' -fuzz '^FuzzBumpFees$$' -fuzztime=5s
	python3 scripts/release.py build
	python3 scripts/release.py scan
	python3 scripts/release.py clean-clone
	python3 scripts/release.py finalize
