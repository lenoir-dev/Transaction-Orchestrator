#!/usr/bin/env python3
"""Run isolated PostgreSQL/Anvil Phase 7 evidence from the source tree."""

import argparse
import contextlib
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
ANVIL_VERSION = "anvil Version: 1.8.5"
FOUNDRY_RELEASE = "https://api.github.com/repos/foundry-rs/foundry/releases/tags/v1.8.5"
DARWIN_ARM64_SHA256 = "56049728e25e44a61c7f2e49438023b0890cb45850ffcb7c882f90cd998baf83"
EXPECTED_DEMO_CHECKSUM = "501706ff13ee280f7ed9a07d25f4a5a1d779419a80063f1cd5bb1c7d2cf1bffb"
EXPECTED_BENCHMARK_CHECKSUM = "d5ea1a086d64324b475256f08b46d40a01a29c0201573d1cacf86c6060e3b967"


def run(args, *, env=None, cwd=ROOT, capture=False):
    print("+", " ".join(str(a) for a in args), flush=True)
    return subprocess.run(args, cwd=cwd, env=env, check=True, text=True,
                          capture_output=capture)


def tool_version(binary, flag="--version"):
    return run([binary, flag], capture=True).stdout.splitlines()[0]


def available_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def require_postgres18():
    tools = {}
    for name in ("initdb", "pg_ctl", "psql"):
        binary = shutil.which(name)
        if not binary:
            raise RuntimeError(f"PostgreSQL 18 tool missing: {name}")
        version = tool_version(binary)
        if "18." not in version:
            raise RuntimeError(f"PostgreSQL 18 required: {name} reports {version}")
        tools[name] = binary
    return tools


def anvil_binary(work):
    configured = os.environ.get("TXORCH_ANVIL_BINARY")
    if configured:
        binary = Path(configured).resolve()
        if tool_version(str(binary)) != ANVIL_VERSION:
            raise RuntimeError("TXORCH_ANVIL_BINARY is not Anvil 1.8.5")
        return binary
    os_name = {"Darwin": "darwin", "Linux": "linux"}.get(platform.system())
    arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine())
    if not os_name or not arch:
        raise RuntimeError("Unsupported Foundry release platform; set TXORCH_ANVIL_BINARY")
    asset_name = f"foundry_v1.8.5_{os_name}_{arch}.tar.gz"
    request = urllib.request.Request(FOUNDRY_RELEASE)
    with urllib.request.urlopen(request, timeout=30) as response:
        release = json.load(response)
    asset = next((item for item in release["assets"] if item["name"] == asset_name), None)
    if not asset:
        raise RuntimeError(f"Official Foundry asset unavailable: {asset_name}")
    digest = asset.get("digest", "")
    expected = DARWIN_ARM64_SHA256 if os_name == "darwin" and arch == "arm64" else digest.removeprefix("sha256:")
    if not expected or (digest and digest != "sha256:" + expected):
        raise RuntimeError("Official Foundry release digest missing or inconsistent")
    archive = work / asset_name
    request = urllib.request.Request(asset["browser_download_url"])
    with urllib.request.urlopen(request, timeout=180) as response, archive.open("wb") as out:
        shutil.copyfileobj(response, out)
    actual = hashlib.sha256(archive.read_bytes()).hexdigest()
    if actual != expected:
        raise RuntimeError(f"Foundry archive checksum mismatch: {actual}")
    with tarfile.open(archive, "r:gz") as tar:
        member = next((m for m in tar.getmembers() if m.name == "anvil" and m.isfile()), None)
        if not member:
            raise RuntimeError("Official Foundry archive lacks Anvil")
        binary = work / "anvil"
        with tar.extractfile(member) as source, binary.open("wb") as output:
            shutil.copyfileobj(source, output)
        binary.chmod(0o755)
    archive.unlink()
    if tool_version(str(binary)) != ANVIL_VERSION:
        raise RuntimeError("Downloaded Anvil version does not match 1.8.5")
    return binary


def python_with_yaml(work):
    try:
        import yaml  # noqa: F401
        return sys.executable
    except ImportError:
        venv = work / "venv"
        run([sys.executable, "-m", "venv", str(venv)])
        python = venv / "bin" / "python3"
        run([str(python), "-m", "pip", "install", "--disable-pip-version-check", "PyYAML==6.0.3"])
        return str(python)


@contextlib.contextmanager
def local_environment(work):
    tools = require_postgres18()
    anvil = anvil_binary(work)
    python = python_with_yaml(work)
    cluster = work / "pgdata"
    log = work / "postgres.log"
    port = available_port()
    run([tools["initdb"], "-D", str(cluster), "-A", "trust", "-U", "txorch", "--no-instructions"])
    run([tools["pg_ctl"], "-D", str(cluster), "-l", str(log), "-o",
         f"-h 127.0.0.1 -p {port} -c unix_socket_directories=", "-w", "start"])
    try:
        admin_url = f"postgres://txorch@127.0.0.1:{port}/postgres?sslmode=disable"
        run([tools["psql"], admin_url, "-v", "ON_ERROR_STOP=1", "-c", "CREATE DATABASE txorch_phase7"])
        db_url = f"postgres://txorch@127.0.0.1:{port}/txorch_phase7?sslmode=disable"
        for migration in sorted((ROOT / "migrations").glob("*.sql")):
            run([tools["psql"], db_url, "-v", "ON_ERROR_STOP=1", "-f", str(migration)])
        env = os.environ.copy()
        env.update(TXORCH_TEST_DATABASE_URL=db_url, TXORCH_ANVIL_BINARY=str(anvil),
                   TXORCH_ANVIL_VERSION=ANVIL_VERSION,
                   TXORCH_POSTGRES_VERSION=tool_version(tools["psql"]),
                   TXORCH_GO_VERSION=tool_version("go", "version"),
                   PATH=str(Path(python).parent) + os.pathsep + os.environ["PATH"])
        yield env
    finally:
        run([tools["pg_ctl"], "-D", str(cluster), "-m", "immediate", "-w", "stop"])


def validate_demo(path):
    data = json.loads(path.read_text())
    counts = data["counts"]
    if (data["result"], counts, data["winner_attempt"], data["final_state"],
        data["confirmation_depth"], data["fault_proxy_sends"],
        data["checksum_sha256"]) != (
        "PASS", {"intents": 1, "nonce_reservations": 1, "attempts": 2,
                 "canonical_receipts": 1, "orphaned_receipts": 1}, 1, "CONFIRMED", 3, 1,
        EXPECTED_DEMO_CHECKSUM):
        raise RuntimeError("Demo evidence violates the expected invariant set")
    if hashlib.sha256(data["checksum_input"].encode()).hexdigest() != EXPECTED_DEMO_CHECKSUM:
        raise RuntimeError("Demo checksum input is inconsistent")
    if len(data["steps"]) != 16 or len(data["attempt_hashes"]) != 2:
        raise RuntimeError("Demo skipped required steps")
    if data.get("operational") != {"status_http": 200, "readiness_http": 200, "metrics_http": 200}:
        raise RuntimeError("Demo operational endpoints did not pass")
    if any(not data.get("telemetry", {}).get(name) for name in ("spans", "metric_names", "log_events")):
        raise RuntimeError("Demo telemetry evidence missing")
    if not all(data.get("tool_versions", {}).get(name) for name in ("go", "postgres", "anvil")):
        raise RuntimeError("Demo tool versions missing")
    if data.get("source_identity") != source_identity():
        raise RuntimeError("Demo source identity differs from current tree")
    print(f"DEMO VERIFIED checksum={EXPECTED_DEMO_CHECKSUM}")


def validate_benchmark(path):
    data = json.loads(path.read_text())
    correct = data["correctness"]
    workload = data["workload"]
    environment = data["environment"]
    if data["result"] != "PASS" or any(correct[field] != expected for field, expected in {
        "intents": 12, "nonce_reservations": 12, "attempts": 15, "signed_attempts": 15,
        "confirmed": 12, "duplicate_active_nonces": 0,
        "checksum_sha256": EXPECTED_BENCHMARK_CHECKSUM}.items()):
        raise RuntimeError("Benchmark correctness gate failed")
    if hashlib.sha256(correct["checksum_input"].encode()).hexdigest() != EXPECTED_BENCHMARK_CHECKSUM:
        raise RuntimeError("Benchmark checksum input is inconsistent")
    for key in ("intents", "signers", "per_signer_intents", "normal_percent",
                "unknown_percent", "replacement_percent", "rpc_profile", "worker_count",
                "db_pool_max_connections", "rpc_concurrency"):
        if not workload.get(key):
            raise RuntimeError(f"Benchmark workload omits {key}")
    for key in ("os", "architecture", "logical_cpus", "cpu_model", "ram_bytes", "go_version",
                "postgres_version", "anvil_version"):
        if not environment.get(key):
            raise RuntimeError(f"Benchmark environment omits {key}")
    if any(value <= 0 for value in data["latency_ms"].values()) or any(
        value <= 0 for value in data["throughput"].values()):
        raise RuntimeError("Benchmark timing/throughput missing")
    if data.get("source_identity") != source_identity():
        raise RuntimeError("Benchmark source identity differs from current tree")
    print(f"BENCHMARK VERIFIED checksum={EXPECTED_BENCHMARK_CHECKSUM}")
    return data


def demo(env, artifacts):
    path = artifacts / "demo-result.json"
    env = env | {"TXORCH_DEMO_RESULT": str(path)}
    run(["go", "test", "./internal/orchestrator", "-run", "^TestPhase7Demo$", "-count=1", "-v"], env=env)
    data = json.loads(path.read_text())
    data["tool_versions"] = {"go": env["TXORCH_GO_VERSION"], "postgres": env["TXORCH_POSTGRES_VERSION"],
                             "anvil": env["TXORCH_ANVIL_VERSION"]}
    data["source_identity"] = source_identity()
    path.write_text(json.dumps(data, indent=2) + "\n")
    validate_demo(path)


def benchmark(env, artifacts, count):
    runs = []
    if platform.system() == "Darwin":
        cpu = run(["sysctl", "-n", "hw.model"], capture=True).stdout.strip()
        ram_bytes = int(run(["sysctl", "-n", "hw.memsize"], capture=True).stdout.strip())
    elif platform.system() == "Linux":
        cpu = next((line.partition(":")[2].strip() for line in Path("/proc/cpuinfo").read_text().splitlines()
                    if line.startswith(("model name", "Hardware"))), platform.machine())
        ram_bytes = int(next(line.split()[1] for line in Path("/proc/meminfo").read_text().splitlines()
                             if line.startswith("MemTotal:"))) * 1024
    else:
        raise RuntimeError("Unsupported benchmark environment")
    for index in range(count):
        path = artifacts / f"benchmark-run-{index+1}.json"
        run(["go", "test", "./internal/orchestrator", "-run", "^TestPhase7BenchmarkSmoke$",
             "-count=1", "-v"], env=env | {"TXORCH_BENCHMARK_RESULT": str(path)})
        data = json.loads(path.read_text())
        data["environment"].update(cpu_model=cpu, ram_bytes=ram_bytes)
        data["source_identity"] = source_identity()
        path.write_text(json.dumps(data, indent=2) + "\n")
        runs.append(validate_benchmark(path))
    median = sorted(run["throughput"]["intents_per_second"] for run in runs)[len(runs)//2]
    aggregate = {"runs": runs, "summary": {"method": "median of all runs, no exclusions",
                                           "median_intents_per_second": median}}
    (artifacts / "benchmark-runs.json").write_text(json.dumps(aggregate, indent=2) + "\n")
    print(f"BENCHMARK {count} RUNS median_intents_per_second={median:.2f}")


def service_smoke(env, work, binary):
    rpc_port, http_port = available_port(), available_port()
    rpc_url = f"http://127.0.0.1:{rpc_port}"
    http_url = f"http://127.0.0.1:{http_port}"
    anvil_log = (work / "anvil.log").open("wb")
    service_log = (work / "service.log").open("wb")
    anvil = subprocess.Popen([env["TXORCH_ANVIL_BINARY"], "--host", "127.0.0.1", "--port", str(rpc_port),
                              "--chain-id", "31337", "--silent"], stdout=anvil_log, stderr=subprocess.STDOUT)
    service = None
    try:
        deadline = time.monotonic() + 10
        while True:
            try:
                request = urllib.request.Request(rpc_url, data=json.dumps({"jsonrpc": "2.0", "id": 1,
                    "method": "anvil_setBalance", "params": ["0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf",
                                                         "0x3635c9adc5dea00000"]}).encode(),
                    headers={"Content-Type": "application/json"})
                with urllib.request.urlopen(request, timeout=1) as response:
                    if "result" in json.load(response):
                        break
            except (OSError, ValueError):
                if time.monotonic() >= deadline or anvil.poll() is not None:
                    raise RuntimeError("Anvil did not start for binary service smoke")
                time.sleep(0.05)
        service_env = env | {"TXORCH_DEV_MODE": "1", "TXORCH_DATABASE_URL": env["TXORCH_TEST_DATABASE_URL"],
                             "TXORCH_RPC_URL": rpc_url, "TXORCH_CHAIN_ID": "31337", "TXORCH_SIGNER_ID": "local",
                             "TXORCH_DEV_PRIVATE_KEY": "0" * 63 + "1", "TXORCH_API_TOKEN": "local-binary-smoke-token",
                             "TXORCH_TENANT_ID": "binary-smoke", "TXORCH_HTTP_ADDR": f"127.0.0.1:{http_port}"}
        service = subprocess.Popen([str(binary), "serve"], env=service_env, stdout=service_log,
                                   stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 10
        while True:
            try:
                with urllib.request.urlopen(http_url + "/health/ready", timeout=1) as response:
                    if response.status == 200 and json.load(response)["ready"]:
                        break
            except (OSError, ValueError):
                if time.monotonic() >= deadline or service.poll() is not None:
                    raise RuntimeError("Binary service did not become ready")
                time.sleep(0.05)
        payload = json.dumps({"chain_id": "31337", "signer_id": "local",
                              "to": "0x1111111111111111111111111111111111111111", "value_wei": "1",
                              "calldata": "0x", "gas_policy": {"profile": "standard"}}).encode()
        headers = {"Content-Type": "application/json", "Authorization": "Bearer local-binary-smoke-token",
                   "Idempotency-Key": "binary-smoke"}
        request = urllib.request.Request(http_url + "/v1/transactions", data=payload, headers=headers)
        with urllib.request.urlopen(request, timeout=15) as response:
            created = json.load(response)
            if response.status != 201 or not created["attempts"]:
                raise RuntimeError("Binary HTTP admission failed")
        with urllib.request.urlopen(request, timeout=15) as response:
            replay = json.load(response)
            if response.status != 200 or replay["intent_id"] != created["intent_id"]:
                raise RuntimeError("Binary HTTP idempotent replay failed")
        get = urllib.request.Request(http_url + "/v1/transactions/" + created["intent_id"],
                                     headers={"Authorization": "Bearer local-binary-smoke-token"})
        with urllib.request.urlopen(get, timeout=15) as response:
            if json.load(response)["intent_id"] != created["intent_id"]:
                raise RuntimeError("Binary HTTP GET failed")
        print("BINARY SERVICE VERIFIED ready, POST, replay and GET")
    finally:
        if service is not None:
            service.terminate()
            try:
                service.wait(timeout=12)
            except subprocess.TimeoutExpired:
                service.kill()
                service.wait()
        anvil.terminate()
        try:
            anvil.wait(timeout=5)
        except subprocess.TimeoutExpired:
            anvil.kill()
            anvil.wait()
        service_log.close()
        anvil_log.close()
        if service is not None and service.returncode not in (0, -15):
            raise RuntimeError(f"Binary service exit {service.returncode}: " +
                               (work / "service.log").read_text(errors="replace")[-2000:])


def source_files():
    for parent, directories, names in os.walk(ROOT):
        directories[:] = [name for name in directories if (not name.startswith(".") or name == ".github")
                        and name != "dist"]
        for name in names:
            path = Path(parent) / name
            relative = path.relative_to(ROOT)
            if relative.parts[0] == "artifacts":
                continue
            if name == "txorch" or (path.suffix not in (".go", ".mod", ".sum", ".py", ".md", ".sql", ".yaml")
                                     and name not in ("Makefile", "Dockerfile", ".dockerignore",
                                                      ".gitignore", ".env.example", "LICENSE",
                                                      "THIRD_PARTY_NOTICES.txt")):
                continue
            yield path


def source_identity():
    return run([sys.executable, "scripts/source_identity.py"], capture=True).stdout.strip()


def clean_copy(env):
    with tempfile.TemporaryDirectory(prefix=".phase7-clean-", dir=ROOT) as directory:
        copy = Path(directory)
        for source in source_files():
            target = copy / source.relative_to(ROOT)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
        copied_identity = run([sys.executable, "scripts/source_identity.py"], cwd=copy, capture=True).stdout.strip()
        if copied_identity != source_identity():
            raise RuntimeError("Clean source copy differs from the source-tree identity")
        child = os.environ.copy()
        for name in ("TXORCH_TEST_DATABASE_URL", "TXORCH_ANVIL_BINARY", "TXORCH_ANVIL_VERSION"):
            child.pop(name, None)
        child["GOMODCACHE"] = str(copy / ".cache" / "go-mod")
        child["GOCACHE"] = str(copy / ".cache" / "go-build")
        child["PIP_NO_CACHE_DIR"] = "1"
        run([sys.executable, "scripts/phase7_local.py", "clone-check"], env=child, cwd=copy)
    print("CLEAN COPY VERIFIED without parent database, Anvil binary or generated files")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=("demo", "benchmark", "verify", "clean-clone", "clone-check"))
    target = parser.parse_args().target
    if target == "clean-clone":
        clean_copy(None)
        return
    artifacts = ROOT / "artifacts"
    artifacts.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".phase7-run-", dir=ROOT) as directory:
        with local_environment(Path(directory)) as env:
            if target in ("verify", "clone-check"):
                run(["make", "verify-phase6"], env=env)
                run(["python3", "scripts/verify_phase7.py"], env=env)
                binary = Path(directory) / "txorch"
                run(["go", "build", "-ldflags", f"-X main.sourceIdentity={source_identity()}",
                     "-o", str(binary), "./cmd/txorch"], env=env)
                service_smoke(env, Path(directory), binary)
            if target in ("demo", "verify", "clone-check"):
                demo(env, artifacts)
            if target in ("benchmark", "verify", "clone-check"):
                benchmark(env, artifacts, 3 if target != "clone-check" else 1)
            if target == "verify":
                run(["docker", "build", "--build-arg", f"SOURCE_IDENTITY={source_identity()}",
                     "-t", "txorch:phase7-local", "."], env=env)
                version = run(["docker", "run", "--rm", "txorch:phase7-local", "version"],
                              env=env, capture=True).stdout.strip()
                if f"source={source_identity()}" not in version:
                    raise RuntimeError("Docker binary does not expose the source-tree identity")
                user = run(["docker", "image", "inspect", "-f", "{{.Config.User}}",
                            "txorch:phase7-local"], env=env, capture=True).stdout.strip()
                if user != "65532:65532":
                    raise RuntimeError(f"Docker runtime user is not non-root: {user}")
    if target == "verify":
        clean_copy(None)
    print(f"PHASE7 {target.upper()} PASS")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, subprocess.CalledProcessError, OSError, KeyError, ValueError) as error:
        sys.exit(str(error))
