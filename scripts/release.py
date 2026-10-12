#!/usr/bin/env python3
"""Build and check unsigned local release evidence for one source tree."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
IMAGE = "txorch:phase8-local"
POSTGRES_IMAGE = "postgres:18.6@sha256:fc973eb97c9fd04bfa1840e0f510719a584ccb3be8debfe6a4144637a9dfe8cf"
REQUIRED = ("go", "postgres", "psql", "initdb", "pg_ctl", "docker", "syft", "trivy", "govulncheck")
PROCESS_TOKEN_HASHES = {"60965168ce762e94", "57de4cf40144bdf7", "7d3194f79e645c42",
                        "487b91042c7cf27a", "cf07194ee232eb53", "eb7526362ee7678c",
                        "a2343fdcb9d59dc2"}


def run(*args, cwd=ROOT, capture=False, env=None):
    print("+", " ".join(str(arg) for arg in args), flush=True)
    return subprocess.run(args, cwd=cwd, env=env, text=True, check=True,
                          stdout=subprocess.PIPE if capture else None)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def source_identity():
    return run(sys.executable, "scripts/source_identity.py", capture=True).stdout.strip()


def docker_health_smoke():
    arch = {"arm64": "arm64", "aarch64": "arm64", "x86_64": "amd64"}.get(platform.machine())
    digests = {"amd64": "6c66ffcc55fa4249197721baa3098bc208014ea1d8aa04b2ed50ac6bccffb226",
               "arm64": "5bdce3dade8b6f0a65d4fccf6908426aee4905a988b08dce79d0a72fe7498310"}
    if arch not in digests:
        raise RuntimeError("Docker health fixture has no pinned Anvil for this architecture")
    name = "txorch-phase8-smoke-" + uuid.uuid4().hex[:12]
    postgres = None
    service = None
    with tempfile.TemporaryDirectory(prefix=".phase8-docker-", dir=ROOT) as directory:
        work = Path(directory)
        archive = work / "foundry.tar.gz"
        url = f"https://github.com/foundry-rs/foundry/releases/download/v1.8.5/foundry_v1.8.5_linux_{arch}.tar.gz"
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=180) as response, archive.open("wb") as output:
                    shutil.copyfileobj(response, output)
                if sha(archive) != digests[arch]:
                    raise RuntimeError("Docker health Anvil archive digest mismatch")
                break
            except OSError:
                if attempt == 2:
                    raise
                time.sleep(2 ** attempt)
        with tarfile.open(archive, "r:gz") as tar:
            anvil_member = next((item for item in tar.getmembers() if item.name == "anvil" and item.isfile()), None)
            forge_member = next((item for item in tar.getmembers() if item.name == "forge" and item.isfile()), None)
            if not anvil_member or not forge_member:
                raise RuntimeError("official Foundry archive lacks Anvil or Forge")
            for item in (anvil_member, forge_member):
                with tar.extractfile(item) as source, (work / item.name).open("wb") as output:
                    shutil.copyfileobj(source, output)
                (work / item.name).chmod(0o755)
        try:
            # Let Docker reserve the host port; probing and releasing a socket races
            # with other local gates before this container starts.
            postgres = run("docker", "run", "--rm", "-d", "--name", name,
                           "-p", "127.0.0.1::8080", "-e", "POSTGRES_USER=txorch",
                           "-e", "POSTGRES_PASSWORD=local-fixture-only", "-e", "POSTGRES_DB=txorch",
                           POSTGRES_IMAGE, capture=True).stdout.strip()
            published = run("docker", "port", name, "8080/tcp", capture=True).stdout.strip()
            if not published.startswith("127.0.0.1:") or not published.rsplit(":", 1)[1].isdigit():
                raise RuntimeError("Docker did not publish the smoke service on loopback")
            host_port = int(published.rsplit(":", 1)[1])
            deadline = time.monotonic() + 30
            while subprocess.run(["docker", "exec", name, "pg_isready", "-h", "127.0.0.1",
                                  "-U", "txorch", "-d", "txorch"],
                                 cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
                if time.monotonic() > deadline:
                    raise RuntimeError("Docker PostgreSQL 18.6 did not become ready")
                time.sleep(.1)
            version = run("docker", "exec", name, "postgres", "--version", capture=True).stdout.strip()
            if "18.6" not in version:
                raise RuntimeError("Docker PostgreSQL version mismatch")
            for migration in sorted((ROOT / "migrations").glob("*.sql")):
                subprocess.run(["docker", "exec", "-e", "PGPASSWORD=local-fixture-only", "-i", name,
                                "psql", "-h", "127.0.0.1", "-U", "txorch", "-d", "txorch",
                                "-v", "ON_ERROR_STOP=1"], input=migration.read_bytes(), cwd=ROOT,
                               stdout=subprocess.DEVNULL, check=True)
            run("docker", "cp", str(work / "anvil"), f"{name}:/tmp/anvil")
            run("docker", "cp", str(work / "forge"), f"{name}:/tmp/forge")
            run("docker", "exec", name, "chmod", "755", "/tmp/anvil")
            run("docker", "exec", name, "chmod", "755", "/tmp/forge")
            anvil_version = run("docker", "exec", name, "/tmp/anvil", "--version", capture=True).stdout.strip()
            forge_version = run("docker", "exec", name, "/tmp/forge", "--version", capture=True).stdout.strip()
            if "1.8.5" not in anvil_version or "1.8.5" not in forge_version:
                raise RuntimeError("Docker Foundry version mismatch")
            run("docker", "exec", "-d", name, "/tmp/anvil", "--host", "127.0.0.1", "--port", "8545",
                "--chain-id", "31337", "--silent")
            db_url = "postgres://txorch:local-fixture-only@127.0.0.1:5432/txorch?sslmode=disable"
            # This public, deterministic key is only for the isolated local fixture.
            key = "0" * 63 + "1"
            service = run("docker", "run", "--rm", "-d", "--network", f"container:{name}",
                          "-e", "TXORCH_DEV_MODE=1", "-e", f"TXORCH_DATABASE_URL={db_url}",
                          "-e", "TXORCH_RPC_URL=http://127.0.0.1:8545", "-e", "TXORCH_CHAIN_ID=31337",
                          "-e", "TXORCH_SIGNER_ID=local", "-e", f"TXORCH_DEV_PRIVATE_KEY={key}",
                          "-e", "TXORCH_API_TOKEN=local-docker-smoke-token", "-e", "TXORCH_TENANT_ID=docker-smoke",
                          "-e", "TXORCH_HTTP_ADDR=0.0.0.0:8080", IMAGE, "serve", capture=True).stdout.strip()
            deadline = time.monotonic() + 30
            while True:
                try:
                    with urllib.request.urlopen(f"http://127.0.0.1:{host_port}/health/ready", timeout=1) as response:
                        if response.status == 200 and json.load(response).get("ready"):
                            break
                except (OSError, ValueError, urllib.error.HTTPError):
                    pass
                if time.monotonic() > deadline:
                    logs = run("docker", "logs", service, capture=True).stdout[-1500:]
                    raise RuntimeError("Docker service readiness failed: " + logs)
                time.sleep(.1)
            with urllib.request.urlopen(f"http://127.0.0.1:{host_port}/health/live", timeout=1) as response:
                if response.status != 200:
                    raise RuntimeError("Docker liveness failed")
            print("DOCKER HEALTH PASS live=200 ready=200 user=65532:65532", version,
                  anvil_version.splitlines()[0], forge_version.splitlines()[0])
            return {"result": "PASS", "liveness_http": 200, "readiness_http": 200,
                    "postgres_version": version, "anvil_version": anvil_version,
                    "forge_version": forge_version}
        finally:
            if service:
                subprocess.run(["docker", "stop", service], cwd=ROOT, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)
            if postgres:
                subprocess.run(["docker", "stop", postgres], cwd=ROOT, stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL)


def public_files():
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(ROOT)
        if any(part in {".git", "dist", ".cache"} or part.startswith(".phase7-")
               or part.startswith(".phase8-") or part == "__pycache__" for part in relative.parts):
            continue
        if relative.parts[0] == "artifacts" and len(relative.parts) == 2:
            continue
        yield path


def preflight():
    for name in REQUIRED:
        if not shutil.which(name):
            raise RuntimeError(f"release tool missing: {name}")
    versions = {name: scanner_version(name) if name == "govulncheck" else
                run(name, "version" if name == "go" else "--version", capture=True).stdout.splitlines()[0]
                for name in ("go", "postgres", "psql", "initdb", "pg_ctl", "trivy", "syft", "govulncheck")}
    if "go1.27.2" not in versions["go"]:
        raise RuntimeError("Go 1.27.2 required")
    if any("18.6" not in versions[name] for name in ("postgres", "psql", "initdb", "pg_ctl")):
        raise RuntimeError("PostgreSQL 18.6 required")
    if "0.75.0" not in versions["trivy"] or "1.54.0" not in versions["syft"]:
        raise RuntimeError("Pinned Trivy 0.75.0 and Syft 1.54.0 required")
    if versions["govulncheck"] != "Scanner: govulncheck@v1.8.0":
        raise RuntimeError("Pinned govulncheck v1.8.0 required")
    for path in public_files():
        relative = path.relative_to(ROOT)
        if path.name == ".DS_Store" or path.suffix in {".log", ".zip", ".pyc"}:
            raise RuntimeError(f"excluded file in public tree: {relative}")
        if path.suffix not in {".go", ".py", ".md", ".yaml", ".sql", ".mod", ".sum", ".json", ".example"}:
            continue
        content = path.read_text(errors="replace")
        user_prefix = "/" + "Users" + "/"
        desktop_segment = "/" + "Desktop" + "/"
        if path.name != "release.py" and (
                re.search(re.escape(user_prefix) + r"[A-Za-z0-9_.-]+/", content) or desktop_segment in content):
            raise RuntimeError(f"developer path in {relative}")
        tokens = re.findall(r"[A-Za-z]+", content.lower())
        if any(hashlib.sha256(token.encode()).hexdigest()[:16] in PROCESS_TOKEN_HASHES for token in tokens):
            raise RuntimeError(f"process reference in {relative}")
    print("RELEASE PREFLIGHT PASS", json.dumps(versions, sort_keys=True))


def validate_sbom(path):
    bom = json.loads(path.read_text())
    if bom.get("bomFormat") != "CycloneDX" or not bom.get("specVersion") or not bom.get("components"):
        raise RuntimeError(f"incomplete CycloneDX SBOM: {path.name}")
    if not all(item.get("name") and item.get("type") and
               (item.get("version") or item.get("type") == "file") for item in bom["components"]):
        raise RuntimeError(f"invalid CycloneDX components: {path.name}")
    if ("/" + "Users" + "/") in path.read_text():
        raise RuntimeError(f"developer path in SBOM: {path.name}")
    return len(bom["components"])


def scanner_version(name):
    output = run(name, "-version", capture=True).stdout.splitlines()
    return next((line for line in output if line.startswith("Scanner:")), "unavailable")


def build():
    DIST.mkdir(exist_ok=True)
    identity = source_identity()
    for arch in ("amd64", "arm64"):
        output = DIST / f"txorch-linux-{arch}"
        builder = f"txorch:phase8-builder-{arch}"
        run("docker", "build", "--target", "build", "--build-arg", f"SOURCE_IDENTITY={identity}",
            "--build-arg", f"RELEASE_GOARCH={arch}", "-t", builder, ".")
        container = run("docker", "create", builder, capture=True).stdout.strip()
        try:
            run("docker", "cp", f"{container}:/out/txorch", str(output))
        finally:
            run("docker", "rm", container)
        details = run("go", "version", "-m", str(output), capture=True).stdout
        if f"GOARCH={arch}" not in details or "GOOS=linux" not in details:
            raise RuntimeError(f"wrong target: {output.name}")
        if ("/" + "Users" + "/") in output.read_bytes().decode("latin1"):
            raise RuntimeError(f"developer path in binary: {output.name}")
        sbom = DIST / f"sbom-linux-{arch}.cdx.json"
        run("syft", "scan", f"file:dist/{output.name}", "--source-name", output.name,
            "-o", f"cyclonedx-json={sbom}")
        data = json.loads(sbom.read_text())
        for component in data.get("components", []):
            if component.get("type") == "file" and component.get("name") == str(output):
                component["name"] = output.name
        sbom.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
        count = validate_sbom(sbom)
        print(f"SBOM VALID {sbom.name} components={count} sha256={sha(sbom)}")
    run("docker", "build", "--build-arg", f"SOURCE_IDENTITY={identity}", "-t", IMAGE, ".")
    user = run("docker", "image", "inspect", "-f", "{{.Config.User}}", IMAGE, capture=True).stdout.strip()
    if user != "65532:65532":
        raise RuntimeError(f"unexpected Docker runtime user {user}")
    version = run("docker", "run", "--rm", IMAGE, "version", capture=True).stdout.strip()
    if f"source={identity}" not in version:
        raise RuntimeError("Docker binary reports another source")
    health = docker_health_smoke()
    (DIST / "build-info.json").write_text(json.dumps({
        "source_identity": identity, "source_identity_kind": "deterministic-source-tree-sha256",
        "go_version": run("go", "version", capture=True).stdout.strip(),
        "build_host_os": platform.system().lower(), "build_host_arch": platform.machine(),
        "builder_image": "golang:1.27.2@sha256:5bc7f572bbaa98885a3a1fd9c0aa76b59e3e14e8628bfc316bbfd0c701e4818c",
        "targets": ["linux/amd64", "linux/arm64"],
        "docker_image": IMAGE, "docker_user": user, "docker_health": health,
        "build_commands": [
            "docker build --target build --build-arg SOURCE_IDENTITY=<source_identity> --build-arg RELEASE_GOARCH=<amd64|arm64> -t txorch:phase8-builder-<arch> .; docker cp /out/txorch dist/txorch-linux-<arch>",
            "docker build --build-arg SOURCE_IDENTITY=<source_identity> -t txorch:phase8-local .",
        ],
        "dependency_state": {"go_mod_sha256": sha(ROOT / "go.mod"),
                             "go_sum_sha256": sha(ROOT / "go.sum")},
        "artifacts_sha256": {p.name: sha(p) for p in sorted(DIST.glob("txorch-linux-*"))},
        "sbom_sha256": {p.name: sha(p) for p in sorted(DIST.glob("*.cdx.json"))},
    }, indent=2, sort_keys=True) + "\n")
    print("RELEASE BUILD PASS", identity)


def scan():
    DIST.mkdir(exist_ok=True)
    vuln = subprocess.run(["govulncheck", "./..."], cwd=ROOT, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    (DIST / "govulncheck.txt").write_text(vuln.stdout)
    if vuln.returncode and len(vuln.stdout) > 8000:
        print(vuln.stdout[:4000])
        print("... scanner output truncated; full output is in dist/govulncheck.txt ...")
    print(vuln.stdout[-4000:])
    if vuln.returncode:
        raise RuntimeError(f"govulncheck reported reachable findings or failed: {vuln.returncode}")
    for kind, command in (
        ("filesystem", ["trivy", "fs", "--scanners", "vuln,secret", "--skip-dirs", ".cache",
                        "--format", "json",
                        "--output", str(DIST / "trivy-filesystem.json"), "."]),
        ("image", ["trivy", "image", "--scanners", "vuln,secret", "--format", "json",
                   "--output", str(DIST / "trivy-image.json"), IMAGE]),
    ):
        run(*command)
        report = json.loads((DIST / f"trivy-{kind}.json").read_text())
        findings = [(item.get("Target"), v.get("VulnerabilityID"), v.get("Severity"))
                    for item in report.get("Results", []) for v in item.get("Vulnerabilities", [])]
        secrets = [(item.get("Target"), s.get("RuleID"))
                   for item in report.get("Results", []) for s in item.get("Secrets", [])]
        print(f"TRIVY {kind}: vulnerabilities={findings} secrets={secrets}")
        if secrets or any(severity in ("HIGH", "CRITICAL") for _, _, severity in findings):
            raise RuntimeError(f"unresolved {kind} scan finding")
    print("RELEASE SCAN PASS")


def clean_clone():
    if os.environ.get("TXORCH_RELEASE_CLONE") == "1":
        print("NESTED CLEAN CLONE SKIPPED")
        return
    if not (ROOT / ".git").exists():
        raise RuntimeError("clean-clone gate requires a committed local Git distribution")
    if run("git", "status", "--porcelain", capture=True).stdout.strip():
        raise RuntimeError("tracked distribution must be clean before clean-clone gate")
    began = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=".phase8-clone-", dir=ROOT) as directory:
        clone = Path(directory) / "repository"
        run("git", "clone", "--no-local", str(ROOT), str(clone))
        if run("git", "rev-parse", "HEAD", cwd=clone, capture=True).stdout.strip() != run(
                "git", "rev-parse", "HEAD", capture=True).stdout.strip():
            raise RuntimeError("clone is not at release HEAD")
        env = os.environ.copy()
        for key in ("TXORCH_TEST_DATABASE_URL", "TXORCH_ANVIL_BINARY", "TXORCH_ANVIL_VERSION"):
            env.pop(key, None)
        env["TXORCH_RELEASE_CLONE"] = "1"
        env["GOCACHE"] = str(clone / ".cache" / "go-build")
        env["GOMODCACHE"] = str(clone / ".cache" / "go-mod")
        run("make", "verify-release", cwd=clone, env=env)
        if run("git", "status", "--porcelain", cwd=clone, capture=True).stdout.strip():
            raise RuntimeError("clean clone gate dirtied tracked files")
    duration = round(time.monotonic() - began, 3)
    DIST.mkdir(exist_ok=True)
    (DIST / "clean-clone.json").write_text(json.dumps({"result": "PASS", "duration_seconds": duration,
        "source_identity": source_identity(),
        "git_head": run("git", "rev-parse", "HEAD", capture=True).stdout.strip()}, indent=2) + "\n")
    print("CLEAN CLONE PASS", duration, "seconds")


def finalize():
    identity = source_identity()
    build_info = json.loads((DIST / "build-info.json").read_text())
    if build_info["source_identity"] != identity:
        raise RuntimeError("build artifacts describe another source")
    for name, digest in build_info["artifacts_sha256"].items():
        if sha(DIST / name) != digest:
            raise RuntimeError(f"binary hash changed: {name}")
    for name, digest in build_info["sbom_sha256"].items():
        if sha(DIST / name) != digest:
            raise RuntimeError(f"SBOM hash changed: {name}")
        validate_sbom(DIST / name)
    for name in ("demo-result.json", "benchmark-runs.json"):
        data = json.loads((ROOT / "artifacts" / name).read_text())
        if data.get("source_identity", data.get("runs", [{}])[0].get("source_identity")) != identity:
            raise RuntimeError(f"evidence describes another source: {name}")
    clone = DIST / "clean-clone.json"
    if os.environ.get("TXORCH_RELEASE_CLONE") != "1" and (
            not clone.exists() or json.loads(clone.read_text())["source_identity"] != identity):
        raise RuntimeError("clean clone evidence missing or mismatched")
    provenance = {"kind": "unsigned local provenance", "signed": False,
        "source_identity": identity,
        "source_identity_kind": "deterministic-source-tree-sha256",
        "verification_gate": "make verify-release", "verification_result": "PASS",
        "build": build_info,
        "demo_sha256": sha(ROOT / "artifacts" / "demo-result.json"),
        "benchmark_sha256": sha(ROOT / "artifacts" / "benchmark-runs.json"),
        "scanner_versions": {name: scanner_version(name) if name == "govulncheck" else
                             run(name, "--version", capture=True).stdout.splitlines()[0]
                             for name in ("govulncheck", "trivy", "syft")},
        "scan_report_sha256": {p.name: sha(p) for p in sorted(DIST.glob("trivy-*.json"))},
        "clean_clone": json.loads(clone.read_text()) if clone.exists() else None,
    }
    output = DIST / "provenance.local.json"
    output.write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
    checksums = sorted(p for p in DIST.iterdir() if p.is_file() and p.name != "SHA256SUMS")
    (DIST / "SHA256SUMS").write_text("".join(f"{sha(p)}  {p.name}\n" for p in checksums))
    for line in (DIST / "SHA256SUMS").read_text().splitlines():
        digest, name = line.split("  ", 1)
        if sha(DIST / name) != digest:
            raise RuntimeError(f"checksum mismatch: {name}")
    print("RELEASE FINALIZED", identity)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=("preflight", "build", "scan", "clean-clone", "finalize"))
    target = parser.parse_args().target
    globals()[target.replace("-", "_")]()


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, OSError, subprocess.CalledProcessError, KeyError, ValueError) as error:
        sys.exit(str(error))
