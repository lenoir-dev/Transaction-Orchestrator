#!/usr/bin/env python3
"""Require the database integration target to be PostgreSQL 18."""

import os
import subprocess
import sys

url = os.environ.get("TXORCH_TEST_DATABASE_URL")
if not url:
    sys.exit("TXORCH_TEST_DATABASE_URL must reference a disposable PostgreSQL 18 database")

try:
    result = subprocess.run(
        ["psql", url, "-Atqc", "SHOW server_version_num"],
        check=True, capture_output=True, text=True, timeout=10,
    )
    version = int(result.stdout.strip())
except (OSError, ValueError, subprocess.CalledProcessError, subprocess.TimeoutExpired):
    sys.exit("could not verify the PostgreSQL test server version")

if not 180000 <= version < 190000:
    sys.exit(f"PostgreSQL 18 required; server_version_num={version}")
print(f"PostgreSQL 18 integration target verified: server_version_num={version}")
