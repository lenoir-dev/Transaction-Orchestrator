#!/usr/bin/env python3
"""Print the deterministic identity of the public source tree, without Git."""

import hashlib
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
EXTENSIONS = {".go", ".mod", ".sum", ".py", ".md", ".sql", ".yaml"}
NAMES = {"Makefile", "Dockerfile", ".dockerignore", ".gitignore", ".env.example", "LICENSE"}


def paths():
    selected = []
    for path in ROOT.rglob("*"):
        if not path.is_file():
            continue
        relative = path.relative_to(ROOT)
        if any(part in {"artifacts", "dist"} or (part.startswith(".") and part != ".github")
               for part in relative.parts[:-1]):
            continue
        if path.suffix in EXTENSIONS or path.name in NAMES:
            selected.append(path)
    return sorted(selected, key=lambda path: path.relative_to(ROOT).as_posix())


def digest():
    value = hashlib.sha256()
    for path in paths():
        content = path.read_bytes()
        value.update(path.relative_to(ROOT).as_posix().encode())
        value.update(b"\0")
        value.update(str(len(content)).encode())
        value.update(b"\0")
        value.update(content)
    return value.hexdigest()


if __name__ == "__main__":
    sys.stdout.write(digest() + "\n")
