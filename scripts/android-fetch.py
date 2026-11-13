#!/usr/bin/env python3
"""Fetch a complete verified tool archive using the publisher's range support."""

import concurrent.futures
import hashlib
import math
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid


def fetch(url: str, size: int, digest: str, destination: Path) -> None:
    """Never install an archive until every byte and its published digest agree."""
    algorithms = {40: "sha1", 64: "sha256", 128: "sha512"}
    algorithm = algorithms.get(len(digest))
    if algorithm is None:
        raise ValueError("A published SHA-1, SHA-256, or SHA-512 digest is required.")
    if destination.is_file():
        with destination.open("rb") as existing:
            if hashlib.file_digest(existing, algorithm).hexdigest() == digest:
                return
        raise RuntimeError("An existing archive has a different digest; select another destination.")
    destination.parent.mkdir(parents=True, exist_ok=True)
    parts = destination.parent / (destination.name + ".parts-" + uuid.uuid4().hex)
    parts.mkdir()
    workers = os.cpu_count() or 1
    part_size = math.ceil(size / workers)

    def transfer(index: int) -> Path:
        start = index * part_size
        end = min(start + part_size, size) - 1
        path = parts / str(index)
        subprocess.run([
            "curl", "--fail", "--location", "--silent", "--show-error",
            "--range", f"{start}-{end}", "--output", str(path), url,
        ], check=True)
        if path.stat().st_size != end - start + 1:
            raise RuntimeError("The server did not return the complete requested byte range.")
        return path

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        files = list(executor.map(transfer, range(math.ceil(size / part_size))))
    assembled = parts / "complete"
    with assembled.open("wb") as output:
        for path in files:
            with path.open("rb") as source:
                shutil.copyfileobj(source, output)
        output.flush()
        os.fsync(output.fileno())
    with assembled.open("rb") as source:
        actual_digest = hashlib.file_digest(source, algorithm).hexdigest()
    if actual_digest != digest:
        raise RuntimeError("The archive does not match the publisher's digest.")
    assembled.rename(destination)


if __name__ == "__main__":
    if len(sys.argv) != 5:
        raise SystemExit("Usage: android-fetch.py URL SIZE DIGEST DESTINATION")
    fetch(sys.argv[1], int(sys.argv[2]), sys.argv[3], Path(sys.argv[4]))
