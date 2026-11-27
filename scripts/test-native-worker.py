#!/usr/bin/env python3
"""Run the native command worker with a temporary isolated Linux workspace."""

import base64
import errno
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time


def secret():
    return base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()


def signal_gate(path, signal, deadline):
    while True:
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_NONBLOCK)
            break
        except OSError as error:
            if error.errno != errno.ENXIO or time.monotonic() >= deadline:
                raise
            time.sleep(0.02)
    with os.fdopen(descriptor, "w") as stream:
        stream.write(signal + "\n")


def verify_effect(path, deadline):
    while True:
        try:
            records = path.read_text().splitlines()
        except FileNotFoundError:
            records = []
        if records == ["effect"]:
            return
        if records or time.monotonic() >= deadline:
            raise RuntimeError(f"unexpected workspace records: {records!r}")
        time.sleep(0.02)


def main():
    request = json.loads(sys.stdin.readline())
    bin_dir = Path(os.environ["GESANG_TEST_NATIVE_WORKER_BIN_DIR"]).resolve(strict=True)
    launcher = shutil.which("bwrap")
    if not launcher:
        raise RuntimeError("Bubblewrap is required for the native worker")
    launcher = str(Path(launcher).resolve(strict=True))
    root = Path(tempfile.mkdtemp(prefix="gesang-physical-"))
    private = root / "private"
    workspace = root / "workspace"
    private.mkdir(mode=0o700)
    workspace.mkdir(mode=0o700)
    release_path = workspace / "release"
    os.mkfifo(release_path, mode=0o600)
    config = {
        "version": 1,
        "worker": request["worker"],
        "scope": request["scope"],
        "domain": request["domain"],
        "policyRevision": "fixture",
        "callbackBase": request["callbackBase"],
        "listen": f'127.0.0.1:{request["workerPort"]}',
        "namespace": {
            "launcher": launcher,
            "gate": str(bin_dir / "chopin-job-gate"),
            "capture": str(bin_dir / "chopin-job-capture"),
            "privateRoot": str(private),
            "workspace": str(workspace),
            "readOnly": [p for p in ("/usr", "/bin", "/lib", "/lib64") if Path(p).exists()],
            "network": "isolated",
            "environment": {},
        },
        "service": {"maxBodyBytes": 4096, "recoveryPageSize": 16, "recoveryIntervalMs": 25},
        "requestTimeoutMs": request["timeoutMs"],
        "responseBytes": 4096,
        "retryAfterMs": 20,
    }
    config_path = root / "worker.json"
    config_path.write_text(json.dumps(config))
    environment = {
        "PATH": "/usr/bin:/bin",
        "CHOPIN_WORKER_TOKEN": request["registration"],
        "CHOPIN_WORKER_JOURNAL_KEY": secret(),
        "CHOPIN_WORKER_OUTPUT_KEY": secret(),
    }
    executable = bin_dir / "chopin-job-worker"
    subprocess.run(
        [executable, "init", "--config", config_path],
        check=True,
        env=environment,
        stdout=sys.stderr,
    )
    print(f"Linux proof directory: {root}", file=sys.stderr)
    with subprocess.Popen(
        [executable, "serve", "--config", config_path],
        env=environment,
        stdout=subprocess.PIPE,
        text=True,
    ) as worker:
        try:
            ready = worker.stdout.readline()
            if not ready or json.loads(ready).get("state") != "listening":
                raise RuntimeError("worker exited before readiness")
            print(json.dumps({"state": "ready"}), flush=True)
            for line in sys.stdin:
                deadline = time.monotonic() + request["timeoutMs"] / 1000
                match line.strip():
                    case "release":
                        signal_gate(release_path, "continue", deadline)
                        print("released", flush=True)
                    case "effect":
                        verify_effect(workspace / "effect", deadline)
                        print("once", flush=True)
                    case _:
                        raise RuntimeError("unknown operation")
        finally:
            try:
                signal_gate(release_path, "cancel", time.monotonic())
            except OSError as error:
                if error.errno not in (errno.ENXIO, errno.EPIPE):
                    raise
            if worker.poll() is None:
                worker.terminate()
                try:
                    worker.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    worker.kill()


if __name__ == "__main__":
    main()
