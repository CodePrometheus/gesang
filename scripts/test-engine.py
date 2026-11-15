"""Run the real execution service and host bridge against an isolated database schema."""

import asyncio
import json
import os
from pathlib import Path
import subprocess
import tempfile


def required(name):
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"{name} is required")
    return value


async def main():
    root = Path(__file__).resolve().parent.parent
    fixture = Path(required("GESANG_TEST_ENGINE_FIXTURE")).resolve(strict=True)
    required("GESANG_TEST_DATABASE_URL")
    milliseconds = int(required("GESANG_TEST_OPERATION_TIMEOUT_MS"))
    if milliseconds <= 0:
        raise ValueError("GESANG_TEST_OPERATION_TIMEOUT_MS must be positive")
    timeout = milliseconds / 1000
    artifacts = Path(required("GESANG_TEST_ARTIFACT_DIR")).resolve()
    artifacts.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="engine-", dir=artifacts))
    cargo = os.environ.get("CARGO", "cargo")
    subprocess.run(
        [cargo, "build", "--locked", "-p", "gesang-host", "--example", "engine-probe"],
        cwd=root,
        check=True,
    )
    env = {
        **os.environ,
        "CHOPIN_PROTOCOL_FIXTURE_DIR": str(directory / "service"),
        "CHOPIN_HOME": str(directory / "home"),
    }
    report = {"passed": False, "provider": "deterministic fixture"}
    print(f"Integration evidence: {directory}", flush=True)
    with (directory / "service.log").open("w") as service_log:
        service = await asyncio.create_subprocess_exec(
            "node", str(fixture), cwd=fixture.parent, env=env,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=service_log,
        )
        probe = None
        try:
            async def receive():
                line = await asyncio.wait_for(service.stdout.readline(), timeout)
                if not line:
                    raise RuntimeError("execution fixture exited; inspect service.log")
                return json.loads(line)

            async def send(operation):
                service.stdin.write((json.dumps({"op": operation}) + "\n").encode())
                await asyncio.wait_for(service.stdin.drain(), timeout)

            ready = await receive()
            await send("goal-wait")
            if (await receive()).get("event") != "goal-wait":
                raise RuntimeError("execution fixture did not enter its question scenario")
            env.update({
                "CHOPIN_TEST_URL": ready["url"],
                "CHOPIN_TEST_CREDENTIAL": ready["credential"],
            })
            with (directory / "host.log").open("w") as host_log:
                probe = await asyncio.create_subprocess_exec(
                    cargo, "run", "--locked", "-p", "gesang-host", "--example", "engine-probe",
                    cwd=root, env=env, stdout=host_log, stderr=asyncio.subprocess.STDOUT,
                )
                # Individual RPCs and observations enforce the configured operation deadline.
                report["host_exit_code"] = await probe.wait()
            if report["host_exit_code"] != 0:
                raise RuntimeError("host integration failed; inspect host.log")
            await send("stop")
            service.stdin.close()
            report["service_exit_code"] = await asyncio.wait_for(service.wait(), timeout)
            if report["service_exit_code"] != 0:
                raise RuntimeError("execution fixture shutdown failed; inspect service.log")
            report["passed"] = True
        finally:
            # These are the exact child handles created by this test, never existing services.
            for child in [probe, service]:
                if child is not None and child.returncode is None:
                    child.kill()
                    await child.wait()
            (directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print("Host execution integration passed", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
