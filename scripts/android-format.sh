#!/usr/bin/env bash
set -euo pipefail

repository_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
tools_root="${XDG_DATA_HOME:-$HOME/.local/share}/gesang/downloads"
formatter="$tools_root/ktfmt-0.64-with-dependencies.jar"
mode="${1:-write}"
if [[ "$mode" != "write" && "$mode" != "--check" ]]; then
    printf '%s\n' 'Usage: scripts/android-format.sh [--check]' >&2
    exit 1
fi

python3 "$repository_root/scripts/android-fetch.py" \
    https://repo.maven.apache.org/maven2/com/facebook/ktfmt/0.64/ktfmt-0.64-with-dependencies.jar \
    71428314 \
    2661245de6b98b5a42f1c39c0cdccf48d7457483c310aba49eb7db7c51f94b026a11939b1cb7b2c7c0554bf5328119a24c075dac91340f0ae1a4a123baa0db83 \
    "$formatter"

cd "$repository_root"
python3 - "$formatter" "$mode" <<'PY'
import os
from pathlib import Path
import subprocess
import sys

formatter, mode = sys.argv[1:]
sources = subprocess.check_output([
    "rg", "--files", "apps/android", "-g", "*.kt", "-g", "*.kts", "-g", "!**/build/**",
], text=True).splitlines()
java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if "JAVA_HOME" in os.environ else "java"
command = [java, "-jar", formatter, "--kotlinlang-style"]
if mode == "--check":
    command += ["--dry-run", "--set-exit-if-changed"]
raise SystemExit(subprocess.run(command + sources).returncode)
PY
