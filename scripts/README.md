# Platform build steps

Repository automation is exposed through `cargo xtask`. Keep shell and download
helpers here only where they bridge platform tools; do not put an independent task
graph or duplicate validation rules in this directory.

| Entry point | Platform helper |
| --- | --- |
| `cargo xtask android-setup` | `android-setup.sh` installs and verifies the native toolchain |
| `cargo xtask android-native` | `android-native.sh` generates bindings and cross-compiles native libraries |
| `cargo xtask test-engine` | `test-engine.py` starts the real execution fixture and runs the host protocol probe |
| `cargo xtask test-native-command-goals` | `test-native-worker.py` starts a native worker for the protected goal recovery probe |
| `cargo xtask test-native-chat` | The protected browser integration probe uses the same worker runner and the demo's existing web entry |

`android-fetch.py` is a download implementation used by toolchain setup. It is not a
second user-facing command interface. Versions and integrity metadata must come
from official release sources and remain synchronized with the native build.

Local and CI checks call the same task entry points. Destructive cleanup commands
are not part of the task interface.
