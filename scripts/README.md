# Platform build steps

Repository automation is exposed through `cargo xtask`. Keep shell and download
helpers here only where they bridge platform tools; do not put an independent task
graph or duplicate validation rules in this directory.

| Entry point | Platform helper |
| --- | --- |
| `cargo xtask android-setup` | `android-setup.sh` installs and verifies the native toolchain |
| `cargo xtask android-native` | `android-native.sh` generates bindings and cross-compiles native libraries |

`android-fetch.py` is a download implementation used by toolchain setup. It is not a
second user-facing command interface. Versions and integrity metadata must come
from official release sources and remain synchronized with the native build.

Local and CI checks call the same task entry points. Destructive cleanup commands
are not part of the task interface.
