# Contributing

Discuss changes that alter public contracts, trust boundaries, storage formats, or execution ownership before implementation. Keep unrelated changes separate.

Use English for source comments, API documentation, diagnostic messages, and contributor documentation. Put translated product text in localization resources. Document intent and invariants where they are not obvious; avoid comments that merely repeat the code.

Public contracts must distinguish acceptance from completion, cancellation requests from confirmed stopping, and disconnected clients from inactive cloud work. Preserve stable request identifiers and additive response compatibility. Changes to a wire format require shared cross-language fixtures and a compatibility check.

Before opening a change, run formatting, strict static checks, relevant unit tests, and the affected integration tests. Changes to persistence require a real database test, including interrupted or competing operations where relevant. Changes to native interfaces require an actual native build and accessibility review. Report omitted checks and their reasons explicitly.

Use `cargo xtask help` to see the shared development entry points. `cargo xtask check`
runs the non-integration checks; `cargo xtask test-store` explicitly runs real store
checks against the database supplied by the developer. Native platform steps are
listed separately so a server-only contributor does not install a mobile toolchain
by accident.

Never include credentials, personal records, developer-specific paths, or fabricated runtime evidence. Do not add a dependency until its maintenance, license, release, and necessity have been checked. Commit the application lockfile and verify locked builds.

Contributions are licensed under the project's Apache-2.0 license. Sign commits using the Developer Certificate of Origin and do not add attribution trailers for tools.
