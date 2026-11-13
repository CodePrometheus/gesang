# Rust conventions

The [official Rust Style Guide](https://doc.rust-lang.org/style-guide/) and
[Rust API Guidelines](https://rust-lang.github.io/api-guidelines/) are the baseline.
The [Google Rust guide](https://google.github.io/styleguide/rustguide.html) supplies
applicable design guidance; codebase-specific platform rules do not apply here.

Use named fields for nontrivial data, newtypes for identities that can be confused,
standard conversion traits, flexible borrowed inputs, and explicit state enums.
Keep constructors and error semantics predictable. Public library types need an
intentional extension strategy: private invariants, constructors, and
`#[non_exhaustive]` where future additions are expected. Wire types have a separate
versioned compatibility contract.

Prefer borrowing and direct ownership. Shared ownership and dynamic dispatch require
a real lifetime or heterogeneous service need; neither is categorically forbidden.
Keep application code direct instead of generalizing every helper into a public API.

Review abstractions by the responsibility they isolate and the duplication they
remove. Names should reveal domain meaning, and control flow should make ownership,
state transitions and failure paths easy to follow. Add extension points at real
host or plugin boundaries; every new layer must justify its maintenance and runtime
cost. Handle a shared cause in its owning module instead of accumulating caller
exceptions.

## Structure and ownership

Use one workspace dependency definition for shared dependencies and inherit workspace lint settings in every crate. Keep public modules and re-exports narrow. Prefer domain types and enums over booleans, unstructured strings, or shared mutable global state.

The application kernel must not import transport requests, business database clients, or infrastructure resource types. Adapters translate these at the boundary. Do not duplicate execution schemas or lifecycle truth to avoid depending on their owner.

## Formatting and documentation

Use the pinned stable toolchain and default formatter settings. Do not maintain a separate house formatting style. Order and wrap code with the formatter instead of manually aligning it.

All necessary comments and public API documentation are English. Document invariants, ownership, units, authorization, failure semantics, and non-obvious ordering. Do not narrate edits or repeat code. Every exported type, field, variant, trait, and method has a useful contract.

## Errors and effects

Libraries expose concrete error types. Preserve an uncertain external outcome as uncertainty; never turn it into an automatic retry of an unsafe operation. Error messages and logs must not disclose credentials or private command content.

Do not use `unwrap`, `expect`, `panic!`, `todo!`, or `unimplemented!` in production paths. A narrow exception requires an explicit invariant and a locally justified lint expectation. Test fixtures may unwrap setup and assertions, with the exemption restricted to test code.

Persist recovery intent before external effects. Keep SQL transactions short and do not hold a synchronous lock across an await. Spawned tasks have an owner, cancellation behavior, an observed result, and an explicit shutdown path. Dropping a future is not proof that an external action stopped.

## Data and performance

Borrow data where possible, share immutable state with explicit ownership, and avoid cloning whole histories or rebuilding clients on each call. Read only the columns and records needed. Stream or page large collections, preserving the source cursor and termination rule.

Resource and timeout values come from explicit configuration and verified product policy. Do not invent truncation, retry, or history limits to make checks pass. Evaluate optimizations under equivalent authorization, encryption, durability, and isolation.

## Dependencies and checks

Use the latest stable direct dependency releases, verify their source and license, and commit the lockfile. A breaking update requires adapting the code and running compatibility checks. Prereleases are not the default interpretation of latest.

Run `cargo fmt --all --check`, strict Clippy, documentation checks, and the relevant locked tests. Real persistence changes require real database tests. Shared protocol changes require common cross-language fixtures and an actual interoperability check. Review test assertions against the behavior claimed by their names.

Manual unsafe code is forbidden. Generated native bindings remain an explicit boundary and require their own compilation and compatibility verification.
