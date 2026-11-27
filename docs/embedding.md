# Embed Gesang in a Rust host

An existing application embeds Gesang behind its own authenticated routes. The
application verifies the current session and maps its stable issuer and subject
to a Gesang owner. The host supplies resource policy, provisioning, and custody.
Chopin owns execution, goals, conversations, and command output.

## Dependencies

Use Rust 1.100 or later and commit the consuming application's lockfile. Pin
the crates used by the host to the same release tag.

```toml
[dependencies]
gesang-api = { git = "https://github.com/CodePrometheus/gesang.git", tag = "v0.1.0-rc1" }
gesang-core = { git = "https://github.com/CodePrometheus/gesang.git", tag = "v0.1.0-rc1" }
gesang-host = { git = "https://github.com/CodePrometheus/gesang.git", tag = "v0.1.0-rc1" }
uuid = "1.29.2"
```

Gesang pins both Chopin Rust dependencies to
`91251b63ed9beb2842b0d8bdaa7a07b792401902` from `v0.7.0-rc2`. If your host
uses a Chopin Rust crate directly, use the same Git URL and revision so Cargo
resolves shared types from one source. The [Chopin host guide](https://github.com/CodePrometheus/chopin-ai/blob/v0.7.0-rc2/apps/tui/cloud.md)
covers the native storage binding and execution service.

Cargo can use the committed SQLx metadata. Set `SQLX_OFFLINE=true` for the
Gesang build if your environment provides an unrelated `DATABASE_URL`.

## Assemble the host

1. Create a `gesang_core::Store` from a PostgreSQL pool and a protected
   `PayloadKey`. Run `Store::migrate` before accepting work. The store checks
   PostgreSQL durability settings before applying its embedded migrations.
2. Implement `InstanceProvider` against the host's resource backend and
   `EngineConnector` against its trusted Chopin endpoint. The provider
   reconciles the actual instance and workspace. Before a replacement
   writer starts, it must independently confirm that the previous writer
   stopped or lost storage access.
3. Construct the shared `EngineAdapter` and `Kernel`. Configure the operation
   timeouts and recovery intervals for your deployment. Run
   `Kernel::run_recovery` under the host's supervisor and report recovery
   failures through its callback. It drives delivery, wake, and retirement
   independently of client connections. On shutdown, close request admission,
   signal the recovery loop, await its current pass, then close the execution
   adapter. Hosts with their own scheduler can call `dispatch_one`, `wake_one`,
   and `retire_one` directly.
4. Mount `ControlPlane::router` on the protected runtime callback listener.
   Supply the actual `ControlPolicy` and separate credential material.
   First-store initialization and execution key release are explicitly enabled
   through `with_initialization_custody` and `with_execution_custody`.
   For protected commands, use `JobAuthority` with the host's `JobPolicy` and
   the Chopin worker. See the [custody protocol](custody.md).

```rust
use std::sync::Arc;

use gesang_core::{InstanceProvider, Kernel, RuntimePolicy, Store};
use gesang_host::{EngineAdapter, EngineConnectionPolicy, EngineConnector};

fn assemble(
    store: Store,
    instances: Arc<dyn InstanceProvider>,
    connector: Arc<dyn EngineConnector>,
    runtime_policy: RuntimePolicy,
    connection_policy: EngineConnectionPolicy,
) -> gesang_core::Result<(Kernel, Arc<EngineAdapter>)> {
    let engine = Arc::new(EngineAdapter::new(
        store.clone(),
        connector,
        connection_policy,
    )?);
    let kernel = Kernel::new(store, instances, engine.clone(), runtime_policy)?;
    Ok((kernel, engine))
}
```

## Submit and resume a task

Derive `VerifiedIdentity` from the verified session on the server, and use
`Store::bind_owner` only in the application's authorized enrollment flow.
For ordinary requests, look up the existing owner with `Store::find_owner`.
The application's permission checks should limit the agent and the available
workspace capabilities before a command is submitted. Use the scoped task
grant for tool execution.

```rust
use gesang_api::{CommandReceipt, CommandSubmission};
use gesang_core::{Error, Kernel, VerifiedIdentity};
use uuid::Uuid;

async fn submit_for_owner(
    kernel: &Kernel,
    identity: &VerifiedIdentity,
    agent_id: Uuid,
    submission: &CommandSubmission,
) -> gesang_core::Result<CommandReceipt> {
    let owner = kernel
        .store()
        .find_owner(identity)
        .await?
        .ok_or(Error::Forbidden)?;
    kernel.submit(&owner, agent_id, submission).await
}
```

Generate the submission's `request_id` before sending it and retain it across
reconnections. Use `Store::receipt` with the same owner and request ID if the
submission response is lost. The receipt records delivery state. For the result,
use `Kernel::snapshot` with the original owner, agent, and goal ID. For a live
view, use `EngineAdapter::observe_goal` with the host-verified runtime. Retain
Chopin's original operation IDs and output cursors when reading command output.

Mount `CommandGateway` and `GoalGateway` to expose admission, receipt lookup, and
the original goal's conversation and artifacts through the same kernel and engine.
The [API contracts](api.md) describe the owner scopes and attachment download policy.
Run the [deployment checks](verification.md) in the target environment before
enabling live model and workspace access.
