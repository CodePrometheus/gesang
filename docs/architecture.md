# Architecture

The kernel is an application library. A standalone server and an existing business host both embed it. Transport, verified identity, instance provisioning, protected key delivery, and notifications are explicit host adapters. Custody policy and metadata use the shared [custody authority](custody.md).

The execution service is the only authority for goals, completion evidence, conversations, tool journals, checkpoints, and recursive improvement. Platform metadata contains owner bindings, grants and revocations, encrypted command delivery records, leases and fencing generations, persistent wakes, notification delivery, and external freshness anchors.

Command delivery is at least once. A stable request identifier is checked against the immutable command content by both platform and execution. Only a durable execution acknowledgement marks delivery accepted. Retrying an uncertain delivery uses the original identifier. A stopped client is unrelated to a stopped goal.

Permission must be selected explicitly. Full access grants only the person's resources within the administrator's ceiling; it does not grant platform administration, another person's data, or exemptions from nondelegable business approvals. Task-specific requests for confirmation remain binding. Device sessions and task delegations are independently revocable.

Task execution and recursive improvement use distinct delegation purposes. Replacing
or revoking one purpose does not replace the other, and neither can authorize actions
in the other domain. Recursive improvement retains its execution-owned budget and
evaluation gates in addition to checking current platform permission.

Each agent has one logical controller domain and one effective writer. A routing lease is insufficient to fence a local database writer. Provisioning adapters must verify prior-writer termination or storage I/O revocation before replacement admission. Tool execution is in a separate protection domain and never receives platform root keys.

The shared host implements command-broker authority. Immutable command acceptance
and the kernel's current-writer and exact-delegation checks share one transaction.
The host supplies actual resource policy and independently resolves the protected
worker's physical identity. An allocation identifier alone does not establish a
process identity. Engine credentials and operation-bound worker callbacks have
distinct purposes and use a dedicated persistent deployment key.

Revocation denies new execution while preserving original-worker observations.
The current writer may read old output and stop the original worker after a runtime
replacement; those operations never create another process. Output cursors retain
their exact byte position. Releasing a command requires fresh execution admission,
and neither a missing worker nor a disconnected client proves process termination.

Every physical workspace resource is exclusively bound to one agent in the platform
store. Binding a set is atomic: overlapping resource sets cannot bypass isolation by
using different workspace descriptions. Hosts must also verify resource identities
and ownership at the infrastructure boundary before adoption. Existing account
ownership is not inferred from an email address or an unverified workspace name.

Instance preparation has a [durable lifecycle](lifecycle.md). The controller records
an immutable plan and instance identity before allocation, admits the actual gated
writer before releasing its startup barrier, and restores storage with scheduling
paused. Only a ready execution instance receives task authority. Migration has its
own writer purpose and must terminate at a confirmed storage head before a different
generation can execute work.

Hosts share the execution connection adapter. It checks current instance identity,
generation, execution owner, and locator before calls, then reuses a sequential
authenticated connection. Cancellation or a transport failure discards partial wire
state. Unknown command outcomes stay uncertain and are queried by their original
request identifier; only typed, definitive execution refusals become rejected
delivery records. Timing policy is explicit host configuration. Connection retirement
and shutdown drain admitted calls without treating disconnection as task completion.

The same adapter supplies typed goal pages, conversation snapshots and history pages,
legacy conversation summaries, and the actual model catalog. Page sizes are supplied
by host policy and source cursors pass through unchanged. Live observations use
dedicated protocol connections, merge producer deltas once, and recheck the exact
writer before exposing state. Retirement, replacement, and shutdown invalidate those
observations; a cancelled read requires a fresh snapshot. An observation revision is
local to its connection and is never a durable history cursor.

The [configuration center](configuration.md) publishes typed immutable revisions,
stages encrypted secret references, and records adoption at declared boundaries.
The configured kernel reads one policy revision for each coordination attempt.

The client stores drafts, device authentication, synchronization cursors, and bounded presentation state. It never keeps the cloud alive or renews task credentials. Notification payloads contain an event identifier and locator, with no conversation content.

Plugins use explicit scopes, typed service dependencies, generation checks, and consumer-first shutdown. An in-process service registry is not a security boundary. Durable recovery definitions and package versions outlive a plugin activation.
