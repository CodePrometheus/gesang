# Trusted service composition

The graph manages trusted host services. It does not load arbitrary code, isolate untrusted
extensions, schedule agent goals, or replace the execution engine. The host issues authority
and selects verified package manifests; the graph never infers additional capabilities.

Services register a typed key, package version, persistent reference, declared dependencies, and
an asynchronous factory. `ServiceScope` separates shared deployment or integration lifetimes from
private owner, instance, and activation lifetimes. Shared services can start before any user or
agent exists. Validation rejects absent providers, interface type collisions, cycles, dependencies
outside the binding, and providers with a shorter resource lifetime. Shared consumers cannot
capture a private provider. Factories can resolve only declared providers.

Handles retain a service generation, not a caller. Each `call` supplies an `Authority` issued by
the trusted host for a verified owner or service identity. Static binding and private ownership
checks precede admission; nested calls must forward or restrict the current authority. Construction
scope grants no user capabilities, and authority cannot be deserialized from requests. The host
must authenticate callers, check current business policy, and enforce resource access at the final
service boundary. These checks are not supplied by the graph or by a capability string alone.

Every activation acquires its immutable version reference before its factory runs. `PinStore`
belongs to the platform's transactional database: insertion must be atomic and identical repeated
requests idempotent. `PinSubject` distinguishes installation, owner, and retained-resource
references. A different version or content digest for the same subject/reference/package is a
conflict. Releasing activation resources never calls `unpin`; durable consumers release their
references separately after they no longer need the package. Old resources and operations must
retain their original provider and configuration bindings; package references alone do not route
those operations.

Lifecycle unit tests use an in-memory reference store and do not establish durability. The kernel's
service integration tests use the real transactional store to check concurrent pin conflicts,
reference separation, connection-pool reopen, failed persistence, and per-transaction commit
policy. The package-reference migration preserves legacy rows and rejects writes through the old
table, so old writers must be upgraded. These tests do not establish business authentication,
untrusted-code isolation, process-crash recovery, or complete resource-provider integration.

Provider replacement validates the candidate graph and pin before revoking the affected
generations. It then refuses new calls, drains accepted calls, closes transitive consumers before
their providers, and rebuilds them against fresh handles. Cleanup failure retains revoked
resources and blocks provider cleanup and replacement until an explicit retry succeeds. Cleanup
hooks release resources and cannot use revoked handles to submit new work. Factories must clean
up partial acquisitions when they return an error.

Lifecycle transitions are serialized within one graph and continue if their requesting future is
cancelled. Hosts should partition independent ownership domains into independent graphs and must
await explicit release before stopping the runtime. Failed starts, cleanup failures, and refused
replacements remain queryable without exposing service values. Store recovery and external
resource fencing remain platform responsibilities.

## Installation and provider selection

`PluginManifest` declares package content, named exports and imports, exact interface versions,
a self-contained configuration schema, and required secret names. Manifest format 1 uses positive
integer interface versions; package versions do not imply interface compatibility. Configuration
uses draft 2020-12 with format validation. External schema retrieval is explicitly denied even
when other dependencies enable network or filesystem retrieval.

The kernel's `PluginCatalog` reuses protected configuration publication for installations and
service selections. Installation settings pin exact published imports. Each installation owns
its configuration revisions and namespaces its request identifiers for package retention. A
selection publishes at most one provider for each exact interface in its business binding.
Missing imports, incompatible contracts, ambiguous choices, and concurrent revision conflicts
fail explicitly. An empty selection exposes no defaults.

`InstalledVersion` and `ServiceEndpoint` preserve the original installation, configuration
revision, and export. `InstalledPlugin::register` checks the native dependency identities and
uses a graph key specific to that installed version. New selections can coexist with old service
generations. Resources and operations must persist their original endpoint before external work;
package retention alone is not an operation journal. Public callers must not choose arbitrary
provider endpoints: the host derives them from current selection or an authorized resource.

Publishing configuration is not a readiness assertion. Hosts register and activate implementations,
observe actual readiness, and then publish the intended selection. Loading an exact installation
does not grant permission to use its secrets; secret resolution checks the original version under
current revocation. Values already delivered to a running service require host-enforced revocation
and shutdown policies. The catalog neither downloads nor verifies package code; trusted installers
must verify content and declarations before using it. Business call authorization, provider resource
reconciliation, and untrusted execution remain separate integration requirements.
