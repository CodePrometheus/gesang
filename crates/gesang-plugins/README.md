# Trusted service composition

The graph manages trusted host services. It does not load arbitrary code, isolate untrusted
extensions, schedule agent goals, or replace the execution engine. The host issues authority
and selects verified package manifests; the graph never infers additional capabilities.

Services register a typed key, package version, persistent reference, exact authority, declared
dependencies, and an asynchronous factory. Validation rejects absent providers, interface type
collisions, cycles, cross-owner dependencies, and dependencies with a shorter resource lifetime.
Factories can resolve only declared providers, and every handle carries the consumer's authority.
Service interfaces must enforce the authority passed to each call at their external boundaries.

Every activation acquires its immutable version reference before its factory runs. `PinStore`
belongs to the platform's transactional database: insertion must be atomic and identical repeated
requests idempotent. A different version or content digest for the same owner/reference/package
is a conflict. Releasing activation resources never calls `unpin`; durable consumers release
their references separately after they no longer need the package. The test store is in memory
and does not establish database durability or process-restart guarantees.

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
