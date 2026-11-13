# Credential custody

Custody is an explicit owner operation, separate from task permissions and general
configuration secrets. The platform wraps the derived service unlock key, never the
client's master data key or password. Trusted administrators with the wrapping key can
recover data. The wrapping key must remain outside user workspaces and their backups.

`Custody::enroll` requires an authenticated owner, an exclusively bound physical
workspace, a stable operation identifier, and an expected custody revision. The host's
`ServiceKeyVerifier` verifies the key against that bound vault without creating or
overwriting data on failure. The binding is checked again before publication. Existing
client-encrypted data cannot become unattended custody until an unlocked client
provides the derived key.

First-store initialization is a separate owner operation. `begin_initialization`
requires an unbound agent with no prior custody or runtime history, fixes the
operation and provisioning-plan digest, and wraps a new random service key before
external effects. The host provisions and verifies new resources, binds their exact
identities, and admits an initialization-purpose writer. Only that current writer
receives the pending key; ordinary task or migration authority cannot substitute.

The initializer creates its authenticated marker and encrypted store, closes it at
the confirmed external anchor, then calls `finish_initialization`. Completion
publishes the ordinary custody receipt without granting task execution. Lost replies
return the original result. The same still-current initializer may retrieve its key
for existing-store reconciliation while that exact custody revision remains active;
creation verification stays denied. Owner revocation cancels pending initialization
and records retirement. Existing data is never a fallback initialization target.

`Custody::release` checks the owner and exact admitted runtime under the same transaction
that reads the active custody version. It records the release before returning secret
material. Encryption authenticates owner, agent, execution-local owner, and version;
copying ciphertext to another record does not produce a valid release. Secret types
redact debug output and clear their owned bytes on drop.

Revocation durably disables the active key, advances the runtime generation, clears its
lease, and records retirement intent in one transaction. Cached connections fail their
next admission check. Already admitted external effects may still finish and must be
reconciled; revocation does not undo them. A successful revocation receipt does not mean
the physical process has stopped.

`Kernel::retire_one` claims durable retirement work and calls `InstanceProvider::retire`.
The provider must stop and independently verify every launch through the recorded
generation, including attempts that never reached admission. It validates the exact
retirement claim with `Store::verify_retirement` before infrastructure changes. Missing
observations remain uncertain. Only verified completion clears the pending retirement;
new custody enrollment is refused until then. The default provider implementation
rejects retirement rather than inventing termination evidence.

Operation receipts remain immutable across revocation and reenrollment. Callers read
`Custody::status` for current availability and pending physical retirement. Active vault
key rotation requires its own verified data migration; enrollment cannot silently
replace the key of an already active vault.

The library and metadata tests do not establish operating-system isolation or a complete
deployment. Host wrapping-key delivery, real vault verification, physical retirement,
the public enrollment interface, and preparation-stage writer admission must be wired
and exercised before unattended recovery is a product guarantee.
