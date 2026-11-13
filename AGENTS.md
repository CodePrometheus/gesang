# Development

Keep the application kernel independent of HTTP, identity providers, business databases, and infrastructure objects. Both standalone and embedded hosts use the same kernel.

Execution owns goals, conversations, tool journals, and learning records. The kernel owns delegated authorization, command delivery, runtime coordination, wake records, and notification delivery. Never maintain a second execution state machine in platform metadata.

Use stable request identifiers. Persist recovery work before issuing external calls. An acknowledgement is not an execution result. Unknown remote outcomes require reconciliation rather than blind replay.

An expired lease is not proof that the previous writer has stopped. Require independent fencing evidence before giving a replacement writer access to storage. Missing or failed status observations mean unknown, never idle.

Public code, comments, and contributor documentation are English. Product strings belong in localization resources. Do not include machine-specific paths, credentials, or recorded personal data.

Run the checks relevant to changed behavior. Integration tests must exercise a real transactional store. Preserve test logs and explicitly report unverified requirements. Do not publish, deploy, or change external infrastructure as part of a local check.

Follow [the Rust conventions](docs/rust.md). Formatting, documentation, and lint checks are release gates, not optional review suggestions. Use current stable direct dependencies with the committed lockfile; test breaking dependency upgrades rather than silently retaining an older major version.
