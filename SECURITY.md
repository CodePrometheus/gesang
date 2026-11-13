# Security

The implementation is under active development and has not passed its release security gates. Do not expose an unfinished deployment to untrusted users.

Do not report private keys, authentication tokens, personal conversation records, or exploit instructions in public issues. Use private vulnerability reporting when enabled on the project repository. Repository maintainers must configure and publish a private reporting contact before the first public release.

Reports should include the affected revision, deployment assumptions, minimum reproduction, observed impact, and any evidence needed to distinguish authorization, isolation, or recovery failures. Use synthetic data and remove credentials.

Security fixes must include a regression check for the failed invariant. Lease expiry is not a writer fence; encryption integrity is not rollback freshness; a plugin scope is not a process isolation boundary. Review these guarantees independently.
