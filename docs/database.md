# Database access

Queries belong to the domain that owns their records. Short queries stay in that
domain's private Rust query module, formatted as multiline SQL. Longer statements
can live in a nearby SQL file when reviewing the SQL separately improves clarity.
Do not split every statement into a file or add a generic repository abstraction
around a single query.

Use `query_as!`, `query!`, or their file variants for new queries. Database rows
have explicit Rust types; convert them into domain values at the storage boundary.
Return concrete errors, preserve uncertain outcomes, and distinguish missing data
from an unavailable store. Existing modules that still use runtime queries retain
their transactional integration tests while they are migrated incrementally.

The service method owns the transaction. Query helpers receive that transaction;
they do not open another connection or commit part of a larger operation. Acquire
credential locks before resource locks, retain admission locks until commit, and
perform provider calls afterward. Resource routing and its package reference commit
together. Provider operation records remain separate from execution-owned goals.

## Schema and query changes

Add a numbered migration; never rewrite a migration already applied to a persistent
store. Platform and service components use separate migration ledgers. The service
ledger name is declared in its migration directory and is shared by runtime setup
and query validation.

Ordinary builds use the checked-in `.sqlx` metadata and do not contact a database.
After changing a query or migration, set `GESANG_TEST_DATABASE_URL` to a dedicated
database with schema-creation privileges and run:

```sh
cargo xtask prepare-sql
cargo xtask check-sql
cargo xtask test-store
```

Both SQL commands create and retain an isolated schema from the source migrations.
`prepare-sql` generates query metadata and archives obsolete generated entries in
the local verification directory. `check-sql` compares every active query against
the migration schema and rejects stale or unused metadata. Review the migrations,
query changes, and generated metadata together.

Compile-time validation checks column names, parameters, types, and nullability.
It does not prove authorization, transaction ordering, idempotency, or recovery.
Those properties require real database and service tests, including failure cases.
