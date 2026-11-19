# Verification requirements

## Host and execution interoperability

Build the locked execution producer with `pnpm build` before running
`cargo xtask test-engine`. Set `GESANG_TEST_ENGINE_FIXTURE` to its
`packages/harness-pi/tests/fixtures/rust-service.mjs`,
`GESANG_TEST_DATABASE_URL` to a dedicated integration PostgreSQL database,
`GESANG_TEST_OPERATION_TIMEOUT_MS` to the test environment's per-operation deadline,
and `GESANG_TEST_ARTIFACT_DIR` to the evidence directory. Node 26.1 or later and
Python 3 must be available on `PATH`.

The check launches the real service, creates an isolated database schema, and uses
the production host adapter for immutable admission, original request lookup,
goal and history pages, goal and conversation observations, stop, connection
retirement, and stale runtime rejection. Service data, logs, and the result report
are retained. It uses a deterministic model and test authority callbacks; it does
not establish cloud model quality, production resource authorization, or deployment.

## Shared services and package references

`cargo xtask test-jobs` verifies the compiled Node command client against the real
HTTP broker, purpose-bound host credentials and an isolated transactional database.
Set `GESANG_TEST_JOB_PROBE` to the execution producer's
`packages/harness-pi/scripts/jobs-probe.mjs`, `GESANG_TEST_DATABASE_URL` to a dedicated
database, and `GESANG_TEST_OPERATION_TIMEOUT_MS` to the environment's operation deadline.
Build that producer with `pnpm build` first. The probe kills the actual submitting
process after durable acceptance and before its response, then starts a new client
to recover the original operation and binary output. Revocation rejects new work
while retaining original observations. Worker process facts are explicit fixtures;
this check does not establish physical sandbox isolation or command execution.
The ordinary store suite excludes this additional Node interoperability check.

`cargo xtask test-command-goals` combines the actual agent service, native encrypted
store, shared HTTP authority, transactional command broker and tool contribution.
Set `GESANG_TEST_COMMAND_GOAL_PROBE` to the producer's
`packages/harness-pi/scripts/command-goal-probe.mjs`, `CHOPIN_STORAGE_NATIVE` to its
compiled native store module, and the database and operation-deadline variables
above. Build the producer and native module first. The fixture prepares a new
protected store; execution children reopen it without creation permission. The
submitting execution process is killed after acceptance and before the response,
then a fresh process resumes the same goal and commits its verified tool result.
Actual grants and freshness anchors are checked throughout. Each execution child
obtains its key through the current custody callback; its environment contains no
store key. The check verifies both durable release audit records and native vault
opening after release. Enrollment verification, physical writer facts
and model responses are explicit fixtures; first-store initialization, physical
sandbox execution, cloud provisioning and model quality require separate checks.

The combined probe now calls the production `openCloudRuntime()` entry rather
than assembling a separate test-only service. Test preparation creates the vault
and protected store; execution children only reopen them. The runtime's actual
controller listener, home synchronization, current authority, command contribution
and paused restoration are active. The probe injects its deterministic model and
trusted evidence verifier. CLI configuration refusal and early-startup guards run
separately; a live-provider CLI deployment remains part of remote acceptance.

`cargo xtask check` runs lifecycle unit tests and the authority construction contract.
`cargo xtask test-store`, with `GESANG_TEST_DATABASE_URL` set to a dedicated database,
also runs shared-service and package-reference tests against isolated real schemas.
These cover concurrent callers, dependency scopes, persistence failure, independent
installation/owner/resource references, concurrent conflicts, pool reopen, migration,
and synchronous commit policy. Schemas and evidence are retained.

Caller authorities in these tests are explicit inputs. The checks establish service
composition and static boundaries, not authentication, current business policy, or
untrusted-plugin isolation. Keep fault-injection checkouts and build directories
separate from the normal workspace; a failed build is not a detected behavior defect.

The store suite also exercises plugin installation, catalog publication, immutable
configuration lookup, dependency/interface validation, installation reference isolation,
and original secret versions under revocation. Native test handlers read real files to
observe routing through the production catalog and graph; they are not a product resource
provider. The schema suite runs with dependency file/HTTP retrieval enabled to verify
the host's explicit denial, including an actual local HTTP endpoint receiving no request.
These checks do not validate package downloads, already-issued credential revocation,
business ingress authentication, cloud side effects, or a resource operation journal.

## Complete product acceptance

- Submit a real goal, close the client, restart the host, and retrieve the original goal and actual result from another client.
- Lose a response after durable acceptance, retry the original request, and prove that execution is not duplicated.
- Revoke a task grant while clients are offline and prove that new actions are denied. Previously issued side effects must remain visible and reconcilable.
- Recover a long command by its original operation reference, output cursor, and exit status. An unknown result must never become an automatic unsafe replay.
- Exercise competing controllers, a partitioned old writer, storage handoff, and stale generations. Database lease changes alone do not pass this requirement.
- Interrupt each external-anchor commit boundary and verify accepted-commit freshness, content integrity, and correct recovery without duplicate side effects.
- Replace or disable a plugin during in-flight work; verify dependency shutdown, permission revocation, pinned recovery definitions, and visible failure states.
- Run recursive improvement beside a long goal, preserving independent budgets, evaluation gates, adoption boundaries, and rollback records.
- Verify native release startup, input, scrolling, system navigation, font scaling, accessibility, offline drafts, and notification deep links on Android.
- Establish memory, storage, latency, and package-size baselines under equivalent encryption, durability, isolation, and history workloads. No performance claim is established yet.
- Verify a clean single-host deployment and an embedded cluster deployment. Keep account mapping, custody, backups, and credential renewal operational after restart.
- Finalize license, distribution identity, dependency delivery, and push configuration before public release.
