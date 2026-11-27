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
retirement, and stale runtime rejection. The background recovery loop also
reconciles a delivery whose upstream receipt exists while its platform
record is pending, then delivers a fresh owner control. Service data, logs,
and the result report are retained. Model responses and resource observations
are test fixtures. Cloud model quality, resource authorization, and deployment
have separate verification requirements.

The same probe reads the original goal and conversation through the public
goal router with a database-backed owner credential. It checks missing goals
and publications, foreign owners, scoped access, and credential revocation.

For a browser integration fixture, set `GESANG_TEST_CHAT_HOST_READY` to a new
file path in a private test directory before running the `engine-probe` example
against the execution fixture. It starts both owner routers on loopback and
writes the URL and test binding to that file. Point the demo's protected host
settings at those values and stop the fixture with SIGINT after verification.
`node --test apps/demo/test/protected.mjs` exercises the demo bridge's request
and byte-stream contract with an isolated HTTP fixture.

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

Run `cargo xtask test-native-command-goals` to repeat the probe with Chopin's
native worker, gate and output capture in a Linux sandbox. Use the same integration
settings and an isolated Linux test host with Python 3.10+ and a working Bubblewrap
installation. Point `GESANG_TEST_NATIVE_WORKER_RUNNER` at the executable
`scripts/test-native-worker.py` and `GESANG_TEST_NATIVE_WORKER_BIN_DIR` at the directory
with the three Linux binaries built from the execution producer. The runner starts
its own worker on loopback and creates a fresh workspace and private journal for the
probe. During the execution-process outage, the worker completes the admitted command,
commits its output and exit status, and leaves exactly one effect in the workspace.
The restored execution process reads that output for the original goal. This check
keeps the deterministic model and enrollment verifier from the shared probe.

`cargo xtask test-native-chat` joins the owner gateways, protected execution,
Linux command worker, and existing web chat. It submits a task in Chrome, closes
the page after the durable receipt, waits for file publication, then reopens the
original conversation and downloads the exact bytes. It uses a real configured
model for tool calls and an independent test verifier for the expected result.

- Use the same database, native store and worker runner settings as above.
- Set `GESANG_TEST_CHOPIN_SOURCE` to an absolute checkout containing the built
  execution service. Run `npm ci` in `apps/demo` before running the browser probe.
- Set `GESANG_TEST_NATIVE_CHAT_DIR` to a fresh private directory for the run. Set
  `GESANG_TEST_NATIVE_CHAT_CHROME_URL` to a dedicated headless Chrome debugging
  endpoint on loopback. The test creates only its own browsing targets.
- Set `GESANG_TEST_MODEL_URL` and `GESANG_TEST_MODEL_ID` to an available
  OpenAI-compatible endpoint and tool-capable model. Optional
  `GESANG_TEST_MODEL_MAX_OUTPUT_TOKENS`, `GESANG_TEST_MODEL_MAX_TOKENS_FIELD`,
  and `GESANG_TEST_MODEL_REASONING_OFF_VALUE` configure its generation budget and
  request format. `GESANG_TEST_MODEL_PROMPT_SUFFIX` may supply an instruction
  specific to the verification model.
- Set `GESANG_TEST_OPERATION_TIMEOUT_MS` for the complete browser run, including
  real inference. The test retains receipts, the downloaded file and screenshots.

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
