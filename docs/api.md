# Platform API

These paths are the transport contract under implementation. Shared request and
response types exist; a complete independently deployable server is not delivered yet.

Embedding hosts may enable the `gesang-api` crate's `schema` feature to generate
schemas from the same contract types. Execution payloads retain the execution
protocol's schema and validation; the platform does not reproduce that protocol.
`CommandSubmission<C>` lets hosts use the upstream command type in their public
schema while the core stores its authenticated JSON representation. Strict execution
request schemas are supplied by the execution protocol's canonical contract export.

The platform API uses versioned paths and camelCase JSON fields. Authentication is required except for public discovery metadata. Resource ownership is taken from a verified identity, never a client-supplied owner field.

| Endpoint | Contract |
| --- | --- |
| `GET /.well-known/gesang-configuration` | Native authorization metadata: `apiVersion`, `issuer`, `authorizationEndpoint`, `tokenEndpoint`, `clientId`, `redirectUri`, `scopes` |
| `GET /v1/self` | The authenticated owner's agent and optional current task grant |
| `PUT /v1/agents/{id}/grant` | Explicit permission selection; atomically replaces and revokes the prior grant |
| `DELETE /v1/agents/{id}/grant/{grantId}` | Idempotent grant revocation for the authenticated owner |
| `POST /v1/agents/{id}/commands` | Encrypted durable outbox acceptance using a stable request ID; pending does not mean the execution service accepted it |
| `GET /v1/requests/{id}` | Original delivery result, scoped to the authenticated owner |
| `GET /v1/agents/{id}/goals?limit={count}` | One page of the execution-owned goal list and its optional cursor |
| `GET /v1/agents/{id}/goals/{goalId}` | Execution-owned goal snapshot, with no parallel platform goal state machine |
| `GET /v1/agents/{id}/goals/{goalId}/conversation` | Current bounded conversation window for the goal's original conversation |
| `GET /v1/agents/{id}/goals/{goalId}/artifacts` | The goal's published attachment metadata under `artifacts` |
| `GET /v1/agents/{id}/goals/{goalId}/artifacts/{artifactId}` | Verified original attachment bytes and a SHA-256 HTTP content digest |

The command routes are implemented by `gesang_host::CommandGateway`. The host
registers an authenticator in its service catalog and supplies a
`CommandGatewayPolicy` that rechecks credential authority in the same
transaction as admission. The built-in `ServiceAccess` policy uses the
PostgreSQL coordination transaction. Hosts using another database provide
their own transaction-bound credential policy.

Grant `submit_command_scope(agent_id)` and `observe_command_scope(agent_id)` to
authorize the corresponding owner requests. Admission returns HTTP 202
while delivery is pending and HTTP 200 for an already settled receipt.
`Location` identifies the original receipt. Use that URL to inspect delivery
after an interrupted client connection or host restart.

Mount the `gesang_host::GoalGateway` beside the command routes to read the original
goal and its results. The owner credential needs `observe_goal_scope(agent_id)`.
To read the next page, send the previous response's cursor as a JSON object
encoded in the `cursor` query parameter.
The host supplies an `ArtifactDownloadPolicy` with per-file and concurrent response
limits. Downloads return bytes after verifying the original publication's size
and checksum. An oversized publication returns HTTP 413. A read that cannot be
resolved against the current execution authority returns HTTP 503.

The command body contains `requestId`, `authorization`, and the upstream typed
`command`. Authorization is either `{"kind":"owner"}` for owner control, or
`{"kind":"grant","id":"...","version":1}` for delegated execution. The trusted
execution adapter derives which class the command requires; a client cannot select
owner control to bypass permission for new execution. Stop and user input remain
available to the authenticated owner after execution permission is revoked. Starting
new model or tool actions still requires a current grant.

Input that supplies a replacement grant requires delegated authorization matching that
grant's exact identifier and version. It cannot be submitted as ordinary owner input.
Grant responses include `purpose`: `task` or `improvement`. The ordinary session and
permission endpoint select task grants; improvement delegation is independently managed.

The host constructs the upstream versioned envelope. A repeated request with different
content is a conflict. A successful platform response does not imply goal completion.

Clients ignore unknown response object fields. Requests reject unknown fields to catch mistakes before effects. Unsupported protocol versions produce an explicit compatibility error. Breaking changes require a new contract version; compatibility windows are a release policy that must be verified before distribution.

Internal callbacks use separately authenticated credentials bound to the deployment,
agent, concrete instance, generation, and execution-local owner. A device access token
is never an internal control credential. Task grants are checked again at action
admission, including after recovery. The control adapter is implemented independently
of the public server and requires explicit host policy and request-size configuration.

Authority actions are `submit`, `input`, `stop`, `model`, `tool`, `owner`, `rsi`,
`resume`, and `migrate`.
`owner` is limited to execution-owned authenticated owner controls and does not authorize
model or tool work. `rsi` requires an improvement-purpose grant and the host's separate
`RsiModel` policy check. Task grants cannot substitute for improvement grants or vice
versa. Managed execution must check these actions at each actual effect boundary and
reject legacy write methods that would bypass goal admission.

`resume` requires a ready execution instance. A starting, unlocking, or draining
execution writer can reconcile storage but cannot resume scheduling or admit model
and tool effects. `migrate` accepts neither a grant nor a tool and requires the
current migration instance in its migrating stage. The host rechecks independent
source fencing before each migration admission. Migration never receives ordinary
execution authority. See [instance preparation](lifecycle.md).

| Internal endpoint | Contract |
| --- | --- |
| `POST /internal/v1/authority` | Current runtime identity plus `grant`, `action`, and optional `tool`; returns `allowed` and an optional reason |
| `POST /internal/v1/wake` | `wake` contains `active` and an optional exact Unix-millisecond `at`; persists recovery without erasing earlier work |
| `POST /internal/v1/anchor/read` | Reads confirmed and pending storage freshness; only a missing record returns 404 |
| `POST /internal/v1/anchor/init` | `head` registers a sequence-zero digest without replacing history |
| `POST /internal/v1/anchor/prepare` | `expected` and `pending` identify one exact successor |
| `POST /internal/v1/anchor/confirm` | `pending` and `local` confirm the trusted storage adapter's durable result |
| `POST /internal/v1/anchor/abort` | `pending` and `verifiedLocal` reconcile a verified uncommitted operation |

Each request includes `agentId`, `epoch`, and `owner`, checked against the credential
and current admission. A head contains `sequence` and a 32-byte content digest encoded
as unpadded base64url. A pending operation contains `operationId`, `generation`, and
`next`. Recovery preserves the original pending generation even when a new writer
resolves it. Conflicts return 409; unavailable control state returns 503 and never
means missing history. Every 503 includes `Retry-After`, derived from the explicitly
configured recovery interval and rounded up to whole seconds. Unavailability is not
a permission denial or a final command rejection. Responses prohibit caching and
omit internal database errors.

Only the protected execution service may hold the internal credential. Tool processes
must not share that service's secrets or protection domain. Tool-name admission does
not authorize arbitrary tool arguments: the resource gateway must enforce the actual
resource and operation. The anchor protocol requires independent writer fencing and
the execution store's actual durable content verification; the HTTP adapter alone
does not prove either property.
