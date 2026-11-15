# Sandbox service

The resource API operates without an agent or a goal. Authentication and sandbox
implementations are selected through the service catalog. Every request receives
its current caller authority; service instances never retain the first caller.

The included local provider creates an ephemeral execution instance from an
administrator-approved profile. It uses an immutable image, a non-root identity,
private scratch mounts, disabled networking, dropped capabilities, and explicit
resource limits. Persistent workspaces and execution-runtime attachment are separate
integration work. This provider is not the complete personal-agent deployment.

## Requests and recovery

| Route | Result |
| --- | --- |
| `POST /v1/sandboxes` | Accepts `{requestId, profile}` and returns an operation location |
| `GET /v1/sandbox-requests/{requestId}` | Recovers the original creation receipt when its response was lost |
| `GET /v1/sandboxes/{id}` | Returns current observation within the caller's ownership scope |
| `POST /v1/sandboxes/{id}/stop` | Accepts a stop with a stable `requestId` |
| `GET /v1/sandboxes/{id}/operations/{requestId}` | Reads the original operation outcome |

Acceptance is not execution success. Preserve the original request identity and
arguments while an outcome is uncertain. A new request identity creates different
work; it is not a transport retry.

The kernel reserves immutable ownership and provider routing together with the
package reference. The service commits an issuance marker before allocation or
start. Recovery observes the original instance instead of replaying either effect.
Stop targets the immutable physical identity and retains attached volumes. A missing
observation never proves termination: a successful removal response or a matching
destruction event supplies that evidence. Expired event retention can leave an
outcome unknown.

Creation permissions bind to the exact installed profile. Observation and stop
permissions refer to the authenticated principal's own sandbox collection. Changing
credentials cannot authorize another principal's resources. Each new effect checks
the recorded grant again; observations of already issued work remain possible after
revocation. Provider configuration comes from the original published installation,
including after a worker restart or a new configuration publication.

## Hosting and verification

`SandboxGateway` mounts the authenticated routes. `ContainerSandbox::run` is a
host-owned recovery future whose result and shutdown must be observed. The
`sandbox-driver` example runs that future in a separate process using a protected
configuration file. The host owns TLS, deployment credentials, and image installation.

For the real integration checks, supply:

- `GESANG_TEST_DATABASE_URL`: a dedicated transactional test database.
- `GESANG_TEST_CONTAINER_CONFIG`: a JSON fixture containing a `ContainerConnection`
  and `ContainerProfile` for an already installed image and explicitly selected limits.
- `GESANG_TEST_ARTIFACT_DIR`: a directory for retained process, resource, and network evidence.

Then run `cargo xtask test-sandbox`. It builds the worker example, exercises real
HTTP authentication and local resources, and interrupts worker processes after
actual allocation and removal responses have been withheld. The proxy forwards
real requests; it controls reply delivery and observation availability. The tests
check original identities, no duplicate effects, revocation, and recovery from the
original configuration. They do not establish cloud deployment, persistent volume
handoff, application readiness, or isolation of arbitrary third-party extensions.
