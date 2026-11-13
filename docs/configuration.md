# Configuration

`ConfigurationCenter` stores typed component settings in deployment or owner scopes.
Each component defines a namespace, schema version, validation, and adoption boundary
through `Configuration`. The host supplies verified access; transport input cannot
construct administrative authority.

Preparation validates the entire change and stages encrypted secret versions in one
durable transaction. It does not change active settings. Publication compares the
expected active revision, validates secret availability, and atomically publishes an
immutable revision. Concurrent publications from the same revision have one winner.
Stable operation identifiers reconcile lost responses; changing an operation's input
is a conflict. Rolling back prepares a new revision using a prior published value.

Nonsecret values are stored as ordinary structured data. Secret fields use opaque,
name-bound references and a separate protected input type. Encryption binds each
secret to its scope, component stream, name, and version. Public revisions and audit
events contain references rather than secret values. The host key is supplied outside
user storage. Secret revocation also invalidates historical references pinned by work.

Consumers apply settings at their declared boundary:

| Boundary | Application rule |
| --- | --- |
| Live | Read and record the current published revision before new admission |
| Checkpoint | Keep the adopted revision until a verified checkpoint permits change |
| Instance | Adopt when a new instance is prepared |

An adoption record reports successful application; it does not apply settings for the
consumer. Consumers must use the returned revision and explicitly record checkpoint
or instance adoption. Revoked secrets cannot be resolved through an old adoption.

`Kernel::with_configuration` consumes the deployment's `host.runtime` settings. Each
coordination attempt uses one revision for lease, delivery, retry, and recovery timing.
Durations are exact positive integer milliseconds; the recovery interval must be less
than the lease. No runtime policy is silently initialized. `Kernel::new` accepts an
explicit fixed policy for embedding and controlled tests.

HTTP body limits, connection settings, provider resources, and user-facing configuration
routes still require host integration. The configuration library alone does not make
all deployment settings dynamically configurable.
