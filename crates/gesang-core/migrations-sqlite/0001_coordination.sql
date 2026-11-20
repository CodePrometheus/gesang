CREATE TABLE gesang_owners (
    id BLOB NOT NULL PRIMARY KEY,
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    UNIQUE (issuer, subject)
) STRICT;

CREATE TABLE gesang_agents (
    id BLOB NOT NULL PRIMARY KEY,
    owner_id BLOB NOT NULL UNIQUE REFERENCES gesang_owners(id),
    engine TEXT NOT NULL,
    engine_owner TEXT NOT NULL,
    legacy_reference TEXT,
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    generation INTEGER NOT NULL DEFAULT 0 CHECK (generation >= 0),
    controller_id BLOB,
    lease_until INTEGER,
    runtime_id BLOB,
    runtime_generation INTEGER,
    runtime_locator TEXT,
    CHECK ((runtime_id IS NULL) = (runtime_generation IS NULL)),
    CHECK ((runtime_id IS NULL) = (runtime_locator IS NULL)),
    CHECK ((controller_id IS NULL) = (lease_until IS NULL))
) STRICT;

CREATE TABLE gesang_grants (
    id BLOB NOT NULL PRIMARY KEY,
    agent_id BLOB NOT NULL REFERENCES gesang_agents(id),
    version INTEGER NOT NULL CHECK (version > 0),
    mode TEXT NOT NULL CHECK (mode IN ('full_access', 'scoped', 'ask')),
    scopes TEXT NOT NULL,
    purpose TEXT NOT NULL DEFAULT 'task' CHECK (purpose IN ('task', 'improvement')),
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    revoked_at INTEGER
) STRICT;
CREATE UNIQUE INDEX gesang_one_active_grant_per_purpose ON gesang_grants(agent_id, purpose)
    WHERE revoked_at IS NULL;

CREATE TABLE gesang_commands (
    request_id BLOB NOT NULL PRIMARY KEY,
    agent_id BLOB NOT NULL REFERENCES gesang_agents(id),
    grant_id BLOB REFERENCES gesang_grants(id),
    grant_version INTEGER,
    authority TEXT NOT NULL DEFAULT 'grant' CHECK (authority IN ('owner', 'grant')),
    content_digest BLOB NOT NULL,
    sealed_command BLOB NOT NULL,
    requirements TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'accepted', 'rejected')),
    sealed_receipt BLOB,
    problem TEXT,
    delivery_owner BLOB,
    delivery_until INTEGER,
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    acknowledged_at INTEGER,
    CHECK (
        (authority = 'owner' AND grant_id IS NULL AND grant_version IS NULL)
        OR (authority = 'grant' AND grant_id IS NOT NULL AND grant_version IS NOT NULL)
    ),
    CHECK ((state = 'accepted') = (sealed_receipt IS NOT NULL)),
    CHECK ((state = 'rejected') = (problem IS NOT NULL)),
    CHECK (delivery_owner IS NULL OR delivery_until IS NOT NULL)
) STRICT;
CREATE INDEX gesang_commands_pending ON gesang_commands(created_at, request_id) WHERE state = 'pending';

CREATE TABLE gesang_wakes (
    agent_id BLOB NOT NULL PRIMARY KEY REFERENCES gesang_agents(id),
    due_at INTEGER NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    claimed_by BLOB,
    claimed_until INTEGER,
    CHECK ((claimed_by IS NULL) = (claimed_until IS NULL))
) STRICT;
CREATE INDEX gesang_wakes_due ON gesang_wakes(due_at);

CREATE TABLE gesang_anchors (
    agent_id BLOB NOT NULL PRIMARY KEY REFERENCES gesang_agents(id),
    sequence INTEGER NOT NULL DEFAULT 0 CHECK (sequence >= 0),
    digest BLOB NOT NULL,
    pending_id BLOB,
    pending_generation INTEGER,
    pending_sequence INTEGER,
    pending_digest BLOB,
    CHECK ((pending_id IS NULL) = (pending_generation IS NULL)),
    CHECK ((pending_id IS NULL) = (pending_sequence IS NULL)),
    CHECK ((pending_id IS NULL) = (pending_digest IS NULL))
) STRICT;

CREATE TABLE gesang_workspace_resources (
    resource_key TEXT NOT NULL PRIMARY KEY CHECK (length(resource_key) > 0),
    agent_id BLOB NOT NULL REFERENCES gesang_agents(id)
) STRICT;
CREATE INDEX gesang_workspace_resources_agent ON gesang_workspace_resources(agent_id, resource_key);

CREATE TABLE gesang_configuration_streams (
    id BLOB NOT NULL PRIMARY KEY,
    scope_key TEXT NOT NULL,
    owner_id BLOB REFERENCES gesang_owners(id),
    component TEXT NOT NULL CHECK (length(component) > 0),
    active_revision INTEGER,
    next_revision INTEGER NOT NULL DEFAULT 1 CHECK (next_revision > 0),
    UNIQUE (scope_key, component),
    FOREIGN KEY (id, active_revision) REFERENCES gesang_configuration_revisions(stream_id, revision),
    CHECK (
        (owner_id IS NULL AND (scope_key = 'deployment' OR scope_key LIKE 'services/%'))
        OR (owner_id IS NOT NULL AND substr(scope_key, 1, 6) = 'owner/'
            AND substr(scope_key, 7) = lower(substr(hex(owner_id), 1, 8) || '-' ||
              substr(hex(owner_id), 9, 4) || '-' || substr(hex(owner_id), 13, 4) || '-' ||
              substr(hex(owner_id), 17, 4) || '-' || substr(hex(owner_id), 21, 12)))
    )
) STRICT;

CREATE TABLE gesang_configuration_secrets (
    id BLOB NOT NULL PRIMARY KEY,
    stream_id BLOB NOT NULL REFERENCES gesang_configuration_streams(id),
    name TEXT NOT NULL CHECK (length(name) > 0),
    sealed_value BLOB NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    revoked_at INTEGER
) STRICT;
CREATE INDEX gesang_configuration_secrets_stream ON gesang_configuration_secrets(stream_id, id);

CREATE TABLE gesang_configuration_revisions (
    stream_id BLOB NOT NULL REFERENCES gesang_configuration_streams(id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    operation_id BLOB NOT NULL,
    expected_revision INTEGER,
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    adoption_boundary TEXT NOT NULL CHECK (adoption_boundary IN ('live', 'checkpoint', 'instance')),
    input_digest BLOB NOT NULL,
    "values" TEXT NOT NULL,
    secret_references TEXT NOT NULL,
    rollback_of INTEGER,
    prepared_by TEXT NOT NULL,
    prepared_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    published_by TEXT,
    published_at INTEGER,
    PRIMARY KEY (stream_id, revision),
    UNIQUE (stream_id, operation_id),
    FOREIGN KEY (stream_id, expected_revision) REFERENCES gesang_configuration_revisions(stream_id, revision),
    FOREIGN KEY (stream_id, rollback_of) REFERENCES gesang_configuration_revisions(stream_id, revision),
    CHECK ((published_by IS NULL) = (published_at IS NULL))
) STRICT;

CREATE TABLE gesang_configuration_adoptions (
    stream_id BLOB NOT NULL REFERENCES gesang_configuration_streams(id),
    consumer_id BLOB NOT NULL,
    revision INTEGER NOT NULL,
    adopted_by TEXT NOT NULL,
    adopted_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    PRIMARY KEY (stream_id, consumer_id),
    FOREIGN KEY (stream_id, revision) REFERENCES gesang_configuration_revisions(stream_id, revision)
) STRICT;

CREATE TABLE gesang_configuration_events (
    id INTEGER PRIMARY KEY,
    stream_id BLOB NOT NULL REFERENCES gesang_configuration_streams(id),
    kind TEXT NOT NULL CHECK (kind IN ('prepared', 'published', 'secret_revoked', 'adopted')),
    actor TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000)
) STRICT;
CREATE INDEX gesang_configuration_events_stream ON gesang_configuration_events(stream_id, id);

CREATE TABLE gesang_custody (
    agent_id BLOB NOT NULL PRIMARY KEY REFERENCES gesang_agents(id),
    revision INTEGER NOT NULL DEFAULT 0 CHECK (revision >= 0),
    active_version INTEGER,
    FOREIGN KEY (agent_id, active_version) REFERENCES gesang_custody_operations(agent_id, revision),
    CHECK (active_version IS NULL OR active_version <= revision)
) STRICT;

CREATE TABLE gesang_custody_operations (
    agent_id BLOB NOT NULL REFERENCES gesang_custody(agent_id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    operation_id BLOB NOT NULL,
    expected_revision INTEGER NOT NULL CHECK (expected_revision >= 0),
    kind TEXT NOT NULL CHECK (kind IN ('enrolled', 'revoked')),
    owner_id BLOB NOT NULL REFERENCES gesang_owners(id),
    engine_owner TEXT NOT NULL,
    input_digest BLOB NOT NULL,
    sealed_key BLOB,
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    PRIMARY KEY (agent_id, revision),
    UNIQUE (agent_id, operation_id),
    CHECK ((kind = 'enrolled') = (sealed_key IS NOT NULL)),
    CHECK (revision = expected_revision + 1)
) STRICT;

CREATE TABLE gesang_runtime_retirements (
    id BLOB NOT NULL PRIMARY KEY,
    agent_id BLOB NOT NULL REFERENCES gesang_agents(id),
    generation INTEGER NOT NULL CHECK (generation >= 0),
    previous_runtime TEXT,
    custody_revision INTEGER NOT NULL,
    requested_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    claimed_by BLOB,
    claimed_until INTEGER,
    finished_at INTEGER,
    FOREIGN KEY (agent_id, custody_revision) REFERENCES gesang_custody_operations(agent_id, revision),
    CHECK ((claimed_by IS NULL) = (claimed_until IS NULL))
) STRICT;
CREATE UNIQUE INDEX gesang_one_pending_retirement ON gesang_runtime_retirements(agent_id)
    WHERE finished_at IS NULL;
CREATE INDEX gesang_pending_retirements ON gesang_runtime_retirements(requested_at, id)
    WHERE finished_at IS NULL;

CREATE TABLE gesang_custody_releases (
    id INTEGER PRIMARY KEY,
    agent_id BLOB NOT NULL,
    revision INTEGER NOT NULL,
    runtime_id BLOB NOT NULL,
    generation INTEGER NOT NULL,
    released_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    FOREIGN KEY (agent_id, revision) REFERENCES gesang_custody_operations(agent_id, revision)
) STRICT;

CREATE TABLE gesang_instance_intents (
    id BLOB NOT NULL PRIMARY KEY,
    agent_id BLOB NOT NULL REFERENCES gesang_agents(id),
    generation INTEGER NOT NULL CHECK (generation > 0),
    plan TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('allocating','starting','unlocking','initializing','migrating','ready','draining','stopped')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    locator TEXT,
    migration_head TEXT,
    problem TEXT,
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    updated_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    UNIQUE (agent_id, generation),
    CHECK (phase IN ('allocating','stopped') OR locator IS NOT NULL),
    CHECK (migration_head IS NULL OR phase='stopped')
) STRICT;

CREATE TABLE gesang_instance_events (
    instance_id BLOB NOT NULL REFERENCES gesang_instance_intents(id),
    revision INTEGER NOT NULL,
    phase TEXT NOT NULL,
    detail TEXT NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    PRIMARY KEY (instance_id, revision)
) STRICT;

CREATE INDEX gesang_completed_migrations
ON gesang_instance_intents (agent_id, generation DESC)
WHERE plan->>'purpose'='migration'
    AND phase='stopped'
    AND migration_head IS NOT NULL
    AND problem IS NULL;

CREATE TABLE gesang_initializations (
    agent_id BLOB NOT NULL PRIMARY KEY REFERENCES gesang_custody(agent_id),
    operation_id BLOB NOT NULL,
    owner_id BLOB NOT NULL REFERENCES gesang_owners(id),
    engine_owner TEXT NOT NULL,
    plan_digest BLOB NOT NULL CHECK (length(plan_digest)=32),
    sealed_key BLOB NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','completed','revoked')),
    completed_runtime TEXT,
    completed_head TEXT,
    receipt_revision INTEGER,
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    UNIQUE (agent_id, operation_id),
    FOREIGN KEY (agent_id, receipt_revision) REFERENCES gesang_custody_operations(agent_id, revision),
    CHECK ((state='completed') = (completed_runtime IS NOT NULL)),
    CHECK ((state='completed') = (completed_head IS NOT NULL)),
    CHECK ((state='completed') = (receipt_revision IS NOT NULL))
) STRICT;

CREATE TABLE gesang_initialization_releases (
    id INTEGER PRIMARY KEY,
    agent_id BLOB NOT NULL,
    operation_id BLOB NOT NULL,
    runtime_id BLOB NOT NULL REFERENCES gesang_instance_intents(id),
    generation INTEGER NOT NULL CHECK (generation>0),
    released_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    FOREIGN KEY (agent_id, operation_id) REFERENCES gesang_initializations(agent_id, operation_id)
) STRICT;

CREATE TABLE gesang_package_pins (
    subject TEXT NOT NULL CHECK (
        COALESCE(json_type(subject) = 'object'
        AND subject->>'kind' IN ('owner', 'installation', 'resource')
        AND CASE subject->>'kind'
            WHEN 'owner' THEN json_type(subject,'$.ownerId') = 'text'
                AND length(unhex(subject->>'ownerId', '-')) = 16
                AND json_remove(subject, '$.kind', '$.ownerId') = '{}'
            WHEN 'installation' THEN json_type(subject,'$.binding') = 'object'
                AND json_remove(subject, '$.kind', '$.binding') = '{}'
            WHEN 'resource' THEN json_type(subject,'$.binding') = 'object'
                AND json_type(subject,'$.resourceId') = 'text'
                AND json_remove(subject, '$.kind', '$.binding', '$.resourceId') = '{}'
            ELSE false
        END, false)
    ),
    owner_id BLOB GENERATED ALWAYS AS (
        CASE WHEN subject->>'kind' = 'owner' THEN unhex(subject->>'ownerId', '-') END
    ) STORED REFERENCES gesang_owners(id),
    reference_id BLOB NOT NULL,
    package TEXT NOT NULL CHECK (length(package) > 0),
    version TEXT NOT NULL CHECK (length(version) > 0),
    digest TEXT NOT NULL CHECK (length(digest) > 0),
    PRIMARY KEY(subject, reference_id, package)
) STRICT;

CREATE TABLE gesang_resource_bindings (
    id BLOB NOT NULL PRIMARY KEY,
    binding TEXT NOT NULL CHECK (json_type(binding) = 'object'),
    principal TEXT NOT NULL CHECK (COALESCE(
        json_type(principal) = 'object'
        AND CASE principal->>'kind'
            WHEN 'owner' THEN json_type(principal,'$.ownerId') = 'text'
                AND length(unhex(principal->>'ownerId', '-')) = 16
                AND json_remove(principal, '$.kind', '$.ownerId') = '{}'
            WHEN 'service' THEN json_type(principal,'$.serviceId') = 'text'
                AND json_remove(principal, '$.kind', '$.serviceId') = '{}'
            ELSE false
        END, false)
    ),
    owner_id BLOB GENERATED ALWAYS AS (
        CASE WHEN principal->>'kind' = 'owner' THEN unhex(principal->>'ownerId', '-') END
    ) STORED REFERENCES gesang_owners(id),
    interface_name TEXT NOT NULL CHECK (length(interface_name) > 0),
    interface_version INTEGER NOT NULL CHECK (interface_version > 0),
    request_id BLOB NOT NULL,
    request_digest BLOB NOT NULL CHECK (length(request_digest) = 32),
    endpoint TEXT NOT NULL CHECK (json_type(endpoint) = 'object'),
    package TEXT NOT NULL CHECK (json_type(package) = 'object'),
    created_at INTEGER NOT NULL DEFAULT (CAST(ROUND(unixepoch('subsec') * 1000) AS INTEGER) * 1000),
    UNIQUE(binding, principal, interface_name, interface_version, request_id)
) STRICT;
