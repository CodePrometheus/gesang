CREATE TABLE gesang_owners (
    id uuid PRIMARY KEY,
    issuer text NOT NULL,
    subject text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (issuer, subject)
);

CREATE TABLE gesang_agents (
    id uuid PRIMARY KEY,
    owner_id uuid NOT NULL UNIQUE REFERENCES gesang_owners(id),
    engine text NOT NULL,
    engine_owner text NOT NULL,
    legacy_reference text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
    controller_id uuid,
    lease_until timestamptz,
    runtime_id uuid,
    runtime_generation bigint,
    runtime_locator text,
    CHECK ((runtime_id IS NULL) = (runtime_generation IS NULL)),
    CHECK ((runtime_id IS NULL) = (runtime_locator IS NULL))
);

CREATE TABLE gesang_grants (
    id uuid PRIMARY KEY,
    agent_id uuid NOT NULL REFERENCES gesang_agents(id),
    version bigint NOT NULL CHECK (version > 0),
    mode text NOT NULL CHECK (mode IN ('full_access', 'scoped', 'ask')),
    scopes jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    revoked_at timestamptz
);
CREATE UNIQUE INDEX gesang_one_active_grant ON gesang_grants(agent_id) WHERE revoked_at IS NULL;

CREATE TABLE gesang_commands (
    request_id uuid PRIMARY KEY,
    agent_id uuid NOT NULL REFERENCES gesang_agents(id),
    grant_id uuid NOT NULL REFERENCES gesang_grants(id),
    grant_version bigint NOT NULL,
    content_digest bytea NOT NULL,
    sealed_command bytea NOT NULL,
    requirements jsonb NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'accepted', 'rejected')),
    sealed_receipt bytea,
    problem jsonb,
    delivery_owner uuid,
    delivery_until timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    acknowledged_at timestamptz,
    CHECK ((state = 'accepted') = (sealed_receipt IS NOT NULL)),
    CHECK ((state = 'rejected') = (problem IS NOT NULL))
);
CREATE INDEX gesang_commands_pending ON gesang_commands(created_at, request_id) WHERE state = 'pending';

CREATE TABLE gesang_wakes (
    agent_id uuid PRIMARY KEY REFERENCES gesang_agents(id),
    due_at timestamptz NOT NULL,
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    claimed_by uuid,
    claimed_until timestamptz
);
CREATE INDEX gesang_wakes_due ON gesang_wakes(due_at);

CREATE TABLE gesang_anchors (
    agent_id uuid PRIMARY KEY REFERENCES gesang_agents(id),
    sequence bigint NOT NULL DEFAULT 0 CHECK (sequence >= 0),
    digest bytea NOT NULL,
    pending_id uuid,
    pending_generation bigint,
    pending_sequence bigint,
    pending_digest bytea,
    CHECK ((pending_id IS NULL) = (pending_generation IS NULL)),
    CHECK ((pending_id IS NULL) = (pending_sequence IS NULL)),
    CHECK ((pending_id IS NULL) = (pending_digest IS NULL))
);

