CREATE TABLE gesang_custody (
    agent_id uuid PRIMARY KEY REFERENCES gesang_agents(id),
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0),
    active_version bigint,
    CHECK (active_version IS NULL OR active_version <= revision)
);

CREATE TABLE gesang_custody_operations (
    agent_id uuid NOT NULL REFERENCES gesang_custody(agent_id),
    revision bigint NOT NULL CHECK (revision > 0),
    operation_id uuid NOT NULL,
    expected_revision bigint NOT NULL CHECK (expected_revision >= 0),
    kind text NOT NULL CHECK (kind IN ('enrolled', 'revoked')),
    owner_id uuid NOT NULL REFERENCES gesang_owners(id),
    engine_owner text NOT NULL,
    input_digest bytea NOT NULL,
    sealed_key bytea,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (agent_id, revision),
    UNIQUE (agent_id, operation_id),
    CHECK ((kind = 'enrolled') = (sealed_key IS NOT NULL)),
    CHECK (revision = expected_revision + 1)
);
ALTER TABLE gesang_custody ADD FOREIGN KEY (agent_id, active_version)
    REFERENCES gesang_custody_operations(agent_id, revision);

CREATE TABLE gesang_runtime_retirements (
    id uuid PRIMARY KEY,
    agent_id uuid NOT NULL REFERENCES gesang_agents(id),
    generation bigint NOT NULL CHECK (generation >= 0),
    previous_runtime jsonb,
    custody_revision bigint NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claimed_by uuid,
    claimed_until timestamptz,
    finished_at timestamptz,
    FOREIGN KEY (agent_id, custody_revision) REFERENCES gesang_custody_operations(agent_id, revision),
    CHECK ((claimed_by IS NULL) = (claimed_until IS NULL))
);
CREATE UNIQUE INDEX gesang_one_pending_retirement ON gesang_runtime_retirements(agent_id)
    WHERE finished_at IS NULL;
CREATE INDEX gesang_pending_retirements ON gesang_runtime_retirements(requested_at, id)
    WHERE finished_at IS NULL;

CREATE TABLE gesang_custody_releases (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    agent_id uuid NOT NULL,
    revision bigint NOT NULL,
    runtime_id uuid NOT NULL,
    generation bigint NOT NULL,
    released_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (agent_id, revision) REFERENCES gesang_custody_operations(agent_id, revision)
);
