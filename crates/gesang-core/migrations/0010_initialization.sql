ALTER TABLE gesang_instance_intents DROP CONSTRAINT gesang_instance_intents_phase_check;
ALTER TABLE gesang_instance_intents ADD CONSTRAINT gesang_instance_intents_phase_check
    CHECK (phase IN ('allocating','starting','unlocking','initializing','migrating','ready','draining','stopped'));

CREATE TABLE gesang_initializations (
    agent_id uuid PRIMARY KEY REFERENCES gesang_custody(agent_id),
    operation_id uuid NOT NULL,
    owner_id uuid NOT NULL REFERENCES gesang_owners(id),
    engine_owner text NOT NULL,
    plan_digest bytea NOT NULL CHECK (octet_length(plan_digest)=32),
    sealed_key bytea NOT NULL,
    state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','completed','revoked')),
    completed_runtime jsonb,
    completed_head jsonb,
    receipt_revision bigint,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (agent_id, operation_id),
    FOREIGN KEY (agent_id, receipt_revision) REFERENCES gesang_custody_operations(agent_id, revision),
    CHECK ((state='completed') = (completed_runtime IS NOT NULL)),
    CHECK ((state='completed') = (completed_head IS NOT NULL)),
    CHECK ((state='completed') = (receipt_revision IS NOT NULL))
);

CREATE TABLE gesang_initialization_releases (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    agent_id uuid NOT NULL,
    operation_id uuid NOT NULL,
    runtime_id uuid NOT NULL REFERENCES gesang_instance_intents(id),
    generation bigint NOT NULL CHECK (generation>0),
    released_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (agent_id, operation_id) REFERENCES gesang_initializations(agent_id, operation_id)
);
