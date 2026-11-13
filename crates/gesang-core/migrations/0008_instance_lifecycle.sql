CREATE TABLE gesang_instance_intents (
    id uuid PRIMARY KEY,
    agent_id uuid NOT NULL REFERENCES gesang_agents(id),
    generation bigint NOT NULL CHECK (generation > 0),
    plan jsonb NOT NULL,
    phase text NOT NULL CHECK (phase IN ('allocating','starting','unlocking','migrating','ready','draining','stopped')),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    locator text,
    migration_head jsonb,
    problem jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (agent_id, generation),
    CHECK (phase IN ('allocating','stopped') OR locator IS NOT NULL),
    CHECK (migration_head IS NULL OR phase='stopped')
);

CREATE TABLE gesang_instance_events (
    instance_id uuid NOT NULL REFERENCES gesang_instance_intents(id),
    revision bigint NOT NULL,
    phase text NOT NULL,
    detail jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (instance_id, revision)
);

INSERT INTO gesang_instance_intents(id,agent_id,generation,plan,phase,locator)
SELECT runtime_id,id,runtime_generation,
    '{"purpose":"execution","provider":"existing","schemaVersion":1,"definition":{}}'::jsonb,
    'ready',runtime_locator FROM gesang_agents WHERE runtime_id IS NOT NULL;
INSERT INTO gesang_instance_events(instance_id,revision,phase,detail)
SELECT id,revision,phase,'{"adoptedExistingAdmission":true}'::jsonb FROM gesang_instance_intents;
