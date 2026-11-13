CREATE TABLE gesang_configuration_streams (
    id uuid PRIMARY KEY,
    scope_key text NOT NULL,
    owner_id uuid REFERENCES gesang_owners(id),
    component text NOT NULL CHECK (length(component) > 0),
    active_revision bigint,
    next_revision bigint NOT NULL DEFAULT 1 CHECK (next_revision > 0),
    UNIQUE (scope_key, component),
    CHECK ((scope_key = 'deployment' AND owner_id IS NULL) OR
           (owner_id IS NOT NULL AND scope_key = 'owner/' || owner_id::text))
);

CREATE TABLE gesang_configuration_secrets (
    id uuid PRIMARY KEY,
    stream_id uuid NOT NULL REFERENCES gesang_configuration_streams(id),
    name text NOT NULL CHECK (length(name) > 0),
    sealed_value bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    revoked_at timestamptz
);
CREATE INDEX gesang_configuration_secrets_stream ON gesang_configuration_secrets(stream_id, id);

CREATE TABLE gesang_configuration_revisions (
    stream_id uuid NOT NULL REFERENCES gesang_configuration_streams(id),
    revision bigint NOT NULL CHECK (revision > 0),
    operation_id uuid NOT NULL,
    expected_revision bigint,
    schema_version bigint NOT NULL CHECK (schema_version > 0),
    adoption_boundary text NOT NULL CHECK (adoption_boundary IN ('live', 'checkpoint', 'instance')),
    input_digest bytea NOT NULL,
    values jsonb NOT NULL,
    secret_references jsonb NOT NULL,
    rollback_of bigint,
    prepared_by jsonb NOT NULL,
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    published_by jsonb,
    published_at timestamptz,
    PRIMARY KEY (stream_id, revision),
    UNIQUE (stream_id, operation_id),
    FOREIGN KEY (stream_id, expected_revision) REFERENCES gesang_configuration_revisions(stream_id, revision),
    FOREIGN KEY (stream_id, rollback_of) REFERENCES gesang_configuration_revisions(stream_id, revision),
    CHECK ((published_by IS NULL) = (published_at IS NULL))
);
ALTER TABLE gesang_configuration_streams ADD CONSTRAINT gesang_configuration_active_revision
    FOREIGN KEY (id, active_revision) REFERENCES gesang_configuration_revisions(stream_id, revision);

CREATE TABLE gesang_configuration_adoptions (
    stream_id uuid NOT NULL REFERENCES gesang_configuration_streams(id),
    consumer_id uuid NOT NULL,
    revision bigint NOT NULL,
    adopted_by jsonb NOT NULL,
    adopted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (stream_id, consumer_id),
    FOREIGN KEY (stream_id, revision) REFERENCES gesang_configuration_revisions(stream_id, revision)
);

CREATE TABLE gesang_configuration_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    stream_id uuid NOT NULL REFERENCES gesang_configuration_streams(id),
    kind text NOT NULL CHECK (kind IN ('prepared', 'published', 'secret_revoked', 'adopted')),
    actor jsonb NOT NULL,
    detail jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX gesang_configuration_events_stream ON gesang_configuration_events(stream_id, id);
