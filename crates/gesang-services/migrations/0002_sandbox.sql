CREATE TABLE gesang_sandbox_instances (
    id uuid PRIMARY KEY REFERENCES gesang_resource_bindings(id),
    endpoint jsonb NOT NULL,
    profile text NOT NULL CHECK(length(profile)>0),
    creator jsonb NOT NULL,
    launch text NOT NULL CHECK(launch IN ('queued','issued','allocated','starting','started','failed')),
    physical_id text,
    stop_requested boolean NOT NULL DEFAULT false,
    removal text NOT NULL DEFAULT 'none' CHECK(removal IN ('none','requested','issued','stopped')),
    stopper jsonb,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((stop_requested AND stopper IS NOT NULL) OR (NOT stop_requested AND stopper IS NULL))
);

CREATE TABLE gesang_sandbox_operations (
    resource_id uuid NOT NULL REFERENCES gesang_sandbox_instances(id),
    request_id uuid NOT NULL,
    action text NOT NULL CHECK(action IN ('create','stop')),
    status text NOT NULL CHECK(status IN ('accepted','succeeded','failed','unknown')),
    problem text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(resource_id,request_id)
);

CREATE INDEX gesang_sandbox_pending ON gesang_sandbox_instances(id)
    WHERE removal IN ('requested','issued') OR launch IN ('queued','issued','allocated','starting');
