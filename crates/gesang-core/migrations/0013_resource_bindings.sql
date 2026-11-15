CREATE TABLE gesang_resource_bindings (
    id uuid PRIMARY KEY,
    binding jsonb NOT NULL CHECK (jsonb_typeof(binding) = 'object'),
    principal jsonb NOT NULL CHECK (COALESCE(
        jsonb_typeof(principal) = 'object'
        AND CASE principal->>'kind'
            WHEN 'owner' THEN jsonb_typeof(principal->'ownerId') = 'string'
                AND principal - ARRAY['kind', 'ownerId'] = '{}'::jsonb
            WHEN 'service' THEN jsonb_typeof(principal->'serviceId') = 'string'
                AND principal - ARRAY['kind', 'serviceId'] = '{}'::jsonb
            ELSE false
        END, false)
    ),
    owner_id uuid GENERATED ALWAYS AS (
        CASE WHEN principal->>'kind' = 'owner' THEN (principal->>'ownerId')::uuid END
    ) STORED REFERENCES gesang_owners(id),
    interface_name text NOT NULL CHECK (length(interface_name) > 0),
    interface_version bigint NOT NULL CHECK (interface_version > 0),
    request_id uuid NOT NULL,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    endpoint jsonb NOT NULL CHECK (jsonb_typeof(endpoint) = 'object'),
    package jsonb NOT NULL CHECK (jsonb_typeof(package) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(binding, principal, interface_name, interface_version, request_id)
);
