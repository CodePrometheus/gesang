CREATE TABLE gesang_service_credentials (
    id uuid PRIMARY KEY,
    token_hash bytea NOT NULL UNIQUE CHECK (octet_length(token_hash) = 32),
    binding jsonb NOT NULL CHECK (jsonb_typeof(binding) = 'object'),
    principal jsonb NOT NULL CHECK (jsonb_typeof(principal) = 'object'),
    scopes jsonb NOT NULL CHECK (jsonb_typeof(scopes) = 'array'),
    revision bigint NOT NULL DEFAULT 1 CHECK (revision > 0),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    issued_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
