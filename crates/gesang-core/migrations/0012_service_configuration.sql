ALTER TABLE gesang_configuration_streams
    ADD CONSTRAINT gesang_configuration_scope CHECK (
        (scope_key = 'deployment' AND owner_id IS NULL)
        OR (owner_id IS NOT NULL AND scope_key = 'owner/' || owner_id::text)
        OR (owner_id IS NULL AND scope_key ~ (
            '^services/(deployment/[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}'
            || '|integration/[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}'
            || '/[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12})'
            || '(/install/[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12})?$'
        ))
    ),
    DROP CONSTRAINT gesang_configuration_streams_check;
