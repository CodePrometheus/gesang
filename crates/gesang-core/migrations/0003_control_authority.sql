ALTER TABLE gesang_commands ALTER COLUMN grant_id DROP NOT NULL;
ALTER TABLE gesang_commands ALTER COLUMN grant_version DROP NOT NULL;
ALTER TABLE gesang_commands ADD COLUMN authority text NOT NULL DEFAULT 'grant' CHECK(authority IN ('owner','grant'));
ALTER TABLE gesang_commands ADD CONSTRAINT gesang_command_authority_binding CHECK (
    (authority='owner' AND grant_id IS NULL AND grant_version IS NULL)
    OR (authority='grant' AND grant_id IS NOT NULL AND grant_version IS NOT NULL)
);
