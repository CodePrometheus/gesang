ALTER TABLE gesang_grants ADD COLUMN purpose text NOT NULL DEFAULT 'task'
    CHECK (purpose IN ('task', 'improvement'));
DROP INDEX gesang_one_active_grant;
CREATE UNIQUE INDEX gesang_one_active_grant_per_purpose ON gesang_grants(agent_id, purpose)
    WHERE revoked_at IS NULL;
