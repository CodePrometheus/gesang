CREATE INDEX gesang_completed_migrations
ON gesang_instance_intents (agent_id, generation DESC)
WHERE plan->>'purpose'='migration'
    AND phase='stopped'
    AND migration_head IS NOT NULL
    AND problem IS NULL;
