CREATE TABLE gesang_workspace_resources (
    resource_key text PRIMARY KEY CHECK (length(resource_key) > 0),
    agent_id uuid NOT NULL REFERENCES gesang_agents(id)
);
CREATE INDEX gesang_workspace_resources_agent ON gesang_workspace_resources(agent_id, resource_key);
