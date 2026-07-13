ALTER TABLE agent_runs ADD COLUMN operation_key TEXT;
ALTER TABLE agent_runs ADD COLUMN lease_token TEXT;

CREATE UNIQUE INDEX idx_agent_runs_tenant_operation
  ON agent_runs(tenant_id, operation_key)
  WHERE operation_key IS NOT NULL;
