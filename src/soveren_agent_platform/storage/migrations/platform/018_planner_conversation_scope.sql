ALTER TABLE agent_runs ADD COLUMN source_id TEXT;

DROP INDEX IF EXISTS idx_agent_runs_tenant_operation;

CREATE UNIQUE INDEX idx_agent_runs_conversation_operation
  ON agent_runs(tenant_id, source_id, operation_key)
  WHERE source_id IS NOT NULL AND operation_key IS NOT NULL;
