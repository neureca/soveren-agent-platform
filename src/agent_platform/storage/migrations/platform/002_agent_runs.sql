CREATE TABLE agent_runs (
  id               TEXT    PRIMARY KEY,
  tenant_id        TEXT    NOT NULL,
  trigger_event_id TEXT    NOT NULL,
  status           TEXT    NOT NULL
                           CHECK(status IN ('running','waiting_approval','completed','failed')),
  input_summary    TEXT,
  output_json      TEXT,
  model            TEXT,
  prompt_version   TEXT,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL
);

CREATE INDEX idx_agent_runs_trigger ON agent_runs(trigger_event_id);
CREATE INDEX idx_agent_runs_status_created ON agent_runs(status, created_at);

