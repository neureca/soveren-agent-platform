CREATE TABLE decision_dispatches (
  id                    TEXT    PRIMARY KEY,
  tenant_id             TEXT    NOT NULL,
  source_id             TEXT    NOT NULL,
  trigger_event_id      TEXT    NOT NULL,
  input_fingerprint     TEXT    NOT NULL,
  status                TEXT    NOT NULL
                                  CHECK(status IN ('planning','dispatching','completed')),
  lease_token           TEXT,
  lease_until           INTEGER,
  run_id                TEXT,
  model                 TEXT,
  prompt_version        TEXT,
  decision_json         TEXT,
  decision_fingerprint  TEXT,
  planner_result_json   TEXT,
  dispatch_context_json TEXT,
  dispatch_target       TEXT,
  effect_id             TEXT,
  dispatch_result_json  TEXT,
  accepted_at           INTEGER,
  completed_at          INTEGER,
  created_at            INTEGER NOT NULL,
  updated_at            INTEGER NOT NULL,
  UNIQUE(tenant_id, source_id, trigger_event_id)
);

CREATE INDEX idx_decision_dispatches_status_lease
  ON decision_dispatches(status, lease_until);
