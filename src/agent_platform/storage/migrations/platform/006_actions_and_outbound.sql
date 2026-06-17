CREATE TABLE actions (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
  run_id          TEXT,
  kind            TEXT    NOT NULL,
  payload_json    TEXT    NOT NULL,
  status          TEXT    NOT NULL
                          CHECK(status IN ('pending','approved','denied','queued','executing','executed','failed','cancelled')),
  approval_policy TEXT    NOT NULL DEFAULT 'manual',
  source_id       TEXT,
  source_event_id TEXT,
  idempotency_key TEXT,
  approved_by     TEXT,
  approved_at     INTEGER,
  executed_at     INTEGER,
  result_json     TEXT,
  last_error      TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  UNIQUE(idempotency_key)
);

CREATE INDEX idx_actions_status_created
  ON actions(status, created_at);

CREATE INDEX idx_actions_run
  ON actions(run_id);

CREATE TABLE outbound_messages (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
  channel         TEXT    NOT NULL,
  destination_id  TEXT    NOT NULL,
  text            TEXT    NOT NULL,
  payload_json    TEXT    NOT NULL DEFAULT '{}',
  status          TEXT    NOT NULL
                          CHECK(status IN ('queued','leased','sent','retrying','dead_letter','cancelled')),
  priority        INTEGER NOT NULL DEFAULT 100,
  run_after       INTEGER NOT NULL,
  lease_owner     TEXT,
  lease_until     INTEGER,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 5,
  idempotency_key TEXT    NOT NULL,
  correlation_id  TEXT,
  last_error      TEXT,
  sent_at         INTEGER,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  UNIQUE(idempotency_key)
);

CREATE INDEX idx_outbound_messages_due
  ON outbound_messages(channel, status, run_after, priority);

