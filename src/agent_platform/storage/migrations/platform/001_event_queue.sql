CREATE TABLE event_queue (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
  recipient       TEXT    NOT NULL,
  message_type    TEXT    NOT NULL,
  schema_version  INTEGER NOT NULL DEFAULT 1,
  payload_json    TEXT    NOT NULL,
  status          TEXT    NOT NULL
                          CHECK(status IN ('queued','leased','done','retrying','dead_letter')),
  priority        INTEGER NOT NULL DEFAULT 100,
  run_after       INTEGER NOT NULL,
  lease_owner     TEXT,
  lease_until     INTEGER,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 5,
  idempotency_key TEXT    NOT NULL,
  correlation_id  TEXT,
  causation_id    TEXT,
  last_error      TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  UNIQUE(idempotency_key)
);

CREATE INDEX idx_event_queue_due
  ON event_queue(recipient, status, run_after, priority);

