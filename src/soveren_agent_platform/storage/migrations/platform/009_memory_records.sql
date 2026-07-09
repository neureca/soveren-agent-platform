CREATE TABLE memory_records (
  id               TEXT    PRIMARY KEY,
  tenant_id        TEXT    NOT NULL,
  scope            TEXT    NOT NULL,
  subject_id       TEXT    NOT NULL,
  kind             TEXT    NOT NULL DEFAULT 'note',
  text             TEXT    NOT NULL,
  metadata_json    TEXT    NOT NULL DEFAULT '{}',
  confidence       REAL    NOT NULL DEFAULT 1,
  source_id        TEXT,
  source_event_id  TEXT,
  created_by       TEXT,
  idempotency_key  TEXT,
  expires_at       INTEGER,
  deleted_at       INTEGER,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_memory_records_idempotency
  ON memory_records(tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_memory_records_subject
  ON memory_records(tenant_id, scope, subject_id, updated_at DESC);

CREATE INDEX idx_memory_records_kind
  ON memory_records(tenant_id, kind, updated_at DESC);
