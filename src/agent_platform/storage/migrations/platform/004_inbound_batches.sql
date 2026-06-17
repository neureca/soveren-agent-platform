CREATE TABLE inbound_batches (
  id                TEXT    PRIMARY KEY,
  tenant_id         TEXT    NOT NULL,
  channel           TEXT    NOT NULL,
  source_id         TEXT    NOT NULL,
  status            TEXT    NOT NULL
                            CHECK(status IN ('collecting','routed','cancelled')),
  first_message_at  INTEGER NOT NULL,
  last_message_at   INTEGER NOT NULL,
  message_count     INTEGER NOT NULL DEFAULT 0,
  decision_json     TEXT    NOT NULL DEFAULT '{}',
  created_at        INTEGER NOT NULL,
  updated_at        INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_inbound_batches_one_collecting
  ON inbound_batches(tenant_id, channel, source_id)
  WHERE status = 'collecting';

CREATE INDEX idx_inbound_batches_open
  ON inbound_batches(tenant_id, channel, source_id, status, updated_at);

CREATE TABLE inbound_batch_messages (
  id              TEXT    PRIMARY KEY,
  batch_id        TEXT    NOT NULL REFERENCES inbound_batches(id),
  tenant_id       TEXT    NOT NULL,
  channel         TEXT    NOT NULL,
  source_id       TEXT    NOT NULL,
  raw_event_id    TEXT    NOT NULL,
  source_event_id TEXT,
  payload_json    TEXT    NOT NULL,
  message_at      INTEGER NOT NULL,
  created_at      INTEGER NOT NULL,
  UNIQUE(raw_event_id)
);

CREATE INDEX idx_inbound_batch_messages_batch
  ON inbound_batch_messages(batch_id, created_at);

