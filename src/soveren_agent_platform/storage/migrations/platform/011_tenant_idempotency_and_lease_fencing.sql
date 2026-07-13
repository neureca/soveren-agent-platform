CREATE TABLE event_queue_v011 (
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
  lease_token     TEXT,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 5,
  idempotency_key TEXT    NOT NULL,
  correlation_id  TEXT,
  causation_id    TEXT,
  last_error      TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  UNIQUE(tenant_id, idempotency_key)
);

INSERT INTO event_queue_v011 (
  id, tenant_id, recipient, message_type, schema_version, payload_json, status,
  priority, run_after, lease_owner, lease_until, lease_token, attempts,
  max_attempts, idempotency_key, correlation_id, causation_id, last_error,
  created_at, updated_at
)
SELECT
  id, tenant_id, recipient, message_type, schema_version, payload_json, status,
  priority, run_after, lease_owner, lease_until, NULL, attempts,
  max_attempts, idempotency_key, correlation_id, causation_id, last_error,
  created_at, updated_at
FROM event_queue;

DROP TABLE event_queue;
ALTER TABLE event_queue_v011 RENAME TO event_queue;

CREATE INDEX idx_event_queue_due
  ON event_queue(recipient, status, run_after, priority);

CREATE TABLE actions_v011 (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
  run_id          TEXT,
  kind            TEXT    NOT NULL,
  payload_json    TEXT    NOT NULL,
  status          TEXT    NOT NULL
                          CHECK(status IN ('pending','approved','denied','queued','executing','executed','failed','cancelled','uncertain')),
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
  UNIQUE(tenant_id, idempotency_key)
);

INSERT INTO actions_v011 (
  id, tenant_id, run_id, kind, payload_json, status, approval_policy, source_id,
  source_event_id, idempotency_key, approved_by, approved_at, executed_at,
  result_json, last_error, created_at, updated_at
)
SELECT
  id, tenant_id, run_id, kind, payload_json, status, approval_policy, source_id,
  source_event_id, idempotency_key, approved_by, approved_at, executed_at,
  result_json, last_error, created_at, updated_at
FROM actions;

UPDATE actions_v011
SET last_error = NULL
WHERE status = 'queued'
  AND EXISTS (
    SELECT 1
    FROM event_queue execution_event
    WHERE execution_event.tenant_id = actions_v011.tenant_id
      AND execution_event.correlation_id = actions_v011.id
      AND execution_event.message_type = 'ExecuteAction'
      AND execution_event.status = 'done'
  );

DROP TABLE actions;
ALTER TABLE actions_v011 RENAME TO actions;

CREATE INDEX idx_actions_status_created
  ON actions(status, created_at);

CREATE INDEX idx_actions_run
  ON actions(run_id);

CREATE TABLE outbound_messages_v011 (
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
  lease_token     TEXT,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 5,
  idempotency_key TEXT    NOT NULL,
  correlation_id  TEXT,
  last_error      TEXT,
  sent_at         INTEGER,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  UNIQUE(tenant_id, idempotency_key)
);

INSERT INTO outbound_messages_v011 (
  id, tenant_id, channel, destination_id, text, payload_json, status, priority,
  run_after, lease_owner, lease_until, lease_token, attempts, max_attempts,
  idempotency_key, correlation_id, last_error, sent_at, created_at, updated_at
)
SELECT
  id, tenant_id, channel, destination_id, text, payload_json, status, priority,
  run_after, lease_owner, lease_until, NULL, attempts, max_attempts,
  idempotency_key, correlation_id, last_error, sent_at, created_at, updated_at
FROM outbound_messages;

DROP TABLE outbound_messages;
ALTER TABLE outbound_messages_v011 RENAME TO outbound_messages;

CREATE INDEX idx_outbound_messages_due
  ON outbound_messages(channel, status, run_after, priority);

CREATE TABLE inbound_batch_messages_v011 (
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
  UNIQUE(tenant_id, raw_event_id)
);

INSERT INTO inbound_batch_messages_v011 (
  id, batch_id, tenant_id, channel, source_id, raw_event_id, source_event_id,
  payload_json, message_at, created_at
)
SELECT
  id, batch_id, tenant_id, channel, source_id, raw_event_id, source_event_id,
  payload_json, message_at, created_at
FROM inbound_batch_messages;

DROP TABLE inbound_batch_messages;
ALTER TABLE inbound_batch_messages_v011 RENAME TO inbound_batch_messages;

CREATE INDEX idx_inbound_batch_messages_batch
  ON inbound_batch_messages(batch_id, created_at);

ALTER TABLE cron_jobs ADD COLUMN lease_token TEXT;
ALTER TABLE cron_jobs ADD COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX idx_cron_jobs_tenant_idempotency
  ON cron_jobs(tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

ALTER TABLE session_mailbox ADD COLUMN idempotency_key TEXT;

DROP INDEX idx_session_mailbox_action;

CREATE UNIQUE INDEX idx_session_mailbox_action
  ON session_mailbox(tenant_id, action_id)
  WHERE action_id IS NOT NULL;

CREATE UNIQUE INDEX idx_session_mailbox_tenant_idempotency
  ON session_mailbox(tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
