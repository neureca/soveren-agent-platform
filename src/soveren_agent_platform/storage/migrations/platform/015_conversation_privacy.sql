CREATE TABLE actions_v015 (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
  source_id       TEXT    NOT NULL,
  run_id          TEXT,
  kind            TEXT    NOT NULL,
  payload_json    TEXT    NOT NULL,
  status          TEXT    NOT NULL
                          CHECK(status IN ('pending','approved','denied','queued','executing','executed','failed','cancelled','uncertain')),
  approval_policy TEXT    NOT NULL DEFAULT 'manual',
  source_event_id TEXT,
  idempotency_key TEXT,
  approved_by     TEXT,
  approved_at     INTEGER,
  executed_at     INTEGER,
  result_json     TEXT,
  last_error      TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  UNIQUE(tenant_id, source_id, idempotency_key)
);

INSERT INTO actions_v015 (
  id, tenant_id, source_id, run_id, kind, payload_json, status,
  approval_policy, source_event_id, idempotency_key, approved_by, approved_at,
  executed_at, result_json, last_error, created_at, updated_at
)
SELECT
  id, tenant_id,
  CASE WHEN source_id IS NULL OR trim(source_id) = '' THEN '__legacy_unscoped__:' || id ELSE source_id END,
  run_id, kind, payload_json, status, approval_policy, source_event_id,
  idempotency_key, approved_by, approved_at, executed_at, result_json,
  last_error, created_at, updated_at
FROM actions;

DROP TABLE actions;
ALTER TABLE actions_v015 RENAME TO actions;

CREATE INDEX idx_actions_status_created
  ON actions(status, created_at);

CREATE INDEX idx_actions_run
  ON actions(run_id);

CREATE INDEX idx_actions_conversation_status
  ON actions(tenant_id, source_id, status, updated_at DESC);

CREATE TABLE outbound_messages_v015 (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
  source_id       TEXT    NOT NULL,
  channel         TEXT    NOT NULL,
  destination_id  TEXT    NOT NULL,
  text            TEXT    NOT NULL,
  payload_json    TEXT    NOT NULL DEFAULT '{}',
  status          TEXT    NOT NULL
                          CHECK(status IN ('queued','leased','sending','sent','retrying','uncertain','dead_letter','cancelled')),
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
  result_json     TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  UNIQUE(tenant_id, source_id, idempotency_key)
);

INSERT INTO outbound_messages_v015 (
  id, tenant_id, source_id, channel, destination_id, text, payload_json,
  status, priority, run_after, lease_owner, lease_until, lease_token, attempts,
  max_attempts, idempotency_key, correlation_id, last_error, sent_at,
  result_json, created_at, updated_at
)
SELECT
  id, tenant_id, '__legacy_unscoped__:' || id, channel, destination_id, text,
  payload_json, status, priority, run_after, lease_owner, lease_until,
  lease_token, attempts, max_attempts, idempotency_key, correlation_id,
  last_error, sent_at, result_json, created_at, updated_at
FROM outbound_messages;

DROP TABLE outbound_messages;
ALTER TABLE outbound_messages_v015 RENAME TO outbound_messages;

CREATE INDEX idx_outbound_messages_due
  ON outbound_messages(channel, status, run_after, priority);

CREATE INDEX idx_outbound_messages_conversation_status
  ON outbound_messages(tenant_id, source_id, status, updated_at DESC);

CREATE TABLE cron_jobs_v015 (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
  source_id       TEXT    NOT NULL,
  name            TEXT    NOT NULL,
  payload_json    TEXT    NOT NULL,
  status          TEXT    NOT NULL
                          CHECK(status IN ('pending','leased','running','fired','uncertain','cancelled','dead_letter')),
  run_at          INTEGER NOT NULL,
  rrule           TEXT,
  timezone        TEXT    NOT NULL DEFAULT 'UTC',
  lease_owner     TEXT,
  lease_until     INTEGER,
  lease_token     TEXT,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 5,
  idempotency_key TEXT,
  last_error      TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL
);

INSERT INTO cron_jobs_v015 (
  id, tenant_id, source_id, name, payload_json, status, run_at, rrule,
  timezone, lease_owner, lease_until, lease_token, attempts, max_attempts,
  idempotency_key, last_error, created_at, updated_at
)
SELECT
  id, tenant_id, '__legacy_unscoped__:' || id, name, payload_json, status,
  run_at, rrule, timezone, lease_owner, lease_until, lease_token, attempts,
  max_attempts, idempotency_key, last_error, created_at, updated_at
FROM cron_jobs;

DROP TABLE cron_jobs;
ALTER TABLE cron_jobs_v015 RENAME TO cron_jobs;

CREATE INDEX idx_cron_jobs_due
  ON cron_jobs(status, run_at);

CREATE INDEX idx_cron_jobs_tenant_name
  ON cron_jobs(tenant_id, name);

CREATE UNIQUE INDEX idx_cron_jobs_conversation_idempotency
  ON cron_jobs(tenant_id, source_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_cron_jobs_conversation_status
  ON cron_jobs(tenant_id, source_id, status, run_at);

CREATE TABLE inbound_batch_messages_v015 (
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
  UNIQUE(tenant_id, source_id, raw_event_id)
);

INSERT INTO inbound_batch_messages_v015 (
  id, batch_id, tenant_id, channel, source_id, raw_event_id, source_event_id,
  payload_json, message_at, created_at
)
SELECT
  id, batch_id, tenant_id, channel, source_id, raw_event_id, source_event_id,
  payload_json, message_at, created_at
FROM inbound_batch_messages;

DROP TABLE inbound_batch_messages;
ALTER TABLE inbound_batch_messages_v015 RENAME TO inbound_batch_messages;

CREATE INDEX idx_inbound_batch_messages_batch
  ON inbound_batch_messages(batch_id, created_at);

DROP INDEX idx_session_mailbox_action;
DROP INDEX idx_session_mailbox_tenant_idempotency;

CREATE UNIQUE INDEX idx_session_mailbox_action
  ON session_mailbox(tenant_id, source_id, action_id)
  WHERE action_id IS NOT NULL;

CREATE UNIQUE INDEX idx_session_mailbox_conversation_idempotency
  ON session_mailbox(tenant_id, source_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE TABLE memory_records_v015 (
  id               TEXT    PRIMARY KEY,
  tenant_id        TEXT    NOT NULL,
  source_id        TEXT    NOT NULL,
  scope            TEXT    NOT NULL,
  subject_id       TEXT    NOT NULL,
  kind             TEXT    NOT NULL DEFAULT 'note',
  text             TEXT    NOT NULL,
  metadata_json    TEXT    NOT NULL DEFAULT '{}',
  confidence       REAL    NOT NULL DEFAULT 1,
  source_event_id  TEXT,
  created_by       TEXT,
  idempotency_key  TEXT,
  expires_at       INTEGER,
  deleted_at       INTEGER,
  created_at       INTEGER NOT NULL,
  updated_at       INTEGER NOT NULL
);

INSERT INTO memory_records_v015 (
  id, tenant_id, source_id, scope, subject_id, kind, text, metadata_json,
  confidence, source_event_id, created_by, idempotency_key, expires_at,
  deleted_at, created_at, updated_at
)
SELECT
  id, tenant_id,
  CASE WHEN source_id IS NULL OR trim(source_id) = '' THEN '__legacy_unscoped__:' || id ELSE source_id END,
  scope, subject_id, kind, text, metadata_json, confidence, source_event_id,
  created_by, idempotency_key, expires_at, deleted_at, created_at, updated_at
FROM memory_records;

DROP TABLE memory_records;
ALTER TABLE memory_records_v015 RENAME TO memory_records;

CREATE UNIQUE INDEX idx_memory_records_conversation_idempotency
  ON memory_records(tenant_id, source_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_memory_records_subject
  ON memory_records(tenant_id, source_id, scope, subject_id, updated_at DESC);

CREATE INDEX idx_memory_records_kind
  ON memory_records(tenant_id, source_id, kind, updated_at DESC);

CREATE TABLE effect_reconciliations_v015 (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
  source_id       TEXT    NOT NULL,
  effect_type     TEXT    NOT NULL
                          CHECK(effect_type IN ('action','outbound','cron')),
  effect_id       TEXT    NOT NULL,
  request_key     TEXT    NOT NULL,
  resolution      TEXT    NOT NULL,
  result_status   TEXT    NOT NULL,
  actor_id        TEXT    NOT NULL,
  evidence_json   TEXT    NOT NULL,
  created_at      INTEGER NOT NULL,
  UNIQUE(tenant_id, source_id, effect_type, request_key)
);

INSERT INTO effect_reconciliations_v015 (
  id, tenant_id, source_id, effect_type, effect_id, request_key, resolution,
  result_status, actor_id, evidence_json, created_at
)
SELECT
  reconciliation.id,
  reconciliation.tenant_id,
  COALESCE(
    CASE reconciliation.effect_type
      WHEN 'action' THEN (SELECT source_id FROM actions WHERE id = reconciliation.effect_id AND tenant_id = reconciliation.tenant_id)
      WHEN 'outbound' THEN (SELECT source_id FROM outbound_messages WHERE id = reconciliation.effect_id AND tenant_id = reconciliation.tenant_id)
      WHEN 'cron' THEN (SELECT source_id FROM cron_jobs WHERE id = reconciliation.effect_id AND tenant_id = reconciliation.tenant_id)
    END,
    '__legacy_unscoped__:' || reconciliation.id
  ),
  reconciliation.effect_type,
  reconciliation.effect_id,
  reconciliation.request_key,
  reconciliation.resolution,
  reconciliation.result_status,
  reconciliation.actor_id,
  reconciliation.evidence_json,
  reconciliation.created_at
FROM effect_reconciliations reconciliation;

DROP TABLE effect_reconciliations;
ALTER TABLE effect_reconciliations_v015 RENAME TO effect_reconciliations;

CREATE INDEX idx_effect_reconciliations_effect
  ON effect_reconciliations(tenant_id, source_id, effect_type, effect_id, created_at);
