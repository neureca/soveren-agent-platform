CREATE TABLE outbound_messages_v012 (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
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
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  UNIQUE(tenant_id, idempotency_key)
);

INSERT INTO outbound_messages_v012 (
  id, tenant_id, channel, destination_id, text, payload_json, status, priority,
  run_after, lease_owner, lease_until, lease_token, attempts, max_attempts,
  idempotency_key, correlation_id, last_error, sent_at, created_at, updated_at
)
SELECT
  id, tenant_id, channel, destination_id, text, payload_json, status, priority,
  run_after, lease_owner, lease_until, lease_token, attempts, max_attempts,
  idempotency_key, correlation_id, last_error, sent_at, created_at, updated_at
FROM outbound_messages;

DROP TABLE outbound_messages;
ALTER TABLE outbound_messages_v012 RENAME TO outbound_messages;

CREATE INDEX idx_outbound_messages_due
  ON outbound_messages(channel, status, run_after, priority);

CREATE TABLE cron_jobs_v012 (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
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

INSERT INTO cron_jobs_v012 (
  id, tenant_id, name, payload_json, status, run_at, rrule, timezone,
  lease_owner, lease_until, lease_token, attempts, max_attempts,
  idempotency_key, last_error, created_at, updated_at
)
SELECT
  id, tenant_id, name, payload_json, status, run_at, rrule, timezone,
  lease_owner, lease_until, lease_token, attempts, max_attempts,
  idempotency_key, last_error, created_at, updated_at
FROM cron_jobs;

DROP TABLE cron_jobs;
ALTER TABLE cron_jobs_v012 RENAME TO cron_jobs;

CREATE INDEX idx_cron_jobs_due
  ON cron_jobs(status, run_at);

CREATE INDEX idx_cron_jobs_tenant_name
  ON cron_jobs(tenant_id, name);

CREATE UNIQUE INDEX idx_cron_jobs_tenant_idempotency
  ON cron_jobs(tenant_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;
