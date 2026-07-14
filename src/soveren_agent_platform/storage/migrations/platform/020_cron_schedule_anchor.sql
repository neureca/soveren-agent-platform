CREATE TABLE cron_jobs_v020 (
  id                      TEXT    PRIMARY KEY,
  tenant_id               TEXT    NOT NULL,
  source_id               TEXT    NOT NULL,
  name                    TEXT    NOT NULL,
  payload_json            TEXT    NOT NULL,
  status                  TEXT    NOT NULL
                                  CHECK(status IN ('pending','leased','running','fired','uncertain','cancelled','dead_letter')),
  schedule_anchor_at      INTEGER NOT NULL,
  run_at                  INTEGER NOT NULL,
  retry_at                INTEGER,
  rrule                   TEXT,
  timezone                TEXT    NOT NULL DEFAULT 'UTC',
  lease_owner             TEXT,
  lease_until             INTEGER,
  lease_token             TEXT,
  attempts                INTEGER NOT NULL DEFAULT 0,
  max_attempts            INTEGER NOT NULL DEFAULT 5,
  idempotency_key         TEXT,
  idempotency_fingerprint TEXT,
  last_error              TEXT,
  created_at              INTEGER NOT NULL,
  updated_at              INTEGER NOT NULL
);

INSERT INTO cron_jobs_v020 (
  id, tenant_id, source_id, name, payload_json, status, schedule_anchor_at,
  run_at, retry_at, rrule, timezone, lease_owner, lease_until, lease_token,
  attempts, max_attempts, idempotency_key, idempotency_fingerprint, last_error,
  created_at, updated_at
)
SELECT
  id, tenant_id, source_id, name, payload_json, status, run_at,
  run_at, retry_at, rrule, timezone, lease_owner, lease_until, lease_token,
  attempts, max_attempts, idempotency_key, idempotency_fingerprint, last_error,
  created_at, updated_at
FROM cron_jobs;

DROP TABLE cron_jobs;
ALTER TABLE cron_jobs_v020 RENAME TO cron_jobs;

CREATE INDEX idx_cron_jobs_due
  ON cron_jobs(status, retry_at, run_at);

CREATE INDEX idx_cron_jobs_tenant_name
  ON cron_jobs(tenant_id, name);

CREATE UNIQUE INDEX idx_cron_jobs_conversation_idempotency
  ON cron_jobs(tenant_id, source_id, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_cron_jobs_conversation_status
  ON cron_jobs(tenant_id, source_id, status, run_at);
