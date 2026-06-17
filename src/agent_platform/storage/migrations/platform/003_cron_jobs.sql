CREATE TABLE cron_jobs (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
  name            TEXT    NOT NULL,
  payload_json    TEXT    NOT NULL,
  status          TEXT    NOT NULL
                          CHECK(status IN ('pending','leased','fired','cancelled','dead_letter')),
  run_at          INTEGER NOT NULL,
  rrule           TEXT,
  timezone        TEXT    NOT NULL DEFAULT 'UTC',
  lease_owner     TEXT,
  lease_until     INTEGER,
  attempts        INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 5,
  last_error      TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL
);

CREATE INDEX idx_cron_jobs_due
  ON cron_jobs(status, run_at);

CREATE INDEX idx_cron_jobs_tenant_name
  ON cron_jobs(tenant_id, name);

