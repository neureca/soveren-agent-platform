ALTER TABLE cron_jobs ADD COLUMN retry_at INTEGER;

DROP INDEX IF EXISTS idx_cron_jobs_due;

CREATE INDEX idx_cron_jobs_due
  ON cron_jobs(status, retry_at, run_at);
