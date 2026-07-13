ALTER TABLE event_queue ADD COLUMN idempotency_fingerprint TEXT;

ALTER TABLE outbound_messages ADD COLUMN idempotency_fingerprint TEXT;

ALTER TABLE cron_jobs ADD COLUMN idempotency_fingerprint TEXT;
