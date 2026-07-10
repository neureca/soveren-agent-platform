ALTER TABLE session_mailbox ADD COLUMN accepted_at INTEGER;
ALTER TABLE session_mailbox ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE session_mailbox ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3;
ALTER TABLE session_mailbox ADD COLUMN run_after INTEGER NOT NULL DEFAULT 0;
ALTER TABLE session_mailbox ADD COLUMN backend_receipt_json TEXT;

CREATE INDEX idx_session_mailbox_delivery
  ON session_mailbox(tenant_id, status, accepted_at, run_after, updated_at);
