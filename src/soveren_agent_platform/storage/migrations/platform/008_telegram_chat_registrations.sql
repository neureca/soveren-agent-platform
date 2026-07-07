CREATE TABLE telegram_chat_registrations (
  tenant_id             TEXT    NOT NULL,
  chat_id               INTEGER NOT NULL,
  registered_by_user_id INTEGER NOT NULL,
  status                TEXT    NOT NULL
                                  CHECK(status IN ('allowed','revoked')),
  created_at            INTEGER NOT NULL,
  updated_at            INTEGER NOT NULL,
  PRIMARY KEY(tenant_id, chat_id)
);

CREATE INDEX idx_telegram_chat_registrations_status
  ON telegram_chat_registrations(tenant_id, status, updated_at);
