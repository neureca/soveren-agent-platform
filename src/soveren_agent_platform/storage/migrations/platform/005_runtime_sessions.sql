CREATE TABLE runtime_sessions (
  id                 TEXT    PRIMARY KEY,
  tenant_id          TEXT    NOT NULL,
  source_id          TEXT    NOT NULL,
  owner_id           TEXT,
  kind               TEXT    NOT NULL,
  backend            TEXT    NOT NULL,
  backend_session_id TEXT    NOT NULL,
  title              TEXT    NOT NULL DEFAULT '',
  cwd                TEXT    NOT NULL DEFAULT '',
  status             TEXT    NOT NULL
                              CHECK(status IN ('starting','idle','busy','closing','closed','failed')),
  current_action_id  TEXT,
  last_used_at       INTEGER,
  last_error         TEXT,
  metadata_json      TEXT    NOT NULL DEFAULT '{}',
  created_at         INTEGER NOT NULL,
  updated_at         INTEGER NOT NULL
);

CREATE INDEX idx_runtime_sessions_active
  ON runtime_sessions(tenant_id, source_id, kind, status, last_used_at);

CREATE INDEX idx_runtime_sessions_backend
  ON runtime_sessions(backend, backend_session_id);

CREATE TABLE session_mailbox (
  id              TEXT    PRIMARY KEY,
  session_id      TEXT    NOT NULL REFERENCES runtime_sessions(id),
  tenant_id       TEXT    NOT NULL,
  source_id       TEXT    NOT NULL,
  source_event_id TEXT,
  action_id       TEXT,
  prompt          TEXT    NOT NULL,
  status          TEXT    NOT NULL
                          CHECK(status IN ('queued','sending','sent','failed','cancelled')),
  result_json     TEXT,
  last_error      TEXT,
  sent_at         INTEGER,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL
);

CREATE UNIQUE INDEX idx_session_mailbox_action
  ON session_mailbox(action_id)
  WHERE action_id IS NOT NULL;

CREATE INDEX idx_session_mailbox_ready
  ON session_mailbox(tenant_id, session_id, status, created_at);

