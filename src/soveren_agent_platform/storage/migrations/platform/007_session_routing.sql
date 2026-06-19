CREATE TABLE runtime_session_events (
  id              TEXT    PRIMARY KEY,
  session_id      TEXT    NOT NULL REFERENCES runtime_sessions(id),
  action_id       TEXT,
  direction       TEXT    NOT NULL
                          CHECK(direction IN ('input','output','control')),
  payload_text    TEXT    NOT NULL,
  marker          TEXT,
  created_at      INTEGER NOT NULL
);

CREATE INDEX idx_runtime_session_events_by_session
  ON runtime_session_events(session_id, created_at);

CREATE TABLE runtime_session_context_snapshots (
  id                  TEXT    PRIMARY KEY,
  session_id          TEXT    NOT NULL REFERENCES runtime_sessions(id),
  version             INTEGER NOT NULL DEFAULT 1,
  source_event_id     TEXT,
  source_range_json   TEXT    NOT NULL DEFAULT '{}',
  summary             TEXT    NOT NULL DEFAULT '',
  keywords_json       TEXT    NOT NULL DEFAULT '[]',
  entities_json       TEXT    NOT NULL DEFAULT '[]',
  files_json          TEXT    NOT NULL DEFAULT '[]',
  cwd                 TEXT    NOT NULL DEFAULT '',
  branch              TEXT,
  topic_key           TEXT,
  open_questions_json TEXT    NOT NULL DEFAULT '[]',
  last_user_intent    TEXT,
  last_agent_state    TEXT,
  confidence          REAL    NOT NULL DEFAULT 0,
  created_at          INTEGER NOT NULL
);

CREATE INDEX idx_runtime_session_snapshots_latest
  ON runtime_session_context_snapshots(session_id, created_at DESC);

CREATE TABLE runtime_session_route_decisions (
  id                  TEXT    PRIMARY KEY,
  tenant_id           TEXT    NOT NULL,
  source_id           TEXT    NOT NULL,
  user_id             TEXT,
  preferred_kind      TEXT,
  fragment_text       TEXT    NOT NULL,
  selected_session_id TEXT,
  action              TEXT    NOT NULL
                              CHECK(action IN ('route_existing','ask_clarification','open_new','no_match')),
  confidence          REAL    NOT NULL DEFAULT 0,
  candidates_json     TEXT    NOT NULL DEFAULT '[]',
  reasons_json        TEXT    NOT NULL DEFAULT '[]',
  created_at          INTEGER NOT NULL,
  FOREIGN KEY (selected_session_id) REFERENCES runtime_sessions(id)
);

CREATE INDEX idx_runtime_session_route_decisions_source
  ON runtime_session_route_decisions(tenant_id, source_id, created_at DESC);

