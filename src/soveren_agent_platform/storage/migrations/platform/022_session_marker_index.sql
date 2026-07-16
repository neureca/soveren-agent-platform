CREATE INDEX idx_runtime_session_events_marker
  ON runtime_session_events(session_id, marker)
  WHERE marker IS NOT NULL;
