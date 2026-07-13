ALTER TABLE outbound_messages ADD COLUMN result_json TEXT;

CREATE TABLE effect_reconciliations (
  id              TEXT    PRIMARY KEY,
  tenant_id       TEXT    NOT NULL,
  effect_type     TEXT    NOT NULL
                          CHECK(effect_type IN ('action','outbound','cron')),
  effect_id       TEXT    NOT NULL,
  request_key     TEXT    NOT NULL,
  resolution      TEXT    NOT NULL,
  result_status   TEXT    NOT NULL,
  actor_id        TEXT    NOT NULL,
  evidence_json   TEXT    NOT NULL,
  created_at      INTEGER NOT NULL,
  UNIQUE(tenant_id, effect_type, request_key)
);

CREATE INDEX idx_effect_reconciliations_effect
  ON effect_reconciliations(tenant_id, effect_type, effect_id, created_at);
