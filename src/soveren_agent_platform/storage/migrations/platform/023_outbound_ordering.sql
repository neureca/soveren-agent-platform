ALTER TABLE outbound_messages ADD COLUMN ordering_key TEXT;
ALTER TABLE outbound_messages ADD COLUMN ordering_position INTEGER;

CREATE UNIQUE INDEX idx_outbound_messages_ordering_position
  ON outbound_messages(tenant_id, source_id, channel, ordering_key, ordering_position)
  WHERE ordering_key IS NOT NULL;

CREATE INDEX idx_outbound_messages_ordering_state
  ON outbound_messages(tenant_id, source_id, channel, ordering_key, ordering_position, status)
  WHERE ordering_key IS NOT NULL;
