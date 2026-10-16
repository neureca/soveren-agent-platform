-- Content-free latency summaries look up the parts of one conversation turn.
CREATE INDEX idx_outbound_turn_lookup
  ON outbound_messages(tenant_id, source_id, correlation_id, channel, status);
