CREATE INDEX idx_event_queue_tenant_due
  ON event_queue(tenant_id, recipient, status, run_after, priority);

CREATE INDEX idx_outbound_messages_tenant_due
  ON outbound_messages(tenant_id, channel, status, run_after, priority);
