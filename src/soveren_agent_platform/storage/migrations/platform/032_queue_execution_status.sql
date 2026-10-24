CREATE INDEX ix_event_queue_scope_status
ON event_queue(tenant_id,recipient,json_extract(payload_json,'$.source_id'),status,created_at DESC);
