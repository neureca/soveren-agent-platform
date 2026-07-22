CREATE TABLE conversation_messages (
  id                TEXT    PRIMARY KEY,
  tenant_id         TEXT    NOT NULL,
  source_id         TEXT    NOT NULL,
  channel           TEXT    NOT NULL,
  direction         TEXT    NOT NULL CHECK(direction IN ('inbound','outbound')),
  author_id         TEXT,
  author_username   TEXT,
  author_display_name TEXT,
  text              TEXT    NOT NULL,
  source_message_id TEXT    NOT NULL,
  source_event_id   TEXT,
  metadata_json     TEXT    NOT NULL DEFAULT '{}',
  occurred_at       INTEGER NOT NULL,
  created_at        INTEGER NOT NULL,
  UNIQUE(tenant_id, source_id, direction, source_message_id)
);

CREATE INDEX idx_conversation_messages_recent
  ON conversation_messages(tenant_id, source_id, occurred_at DESC, id DESC);

INSERT INTO conversation_messages (
  id, tenant_id, source_id, channel, direction, author_id, author_username,
  author_display_name, text,
  source_message_id, source_event_id, metadata_json, occurred_at, created_at
)
SELECT
  'history_in_' || id,
  tenant_id,
  source_id,
  channel,
  'inbound',
  CAST(CASE WHEN json_valid(payload_json) THEN COALESCE(
    json_extract(payload_json, '$.user_id'),
    json_extract(payload_json, '$.from_user_id'),
    json_extract(payload_json, '$.sender_id'),
    json_extract(payload_json, '$.payload.user_id'),
    json_extract(payload_json, '$.payload.from_user_id'),
    json_extract(payload_json, '$.payload.sender_id')
  ) END AS TEXT),
  CASE WHEN json_valid(payload_json) THEN NULLIF(LTRIM(TRIM(CAST(COALESCE(
    json_extract(payload_json, '$.username'),
    json_extract(payload_json, '$.from_username'),
    json_extract(payload_json, '$.payload.username'),
    json_extract(payload_json, '$.payload.from_username')
  ) AS TEXT)), '@'), '') END,
  CASE WHEN json_valid(payload_json) THEN COALESCE(
    json_extract(payload_json, '$.display_name'),
    json_extract(payload_json, '$.author_name'),
    json_extract(payload_json, '$.sender_name'),
    NULLIF(TRIM(
      COALESCE(json_extract(payload_json, '$.first_name'), json_extract(payload_json, '$.from_first_name'), '')
      || ' ' ||
      COALESCE(json_extract(payload_json, '$.last_name'), json_extract(payload_json, '$.from_last_name'), '')
    ), ''),
    json_extract(payload_json, '$.payload.display_name'),
    json_extract(payload_json, '$.payload.author_name'),
    json_extract(payload_json, '$.payload.sender_name'),
    NULLIF(TRIM(
      COALESCE(
        json_extract(payload_json, '$.payload.first_name'),
        json_extract(payload_json, '$.payload.from_first_name'),
        ''
      )
      || ' ' ||
      COALESCE(
        json_extract(payload_json, '$.payload.last_name'),
        json_extract(payload_json, '$.payload.from_last_name'),
        ''
      )
    ), '')
  ) END,
  CASE WHEN json_valid(payload_json)
    THEN COALESCE(json_extract(payload_json, '$.text'), '')
    ELSE ''
  END,
  raw_event_id,
  source_event_id,
  '{}',
  message_at,
  created_at
FROM inbound_batch_messages;

INSERT INTO conversation_messages (
  id, tenant_id, source_id, channel, direction, author_id, author_username,
  author_display_name, text,
  source_message_id, source_event_id, metadata_json, occurred_at, created_at
)
SELECT
  'history_out_' || id,
  tenant_id,
  source_id,
  channel,
  'outbound',
  NULL,
  NULL,
  NULL,
  text,
  id,
  NULL,
  '{}',
  sent_at,
  created_at
FROM outbound_messages
WHERE status = 'sent' AND sent_at IS NOT NULL;

CREATE VIRTUAL TABLE conversation_messages_fts USING fts5(
  text,
  content='conversation_messages',
  content_rowid='rowid',
  tokenize='unicode61 remove_diacritics 2'
);

INSERT INTO conversation_messages_fts(rowid, text)
SELECT rowid, text FROM conversation_messages;

CREATE TRIGGER conversation_messages_fts_insert AFTER INSERT ON conversation_messages BEGIN
  INSERT INTO conversation_messages_fts(rowid, text) VALUES (new.rowid, new.text);
END;

CREATE TRIGGER conversation_messages_fts_delete AFTER DELETE ON conversation_messages BEGIN
  INSERT INTO conversation_messages_fts(conversation_messages_fts, rowid, text)
  VALUES ('delete', old.rowid, old.text);
END;

CREATE TRIGGER conversation_messages_fts_update AFTER UPDATE ON conversation_messages BEGIN
  INSERT INTO conversation_messages_fts(conversation_messages_fts, rowid, text)
  VALUES ('delete', old.rowid, old.text);
  INSERT INTO conversation_messages_fts(rowid, text) VALUES (new.rowid, new.text);
END;
