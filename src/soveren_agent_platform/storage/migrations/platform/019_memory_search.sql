CREATE VIRTUAL TABLE memory_records_fts USING fts5(
  kind,
  text,
  metadata_json,
  content='memory_records',
  content_rowid='rowid',
  tokenize='unicode61'
);

INSERT INTO memory_records_fts(rowid, kind, text, metadata_json)
SELECT rowid, kind, text, metadata_json FROM memory_records;

CREATE TRIGGER memory_records_fts_insert AFTER INSERT ON memory_records BEGIN
  INSERT INTO memory_records_fts(rowid, kind, text, metadata_json)
  VALUES (new.rowid, new.kind, new.text, new.metadata_json);
END;

CREATE TRIGGER memory_records_fts_delete AFTER DELETE ON memory_records BEGIN
  INSERT INTO memory_records_fts(memory_records_fts, rowid, kind, text, metadata_json)
  VALUES ('delete', old.rowid, old.kind, old.text, old.metadata_json);
END;

CREATE TRIGGER memory_records_fts_update AFTER UPDATE ON memory_records BEGIN
  INSERT INTO memory_records_fts(memory_records_fts, rowid, kind, text, metadata_json)
  VALUES ('delete', old.rowid, old.kind, old.text, old.metadata_json);
  INSERT INTO memory_records_fts(rowid, kind, text, metadata_json)
  VALUES (new.rowid, new.kind, new.text, new.metadata_json);
END;
