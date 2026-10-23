ALTER TABLE outbound_messages ADD COLUMN routing_question_revision INTEGER;
ALTER TABLE dialogue_confirmed_references ADD COLUMN question_revision INTEGER;

-- Historical routing-question rows were all issued at the initial generation.
UPDATE outbound_messages
   SET routing_question_revision = 1
 WHERE routing_question_id IS NOT NULL;
UPDATE dialogue_confirmed_references
   SET question_revision = 1
 WHERE question_id IS NOT NULL;

CREATE INDEX ix_dialogue_confirmed_question_generation
  ON dialogue_confirmed_references(tenant_id, source_id, question_id, question_revision);
