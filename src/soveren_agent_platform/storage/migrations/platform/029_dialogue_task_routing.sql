ALTER TABLE outbound_messages ADD COLUMN task_id TEXT;
ALTER TABLE outbound_messages ADD COLUMN task_revision INTEGER;
ALTER TABLE outbound_messages ADD COLUMN routing_question_id TEXT;
ALTER TABLE outbound_messages ADD COLUMN reply_to_message_id INTEGER;
ALTER TABLE outbound_messages ADD COLUMN message_thread_id INTEGER;

CREATE TABLE dialogue_tasks (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    title TEXT NOT NULL,
    original_request TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','closed','cancelled')),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    terminal_at INTEGER,
    UNIQUE (tenant_id, source_id, id)
);

CREATE TABLE dialogue_questions (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    text TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','resolved')),
    revision INTEGER NOT NULL CHECK (revision >= 1),
    candidates_json TEXT NOT NULL,
    has_more_candidates INTEGER NOT NULL CHECK (has_more_candidates IN (0,1)),
    resolved_task_id TEXT,
    created_at INTEGER NOT NULL,
    resolved_at INTEGER,
    UNIQUE (tenant_id, source_id, id),
    FOREIGN KEY (tenant_id, source_id, resolved_task_id)
      REFERENCES dialogue_tasks(tenant_id, source_id, id)
);

CREATE TABLE dialogue_plans (
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    plan_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    result_json TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, source_id, plan_id)
);

CREATE TABLE dialogue_inputs (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    input_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    text TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    channel TEXT NOT NULL,
    source_message_id TEXT,
    reply_to_message_id TEXT,
    message_thread_id TEXT,
    task_id TEXT,
    question_id TEXT,
    plan_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    UNIQUE (tenant_id, source_id, input_id),
    CHECK (task_id IS NOT NULL OR question_id IS NOT NULL),
    FOREIGN KEY (tenant_id, source_id, task_id)
      REFERENCES dialogue_tasks(tenant_id, source_id, id),
    FOREIGN KEY (tenant_id, source_id, question_id)
      REFERENCES dialogue_questions(tenant_id, source_id, id)
);

CREATE TABLE dialogue_messages (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('user','assistant')),
    actor_id TEXT,
    text TEXT NOT NULL,
    occurred_at INTEGER NOT NULL,
    input_id TEXT,
    source_message_id TEXT,
    UNIQUE (tenant_id, source_id, message_id),
    UNIQUE (tenant_id, source_id, input_id),
    FOREIGN KEY (tenant_id, source_id, task_id)
      REFERENCES dialogue_tasks(tenant_id, source_id, id),
    FOREIGN KEY (tenant_id, source_id, input_id)
      REFERENCES dialogue_inputs(tenant_id, source_id, input_id)
);

CREATE TABLE dialogue_focus (
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    actor_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, source_id, actor_id),
    FOREIGN KEY (tenant_id, source_id, task_id)
      REFERENCES dialogue_tasks(tenant_id, source_id, id)
);

CREATE TABLE dialogue_confirmed_references (
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    channel TEXT NOT NULL,
    external_message_id TEXT NOT NULL,
    outbound_message_id TEXT NOT NULL,
    task_id TEXT,
    question_id TEXT,
    confirmed_at INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, source_id, channel, external_message_id),
    UNIQUE (tenant_id, source_id, outbound_message_id),
    CHECK ((task_id IS NOT NULL AND question_id IS NULL) OR
           (task_id IS NULL AND question_id IS NOT NULL)),
    FOREIGN KEY (tenant_id, source_id, task_id)
      REFERENCES dialogue_tasks(tenant_id, source_id, id),
    FOREIGN KEY (tenant_id, source_id, question_id)
      REFERENCES dialogue_questions(tenant_id, source_id, id)
);

CREATE INDEX ix_dialogue_tasks_candidates
  ON dialogue_tasks(tenant_id, source_id, status, updated_at DESC, id);
CREATE INDEX ix_dialogue_tasks_search
  ON dialogue_tasks(tenant_id, source_id, title);
CREATE INDEX ix_dialogue_messages_task_sequence
  ON dialogue_messages(tenant_id, source_id, task_id, sequence DESC);
CREATE INDEX ix_dialogue_questions_pending
  ON dialogue_questions(tenant_id, source_id, actor_id, status, created_at DESC);
CREATE INDEX ix_dialogue_inputs_task
  ON dialogue_inputs(tenant_id, source_id, task_id, occurred_at, input_id);
CREATE INDEX ix_dialogue_inputs_question
  ON dialogue_inputs(tenant_id, source_id, question_id, sequence);
CREATE UNIQUE INDEX ux_dialogue_inputs_source_message
  ON dialogue_inputs(tenant_id, source_id, channel, source_message_id)
  WHERE source_message_id IS NOT NULL;
CREATE INDEX ix_outbound_task_admission
  ON outbound_messages(tenant_id, source_id, task_id, status);
