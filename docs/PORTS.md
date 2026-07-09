# Soveren Agent Platform Ports

## Direction

The platform should depend on runtime guarantees, not on SQLite tables. SQLite
is the bundled default adapter; ports and their semantics are the contract.

Do not introduce a generic CRUD repository such as `save(table, dict)`. That
would hide the important semantics: leases, idempotency, retry/dead-letter,
FIFO mailbox draining, and transactional state transitions.

Instead, define ports per runtime boundary.

## Queue Port

Current port:

- `soveren_agent_platform.queue.contracts.DurableQueue`
- `soveren_agent_platform.queue.contracts.QueueEvent`
- `soveren_agent_platform.queue.sqlite.SQLiteEventQueue`

Required semantics:

- enqueue with `idempotency_key`
- claim due events with a lease
- reclaim expired leases
- mark done
- mark retry or dead-letter according to attempts

SQLite implements this with `event_queue`. RabbitMQ/SQS/NATS/Postgres/etc.
should implement the same semantics explicitly. If the broker does not support
delayed retries or idempotency natively, the adapter must provide that layer.

## Store Ports

The next database abstraction should be module-specific:

- `ActionStore`: insert action, approve/deny, mark executing/queued/executed/failed
- `ActionDispatchEffects`: create an action and atomically route auto-approved actions to execution
- `OutboundQueue`: enqueue outbound, claim due by channel, mark sent/retry
- `CronStore`: insert job, claim due, complete recurring/one-shot jobs, fail
- `SessionStore`: get session, set status
- `SessionMailboxStore`: enqueue prompt, claim next for idle session, mark sent/requeue/fail
- `SessionLifecyclePolicy` / `close_session` / `close_idle_sessions`: backend-aware session teardown and idle cleanup
- `SessionInspector`: backend-specific live context reader for Codex, Claude, or other execution backends
- `SessionSnapshotStore`: refresh/latest searchable session context snapshots
- `BatchStore`: append inbound message, load batch state, atomically route batch into the next durable queue
- `RunStore`: insert/finalize planner runs
- `MemoryStore`: remember/search/get/forget explicit app-neutral memory records
- `SandboxRuntime`: acquire/destroy an execution sandbox, ensure container
  directories, and build a bounded exec command for session backends

Each port should encode atomic operations, not expose table-shaped CRUD.

Implemented store ports:

- `soveren_agent_platform.actions.contracts.ActionStore`
- `soveren_agent_platform.actions.sqlite.SQLiteActionStore`
- `soveren_agent_platform.decisions.effects.ActionDispatchEffects`
- `soveren_agent_platform.decisions.sqlite.SQLiteActionDispatchEffects`
- `soveren_agent_platform.outbound.contracts.OutboundQueue`
- `soveren_agent_platform.outbound.sqlite.SQLiteOutboundQueue`
- `soveren_agent_platform.cron.contracts.CronStore`
- `soveren_agent_platform.cron.sqlite.SQLiteCronStore`
- `soveren_agent_platform.batching.contracts.BatchStore`
- `soveren_agent_platform.batching.sqlite.SQLiteBatchStore`
- `soveren_agent_platform.sessions.contracts.SessionStore`
- `soveren_agent_platform.sessions.contracts.SessionMailboxStore`
- `soveren_agent_platform.sessions.contracts.SessionInspector`
- `soveren_agent_platform.sessions.contracts.SessionSnapshotStore`
- `soveren_agent_platform.sessions.lifecycle.SessionLifecyclePolicy`
- `soveren_agent_platform.sessions.lifecycle.close_session`
- `soveren_agent_platform.sessions.lifecycle.close_idle_sessions`
- `soveren_agent_platform.sessions.sqlite.SQLiteSessionStore`
- `soveren_agent_platform.sessions.sqlite.SQLiteSessionMailboxStore`
- `soveren_agent_platform.sessions.sqlite.SQLiteSessionSnapshotStore`
- `soveren_agent_platform.runs.contracts.RunStore`
- `soveren_agent_platform.runs.sqlite.SQLiteRunStore`
- `soveren_agent_platform.memory.contracts.MemoryStore`
- `soveren_agent_platform.memory.sqlite.SQLiteMemoryStore`
- `soveren_agent_platform.sandbox.contracts.SandboxRuntime`
- `soveren_agent_platform.sandbox.docker.DockerSandboxRuntime`

## Sandbox Port

Sandboxing is an optional execution-plane port. It exists so Codex, Claude, or
other tool-capable session backends can run behind a tenant boundary without
making the whole platform depend on one sandbox product.

The port is deliberately narrow:

- `acquire(SandboxSpec) -> SandboxHandle`
- `destroy(SandboxHandle)`
- `ensure_directory(SandboxHandle, path)`
- `exec_command(SandboxHandle, command, env, workdir, interactive)`

The bundled Docker implementation uses host Docker as a trusted infrastructure
dependency and creates sibling containers with memory, CPU, PID, and network
limits. It labels managed containers with a tenant hash, not the raw tenant id.
It rejects host network and container namespace sharing.

The Docker socket is not a tenant capability. Apps should expose Docker access
only through a runner/gateway boundary with fixed policy, never through model
tools, tenant sandboxes, or ordinary app handlers.

OpenShell, VM, remote runner, and Kubernetes implementations should implement
the same port and preserve the same ownership boundary.

## Memory Port

Memory is an explicit app-controlled capability, not implicit prompt state.
The bundled SQLite adapter stores `memory_records` with tenant, scope,
subject, kind, text, metadata, confidence, optional expiry, and a soft-delete
timestamp.

The reusable dynamic tool registration point is
`soveren_agent_platform.memory.register_memory_tools`, which exposes:

- `platform.memory/search_memory`
- `platform.memory/get_memory`
- `platform.memory/remember` only when `allow_write=True`
- `platform.memory/forget` only when `allow_write=True`

Apps decide whether to register these tools and whether memory results are
inserted into planner prompts. Write tools are disabled by default so model
access to memory remains an explicit policy choice.

## Session Indexing

Session routing is backend-neutral. Routers consume `runtime_sessions`,
`session_mailbox` state, and `runtime_session_context_snapshots`; they should
not call Codex, Claude, tmux, or app-server APIs directly.

Two workers own different parts of session lifecycle:

- `session_mailbox` owns delivery to a concrete session. It waits for idle
  sessions, marks them busy while sending, records input/output events, and
  refreshes the snapshot after successful capture.
- `session_indexer` owns asynchronous discovery/enrichment. It reads active
  sessions from `SessionStore`, delegates live reads to backend-specific
  `SessionInspector` implementations, records new observations, and refreshes
  snapshots.

Idle cleanup is exposed as helpers rather than a mandatory daemon:
`close_idle_sessions(...)` selects only idle sessions by TTL and per-source
active-session limits, skips sessions with `queued`/`sending` mailbox items,
delegates teardown to the registered `SessionBackend`, then records the
close/failure in platform tables. This keeps resource policy in the app while
keeping teardown semantics in the platform.

Mailbox enqueue and lifecycle claim use SQLite write transactions so either a
prompt is queued before cleanup sees pending work, or cleanup claims the session
before enqueue can target it. Enqueue is only accepted for `idle` or `busy`
sessions.

Codex app-server support is exposed as a `CodexThreadInspector`, behind the
generic `SessionInspector` port. App-specific routing LLMs may receive platform
tools such as `search_session_snapshots` or `get_session_context`, but those
tools must read the generalized platform index and only use backend inspectors
as bounded enrichment. The reusable dynamic tool registration point is
`soveren_agent_platform.sessions.register_session_directory_tools`, which exposes:

- `platform.sessions/list_runtime_sessions`
- `platform.sessions/search_session_snapshots`
- `platform.sessions/get_session_context`
- `platform.sessions/refresh_session_candidate`

## Migration Ports

The bundled SQLite adapter ships SQL migrations applied with namespace
`platform`. Other storage adapters should provide their own bootstrap/schema
management while preserving the same platform ports.

Implemented provider API:

```text
apply_platform_migrations(conn)
apply_app_migrations(conn, DirectoryMigrationProvider(path), namespace="poruchen")
apply_migrations(conn, PackageMigrationProvider(package, resource), namespace="app")
```

For existing SQLite apps, adoption must support a baseline/compatibility path:
if a table already exists and matches the platform contract, mark the platform
migration as applied instead of recreating the table.
