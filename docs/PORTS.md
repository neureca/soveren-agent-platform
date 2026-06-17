# Agent Platform Ports

## Direction

The platform should depend on runtime guarantees, not on SQLite tables.

Do not introduce a generic CRUD repository such as `save(table, dict)`. That
would hide the important semantics: leases, idempotency, retry/dead-letter,
FIFO mailbox draining, and transactional state transitions.

Instead, define ports per runtime boundary.

## Queue Port

Current port:

- `agent_platform.queue.contracts.DurableQueue`
- `agent_platform.queue.contracts.QueueEvent`
- `agent_platform.queue.sqlite.SQLiteEventQueue`

Required semantics:

- enqueue with `idempotency_key`
- claim due events with a lease
- reclaim expired leases
- mark done
- mark retry or dead-letter according to attempts

SQLite implements this with `event_queue`. RabbitMQ/SQS/etc. should implement
the same semantics explicitly. If the broker does not support delayed retries
or idempotency natively, the adapter must provide that layer.

## Store Ports

The next database abstraction should be module-specific:

- `ActionStore`: insert action, approve/deny, mark executing/queued/executed/failed
- `OutboundQueue`: enqueue outbound, claim due by channel, mark sent/retry
- `CronStore`: insert job, claim due, complete recurring/one-shot jobs, fail
- `SessionStore`: get session, set status
- `SessionMailboxStore`: enqueue prompt, claim next for idle session, mark sent/requeue/fail
- `BatchStore`: append inbound message, load batch state, route batch
- `RunStore`: insert/finalize planner runs

Each port should encode atomic operations, not expose table-shaped CRUD.

Implemented store ports:

- `agent_platform.actions.contracts.ActionStore`
- `agent_platform.actions.sqlite.SQLiteActionStore`
- `agent_platform.outbound.contracts.OutboundQueue`
- `agent_platform.outbound.sqlite.SQLiteOutboundQueue`
- `agent_platform.cron.contracts.CronStore`
- `agent_platform.cron.sqlite.SQLiteCronStore`
- `agent_platform.batching.contracts.BatchStore`
- `agent_platform.batching.sqlite.SQLiteBatchStore`
- `agent_platform.sessions.contracts.SessionStore`
- `agent_platform.sessions.contracts.SessionMailboxStore`
- `agent_platform.sessions.sqlite.SQLiteSessionStore`
- `agent_platform.sessions.sqlite.SQLiteSessionMailboxStore`
- `agent_platform.runs.contracts.RunStore`
- `agent_platform.runs.sqlite.SQLiteRunStore`

## Migration Ports

Platform migrations are currently bundled SQL files applied with namespace
`platform`.

Implemented provider API:

```text
apply_platform_migrations(conn)
apply_app_migrations(conn, DirectoryMigrationProvider(path), namespace="poruchen")
apply_migrations(conn, PackageMigrationProvider(package, resource), namespace="app")
```

For existing SQLite apps, adoption must support a baseline/compatibility path:
if a table already exists and matches the platform contract, mark the platform
migration as applied instead of recreating the table.
