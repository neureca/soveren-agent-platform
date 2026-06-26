# Soveren Agent Platform Architecture

## Purpose

This repository is the reusable runtime core for durable agent applications.
It owns mechanics, not product policy.

Applications such as `poruchen` and `pulsell-agent` should depend on this
platform for queues, workers, batching, sessions, scheduling, actions, and
integration contracts. They should keep prompts, business tools, product copy,
and private schema in their own repos.

## Topology

```text
application repo
  app prompts, policies, tools, product schema
        |
        v
soveren-agent-platform package
  durable runtime, ports, workers, adapters, migrations
        |
        v
storage / broker / external APIs
  bundled SQLite adapter today, replaceable adapters later
```

Current package names:

```text
distribution: soveren-agent-platform
import: soveren_agent_platform
local repo: soveren-agent-platform
```

The local repo name may change independently from the Python package. Do not
rename the Python distribution/import namespace without an explicit migration.

## Adapter Policy

SQLite is the bundled default adapter for the first embedded runtime. It is not
the platform contract.

The platform contract is the set of module-specific ports and their runtime
semantics: idempotency, leases, retries, dead-letter behavior, FIFO mailbox
delivery, atomic batch routing, and typed app-provided handlers. A replacement
adapter must preserve those semantics even when the underlying broker/database
has different primitives.

## Modules

### `soveren_agent_platform.queue`

Durable event queue contract and adapters.

Required semantics:

- enqueue with idempotency key
- claim due events with lease
- recover expired leases
- retry with delayed `run_after`
- dead-letter after max attempts

SQLite implements this with `event_queue`. Other brokers must preserve the
same semantics, even if they need an additional idempotency/retry layer.

### `soveren_agent_platform.batching`

Durable inbound batching.

Ingress events enter as `InboundMessageReceived`, are stored in
`inbound_batches` / `inbound_batch_messages`, then flushed as `ChatBatchReady`.

`BatchStore.route_batch(...)` is the atomic boundary for changing batch state
and enqueueing the next event. Do not split that operation into unrelated
calls.

### `soveren_agent_platform.agent`

Queue-to-agent worker.

The platform worker claims queue events for a recipient and passes a typed
`AgentEvent` to an app-provided `AgentHandler`.

The platform does not decide product behavior here. The app handler does.

### `soveren_agent_platform.context`

Read-only rich context builder for planner/agent turns.

It can include:

- trigger event
- current batch
- session routing hints
- active sessions
- mailbox state
- pending actions
- outbound messages
- cron state

It must not perform side effects.

### `soveren_agent_platform.decisions`

Typed dispatch from app-defined decisions into platform effects.

The platform owns generic routing to:

- outbound queue
- actions
- session mailbox
- cron jobs

Apps own concrete decision schemas and business meaning.

### `soveren_agent_platform.actions` and `soveren_agent_platform.approvals`

Generic side-effect lifecycle:

```text
pending -> approved -> queued/executing -> executed
        -> denied
        -> failed
```

`ActionDispatchEffects` is the atomic boundary for action creation plus
execution intent. Auto-approved actions must not leave a durable action row
without a durable execution event.

### `soveren_agent_platform.outbound`

Channel-neutral outbound queue.

The platform owns durable send/retry state. Apps register concrete senders for
Telegram, email, webhooks, or other channels.

### `soveren_agent_platform.cron`

Durable scheduler core.

Cron workers lease due jobs, call app-provided handlers, complete one-shot jobs,
advance recurring jobs, or retry/dead-letter failures.

### `soveren_agent_platform.sessions`

Execution session runtime.

Main concepts:

- `runtime_sessions`: generalized session handles and status.
- `session_mailbox`: FIFO prompts waiting before a busy/idle session.
- `runtime_session_events`: input/output/control observations.
- `runtime_session_context_snapshots`: searchable routing summaries.
- `runtime_session_route_decisions`: audit trail for routing decisions.

Roles must stay separate:

- mailbox worker delivers prompts to concrete sessions.
- indexer worker refreshes generalized context from backend inspectors.
- lifecycle cleanup closes idle sessions selected by TTL or per-source active
  session limits.
- router reads generalized sessions/snapshots/mailbox state.
- backend inspectors read Codex, Claude, tmux, or other native session state.

Routers must not call Codex/Claude/tmux APIs directly. Use generalized
snapshots first, then bounded inspector enrichment if needed.

Lifecycle cleanup is backend-aware but policy-neutral. It calls the registered
`SessionBackend.close(...)`, records a control event, and marks the session
`closed` or `failed`. Automatic cleanup only closes `idle` sessions with no
`queued` or `sending` mailbox items; `busy` sessions stay owned by the mailbox
worker until they complete, fail, or are handled by an app-level timeout policy.
Explicit forced close cancels queued mailbox items before backend teardown, but
does not interrupt active `sending` work.
Mailbox enqueue accepts prompts only for routable `idle` or `busy` sessions.

### `soveren_agent_platform.llm`

LLM transport contracts and reusable backends.

The platform owns transport mechanics. Apps own model policy, prompts, and
business-specific output schemas.

### `soveren_agent_platform.interfaces` and `soveren_agent_platform.telegram`

Generic channel adapters.

Telegram is one interface, not the core of the platform. The platform may ship
generic Telegram normalization and optional runtime adapters, while apps keep
product-specific copy and command policy.

### `soveren_agent_platform.app_api`

Composition helpers for standard worker sets.

`AgentPlatformApp` wires platform workers into one cooperative runtime, but
apps still choose which modules to enable and which handlers/adapters to
register.

### `soveren_agent_platform.storage`

SQLite setup, WAL/runtime pragmas, and migration runner.

Platform migrations use namespace `platform`. App migrations must use their own
namespace via `apply_app_migrations(...)`.

## Event Flow

Typical inbound flow:

```text
channel adapter
  -> event_queue(recipient="batching", message_type="InboundMessageReceived")
  -> batching worker
  -> inbound_batches / inbound_batch_messages
  -> event_queue(recipient="agent", message_type="ChatBatchReady")
  -> agent worker
  -> app AgentHandler
  -> decisions/actions/outbound/session mailbox/cron
```

Typical session prompt flow:

```text
decision dispatcher
  -> session_mailbox(queued)
  -> session mailbox worker
  -> runtime_sessions.status = busy
  -> SessionBackend.send(...)
  -> SessionBackend.capture(...)
  -> runtime_session_events(input/output)
  -> runtime_session_context_snapshots(refresh)
  -> runtime_sessions.status = idle/busy/failed
```

Typical session indexing flow:

```text
session indexer worker
  -> SessionStore.list_active(...)
  -> SessionInspector.inspect(...)
  -> runtime_session_events
  -> runtime_session_context_snapshots(refresh)
```

## Storage Boundaries

SQLite is the first bundled adapter, not the platform contract.

Use module-specific ports:

- `DurableQueue`
- `BatchStore`
- `ActionStore`
- `ActionDispatchEffects`
- `OutboundQueue`
- `CronStore`
- `SessionStore`
- `SessionMailboxStore`
- `SessionSnapshotStore`
- `SessionInspector`
- `RunStore`

Do not add generic table repositories. Do not make application code depend on
SQLite table names as public API; table layouts belong to the bundled SQLite
adapter.

## Concurrency Rules

- Workers must be restartable.
- Queue claims use leases.
- Stale leases must be recoverable.
- Idempotency keys are required for externally retried events.
- Work is done only after durable side effects are committed.
- SQLite workers should own their own connection.
- In-memory queues or caches may be wakeup hints only; they are not source of truth.

## Extension Rules

When adding a new reusable capability:

1. Put the contract in the owning module.
2. Add a SQLite adapter only if storage is needed.
3. Add a migration under `storage/migrations/platform` only for platform-owned tables.
4. Add worker/composition wiring if it is runtime behavior.
5. Add fake-tested port tests and SQLite transaction tests for risky boundaries.
6. Update `docs/PORTS.md` and this file if a new architectural boundary appears.

When adding app-specific behavior:

1. Keep it in the app repo.
2. Register it through platform ports.
3. Do not add product schema or product copy to platform migrations/adapters.

## Validation

Run:

```bash
uv sync --group dev
uv run ruff check src tests
uv run mypy
uv run pytest
```

All public platform boundaries should remain typed and covered by focused tests.
