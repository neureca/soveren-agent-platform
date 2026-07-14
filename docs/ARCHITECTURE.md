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

## Organization And Conversation Boundaries

`tenant_id` identifies the organization that owns configuration, billing, and
application authorization. It is not the privacy boundary for chat state.
`source_id` identifies one conversation inside that organization:

- each direct chat has its own `source_id` and is private from every other chat;
- one group chat has one `source_id`, so its participants share that
  conversation's sessions, memory, actions, schedules, and files;
- organization-wide data is app-owned and may be exposed only through an
  explicitly authorized tool. It is never included in planner context merely
  because records have the same `tenant_id`.

Conversation-private storage and transitions are keyed by the pair
`(tenant_id, source_id)`. `owner_id` is routing metadata, not an ACL. A guessed
row id, event id, destination id, correlation id, or idempotency key must not
cross the conversation boundary.

## Adapter Policy

SQLite is the bundled default adapter for the first embedded runtime. It is not
the platform contract.

The platform contract is the set of module-specific ports and their runtime
semantics: idempotency, leases, retries, dead-letter behavior, FIFO mailbox
delivery, atomic batch routing, and typed app-provided handlers. A replacement
adapter must preserve those semantics even when the underlying broker/database
has different primitives.

Consumer-facing storage contracts are asynchronous. Applications call typed
ports and SQLite adapters with `await`; they do not receive raw SQLite
connections or call synchronous store functions. The bundled adapter may use
synchronous `sqlite3` transaction callbacks internally, but it runs each whole
operation under the connection lock in a worker thread.

## Modules

### `soveren_agent_platform.queue`

Durable event queue contract and adapters.

Required semantics:

- enqueue with idempotency key
- claim due events with lease
- recover expired leases
- complete or retry only with the current opaque lease token
- renew every claimed lease while work is processing, including items waiting
  behind earlier items in the same claimed batch
- retry with delayed `run_after`
- dead-letter after max attempts

SQLite implements this with `event_queue`. Other brokers must preserve the
same semantics, even if they need an additional idempotency/retry layer.
Idempotency keys are scoped by `tenant_id`; one tenant cannot suppress another
tenant's event by reusing a key. A reclaimed lease receives a new token, so a
late worker cannot overwrite the new owner's state.
For every idempotent command, a replay is valid only when its immutable input
matches the original command. Reusing a key with different input raises
`IdempotencyConflictError`; this detects caller conflicts but does not claim
exactly-once execution of an external effect.

### `soveren_agent_platform.batching`

Durable inbound batching.

Ingress events enter as `InboundMessageReceived`, are stored in
`inbound_batches` / `inbound_batch_messages`, then flushed as `ChatBatchReady`.
Raw event idempotency is tenant-scoped. Multi-participant batch text uses
per-batch pseudonyms; channel identities stay in structured fields for trusted
routing and are redacted before the default model boundary.

`BatchStore.route_batch(...)` is the atomic boundary for changing batch state
and enqueueing the next event. Do not split that operation into unrelated
calls.
All batch reads and transitions require the owning `tenant_id` and `source_id`;
a batch id is never sufficient authorization by itself.

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
Every included batch, session, mailbox item, action, outbound message, and cron
job must match both the trigger's `tenant_id` and explicit `source_id`.

### `soveren_agent_platform.memory`

App-neutral durable memory records.

The platform owns the storage port, bundled SQLite adapter, and optional dynamic
tools. Apps own memory policy: what can be remembered, which subject a memory
belongs to, whether model-initiated writes are allowed, retention rules, and
whether memory is injected into prompts.

Memory is explicit. Platform storage can contain memory records by default, but
planner context and Codex threads do not see memory unless the app reads the
`MemoryStore` or registers `platform.memory` tools.
Every platform memory record belongs to one conversation. Organization-wide
knowledge belongs behind a separately authorized application tool rather than
an unscoped platform-memory query.

### `soveren_agent_platform.decisions`

Typed dispatch from app-defined decisions into platform effects.

The platform owns generic routing to:

- outbound queue
- actions
- session mailbox
- cron jobs

Apps own concrete decision schemas and business meaning.

Planner runs are claimed by tenant, trigger event, model, and prompt version.
The raw LLM response is persisted before decision dispatch. A dispatch retry
re-parses that durable response instead of calling the model again; concurrent
or stale planners are fenced by a run lease token.

### `soveren_agent_platform.actions` and `soveren_agent_platform.approvals`

Generic side-effect lifecycle:

```text
pending -> approved -> queued/executing -> executed
        -> denied
        -> failed
        -> uncertain
```

`ActionDispatchEffects` is the atomic boundary for action creation plus
execution intent. Auto-approved actions must not leave a durable action row
without a durable execution event.
Manual approvals use the same atomic boundary through
`approve_action_and_enqueue(...)`. Every action lookup and transition requires
the tenant and source ids. If a leased execution disappears after an external call may
have started, recovery records `uncertain` and does not replay the executor.
Unexpected executor exceptions are also uncertain. Automatic retry is allowed
only for `ActionNotStartedError` or an explicit
`ActionExecutionResult.retryable_failure(...)`.
For a safe retry, the action returns from `executing` to `queued` before the
execution event moves to `retrying` or `dead_letter`. If the worker stops between
those transitions, lease recovery sees a retryable action instead of incorrectly
classifying an execution that never started as uncertain.
An executor result of `queued` means a downstream durable handoff has already
succeeded. Lease recovery closes the original execution event without invoking
the executor again; a later downstream callback owns the final transition.

### `soveren_agent_platform.outbound`

Channel-neutral outbound queue.

The platform owns durable send/retry state. Apps register concrete senders for
Telegram, email, webhooks, or other channels.
Before calling a sender, the message moves from `leased` to `sending`. A crash,
timeout, or unexpected sender exception after that transition becomes
`uncertain`, not retryable. Only `SendNotStartedError` proves that retry is safe.
Provider result metadata is stored separately from the original outbound
payload.

### `soveren_agent_platform.cron`

Durable scheduler core.

Cron schedules are validated before insertion and again before a legacy row can
be leased. Workers move leased jobs to `running` before calling app-provided
handlers. A crash, timeout, or unexpected handler exception becomes
`uncertain`; only `CronNotStartedError` permits automatic retry. Successful jobs
complete one-shot work or advance recurring schedules. `run_at` remains the
business schedule anchor; retry backoff is stored separately in `retry_at`, so
a delayed retry cannot shift a recurring schedule.
Action, outbound, and cron decision idempotency is scoped by
`(tenant_id, source_id)`, so equal keys in two private chats do not suppress or
return each other's effects.

### `soveren_agent_platform.reconciliation`

Explicit, audited resolution for uncertain actions, outbound messages, and cron
jobs. Every request is conversation-scoped and requires `tenant_id`,
`source_id`, a stable `request_key`, an
operator `actor_id`, and provider evidence. Resolutions such as `not_executed`,
`not_sent`, and `not_fired` are the only paths that requeue an uncertain effect.
Repeating the same request is idempotent; reusing its key with different input
is rejected.

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
Cancellation during backend close is persisted as `failed` before cancellation
is propagated. `SQLiteSessionLifecycle.recover_stale_closing_sessions(...)` converts abandoned
`closing` rows into an explicit uncertain failure for app maintenance jobs.
Mailbox enqueue accepts prompts only for routable `idle` or `busy` sessions.
Mailbox `sending` rows distinguish unaccepted delivery from accepted backend
work through durable `accepted_at` and backend receipt fields. Accepted work may
retry capture, but an unaccepted stale or failed send is never blindly resent.
Receipt-aware backends recover the exact accepted operation. The Codex adapter
persists the `turn/start` turn ID and uses that ID after app-server restarts, so
recovery cannot complete a mailbox item with output from an older turn.
Pending capture polls do not consume the transport-error retry budget; accepted
work has a separate absolute deadline. Live notifications and persisted turn
reads use the same terminal-status rules, including `interrupted` as failure.

Sandboxed execution is optional and explicit. The default session backends keep
their existing local behavior. Apps that need untrusted-user isolation can wrap Codex
app-server with `SandboxedCodexAppServerBackend`, backed by a `SandboxManager`.
The MVP manager implementation is a Docker sibling-container driver for single-host
`docker compose` deployments. Docker is a host prerequisite when sandbox mode is
enabled. The high-level factory creates or validates one internal network per
conversation, the shared public proxy network and proxy, and host packet-filter rules.
It then creates or reuses one container per `(tenant_id, source_id)` boundary and applies
hard CPU/memory/PID/disk limits, and starts Codex app-server inside that container
through `docker exec -i`. The supported composition point is
`create_sandboxed_codex_backend(...)`; product integrations select an
organization, conversation, and coarse resource profile rather than
constructing Docker options.

The platform must not give Telegram users, app handlers, or Codex threads
direct access to the Docker socket or arbitrary Docker commands. Docker access
is platform infrastructure, not a model tool or product extension point.
Alternative sandbox drivers are outside the MVP scope.

Conversation containers run as a non-root user with all Linux capabilities dropped
and `no-new-privileges` enabled. They join only their conversation-specific internal
network. Host `DOCKER-USER` and `INPUT` rules allow traffic only to the shared
Squid proxy on port 3128 and the organization's credential broker on port 8080,
then drop direct peer and bridge-gateway access. A
packaged proxy provides public HTTP/HTTPS egress while blocking private,
loopback, link-local, and metadata destinations.
Conversation networks must be IPv4-only in the MVP; acquisition fails when IPv6 is
enabled because the host packet-filter policy is not yet dual-stack.
An existing conversation network is reused only when its managed, organization,
and conversation hash labels match the requested boundary. Broker responses pass
the conversation firewall only as established or related connections originating
from the broker address and port.
`CodexApiKeyCredentials` never provisions the provider key into a conversation
container. The trusted Docker manager creates one credential broker per active
organization, streams the key into broker tmpfs over stdin, and the broker removes
that file after loading the key into process memory. Codex is launched with a
non-secret custom model-provider URL and no OpenAI auth cache. The broker accepts
only the Responses and Responses compaction POST routes, overwrites client auth
headers, and uses a fixed OpenAI API upstream. Tenant-wide broker policy bounds
concurrency, request rate, request size, queue wait, and optionally model names.
The broker starts in the first explicitly broker-enabled private conversation
network, is attached only to other broker-enabled conversations for that tenant,
and reaches OpenAI only through the managed
Squid proxy; tenant brokers never share the public bridge network.
The broker is removed when the organization's last active sandbox stops.
Trusted personal auth-file providers still place their cache in the conversation
`CODEX_HOME` and are readable from that sandbox.
Hard writable-layer quotas remain fail-closed: `overlay2` deployments require an
XFS backing filesystem mounted with `pquota` rather than silently dropping the
disk boundary on an unsupported host.
Managed conversation containers carry tenant and conversation hashes plus a
hash of their resolved spec and Docker hardening policy version. A policy
change therefore fails reuse until the
old sandbox is explicitly destroyed and recreated.
Tenant network bootstrap is compensating: if container acquisition fails, the
manager removes the proxy attachment, network, and exact host firewall policy.
The resolved subnet, proxy address, and credential-broker address are retained in
the sandbox handle. Rotation removes the old broker's firewall rules before its
address can be reused, and conversation cleanup removes both retained and current
rules.

`create_sandbox_manager(...)` creates the single process-owned `DockerSandboxManager`
shared by every conversation backend. Backend composition requires the manager as
an explicit dependency, so no backend can create an independent capacity owner. It defaults to
one active conversation sandbox. Capacity is released when a sandbox stops or is
destroyed. On the first acquire after a
control-plane restart, the manager stops running managed conversation containers left by
the previous process before reusing only the requested conversation boundary. The
sandboxed Codex backend single-flights initialization, can host multiple threads
inside one app-server, and stops after its last thread remains closed for the
configured idle interval. `AgentPlatformApp` discovers shutdown-capable session
backends from each live `SessionBackendRegistry` after workers stop, including
backends registered after application composition.

### `soveren_agent_platform.sandbox`

Optional execution sandbox lifecycle.

Main concepts:

- `SandboxResourceProfile`: coarse memory, CPU, PID, disk, and temporary-storage
  limits exposed to product integration.
- `SandboxSpec`: infrastructure-level organization/conversation boundary,
  image, limits, network,
  workspace, and startup command.
- `SandboxHandle`: resolved sandbox identity and container paths.
- `CredentialBrokerPolicy`: tenant-wide inference limits and optional model allowlist.
- `CredentialBrokerProvisioner`: trusted manager capability that provisions a broker
  without exposing provider credentials to a conversation sandbox.
- `SandboxManager`: acquire, stop, destroy, ensure directory, run bounded setup
  commands, and build the long-lived app-server exec command.
- `DockerSandboxManager`: bundled Docker CLI implementation that owns shared
  egress bootstrap, tenant credential brokers, and conversation-container lifecycle.

Sandbox tenant and conversation ids are runtime routing inputs, not public
labels. The Docker manager labels containers with hashes of both values so raw
chat/user ids do not leak into Docker metadata by default.

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
- `EffectReconciler`
- `MemoryStore`

Do not add generic table repositories. Do not make application code depend on
SQLite table names as public API; table layouts belong to the bundled SQLite
adapter.

## Concurrency Rules

- Workers must be restartable.
- Queue claims use leases.
- Stale leases must be recoverable.
- Active leases must be renewed for processing and already-claimed waiting work.
- Lease completion/retry must present the current opaque fencing token.
- Idempotency keys are required for externally retried events and are scoped to
  the tenant boundary.
- Work is done only after durable side effects are committed.
- SQLite workers should own their own connection.
- Concurrent migrators must recheck each version after acquiring the SQLite
  write lock.
- Async adapters serialize complete operations on a shared SQLite connection;
  a transaction must never be split across independent `to_thread` calls or be
  exposed as a consumer-facing synchronous operation.
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
