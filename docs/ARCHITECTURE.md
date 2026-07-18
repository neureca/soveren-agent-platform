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

`ConversationScope` is the trusted execution representation of that pair. The
planner derives it from the raw `AgentEvent` before model redaction and carries
it separately through `LlmRequest` and `OpenSpec`. It is never prompt text,
model metadata, or a dynamic-tool argument. A conversation-bound backend must
reject a missing or mismatched scope before opening a thread, process, or
sandbox. A `CodexAppServerBackend` with a bound `DynamicToolRegistry` exposes
that registry boundary to the same check instead of relying on integrator
wiring alone.

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
- terminalize an expired lease after the maximum attempt by default
- allow an effect-aware worker to opt into exhausted-lease recovery only when
  it guarantees that recovery will reconcile state without starting the effect
  again
- optionally fence claim selection and every expired/exhausted cleanup mutation
  by `tenant_id`; omitting the scope preserves the generic global worker mode

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
`channel`, `source_id`, and `raw_event_id` must be non-empty strings and
`message_at` must be an integer; malformed input fails before any batch row is
written. Raw event idempotency is scoped by tenant and source. Multi-participant batch text uses
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
Text queries use the SQLite FTS index across all eligible records before the
result limit is applied; an older relevant record is not hidden by a window of
newer unrelated records.

### `soveren_agent_platform.decisions`

Typed dispatch from app-defined decisions into platform effects.

The platform owns generic routing to:

- outbound queue
- actions
- session mailbox
- cron jobs

Apps own concrete decision schemas and business meaning.

Planner runs are claimed by tenant, source, trigger event, model, and prompt
version. A source id is required before the run is claimed, so cached output
cannot cross a private conversation boundary.
The raw LLM response is persisted before decision dispatch. A dispatch retry
re-parses that durable response instead of calling the model again; concurrent
or stale planners are fenced by a run lease token.
Failed planner runs preserve grouped failure details recursively in durable
output. For a session-backed LLM call, the request failure precedes any backend
cleanup failure, so both remain observable without replacing the root cause.

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
For a non-final safe retry, the action returns from `executing` to `queued`
before the execution event moves to `retrying`. If the worker stops between
those transitions, lease recovery sees a retryable action instead of incorrectly
classifying an execution that never started as uncertain. On the final attempt,
the action moves to `failed` before the execution event moves to `dead_letter`;
if the final lease itself expires, the action worker reclaims it in recovery-only
mode. Recovery never calls the executor: an action that did not start becomes
`failed`, while an `executing` action becomes `uncertain`. A missing registered
executor is deterministic configuration failure and moves the action to `failed`
without calling an external system.
An executor result of `queued` means a downstream durable handoff has already
succeeded. Lease recovery closes the original execution event without invoking
the executor again; a later downstream callback owns the final transition.

### `soveren_agent_platform.outbound`

Channel-neutral outbound queue.

The platform owns durable send/retry state. Apps register concrete senders for
Telegram, email, webhooks, or other channels.
Before calling a sender, the message moves from `leased` to `sending`. A crash,
timeout, or unexpected sender exception after that transition becomes
`uncertain`, not retryable. `SendResult` represents expected `sent`,
`retryable_failure`, and `permanent_failure` outcomes. A retryable result or
`SendNotStartedError` must prove that the provider did not accept the send; a
permanent result moves directly to `dead_letter`. Provider result metadata is
stored separately from the original outbound payload. Channel claims may be
tenant-fenced; all expired `sending` and exhausted-lease cleanup uses the same
channel and optional tenant scope as the claim.

Plain or app-rendered Telegram text longer than 4096 characters is partitioned
before the sender is called. `enqueue_telegram_text(...)` creates one durable
row and stable part idempotency key per chunk. It rejects automatic splitting
of long `parse_mode` markup because a raw boundary can corrupt entities.
Multipart rows also carry a shared ordering key and one-based position. The
store only leases a part after its immediate predecessor is `sent`; an
`uncertain` predecessor blocks successors for explicit reconciliation, while a
`dead_letter` or `cancelled` predecessor cancels all queued or retrying
successors. This keeps retry concurrency from reordering Telegram parts without
claiming exactly-once delivery.
`TelegramSender` performs one Telegram API call per row and rejects a plain
over-limit row as a deterministic permanent failure.

### `soveren_agent_platform.cron`

Durable scheduler core.

Cron schedules are validated before insertion and again before a legacy row can
be leased. Workers move leased jobs to `running` before calling app-provided
handlers. A crash, timeout, or unexpected handler exception becomes
`uncertain`; only `CronNotStartedError` permits automatic retry. Successful jobs
complete one-shot work or advance recurring schedules. `run_at` remains the
next business execution time, while immutable `schedule_anchor_at` remains the
RRULE `DTSTART`. Retry backoff is stored separately in `retry_at`, so neither a
delayed retry nor recurrence advancement can reset finite RRULE state.
Cron claims and expired-lease cleanup may be tenant-fenced. Omitting the scope
is an explicit global scheduler mode and requires a tenant-aware handler.
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
- backend inspectors read Codex, Claude, or other native session state.

Routers must not call backend-native APIs directly. Use generalized
snapshots first, then bounded inspector enrichment if needed.
Each inspected item must identify the same runtime session selected by the
worker. An ordinary inspection or persistence failure is isolated to that
session so later sessions in the batch still advance; worker cancellation and
failure to list the batch remain request-level failures.

The tmux module is deliberately below the `SessionBackend` boundary. Its
`TmuxCommandSession.capture_until(...)` requires an explicit completion marker;
it is not publicly exported as a generic backend because terminal silence does
not prove that a command completed.

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
work has a separate absolute deadline. At that deadline, a backend implementing
`DeliveryAbortBackend` receives the exact persisted receipt. Codex interrupts
that turn, attempts to archive the thread, and releases local thread ownership;
the mailbox then records failure even when cleanup itself fails. This is
best-effort cleanup, not an atomic rollback or exactly-once boundary. Live
notifications and persisted turn reads use the same terminal-status rules,
including `interrupted` as failure.

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

Optional Codex collaboration presets are represented by the typed
`CodexCollaborationMode` contract and serialized to the app-server's required
`{mode, settings}` object. Raw mode strings are not part of the platform API.
Sandbox idle-stop is eligible only when the backend has no active Codex thread
and no in-flight `open`, `send`, `capture`, `abort`, or `close` operation.
Starting an operation therefore reserves the backend before any awaited
app-server I/O. Deadline abort discards the sandbox adapter's active-thread
ownership even when remote interrupt/archive reports an error, allowing backend
shutdown to terminate the remaining conversation process.

The platform must not give Telegram users, app handlers, or Codex threads
direct access to the Docker socket or arbitrary Docker commands. Docker access
is platform infrastructure, not a model tool or product extension point.
Alternative sandbox drivers are outside the MVP scope.

Conversation containers run as a non-root user with all Linux capabilities dropped
and `no-new-privileges` enabled. They join only their conversation-specific internal
network. Host `DOCKER-USER` and `INPUT` rules allow traffic only to the shared
Squid proxy on port 3128 and the shared credential broker's network-specific
address on port 8080,
then drop direct peer and bridge-gateway access. A
packaged proxy provides public HTTP/HTTPS egress while blocking private,
loopback, link-local, and metadata destinations.
Conversation networks must be IPv4-only in the MVP; acquisition fails when IPv6 is
enabled because the host packet-filter policy is not yet dual-stack.
An existing conversation network is reused only when its managed, organization,
and conversation hash labels match the requested boundary. Broker responses pass
the conversation firewall only as established or related connections originating
from the broker address and port. Squid responses are restricted the same way:
only established or related connections originating from proxy port 3128 can
return to a conversation network; the shared proxy cannot initiate new tenant
connections.
`CodexApiKeyCredentials` never provisions the provider key into a conversation
container. The trusted Docker manager creates one credential broker shared by active
organizations on that Docker host. It serializes one tenant-scoped registry update to
Docker exec stdin; an admin process forwards it to a broker-only Unix socket, and the
broker validates and atomically replaces or removes only that tenant's memory registry.
Raw credentials remain only in the trusted manager and shared broker process memory;
no credential file is created.
Codex is launched with a non-secret custom model-provider URL, a broker-only
`NO_PROXY` exception, and no OpenAI auth cache. The built-in OpenAI binding accepts
only the Responses and Responses compaction POST routes, overwrites client auth
headers, and uses a fixed OpenAI API upstream. Tenant-wide broker policy bounds
concurrency, request rate, request size, request-body read time, queue wait, and
optionally model names. Stable per-binding admission state is separate from replaceable
secret/configuration state, so a live registry update does not reset active concurrency
or rolling rate counters. Per-tenant and broker-wide in-flight and buffered-body budgets
bound aggregate use across bindings; the global body budget is capped at half of the
broker cgroup memory.

Generic protected HTTP bindings use an opaque capability path plus the destination
address of an authorized conversation-network interface. The broker derives tenant
identity from that local destination address before resolving the OpenAI binding or
opaque capability. Tenant identity is never accepted from an HTTP header, path, query,
or body, and registry validation rejects an interface address assigned to two tenants.
Each binding fixes one
public HTTPS port-443 origin, credential header, method set, normalized path prefixes,
request-header allowlist, and resource policy. Conversation scope is the default;
tenant scope explicitly authorizes every managed conversation network in the
organization. Redirects are returned rather than followed, and clients cannot choose
an upstream host. OAuth refresh, cookies, arbitrary proxying, and query/body secret
injection are outside this boundary.

The broker starts in the first binding-enabled private conversation network, attaches
to every network required by active tenant registries, and reaches every upstream only
through the managed Squid proxy; it never joins the public bridge network. When an
organization's last active sandbox stops, its registry and network attachments are
removed from the broker. The process-owned manager retains that memory-only registry
while stopped sandboxes remain resumable and restores it before starting one. The shared
container is removed when no active tenant registry remains. The manager also extends
tenant-scoped grants before a newly created conversation sandbox starts. A new
control-plane process has no retained registries and removes the previous shared broker
before returning the first sandbox handle; applications must then provision current
credentials from their own secret stores. Registry update uncertainty decommissions the
shared broker so a partially known authorization state is never left serving traffic;
the next broker prepare or provision restores every still-active in-memory tenant registry.
Trusted personal auth-file providers still place their cache in the conversation
`CODEX_HOME` and are readable from that sandbox.
Hard writable-layer quotas remain fail-closed: `overlay2` deployments require an
XFS backing filesystem mounted with `pquota` rather than silently dropping the
disk boundary on an unsupported host.
Managed conversation containers carry tenant and conversation hashes plus a
hash of their resolved spec and Docker hardening policy version. A policy
change therefore fails reuse until the old sandbox is explicitly destroyed and
recreated. An image-only change is the deliberate exception: new conversations
use the configured image, while an existing stateful container retains its
actual image and writable workspace until explicit destruction. The returned
handle reports both the actual and configured image when that update is
deferred. Any simultaneous resource, command, environment, network, or
hardening-policy change still fails closed.
The shared egress proxy is stateless. When its image changes under the same
firewall-policy version, the manager replaces it only after verifying that no
managed conversation container is running. It removes the old proxy-specific
allow and response rules before replacement while retaining each conversation
network's fail-closed drop rules. An egress firewall-policy version change
requires its matching explicit rule migration and fails closed otherwise.
First-acquire recovery after a control-plane restart stops orphaned conversation
containers before the image check, so a normal package update does not require
manual egress removal.
Tenant network bootstrap is compensating: if container acquisition fails, the
manager first revokes and disconnects any prepared credential-broker attachment, then
removes the proxy attachment, network, and exact host firewall policy. That compensation
restores the manager's memory registry to its exact pre-prepare state, so a failed resume
does not discard credentials needed by a later retry. Broker prepare, sandbox start,
sandbox stop, and tenant deactivation remain serialized per tenant. Shared broker
container, network, and registry mutations additionally use one host-level lock so
operations from different tenants cannot interleave lifecycle state.
The resolved subnet, proxy address, and credential-broker host are retained in the
sandbox handle. Binding rotation atomically replaces the registry while preserving
its capability and admission counters. Request-body completion is followed by registry
revalidation under the update lock; a request admitted there may finish, while an earlier
request loses to a completed revoke or rotation. Broker replacement
removes old firewall rules before its address can be reused. Conversation cleanup
removes both retained and current rules.

`create_sandbox_manager(...)` creates the single process-owned `DockerSandboxManager`
shared by every conversation backend. Backend composition requires the manager as
an explicit dependency, and the high-level backend factory requires registration
under its deterministic conversation-derived name. No backend can silently create
an independent capacity owner or naming path. The manager defaults to
one active conversation sandbox. Capacity is released when a sandbox stops or is
destroyed. On the first acquire after a
control-plane restart, the manager stops running managed conversation containers left by
the previous process before reusing only the requested conversation boundary. The
sandboxed Codex backend single-flights initialization, can host multiple threads
inside one app-server, and stops after its last thread remains closed for the
configured idle interval. Backend activation cancels and awaits an in-progress
idle shutdown before acquiring a new sandbox, and a stopped backend is never
returned from the cache. `AgentPlatformApp` discovers shutdown-capable session
backends from each live `SessionBackendRegistry` after workers stop, including
backends registered after application composition.
The Codex app-server stdout reader dispatches server-initiated dynamic tool calls
to tracked tasks so a slow app-owned tool cannot block unrelated responses on the
same conversation transport. Those tasks are cancelled and awaited at client
shutdown; the adapter does not invent automatic timeout or retry semantics for
side-effecting tools.

Terminal Codex turn text and errors are removed from live notification maps
after capture copies the outcome. Non-terminal timed-out turns stay registered
for later capture, while archive, deadline abort, and client shutdown release
all live state owned by the thread. A repeated exact capture can read persisted
`thread/read` history after live state is released.

### `soveren_agent_platform.sandbox`

Optional execution sandbox lifecycle.

Main concepts:

- `SandboxResourceProfile`: coarse memory, CPU, PID, disk, and temporary-storage
  limits exposed to product integration.
- `SandboxSpec`: infrastructure-level organization/conversation boundary,
  image, limits, network,
  workspace, and startup command.
- `SandboxHandle`: resolved sandbox identity and container paths.
- `CredentialBrokerPolicy`: tenant-wide inference limits, bounded request-body
  reads, and an optional model allowlist.
- `CredentialBrokerProvisioner`: trusted manager capability that provisions a broker
  without exposing provider credentials to a conversation sandbox.
- `HttpCredentialBinding`: fixed-origin, policy-bound static header credential
  definition; conversation scope is the default and tenant scope is explicit.
- `CredentialBrokerCapability`: opaque conversation-network URL returned without
  credential bytes. It is authorization material and must not be logged or shared.
- `HttpCredentialBrokerProvisioner`: trusted provision/rotate/revoke capability for
  protected HTTP bindings.
- `SandboxManager`: acquire, stop, destroy, ensure directory, run bounded setup
  commands, and build the long-lived app-server exec command.
- `DockerSandboxManager`: bundled Docker CLI implementation that owns shared
  egress and credential-broker bootstrap plus conversation-container lifecycle.

Every Docker CLI subprocess has a wall-clock timeout. Timeout and task
cancellation terminate the child, escalate to kill after a short grace period,
and await process reaping before control returns.

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
Startup is atomic with respect to owned runtime resources. If storage bootstrap,
worker construction, or task scheduling fails, the app stops partial worker
state, closes every managed resource, becomes terminal, and reports rollback
failures alongside the original error.
Supervisor cancellation propagates into in-flight leased item processing and
joins that child task before worker shutdown returns, so managed backends are
not closed while a worker-side effect is still executing.
Polling workers tolerate isolated storage-claim failures, reset that failure
budget after a successful claim, and raise after a bounded consecutive failure
limit. This keeps transient SQLite failures recoverable while allowing the
supervisor and container runtime to observe a permanently broken worker.
The high-level Telegram runtime binds its fixed `tenant_id` to batching, agent,
actions, and outbound claims because those handlers and registries are
tenant-specific. Lower-level worker composition may omit `tenant_id` only when
the consuming app intentionally runs a global worker with tenant-aware handlers.

### `soveren_agent_platform.storage`

SQLite setup, WAL/runtime pragmas, and migration runner.

Platform migrations use namespace `platform`. App migrations must use their own
namespace via `apply_app_migrations(...)`.
Storage bootstrap validates platform table, index, and trigger definitions, so
the memory-search synchronization triggers are part of the runtime health gate.

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
  -> SessionStore.list_active(..., after_session_id=process-local cursor)
  -> SessionInspector.inspect(...)
  -> SessionIndexStore.index_inspection(...)
  -> append event + refresh snapshot, atomically; or
  -> repair a missing/stale snapshot for an existing marker without another event
```

The indexer advances through stable session-ID pages and wraps after the last
page. Its cursor is process-local because indexing is repeatable enrichment,
not a durable delivery workflow; a restart simply begins a fresh scan.

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
- `SessionIndexStore`
- `SessionSnapshotStore`
- `SessionInspector`
- `RunStore`
- `EffectReconciler`
- `MemoryStore`

Do not add generic table repositories. Do not make application code depend on
SQLite table names as public API; table layouts belong to the bundled SQLite
adapter.

## Concurrency Rules

- Durable worker state must recover when a new app instance starts after a
  process restart. A stopped `AgentPlatformApp` instance is terminal because
  its managed runtime resources have been closed.
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
