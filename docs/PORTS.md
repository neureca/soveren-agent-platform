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
- expose the opaque `lease_token` on claimed work
- renew an unexpired lease with the current token
- mark done only with the current token
- mark retry or dead-letter only with the current token and attempt state

Idempotency keys are scoped to `tenant_id`. A replacement adapter must prevent
a stale owner from completing or reopening work after another owner reclaimed
the lease. A key replay with the same immutable input returns the adapter's
normal not-created result; the same key with different input raises
`IdempotencyConflictError`.

SQLite implements this with `event_queue`. RabbitMQ/SQS/NATS/Postgres/etc.
should implement the same semantics explicitly. If the broker does not support
delayed retries or idempotency natively, the adapter must provide that layer.

All storage port methods that perform I/O are asynchronous. Bundled SQLite
adapters expose `await Adapter.open(...)`, async operations, and `await
adapter.close()`. Raw connections and synchronous transaction functions belong
to the adapter implementation and are not consumer integration APIs.

## Store Ports

The next database abstraction should be module-specific:

- `ActionStore`: conversation-scoped insert/get/approve/deny and guarded
  executing/queued/executed/failed/uncertain transitions
- `ActionDispatchEffects`: create an action and atomically route auto-approved actions to execution
- `SQLiteApprovalService`: atomically approve a manual action and enqueue its
  execution event
- `OutboundQueue`: conversation-scoped enqueue, claim/renew by channel, explicit
  leased/sending/sent/uncertain transitions, and safe pre-send retry
- `CronStore`: validated idempotent insert, claim/renew, explicit
  leased/running/uncertain transitions, separate schedule and retry timestamps,
  and fenced completion for recurring and one-shot jobs
- `SessionStore`: get session, set status
- `SessionMailboxStore`: enqueue prompt, claim next for idle session, mark sent/requeue/fail
- `SQLiteSessionLifecycle`: backend-aware session teardown, idle cleanup, and stale-close recovery
- `SessionInspector`: backend-specific live context reader for Codex, Claude, or other execution backends
- `SessionSnapshotStore`: refresh/latest searchable session context snapshots
- `BatchStore`: append inbound message, load batch state, atomically route batch into the next durable queue
- `RunStore`: claim a tenant/event/model/prompt operation, return cached planner
  output, and finalize only with the current run token
- `EffectReconciler`: conversation-scoped, audited, idempotent resolution of uncertain
  actions, outbound messages, and cron jobs
- `MemoryStore`: remember/search/get/forget explicit app-neutral memory records
- `SandboxRuntime`: acquire/stop/destroy an execution sandbox, ensure container
  directories, run bounded setup commands, and build the app-server exec command

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
- `soveren_agent_platform.sessions.lifecycle.SQLiteSessionLifecycle`
- `soveren_agent_platform.sessions.sqlite.SQLiteSessionStore`
- `soveren_agent_platform.sessions.sqlite.SQLiteSessionMailboxStore`
- `soveren_agent_platform.sessions.sqlite.SQLiteSessionSnapshotStore`
- `soveren_agent_platform.runs.contracts.RunStore`
- `soveren_agent_platform.runs.sqlite.SQLiteRunStore`
- `soveren_agent_platform.reconciliation.contracts.EffectReconciler`
- `soveren_agent_platform.reconciliation.sqlite.SQLiteEffectReconciler`
- `soveren_agent_platform.memory.contracts.MemoryStore`
- `soveren_agent_platform.memory.sqlite.SQLiteMemoryStore`
- `soveren_agent_platform.sandbox.contracts.SandboxRuntime`
- `soveren_agent_platform.sandbox.contracts.CredentialBrokerRuntime`
- `soveren_agent_platform.sandbox.docker.DockerSandboxRuntime`

## Sandbox Port

Sandboxing is an optional execution-plane port. It exists so Codex, Claude, or
other tool-capable session backends can run behind a conversation boundary without
making the whole platform depend on one sandbox product.

The port is deliberately narrow:

- `acquire(SandboxSpec) -> SandboxHandle`
- `stop(SandboxHandle)`
- `destroy(SandboxHandle)`
- `ensure_directory(SandboxHandle, path)`
- `run_command(SandboxHandle, command, input_data, env, workdir)`
- `exec_command(SandboxHandle, command, env, workdir, interactive)`

API-key isolation is a separate trusted capability:

- `provision_credential_broker(SandboxHandle, api_key, CredentialBrokerPolicy)`
  returns a non-secret conversation-network endpoint.

The bundled Docker implementation uses host Docker as a trusted infrastructure
dependency and creates sibling containers with memory, CPU, PID, disk, temporary
storage, user, and network limits. It labels managed containers with tenant and
conversation hashes, not raw ids. It rejects host/container namespace sharing and any
network outside its infrastructure allowlist. Capacity belongs to one runtime
instance; `create_sandbox_pool(...)` is the process-local composition root shared
by all conversation backends and defaults to one active conversation sandbox.

The Docker socket is not a tenant capability. The platform deployment owns
Docker access and must never expose it through model tools, conversation sandboxes, or
ordinary app handlers. Product integrations configure organization/conversation boundaries and
resource profiles; they do not pass arbitrary Docker options. API keys are
streamed only to a tenant credential broker and never enter the conversation
sandbox. Trusted personal auth-file providers still use `run_command` stdin;
their cache is intentionally sandbox-local. Neither path places secret bytes in
Docker arguments, environment metadata, or labels.

The high-level Docker runtime automatically creates one internal network per
conversation, a public uplink network, one small shared egress proxy, one
credential broker per active organization, and host `DOCKER-USER`/`INPUT` rules.
The packaged compose file can pre-create the shared
proxy and public network; conversation networks remain runtime-owned. Conversation
containers can reach only their Squid address on port 3128 and tenant broker on
port 8080; they cannot route
directly to peer containers, the Docker bridge gateway, or public networks. The
runtime rejects an existing conversation network unless its ownership labels match,
and broker response rules accept only established or related connections.
proxy blocks private, loopback, link-local, and cloud metadata destinations
before forwarding public HTTP/HTTPS traffic. The broker has a fixed OpenAI
upstream and only exposes the two Codex Responses API POST routes. The API key
exists only in broker memory and is discarded with the broker when the tenant's
last active sandbox stops. Broker containers have no direct public-network
attachment and use the managed Squid proxy for their fixed OpenAI upstream.

Other sandbox drivers are outside the MVP scope. If one is added later, it
should implement the same port and preserve the same ownership boundary.

## Memory Port

Memory is an explicit app-controlled capability, not implicit prompt state.
The bundled SQLite adapter stores `memory_records` with organization,
conversation, scope, subject, kind, text, metadata, confidence, optional expiry,
and a soft-delete timestamp. All read/write operations require both
`tenant_id` and `source_id`; organization-wide knowledge must use a separate,
explicitly authorized app tool.

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

Idle cleanup is exposed through `SQLiteSessionLifecycle` rather than a mandatory daemon:
`lifecycle.close_idle_sessions(...)` selects only idle sessions by TTL and per-source
active-session limits, skips sessions with `queued`/`sending` mailbox items,
delegates teardown to the registered `SessionBackend`, then records the
close/failure in platform tables. This keeps resource policy in the app while
keeping teardown semantics in the platform.

Mailbox enqueue and lifecycle claim use SQLite write transactions so either a
prompt is queued before cleanup sees pending work, or cleanup claims the session
before enqueue can target it. Enqueue is only accepted for `idle` or `busy`
sessions.
Mailbox decision idempotency and optional action ids are scoped by
`(tenant_id, source_id)`.

Backend send and capture are separate durable phases. Once a mailbox item has a
durable acceptance timestamp, retries may recapture that backend operation but
must not resend the prompt. A stale send without durable acceptance is terminal
and explicitly uncertain. This is an at-most-once policy at the send boundary,
not an exactly-once guarantee.
Backends may implement the optional `DeliveryCaptureBackend` capability to bind
recovery to the persisted `SendReceipt`; Codex uses it to read the exact accepted
turn rather than whichever turn happens to be newest after an app-server restart.
Conversation-bound backends and inspectors expose `tenant_id` and `source_id`;
every open, delivery, close, and inspection path rejects a resource bound to
another organization or conversation. Correct
registry wiring is therefore not itself the isolation boundary.
Backend `timed_out` capture results remain pending without consuming transport
failure attempts. The mailbox enforces a separate persisted acceptance-age
deadline so active work can be polled after restart without waiting forever.
Session LLM calls treat a backend `timed_out` result as a timeout and enforce
the request deadline around send/capture. Lifecycle cancellation records a
failed state before propagating, while stale `closing` recovery records an
explicitly uncertain close outcome.

Codex app-server support is exposed as a `CodexThreadInspector`, behind the
generic `SessionInspector` port. App-specific routing LLMs may receive platform
tools such as `search_session_snapshots` or `get_session_context`, but those
tools must read the generalized platform index and only use backend inspectors
as bounded enrichment. The reusable dynamic tool registration point is
`soveren_agent_platform.sessions.SQLiteSessionDirectoryTools.register(...)`, which exposes:

- `platform.sessions/list_runtime_sessions`
- `platform.sessions/search_session_snapshots`
- `platform.sessions/get_session_context`
- `platform.sessions/refresh_session_candidate`

Registration requires `source_id`; every tool operation is confined to that
source and the model cannot override it. Model-facing payloads omit raw
source and backend-session identifiers.

## Migration Ports

The bundled SQLite adapter ships SQL migrations applied with namespace
`platform`. Other storage adapters should provide their own bootstrap/schema
management while preserving the same platform ports.

Consumer bootstrap API:

```text
await bootstrap_platform_storage(db_path)
```

Raw migration runners remain inside the bundled adapter. Applications own and
run their separate schema pipeline before starting platform workers.

For existing SQLite apps, adoption must support a baseline/compatibility path:
if a table already exists and matches the platform contract, mark the platform
migration as applied instead of recreating the table.
