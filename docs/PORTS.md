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
- move an expired lease directly to dead-letter when it has already consumed
  the maximum attempt, rather than reclaiming it again
- permit explicit exhausted-lease recovery for an effect-aware worker only when
  that worker performs terminal reconciliation without invoking the effect
- accept an optional `tenant_id` claim scope and apply it to candidate selection
  and all expired/exhausted cleanup; omission means an intentional global worker

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
- `OutboundQueue`: conversation-scoped enqueue, optional tenant-scoped claim by
  channel, lease renewal, explicit leased/sending/sent/uncertain/dead-letter
  transitions, and safe pre-send retry
- `CronStore`: validated idempotent insert, optional tenant-scoped claim and
  expired-lease cleanup, renew, explicit
  leased/running/uncertain transitions, immutable RRULE anchor, separate next
  schedule and retry timestamps, and fenced completion for recurring and
  one-shot jobs
- `SessionStore`: conversation-scoped get session and set status
- `SessionMailboxStore`: enqueue prompt, claim next for idle session, mark sent/requeue/fail
- `SQLiteSessionLifecycle`: backend-aware session teardown, idle cleanup, and stale-close recovery
- `SessionInspector`: backend-specific live context reader for Codex, Claude, or other execution backends
- `SessionEventStore`: conversation-scoped append/read of session observations
- `SessionSnapshotStore`: conversation-scoped refresh/latest searchable session context snapshots
- `SessionIndexStore`: atomically deduplicate a conversation-scoped inspection
  across its full marker history, append the observation, and refresh its
  snapshot; an existing marker suppresses work only when the latest snapshot
  covers that marker event, otherwise the adapter repairs the snapshot without
  appending another event
- `BatchStore`: append inbound message, load batch state, atomically route batch into the next durable queue
- `RunStore`: claim a tenant/source/event/model/prompt operation, return cached
  planner output, and finalize only with the current run token
- `EffectReconciler`: conversation-scoped, audited, idempotent resolution of uncertain
  actions, outbound messages, and cron jobs
- `MemoryStore`: remember/search/get/forget explicit app-neutral memory records
- `SandboxManager`: acquire/stop/destroy an execution sandbox, ensure container
  directories, run bounded setup commands, and build the app-server exec command

Each port should encode atomic operations, not expose table-shaped CRUD.
When an action executor proves that no effect started, the worker persists the
action's `executing -> queued` retry state before releasing its queue lease to
`retrying`. On the final attempt it instead persists `executing -> failed`
before moving the event to `dead_letter`. This ordering keeps an interruption
recoverable and terminalizes exhausted work without claiming an exactly-once
boundary across the action store and queue. If that final lease expires, the
action worker uses recovery-only claim mode: it never invokes the executor and
converges the action to `failed` or `uncertain` before closing the event. A
missing executor is a deterministic configuration failure, not an uncertain
external outcome.

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
- `soveren_agent_platform.sessions.contracts.SessionIndexStore`
- `soveren_agent_platform.sessions.contracts.SessionSnapshotStore`
- `soveren_agent_platform.sessions.lifecycle.SessionLifecyclePolicy`
- `soveren_agent_platform.sessions.lifecycle.SQLiteSessionLifecycle`
- `soveren_agent_platform.sessions.sqlite.SQLiteSessionStore`
- `soveren_agent_platform.sessions.sqlite.SQLiteSessionMailboxStore`
- `soveren_agent_platform.sessions.sqlite.SQLiteSessionIndexStore`
- `soveren_agent_platform.sessions.sqlite.SQLiteSessionSnapshotStore`
- `soveren_agent_platform.runs.contracts.RunStore`
- `soveren_agent_platform.runs.sqlite.SQLiteRunStore`
- `soveren_agent_platform.reconciliation.contracts.EffectReconciler`
- `soveren_agent_platform.reconciliation.sqlite.SQLiteEffectReconciler`
- `soveren_agent_platform.memory.contracts.MemoryStore`
- `soveren_agent_platform.memory.sqlite.SQLiteMemoryStore`
- `soveren_agent_platform.sandbox.contracts.SandboxManager`
- `soveren_agent_platform.sandbox.contracts.CredentialBrokerProvisioner`
- `soveren_agent_platform.sandbox.contracts.HttpCredentialBrokerProvisioner`
- `soveren_agent_platform.sandbox.docker.DockerSandboxManager`

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
- `provision_http_credential(SandboxHandle, credential, HttpCredentialBinding)`
  returns an opaque conversation-network capability for one fixed HTTPS origin.
- `revoke_http_credential(SandboxHandle, name, scope)` removes the selected binding;
  conversation scope is the default and tenant scope must be explicit.

The generic credential provisioner is intentionally separate from `SandboxManager`.
Sandbox implementations that cannot provide protected HTTP credentials still satisfy
the execution port; consumers must test for `HttpCredentialBrokerProvisioner` before
using that optional capability. The bundled `SandboxedCodexAppServerBackend` performs
that check in its high-level provision/revoke methods.

The bundled Docker implementation uses host Docker as a trusted infrastructure
dependency and creates sibling containers with memory, CPU, PID, disk, temporary
storage, user, and network limits. It labels managed containers with tenant and
conversation hashes, not raw ids. It rejects host/container namespace sharing and any
network outside its infrastructure allowlist. Capacity belongs to one manager
instance; `create_sandbox_manager(...)` is the process composition root shared
by all conversation backends and defaults to one active conversation sandbox.
Docker CLI operations are wall-clock bounded; timeout or caller cancellation
terminates and reaps the child process before returning.
An idle stop removes the tenant broker container but keeps active bindings only in the
current manager process, allowing the same stopped sandbox to resume without capability
churn. Process restart intentionally loses that registry; the consuming application
must provision credentials again from its durable secret store.
The sandboxed Codex adapter stops an idle sandbox only after both its active
thread set and its in-flight backend-operation count reach zero. Implementations
must not reclaim a sandbox while an `open`, `send`, `capture`, or `close` call
that already reserved the backend is awaiting I/O.
Existing stateful Docker sandboxes tolerate only image-reference drift: they
keep their actual image and writable state until explicit destruction, while
new conversations use the configured image. Every other resolved-spec or
hardening-policy change remains a fail-closed incompatibility. The stateless
shared egress proxy is replaced automatically for an image change under the
same firewall-policy version only when no managed conversation container is
running; old proxy-specific allow rules are removed while conversation-network
drop rules remain installed. A firewall-policy version change requires an
explicit matching rule migration and otherwise fails closed.

The Docker socket is not a tenant capability. The platform deployment owns
Docker access and must never expose it through model tools, conversation sandboxes, or
ordinary app handlers. Product integrations configure organization/conversation boundaries and
resource profiles; they do not pass arbitrary Docker options. Protected credentials
are streamed as a complete registry only to a tenant credential broker and never enter
the conversation sandbox. Trusted personal auth-file providers still use `run_command` stdin;
their cache is intentionally sandbox-local. Neither path places secret bytes in
Docker arguments, environment metadata, or labels.

The high-level Docker manager automatically creates one internal network per
conversation, a public uplink network, one small shared egress proxy, one
credential broker per active organization, and host `DOCKER-USER`/`INPUT` rules.
The packaged compose file can pre-create the shared
proxy and public network; conversation networks remain manager-owned. Conversation
containers can reach only their Squid address on port 3128 and tenant broker on
port 8080; they cannot route
directly to peer containers, the Docker bridge gateway, or public networks. The
manager rejects an existing conversation network unless its ownership labels match,
and broker/proxy response rules accept only established or related connections
from their declared service ports. The proxy blocks private, loopback,
link-local, and cloud metadata destinations
before forwarding public HTTP/HTTPS traffic. The broker's built-in OpenAI binding
has a fixed upstream and exposes only the two Codex Responses API POST routes.
Generic bindings use opaque capabilities, fixed public HTTPS origins, explicit method
and path-prefix policy, and per-conversation-network authorization. Credentials exist
only in trusted manager and broker process memory. Idle stop discards the broker copy;
explicit revocation, conversation destruction, or process exit discards the applicable
manager copy. Broker containers have no direct public-network attachment and use the
managed Squid proxy for every upstream. Binding policy bounds request-body read time as
well as size, queue wait, rate, and concurrency so a slow partial upload cannot occupy
a request slot forever. Binding runtime counters survive secret/configuration replacement.
A broker-wide in-flight limit and cgroup-bounded aggregate body budget apply across all
bindings. After a bounded body is read, registry revalidation and forwarding admission
share the registry-update lock; revoke denies requests not yet admitted, while admitted
requests may finish. Tenant lifecycle serialization also keeps broker prepare/start and
stop/idle-removal in one ordering boundary.

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
Text search uses the bundled SQLite FTS index over all eligible records before
applying the result limit. Empty-token searches retain newest-first ordering.

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
  `SessionInspector` implementations, then uses `SessionIndexStore` to record a
  new observation and refresh its snapshot in one atomic operation. Marker
  deduplication covers the full history for that private conversation. The
  bundled adapter also heals state left by an older split write: when a marker
  event exists but the latest snapshot is absent or points to an older event,
  it refreshes the snapshot in the same transaction without duplicating the
  event. `SessionIndexUpdate.recorded` means an event was appended;
  `snapshot_id` means snapshot work was committed.

`SessionInspection` owns its direction and reported session identity. The
worker verifies that identity against the selected `RuntimeSession` before
persistence. Ordinary inspector and index-store failures are item-local and do
not stop later sessions in the batch. Cancellation remains worker-terminal;
failure to list active sessions remains a whole-pass failure.
`SessionStore.list_active(...)` returns sessions in stable ID order and accepts
an optional exclusive `after_session_id`. The worker keeps that cursor only in
process memory so every active session is eventually visited without adding a
durable scheduler or delivery guarantee. Restarting the worker starts a new
scan from the first active session.

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

All public session, event, snapshot, and mailbox operations that address a
concrete session require its owning `tenant_id` and `source_id`. Mailbox-ready
targets carry both `session_id` and `source_id`; a worker must not rediscover a
conversation from an unscoped session id. A replacement adapter must return no
record or reject the operation when any ownership component does not match.

Backend send and capture are separate durable phases. Once a mailbox item has a
durable acceptance timestamp, retries may recapture that backend operation but
must not resend the prompt. A stale send without durable acceptance is terminal
and explicitly uncertain. This is an at-most-once policy at the send boundary,
not an exactly-once guarantee.
Backends may implement the optional `DeliveryCaptureBackend` capability to bind
recovery to the persisted `SendReceipt`; Codex uses it to read the exact accepted
turn rather than whichever turn happens to be newest after an app-server restart.
The mailbox validates both `SendReceipt` and `CaptureResult` at the adapter
boundary. A malformed receipt is treated as an uncertain send and never retried;
a malformed capture follows accepted-delivery retry/failure semantics instead of
escaping with the mailbox row left in `sending` forever.
Backends may also implement `DeliveryAbortBackend`. After the persisted
acceptance-age deadline, the mailbox passes that same receipt to abort the exact
operation before recording failure. Abort is best effort: cleanup errors are
recorded with the failed delivery, and the prompt is never resent.
Conversation-bound backends and inspectors expose `tenant_id` and `source_id`;
every open, delivery, close, and inspection path rejects a resource bound to
another organization or conversation. Correct
registry wiring is therefore not itself the isolation boundary.
`ConversationScope` carries the trusted pair through `LlmRequest` and
`OpenSpec`. Bound backends reject an absent scope as well as a mismatch before
backend I/O. Plain Codex backends derive their boundary from a bound
`DynamicToolRegistry`, so wrapping the backend in `SessionLlmBackend` cannot
hide the tool registry's owner. Scope values remain control-plane data and are
not serialized into prompts, model metadata, or dynamic tool calls.
Backend `timed_out` capture results remain pending without consuming transport
failure attempts. The mailbox enforces a separate persisted acceptance-age
deadline so active work can be polled after restart without waiting forever.
The Codex implementation interrupts the exact turn, attempts thread archive,
and releases transient turn state. Terminal live state is released after capture;
pending timed-out state remains available until it becomes terminal or its
thread is archived or aborted.
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
