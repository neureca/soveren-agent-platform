# Soveren Agent Platform Integration API

This document is the consumer-facing contract for wiring an application to the
platform runtime. `docs/ARCHITECTURE.md` explains why the pieces exist; this
file explains how to connect them.

For a practical app-level setup with package dependency, Telegram token wiring,
and app-owned tools such as ClickUp, see the
[Consuming App Guide](CONSUMING_APP.md).

## Package Dependency

The import package is `soveren_agent_platform`; the distribution package is
`soveren-agent-platform`.

Production deployments must use a versioned dependency from a package index or
a tagged git source:

```toml
dependencies = [
  "soveren-agent-platform>=0.12,<0.13",
]
```

During local development, keep the normal dependency and add a local uv source
override in the consuming app only:

```toml
[tool.uv.sources]
soveren-agent-platform = { path = "/Users/me/projects/agents/soveren-agent-platform", editable = true }
```

Do not deploy an application whose production dependency is an absolute local
path.

## Default Adapter

SQLite is the bundled default adapter, not the platform contract.

The default adapter is useful for embedded agents and first production
deployments because it provides durable queueing, leases, retries, batching,
cron state, action state, outbound state, run tracking, and session mailbox
state without operating a separate broker or database.

Apps should still code against platform ports and composition APIs rather than
SQLite tables. A larger deployment can replace:

- `SQLiteEventQueue` with a RabbitMQ/SQS/NATS/Postgres queue adapter that keeps
  the same idempotency, lease, retry, and dead-letter semantics.
- SQLite stores with Postgres/Mongo/etc. store adapters that implement the same
  module-specific ports.
- bundled SQLite migrations with adapter-specific bootstrap/schema management.

Do not treat `event_queue` or other SQLite tables as app-owned integration
APIs. They are implementation details of the bundled adapter.

Consumer-facing storage I/O is asynchronous. Open concrete SQLite adapters with
`await SQLite...open(db_path)`, call their port methods with `await`, and close
owned adapters during application shutdown. Raw SQLite connections and sync
store functions are implementation details and are not exported integration
APIs.

## Storage Bootstrap

The platform owns only its own runtime tables. Application tables and product
data must stay in the application repo and use a non-`platform` migration
namespace.

For the standard path, let `AgentPlatformApp` bootstrap platform storage before
workers start:

```python
from pathlib import Path

from soveren_agent_platform.app_api import AgentPlatformApp

db_path = Path("data/app.db")
app = AgentPlatformApp(db_path=db_path)
```

If the app has a separate migration pipeline, call the helper there and disable
runtime bootstrap:

```python
from pathlib import Path

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.storage import bootstrap_platform_storage

db_path = Path("data/app.db")
await bootstrap_platform_storage(db_path)

app = AgentPlatformApp(db_path=db_path, bootstrap_storage=False)
```

`bootstrap_platform_storage()` applies bundled platform migrations and then
validates the resulting table, index, and trigger definitions. It does not run
app-owned migrations.

## Minimal Runtime

An app provides handlers and registries; the platform provides durable workers.

```python
import asyncio
from pathlib import Path

from soveren_agent_platform.agent import AgentEvent, AgentHandler
from soveren_agent_platform.app_api import AgentPlatformApp


class MyAgentHandler(AgentHandler):
    async def handle(self, event: AgentEvent) -> None:
        # Parse event.payload, call the app planner, and emit app side effects.
        ...


async def main() -> None:
    app = (
        AgentPlatformApp(db_path=Path("data/app.db"))
        .use_batching()
        .use_agent(handler=MyAgentHandler())
    )
    await app.start()
    try:
        await app.wait()
    finally:
        await app.stop()


asyncio.run(main())
```

`AgentPlatformApp.start()` is fail-fast for platform schema and worker startup
errors. A failed start is terminal for that app instance: the app stops any
partially started workers and closes every managed session or sandbox resource
before propagating the original error. If rollback also fails, both failures are
reported in a `BaseExceptionGroup`. Keep
`AgentPlatformApp.wait()` in the process lifecycle so an unrecoverable worker
failure terminates the service instead of leaving a live but non-functional
process. Queue claim errors are logged and retried, but five consecutive polling
failures terminate the worker by default so a permanent storage failure reaches
the supervisor. Workers expose `max_consecutive_failures` for
deployment-specific tuning.
`AgentPlatformApp.stop()` is terminal for that app instance because it closes
managed session and sandbox resources. Create a new app instance to restart the
runtime against the same durable database.
When its shutdown deadline expires, the supervisor cancels and joins in-flight
leased item processing before those managed resources are closed.

## Inbound Messages

The batching worker consumes durable events with:

- `recipient="batching"`
- `message_type="InboundMessageReceived"`
- a stable `idempotency_key`
- payload fields: `channel`, `source_id`, `raw_event_id`, `text`,
  `message_at`

`channel`, `source_id`, and `raw_event_id` must be non-empty strings;
`message_at` must be an integer timestamp. The worker does not synthesize these
identity or ordering fields. Invalid input is retried/dead-lettered by the queue
without writing a partial batch message.

For generic sources, enqueue through the asynchronous queue port:

```python
from soveren_agent_platform.queue import SQLiteEventQueue

events = await SQLiteEventQueue.open(db_path)
async with events:
    await events.enqueue(
        tenant_id="tenant-a",
        recipient="batching",
        message_type="InboundMessageReceived",
        payload={
            "channel": "web",
            "source_id": "chat-42",
            "raw_event_id": "web:chat-42:msg-100",
            "text": "hello",
            "message_at": 1_720_000_000,
        },
        idempotency_key="web:chat-42:msg-100",
        correlation_id="web:chat-42",
    )
```

Idempotency keys identify one immutable command. Repeating the same key and
input is a normal replay; reusing the key with a changed payload or destination
raises `soveren_agent_platform.idempotency.IdempotencyConflictError`. This is
conflict detection, not an exactly-once guarantee for downstream effects.

For Telegram, normalize to `TelegramInboundMessage` and use the helper:

```python
from soveren_agent_platform.queue import SQLiteEventQueue
from soveren_agent_platform.telegram import TelegramInboundMessage, enqueue_telegram_message

events = await SQLiteEventQueue.open(db_path)
async with events:
    await enqueue_telegram_message(
        events,
        TelegramInboundMessage(
            tenant_id="tenant-a",
            chat_id=123,
            update_id=456,
            user_id=789,
            username="user",
            text="hello",
            payload={"date": 1_720_000_000},
            reply_to_message_id=123,
            message_thread_id=7,
        ),
    )
```

Telegram normalizes reply and forum-topic identifiers to these top-level
integer fields and retains the original update in `payload`. The batching event
copies the normalized fields so trusted app routing can resolve a reply without
parsing provider-specific JSON.

The optional Telegram adapter lives under `soveren_agent_platform.telegram`; core
platform imports do not require Telegram adapter dependencies.

For the default Telegram polling app, use `create_telegram_agent_app(...)` from
`soveren_agent_platform.telegram`. It wires Telegram ingress, Telegram outbound,
batching, agent, actions, cron scheduling, and worker lifecycle from a token,
database path, tenant id, and app-provided `AgentHandler`. It also accepts
`registration_user_ids`, `allowed_chat_ids`, `allowed_user_ids`,
`quiet_window_s`, `max_window_s`, and `max_count` for the common production
knobs. `registration_user_ids` lets trusted users register new chats with
`/start` or `/register`; the resulting `chat_id` is stored in platform storage.
Revoke a stored authorization through
`await telegram_app.revoke_registered_chat(chat_id)`. Lower-level integrations can
call `TelegramChatRegistry.revoke(tenant_id=..., chat_id=...)`; revocation is
tenant-scoped and idempotent. A still-trusted registration user can register the
chat again, so remove compromised users from the registration policy as well.
The high-level runtime refuses to start without a registration policy, an
access allowlist, or explicit `allow_all_updates=True`. Callback hooks pass
through the same chat/user access check as messages. In groups, model-facing
batch text identifies participants by Telegram username, then display name,
then a deterministic per-batch `participant_N` fallback. Telegram user ids
remain internal.
The high-level runtime also passes its fixed `tenant_id` to batching, agent,
actions, cron, and Telegram outbound workers, so equal recipient/channel names
in the same database cannot cross organization boundaries.
When the handler uses a sandboxed Codex runtime, create one `AgentPlatformApp`,
configure Codex on it, construct the handler from the resulting planner, and
pass that same app as `platform=` to `create_telegram_agent_app(...)`. The
factory adds the Telegram workers to that composition root and transfers its
lifecycle to the returned `TelegramAgentApp`. The database paths must match.
An explicitly supplied `bootstrap_storage` value must also match the app's
setting. Existing Telegram worker names are rejected before the queue is opened,
instead of producing a partially composed runtime.
Lower-level helpers such as
`build_telegram_polling_application(...)`, `enqueue_telegram_update(...)`, and
`TelegramSender` are intended for webhook deployments or custom lifecycle
control. The polling builder and `enqueue_telegram_update(...)` still require an
access policy or explicit `allow_all_updates=True`; constructing
`TelegramAccessPolicy()` without a chat or user allowlist is rejected.
Ingress helpers receive a `DurableQueue`, not a raw SQLite connection. Polling
setups that enable chat registration also provide a `TelegramChatRegistry`;
the bundled high-level runtime wires both adapters automatically.

### Telegram Turn Latency And Polling

The standard Telegram factory accepts these optional settings:

```python
runtime = await create_telegram_agent_app(
    # Existing token, db_path, tenant_id, handler, access and platform settings.
    ...,
    batching_idle_max_s=1.0,
    agent_idle_max_s=1.0,
    outbound_idle_max_s=1.0,
    latency_logging=True,
)
```

`batching_idle_max_s` is optional and defaults to **5.0 seconds**, preserving
existing clients. Set it to `1.0` for a shorter batching polling pause. It flows
through `use_batching` to `run_batching_worker(idle_max_s=...)` and the queue
worker; the initial pause is capped by the maximum. The value must be finite
and positive. This controls polling only: the default `quiet_window_s=2` and all
batching decisions remain unchanged. The quiet window uses the existing
integer provider/UTC timestamps and is evaluated when the worker runs. It is
not a strict continuous-time partition: an inbound accepted before a due flush
is actually processed can still join the collecting batch, even after two
seconds. Conversely, provider timestamp quantization can shorten the physical
wait. Increasing polling frequency does not change that source-of-truth rule.

The agent and outbound idle maximums default to **one second** in this factory.
Each idle worker makes at most one empty claim per second at the default;
combined, those two workers make at most two empty claims per second. The knobs
must be finite and positive. Custom `AgentPlatformApp.use_agent(...)` composition
already accepts polling settings; `use_outbound(...)` now forwards
`idle_initial_s` and `idle_max_s` too. Lower-level defaults remain ten seconds.
Lease renewal, retries, due times, and multipart ordering retain their existing
semantics. This removes the previous idle-polling ceiling from those two
stages; it does not reduce batching deadlines, active work ahead of a message,
sandbox cold starts, model time, retry backoff, or provider request latency.

`latency_logging=True` enables the bundled SQLite observer. Configure Python
logging at INFO for `soveren_agent_platform.runtime.latency`. Each summary is
one `turn_latency {JSON}` line; a structured logging formatter can instead read
`LogRecord.turn_latency` as an object. It is one summary per **routed batch /
agent queue event**, so several batched Telegram updates share one turn.
No per-stage log records or span table are created.

The schema has `schema_version=1`, durable `event_id`, `batch_id`, `run_ids`,
`outbound_ids`, inbound/part/attempt counts, reply state counts, `outcome`,
`total_ms`, and `phases`. There is no message text, model output, prompt,
provider metadata, error message, credential, chat ID, or username in a summary.
Agent and outbound failure logs also report exception class rather than its
potentially sensitive message. Other platform/application logging is outside
this observer's contract.

Every phase contains `duration_ms`, `availability` (`measured`, `partial`, or
`unavailable`), and `reason`. Unknown durations are JSON null, never a zero
substitute. Zero can be an actual measured value after rounding. Durations use
`time.monotonic()` within this process:

| Phase | Measured boundary |
| --- | --- |
| `telegram_ingress` | Accepted PTB callback entry through durable enqueue; summed for the batch's updates. Excludes Telegram network delivery and updates rejected by access/registration policy. |
| `batching` | First accepted enqueue through committed `route_batch`; includes batching worker polling and the quiet window. |
| `agent_wait` | Committed batch route through agent handler entry; includes waiting behind already claimed work. |
| `session_open` | Session backend `open`, including sandbox acquisition and app-server initialization when they occur there. |
| `codex_turn` | Session backend send plus capture; includes tools and transport waits, and failed calls. |
| `dispatch` | Decision dispatcher call; includes durable effect enqueue. |
| `outbound_wait` | Committed enqueue (or previous observed retry completion) through `mark_sending`; summed across observed parts/attempts. |
| `telegram_send` | Channel sender call; summed across observed parts/attempts, including failed calls. |

`codex_turn_details` is an additive field in the same schema-version-1 JSON
record. It exposes actual app-server observations, without logging stream data:

| Field | Measured boundary |
| --- | --- |
| `start_ack` | Entry to the native `turn/start` request through the await returning a valid turn ID. Excludes session setup/`thread/resume` before that request. |
| `start_to_first_output` | The same request entry through receipt of the first nonempty `item/agentMessage/delta` notification for that exact thread/turn. Empty deltas, reasoning, tools, stderr and unrelated notifications do not qualify. |
| `first_output_to_completion` | That first delta through receipt of `turn/completed`, including failed/interrupted completions. |
| `reconnects` | Count of observed native `error` notifications with `willRetry=true` for those turns; this is not a TCP connection count. |

The three duration fields contain `duration_ms`, `availability`, `samples` and
`reason`. `turns_observed` counts turn observations, capped at 256 per aggregate.
Distinct observed turns are summed; repeated live capture updates its observation
instead of counting the ACK again. A later history-only read is a separate
unavailable observation; it does not recover clocks from a released live state. Missing stages across some turns are
`partial`. `reconnects_availability` distinguishes an observed zero from an
unavailable counter. These details are separate from the unchanged outer
`phases.codex_turn`, which still measures the complete session send/capture.

ACK latency and time to first output share a start and overlap: app-server may
emit output even before the `turn/start` ACK. Reader-owned event clocks retain
those early notifications; capture contributes them to the current worker's
aggregate, so a reused reader cannot attribute output to an earlier turn's
logging context. No native payload or token usage enters the observer.
No ACK (request error/timeout), no nonempty delta, capture timeout/cancellation,
or no `turn/completed` means the corresponding duration is null/unavailable.
An interrupt or transport disconnect is not a fabricated completion event.
History-only capture after process/app-server replacement cannot recover old
monotonic clocks and reports `history_recovery`; backend adapters without
native event clocks also leave those stages unavailable. The existing overall
best-effort restart/emission limits still apply. There are no new DB writes or
per-stage log lines for these measurements.

The same `codex_turn_details` now also contains a `runtime` object. It measures
only work inside the observed session `phases.codex_turn` calls; cold setup in
`session_open`, close after capture, and detached/background tasks are excluded.
No application wiring beyond `latency_logging=True` is needed.

| `codex_turn_details.runtime` field | Measured boundary |
| --- | --- |
| `prepare_turn` | `_activate_backend(prepare_turn=True)` in sandboxed send, including idle-stop cancellation and lifecycle-lock waiting. |
| `prepare_warm` | Work after taking the lifecycle lock when a backend already exists. This is entry state; the branch can still trigger a rebuild. |
| `prepare_cold` | Work under that lock when no backend exists and preparation builds one. A cold `open()` before the LLM turn belongs to `session_open`, not this metric. |
| `sandbox_acquire` | Existing `sandbox_manager.acquire` calls in preparation/build, including manager-owned readiness/relay checks performed there. |
| `credentials_refresh` | Existing `refresh_for_turn` hook call, when invoked. Includes the provider's guard, callback/binding update and any error revocation; it does not imply an OAuth rotation occurred. A trusted wrapper may expose a no-op hook for a startup-only provider. |
| `backend_build` | Entire `_build_backend_locked`, including its nested acquire when needed, directory setup, credential provisioning and native adapter construction. |
| `backend_rebuild` | Build after an existing backend is shut down due to sandbox replacement or changed launch configuration; overlaps `backend_build`. |
| `backend_shutdown` | Existing native backend shutdown calls inside the observed turn. A later idle/application shutdown is excluded. |
| `ensure_thread` | Native send's `ensure_thread`, including initialization/resume when needed, before `turn/start`. |
| `native_event_window` | Union of observed request-entry-to-latest-ACK/delta/completion intervals, clipped to each observed outer call. Repeated observations replace their interval. A timeout does not extend this window to an invented completion. |
| `accounted` | Union of all runtime spans and native event windows, calculated separately inside each observed outer call and then summed. Parent/child spans and overlapping native metrics are not added twice. |
| `unmeasured` | Observed outer-call elapsed time minus that union. It is an unattributed residual, not a claim that a particular subsystem caused the gap. |
| `prepare_unmeasured` | `prepare_turn` elapsed time minus the union of its measured child work; warm/cold branch envelopes are excluded from this subtraction. Includes lock/idle cancellation and other uncovered preparation work. |

`runtime.scope` is `"phases.codex_turn"`; `runtime.attempts` counts retained,
finished outer-call observations. Each table field is an object with this shape:

```json
{"duration_ms": 12.34, "availability": "measured", "samples": 1,
 "incomplete_samples": 0, "reason": null}
```

Absent calls/coverage use `duration_ms=null`, `availability="unavailable"`,
`samples=0`, `reason="not_observed_in_codex_turn"`. Optional rebuild/shutdown or
uninvoked refresh are not zero substitutes. A stage interrupted by error or
cancellation records elapsed-to-failure with `availability="partial"` and
`reason="incomplete_observation"`. Derived union/residual measurements belonging
to failed outer calls are also partial. A derived residual can legitimately be
zero when observed coverage is exact. `incomplete_samples` never exceeds
`samples`. A complete timed stage is still measured when another stage later
fails; phase/outcome semantics are not inferred from each other's status.

Two nested detail groups use the same metric shape and remain inside the same
terminal JSON record:

- `runtime.sandbox_acquire_details`: `orphan_recovery`,
  `conversation_lock_wait`, `capacity_reservation`, `tenant_lock_wait`,
  `ensure_egress`, `egress_lock_wait`, `internal_network`, `network_subnet`,
  `public_network`, `egress_container`, `egress_start`, `egress_connect`,
  `egress_health`, `relay_reconcile`, `relay_identity`, `relay_resolve`,
  `relay_host_check`, `relay_firewall`, `relay_probe`, `network_policy_inspect`,
  `network_policy_apply`, `broker_network_prepare`, `broker_remove_unowned`,
  `container_lookup`, `reuse_existing`, `container_create`.
- `runtime.credentials_refresh_details`: `bind`, `binding_lock_wait`,
  `auth_file_check`, `get_access_token`, `grant_validation`,
  `provision_http_credential`, `endpoint_validation`, `revocation`,
  `host_conversation_lock_wait`, `host_tenant_lock_wait`.
- Both groups include `broker_tenant_lock_wait`, `broker_shared_lock_wait`,
  `broker_verified_networks`, `broker_container`, `broker_health`,
  `broker_network_access`, `broker_registry_sync`, `broker_prune_networks`,
  plus `parent_samples`, `accounted`, `unmeasured`.

Lock metrics include only the original lock acquisition, not work while holding
it. Capacity reservation includes waiting for a slot. Egress/network/container
metrics surround the existing inspect/create/connect/start/health operations;
`relay_firewall` includes the awaited, shielded firewall reconciliation and
`relay_probe` the existing connection probe. `auth_file_check` measures only the
existing existence/symlink guard, without reading `auth.json`. `get_access_token`
measures the app callback, including any app-owned OAuth/storage work it awaits;
no claim about that callback's internal stages is made. Broker metrics surround
its original verification/container/health/firewall/registry/pruning operations.
`revocation` includes the original cancellation-safe cleanup drain.

A broker span belongs to the group whose parent call actually encloses it.
Internal parent IDs distinguish adjacent zero-duration spans and are never
serialized. `accounted` is the union of observed descendants clipped to each
parent, excluding the broad `ensure_egress` and `relay_reconcile` envelopes for
acquire, and `bind` for refresh. Other nested operations, including broker work
inside provisioning/preparation, overlap and are counted only once. The group's
`unmeasured` is parent elapsed time minus that union; `parent_samples` counts
retained parent calls. `runtime.egress_unmeasured` subtracts the egress child
union (excluding the relay envelope); `runtime.relay_unmeasured` subtracts the
identity/resolve/host-check/firewall/probe union from relay reconciliation.
Missing parent calls leave residuals unavailable. Failed parents make residuals
partial; successful child stages retain measured status. These details share
the existing 256-span budget, rather than adding an unbounded per-subsystem
cache. They contain only static keys, counts, durations and availability reasons;
no URLs, addresses, container identifiers, bearer values or exception messages
are collected. Original operational logs and security policy are unchanged.

These fields intentionally overlap. In particular, do not add `prepare_turn`
to `sandbox_acquire` or `credentials_refresh`, or add `start_ack` to time-to-first
output. For untruncated observations with available `accounted`,
`accounted + unmeasured` equals `phases.codex_turn` within rounding. Failure
flags change availability, not that interval arithmetic. When no child/native interval was observed,
`accounted` is unavailable and `unmeasured` contains the whole observed elapsed
call. Event-only/history-recovery gaps are not silently converted to completion.

Each outer call owns its observations by asyncio task; copied ContextVars in a
reader or idle task cannot add work to another task's runtime measurements.
Concurrent LLM calls have separate outer-call windows, even in one aggregate.
At most 256 outer observations and 256 runtime spans are retained per aggregate,
plus the existing bounded native observations. `runtime.truncated=true` and
`availability="partial"` / `reason="observation_limit"` mark retained values
when that budget is exhausted. Restart and receipt replay cannot recreate
missing clocks or call the model solely for measurement. No timestamps, prompts,
credential values or content are added to DB rows or per-event logs.

These sums are not an additive decomposition of `total_ms`: multipart waits and
batched ingress can overlap, and context building, cleanup, storage finalization,
and agent retry backoff can occupy additional time. `total_ms` runs from the
first observed PTB callback to the final summary check. `measurement_scope` is
`observed_attempts_in_this_process`; `partial` marks missing stages/attempts.
`send_attempts_measured` and `wait_attempts_measured` can be compared with durable
`outbound_attempts`; a partially observed send/wait phase is explicitly partial.
Identifiers are capped at 256 per summary (`identifier_limit_reached`), while
counts include all correlated rows.

Warm preparation optimization (candidate wheel and matching broker image):

- Acquire still verifies tenant/conversation network ownership, broker identity,
  health, current Docker `StartedAt`, attachment/IP and exact firewall policy.
  It omits the admin registry write only when the complete wire payload matches
  a successful acknowledgement in that same container/process generation.
  No TTL or unverified warm-state cache replaces those checks. Network/grant/IP
  drift changes the payload and requires an update. Same-ID restart invalidates
  every acknowledgement and restores all active registries before sandbox use.
- SIWC still checks for native `auth.json`, invokes the application's current
  token callback and updates the binding on every turn, even for identical token
  bytes. Capabilities and admission counters remain stable across rotation.
  A failed/uncertain update clears acknowledgements and decommissions the broker;
  ownership failure also decommissions it rather than leaving stale serving
  grants. The existing retained-state recovery and revoke-on-error apply.
  Decommission drains under the lifecycle owner before propagating repeated
  caller cancellation; another update cannot enter while removal is unfinished.
- Docker network firewall reconciliation uses one stdlib helper for each egress
  or broker rule group. It reads actual chains, computes exact owned deletions/
  insertions, uses one `iptables-restore --noflush` transaction only when needed,
  and verifies the resulting policy. Healthy unchanged order needs no mutation.
  DROP guards, established-response restrictions and allow-before-drop ordering
  remain; foreign rules retain their relative order and are never flushed.
  Broker address changes revoke old exact allows before granting the new address.
- The single named network-policy helper is serialized across egress and broker.
  It uses the same ownership-checked removal, cancellation drain and separate
  15-second mutation/cleanup budgets as relay reconciliation. Unknown removal or
  foreign helper ownership aborts setup. Host relay IP/MAC policy and its probe
  remain in their existing separate transaction and lock.
- The broker image includes `/opt/soveren/credential_broker_admin.py`. This narrow
  stdlib client forwards bounded stdin through the same permission-restricted
  Unix socket and length-prefixed protocol; response size is at most 4096 bytes,
  payload at most 1 MiB, and the socket exchange has a ten-second deadline.
  No HTTP-server dependencies are imported by each admin exec. Errors are generic
  and credentials never appear in argv, files or logs. The old
  `credential_broker.py admin` command dispatches to this client before server
  imports, allowing an older manager to use the new image.

The supported composition API accepts optional trusted deployment configuration
`credential_broker_image: str | None = None`. Omission keeps the existing release
reference. For a reversible pilot, load the candidate image under its separate
tag and pass it when configuring the runtime; the release tag need not change:

```python
codex = platform.configure_sandboxed_codex(
    credentials_for_tenant=credentials_for_tenant,
    model=model,
    credential_broker_image="soveren-credential-broker:warm-optimization-20261016-amd64",
)
```

The image override changes only the broker image, retaining its network alias,
UID, policy, quotas and the sandbox/egress images. It is process composition
configuration, never tenant input or a model tool. Empty/non-string values fail
before runtime activation. Changing it requires restarting the owning runtime.
With no broker container present, first provisioning creates the candidate
directly; no data migration or release-tag replacement is required. Reverting
the wheel and omitting/reverting this parameter restores the previous pairing.

The new manager requires the matching new broker image; an older image lacks the
admin entrypoint and fails closed. Install the image and wheel together and
recreate the broker through a fresh control-plane activation. Changing the image
tag alone does not update an existing container. The default image references,
platform version, CPU/memory limits and DB schema are unchanged
by this optimization. The pilot opts into its broker image via the additive parameter. Retain the previous image and wheel as a rollback pair;
registries are memory-only and are reprovisioned by the application after restart.
A skipped acquire registry write is absent (`null/unavailable`) in
`broker_registry_sync`; it is not fabricated as a zero-duration call. The single
terminal `turn_latency` contract and content-free detail keys are unchanged.

Application integration requirements:

- Pass the same `platform=` configured for sandboxed Codex to the Telegram
  factory; the factory owns the observer and all its worker wiring.
- Use `PlannerRuntime.run_dispatch_turn(...)` or `run_planner_dispatch_turn(...)`
  inside the awaited handler. Standard `OutboundDecisionHandler` correlates
  effects automatically. Custom handlers, including `enqueue_telegram_text`,
  must set `correlation_id=event.id` (or `context.source_event_id`) on **every
  part**, with the owning tenant/source. Correlation is an identifier, never
  authorization. Missing correlation cannot be inferred from text/chat identity.
- Await all effects belonging to the reply before returning the handler. Detached
  tasks, later actions, mailbox replies, custom storage adapters and custom
  senders are outside this initial end-to-end contract. A non-session LLM backend
  has unavailable session/Codex phases; raw enqueue helpers have unavailable PTB
  ingress phases.
- Run the normal platform bootstrap before starting workers. Migration 028 adds
  only the outbound turn lookup index; no log table, span writes or backfill.

The observer checks durable states after a worker successfully commits an owned
transition. `sent` requires a done agent event and every correlated Telegram
part sent. Retryable failures produce no intermediate summary. A terminal agent
failure emits `agent_failed`; an unsuccessful part emits `send_failed`; an
ambiguous send emits `uncertain`, even when some parts were delivered. The
remaining queued/blocked parts are shown in `reply_states`. An agent event with
no correlated Telegram reply emits `no_reply`, not a Telegram delivery success.
The first terminal outcome is summarized; later manual reconciliation is owned
by the existing reconciliation API. Completed receipt replays perform no new
inference or send and do not create a new turn measurement.

**Restart and reliability limits:** phase clocks and log deduplication are
process-local, bounded to 2048 entries in each observer cache (plus 256 part
measurements per turn). Restart, eviction, multiple runtimes, a lost lease,
unobserved work or a failed log sink can produce a missing/duplicate summary or
unavailable stages. No background recovery scanner is introduced. In particular,
a crash after durable completion but before logging, and an exhausted lease
terminalized by queue cleanup without an observed worker transition, may produce
no summary. A recovered outbound attempt uses its durable correlation to load
the original event/batch/run IDs and UTC Unix-second timestamps, with lost
monotonic stages null; no wall-clock subtraction is presented as a measured
phase. The UTC fields distinguish provider `first_message_at_unix_s`, platform
`batch_created_at_unix_s`, agent `event_created_at_unix_s` /
`event_updated_at_unix_s`, and confirmed `last_sent_at_unix_s`.
Exactly-once logs across crashes would require a durable emission/outbox boundary
and a deduplicating sink; complete recovered phases would also require durable
measurements. This API deliberately makes neither guarantee. Telemetry read
failures do not retry a committed business effect.

`LlmResponse.duration_ms`, `input_tokens`, `output_tokens`, and `cost_usd` are now
nullable; consumers must handle `None`. A session-backed response measures the
actual open/send/capture elapsed time before close, using a monotonic clock;
its unreported cost/token usage stays `None`. An explicitly reported numeric
zero remains valid. Do not treat absent Codex usage as free inference.

## Standard Worker Modules

Compose only the modules the app needs:

```python
from soveren_agent_platform.actions import ActionRegistry
from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.sessions import SessionBackendRegistry, SessionInspectorRegistry

app = (
    AgentPlatformApp(db_path=db_path)
    .use_batching()
    .use_agent(handler=agent_handler)
    .use_actions(registry=ActionRegistry())
    .use_cron(tenant_id="tenant-a")
    .use_session_mailbox(
        tenant_id="tenant-a",
        session_backends=SessionBackendRegistry(),
    )
    .use_session_indexer(
        tenant_id="tenant-a",
        session_inspectors=SessionInspectorRegistry(),
    )
)
```

Pass a `SessionBackendRegistry` when backends can be registered after application
composition. Mailbox workers read that live registry, and `AgentPlatformApp`
discovers its shutdown-capable backends at stop time so late registrations are
closed with the rest of the runtime.

Register each concrete channel sender before enabling its outbound worker:

```python
from soveren_agent_platform.outbound import OutboundRegistry

outbound = OutboundRegistry()
outbound.register("telegram", telegram_sender)
app.use_outbound(registry=outbound, channels=["telegram"], tenant_id="tenant-a")
```

`tenant_id` is optional on batching, agent, actions, outbound, and cron workers.
When supplied, it fences due-row selection and expired/exhausted cleanup.
`AgentPlatformApp.use_cron(...)` always publishes due jobs as idempotent
`CronJobDue` events to the durable agent queue. For the bundled SQLite runtime,
event insertion and advancing the one-shot or recurring schedule commit in one
transaction. A crash before that transaction leaves a reclaimable lease; a
successful commit contains both the event and the next cron state. Application
bootstrap does not construct or supply a cron handler. `run_cron_worker(...)`
and `run_cron_store_worker(...)` expose the same atomic event-publication path
for custom composition.

`AgentPlatformApp` owns one `agent_recipient`, defaulting to `"agent"`, and uses
it for batching output, cron events, and the agent worker. Configure a
non-default value once on the application constructor; individual standard
workers reject routing overrides that could leave internal events without a
consumer.

### Scheduled Job Controls

Applications create scheduled work through their typed planner decisions. The
decision schema and natural-language policy remain app-owned; the platform
handler persists the resulting job with the trusted tenant/source identity from
the dispatch context:

```python
from datetime import datetime
from typing import Literal

from soveren_agent_platform.decisions import (
    BaseDecision,
    CronDecisionHandler,
    DecisionDispatcher,
    DecisionRegistry,
)


class ScheduleReminder(BaseDecision):
    kind: Literal["schedule_reminder"]
    run_at: datetime
    text: str
    recurrence: str | None = None
    timezone: str = "UTC"


decisions = DecisionRegistry()
decisions.register("schedule_reminder", ScheduleReminder)

dispatcher = DecisionDispatcher()
dispatcher.register(
    "schedule_reminder",
    CronDecisionHandler(
        name="reminder",
        run_at=lambda decision, context: int(decision.run_at.timestamp()),
        payload=lambda decision, context: {"text": decision.text},
        rrule="recurrence",
        timezone="timezone",
    ),
)
```

Pass `decisions` as `decision_parser` and `dispatcher` to
`PlannerRuntime.run_dispatch_turn(...)`. The runtime must be constructed with
`DecisionEffects` as shown in [Planner Composition](#planner-composition); its
`cron` port receives the job. At due time,
`AgentPlatformApp.use_cron(...)` atomically advances the schedule and publishes
one durable `CronJobDue` event to the configured agent recipient. Its payload
contains `cron_job_id`, `source_id`, `name`, the app-owned `payload`, and
`run_at`, so the app's normal `AgentHandler` performs the reminder behavior.
No separate cron handler or cron event worker is involved.

The public schedule-control port lists and cancels jobs inside one private
conversation. The bundled Codex tools bind that scope during registration, so
the model supplies only an optional list limit or a job id:

```python
from soveren_agent_platform.cron import (
    SQLiteCronStore,
    register_scheduled_job_tools,
)
from soveren_agent_platform.sessions import DynamicToolRegistry

scheduled_jobs = await SQLiteCronStore.open(db_path)

def tools_for(scope):
    tools = DynamicToolRegistry()
    register_scheduled_job_tools(
        tools,
        scheduled_jobs,
        tenant_id=scope.tenant_id,
        source_id=scope.source_id,
    )
    # Register app-owned tools on the same conversation-bound registry.
    return tools
```

This exposes `platform_schedules/list_scheduled_jobs` and
`platform_schedules/cancel_scheduled_job`. Listing returns active job id, name,
status, next business `run_at`, RRULE, and timezone. It deliberately omits the
app-owned payload and all routing/lease fields.
Pass `tools_for` as `tool_registry_factory` to the same
`AgentPlatformApp.configure_sandboxed_codex(...)` call that creates the planner
backend.

Cancellation outcomes are:

- `cancelled`: pending or leased work will not run;
- `current_run_may_complete`: the current due event is already queued, leased,
  or retrying; that event is not interrupted, but the job will not recur;
- `already_cancelled` or `already_finished`: the job was already terminal;
- `not_found`: the id does not exist in the registered conversation, including
  an id owned by another tenant or source.

The operation does not interrupt or remove the current dispatched event and
does not claim exactly-once delivery. Close the owned `SQLiteCronStore` during
application shutdown.
Product policy still owns how natural-language recurrence becomes a validated
RRULE and when the agent should ask the user to disambiguate multiple jobs.

### Migrating From 0.6

Update the consuming dependency to `soveren-agent-platform>=0.12,<0.13` and let
the normal platform bootstrap apply migration
`027_cron_event_only_runtime.sql`. The migration preserves pending, leased,
fired, cancelled, and dead-letter jobs. Legacy `running` and `uncertain` rows
from the removed direct-execution path become `dead_letter` with an explanatory
`last_error`; the obsolete `retry_at` column is removed.

Remove imports and calls for `CronHandler`, `QueueCronHandler`,
`CronEventStore`, `CronNotStartedError`, `run_cron_event_worker`, and
`run_cron_event_store_worker`. There is no compatibility wrapper. Standard
composition uses `AgentPlatformApp.use_cron(...)`; custom composition uses
`run_cron_worker(...)` or `run_cron_store_worker(...)`. Both paths publish
`CronJobDue` through the durable agent queue and advance the schedule in the
same SQLite transaction.

Cron-specific uncertain-effect reconciliation is removed. This does not change
action and outbound reconciliation, which still handles uncertain external
effects. Do not edit `cron_jobs` manually during the upgrade.

The app owns all product policy:

- planner prompts and model choice
- decision schemas exposed to users
- action executors and approval copy
- outbound channel credentials
- app-owned database migrations
- tenant/user authorization

The platform owns runtime mechanics:

- durable queue semantics
- inbound batching
- worker leasing/retries/dead-letter behavior
- run tracking
- action/outbound/session/cron lifecycle tables
- explicit memory records when the app opts into memory
- execution-session mailbox and indexing contracts

`tenant_id` is the organization boundary. `source_id` is the private
conversation boundary inside that organization. Direct chats use distinct
source ids; participants in one group chat share one source id. Platform
conversation state never falls back to tenant-wide reads. Organization-wide
business data must be exposed through an app-owned tool that performs its own
authorization.

## Planner Composition

`PlannerRuntime` composes storage and routing ports; it does not expose a raw
database connection. For the bundled adapter, keep these objects open for the
application lifetime:

```python
from soveren_agent_platform.actions import SQLiteActionStore
from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.context import SQLitePlannerContextBuilder
from soveren_agent_platform.cron import SQLiteCronStore
from soveren_agent_platform.decisions import (
    DecisionEffects,
    SQLiteActionDispatchEffects,
    SQLiteDecisionDispatchStore,
)
from soveren_agent_platform.outbound import SQLiteOutboundQueue
from soveren_agent_platform.queue import SQLiteEventQueue
from soveren_agent_platform.runs import SQLiteRunStore
from soveren_agent_platform.runtime import PlannerRuntime
from soveren_agent_platform.sessions import (
    CodexApiKeyCredentials,
    DeterministicSessionRouter,
    SQLiteSessionMailboxStore,
)


async def credentials_for_tenant(tenant_id: str):
    api_key = await application_secrets.openai_api_key(tenant_id)
    return CodexApiKeyCredentials(api_key)


run_store = await SQLiteRunStore.open(db_path)
decision_dispatch_store = await SQLiteDecisionDispatchStore.open(db_path)
context_builder = await SQLitePlannerContextBuilder.open(db_path)
session_router = await DeterministicSessionRouter.open(db_path)
action_store = await SQLiteActionStore.open(db_path)
outbound_queue = await SQLiteOutboundQueue.open(db_path)
event_queue = await SQLiteEventQueue.open(db_path)
session_mailbox = await SQLiteSessionMailboxStore.open(db_path)
cron_store = await SQLiteCronStore.open(db_path)
action_dispatch = await SQLiteActionDispatchEffects.open(db_path)
effects = DecisionEffects(
    actions=action_store,
    outbound=outbound_queue,
    events=event_queue,
    session_mailbox=session_mailbox,
    cron=cron_store,
    action_dispatch=action_dispatch,
)
platform = AgentPlatformApp(db_path=db_path)
codex_runtime = platform.configure_sandboxed_codex(
    credentials_for_tenant=credentials_for_tenant,
    model="your-codex-model",
    resources="small",
)

planner = PlannerRuntime(
    run_store=run_store,
    context_builder=context_builder,
    llm_backend=codex_runtime,
    session_router=session_router,
    effects=effects,
    decision_dispatch_store=decision_dispatch_store,
)

result = await planner.run_turn(
    event=event,
    prompt_builder=prompt_builder,
    decision_parser=decision_parser,
    config=planner_config,
)
```

The consuming app selects the backend type, model, tenant credential resolver,
and resource profile during bootstrap. `PlannerRuntime` owns that `LlmBackend`
dependency; an event, user message, prompt, or model response cannot select or
replace it for an individual turn. `SandboxedCodexRuntime` resolves the trusted
`ConversationScope` produced by the planner, lazily creates one backend per
organization/conversation pair, and reuses it across turns.
When configured, `output_schema` applies only to the planner turn. Durable
session prompts sent through the mailbox remain ordinary conversation turns and
do not inherit the planner schema.

The consuming app also owns prompts, decision parsing, and business policy.
Keep every explicitly opened storage/routing adapter above open for the
application lifetime and close it during shutdown. `AgentPlatformApp`
automatically owns the configured Codex runtime lifecycle; apps must not add a
second lifecycle wrapper.
To dispatch decisions through platform effects, construct `PlannerRuntime` with
an explicit `DecisionEffects`; omitted effects cannot accidentally execute a
decision.
Planner replay is valid only for the same complete `AgentEvent`. Reusing its
tenant/source/event/model/prompt operation key with changed event data raises
`IdempotencyConflictError` instead of returning an older decision.

Passing `decision_dispatch_store` enables one accepted decision per
`(tenant_id, source_id, trigger_event_id)`. The receipt claim happens before
the LLM call, so a completed replay and an active concurrent attempt do not
spend another inference. Once a validated decision is accepted, changing
`model` or `prompt_version` cannot replace it:

- a crash after acceptance but before the effect re-dispatches the stored
  decision;
- a crash after the effect but before receipt completion relies on the same
  effect idempotency key, then records the recovered effect result;
- a crash after receipt completion returns the stored
  `PlannerDispatchResult` without invoking the dispatcher.

Port-based `PlannerRuntime` composition remains backward compatible when the
store is omitted, but then it retains the earlier run-level semantics and does
not provide this business guarantee. Set
`decision_dispatch_receipts_enabled=False` only as an explicit compatibility
opt-out. The low-level SQLite helper automatically composes
`SQLiteDecisionDispatchStore` when it receives a connection.

Decision receipts do not provide exactly-once execution of arbitrary external
calls. Decision handlers must route through durable idempotent platform
effects; uncertain external outcomes still require reconciliation.
Decision payloads, planner/dispatch context, and `DispatchResult.metadata` use
the strict recursive `JsonObject` contract: null, booleans, finite numbers,
strings, lists, and string-keyed objects only. App decision schemas must
be Pydantic models with a string `kind`; `BaseDecision` remains the convenient
strict default rather than a requirement. Aliases and computed fields are
persisted in Pydantic round-trip form so the same registry can parse a receipt
replay. Decision effects require an outbound adapter with
`enqueue_with_result()`, preventing receipt replay from losing the original
message ID.
The platform owns dispatch identity (`tenant_id`, `source_id`, `source_event_id`,
and `run_id`). Consumers can add only `actor_id` and strict JSON metadata
through `DispatchContextExtras`; standard effect idempotency always derives
from the immutable source event ID.

A failed planner run stores `error_type` and `error` in its durable output. If
one operation reports multiple failures through `BaseExceptionGroup`, the
output also contains a recursive `errors` list in the original order. Session
LLM failures put the request failure first and cleanup failure second, so a
failed close cannot replace the cause of the failed model call.

## Codex Runtime

### Auth Failures And Accepted-Turn Replay (Since 0.8.1)

This guard is available from 0.8.1 and is absent from 0.8.0. It does not change
the provider id, endpoint, auth configuration, or native reconnect settings.

The adapter interrupts the named turn at the first observable auth/region 403
or native `unauthorized` error notification. It exposes `CodexTurnFailure`, a
subclass of both `CodexAppServerError` and the generic
`NonRetryableEventError` (exported from `soveren_agent_platform.agent`). The
agent worker writes `dead_letter` after the actual first attempt with the
current lease token. A cleanup failure cannot hide this classification.
Other handler failures keep the existing delayed retry policy. Native
transient reconnect notifications remain non-terminal and can finish normally.
Once Codex reports an accepted turn as failed or interrupted, the queue does
not start the entire event again, including any tools that may already have run.
Recovering the exact accepted turn continues to use `capture_delivery` and the
stored receipt, rather than resending the prompt.

For Codex 0.143.0, refresh-403 HTTP metadata is lost; the compatibility adapter
recognizes only the exact anchored `Failed to refresh token: 403 Forbidden: ...`
format of the native error detail. Model output, country keywords, and arbitrary
text containing `403` do not trigger auth classification. This is verified by
offline contract tests against the packaged binary and synthetic OAuth/Responses
endpoints; no real credentials or inference API are needed.

A strict total retry budget for timeout/429/502/503 is **not implemented**.
Codex 0.143.0 ignores changes to built-in `openai` provider definitions, has
hidden request retries, and treats ordinary HTTP 429 as a terminal `RetryLimit`
(`retry_429=false`). A provider alias alone cannot resolve the 429 behavior.
Changing Codex/provider control requires a separate reviewed decision. A lost
`turn/start` response is still an ambiguous acceptance; this change does not
claim exactly-once effects across crashes or roll back already running tools.

The source behavior is pinned to
[Codex 0.143.0 retry handling](https://github.com/openai/codex/blob/c4d748f586a84a3ed5b6aceb82e9a1db4abb1cda/codex-rs/core/src/responses_retry.rs),
[auth refresh](https://github.com/openai/codex/blob/c4d748f586a84a3ed5b6aceb82e9a1db4abb1cda/codex-rs/login/src/auth/manager.rs),
and [provider configuration](https://github.com/openai/codex/blob/c4d748f586a84a3ed5b6aceb82e9a1db4abb1cda/codex-rs/model-provider-info/src/lib.rs).

Externally triggered Codex workloads use
`AgentPlatformApp.configure_sandboxed_codex(...)`.
This is the supported planner path for Telegram, webhooks, email, and other
inputs controlled by users outside the trusted application process.

The low-level `CodexAppServerBackend` remains available for trusted local
processes and backend-adapter development through the sessions package. The
generic session-to-LLM adapter is internal and is not exported from the public
LLM package, so host execution cannot be selected accidentally through
autocomplete.

The supported MVP path is Docker. The trusted application control plane needs
Docker CLI access. In a compose deployment, mount `/var/run/docker.sock` only
into that service. Conversation sandbox containers never receive the socket.
The package creates one internal bridge network per conversation with Docker
inter-container connectivity disabled, a public proxy network, one shared egress
proxy, one shared credential broker per Docker host, and fail-closed host firewall rules. It then creates the
conversation container and applies the `small` or `medium`
resource profile, registers the conversation backend internally,
and owns shutdown/idle-stop behavior. No repository checkout or separate
infrastructure command is required by the application integrator.
The MVP assumes one trusted control-plane process per Docker host; overlapping
replicas must not manage the same sandbox labels and networks.

```python
from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.context import SQLitePlannerContextBuilder
from soveren_agent_platform.runs import SQLiteRunStore
from soveren_agent_platform.runtime import PlannerRuntime
from soveren_agent_platform.sessions import CodexApiKeyCredentials


async def credentials_for_tenant(tenant_id: str):
    api_key = await application_secrets.openai_api_key(tenant_id)
    return CodexApiKeyCredentials(api_key)


platform = AgentPlatformApp(db_path=db_path)
codex_runtime = platform.configure_sandboxed_codex(
    credentials_for_tenant=credentials_for_tenant,
    model="your-codex-model",
    resources="small",
    max_active_sandboxes=4,
)

run_store = await SQLiteRunStore.open(db_path)
context_builder = await SQLitePlannerContextBuilder.open(db_path)
planner = PlannerRuntime(
    run_store=run_store,
    context_builder=context_builder,
    llm_backend=codex_runtime,
)
```

Keep `run_store` and `context_builder` for the application lifetime and close
them from the application shutdown path. `AgentPlatformApp.stop()` closes the
Codex runtime; it does not take ownership of app-opened planner stores. The
complete Telegram + Codex lifecycle is shown in
[the consuming-app guide](CONSUMING_APP.md#sandboxed-codex).

### Selective Sandbox Egress

Trusted application bootstrap may pass a tuple of `SandboxEgressUpstream` groups
as `egress_upstreams` to `configure_sandboxed_codex`. Import the value object from
`soveren_agent_platform.sandbox`:

```python
from soveren_agent_platform.sandbox import SandboxEgressUpstream

codex_runtime = platform.configure_sandboxed_codex(
    credentials_for_tenant=credentials_for_tenant,
    model="your-codex-model",
    resources="small",              # remains 512 MiB
    max_active_sandboxes=3,          # application-owned capacity
    egress_upstreams=(
        SandboxEgressUpstream(
            proxy_url="http://host.docker.internal:10810",
            destination_hosts=("api.provider.example", "chat.provider.example"),
        ),
        SandboxEgressUpstream(
            proxy_url="http://second-proxy.example:3128",
            destination_hosts=("api.other-provider.example",),
        ),
    ),
)
```

The policy belongs to the single shared Squid on that Docker host and covers
both conversation HTTP(S) and credential-broker forwarding. It is not a
tenant/model tool setting. Applications own the list of actual provider
hostnames and the HTTP parent assigned to each group; the platform has no
built-in provider list. An empty tuple retains direct egress. Selected exact
hostnames use only their group's parent; every
other destination is forced direct. Match ignores case and a DNS trailing dot;
subdomains require their own entries. URLs, paths, IP literals, wildcards, and
suffix selectors are rejected. HTTPS selection uses the CONNECT hostname;
model names and encrypted request paths cannot select a route.
Duplicate hostnames within or across groups are rejected after case/trailing-dot
normalization, even if both entries assign the same proxy. Groups using the
same normalized proxy URL are combined into one peer, and group order has no
routing effect. A policy is bounded to 16 input groups and 256 total hostnames.

`proxy_url` requires `http://host:port`, including an explicit port, without
credentials, query, or path. SOCKS and authenticated upstream proxies are not
supported. Container-local loopback upstreams are rejected. The special
`host.docker.internal` hostname receives a `host-gateway` mapping in the shared
egress container only; operators must provide a guarded host listener.
Private/loopback/metadata destination denies apply before parent selection.
A selected hostname that cannot resolve locally is also denied (403), so a
parent cannot independently resolve it and bypass those checks. Upstream
connectivity failure returns a proxy error (normally 503); an upstream refusal
remains a failure. There is no fallback to direct or another group's parent.
Other groups and direct destinations remain usable while one parent is unavailable. Proxy health
only proves that Squid is listening, not upstream or provider readiness.

For operator-provisioned host HTTP listeners, opt in separately at the Docker
boundary (available since platform 0.10.0; older packages reject this keyword):

```python
from soveren_agent_platform.sandbox import DockerEgressHostRelay

# Alongside a matching SandboxEgressUpstream in configure_sandboxed_codex:
# egress_host_relays=(DockerEgressHostRelay(
#     proxy_url="http://host.docker.internal:10810",
# ),)
```

Each relay must uniquely reference a configured parent URL. Only
`host.docker.internal` or a private IPv4 address assigned to the Docker host is
supported. The manager does not create listeners. It reconciles its own
endpoint-specific INPUT guard using the current Squid public IP **and MAC** on
every managed acquisition, including first use after creation, replacement,
restart, or a control-plane restart. The old generation's allow is revoked before
rotation. Chains remain default-deny when a relay is removed. The guard is first
in INPUT for its destination IP/port and accepts only Squid, without changing the
host's default policy, unrelated rules, or allowing the Docker subnet.
The allow and INPUT hook change in one firewall transaction; ordinary acquire
does not temporarily close a working generation. Caller cancellation is deferred
only during the mutation until its named Docker helper finishes. Apply and stale
helper cleanup use separate 15-second budgets (plus Docker CLI process-reaping
grace). Timeout or a failed/lost CLI response triggers owned-container cleanup;
an uncertain cleanup aborts setup. Retry first fences the same helper name, also
after control-plane restart. A foreign container with that name is never removed.
Do not install another process that rewrites or moves these `SVR-RELAY-*` chains.
One trusted control plane owns each host's shared egress/relay policy.

A bounded TCP probe from Squid checks only the assigned host parent and logs
`docker_egress_host_relay_probe` with `proxy_url`, `egress_container_id`, and
`available`. It does not claim provider, TLS, auth, or end-to-end readiness.
An unavailable relay does not fail acquisition: requests to its selected hosts
remain fail-closed through Squid, while direct and other groups keep working.
Invalid Docker identity, an endpoint not assigned to the host, or inability to
install the isolation guard is a setup error and aborts acquisition. Existing
operator boot-time default-deny guards remain required before the manager runs.
Docker Desktop's special host mapping can point outside the Linux Docker host;
such mappings fail the local-address check. Use a guarded listener on an actual
Linux host bridge address for this capability.

Each group requires both `proxy_url` and `destination_hosts`. The image and the
manager use the same validation and canonical group representation. For
standalone Compose the equivalent non-secret environment is
`SOVEREN_EGRESS_UPSTREAM_ROUTES`, a JSON array of objects with those exact keys:

```json
[
  {"proxy_url":"http://host.docker.internal:10810","destination_hosts":["api.provider.example"]},
  {"proxy_url":"http://second-proxy.example:3128","destination_hosts":["api.other-provider.example"]}
]
```

Unset/empty or `[]` means direct; invalid/incomplete groups stop startup. The
earlier unreleased single-upstream environment fields are rejected explicitly.
Do not configure only Compose and
leave the application policy unset: the manager compares actual Docker env to
its bootstrap policy and replaces a mismatch only when no sandboxes run.
Changing, adding, or removing the policy uses the existing safe egress rotation:
stop active conversation sandboxes first; preserve their workspaces and the
network drop rules. Old images without the routing capability fail acquisition
instead of ignoring the settings. This feature is available from 0.8.0 and needs
a matching platform package and egress image. Version 0.7.1 does not implement it.

See [the Pulsy pilot contract and host steps](SANDBOX_EGRESS_PILOT.md).

### Migrating From 0.5

Remove `CodexAppServerLlmBackend` and any app-owned wrapper that creates,
registers, caches, or shuts down Codex conversation backends. Configure one
runtime through `AgentPlatformApp.configure_sandboxed_codex(...)`, pass it to
`PlannerRuntime(llm_backend=...)`, and remove `llm_backend` from `run_turn(...)`
and `run_dispatch_turn(...)` calls. The application still owns the model choice
and tenant secret source; the platform owns conversation routing, backend
registration, sandbox creation, and shutdown.

Custom `SessionMailboxStore` adapters must update `ready_sessions(...)` and
`fail_stale_sending(...)` to accept the optional `backend_prefix` keyword. The
mailbox worker now passes it on every call so ownership filtering is enforced at
the same transactional boundary as ready selection and stale-delivery cleanup.

`SandboxedCodexRuntime.run(...)` requires the trusted conversation scope placed
on `LlmRequest` by `PlannerRuntime`. It rejects a missing scope and a model that
does not match bootstrap configuration before creating a container. Its
internal backend name is always `codex:<conversation-hash>`, so raw
organization/chat ids do not appear in session backend metadata. Repeated turns
for one conversation reuse the same registered backend.

Codex collaboration presets use a typed provider contract rather than raw
strings. Only the app-server modes `default` and `plan` are accepted, and the
preset carries the model and optional settings that Codex applies to the turn:

```python
from soveren_agent_platform.sessions import CodexCollaborationMode

collaboration_mode = CodexCollaborationMode(
    mode="default",
    model="your-codex-model",
)

# Pass collaboration_mode=collaboration_mode to the single
# configure_sandboxed_codex(...) call during bootstrap.
```

The collaboration preset is optional. Arbitrary mode strings are rejected
before backend I/O instead of being forwarded to the experimental Codex API.

The same runtime owns durable interactive sessions and their mailbox. No
manager, backend name, or registry is exposed to the application:

```python
from soveren_agent_platform.llm import (
    CodexSessionOpenRequest,
    CodexSessionPrompt,
)

platform.use_codex_session_mailbox(tenant_id="organization-123")

opened = await codex_runtime.open_session(CodexSessionOpenRequest(
    tenant_id="organization-123",
    source_id="telegram-chat-123",
    owner_id="789",
    title="Primary Telegram session",
))

queued = await codex_runtime.enqueue_prompt(CodexSessionPrompt(
    session_id=opened.session_id,
    tenant_id="organization-123",
    source_id="telegram-chat-123",
    prompt="Continue with the repository analysis",
    idempotency_key="telegram-message-456",
))
```

`open_session(...)` closes the backend thread if persistence fails. Existing
sessions receive prompts through the durable mailbox by their platform
`session_id`. Its `CodexSessionOpenResult` exposes only that platform identity;
Codex thread ids, transport handles, backend names, and the backend registry
remain internal. `close_session(...)` and `close_idle_sessions(...)` apply the
existing platform lifecycle rules to the same private backend registry. Before
mailbox workers start after a process restart, active persisted Codex sessions
rehydrate their deterministic backend registrations without starting a container.
The runtime reserves the internal `codex:` backend namespace and limits its
mailbox worker and automatic cleanup to that namespace. Custom sessions for the
same tenant remain owned by their configured backend and worker. Compose that
worker with an explicit, non-overlapping `backend_prefix`; `AgentPlatformApp`
rejects overlapping prefixes in either registration order. An unscoped generic
worker owns every backend for its tenant and therefore cannot be combined with
any other mailbox worker. Supplying a custom session id to the Codex facade is
rejected before enqueue or close.
Sandbox backends are conversation-bound: `SessionRuntime`, mailbox delivery,
lifecycle cleanup, and inspectors reject a backend composed for a different
`tenant_id` or `source_id` before backend I/O.
The runtime's `output_schema` constrains planner responses only; mailbox prompts
on those durable sessions are sent without that schema.
`PlannerRuntime` automatically puts the raw organization/conversation pair in
the trusted `LlmRequest.conversation_scope`, and the runtime forwards it through
`OpenSpec`. Direct callers of a session-backed `LlmRequest` must pass
`ConversationScope(tenant_id=..., source_id=...)`; a bound backend rejects both
a missing scope and a mismatch before opening a thread or sandbox. This value
is execution control data, not model context.

For API billing, resolve `CodexApiKeyCredentials` from the trusted
`tenant_id` through `credentials_for_tenant`. The trusted control plane streams
an atomic tenant-scoped registry update over stdin
to a broker-only Unix socket. The shared broker validates the update and replaces or
removes only that tenant's registry. Credentials remain only in trusted manager and
broker process memory.
Secret bytes are never written to the conversation `CODEX_HOME`, sandbox environment,
Docker arguments, labels, image, or broker filesystem. Codex receives only a
non-secret custom-provider URL and can call the broker's fixed
`POST /v1/responses` and `POST /v1/responses/compact` routes. The broker replaces
all client auth/project headers and injects the real key on its fixed
`https://api.openai.com` upstream through the managed Squid boundary. The broker
has no direct public-network attachment. The Codex process bypasses Squid only
for the broker's conversation-network hostname and address; every public HTTP(S)
request still uses the managed proxy.

The broker derives tenant identity from the local destination address of its
conversation-network interface before looking up any binding. It never accepts a
tenant id from request headers, paths, query parameters, or bodies, and rejects a
registry update if one interface address would belong to two tenants.

`CredentialBrokerPolicy` optionally limits tenant-wide concurrency, requests per
minute, request size, request-body read time, complete upstream response time, queue
wait, and allowed model names. Use one OpenAI
project-scoped key and one consistent policy for every conversation backend in an
organization. Replacing that key or policy atomically updates the binding without
changing the provider URL. After reading the bounded request body, the broker re-resolves
the current binding under the same registry lock used by rotation and marks the request
as admitted for forwarding. A request admitted before replacement may finish with the
binding selected at admission; a request still waiting or reading its body is revalidated
against the replacement. Active concurrency and rate-window state survive registry updates.
When an organization's last active sandbox stops, its broker registry and network
attachments are removed while its bindings remain only in the current manager process.
Resuming that sandbox restores the same capability URLs. The shared broker container is
removed only when no active tenant registry remains. An uncertain update decommissions
the shared broker for every tenant; the next broker prepare or provision restores all
still-active in-memory registries. The public provision/revoke API does not expose this placement.

For another static header credential, define a fixed HTTPS binding and provision
it through the runtime's trusted conversation scope:

```python
import os

from soveren_agent_platform.sandbox import HttpCredentialBinding


github = await codex_runtime.provision_http_credential(
    tenant_id="organization-123",
    source_id="telegram-chat-123",
    credential=os.environ["GITHUB_TOKEN"].encode("ascii"),
    binding=HttpCredentialBinding(
        name="github",
        target_origin="https://api.github.com",
        credential_header="Authorization",
        credential_prefix="Bearer ",
        allowed_methods=("GET", "POST"),
        allowed_path_prefixes=("/repos", "/user"),
    ),
)

# Give only this URL, never the real token, to the authorized sandbox tool.
github_api_url = github.base_url

# Re-provisioning the same name and scope rotates the secret in place.
# Explicit revocation removes the binding from the broker registry.
await codex_runtime.revoke_http_credential(
    tenant_id="organization-123",
    source_id="telegram-chat-123",
    name="github",
)
```

`HttpCredentialBinding` is conversation-private by default. Set `scope="tenant"`
only for an organization credential that every conversation in that tenant may use.
The caller must explicitly provide a non-empty method set and normalized path-prefix
allowlist; there is no authorize-all path default.
Authorization requires both the opaque URL capability and the broker interface of an
allowed conversation network. A tenant-scoped binding is extended automatically when
that manager process creates another conversation network for the tenant. The broker
appends the requested path to the fixed HTTPS port-443 origin, enforces method and
path-prefix policy, forwards only allowlisted request headers, injects the configured
credential header, and never follows redirects.
It does not support arbitrary proxy targets, OAuth refresh, cookies, or query/body
credential injection.

Treat `CredentialBrokerCapability.base_url` as a bearer capability: it hides the real
credential but authorizes its bounded use. Do not log it or send it to unrelated
conversations. The platform intentionally does not persist credential bytes or collect
them from chat. The consuming application owns secure input, authorization, encryption
at rest, rotation policy, and retrieval from its secret store; pass bytes to the broker
only for the active binding lifecycle. Rotation and revocation affect subsequent
admissions; a request admitted for forwarding before the registry update may still finish.
Requests that have only entered the broker or started uploading are revalidated first.
A control-plane process restart discards the manager's memory registries, removes the
previous shared broker on first activation, and requires applications to provision
current credentials again.

Per-binding limits are enforced together with tenant-wide and broker-wide admission.
By default each tenant can use at most 8 in-flight requests and 32 MiB of buffered request
bodies, while the broker allows at most 16 in-flight requests and one global body budget
of at most 64 MiB and at most half of its cgroup memory limit. Known content lengths
reserve their actual size; chunked bodies reserve the binding maximum. This prevents one
tenant's individually valid bindings from consuming the entire shared broker. A registry
whose per-binding request maximum cannot fit both effective budgets is rejected fail-closed.

Code inside a conversation sandbox cannot read the real API key, but it can consume
the organization's permitted inference capacity through the broker. Use upstream
OpenAI project budgets in addition to broker limits.

For SIWC ChatGPT-plan inference, use the separate public
`CodexChatGPTPlanCredentials` provider:

```python
from soveren_agent_platform.sessions import (
    ChatGPTPlanAccessToken,
    CodexChatGPTPlanCredentials,
    CodexClientInfo,
    TenantBoundaryError,
)


async def fresh_plan_token() -> ChatGPTPlanAccessToken:
    # App-owned: verified registration, serialized refresh and atomic persistence.
    return await application_auth.current_plan_access_token(minimum_validity_s=300)


plan_credentials = CodexChatGPTPlanCredentials(
    tenant_id="my-tenant",
    get_access_token=fresh_plan_token,
    client_info=CodexClientInfo(name="My App", version="1.0.0", title="My App"),
    minimum_validity_s=300,
)


async def credentials_for_tenant(tenant_id: str):
    if tenant_id != "my-tenant":
        raise TenantBoundaryError("unrecognized application tenant")
    return plan_credentials


# Pass this resolver to configure_sandboxed_codex during normal bootstrap.
```

`ChatGPTPlanAccessToken(access_token, expires_at)` requires an aware expiration
datetime. The callback owns identity/plan-scope verification, `earliest_refresh_at`,
single-owner refresh, cancellation/crash recovery and atomic storage of the
complete rotated bundle. Return only its current access token and expiration;
never pass a refresh token to this provider. Keep one provider per tenant in the
process, backed by the same durable OAuth owner for every conversation.
Its local lock serializes binding updates, not remote OAuth rotation across
independently constructed providers or processes.

The provider validates a handle's tenant before token lookup. It rejects an
existing native `CODEX_HOME/auth.json`, including a symlink, without reading or
removing it. Switching a persistent sandbox therefore requires fencing its old
native writers and explicitly retiring that auth cache while preserving the
workspace/history; startup does not silently mix or migrate credentials.

Before each new turn the runtime obtains the latest grant and updates the
existing protected conversation binding for POST
`https://api.openai.com/v1/responses` (including its Responses subpaths). The
broker continues using the configured Squid egress. The bearer never enters the
sandbox's files, environment or Docker arguments. Stable broker capabilities
let a new access token take effect without restarting app-server; requests
admitted before rotation may finish with their original token. Every preparation
failure revokes this conversation binding, including transient verification
errors and cancellation. Revocation drains before the failure returns. Callback
errors retain their type: `CodexCredentialUnavailable()` is terminal and prevents
automatic replay; a transient application exception remains retryable without
becoming reauthorization. A short/expired grant produces terminal failure with
reason `access_token_expired_or_expiring`. A cleanup failure is grouped with the
original error rather than masking it.

After revocation, a later valid grant receives a new capability URL. The runtime
rebuilds app-server with this changed configuration and resumes the saved
thread. If other accepted turns still need capture, a new turn fails admission
until they finish; the runtime does not kill/replay those turns. Ordinary access
token replacement preserves the capability and does not restart app-server.

The SIWC provider disables unsupported hosted/lookup tool features and WebSockets.
It does not rewrite native request payloads or upgrade the packaged binary.
Flat native tools remain a live SIWC compatibility risk with pinned Codex
0.143.0; local contract tests do not establish inference/tool acceptance,
refresh concurrency, deployed proxy correctness or hosted-product eligibility.
Set the client name to the stable actual app name used as OAuth `agent_name_hint`.
OAuth bootstrap, secure transfer to a VM and user consent remain app-owned.

For a personal trusted deployment, `CodexAuthFileCredentials` copies a file-based
Codex login cache into the conversation
`CODEX_HOME`. Treat that source file as a secret. `ExistingCodexCredentials`
explicitly selects credentials already persisted in the conversation container.
Those two trusted-login providers remain readable by code inside their conversation
sandbox and are not substitutes for API-key brokering.

The packaged images are `ghcr.io/neureca/soveren-codex-sandbox:0.12.0`,
`ghcr.io/neureca/soveren-sandbox-egress:0.12.0`, and
`ghcr.io/neureca/soveren-credential-broker:0.12.0`. Codex runs as UID 10001. The
runtime drops Linux capabilities, enables
`no-new-privileges`, limits CPU, memory, PIDs, `/tmp`, and the writable container
layer, and permits only TCP traffic to Squid on port 3128 and the shared credential
broker's network-specific address on port 8080. Conversation-specific bridge
networks disable inter-container connectivity, while host `DOCKER-USER`/`INPUT`
rules add the explicit service exceptions and block direct peer and bridge
gateway access even when proxy variables are bypassed. The egress proxy allows
public HTTP/HTTPS while blocking private, loopback, link-local, and cloud
metadata destinations. Rootless Docker and hosts without the required iptables
chains fail closed. A Docker storage driver that cannot enforce
`--storage-opt size=...` fails container creation instead of silently running
without a disk quota. For `overlay2`, Docker requires an XFS backing filesystem
mounted with `pquota`; treat that as a host prerequisite for sandbox mode.

Package upgrades select the new packaged images for new conversations. An
existing conversation container keeps its previous image and writable
workspace until the app explicitly destroys that sandbox; its handle exposes
`image` as the actual image plus `configured_image` and
`image_update_state="deferred_until_destroy"`. Resource-profile, command,
environment, network, and hardening-policy drift still fails reuse. The
stateless shared egress proxy is replaced automatically once managed
conversation containers are stopped, including during normal first-acquire
recovery after a process restart. Explicit sandbox destruction is therefore an
operator decision that discards that conversation's container-local state, not
a package-upgrade prerequisite.

One backend hosts multiple Codex threads for the same conversation boundary.
`AgentPlatformApp.configure_sandboxed_codex(...)` creates the only supported
process-level manager and registers every conversation backend internally. A
second process runtime is rejected before Docker I/O. Its default capacity is
four active conversation sandboxes; further conversations wait until a slot is
released. This conservative initial limit permits multi-chat load measurement
and should be tuned from observed host CPU and memory use. Planner turns and
durable sessions share this manager and its private registry. The manager also
stops orphaned managed conversation containers
once on first use after a control-plane restart. Before sending every new turn,
the backend reacquires the same conversation
sandbox. This starts a stopped container and recreates and rehydrates an unavailable
shared credential broker before `turn/start`. Recovery never retries a turn that was
already accepted; a broker failure during an active turn is returned as that turn's
failure and the next turn performs preflight again. An open durable Codex thread
does not pin its conversation sandbox. Once no turn is pending, the backend stops
after five idle minutes by default while retaining the thread id; the next prompt
starts the same sandbox and resumes that thread. When all active slots are in use,
waiting conversations are admitted in order and idle backends stop against
unmet capacity demand instead of waiting for their idle timer. Pending and
in-flight turns never yield their slot.

The Docker manager emits structured `INFO` records for newly acquired, waiting,
cancelled, and released capacity. Records expose
`sandbox_capacity_event`, `sandbox_capacity_active`,
`sandbox_capacity_waiting`, `sandbox_capacity_limit`, and
`sandbox_capacity_wait_ms` without raw tenant or conversation ids. These fields
are the MVP signal for choosing a production capacity limit from observed demand
and wait time.

Codex stdio transport is bounded independently from model output. The default
maximum JSON-RPC frame is 8 MiB and the default accumulated agent text is 1 MiB
per turn. A frame or turn over its limit fails explicitly; JSON and answer text
are never silently truncated. An oversized live turn is interrupted once, while
recovery of an already accepted turn reads paginated `thread/turns/list` results
instead of materializing the entire thread. Advanced direct backend composition
can override `max_json_rpc_frame_bytes` and `max_turn_output_bytes`; the
business-facing sandbox factory intentionally uses the platform defaults.

`AgentPlatformApp.stop()` shuts down its configured `SandboxedCodexRuntime`, which
closes every created app-server and stops its sandbox without deleting the
persistent workspace or Codex state. Never share one sandbox between two
private `source_id` values, even when they belong to the same organization.

Planner model-boundary context is redacted by default. Raw channel identifiers
such as Telegram `chat_id`, `user_id`, update ids, source ids, and raw webhook
payloads stay available in platform storage/routing/authorization
paths, but prompt builders and `LlmRequest.metadata` receive a sanitized copy
with those fields replaced by explicit `[redacted:...]` markers. Apps can pass a
custom `ModelRedactionPolicy` through `PlannerRuntimeConfig` when they need a
different model-boundary policy.
Batch and history presentation intentionally expose normalized public
usernames and display names, never the raw channel user id.
The unredacted `LlmRequest.conversation_scope` is consumed only by trusted
backend boundary checks. Bundled LLM backends do not add it to HTTP payloads,
Codex prompts, `OpenSpec.metadata`, or dynamic tool arguments.
Tenant ids and approval actor ids are redacted by default as well because apps
may derive them from channel identities.
Memory dynamic tools apply the same default redaction recursively to app-owned
metadata and omit memory routing/audit identifiers such as `subject_id`,
`source_id`, `source_event_id`, and `created_by`. Apps can pass an explicit
`ModelRedactionPolicy` to `register_memory_tools(...)` for metadata fields, but
the routing/audit identifiers remain platform-internal.
The model-facing `remember` tool cannot set audit provenance fields; trusted app
code may still provide them through `MemoryStore.remember(...)`.
Session directory tools require and enforce their registered `source_id` boundary for list,
search, get, and refresh calls and omit raw source/backend session identifiers
from model-facing results.
Model-facing custom tools must be registered with handlers in a
`DynamicToolRegistry`; the high-level runtime does not accept bare tool schemas
that could be advertised but never executed. Provide a
`tool_registry_factory(ConversationScope)` when creating the runtime. It is
called once for each conversation, and the returned registry is bound to that
scope. Reusing one registry for another private conversation is rejected.
The bundled Codex transport admits at most eight concurrent dynamic tool calls per
conversation by default. Further calls receive an explicit capacity failure before
their handlers run. There is no implicit pending queue, timeout, or automatic retry;
completion releases capacity for a later call.

### Dynamic Tool Namespaces In 0.7.1

Codex app-server requires dynamic tool namespaces to match
`^[a-zA-Z0-9_-]+$`. The platform now registers `platform_conversation`,
`platform_schedules`, `platform_sessions`, and `platform_memory`. Their public
`*_TOOL_NAMESPACE` constants keep the same Python names; their string values
use underscores. Tool names, input schemas, handlers, and conversation scope
are unchanged. Update any app prompt or integration that hardcodes the old
dotted namespace strings to use the new values.

`DynamicToolSpec` rejects invalid namespaces at construction, and the backend's
raw-dictionary spec normalization applies the same check. Namespaces are never
rewritten automatically; registration and `item/tool/call` dispatch use the
same exact identifier. App-owned namespaces must also satisfy this contract
(for example, `pulsy_database` instead of `pulsy.database`).

App-server persists the tools registered at thread creation. `thread/resume`
in the packaged Codex 0.143.0 does not accept replacement `dynamicTools`.
If a thread was created by an older Codex with dotted tool namespaces, create
a new runtime session with the corrected registry; there are no legacy aliases.
A rejected `thread/start` has no usable thread to resume. Updating the package
does not requeue an existing dead-letter event.

To verify registration against an actual Codex binary without model inference
or credentials, run from the platform repo root:

```bash
SOVEREN_TEST_CODEX_BINARY=/absolute/path/to/codex \
  uv run pytest tests/test_dynamic_tool_namespaces.py
```

Use Codex 0.143.0, the version pinned in `deploy/sandbox/Dockerfile`.

## Conversation History

The standard batching and outbound paths automatically maintain a durable
message history. Inbound messages are recorded together with batching ingress;
outbound messages appear only after a confirmed send or explicit `sent`
reconciliation. The history is always scoped to one `(tenant_id, source_id)`.

```python
from soveren_agent_platform.conversation_history import SQLiteConversationHistoryStore

history = await SQLiteConversationHistoryStore.open(db_path)
recent = await history.recent(
    tenant_id="tenant-a",
    source_id="chat-1",
    limit=30,
)
matches = await history.search(
    tenant_id="tenant-a",
    source_id="chat-1",
    query="what did we decide about the tariff",
    context_before=4,
    context_after=4,
)
```

Register the read-only model tools on the same conversation-bound registry used
by the session backend:

```python
from soveren_agent_platform.conversation_history import register_conversation_history_tools
from soveren_agent_platform.sessions import DynamicToolRegistry

tools = DynamicToolRegistry()
register_conversation_history_tools(
    tools,
    history,
    tenant_id="tenant-a",
    source_id="chat-1",
)
```

This exposes `platform_conversation/read_recent_messages` and
`platform_conversation/search_message_history`. Each inbound author is returned
as `{ref, username?, display_name}`. `ref` is stable for the lifetime of the
conversation tool registry; `username` is the normalized public `@handle` when
available, and `display_name` falls back to the reference when unavailable.
Raw user ids and routing identifiers are omitted, and metadata uses the model
redaction policy. No
participant registration is required. The tools cannot read another chat and
cannot write or delete history. Tool responses have a fixed total byte budget
and return `truncated=true` instead of exceeding the model transport. Individual
oversized fields use explicit `text_truncated`, `metadata_truncated`, or
presentation truncation markers. A truncated recent read includes
`next_before_message_id`; pass it back as `before_message_id` to continue with
older messages. Search hits may include `context_truncated=true` when distant
neighbors do not fit. Use
`prune_history_before(...)` from trusted application code to bound the
searchable projection. This does not erase source records from
batching, outbound, runs, sessions, or other operational stores; complete
conversation erasure requires a separate application data-lifecycle policy.

For explicit search from an authorized private Telegram chat into registered
groups, opt in at the application composition point:

```python
from soveren_agent_platform.telegram import register_telegram_group_history_tools
from soveren_agent_platform.sessions import DynamicToolRegistry

tools = DynamicToolRegistry()
register_telegram_group_history_tools(
    tools,
    history=history,
    groups=telegram_runtime.chat_registry,
    bot=telegram_runtime.telegram_app.bot,
    tenant_id="tenant-a",
    private_chat_id=trusted_telegram_user_id,
    requester_user_id=trusted_telegram_user_id,
)
```

The application must derive the user id from authenticated Telegram ingress
and register these tools only for private users it permits. The platform
requires the private chat id to equal that user id, binds the tool registry to
the private conversation, and checks current group registration and Telegram
membership before listing and before each search. The bot must be a group
administrator so the membership check is reliable. Telegram failure or lost
access denies the read. The model receives a group title and opaque reference
from `list_searchable_groups`, then passes that reference to
`search_group_messages`; raw Telegram chat ids are not model arguments.
Group-to-private searches are never exposed. Search results use the same
redaction and output budget as current-conversation history tools.

Search accepts words handled by the configured SQLite FTS tokenizer, including
one-character terms. Empty or punctuation-only queries return no matches. Query
size and unique-token count are bounded. This is lexical prefix search;
morphology and semantic similarity are outside the FTS contract.

Custom channel integrations that bypass standard batching or outbound workers
must call `ConversationHistoryStore.record(...)` after their equivalent durable
ingress or confirmed delivery boundary.

## Dialogue Tasks

`soveren_agent_platform.dialogue` stores logical task assignments independently
of native model sessions and whole-chat message history. The application owns
the semantic router and builds a typed immutable `DialoguePlan`; the store owns
scoped persistence, replay, lifecycle revisions, task context and message
references.

```python
from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.dialogue import (
    DialogueInput,
    DialoguePlan,
    DialogueRoute,
    SQLiteDialogueStore,
)

scope = ConversationScope("tenant-a", "telegram:42")
store = await SQLiteDialogueStore.open(db_path)
plan = DialoguePlan(
    plan_id="router-event-123",
    scope=scope,
    inputs=(DialogueInput("input-123", "user-7", "Prepare the report", 1_760_000_000, "telegram"),),
    routes=(DialogueRoute("input-123", "create", task_key="new:input-123", title="Monthly report"),),
)
result = await store.apply_plan(plan)
accepted_plan = await store.get_plan(scope, plan.plan_id)  # exact immutable plan for retry
task_id = result.assignments[0].task_id
task = await store.get_task(scope, task_id)
context = await store.get_context(scope, task_id, through_input_id="input-123")
```

Each plan contains one route for each stable input id. Supported actions are
`create`, `attach`, `question`, `resolve_question`, `close` and `cancel`.
Within a plan, `create` uses the first input-derived local key
`new:<first_input_id>`; later messages can attach to that key. The result
contains one assignment per input with task id, question id and lifecycle
revision where applicable. Replaying the same scoped plan id and content
returns its stored result; changed content raises `DialogueConflictError`.
Load the accepted plan on retry before rebuilding dynamic candidate metadata.

`DialogueTask` preserves its original request and exposes an independent
`active`, `closed` or `cancelled` lifecycle. Transcript and task candidate reads
are bounded; `list_candidates` and `search_candidates` paginate up to 100 rows
per call. `get_context` accepts `limit`, `before_message_id`, and
`through_input_id`; the last option uses durable insertion order to exclude
later user inputs even when their timestamps match. Confirmed assistant messages
at read time are included. `get_focus(scope, actor_id)` returns only a hint; the
application must check that its task is still active before using it.

A `question` route persists `question_text`, the original input and the
store-verified active-task candidate snapshot with lifecycle revisions. Use
`get_question` or `list_pending_questions(scope, actor_id, limit=...)` to restore
the exact prompt after restart. `DialogueQuestion.delivery_confirmed` is derived
from a scoped confirmed outbound reference, so queued or uncertain delivery is
not evidence that the user received the question. `resolve_question` must
include its current question revision and either select one of its still-active
candidates by `task_id`, or create a task with
`task_key="new:<answer_input_id>"` and a title. New-task resolution uses the
first pending question input as `original_request`, moves every original input
and the answer into the new task, and lets later inputs in the same plan attach
to that key. Another participant may answer only by replying to the confirmed
routing-question message in the same conversation; a semantic answer without
that reference is limited to that actor's question and is accepted only after
delivery is confirmed. An existing selected task's `original_request` remains
unchanged. Invalid or stale transitions raise `DialogueConflictError`; they do
not reopen terminal tasks or overwrite a resolved question.

Older conversation history stays unassigned. No migration infers task ownership
from legacy messages or native LLM sessions.

## Memory

The platform includes an explicit memory port and bundled SQLite adapter. The
default migrations create storage for memory records, but nothing is written to
memory and nothing is injected into model context unless the application chooses
to do so.

```python
from soveren_agent_platform.memory import SQLiteMemoryStore

memory = await SQLiteMemoryStore.open(db_path)
memory_id, created = await memory.remember(
    tenant_id="tenant-a",
    source_id="chat-1",
    scope="user",
    subject_id="telegram:789",
    kind="preference",
    text="Prefers concise status updates.",
    idempotency_key="telegram:789:preference:concise-status",
)

records = await memory.search(
    tenant_id="tenant-a",
    source_id="chat-1",
    scope="user",
    subject_id="telegram:789",
    query="status updates",
)
```

For Codex app-server dynamic tools, register memory explicitly:

```python
from soveren_agent_platform.memory import MemoryToolAccess, register_memory_tools
from soveren_agent_platform.sessions import DynamicToolRegistry

tools = DynamicToolRegistry()
register_memory_tools(
    tools,
    memory,
    tenant_id="tenant-a",
    source_id="chat-1",
    access=MemoryToolAccess(scope="source", subject_id="telegram:123"),
    allow_write=False,
)
```

`allow_write=False` is the default and exposes only `search_memory` and
`get_memory`. Set `allow_write=True` only when the application policy allows
model-initiated memory writes/deletes. Prompt builders can also read
`MemoryStore` directly and inject selected records into their own prompts.
When `MemoryToolAccess` sets `scope` or `subject_id`, tool calls are confined to
that registered access boundary unless the app explicitly enables the matching
override flag.
Non-empty text queries are evaluated by SQLite FTS across every eligible record
in the conversation before `limit` is applied. Searches without usable tokens
return the newest matching records.

## Actions And Outbound

Use `ActionRegistry` to map action kinds to app-provided executors. Use
`OutboundRegistry` to map channel names to app-provided senders.

The platform stores action/outbound state and runs retryable workers. The app
performs external side effects inside executors/senders and must make those side
effects idempotent where the external API can be retried.

Action executors return an `ActionExecutionResult` rather than encoding
business outcome in exceptions:

```python
from soveren_agent_platform.actions import ActionExecutionResult


async def execute(action):
    if not is_valid(action.payload):
        return ActionExecutionResult.permanent_failure("invalid payload")
    if rate_limited():
        return ActionExecutionResult.retryable_failure("rate limited", retry_after_s=60)
    return ActionExecutionResult.executed({"ok": True})
```

Unexpected executor exceptions have an ambiguous external outcome and move the
action to `uncertain`. Return `retryable_failure(...)` for expected recoverable
results, or raise `ActionNotStartedError` only when the executor can prove that
no external attempt began. A permanent failure must be returned explicitly. An
invalid return type, status, or result payload is also treated as an ambiguous
outcome and is never retried automatically. An
unregistered action kind is treated as deterministic app configuration failure:
the action becomes `failed` and its execution event is completed without an
external call.

Channel senders use the equivalent `SendResult` contract:

```python
from soveren_agent_platform.outbound import SendResult


async def send(message):
    if provider_rejected_destination(message.destination_id):
        return SendResult.permanent_failure("destination rejected")
    if provider_rate_limited():
        return SendResult.retryable_failure("rate limited", retry_after_s=60)
    provider_message = await provider_send(message)
    return SendResult.sent({"provider_message_id": provider_message.id})
```

Unexpected sender exceptions have an unknown external acceptance outcome and
move the message to `uncertain`. The bundled Telegram sender maps `RetryAfter`
to a retryable result, maps `BadRequest`, `Forbidden`, and preflight text-limit
failure to a permanent result, and leaves transport/network failures uncertain.

Task-specific output is attached as typed metadata when enqueued:

```python
from soveren_agent_platform.outbound import OutboundTaskReference

await outbound.enqueue(
    tenant_id=scope.tenant_id,
    source_id=scope.source_id,
    channel="telegram",
    destination_id="42",
    text="The task result",
    idempotency_key="execution-123:reply",
    task_reference=OutboundTaskReference(task_id=task.task_id, task_revision=task.revision),
    reply_to_message_id=1234,
    message_thread_id=7,
)
```

Use `OutboundTaskReference(routing_question_id=...)` for a standalone routing
question; it intentionally has no selected task. The additive queue arguments
`task_reference`, `reply_to_message_id`, and `message_thread_id` are optional.
Task references are checked against the exact conversation and active
lifecycle revision during enqueue and again at `mark_sending`. On confirmed
delivery, task assistant transcript and typed external-message lookup are
recorded with the existing sent/history transaction. Telegram sends return the
provider message id for that lookup. Close/cancel suppresses queued, retrying
and leased task output, clears leases and records a reason; sending/sent output
is preserved. Existing non-task queue requests keep their prior behavior.

For arbitrary Telegram output, call `enqueue_telegram_text(...)`; it partitions
the text into separately durable rows of at most 4096 characters before any
Telegram API call. All rows for one response are inserted atomically through
`OutboundQueue.enqueue_many`, so an insertion failure cannot leave a sendable
prefix. Multipart rows share an ordering key: a later part cannot be
claimed until its immediate predecessor is `sent`. An `uncertain` predecessor
blocks the chain for explicit reconciliation; `dead_letter` or `cancelled`
cancels the unsent remainder. This preserves order across retries without
claiming exactly-once delivery. Long `parse_mode` markup must first be rendered
to plain text or split by app-owned formatting logic so a chunk boundary cannot
break markup.

Manual approval must use `SQLiteApprovalService.approve(...)` or
`approve_action_and_enqueue(...)`. These operations require `tenant_id` and
`source_id`, and atomically persist approval plus the idempotent `ExecuteAction`
event. The
low-level row transition alone does not schedule execution. If execution loses
its queue lease before recording an outcome, the action becomes `uncertain` and
is not automatically replayed.
For retryable outcomes, non-final attempts return the action to `queued` before
the event becomes `retrying`. The final attempt marks the action `failed` before
the event becomes `dead_letter`. An expired final lease is claimed only for
reconciliation: the executor is not called, and the action becomes `failed` if
no call started or `uncertain` if it was already executing.
If an executor returns `ActionExecutionResult.queued(...)`, the platform treats
that as a successful durable handoff and does not invoke the executor again.
The downstream completion may transition the conversation-scoped action from
`queued` to `executed` or `failed`.

Resolve uncertain effects only after checking the provider:

```python
from soveren_agent_platform.reconciliation import SQLiteEffectReconciler

reconciler = await SQLiteEffectReconciler.open(db_path)
result = await reconciler.resolve_action(
    action_id,
    tenant_id="tenant-a",
    source_id="chat-1",
    resolution="not_executed",
    request_key="provider-check-2026-07-11-1",
    actor_id="operator-42",
    evidence={"provider_lookup": "not_found"},
)
await reconciler.close()
```

Equivalent outbound resolutions are `sent`, `failed`, and `not_sent`. Only the
explicit negative resolution requeues work. The same request key and payload
is idempotent. Cron does not participate in effect reconciliation because its
only effect is the atomic publication of a durable `CronJobDue` event.

## Sessions

Execution sessions are backend-neutral. Register session backends with
`SessionBackendRegistry` and live context inspectors with
`SessionInspectorRegistry`.

Routing and planner tools should read generalized platform session state and
snapshots. Backend-specific APIs such as Codex app-server are adapters behind
the platform session ports, not app-level routing dependencies.

The tmux module is a low-level command-session utility, not a supported
`SessionBackend` or LLM backend. It exposes `capture_until(...)` only for callers
that define an explicit completion marker; terminal silence is never treated as
successful completion.

Session lifecycle cleanup:

```python
from soveren_agent_platform.sessions import (
    SessionLifecyclePolicy,
    SQLiteSessionLifecycle,
)

lifecycle = await SQLiteSessionLifecycle.open(
    db_path,
    session_backends=session_backends,
)
async with lifecycle:
    closed = await lifecycle.close_idle_sessions(
        tenant_id="tenant-a",
        policy=SessionLifecyclePolicy(
            max_active_sessions_per_source=3,
            idle_ttl_s=3600,
        ),
    )

    manual = await lifecycle.close_session(
        session_id="runtime-session-id",
        tenant_id="tenant-a",
        source_id="chat-1",
        reason="manual close",
    )

    forced = await lifecycle.close_session(
        session_id="runtime-session-id",
        tenant_id="tenant-a",
        source_id="chat-1",
        force=True,
        reason="forced close",
    )
```

`SQLiteSessionLifecycle.close_idle_sessions(...)` is intended for an app-owned maintenance job or
worker. It only closes `idle` sessions, calls the registered backend close hook,
marks successful closes as `closed`, and records control events. It skips
sessions with `queued` or `sending` mailbox items so cleanup cannot strand
pending work. `busy` sessions are left to the mailbox worker or an app-level
timeout policy.

`SQLiteSessionLifecycle.close_session(...)` requires the owning `tenant_id` and `source_id`, and
returns `session not found` for a cross-organization or cross-conversation id.
With `force=False` it refuses to close sessions with pending mailbox
items. `force=True` explicitly cancels `queued` mailbox items before closing the
backend session, but still refuses `sending` mailbox items and `busy` sessions.

Register model-facing session directory tools without exposing storage handles:

```python
from soveren_agent_platform.sessions import SQLiteSessionDirectoryTools

directory_tools = await SQLiteSessionDirectoryTools.open(db_path)
directory_tools.register(
    tools,
    tenant_id="tenant-a",
    source_id="chat-1",
    session_inspectors=session_inspectors,
)
```

Keep `directory_tools` open for as long as the registered handlers can run, and
close it during application shutdown. Each registration stays inside the
supplied private conversation boundary.

Mailbox delivery is intentionally at-most-once at the backend-send boundary.
After `send()` returns, the mailbox persists acceptance and retries only
`capture()`. A crash or exception before durable acceptance is marked failed
with an uncertain delivery outcome and is not resent automatically. This avoids
claiming exactly-once behavior while preventing blind duplicate Codex turns.
The worker validates `SendReceipt` and `CaptureResult` at runtime. A malformed
send receipt is terminal and uncertain; a malformed capture result consumes the
accepted-delivery retry budget and cannot leave the session permanently busy.
An accepted operation that is still running is polled without consuming capture
failure attempts until the configured absolute pending deadline. At that point,
an optional `DeliveryAbortBackend` receives the persisted receipt before the
mailbox/session is failed. Codex uses `turn/interrupt`, attempts thread archive,
and releases sandbox ownership; cleanup errors are recorded but cannot make
remote cancellation atomic. Failed and interrupted Codex turns are never
completed as successful mailbox deliveries.

## Validation

Before integrating a release, run platform checks in this repo:

```bash
uv sync --group dev
uv run ruff check src tests
uv run mypy
uv run pytest
```

Then run the consuming app's own checks with the exact package source it will
deploy.

## Optional Read-only GitHub

Install `soveren-agent-platform[github]` for the optional
`soveren_agent_platform.github` module. No GitHub libraries are imported by the
core package or required by consumers who do not use this module. This is an
opt-in reusable provider integration; application policy still belongs to the app.

Public exports: `GitHubAppCredentials`, `GitHubRepositoryAccess`, `GitHubAccess`,
`GitHubConnector`, `GitHubError`, `GitHubErrorCode`, `register_github_tools`.
See the complete [bootstrap handoff](GITHUB_CONSUMER_HANDOFF.md).

`GitHubAppCredentials(app_id, installation_id, private_key)` accepts trusted
in-memory PEM bytes. `GitHubAccess(conversation, repositories)` contains frozen
repository grants (exact `owner/name`, tuple of explicit `refs`, tuple of PR
numbers, frozenset of allowed operation names). Up to 100 repositories and 32
refs/PRs each are allowed. Never construct these from unvalidated model output.
The token POST requests only granted repository names and read permissions.
GitHub must also authorize those repositories for the selected installation.
The integration targets GitHub.com; Enterprise Server/custom origins are outside
this slice. API version is explicitly pinned to `2022-11-28`. PR metadata's
canonical repository spelling is compared without case sensitivity; it must
still identify the granted owner/repository. Tool arguments use the spelling
from the trusted grant.

`register_github_tools(registry, connector, scope=scope)` registers the granted
operations in `platform_github` on an existing `DynamicToolRegistry`:

| Tool | Arguments in addition to `repository` | Result contract |
| --- | --- | --- |
| `resolve_ref` | `ref` | Pin an explicitly allowed branch/tag/SHA once; return immutable `commit`. No implicit default. |
| `list_tree` | `commit`, optional `prefix`, `offset` | Recursive tree/path discovery, sorted paths, at most 200 entries per window. `provider_truncated` and `next_offset` distinguish GitHub truncation from local pagination. |
| `read_file` | `commit`, `path`, optional `start_line`, `end_line`, `max_bytes` | Regular UTF-8 file text; 1-based inclusive LF line numbers, immutable citation, total lines, next line and truncation. |
| `read_pull_request` | `number`, optional `offset` | First successful metadata snapshot (title/state/base/head), immutable merge-base-to-head compare, up to 30 files per window, paths/status/patches and base/head citations. |

`GitHubSnapshot(repository, commit, tree)` is a public immutable value for
trusted app persistence. After a successful `resolve_ref`, call the synchronous,
cache-only `connector.get_ref_snapshot(scope, repository=..., ref=...)` to
obtain the complete typed pin; it returns `None` when that authorized ref has
not been resolved in this connector. On restart, the host may call
`await connector.restore_snapshot(scope, repository=..., ref=..., snapshot=...)`
to restore that stored pin. Both methods require the connector's exact
conversation, the current repository grant, the original explicitly allowed
ref, and a current `list_tree` or `read_file` grant. Restore checks the stored
repository and immutable SHA forms before populating the connector cache; a
restored snapshot does not grant an arbitrary SHA or bypass per-operation
authorization. App-owned code must retain the original ref with the snapshot,
persist it only after the first successful resolve, and select the appropriate
connector/policy revision.

`commit` must have been resolved through this same connector (ref, PR base/head,
or compare merge-base). Reads
of a resolved ref remain at its original SHA even if the branch moves. Create a
new connector/registry after a trusted refresh of the app's selection or grant;
there is no model-facing grant/update tool. PR metadata is a snapshot, not a live
status feed. Fork heads are read only through the granted base repository; an
unavailable fork object returns an explicit provider failure and never expands
access to the fork repository.
Concurrent initial reads of the same repository/ref or repository/PR share one
snapshot attempt, including failures. Different selections are independent;
there is no global lock held during provider requests. A later explicit read
may retry a failed attempt. Cancelling one waiter preserves its peers; cancelling
the final waiter cancels and joins the abandoned initial read. Revoke cancels
pending initial reads, and close joins them before closing the HTTP client.

Files are limited to 256 KiB, 32 path components, 1024 path characters, 200 lines
and 24000 UTF-8 content bytes per window. Default `end_line` is `start_line+199`.
Continuation reuses commit/path with `next_line`; a single oversized line fails
with `line_exceeds_byte_budget`. Empty text is successful with zero lines and
null citation line range; out-of-range reads are empty with null line range.
Symlinks, submodules, directories, invalid UTF-8 and control-byte binary content
are rejected; no target is followed. Line chunks preserve their original text.
Tree and blob responses must identify the requested SHA. Decoded blob length
must equal both the tree entry size and blob size. Missing/mismatched identities,
inconsistent lengths and malformed base64 return `invalid_provider_response`,
without a successful citation or an empty-content fallback.

Tree and diff windows have a 24000-byte content budget; the serialized tool
response has a 192 KiB ceiling (including JSON escapes/citations). Every HTTP
response is bounded to 8 MiB; each request has a 20-second timeout, each tool
operation a 45-second deadline. Eight in-flight operations per connector are
admitted; excess calls fail immediately. No unbounded queue or background job.
GitHub-truncated trees cannot be completed by advancing `next_offset`; it only
advances over entries GitHub returned. File traversal uses nonrecursive trees
and can still read a known path when recursive discovery is incomplete.

PR compare returns at most 300 changed files; `file_list_complete=false` at
that cap or if the pinned metadata count differs. `next_offset` continues only
the returned file list. Missing/oversized patches set availability/truncation
flags. `patch_completeness=not_guaranteed_by_provider` always warns that GitHub
patches are excerpts without a provider completeness guarantee, even when
`truncated=false` reports no known omission. Hunk headers supply old/new line
ranges; file citations identify both immutable sources. Rename base citations use
`previous_path`; added-file base and removed-file head citations are null. PR bodies/comments,
reviews and write operations are not exposed.

Errors are unsuccessful tool results with `{"error": code}`; no provider body,
exception string, request URL or credentials are returned/logged. Codes include
`scope_denied`, `invalid_arguments`, `connector_closed`, `connector_busy`,
`authentication_failed`, `provider_forbidden`,
`provider_not_found_or_inaccessible` (404 does not prove nonexistence),
`provider_rate_limited` (429 or 403 with rate-limit headers), `provider_timeout`,
`provider_unavailable`, `invalid_provider_response`, `response_too_large`,
`path_not_found` (complete tree proved local absence),
`not_a_regular_utf8_text_file`, and `line_exceeds_byte_budget`.
Concurrent refresh waiters share one token attempt and its outcome, including
failure. Cancelling one waiter preserves the others; the final cancelled waiter
joins the abandoned refresh. Revoke cancels refresh, and close joins it before
closing the HTTP client. A later explicit read can retry a failed refresh; no
stale token or retained error cache is used.
No automatic retry is performed except one GET 401 authentication recovery.
Callers choose any later read retry; partial/error never means no matching code.

There is no indexed code-search tool in this slice. The
[official Search API](https://docs.github.com/en/rest/search/search#search-code)
considers only default-branch indexed files smaller than 384 KB. It cannot
establish absence on the selected branch/SHA. Use tree discovery and bounded
file reads; branch-aware content search remains explicit follow-up work.
