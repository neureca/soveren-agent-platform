# Soveren Agent Platform Integration API

This document is the consumer-facing contract for wiring an application to the
platform runtime. `docs/ARCHITECTURE.md` explains why the pieces exist; this
file explains how to connect them.

For a practical app-level setup with package dependency, Telegram token wiring,
and app-owned tools such as ClickUp, see `docs/CONSUMING_APP.md`.

## Package Dependency

The import package is `soveren_agent_platform`; the distribution package is
`soveren-agent-platform`.

Production deployments must use a versioned dependency from a package index or
a tagged git source:

```toml
dependencies = [
  "soveren-agent-platform>=0.3,<0.4",
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

`AgentPlatformApp.start()` is fail-fast for platform schema errors. Keep
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
        ),
    )
```

The optional Telegram adapter lives under `soveren_agent_platform.telegram`; core
platform imports do not require Telegram adapter dependencies.

For the default Telegram polling app, use `create_telegram_agent_app(...)` from
`soveren_agent_platform.telegram`. It wires Telegram ingress, Telegram outbound,
batching, agent, actions, and worker lifecycle from a token, database path,
tenant id, and app-provided `AgentHandler`. It also accepts
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
batch text uses deterministic per-batch participant labels rather than Telegram
names or ids.
The high-level runtime also passes its fixed `tenant_id` to batching, agent,
actions, and Telegram outbound workers, so equal recipient/channel names in the
same database cannot cross organization boundaries.
Lower-level helpers such as
`build_telegram_polling_application(...)`, `enqueue_telegram_update(...)`, and
`TelegramSender` are intended for webhook deployments or custom lifecycle
control. The polling builder and `enqueue_telegram_update(...)` still require an
access policy or explicit `allow_all_updates=True`; constructing
`TelegramAccessPolicy()` without a chat or user allowlist is rejected.
Ingress helpers receive a `DurableQueue`, not a raw SQLite connection. Polling
setups that enable chat registration also provide a `TelegramChatRegistry`;
the bundled high-level runtime wires both adapters automatically.

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
    .use_cron(handler=cron_handler, tenant_id="tenant-a")
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

`tenant_id` is optional on low-level batching, agent, actions, outbound, and cron
workers for compatibility with intentionally global workers. When supplied, it
fences due-row selection and expired/exhausted cleanup. A sender or handler
bound to one tenant must always use the scoped form. A global cron worker must
use a tenant-aware handler such as `QueueCronHandler`, which routes each job with
the `tenant_id` carried by that job.

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
from soveren_agent_platform.context import SQLitePlannerContextBuilder
from soveren_agent_platform.runs import SQLiteRunStore
from soveren_agent_platform.runtime import PlannerRuntime
from soveren_agent_platform.sessions import DeterministicSessionRouter

run_store = await SQLiteRunStore.open(db_path)
context_builder = await SQLitePlannerContextBuilder.open(db_path)
session_router = await DeterministicSessionRouter.open(db_path)

planner = PlannerRuntime(
    run_store=run_store,
    context_builder=context_builder,
    session_router=session_router,
)

result = await planner.run_turn(
    event=event,
    prompt_builder=prompt_builder,
    llm_backend=llm_backend,
    decision_parser=decision_parser,
    config=planner_config,
)
```

The consuming app owns prompts, model configuration, decision parsing, and
business policy. Close the three opened adapters during application shutdown.
To dispatch decisions through platform effects, construct `PlannerRuntime` with
an explicit `DecisionEffects`; omitted effects cannot accidentally execute a
decision.

## Optional Sandboxed Codex Runtime

By default, Codex app-server runs wherever the consuming app registers the
regular `CodexAppServerBackend`. Sandboxed execution is opt-in.

The supported MVP path is Docker. The trusted application control plane needs
Docker CLI access. In a compose deployment, mount `/var/run/docker.sock` only
into that service. Conversation sandbox containers never receive the socket.
The package creates one internal network per conversation, a public proxy network, one
shared egress proxy, one credential broker per active organization, and fail-closed host firewall rules. It then creates the
conversation container and applies the `small` or `medium`
resource profile, registers the backend,
and owns shutdown/idle-stop behavior. No repository checkout or separate
infrastructure command is required by the application integrator.
The MVP assumes one trusted control-plane process per Docker host; overlapping
replicas must not manage the same sandbox labels and networks.

```python
import os

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.sessions import (
    CodexApiKeyCredentials,
    SessionBackendRegistry,
    create_sandbox_manager,
    create_sandboxed_codex_backend,
)

session_backends = SessionBackendRegistry()
sandbox_manager = create_sandbox_manager(max_active_sandboxes=1)
codex_backend = create_sandboxed_codex_backend(
    tenant_id="organization-123",
    source_id="telegram-chat-123",
    credentials=CodexApiKeyCredentials(os.environ["OPENAI_API_KEY"]),
    resources="small",
    session_backends=session_backends,
    sandbox_manager=sandbox_manager,
)

app = AgentPlatformApp(db_path=db_path).use_session_mailbox(
    tenant_id="organization-123",
    session_backends=session_backends,
)
```

The factory backend name is always `codex:<conversation-hash>`, so raw
organization/chat ids do not appear in session backend metadata and
multiple conversation backends can share one registry without colliding. The
registry argument is required and rejects a second backend for the same
conversation before either backend can acquire the sandbox.

Codex collaboration presets use a typed provider contract rather than raw
strings. Only the app-server modes `default` and `plan` are accepted, and the
preset carries the model and optional settings that Codex applies to the turn:

```python
from soveren_agent_platform.sessions import CodexCollaborationMode

codex_backend = create_sandboxed_codex_backend(
    tenant_id="organization-123",
    source_id="telegram-chat-123",
    credentials=CodexApiKeyCredentials(os.environ["OPENAI_API_KEY"]),
    resources="small",
    session_backends=session_backends,
    sandbox_manager=sandbox_manager,
    collaboration_mode=CodexCollaborationMode(
        mode="default",
        model="your-codex-model",
    ),
)
```

The collaboration preset is optional. Arbitrary mode strings are rejected
before backend I/O instead of being forwarded to the experimental Codex API.

Open and persist a durable runtime session through the typed composition API:

```python
from soveren_agent_platform.sessions import SessionOpenRequest, SessionRuntime, SQLiteSessionStore

session_store = await SQLiteSessionStore.open(db_path)
sessions = SessionRuntime(session_store, session_backends)
opened = await sessions.open_session(SessionOpenRequest(
    tenant_id="organization-123",
    source_id="telegram-chat-123",
    owner_id="789",
    kind="codex_cli",
    backend=codex_backend.name,
    cwd="/workspace",
    title="Primary Telegram session",
))
```

`SessionRuntime.open_session(...)` closes the backend thread if persistence
fails. Existing sessions receive prompts through the durable mailbox by their
platform `session_id`. For one-shot planner calls that do not need a durable
runtime session, wrap the backend in `SessionLlmBackend` instead.
Sandbox backends are conversation-bound: `SessionRuntime`, mailbox delivery,
lifecycle cleanup, and inspectors reject a backend composed for a different
`tenant_id` or `source_id` before backend I/O.
`PlannerRuntime` automatically puts the raw organization/conversation pair in
the trusted `LlmRequest.conversation_scope`, and `SessionLlmBackend` forwards it
through `OpenSpec`. Direct callers of a session-backed `LlmRequest` must pass
`ConversationScope(tenant_id=..., source_id=...)`; a bound backend rejects both
a missing scope and a mismatch before opening a thread or sandbox. This value
is execution control data, not model context.

For API billing, use `CodexApiKeyCredentials(os.environ["OPENAI_API_KEY"])`.
The trusted control plane streams a complete tenant credential registry over
stdin to a broker-only Unix socket. The broker validates and atomically replaces
that registry. Credentials remain only in trusted manager and broker process memory.
Secret bytes are never written to the conversation `CODEX_HOME`, sandbox environment,
Docker arguments, labels, image, or broker filesystem. Codex receives only a
non-secret custom-provider URL and can call the broker's fixed
`POST /v1/responses` and `POST /v1/responses/compact` routes. The broker replaces
all client auth/project headers and injects the real key on its fixed
`https://api.openai.com` upstream through the managed Squid boundary. The broker
has no direct public-network attachment. The Codex process bypasses Squid only
for the broker's conversation-network hostname and address; every public HTTP(S)
request still uses the managed proxy.

`CredentialBrokerPolicy` optionally limits tenant-wide concurrency, requests per
minute, request size, request-body read time, queue wait, and allowed model names. Use one OpenAI
project-scoped key and one consistent policy for every conversation backend in an
organization. Replacing that key or policy atomically updates the binding without
changing the provider URL. After reading the bounded request body, the broker re-resolves
the current binding under the same registry lock used by rotation and marks the request
as admitted for forwarding. A request admitted before replacement may finish with the
binding selected at admission; a request still waiting or reading its body is revalidated
against the replacement. Active concurrency and rate-window state survive registry updates.
The broker
container is removed when the organization's last active sandbox stops. Its registry
remains only in the current manager process so resuming an idle sandbox recreates the
broker without changing active capability URLs.

For another static header credential, define a fixed HTTPS binding and provision it
through the conversation backend:

```python
import os

from soveren_agent_platform.sandbox import HttpCredentialBinding


github = await codex_backend.provision_http_credential(
    os.environ["GITHUB_TOKEN"].encode("ascii"),
    HttpCredentialBinding(
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
await codex_backend.revoke_http_credential("github")
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
A control-plane process
restart discards the manager's memory registry, removes the previous broker on first
tenant activation, and requires the application to provision current credentials again.

Per-binding limits are enforced together with broker-wide admission. By default the
broker accepts at most 16 in-flight requests and reserves one shared request-body budget
of at most 64 MiB and at most half of its cgroup memory limit. Known content lengths reserve
their actual size; chunked bodies reserve the binding maximum. This prevents individually
valid bindings from exhausting the tenant broker when used concurrently. A registry whose
per-binding request maximum cannot fit the effective broker budget is rejected fail-closed.

Code inside a conversation sandbox cannot read the real API key, but it can consume
the organization's permitted inference capacity through the broker. Use upstream
OpenAI project budgets in addition to broker limits. For a personal trusted deployment,
`CodexAuthFileCredentials` copies a file-based Codex login cache into the conversation
`CODEX_HOME`. Treat that source file as a secret. `ExistingCodexCredentials`
explicitly selects credentials already persisted in the conversation container.
Those two trusted-login providers remain readable by code inside their conversation
sandbox and are not substitutes for API-key brokering.

The packaged images are `ghcr.io/neureca/soveren-codex-sandbox:0.3.0`,
`ghcr.io/neureca/soveren-sandbox-egress:0.3.0`, and
`ghcr.io/neureca/soveren-credential-broker:0.3.0`. Codex runs as UID 10001. The
runtime drops Linux capabilities, enables
`no-new-privileges`, limits CPU, memory, PIDs, `/tmp`, and the writable container
layer, and permits only TCP traffic to Squid on port 3128 and its tenant credential
broker on port 8080. Conversation-specific
networks plus host `DOCKER-USER`/`INPUT` rules block direct peer and bridge
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

One backend hosts multiple Codex threads for the same conversation boundary. Create one
`create_sandbox_manager(...)` at the process composition root and pass that same manager to
every conversation backend. The argument is required, so a backend cannot silently
create an independent capacity owner. Its default capacity is one active conversation
sandbox, so another conversation waits until the slot is released. The manager also
stops orphaned managed conversation containers once on first use after a control-plane
restart. When the last thread
closes, the backend stops after five idle minutes by default.
`AgentPlatformApp.stop()` closes app-server and stops the sandbox without
deleting its persistent workspace or Codex state. Never share one sandbox
between two private `source_id` values, even when they belong to the same
organization.

Planner model-boundary context is redacted by default. Raw channel identifiers
such as Telegram `chat_id`, `user_id`, usernames, update ids, source ids, and
raw webhook payloads stay available in platform storage/routing/authorization
paths, but prompt builders and `LlmRequest.metadata` receive a sanitized copy
with those fields replaced by explicit `[redacted:...]` markers. Apps can pass a
custom `ModelRedactionPolicy` through `PlannerRuntimeConfig` when they need a
different model-boundary policy.
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
`DynamicToolRegistry`; the high-level sandbox factory does not accept bare tool
schemas that could be advertised but never executed.
Each registry is bound to the first `(tenant_id, source_id)` supplied by memory,
session, or sandbox composition. Reusing it for another private conversation is
rejected; build one registry per conversation.

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
For arbitrary Telegram output, call `enqueue_telegram_text(...)`; it partitions
the text into separately durable rows of at most 4096 characters before any
Telegram API call. Long `parse_mode` markup must first be rendered to plain text
or split by app-owned formatting logic so a chunk boundary cannot break markup.

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

Equivalent outbound resolutions are `sent`, `failed`, and `not_sent`; cron
resolutions are `fired`, `failed`, and `not_fired`. Only the explicit negative
resolution requeues work. The same request key and payload is idempotent.

## Sessions

Execution sessions are backend-neutral. Register session backends with
`SessionBackendRegistry` and live context inspectors with
`SessionInspectorRegistry`.

Routing and planner tools should read generalized platform session state and
snapshots. Backend-specific APIs such as Codex app-server or tmux are adapters
behind the platform session ports, not app-level routing dependencies.

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
