# Consuming App Integration Guide

This guide describes how an application repo should connect the published
`soveren-agent-platform` package. Product-specific integrations such as
ClickUp, private prompts, auth rules, and business schemas stay in the
application repo.

## Install The Package

Use the published package in deployable app dependencies:

```toml
dependencies = [
  "soveren-agent-platform[telegram]>=0.4,<0.5",
]
```

Use the `telegram` extra only when the app uses the bundled Telegram adapter.
Apps that enqueue generic inbound messages or use their own Telegram adapter can
depend on `soveren-agent-platform>=0.4,<0.5` without extras.

For active local platform development, keep the versioned dependency and add a
local `uv` source override in the app repo only:

```toml
[tool.uv.sources]
soveren-agent-platform = { path = "/Users/me/projects/agents/soveren-agent-platform", editable = true }
```

Do not deploy an app with an absolute local path dependency.

## App-Owned Configuration

The platform does not read product credentials from process environment by
itself. The consuming app reads secrets, constructs adapters/executors, and
registers them with the platform.

Typical app environment:

```text
SOVEREN_DB_PATH=data/agent.db
SOVEREN_TENANT_ID=default
TELEGRAM_BOT_TOKEN=123456:telegram-token
CLICKUP_API_TOKEN=pk_clickup_token
CLICKUP_TEAM_ID=123
CLICKUP_LIST_ID=456
OPENAI_API_KEY=sk-project-key
```

`TELEGRAM_BOT_TOKEN`, `CLICKUP_API_TOKEN`, and ClickUp workspace/list ids are
application-owned secrets. Keep them out of platform migrations, platform docs
examples committed with real values, and model prompts unless the app
explicitly decides to expose a redacted value.

## Telegram Polling Adapter

The bundled Telegram adapter normalizes Telegram updates into platform inbound
events and can send outbound Telegram messages through the outbound worker.
Polling is the simplest deployment mode for a first server or local agent.
For the default case, the consuming app only provides the bot token, database
path, tenant id, app handler, and app-owned registries.

All platform storage I/O in application code is asynchronous. Open only the
typed SQLite adapters the app needs, keep them for the process lifetime, and
close them during shutdown; do not pass raw SQLite connections through product
code.

```python
import asyncio
import os
from pathlib import Path

from soveren_agent_platform.actions import ActionRegistry
from soveren_agent_platform.agent import AgentEvent, AgentHandler
from soveren_agent_platform.outbound import OutboundQueue, SQLiteOutboundQueue
from soveren_agent_platform.telegram import create_telegram_agent_app, enqueue_telegram_text


TENANT_ID = os.environ.get("SOVEREN_TENANT_ID", "default")
DB_PATH = Path(os.environ.get("SOVEREN_DB_PATH", "data/agent.db"))


class AppAgentHandler(AgentHandler):
    def __init__(self, outbound: OutboundQueue) -> None:
        self.outbound = outbound

    async def handle(self, event: AgentEvent) -> None:
        text = str(event.payload.get("text") or "").strip()
        await enqueue_telegram_text(
            self.outbound,
            tenant_id=event.tenant_id,
            source_id=str(event.payload["source_id"]),
            destination_id=str(event.payload["source_id"]),
            text=f"Received: {text}",
            idempotency_key=f"telegram-reply:{event.id}",
            correlation_id=event.id,
        )

async def main() -> None:
    actions = ActionRegistry()
    outbound = await SQLiteOutboundQueue.open(DB_PATH)
    handler = AppAgentHandler(outbound)

    app = await create_telegram_agent_app(
        token=os.environ["TELEGRAM_BOT_TOKEN"],
        db_path=DB_PATH,
        tenant_id=TENANT_ID,
        handler=handler,
        actions=actions,
        registration_user_ids=[987654321],
        quiet_window_s=2,
        max_window_s=10,
        max_count=20,
    )

    try:
        await app.run()
    finally:
        await outbound.close()


asyncio.run(main())
```

`create_telegram_agent_app(...)` builds the Telegram polling application,
registers the Telegram outbound sender, starts platform workers, and owns the
shutdown sequence. Every bundled worker is fenced to the supplied tenant. Apps
that need custom polling lifecycle can use
`build_telegram_polling_application(...)` and `TelegramSender(...)` directly.

Use `enqueue_telegram_text(...)` for model or tool output whose length is not
known in advance. It preserves the text exactly, creates Telegram-safe chunks
of at most 4096 characters, and enqueues every chunk as a separate durable
outbound effect. Do not split inside a custom sender after one queue row has
already entered `sending`: a partial multi-message side effect would otherwise
have no honest durable state.
For long HTML/Markdown output, render it to plain text first or split it with
app-owned formatting logic; the generic helper rejects long `parse_mode` input
rather than cutting through markup entities.

The high-level runtime is deny-by-default. Configure `registration_user_ids`,
`allowed_chat_ids`, or `allowed_user_ids`; otherwise construction fails. A
listed registration user can send `/start` or `/register` in a private chat or
group, and that `chat_id` is saved for future messages. Registration commands
are consumed by the adapter and are not sent to the agent as ordinary work.
An app administrator can revoke that stored access with
`await app.revoke_registered_chat(chat_id)`. The operation affects only the app's
tenant and returns whether an allowed registration was changed. A user who
remains in `registration_user_ids` can deliberately register the chat again.
`allow_all_updates=True` is the explicit unrestricted mode and should only be
used when the application intentionally accepts every update delivered to the
bot. Message and callback hooks use the same access decision. Product-specific
callback authorization, such as whether an actor may approve an action, still
belongs to the app.

`allowed_chat_ids` and `allowed_user_ids` are still available for static
allowlists. `quiet_window_s`, `max_window_s`, and `max_count` control inbound
batching for each Telegram chat independently.

Telegram enables Privacy Mode for group bots by default. If the agent must see
ordinary group messages rather than only commands, mentions, and replies,
disable Privacy Mode for the bot in BotFather with `/setprivacy` and re-add the
bot to the group, or make it a group administrator. Telegram documents the
exact delivery rules in [Bot Features](https://core.telegram.org/bots/features#privacy-mode).
Group batch text identifies participants by Telegram public username, then
display name, then a per-batch label such as `participant_1`. Telegram user ids
remain structured routing data and are redacted at the default model boundary.

## Telegram Webhook Adapter

For production web servers, the app may receive Telegram webhook requests
instead of polling. Keep the same boundary: the web app owns HTTP routing and
the bot token, then hands normalized Telegram updates to the platform queue.

When the web framework already builds Telegram update objects, enqueue them
directly:

```python
from soveren_agent_platform.queue import SQLiteEventQueue
from soveren_agent_platform.telegram import TelegramAccessPolicy, enqueue_telegram_update


events = await SQLiteEventQueue.open(DB_PATH)  # create during application startup
telegram_access = TelegramAccessPolicy(allowed_user_ids=frozenset(TRUSTED_TELEGRAM_USER_IDS))


async def handle_telegram_webhook(update) -> dict[str, bool]:
    await enqueue_telegram_update(
        events,
        update,
        tenant_id=TENANT_ID,
        access_policy=telegram_access,
    )
    return {"ok": True}
```

Close `events` with `await events.close()` from the web application's shutdown
hook.

When the app receives raw JSON and does not use the bundled Telegram adapter,
convert it into `TelegramInboundMessage` and call `enqueue_telegram_message(...)`.
The app should keep webhook signature checks, allowed-user checks, and bot-token
handling outside the platform package. `enqueue_telegram_update(...)` enforces
an explicit platform access policy; the lower-level typed-message helper assumes
the app has already completed those checks.

## ClickUp And Other Product Tools

ClickUp is not a platform module. It is a product tool registered by the app as
an action executor.

```python
import os

from soveren_agent_platform.actions import ActionExecutionResult, ActionRegistry


class CreateClickUpTaskExecutor:
    def __init__(self, *, token: str, list_id: str) -> None:
        self.token = token
        self.list_id = list_id

    async def execute(self, action):
        payload = action.payload
        title = str(payload.get("title") or "").strip()
        if not title:
            return ActionExecutionResult.permanent_failure("missing title")

        # Call the app-owned ClickUp client here. Use action.idempotency_key or
        # action.id as the external idempotency/correlation key where possible.
        clickup_task_id = await create_clickup_task(
            token=self.token,
            list_id=self.list_id,
            title=title,
            description=str(payload.get("description") or ""),
        )
        return ActionExecutionResult.executed({"clickup_task_id": clickup_task_id})


actions = ActionRegistry()
actions.register(
    "clickup.create_task",
    CreateClickUpTaskExecutor(
        token=os.environ["CLICKUP_API_TOKEN"],
        list_id=os.environ["CLICKUP_LIST_ID"],
    ),
)
```

The platform persists the action, leases execution, retries retryable failures,
and marks terminal status. The app owns the ClickUp client, payload validation,
authorization, approval copy, and idempotency mapping to ClickUp.

For a manual action, approve and enqueue execution through the atomic approval
service rather than changing the row alone:

```python
from soveren_agent_platform.approvals import SQLiteApprovalService

approvals = await SQLiteApprovalService.open(DB_PATH)
result = await approvals.approve(
    tenant_id=TENANT_ID,
    source_id=SOURCE_ID,
    action_id=action_id,
    approver_id=str(telegram_user_id),
)
```

The app parses its own callback data and authorizes the actor. The platform
enforces the organization/conversation boundary, changes `pending` to `approved`, and writes the
single durable `ExecuteAction` event in the same transaction. Repeating the
approval returns the existing event.

## Optional Memory

Platform memory is explicit. The package ships a default SQLite-backed
`MemoryStore`, but it does not remember anything automatically and is not added
to prompts by default.

Use `SQLiteMemoryStore` from the consuming app when product policy says a fact
should be remembered:

```python
from soveren_agent_platform.memory import SQLiteMemoryStore

memory = await SQLiteMemoryStore.open(DB_PATH)
await memory.remember(
    tenant_id="tenant-a",
    source_id="chat-1",
    scope="user",
    subject_id="telegram:789",
    kind="preference",
    text="Prefers concise status updates.",
    idempotency_key="telegram:789:preference:concise-status",
)
```

To let a Codex thread read memory through dynamic tools, register it explicitly:

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
)
```

Write tools are disabled unless `allow_write=True` is passed. Keep writes
behind app policy, approval, or typed decisions when the memory contains user or
business data. `MemoryToolAccess` is an authorization boundary: model-provided
`scope` or `subject_id` values outside that boundary are rejected unless the app
explicitly enables override flags.

## Conversation History

Unlike semantic memory, message history is collected automatically when the
app uses platform batching and outbound workers. It contains inbound messages
and only confirmed outbound deliveries, partitioned by `(tenant_id,
source_id)`. This supports questions such as "what did we decide about the
tariff?" without granting access to another private or group chat.

Give a Codex conversation read-only access through its existing dynamic-tool
registry:

```python
from soveren_agent_platform.conversation_history import (
    SQLiteConversationHistoryStore,
    register_conversation_history_tools,
)
from soveren_agent_platform.sessions import DynamicToolRegistry

history = await SQLiteConversationHistoryStore.open(DB_PATH)
tools = DynamicToolRegistry()
register_conversation_history_tools(
    tools,
    history,
    tenant_id=TENANT_ID,
    source_id=SOURCE_ID,
)
```

The registered tools can read recent messages and search FTS results with
neighboring context. They cannot write history, override the registered chat,
or receive raw participant ids. Channel-provided public usernames and display
names are included by default, with the stable `participant_N` reference as
fallback; no participant registration is needed. Participant references remain
stable while that conversation's tool registry is alive. Empty searches return
no matches; search is SQLite FTS prefix matching rather than semantic retrieval.
Tool output is byte-bounded and reports `truncated=true` explicitly. Continue a
truncated recent read with its `next_before_message_id`; search results mark a
trimmed neighbor window with `context_truncated=true`.

Bound the searchable history from trusted app code with
`history.prune_history_before(...)`. This removes only the history projection,
not source records retained by batching, outbound, runs, or sessions.

History is not a replacement for `MemoryStore`: use history for recalling what
was said in a chat, and explicit memory for durable facts or preferences chosen
by application policy.

## Optional Sandboxed Codex

If the app exposes a tool-capable Codex session to Telegram or other external
users, run Codex behind a conversation sandbox. In the MVP, sandbox mode requires
Docker on the host. The high-level factory creates conversation networks, host
firewall rules, the shared egress boundary, and conversation containers; consuming
code does not manage Docker networks, images, or proxy configuration.

```python
import asyncio
import os
from pathlib import Path

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.sessions import (
    CodexApiKeyCredentials,
    SessionOpenRequest,
    SessionRuntime,
    SessionBackendRegistry,
    SQLiteSessionStore,
    create_sandbox_manager,
    create_sandboxed_codex_backend,
)


DB_PATH = Path("data/agent.db")
TENANT_ID = "organization-123"
SOURCE_ID = "telegram-chat-123"


async def main() -> None:
    session_store = await SQLiteSessionStore.open(DB_PATH)
    session_backends = SessionBackendRegistry()
    sandbox_manager = create_sandbox_manager(max_active_sandboxes=1)
    codex_backend = create_sandboxed_codex_backend(
        tenant_id=TENANT_ID,
        source_id=SOURCE_ID,
        credentials=CodexApiKeyCredentials(os.environ["OPENAI_API_KEY"]),
        resources="small",
        session_backends=session_backends,
        sandbox_manager=sandbox_manager,
    )
    platform = AgentPlatformApp(db_path=DB_PATH).use_session_mailbox(
        tenant_id=TENANT_ID,
        session_backends=session_backends,
    )

    await platform.start()
    try:
        sessions = SessionRuntime(session_store, session_backends)
        opened = await sessions.open_session(SessionOpenRequest(
            tenant_id=TENANT_ID,
            source_id=SOURCE_ID,
            kind="codex_cli",
            backend=codex_backend.name,
            cwd="/workspace",
        ))
        print(opened.session_id)
        await platform.wait()
    finally:
        try:
            await platform.stop()
        finally:
            await session_store.close()


asyncio.run(main())
```

Use the returned platform `session_id` for mailbox decisions. The runtime closes
the backend thread if the platform session row cannot be persisted.
Accepted mailbox work is never blindly resent. If an accepted Codex turn stays
pending past the mailbox deadline, the platform best-effort interrupts the exact
persisted turn, archives/releases the thread, and records the mailbox/session as
failed. A cleanup error is retained in that failure; this is not an exactly-once
or transactional cancellation guarantee.

The trusted application service needs Docker CLI access. When that service runs
in compose, mount `/var/run/docker.sock` there and nowhere else. Do not expose
Docker commands as tools or mount the socket into conversation sandbox containers.
Product code chooses only the organization/conversation boundary and `small`/`medium` profile;
image, network, command, labels, and hardening flags stay platform-owned.
Create exactly one `create_sandbox_manager(...)` at the process composition root and pass it
to every conversation backend. The backend factory requires this dependency so the configured
active-slot limit and restart recovery have one owner.

`tenant_id` identifies the organization. Each direct or group chat has its own
`source_id` and backend. One conversation sandbox can contain multiple Codex
threads for that chat, but it must never be reused for another private source.
Create one `DynamicToolRegistry` per conversation as well. The registry binds
to its first organization/source pair and rejects reuse for another source.

Use `CodexApiKeyCredentials` for API-key billing. It provisions one tenant registry
inside the Docker host's shared credential broker; the real key is streamed only to
that broker, held in broker memory, and never written into a conversation sandbox. Codex gets
only the broker URL as a custom model provider. The broker accepts only the two
Responses API routes Codex needs, replaces client auth headers, and uses a fixed
OpenAI upstream through the managed Squid proxy. It has no direct public-network
attachment. The packaged sandbox, egress proxy, credential broker, idle
stop, shared active-slot limit, and application shutdown hook are supplied by
the platform.
After a package update, new conversations use the new sandbox image. Existing
conversation containers retain their prior image and writable state until they
are explicitly destroyed; the stateless egress proxy rotates automatically
after running conversation containers are stopped. Integrators do not remove
or recreate platform-managed proxy infrastructure during a normal upgrade.

The default `CredentialBrokerPolicy` caps tenant concurrency, request rate,
queue wait, request size, request-body read time, and complete upstream response time.
An optional model allowlist can narrow model use.
All conversations for one organization must use the same API key and policy;
changing either atomically replaces the OpenAI binding. Code in a sandbox cannot recover the real
key, but it can spend the capacity made available through the broker, so use a
project-scoped OpenAI key and upstream project budget as the outer cost boundary.

The same tenant-isolated broker registry can protect app-supplied static API credentials without
putting them in Codex context or the conversation filesystem:

```python
from soveren_agent_platform.sandbox import HttpCredentialBinding


clickup = await codex_backend.provision_http_credential(
    os.environ["CLICKUP_API_TOKEN"].encode("ascii"),
    HttpCredentialBinding(
        name="clickup",
        target_origin="https://api.clickup.com",
        credential_header="Authorization",
        credential_prefix="",
        allowed_methods=("GET", "POST"),
        allowed_path_prefixes=("/api/v2",),
    ),
)
```

Give `clickup.base_url` only to the conversation tool that needs it. A request to
`{clickup.base_url}/api/v2/...` is accepted only from the authorized conversation
network, forwarded to the fixed ClickUp origin, and receives the injected header.
The default scope is `conversation`; use `scope="tenant"` only after the app has
authorized organization-wide use. The current manager automatically grants that
binding to new conversation networks in the organization. Methods and path prefixes
are mandatory authorization policy, not permissive defaults. Re-provision the same
`name` and scope to rotate the token without changing the capability URL, and call
`await codex_backend.revoke_http_credential("clickup")` to revoke it.

The capability URL is bounded authorization material even though it does not contain
the real token. Do not log it or expose it to another tenant. The app remains the
credential authority: collect credentials through an authenticated app flow, store
them encrypted in the app's secret store, decide conversation/tenant scope, and pass
bytes to the platform only while provisioning. Chat text is ordinary model context;
the platform does not guess that a pasted string is a secret or silently move it into
the broker. Idle stop removes the broker container but the same manager process restores
its memory-only bindings before resuming a sandbox. After a control-plane process
restart, provision current credentials again from the app's secret store and replace
the old capability URL in the authorized tool. Rotation or revocation wins against a
request that has not passed the broker's post-body registry revalidation. A request already
admitted for forwarding can still finish; the platform does not claim cancellation of an
external request after that boundary.
Prefer a typed app-owned action executor for business side effects that need approval,
durable status, or reconciliation. Use a protected HTTP binding when code inside the
sandbox genuinely needs bounded direct access to the provider API.

`CodexAuthFileCredentials` and `ExistingCodexCredentials` are explicit trusted
personal-login modes. Their auth cache lives inside the conversation sandbox
and is therefore readable by code in that sandbox; do not use them for an
organization secret exposed to untrusted participants.

Expected executor outcomes:

- Return `ActionExecutionResult.executed(...)` after the external side effect is
  complete.
- Return `ActionExecutionResult.retryable_failure(...)` for rate limits,
  temporary network failures, and other recoverable external errors.
- Return `ActionExecutionResult.permanent_failure(...)` for invalid payloads,
  denied business rules, missing required app configuration, or non-retryable
  provider errors.
- Do not use exceptions for expected business outcomes. Unexpected exceptions
  become `uncertain`; they are not replayed automatically.
- A missing executor registration is deterministic app configuration failure;
  the platform marks the action `failed` without attempting an external call.
- Raise `ActionNotStartedError` only when the executor can prove that no
  external request began. Otherwise return a typed result or let reconciliation
  determine the provider outcome.
- On the final retryable attempt, the platform marks the action `failed` and
  dead-letters the execution event. An expired final lease is reclaimed only to
  reconcile the action to `failed` or `uncertain`; it never starts another
  executor call.
- If a worker loses its lease while an external effect is in flight, recovery
  marks the action `uncertain` and does not invoke the executor again. An
  operator must check the provider and call `EffectReconciler` with an audited
  `executed`/`failed`/`not_executed` resolution. This is at-least-once
  infrastructure with fenced state transitions, not an exactly-once guarantee.

## Ownership Boundaries

Keep these in the app repo:

- Telegram bot token and webhook/polling deployment policy.
- ClickUp tokens, workspace/list ids, and provider client code.
- Planner prompts, model choice, and decision schemas.
- Product authorization, user mapping, and tenant rules.
- Product tables and product migrations.

Keep these in the platform package:

- Durable queue, leases, retries, and dead-letter behavior.
- Inbound batching and `ChatBatchReady` delivery.
- Action, outbound, cron, run, and session lifecycle mechanics.
- SQLite platform migrations and replaceable runtime ports.
- Generic Telegram normalization and optional Telegram adapter.

## Integration Checklist

1. Add `soveren-agent-platform[telegram]>=0.4,<0.5` to the app dependencies.
2. Add app env variables for DB path, tenant id, Telegram token, and provider
   secrets.
3. Start the default polling runtime with `create_telegram_agent_app(...)`.
4. Set `registration_user_ids` when trusted users should be able to register
   new private chats or groups with `/start` or `/register`.
5. Set `allowed_chat_ids` / `allowed_user_ids` when the bot uses a static
   allowlist; use `allow_all_updates=True` only for an intentionally unrestricted
   deployment.
6. Tune `quiet_window_s`, `max_window_s`, and `max_count` only when default
   batching is too eager or too slow.
7. Use lower-level Telegram adapter functions only for custom polling lifecycle
   or webhook deployments.
8. Register app-owned action executors such as ClickUp through
   `ActionRegistry`.
9. Keep external side effects idempotent across retries.
10. When sandbox mode is enabled, provide Docker access to the trusted control
    plane, create one process-owned sandbox manager, use brokered
    `CodexApiKeyCredentials`,
    and register conversation backends with `create_sandboxed_codex_backend(...)`.
    The platform owns conversation networks, host firewall policy, shared egress,
    and shared credential-broker setup. The Docker host must support the `DOCKER-USER` and `INPUT` iptables
    chains.
11. Run platform checks here before release and app checks in the consuming repo
   with the exact package version it will deploy.
