# Consuming App Integration Guide

This guide describes how an application repo should connect the published
`soveren-agent-platform` package. Product-specific integrations such as
ClickUp, private prompts, auth rules, and business schemas stay in the
application repo.

## Install The Package

Use the published package in deployable app dependencies:

```toml
dependencies = [
  "soveren-agent-platform[telegram]>=0.2,<0.3",
]
```

Use the `telegram` extra only when the app uses the bundled Telegram adapter.
Apps that enqueue generic inbound messages or use their own Telegram adapter can
depend on `soveren-agent-platform>=0.2,<0.3` without extras.

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

```python
import asyncio
import os
from pathlib import Path

from soveren_agent_platform.actions import ActionRegistry
from soveren_agent_platform.agent import AgentEvent, AgentHandler
from soveren_agent_platform.telegram import create_telegram_agent_app


TENANT_ID = os.environ.get("SOVEREN_TENANT_ID", "default")
DB_PATH = Path(os.environ.get("SOVEREN_DB_PATH", "data/agent.db"))


class AppAgentHandler(AgentHandler):
    async def handle(self, event: AgentEvent) -> None:
        # Parse event.payload, run the app planner, and dispatch app decisions.
        ...


async def main() -> None:
    actions = ActionRegistry()

    app = create_telegram_agent_app(
        token=os.environ["TELEGRAM_BOT_TOKEN"],
        db_path=DB_PATH,
        tenant_id=TENANT_ID,
        handler=AppAgentHandler(),
        actions=actions,
        registration_user_ids=[987654321],
        quiet_window_s=2,
        max_window_s=10,
        max_count=20,
    )

    await app.run()


asyncio.run(main())
```

`create_telegram_agent_app(...)` builds the Telegram polling application,
registers the Telegram outbound sender, starts platform workers, and owns the
shutdown sequence. Apps that need custom polling lifecycle can use
`build_telegram_polling_application(...)` and `TelegramSender(...)` directly.

If no access policy is configured, the adapter accepts every message Telegram
delivers to the bot. `registration_user_ids` enables trusted chat registration:
a listed user can send `/start` or `/register` in a private chat or group, and
that `chat_id` is saved for future messages. Registration commands are consumed
by the adapter and are not sent to the agent as ordinary work.

`allowed_chat_ids` and `allowed_user_ids` are still available for static
allowlists. `quiet_window_s`, `max_window_s`, and `max_count` control inbound
batching for each Telegram chat independently.

## Telegram Webhook Adapter

For production web servers, the app may receive Telegram webhook requests
instead of polling. Keep the same boundary: the web app owns HTTP routing and
the bot token, then hands normalized Telegram updates to the platform queue.

When the web framework already builds Telegram update objects, enqueue them
directly:

```python
from soveren_agent_platform.storage import open_sqlite
from soveren_agent_platform.telegram import enqueue_telegram_update


conn = open_sqlite(DB_PATH)


async def handle_telegram_webhook(update) -> dict[str, bool]:
    enqueue_telegram_update(conn, update, tenant_id=TENANT_ID)
    return {"ok": True}
```

When the app receives raw JSON and does not use the bundled Telegram adapter,
convert it into `TelegramInboundMessage` and call `enqueue_telegram_message(...)`.
The app should keep webhook signature checks, allowed-user checks, and bot-token
handling outside the platform package.

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

## Optional Memory

Platform memory is explicit. The package ships a default SQLite-backed
`MemoryStore`, but it does not remember anything automatically and is not added
to prompts by default.

Use `SQLiteMemoryStore` from the consuming app when product policy says a fact
should be remembered:

```python
from soveren_agent_platform.memory import SQLiteMemoryStore

memory = SQLiteMemoryStore(conn)
await memory.remember(
    tenant_id="tenant-a",
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
    access=MemoryToolAccess(scope="source", subject_id="telegram:123"),
)
```

Write tools are disabled unless `allow_write=True` is passed. Keep writes
behind app policy, approval, or typed decisions when the memory contains user or
business data. `MemoryToolAccess` is an authorization boundary: model-provided
`scope` or `subject_id` values outside that boundary are rejected unless the app
explicitly enables override flags.

## Optional Sandboxed Codex

If the app exposes a tool-capable Codex session to Telegram or other external
users, run Codex behind a tenant sandbox. In the MVP, sandbox mode requires
Docker on the host. The high-level factory creates tenant networks, host
firewall rules, the shared egress boundary, and tenant containers; consuming
code does not manage Docker networks, images, or proxy configuration.

```python
import asyncio
from pathlib import Path

from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.sessions import (
    CodexAuthFileCredentials,
    SessionOpenRequest,
    SessionRuntime,
    SessionBackendRegistry,
    SQLiteSessionStore,
    create_sandbox_pool,
    create_sandboxed_codex_backend,
)
from soveren_agent_platform.storage.sqlite import open_sqlite


DB_PATH = Path("data/agent.db")
TENANT_ID = "telegram-chat-123"


async def main() -> None:
    conn = open_sqlite(DB_PATH)
    session_backends = SessionBackendRegistry()
    sandbox_pool = create_sandbox_pool(max_active_sandboxes=1)
    codex_backend = create_sandboxed_codex_backend(
        tenant_id=TENANT_ID,
        credentials=CodexAuthFileCredentials(Path("/run/secrets/codex-auth.json")),
        resources="small",
        session_backends=session_backends,
        sandbox_runtime=sandbox_pool,
    )
    platform = AgentPlatformApp(db_path=DB_PATH).use_session_mailbox(
        tenant_id=TENANT_ID,
        session_backends=session_backends,
    )

    await platform.start()
    try:
        sessions = SessionRuntime(SQLiteSessionStore(conn), session_backends)
        opened = await sessions.open_session(SessionOpenRequest(
            tenant_id=TENANT_ID,
            source_id="123",
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
            conn.close()


asyncio.run(main())
```

Use the returned platform `session_id` for mailbox decisions. The runtime closes
the backend thread if the platform session row cannot be persisted.

The trusted application service needs Docker CLI access. When that service runs
in compose, mount `/var/run/docker.sock` there and nowhere else. Do not expose
Docker commands as tools or mount the socket into tenant sandbox containers.
Product code chooses only the tenant boundary and `small`/`medium` profile;
image, network, command, labels, and hardening flags stay platform-owned.
Use one `create_sandbox_pool(...)` as the process composition root whenever the
application constructs more than one tenant backend. This keeps the configured
active-slot limit shared rather than duplicated per backend.

For the first product shape, `tenant_id` can be the Telegram chat tenant. That
means one active sandbox can contain one Codex app-server process and multiple
Codex threads for that chat. Do not reuse that sandbox for another customer,
workspace, or chat when data isolation matters.

Use `CodexApiKeyCredentials` for API-key billing or
`CodexAuthFileCredentials` for a trusted personal ChatGPT login cache. Both are
streamed through stdin and never added to Docker metadata. The packaged image,
egress proxy, idle stop, shared active-slot limit, and application shutdown hook
are supplied by the platform.

Expected executor outcomes:

- Return `ActionExecutionResult.executed(...)` after the external side effect is
  complete.
- Return `ActionExecutionResult.retryable_failure(...)` for rate limits,
  temporary network failures, and other recoverable external errors.
- Return `ActionExecutionResult.permanent_failure(...)` for invalid payloads,
  denied business rules, missing required app configuration, or non-retryable
  provider errors.
- Do not use exceptions for expected business outcomes. Unexpected exceptions
  are treated as retryable by the action worker.

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

1. Add `soveren-agent-platform[telegram]>=0.2,<0.3` to the app dependencies.
2. Add app env variables for DB path, tenant id, Telegram token, and provider
   secrets.
3. Start the default polling runtime with `create_telegram_agent_app(...)`.
4. Set `registration_user_ids` when trusted users should be able to register
   new private chats or groups with `/start` or `/register`.
5. Set `allowed_chat_ids` / `allowed_user_ids` only when the bot must be scoped
   to a static list.
6. Tune `quiet_window_s`, `max_window_s`, and `max_count` only when default
   batching is too eager or too slow.
7. Use lower-level Telegram adapter functions only for custom polling lifecycle
   or webhook deployments.
8. Register app-owned action executors such as ClickUp through
   `ActionRegistry`.
9. Keep external side effects idempotent across retries.
10. When sandbox mode is enabled, provide Docker access to the trusted control
    plane, create one process-local sandbox pool, choose a credential provider,
    and register tenant backends with `create_sandboxed_codex_backend(...)`.
    The platform owns tenant networks, host firewall policy, and shared egress
    setup. The Docker host must support the `DOCKER-USER` and `INPUT` iptables
    chains.
11. Run platform checks here before release and app checks in the consuming repo
   with the exact package version it will deploy.
