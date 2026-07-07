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
    )

    await app.run()


asyncio.run(main())
```

`create_telegram_agent_app(...)` builds the Telegram polling application,
registers the Telegram outbound sender, starts platform workers, and owns the
shutdown sequence. Apps that need custom polling lifecycle can use
`build_telegram_polling_application(...)` and `TelegramSender(...)` directly.

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
4. Use lower-level Telegram adapter functions only for custom polling lifecycle
   or webhook deployments.
5. Register app-owned action executors such as ClickUp through
   `ActionRegistry`.
6. Keep external side effects idempotent across retries.
7. Run platform checks here before release and app checks in the consuming repo
   with the exact package version it will deploy.
