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
  "soveren-agent-platform>=0.2,<0.3",
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
bootstrap_platform_storage(db_path)

app = AgentPlatformApp(db_path=db_path, bootstrap_storage=False)
```

`bootstrap_platform_storage()` applies bundled platform migrations and then
validates the resulting schema. It does not run app-owned migrations.

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
        await asyncio.Event().wait()
    finally:
        await app.stop()


asyncio.run(main())
```

`AgentPlatformApp.start()` is fail-fast for platform schema errors. Worker claim
errors after startup are runtime errors and are logged/retried by the worker
loop.

## Inbound Messages

The batching worker consumes durable events with:

- `recipient="batching"`
- `message_type="InboundMessageReceived"`
- a stable `idempotency_key`
- payload fields: `channel`, `source_id`, `raw_event_id`, `text`,
  `message_at`

For generic sources, enqueue directly:

```python
from soveren_agent_platform.queue import durable
from soveren_agent_platform.storage import open_sqlite

conn = open_sqlite(db_path)
durable.enqueue(
    conn,
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

For Telegram, normalize to `TelegramInboundMessage` and use the helper:

```python
from soveren_agent_platform.storage import open_sqlite
from soveren_agent_platform.telegram import TelegramInboundMessage, enqueue_telegram_message

conn = open_sqlite(db_path)
enqueue_telegram_message(
    conn,
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
Lower-level helpers such as
`build_telegram_polling_application(...)`, `enqueue_telegram_update(...)`, and
`TelegramSender` are intended for webhook deployments or custom lifecycle
control.

## Standard Worker Modules

Compose only the modules the app needs:

```python
from soveren_agent_platform.actions import ActionRegistry
from soveren_agent_platform.app_api import AgentPlatformApp
from soveren_agent_platform.outbound import OutboundRegistry
from soveren_agent_platform.sessions import SessionBackendRegistry, SessionInspectorRegistry

app = (
    AgentPlatformApp(db_path=db_path)
    .use_batching()
    .use_agent(handler=agent_handler)
    .use_actions(registry=ActionRegistry())
    .use_outbound(registry=OutboundRegistry(), channels=["telegram"])
    .use_cron(handler=cron_handler)
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

## Optional Sandboxed Codex Runtime

By default, Codex app-server runs wherever the consuming app registers the
regular `CodexAppServerBackend`. Sandboxed execution is opt-in.

For a first `docker compose` deployment, use the bundled Docker runtime as a
sibling-container runner. The app/backend can still run inside Docker, but only
the trusted sandbox runner process should have access to the host Docker socket.
Tenant sandboxes and the main app backend should not mount
`/var/run/docker.sock`.

```python
from soveren_agent_platform.sandbox import DockerSandboxRuntime, SandboxSpec
from soveren_agent_platform.sessions import SandboxedCodexAppServerBackend

sandbox_runtime = DockerSandboxRuntime()
codex_backend = SandboxedCodexAppServerBackend(
    sandbox_runtime=sandbox_runtime,
    sandbox_spec=SandboxSpec(
        tenant_id="telegram-chat-123",
        image="soveren-codex-sandbox:latest",
        memory="512m",
        cpus="0.5",
        pids_limit=128,
        network="bridge",
    ),
)
```

Register `codex_backend` in the usual `SessionBackendRegistry`. One sandbox can
hold one Codex app-server process and multiple Codex threads for the same tenant
boundary. Do not share one sandbox across tenants that must not see each
other's files, session state, or credentials.

The Docker driver intentionally exposes only a small policy surface: image,
resource limits, network name, env, labels, workspace path, and startup command.
It does not expose arbitrary Docker options such as privileged mode, host
network, container namespace sharing, or host volume mounts.

OpenShell can be added later as another `SandboxRuntime`; it is not required for
the MVP Docker path.

Planner model-boundary context is redacted by default. Raw channel identifiers
such as Telegram `chat_id`, `user_id`, usernames, update ids, source ids, and
raw webhook payloads stay available in platform storage/routing/authorization
paths, but prompt builders and `LlmRequest.metadata` receive a sanitized copy
with those fields replaced by explicit `[redacted:...]` markers. Apps can pass a
custom `ModelRedactionPolicy` through `PlannerRuntimeConfig` when they need a
different model-boundary policy.

## Memory

The platform includes an explicit memory port and bundled SQLite adapter. The
default migrations create storage for memory records, but nothing is written to
memory and nothing is injected into model context unless the application chooses
to do so.

```python
from soveren_agent_platform.memory import SQLiteMemoryStore

memory = SQLiteMemoryStore(conn)
memory_id, created = await memory.remember(
    tenant_id="tenant-a",
    scope="user",
    subject_id="telegram:789",
    kind="preference",
    text="Prefers concise status updates.",
    idempotency_key="telegram:789:preference:concise-status",
)

records = await memory.search(
    tenant_id="tenant-a",
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

Unexpected executor exceptions are treated as retryable failures. A permanent
failure must be returned explicitly. When the queue exhausts its retry budget,
the action is marked `failed`; until then it stays retryable.

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
    close_idle_sessions,
    close_session,
)

closed = await close_idle_sessions(
    conn,
    tenant_id="tenant-a",
    session_backends=session_backends,
    policy=SessionLifecyclePolicy(
        max_active_sessions_per_source=3,
        idle_ttl_s=3600,
    ),
)

manual = await close_session(
    conn,
    session_id="runtime-session-id",
    session_backends=session_backends,
    reason="manual close",
)

forced = await close_session(
    conn,
    session_id="runtime-session-id",
    session_backends=session_backends,
    force=True,
    reason="forced close",
)
```

`close_idle_sessions(...)` is intended for an app-owned maintenance job or
worker. It only closes `idle` sessions, calls the registered backend close hook,
marks successful closes as `closed`, and records control events. It skips
sessions with `queued` or `sending` mailbox items so cleanup cannot strand
pending work. `busy` sessions are left to the mailbox worker or an app-level
timeout policy.

`close_session(..., force=False)` refuses to close sessions with pending mailbox
items. `force=True` explicitly cancels `queued` mailbox items before closing the
backend session, but still refuses `sending` mailbox items and `busy` sessions.

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
