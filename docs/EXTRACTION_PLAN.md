# Agent Platform Extraction Plan

## Цель

Вынести runtime-ядро агента в отдельную открытую репу
`/Users/me/projects/agents/agent-platform`, чтобы `poruchen` и
`pulsell-agent` зависели от одной платформы, а не копировали механику друг у
друга.

Целевая топология:

```text
/Users/me/projects/agents/agent-platform   # open runtime package
/Users/me/projects/agents/poruchen         # private app, donor runtime
/Users/me/projects/pulsell-agent           # open app, future consumer
```

## Неразрушающее правило

`poruchen` пока не трогаем. Первая работа идет в отдельной репе платформы.
Интеграционные PR в приложения должны начаться только после того, как
соответствующий slice платформы проходит собственные тесты.

## Архитектурный разрез

Основные подключаемые модули платформы:

- `agent_platform.agent` — агентский runtime: берет события из durable queue и
  передает их app-provided агентскому handler.
- `agent_platform.batching` — durable inbound batching: собирает короткие
  входящие сообщения в SQLite и выпускает `ChatBatchReady` в агентский runtime.
- `agent_platform.cron` — cron/runtime планировщик: хранит due jobs, lease-ит
  их, вызывает app-provided handler и поддерживает retry/dead-letter.
- `agent_platform.telegram` — один из интерфейсов связи: нормализует Telegram
  ingress и кладет события в платформенную очередь. Это не ядро платформы, а
  подключаемый channel/interface рядом с будущими web/email/other interfaces.
  Optional `python-telegram-bot` adapter exists, but core does not depend on a
  Telegram SDK.
- `agent_platform.sessions` — execution session contracts, routing metadata и
  durable mailbox перед busy/idle session backends, persistent snapshots and
  deterministic routing, backend registry, plus optional reusable backends and
  Codex app-server dynamic tool contracts.
- `agent_platform.context` — rich context builder for planner turns: trigger,
  inbound batch, session routing, mailbox, pending actions, outbound queue, and
  cron snapshot, plus an optional app-neutral prompt formatter.
- `agent_platform.actions` / `agent_platform.approvals` — generic side-effect
  lifecycle: pending, approved, queued, executing, executed, failed.
- `agent_platform.outbound` — channel-neutral outgoing messages. Telegram is
  just one sender adapter.
- `agent_platform.decisions` — strict LLM JSON parsing plus dispatcher from
  typed decisions into platform side effects.
- `agent_platform.app_api` — runtime composition layer that starts/stops the
  standard worker set as one application.

Платформа владеет механикой:

- SQLite connection setup и WAL/runtime pragmas
- runtime ports for queue and module-specific stores
- namespaced migrations and app migration providers
- durable queue с lease/retry/dead-letter/idempotency
- LLM transport contracts
- agent run tracking
- read-only rich context assembly for planner calls
- optional rich context prompt formatting helper
- generic worker loop
- decision registry/parser framework
- action lifecycle и approval runtime
- inbound batching engine
- scheduler core
- execution sessions, mailbox, routing contracts
- Telegram adapter contracts and PTB runtime foundation
- Codex app-server dynamic tool declaration and fail-closed tool-call adapter

Приложения владеют политикой и доменом:

- prompts
- concrete decisions
- concrete action executors
- ClickUp/Pulsell integrations
- user-facing copy
- allowlists and product policies
- app-specific migrations and tables

## Phase 0. Repo bootstrap

Статус: done for the initial skeleton.

Deliverables:

- `pyproject.toml`
- `src/agent_platform`
- `tests`
- platform README
- this extraction plan

Gate:

- `uv run pytest` passes inside `agent-platform`
- no imports from `poruchen` or `pulsell-agent`

## Phase 1. Storage and queue foundation

Статус: first slice extracted into platform.

Platform modules:

- `agent_platform.storage.sqlite`
- `agent_platform.storage.migrations`
- `agent_platform.queue.durable`

Important changes from `poruchen` donor code:

- no default `tenant_id="soverenai"`
- migrations are namespaced by `(namespace, version)`
- platform schema contains only `event_queue` and `agent_runs` in this slice
- no `users`, `positions`, `notes`, ClickUp, Telegram audit, or app seed data

Tests:

- platform migrations are idempotent
- queue enqueue is idempotent by key
- due events are leased atomically
- expired leases are reclaimed
- retry vs dead-letter follows `attempts >= max_attempts`
- done events clear lease fields

Next app integration:

1. Add editable dependency in a separate `poruchen` branch:
   `agent-platform @ file:///Users/me/projects/agents/agent-platform`.
2. Replace local imports for storage/queue only.
3. Split `poruchen` migration history so platform migrations run first and
   app migrations keep app-owned tables.
4. Run full `poruchen` tests.

Do not move `poruchen`'s `001_init.sql` wholesale. It contains app-owned user,
position, tenant, and seed data.

## Phase 2. LLM contracts and run tracking

Статус: first contracts extracted into platform.

Platform modules:

- `agent_platform.llm.contracts`
- `agent_platform.runs.store`

Current scope:

- backend-neutral `LlmRequest`
- backend-neutral `LlmResponse`
- `LlmBackend` protocol
- `insert_run`
- `finalize_run`

Still app-owned for now:

- concrete Claude tmux backend
- concrete OpenAI-compatible API backend
- concrete Codex app-server backend
- backend selection from app settings
- planner prompt construction

Next extraction:

1. Add an LLM backend registry/factory that does not depend on `poruchen`
   settings.
2. Move reusable concrete backends only after constructor config is made
   platform-neutral.
3. Add fake backend tests for planner orchestration before touching app code.

## Phase 2a. Agent runtime module

Статус: initial module exists.

Platform modules:

- `agent_platform.agent.contracts`
- `agent_platform.agent.worker`

Responsibility:

- claim queue events for a configured `recipient`
- decode payload
- call app-provided `AgentHandler`
- mark events done or retry/dead-letter via durable queue

This is the main "agent module" in the platform. Concrete agent behavior still
lives in app repos.

Gate:

- queued event reaches `AgentHandler`
- successful handler marks event `done`
- failed handler routes event through retry/dead-letter lifecycle

## Phase 2b. Cron module

Статус: initial module exists.

Platform modules:

- `agent_platform.cron.contracts`
- `agent_platform.cron.store`
- `agent_platform.cron.worker`
- `agent_platform.cron.queue_handler`

Responsibility:

- store cron jobs in `cron_jobs`
- claim due jobs with lease semantics
- call app-provided `CronHandler`
- retry failed jobs
- advance recurring jobs via RRULE
- optionally emit queue events using `QueueCronHandler`

Gate:

- due job is claimed once
- successful one-shot job becomes `fired`
- failed job retries or goes `dead_letter`
- recurring job advances to next `run_at`

## Phase 2c. Batching module

Статус: initial module exists.

Platform modules:

- `agent_platform.batching.contracts`
- `agent_platform.batching.rules`
- `agent_platform.batching.store`
- `agent_platform.batching.worker`

Responsibility:

- store open inbound batches by `(tenant_id, channel, source_id)`
- dedupe raw inbound events
- decide `wait` vs `flush`
- schedule durable flush wakeups
- emit `ChatBatchReady` to the agent module

Gate:

- Telegram/interface ingress goes to `recipient="batching"`
- batching worker emits `ChatBatchReady` to `recipient="agent"`
- all batch state is durable in SQLite

## Phase 2d. Interface modules

Статус: initial Telegram interface and optional PTB adapter exist.

Platform modules:

- `agent_platform.interfaces.channels`
- `agent_platform.telegram.contracts`
- `agent_platform.telegram.ingress`
- `agent_platform.telegram.ptb`

Responsibility:

- define channel/interface contracts separately from core agent runtime
- provide Telegram as one bundled interface
- convert normalized Telegram input into queue events for the agent module

Telegram is not the center of the platform. It is one supported source/channel.

## Phase 3. Planner, decisions, and actions

Статус: initial planner envelope, rich context builder, decision registry,
decision dispatcher, and generic action runtime exist.

Target platform modules:

- `agent_platform.runtime.planner`
- `agent_platform.context.builder`
- `agent_platform.context.formatting`
- `agent_platform.decisions.registry`
- `agent_platform.decisions.parser`
- `agent_platform.decisions.dispatcher`
- `agent_platform.actions.registry`
- `agent_platform.actions.store`
- `agent_platform.actions.worker`
- `agent_platform.approvals.runtime`

Required design:

- planner receives `AgentEvent`
- planner asks `SessionRouter` for snapshots and route hints
- platform builds a read-only rich context from trigger, batch, session routing,
  mailbox, pending actions, outbound messages, and cron jobs
- planner injects `session_routing` and `planner_context` into
  `LlmRequest.metadata`
- app prompt builder decides how visible rich context should appear in prompt
  text; it may use the platform formatter or provide its own
- platform parser validates JSON and dispatches by registered `kind`
- app repos register concrete Pydantic models
- dispatcher maps typed decisions to platform effects: `outbound`, `actions`,
  `session_mailbox`, or `cron`
- platform action lifecycle owns generic statuses
- app repos register executors and approval policy per action kind
- outbound user messages go through `agent_platform.outbound`, not direct
  Telegram-specific worker code

Gate before app integration:

- fake LLM returns a registered decision
- planner stores a run
- planner stores and passes rich platform context
- decision parser returns the concrete app model
- decision dispatcher receives the concrete app model
- write-kind decision creates an action instead of executing immediately

## Phase 4. Telegram runtime and batching

Target platform modules:

- `agent_platform.telegram.runtime`
- `agent_platform.telegram.normalized_update`
- `agent_platform.telegram.outbound`
- `agent_platform.telegram.callbacks`
- `agent_platform.batching.store`
- `agent_platform.batching.engine`
- `agent_platform.batching.worker`

Rules:

- First adapter is `python-telegram-bot`, matching the donor runtime.
- `aiogram` support is deferred.
- Platform owns normalized ingress and batching mechanics.
- Apps own media handling, commands, allowlists, and user-facing messages.

Gate:

- fake Telegram update is normalized and persisted
- text update enqueues durable inbound event
- batcher flushes a durable `ChatBatchReady`
- app hook can classify control messages without forking the platform worker

## Phase 5. Sessions and scheduler

Статус: initial session mailbox, event log, snapshots, deterministic router,
stub backend, tmux backend, Codex app-server session backend, and dynamic
Codex tool adapter exist.

Target platform modules:

- `agent_platform.sessions.backend`
- `agent_platform.sessions.mailbox`
- `agent_platform.sessions.mailbox_worker`
- `agent_platform.sessions.store`
- `agent_platform.sessions.events`
- `agent_platform.sessions.routing`
- `agent_platform.sessions.snapshots`
- `agent_platform.sessions.backends.stub`
- `agent_platform.sessions.backends.tmux`
- `agent_platform.sessions.backends.codex_app_server`
- `agent_platform.sessions.backends.codex_tools`
- `agent_platform.sessions.registry`
- `agent_platform.scheduler.store`
- `agent_platform.scheduler.worker`

Rules:

- platform owns session handles, mailbox lifecycle, backend protocol
- platform owns Codex dynamic tool wire protocol and fail-closed dispatch
- apps own routing scoring policy, prompt injection, concrete dynamic tools, and
  approval/idempotency policy
- scheduler is generic job dispatch, not hardcoded reminders

Gate:

- session mailbox queues prompts while session is busy
- mailbox drains after session returns to idle
- router chooses existing sessions from snapshots and logs route decisions
- Codex dynamic tools are declared at `thread/start` and answered through
  `item/tool/call`
- scheduler claims due rows once and enqueues a platform event
- recurring schedule advances deterministically

## Phase 6. App adoption order

Before app adoption, platform should expose a single composition surface:

- `AgentPlatformApp.use_batching()`
- `AgentPlatformApp.use_agent(...)`
- `AgentPlatformApp.use_actions(...)`
- `AgentPlatformApp.use_outbound(...)`
- `AgentPlatformApp.use_cron(...)`
- `AgentPlatformApp.use_session_mailbox(...)`

Статус: initial runtime supervisor and composition helper exist.

Use this order for each application:

1. Storage and queue
2. LLM contracts and run tracking
3. Planner/decision/action framework
4. Telegram runtime and batching
5. Sessions and scheduler

`poruchen` should migrate first because it is the donor and has the stronger
test surface. `pulsell-agent` should consume the platform after each slice is
stable enough, not reimplement the runtime inside its own repo.

## Packaging strategy

Active development:

```toml
dependencies = [
  "agent-platform @ file:///Users/me/projects/agents/agent-platform",
]
```

Released apps:

```toml
dependencies = [
  "agent-platform>=0.1,<0.2",
]
```

Versioning:

- stay on `0.x` while APIs are moving
- tag platform releases before app dependency bumps
- app repos pin version ranges, not floating branches

## Backlog

1. Move batching/session mailbox onto explicit ports.
2. Add module-specific store ports for actions, outbound, cron, sessions,
   mailbox, batching, and runs.
3. Add baseline/compatibility checks for adopting existing SQLite schemas.
4. Extract and neutralize concrete LLM backends.
5. Add fake planner end-to-end test that covers context, dispatch, actions, and outbound.
6. Extract PTB runtime builder and callback hooks.
7. Integrate Phase 1 into `poruchen` in a separate branch.
8. Integrate Phase 1 into `pulsell-agent` after `poruchen` passes.

## Known risks

- Copying `poruchen` schema verbatim would leak private tenant and role policy.
- Moving `handler.py` too early would bake ClickUp and CLI policies into the
  platform.
- Genericizing Telegram before one PTB path is stable would create adapter
  churn.
- App repos can drift again if platform APIs are too thin and teams copy worker
  internals back into apps.
