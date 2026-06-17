# Agent Platform

Reusable runtime core for durable agent applications.

This repository is the extraction target for the shared runtime currently
implemented inside `poruchen`. It is intentionally separate from both
application repositories:

- `agent-platform` owns reusable mechanics: SQLite runtime primitives, durable
  queueing, run tracking, decision/action framework, batching, scheduler,
  sessions, and integration contracts.
- `poruchen` owns private product behavior: prompts, ClickUp tools, approval
  copy, policies, and app-specific schema.
- `pulsell-agent` owns Pulsell-specific media, transcription, vision, task, and
  workflow behavior.

The first usable slice in this repo contains:

- SQLite connection setup
- durable queue port with a SQLite adapter
- layered migration runner with platform/app migration providers
- platform migrations for `event_queue` and `agent_runs`
- durable queue API
- inbound batching module with SQLite state and flush wakeups
- agent worker module that consumes queue events and calls app-provided agents
- cron module with due-job leasing and app-provided handlers
- Telegram interface module that normalizes Telegram ingress into queue events
- optional python-telegram-bot adapter for inbound normalization and outbound sending
- LLM backend contracts and reusable OpenAI-compatible/session-backed backends
- agent run persistence helpers
- rich planner context builder for batches, sessions, mailbox, actions,
  outbound, cron, and routing metadata
- optional app-neutral prompt formatter for rich planner context
- planner envelope that injects session routing and rich context into LLM
  requests
- execution session mailbox for prompts queued behind busy sessions
- persistent execution-session events, snapshots, and deterministic routing
- reusable stub and tmux execution session backends
- reusable Codex app-server execution session backend
- Codex app-server dynamic tool contracts, registry, and fail-closed JSON-RPC
  tool-call handling
- session backend registry for wiring reusable and custom backends
- platform SQLite migration and schema compatibility checks
- generic actions/approvals lifecycle with app-registered executors
- generic outbound channel queue with app-registered senders
- decision dispatcher that maps typed decisions to outbound, actions, session
  mailbox, or cron side effects
- planner-dispatch helper for fake-tested context to side-effect pipelines
- runtime supervisor and `AgentPlatformApp` composition helper for standard
  platform workers

See [docs/EXTRACTION_PLAN.md](docs/EXTRACTION_PLAN.md) for the rollout plan.
See [docs/PORTS.md](docs/PORTS.md) for the queue/store abstraction strategy.

## Local Development

```bash
uv sync
uv run pytest
```
