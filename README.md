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
- layered migration runner
- platform migrations for `event_queue` and `agent_runs`
- durable queue API
- inbound batching module with SQLite state and flush wakeups
- agent worker module that consumes queue events and calls app-provided agents
- cron module with due-job leasing and app-provided handlers
- Telegram interface module that normalizes Telegram ingress into queue events
- LLM backend contracts
- agent run persistence helpers
- planner envelope that injects session routing metadata into LLM requests
- execution session mailbox for prompts queued behind busy sessions
- persistent execution-session events, snapshots, and deterministic routing
- reusable stub and tmux execution session backends
- generic actions/approvals lifecycle with app-registered executors
- generic outbound channel queue with app-registered senders
- decision dispatcher that maps typed decisions to outbound, actions, session
  mailbox, or cron side effects
- runtime supervisor and `AgentPlatformApp` composition helper for standard
  platform workers

See [docs/EXTRACTION_PLAN.md](docs/EXTRACTION_PLAN.md) for the rollout plan.

## Local Development

```bash
uv sync
uv run pytest
```
