<p align="center">
  <img src="docs/assets/soveren-logo.svg" width="96" height="96" alt="Soveren logo" />
</p>

<h1 align="center">Soveren Agent Platform</h1>

<p align="center">
  Reusable runtime core for durable agent applications.
</p>

This repository is the extraction target for the shared runtime currently
implemented inside `poruchen`. It is intentionally separate from both
application repositories:

- `soveren-agent-platform` owns reusable mechanics: durable queueing, run tracking,
  decision/action framework, batching, scheduler, sessions, integration
  contracts, and bundled SQLite adapters for the default embedded runtime.
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
- optional python-telegram-bot runtime builder with message and callback hooks
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

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the current architecture.
See [docs/API.md](docs/API.md) for the consumer integration API and quick start.
See [docs/EXTRACTION_PLAN.md](docs/EXTRACTION_PLAN.md) for the rollout plan.
See [docs/PORTS.md](docs/PORTS.md) for the queue/store abstraction strategy.

## Consumer Quick Start

```python
from pathlib import Path

from soveren_agent_platform.agent import AgentEvent, AgentHandler
from soveren_agent_platform.app_api import AgentPlatformApp


class AppAgentHandler(AgentHandler):
    async def handle(self, event: AgentEvent) -> None:
        ...


app = (
    AgentPlatformApp(db_path=Path("data/app.db"))
    .use_batching()
    .use_agent(handler=AppAgentHandler())
)
```

`AgentPlatformApp` applies and validates platform migrations before workers
start. Apps with a separate migration pipeline can call
`soveren_agent_platform.storage.bootstrap_platform_storage(db_path)` themselves and pass
`bootstrap_storage=False`.

## Local Development

```bash
uv sync --group dev
uv run ruff check src tests
uv run mypy
uv run pytest
```
