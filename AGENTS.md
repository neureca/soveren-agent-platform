# AGENTS.md

Guidance for coding agents working in this repository.

## Read First

Before changing architecture or module boundaries, read:

- `docs/ARCHITECTURE.md` — current runtime architecture, module ownership,
  event/session flows, extension rules, and package naming notes.
- `docs/API.md` — consumer-facing integration API, bootstrap contract, runtime
  composition, and packaging/deployment dependency guidance.
- `docs/PORTS.md` — queue/store ports, adapter semantics, and persistence
  boundaries.
- `docs/EXTRACTION_PLAN.md` — extraction history and rollout context.

This repo is the reusable runtime core. Keep product prompts, business tools,
private schema, product copy, and app-specific workflows in application repos.

## Commands

Run from the repo root:

```bash
uv sync --group dev
uv run ruff check src tests
uv run mypy
uv run pytest
```

For a focused test:

```bash
uv run pytest tests/test_session_mailbox.py
uv run pytest tests/test_session_mailbox.py::test_mailbox_drain_uses_session_and_mailbox_ports
```

Before considering a code change done, run:

```bash
uv run ruff check src tests && uv run mypy && uv run pytest
```

## Python Style

- Target Python is `>=3.12`.
- Use type annotations on public functions, protocols, dataclasses, worker
  entrypoints, and adapter methods.
- Prefer `dataclass(slots=True)` or `dataclass(frozen=True, slots=True)` for
  typed value objects.
- Use `Protocol` for runtime ports.
- Avoid untyped dictionaries at module boundaries. If payloads must be dynamic,
  keep them at the edge and convert to typed objects quickly.
- Do not silence mypy unless there is a precise reason and a narrow comment.
- Keep imports sorted by `ruff`.

## Engineering Rules

- Preserve the architecture documented in `docs/ARCHITECTURE.md`.
- Preserve port semantics documented in `docs/PORTS.md`.
- Do not introduce generic CRUD repositories.
- Do not split atomic runtime boundaries such as batch routing, action dispatch,
  queue lease/retry, or session mailbox delivery.
- Do not make routers depend directly on Codex, Claude, tmux, or app-server
  native APIs; route through generalized session snapshots and platform ports.
- Do not add app-owned tables or product seed data to platform migrations.
- If a new architectural boundary appears, update `docs/ARCHITECTURE.md` and
  `docs/PORTS.md` in the same change.

## Testing Expectations

Add or update tests when changing:

- queue lease/retry/idempotency behavior
- batching decisions or `route_batch`
- action dispatch and approval transitions
- mailbox/session status transitions
- snapshot/routing/indexing behavior
- migration/schema compatibility
- public composition APIs in `AgentPlatformApp`

Prefer focused unit tests around ports and fake adapters. Add integration-style
SQLite tests for transaction boundaries.
