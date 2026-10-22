# Trusted dialogue execution context

`ToolExecutionContext` carries host-authored identity for one model execution:

```python
from soveren_agent_platform.conversation import ConversationScope
from soveren_agent_platform.execution import ToolExecutionContext

context = ToolExecutionContext(
    scope=ConversationScope(tenant_id="tenant-a", source_id="telegram:42"),
    execution_id="execution-123",
    task_id="task-456",
)
```

`scope` remains the existing conversation boundary. `execution_id` identifies
this execution, and `task_id` is optional so a routing phase can run before a
task has been assigned. The platform does not add this context to prompts,
request metadata, native RPC arguments, or model-provided values. A dynamic
tool handler reads it from `DynamicToolCall.execution_context`.
Nested registries can use `DynamicToolRegistry.dispatch(call, allowlist=...)`
to preserve the host-resolved context through their own scope and selector
checks; `call(params, execution_context=...)` remains available for raw callback
parameters and never reads context from those parameters.

`LlmRequest` and `PlannerRuntimeConfig` accept three optional request settings:

- `execution_context`: the trusted `ToolExecutionContext` for this model turn;
- `output_schema`: a schema override for this turn (the configured backend
  schema remains the default when this field is `None`);
- `dynamic_tool_allowlist`: an immutable tuple of exact
  `(namespace, tool_name)` selectors. `None` retains the conversation registry's
  current tools, while `()` advertises and permits no dynamic tools.

For example, the route phase supplies `task_id=None` and an empty allowlist.
An execution phase supplies its persisted task ID and the selectors authorized
for that phase. The application owns those decisions and must build the context
from trusted persisted execution state.

`SessionLlmBackend` forwards contextual requests through the additive
`ContextualSessionBackend.send_with_context(...)` capability. A backend that
does not implement the requested contextual capability raises
`UnsupportedContextualSessionError`. Existing requests without a context or
allowlist continue through the original `send(...)` or output-schema path.

Codex app-server registers dynamic tools at `thread/start`, so the platform
filters the request allowlist before it creates the native thread. It also
checks the allowlist again when dispatching each callback. Before every
`turn/start`, including legacy no-context sends, the host prepares an immutable
binding for that native turn. A tool callback that arrives before the start
acknowledgment waits for it, then must carry the exact acknowledged turn ID.
Callbacks are accepted only for a prepared or active turn; unknown, mismatched,
failed, completed, released, or closed turns fail closed. Legacy no-context
callbacks keep their default tools after the turn is acknowledged. Releasing a
turn removes its identity, so late callbacks cannot fall through to legacy
dispatch and completed threads do not leave per-turn tombstones. RPC fields
cannot supply or replace the trusted context. The scope must match both the
LLM request and the opened native thread.

The conversation backend, registry, and sandbox remain cached by the original
conversation scope. The context binding is per native turn; it does not create
a per-task sandbox or native-thread persistence layer.
