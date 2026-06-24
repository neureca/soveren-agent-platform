"""Execution session contracts and mailbox."""

from soveren_agent_platform.sessions.backend import CaptureResult, OpenResult, OpenSpec, SessionBackend
from soveren_agent_platform.sessions.backends import (
    CodexAppServerBackend,
    CodexAppServerError,
    CodexThreadInspector,
    DynamicToolCall,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
    StubBackend,
    TmuxBackend,
)
from soveren_agent_platform.sessions.contracts import (
    MailboxItem,
    RuntimeSession,
    RuntimeSessionContextSnapshot,
    RuntimeSessionEvent,
    SessionEventStore,
    SessionInspection,
    SessionInspector,
    SessionMailboxStore,
    SessionSnapshotStore,
    SessionStore,
)
from soveren_agent_platform.sessions.events import record_session_event
from soveren_agent_platform.sessions.indexer_worker import (
    index_store_once,
    run_session_indexer_store_worker,
    run_session_indexer_worker,
)
from soveren_agent_platform.sessions.inspector_registry import (
    SessionInspectorMapping,
    SessionInspectorRegistry,
)
from soveren_agent_platform.sessions.lifecycle import (
    CloseSessionResult,
    SessionLifecyclePolicy,
    close_idle_sessions,
    close_session,
)
from soveren_agent_platform.sessions.mailbox import enqueue_prompt
from soveren_agent_platform.sessions.mailbox_worker import (
    drain_once,
    drain_store_once,
    run_session_mailbox_store_worker,
    run_session_mailbox_worker,
)
from soveren_agent_platform.sessions.registry import SessionBackendMapping, SessionBackendRegistry
from soveren_agent_platform.sessions.routing import (
    DeterministicSessionRouter,
    RouteHint,
    SessionRouter,
    SessionRouteRequest,
    SessionRouteResult,
    SessionSnapshot,
)
from soveren_agent_platform.sessions.sqlite import (
    SQLiteSessionEventStore,
    SQLiteSessionMailboxStore,
    SQLiteSessionSnapshotStore,
    SQLiteSessionStore,
)
from soveren_agent_platform.sessions.tools import SESSION_TOOL_NAMESPACE, register_session_directory_tools

__all__ = [
    "RouteHint",
    "CaptureResult",
    "CodexAppServerBackend",
    "CodexAppServerError",
    "CodexThreadInspector",
    "CloseSessionResult",
    "DeterministicSessionRouter",
    "DynamicToolCall",
    "DynamicToolRegistry",
    "DynamicToolResult",
    "DynamicToolSpec",
    "MailboxItem",
    "OpenResult",
    "OpenSpec",
    "RuntimeSessionContextSnapshot",
    "RuntimeSessionEvent",
    "RuntimeSession",
    "SessionInspection",
    "SessionInspector",
    "SessionInspectorMapping",
    "SessionInspectorRegistry",
    "SessionLifecyclePolicy",
    "SessionRouteRequest",
    "SessionRouteResult",
    "SessionBackend",
    "SessionBackendMapping",
    "SessionBackendRegistry",
    "SessionEventStore",
    "SessionMailboxStore",
    "SessionSnapshotStore",
    "SessionRouter",
    "SessionSnapshot",
    "SessionStore",
    "SESSION_TOOL_NAMESPACE",
    "SQLiteSessionEventStore",
    "SQLiteSessionMailboxStore",
    "SQLiteSessionSnapshotStore",
    "SQLiteSessionStore",
    "StubBackend",
    "TmuxBackend",
    "drain_store_once",
    "drain_once",
    "close_idle_sessions",
    "close_session",
    "enqueue_prompt",
    "index_store_once",
    "record_session_event",
    "register_session_directory_tools",
    "run_session_indexer_store_worker",
    "run_session_indexer_worker",
    "run_session_mailbox_store_worker",
    "run_session_mailbox_worker",
]
