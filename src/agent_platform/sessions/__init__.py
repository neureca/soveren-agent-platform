"""Execution session contracts and mailbox."""

from agent_platform.sessions.backend import CaptureResult, OpenResult, OpenSpec, SessionBackend
from agent_platform.sessions.backends import (
    CodexAppServerBackend,
    CodexAppServerError,
    DynamicToolCall,
    DynamicToolRegistry,
    DynamicToolResult,
    DynamicToolSpec,
    StubBackend,
    TmuxBackend,
)
from agent_platform.sessions.events import record_session_event
from agent_platform.sessions.mailbox import enqueue_prompt
from agent_platform.sessions.mailbox_worker import drain_once, run_session_mailbox_worker
from agent_platform.sessions.registry import SessionBackendMapping, SessionBackendRegistry
from agent_platform.sessions.routing import (
    DeterministicSessionRouter,
    RouteHint,
    SessionRouteRequest,
    SessionRouteResult,
    SessionRouter,
    SessionSnapshot,
)

__all__ = [
    "RouteHint",
    "CaptureResult",
    "CodexAppServerBackend",
    "CodexAppServerError",
    "DeterministicSessionRouter",
    "DynamicToolCall",
    "DynamicToolRegistry",
    "DynamicToolResult",
    "DynamicToolSpec",
    "OpenResult",
    "OpenSpec",
    "SessionRouteRequest",
    "SessionRouteResult",
    "SessionBackend",
    "SessionBackendMapping",
    "SessionBackendRegistry",
    "SessionRouter",
    "SessionSnapshot",
    "StubBackend",
    "TmuxBackend",
    "drain_once",
    "enqueue_prompt",
    "record_session_event",
    "run_session_mailbox_worker",
]
