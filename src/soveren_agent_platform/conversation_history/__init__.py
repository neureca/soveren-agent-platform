"""Conversation history contracts, adapters, and tools."""

from soveren_agent_platform.conversation_history.contracts import (
    ConversationHistoryStore,
    ConversationMessage,
    ConversationSearchHit,
    MessageDirection,
)
from soveren_agent_platform.conversation_history.sqlite import SQLiteConversationHistoryStore
from soveren_agent_platform.conversation_history.tools import (
    CONVERSATION_HISTORY_TOOL_NAMESPACE,
    register_conversation_history_tools,
)

__all__ = [
    "CONVERSATION_HISTORY_TOOL_NAMESPACE",
    "ConversationHistoryStore",
    "ConversationMessage",
    "ConversationSearchHit",
    "MessageDirection",
    "SQLiteConversationHistoryStore",
    "register_conversation_history_tools",
]
