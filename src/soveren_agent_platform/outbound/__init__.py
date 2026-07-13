"""Outbound channel runtime."""

from soveren_agent_platform.outbound.contracts import (
    ChannelSender,
    OutboundMessage,
    OutboundQueue,
    SendNotStartedError,
    SendResult,
)
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.outbound.sqlite import SQLiteOutboundQueue
from soveren_agent_platform.outbound.worker import run_outbound_queue_worker, run_outbound_worker

__all__ = [
    "ChannelSender",
    "OutboundMessage",
    "OutboundQueue",
    "OutboundRegistry",
    "SendNotStartedError",
    "SendResult",
    "SQLiteOutboundQueue",
    "run_outbound_queue_worker",
    "run_outbound_worker",
]
