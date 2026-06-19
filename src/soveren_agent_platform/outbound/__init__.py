"""Outbound channel runtime."""

from soveren_agent_platform.outbound.contracts import ChannelSender, OutboundMessage, OutboundQueue, SendResult
from soveren_agent_platform.outbound.registry import OutboundRegistry
from soveren_agent_platform.outbound.sqlite import SQLiteOutboundQueue
from soveren_agent_platform.outbound.store import enqueue_outbound
from soveren_agent_platform.outbound.worker import run_outbound_queue_worker, run_outbound_worker

__all__ = [
    "ChannelSender",
    "OutboundMessage",
    "OutboundQueue",
    "OutboundRegistry",
    "SendResult",
    "SQLiteOutboundQueue",
    "enqueue_outbound",
    "run_outbound_queue_worker",
    "run_outbound_worker",
]
