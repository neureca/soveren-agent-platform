"""Outbound channel runtime."""

from agent_platform.outbound.contracts import ChannelSender, OutboundMessage, OutboundQueue, SendResult
from agent_platform.outbound.registry import OutboundRegistry
from agent_platform.outbound.sqlite import SQLiteOutboundQueue
from agent_platform.outbound.store import enqueue_outbound
from agent_platform.outbound.worker import run_outbound_queue_worker, run_outbound_worker

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
