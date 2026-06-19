"""Generic channel contracts for non-core communication interfaces."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class ChannelInboundMessage:
    channel: str
    tenant_id: str
    source_id: str
    sender_id: str | None
    text: str | None
    payload: dict[str, Any]


@dataclass(slots=True)
class ChannelOutboundMessage:
    channel: str
    tenant_id: str
    destination_id: str
    text: str
    payload: dict[str, Any] | None = None

